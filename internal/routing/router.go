package routing

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"math"
	"math/rand"
	"net/http"
	"time"

	"github.com/JosineyJr/rdb25_02/internal/config"
	"github.com/JosineyJr/rdb25_02/internal/storage"
	"github.com/JosineyJr/rdb25_02/pkg/payments"
)

type AdaptiveRouter struct {
	KalmanDefault  *KalmanFilter
	PIDDefault     *PIDController
	KalmanFallback *KalmanFilter
	PIDFallback    *PIDController
	agg            *storage.RedisAggregator

	Client  *http.Client
	Workers int
	Queue   *PriorityQueue
	Done    chan struct{}
}

func NewAdaptiveRouter(
	kD *KalmanFilter,
	pD *PIDController,
	kF *KalmanFilter,
	pF *PIDController,
	workers int,
	agg *storage.RedisAggregator,
) *AdaptiveRouter {
	return &AdaptiveRouter{
		KalmanDefault:  kD,
		PIDDefault:     pD,
		KalmanFallback: kF,
		PIDFallback:    pF,
		Client:         &http.Client{Timeout: 2 * time.Second},
		Workers:        workers,
		Queue:          NewPriorityQueue(),
		Done:           make(chan struct{}),
		agg:            agg,
	}
}

func (ar *AdaptiveRouter) Start(ctx context.Context) {
	for i := 0; i < ar.Workers; i++ {
		go func() {
			for {
				select {
				case <-ar.Done:
					return
				default:
					br := ar.Queue.Pop()
					if br == nil {
						continue
					}
					ar.handleBatch(br)
				}
			}
		}()
	}
}

func (ar *AdaptiveRouter) handleBatch(br *batchRequest) {
	healthDefault := ar.KalmanDefault.Estimate()
	healthFallback := ar.KalmanFallback.Estimate()

	penaltyDefault := math.Max(0, ar.PIDDefault.Compute(healthDefault))
	penaltyFallback := math.Max(0, ar.PIDFallback.Compute(healthFallback))

	costDefault := 0.05
	costFallback := 0.15

	scoreDefault := 1.0 / (1.0 + penaltyDefault + costDefault)
	scoreFallback := 1.0 / (1.0 + penaltyFallback + costFallback)

	totalScore := scoreDefault + scoreFallback
	if totalScore == 0 {
		if br.Priority < 1000 {
			br.Priority++
			ar.Queue.Push(br)
		}
		return
	}

	fracDefault := scoreDefault / totalScore
	var target func(*payments.PaymentsPayload) bool
	if rand.Float64() < fracDefault {
		target = ar.sendToDefault
	} else {
		target = ar.sendToFallback
	}

	failedPayloads := make([]*payments.PaymentsPayload, 0)

	for _, payload := range br.Requests {
		if !target(payload) {
			failedPayloads = append(failedPayloads, payload)
		}
	}

	if len(failedPayloads) > 0 {
		if br.Priority < 1000 {
			failedBatch := &batchRequest{
				Requests: failedPayloads,
				Priority: br.Priority + 1,
			}
			ar.Queue.Push(failedBatch)
		}
	}
}

func (ar *AdaptiveRouter) Stop() {
	close(ar.Done)
}

func (ar *AdaptiveRouter) EnqueueBatch(reqs []*payments.PaymentsPayload, priority int) {
	br := &batchRequest{Requests: reqs, Priority: priority}
	ar.Queue.Push(br)
}

func (ar *AdaptiveRouter) sendToDefault(payload *payments.PaymentsPayload) bool {
	return ar.sendRequest(
		config.PAYMENTS_PROCESSOR_URL_DEFAULT,
		"default",
		payload,
		ar.KalmanDefault,
	)
}

func (ar *AdaptiveRouter) sendToFallback(payload *payments.PaymentsPayload) bool {
	return ar.sendRequest(
		config.PAYMENTS_PROCESSOR_URL_FALLBACK,
		"fallback",
		payload,
		ar.KalmanFallback,
	)
}

func (ar *AdaptiveRouter) sendRequest(
	url, processorName string,
	payload *payments.PaymentsPayload,
	kf *KalmanFilter,
) bool {
	body, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Error marshalling payload: %v", err)
		return false
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		log.Printf("Error creating request: %v", err)
		return false
	}
	req.Header.Set("Content-Type", "application/json")

	startTime := time.Now()
	resp, err := ar.Client.Do(req)
	latency := time.Since(startTime).Seconds()

	var measuredState State
	measuredState.Latency = latency

	success := false
	if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		success = true
		measuredState.ErrorRate = 0.0
		ar.agg.Update(context.Background(), processorName, payload.Amount)
	} else {
		measuredState.ErrorRate = 1.0
	}

	kf.Predict()
	kf.Update(measuredState, kf.R)

	if resp != nil {
		resp.Body.Close()
	}

	return success
}
