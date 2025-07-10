package routing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/JosineyJr/rdb25_02/internal/config"
	"github.com/JosineyJr/rdb25_02/internal/storage"
	"github.com/JosineyJr/rdb25_02/pkg/payments"
)

type AdaptiveRouter struct {
	agg     *storage.RedisAggregator
	client  *http.Client
	workers int
	queue   *PriorityQueue
	done    chan struct{}
}

func NewAdaptiveRouter(
	workers int,
	agg *storage.RedisAggregator,
) *AdaptiveRouter {
	return &AdaptiveRouter{
		client:  &http.Client{Timeout: 2 * time.Second},
		workers: workers,
		queue:   NewPriorityQueue(),
		done:    make(chan struct{}),
		agg:     agg,
	}
}

func (ar *AdaptiveRouter) Start(ctx context.Context) {
	for i := 0; i < ar.workers; i++ {
		go func() {
			for {
				select {
				case <-ar.done:
					return
				default:
					br := ar.queue.Pop()
					if br == nil {
						continue
					}
					go ar.handleBatch(br)
				}
			}
		}()
	}
}

func (ar *AdaptiveRouter) handleBatch(br *batchRequest) {
	var target func(*payments.PaymentsPayload) bool
	target = ar.sendToDefault

	failedPayloads := make([]*payments.PaymentsPayload, 0)

	for _, payload := range br.Requests {
		if !target(payload) {
			failedPayloads = append(failedPayloads, payload)
		}
	}

	if len(failedPayloads) > 0 {
		if br.Priority < 10 {
			failedBatch := &batchRequest{
				Requests: failedPayloads,
				Priority: br.Priority + 1,
			}
			ar.queue.Push(failedBatch)
		} else {
			fmt.Println("batch discarded 2")
		}
	}
}

func (ar *AdaptiveRouter) Stop() {
	close(ar.done)
}

func (ar *AdaptiveRouter) EnqueueBatch(reqs []*payments.PaymentsPayload, priority int) {
	br := &batchRequest{Requests: reqs, Priority: priority}
	ar.queue.Push(br)
}

func (ar *AdaptiveRouter) sendToDefault(payload *payments.PaymentsPayload) bool {
	return ar.sendRequest(
		config.PAYMENTS_PROCESSOR_URL_DEFAULT,
		payments.DefaultProcessor,
		payload,
	)
}

func (ar *AdaptiveRouter) sendToFallback(payload *payments.PaymentsPayload) bool {
	return ar.sendRequest(
		config.PAYMENTS_PROCESSOR_URL_FALLBACK,
		payments.FallbackProcessor,
		payload,
	)
}

func (ar *AdaptiveRouter) sendRequest(
	url, processorName string,
	payload *payments.PaymentsPayload,
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
	resp, err := ar.client.Do(req)
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

	if resp != nil {
		resp.Body.Close()
	}

	return success
}
