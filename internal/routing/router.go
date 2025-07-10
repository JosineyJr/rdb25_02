package routing

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/JosineyJr/rdb25_02/internal/config"
	"github.com/JosineyJr/rdb25_02/internal/storage"
	"github.com/JosineyJr/rdb25_02/pkg/payments"
	"github.com/valyala/fasthttp"
)

// --- Estados do Circuit Breaker ---
type CircuitState int

const (
	StateClosed CircuitState = iota
	StateOpen
	StateHalfOpen
)

const (
	failureThreshold = 15
	openStateTimeout = 5 * time.Second
)

type AdaptiveRouter struct {
	agg             *storage.RedisAggregator
	client          *fasthttp.Client
	workers         int
	done            chan struct{}
	payloadChan     chan *payments.PaymentsPayload
	cbState         CircuitState
	cbFailures      int
	cbLastOpenTime  time.Time
	cbMutex         sync.RWMutex
	defaultLatency  float64
	fallbackLatency float64
}

func NewAdaptiveRouter(
	workers int,
	agg *storage.RedisAggregator,
) *AdaptiveRouter {
	return &AdaptiveRouter{
		client:          &fasthttp.Client{},
		workers:         workers,
		payloadChan:     make(chan *payments.PaymentsPayload, 5192),
		done:            make(chan struct{}),
		agg:             agg,
		cbState:         StateClosed,
		cbFailures:      0,
		defaultLatency:  0.0,
		fallbackLatency: 0.0,
	}
}

func (ar *AdaptiveRouter) EnqueuePayment(p *payments.PaymentsPayload) {
	ar.payloadChan <- p
}

func (ar *AdaptiveRouter) UpdateHealthMetrics(
	defaultLatency, fallbackLatency float64,
	isDefaultFailing bool,
) {
	ar.cbMutex.Lock()
	defer ar.cbMutex.Unlock()
	ar.defaultLatency = defaultLatency
	ar.fallbackLatency = fallbackLatency
	if isDefaultFailing && ar.cbState != StateOpen {
		ar.openCircuit()
	}
}

func (ar *AdaptiveRouter) Start(ctx context.Context) {
	for i := 0; i < ar.workers; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case p := <-ar.payloadChan:
					targetFunc, processorName := ar.chooseProcessor()
					success := targetFunc(p)

					ar.updateCircuitState(processorName, success)

					if !success {
						log.Printf("Request for %s failed. Re-queueing once.", p.CorrelationID)
						ar.payloadChan <- p
					}
				}
			}
		}()
	}
}

func (ar *AdaptiveRouter) chooseProcessor() (target func(*payments.PaymentsPayload) bool, processorName string) {
	ar.cbMutex.Lock()
	defer ar.cbMutex.Unlock()

	if ar.cbState == StateOpen {
		if time.Since(ar.cbLastOpenTime) > openStateTimeout {
			log.Println("Circuit timeout expired. Moving to Half-Open state.")
			ar.cbState = StateHalfOpen
		} else {
			return ar.sendToFallback, payments.FallbackProcessor
		}
	}

	if ar.cbState == StateHalfOpen {
		log.Println("Circuit is Half-Open. Routing next request to 'default' as a test.")
		return ar.sendToDefault, payments.DefaultProcessor
	}

	if ar.fallbackLatency > 0 && ar.defaultLatency > (5*ar.fallbackLatency) {
		log.Printf(
			"Efficiency alert: Default latency (%.2fms) > 3 * Fallback latency (%.2fms). Using Fallback.",
			ar.defaultLatency*1000,
			ar.fallbackLatency*1000,
		)
		return ar.sendToFallback, payments.FallbackProcessor
	}

	return ar.sendToDefault, payments.DefaultProcessor
}

func (ar *AdaptiveRouter) updateCircuitState(processorName string, success bool) {
	ar.cbMutex.Lock()
	defer ar.cbMutex.Unlock()

	if processorName == payments.FallbackProcessor {
		return
	}

	if ar.cbState == StateHalfOpen {
		if !success {
			log.Println("Test request to 'default' failed. Re-opening circuit.")
			ar.openCircuit()
		} else {
			log.Println("Test request to 'default' succeeded. Closing circuit.")
			ar.resetCircuit()
		}
		return
	}

	if !success {
		ar.cbFailures++
		if ar.cbFailures >= failureThreshold {
			log.Printf("%d consecutive failures reached. Opening circuit.", ar.cbFailures)
			ar.openCircuit()
		}
	} else if success {
		ar.resetCircuit()
	}
}

func (ar *AdaptiveRouter) resetCircuit() {
	ar.cbState = StateClosed
	ar.cbFailures = 0
}

func (ar *AdaptiveRouter) openCircuit() {
	ar.cbState = StateOpen
	ar.cbLastOpenTime = time.Now()
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
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(url)
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.SetContentType("application/json")

	body, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Error marshalling payload: %v", err)
		return false
	}
	req.SetBody(body)

	err = ar.client.Do(req, resp)
	if err != nil {
		return false
	}

	if resp.StatusCode() >= 200 && resp.StatusCode() < 300 {
		ar.agg.Update(context.Background(), processorName, payload.Amount)
		return true
	}

	return false
}

func (ar *AdaptiveRouter) Stop() {
	close(ar.done)
}
