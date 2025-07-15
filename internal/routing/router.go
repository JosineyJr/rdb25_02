package routing

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JosineyJr/rdb25_02/internal/config"
	"github.com/JosineyJr/rdb25_02/internal/storage"
	"github.com/JosineyJr/rdb25_02/pkg/payments"
	jsoniter "github.com/json-iterator/go"
	"github.com/valyala/fasthttp"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type CircuitState int

const (
	StateClosed CircuitState = iota
	StateOpen
	StateHalfOpen
)

const (
	failureThreshold         = 5
	openStateTimeout         = 5 * time.Second
	absoluteLatencyThreshold = 0.2
)

type AdaptiveRouter struct {
	cbLastOpenTime  time.Time
	agg             *storage.RedisDB
	client          *fasthttp.Client
	PayloadChan     chan *payments.PaymentsPayload
	workers         int
	cbState         CircuitState
	cbFailures      atomic.Int32
	defaultLatency  float64
	fallbackLatency float64
	cbMutex         sync.RWMutex
}

func NewAdaptiveRouter(
	workers int,
	agg *storage.RedisDB,
) *AdaptiveRouter {
	return &AdaptiveRouter{
		client:          &fasthttp.Client{},
		workers:         workers,
		PayloadChan:     make(chan *payments.PaymentsPayload, 6500),
		agg:             agg,
		cbState:         StateClosed,
		defaultLatency:  0.0,
		fallbackLatency: 0.0,
	}
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
				case p := <-ar.PayloadChan:
					if ar.agg.PaymentIsProcessed(ctx, p.CorrelationID) {
						continue
					}

					targetFunc, processorName := ar.chooseProcessor()
					if targetFunc == nil {
						ar.PayloadChan <- p
						time.Sleep(100 * time.Millisecond)
						continue
					}

					success := targetFunc(ctx, p)

					ar.updateCircuitState(processorName, success)

					if !success {
						ar.PayloadChan <- p
						continue
					}

					ar.agg.MarkAsProcessed(ctx, p.CorrelationID)
				}
			}
		}()
	}
}

func (ar *AdaptiveRouter) chooseProcessor() (target func(context.Context, *payments.PaymentsPayload) bool, processorName string) {
	ar.cbMutex.RLock()
	defer ar.cbMutex.RUnlock()

	if ar.cbState == StateOpen {
		if time.Since(ar.cbLastOpenTime) > openStateTimeout {
			ar.cbState = StateHalfOpen
		} else {
			return ar.sendToFallback, payments.FallbackProcessor
		}
	}

	if ar.cbState == StateHalfOpen {
		return ar.sendToDefault, payments.DefaultProcessor
	}

	if ar.defaultLatency < absoluteLatencyThreshold {
		return ar.sendToDefault, payments.DefaultProcessor
	}

	if ar.fallbackLatency > 0 && ar.defaultLatency > (3*ar.fallbackLatency) {
		if ar.fallbackLatency > 0.1 {
			return nil, ""
		}

		return ar.sendToFallback, payments.FallbackProcessor
	}

	if ar.defaultLatency > 0.1 {
		return nil, ""
	}

	return ar.sendToDefault, payments.DefaultProcessor
}

func (ar *AdaptiveRouter) updateCircuitState(processorName string, success bool) {
	if processorName == payments.FallbackProcessor {
		return
	}
	ar.cbMutex.Lock()
	defer ar.cbMutex.Unlock()

	if ar.cbState == StateHalfOpen {
		if !success {
			ar.openCircuit()
		} else {
			ar.resetCircuit()
		}
		return
	}

	if !success {
		newFailures := ar.cbFailures.Add(1)
		if newFailures >= failureThreshold {
			ar.openCircuit()
		}
	} else if success {
		ar.resetCircuit()
	}
}

func (ar *AdaptiveRouter) resetCircuit() {
	ar.cbState = StateClosed
	ar.cbFailures.Store(0)
}

func (ar *AdaptiveRouter) openCircuit() {
	ar.cbState = StateOpen
	ar.cbLastOpenTime = time.Now()
}

func (ar *AdaptiveRouter) sendToDefault(
	ctx context.Context,
	payload *payments.PaymentsPayload,
) bool {
	return ar.sendRequest(
		ctx,
		config.PAYMENTS_PROCESSOR_URL_DEFAULT,
		payments.DefaultProcessor,
		payload,
	)
}

func (ar *AdaptiveRouter) sendToFallback(
	ctx context.Context,
	payload *payments.PaymentsPayload,
) bool {
	return ar.sendRequest(
		ctx,
		config.PAYMENTS_PROCESSOR_URL_FALLBACK,
		payments.FallbackProcessor,
		payload,
	)
}

func (ar *AdaptiveRouter) sendRequest(
	ctx context.Context,
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

	payload.RequestedAt = time.Now().UTC()
	body, err := json.Marshal(payload)
	if err != nil {
		return false
	}
	req.SetBody(body)

	err = ar.client.Do(req, resp)
	if err != nil {
		return false
	}

	if resp.StatusCode() >= 200 && resp.StatusCode() < 300 {
		ar.agg.Update(ctx, processorName, payload)
		return true
	}

	return false
}
