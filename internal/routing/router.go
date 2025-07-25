package routing

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/JosineyJr/rdb25_02/internal/config"
	"github.com/JosineyJr/rdb25_02/internal/storage"
	"github.com/JosineyJr/rdb25_02/pkg/payments"
	jsoniter "github.com/json-iterator/go"
	"github.com/valyala/fasthttp"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

const (
	StateClosed   int32 = 0
	StateOpen     int32 = 1
	StateHalfOpen int32 = 2
)

const (
	failureThreshold = 15
	openStateTimeout = 5 * time.Second
)

type AdaptiveRouter struct {
	cbLastOpenTime  time.Time
	agg             *storage.RedisDB
	client          *fasthttp.Client
	PayloadChan     chan payments.PaymentsPayload
	BytesChan       chan []byte
	workers         int
	cbState         atomic.Int32
	cbFailures      atomic.Int32
	defaultLatency  atomic.Int32
	fallbackLatency atomic.Int32
}

func NewAdaptiveRouter(
	workers int,
	agg *storage.RedisDB,
) *AdaptiveRouter {
	return &AdaptiveRouter{
		client:      &fasthttp.Client{},
		workers:     workers,
		PayloadChan: make(chan payments.PaymentsPayload, 6500),
		BytesChan:   make(chan []byte, 6500),
		agg:         agg,
	}
}

func (ar *AdaptiveRouter) UpdateHealthMetrics(
	defaultLatency, fallbackLatency int,
	isDefaultFailing bool,
) {
	ar.defaultLatency.Store(int32(defaultLatency))
	ar.fallbackLatency.Store(int32(fallbackLatency))

	if isDefaultFailing && ar.cbState.Load() != StateOpen {
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
				case b := <-ar.BytesChan:
					var payload payments.PaymentsPayload
					if err := json.Unmarshal(b, &payload); err != nil {
						continue
					}
					ar.PayloadChan <- payload
				case p := <-ar.PayloadChan:
					if ar.agg.PaymentIsProcessed(ctx, p.CorrelationID) {
						continue
					}

					targetFunc, processorName := ar.chooseProcessor()
					if targetFunc == nil {
						ar.PayloadChan <- p
						continue
					}

					success := targetFunc(ctx, &p)

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
	state := ar.cbState.Load()

	if state == StateOpen {
		if time.Since(ar.cbLastOpenTime) > openStateTimeout {
			ar.cbState.Store(StateHalfOpen)
		} else {
			return ar.sendToFallback, payments.FallbackProcessor
		}
	}

	dl := ar.defaultLatency.Load()
	fl := ar.fallbackLatency.Load()

	if state == StateHalfOpen {
		if dl > 80 {
			return nil, ""
		}

		return ar.sendToDefault, payments.DefaultProcessor
	}

	if fl > 0 && dl > (3*fl) {
		if fl > 20 {
			return nil, ""
		}

		return ar.sendToFallback, payments.FallbackProcessor
	}

	if dl > 80 {
		return nil, ""
	}

	return ar.sendToDefault, payments.DefaultProcessor
}

func (ar *AdaptiveRouter) updateCircuitState(processorName string, success bool) {
	if processorName == payments.FallbackProcessor {
		return
	}
	if ar.cbState.Load() == StateHalfOpen {
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
	ar.cbState.Store(StateClosed)
	ar.cbFailures.Store(0)
}

func (ar *AdaptiveRouter) openCircuit() {
	ar.cbState.Store(StateOpen)
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
