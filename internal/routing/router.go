package routing

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/JosineyJr/rdb25_02/internal/config"
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
	failureThreshold = 2
	openStateTimeout = 2 * time.Second
)

type AdaptiveRouter struct {
	cbLastOpenTime  time.Time
	paymentsConn    *net.UnixConn
	client          *fasthttp.Client
	PayloadChan     chan string
	workers         int
	cbState         atomic.Int32
	cbFailures      atomic.Int32
	defaultLatency  atomic.Int32
	fallbackLatency atomic.Int32
}

func NewAdaptiveRouter(
	workers int,
	paymentsConn *net.UnixConn,
) *AdaptiveRouter {
	return &AdaptiveRouter{
		client:       &fasthttp.Client{},
		workers:      workers,
		PayloadChan:  make(chan string, 5500),
		paymentsConn: paymentsConn,
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
				case p := <-ar.PayloadChan:
					// if ar.agg.PaymentIsProcessed(ctx, p.CorrelationID) {
					// 	continue
					// }

					targetFunc, processorName := ar.chooseProcessor()
					if targetFunc == nil {
						time.Sleep(100 * time.Millisecond)
						ar.PayloadChan <- p
						continue
					}

					success := targetFunc(ctx, &p)

					ar.updateCircuitState(processorName, success)

					if !success {
						time.Sleep(100 * time.Millisecond)
						ar.PayloadChan <- p
						continue
					}

					// ar.agg.MarkAsProcessed(ctx, p.CorrelationID)
				}
			}
		}()
	}
}

func (ar *AdaptiveRouter) chooseProcessor() (target func(context.Context, *string) bool, processorName string) {
	state := ar.cbState.Load()

	if state == StateOpen {
		if time.Since(ar.cbLastOpenTime) > openStateTimeout {
			ar.cbState.Store(StateHalfOpen)
		} else {
			return nil, ""
		}
	}

	dl := ar.defaultLatency.Load()
	fl := ar.fallbackLatency.Load()

	if state == StateHalfOpen {
		// if dl > 30 {
		// 	return nil, ""
		// }

		return ar.sendToDefault, payments.DefaultProcessor
	}

	if fl > 0 && dl > (3*fl) {
		if fl > 30 {
			return ar.sendToDefault, payments.DefaultProcessor
		}

		return ar.sendToFallback, payments.FallbackProcessor
	}

	// if dl > 30 {
	// 	return nil, ""
	// }

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
	correlationId *string,
) bool {
	return ar.sendRequest(
		ctx,
		config.PAYMENTS_PROCESSOR_URL_DEFAULT,
		payments.DefaultProcessor,
		correlationId,
	)
}

func (ar *AdaptiveRouter) sendToFallback(
	ctx context.Context,
	correlationId *string,
) bool {
	return ar.sendRequest(
		ctx,
		config.PAYMENTS_PROCESSOR_URL_FALLBACK,
		payments.FallbackProcessor,
		correlationId,
	)
}

func (ar *AdaptiveRouter) sendRequest(
	ctx context.Context,
	url, processorName string,
	correlationId *string,
) bool {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.SetRequestURI(url)
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.SetContentType("application/json")

	payload := payments.PaymentsPayload{
		CorrelationID: correlationId,
		RequestedAt:   time.Now().UTC(),
		Amount:        19.9,
	}
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
		ar.paymentsConn.Write(
			[]byte(processorName + "|" + strconv.Itoa(int(payload.RequestedAt.UnixNano())) + "\n"),
		)
		return true
	}

	if resp.StatusCode() == http.StatusUnprocessableEntity {
		return true
	}

	return false
}
