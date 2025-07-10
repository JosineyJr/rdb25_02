package routing

import (
	"context"
	"math"
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
	failureThreshold = 15
	openStateTimeout = 5 * time.Second
)

type AdaptiveRouter struct {
	cbLastOpenTime  time.Time
	agg             *storage.RedisAggregator
	client          *fasthttp.Client
	done            chan struct{}
	PayloadChan     chan *payments.PaymentsPayload
	workers         int
	cbState         atomic.Int32
	cbFailures      atomic.Int32
	defaultLatency  atomic.Uint64
	fallbackLatency atomic.Uint64
	cbMutex         sync.RWMutex
}

func NewAdaptiveRouter(
	workers int,
	agg *storage.RedisAggregator,
) *AdaptiveRouter {
	ar := &AdaptiveRouter{
		client:      &fasthttp.Client{},
		workers:     workers,
		PayloadChan: make(chan *payments.PaymentsPayload, 10240),
		done:        make(chan struct{}),
		agg:         agg,
		cbMutex:     sync.RWMutex{},
	}
	ar.cbState.Store(int32(StateClosed))
	return ar
}

func (ar *AdaptiveRouter) UpdateHealthMetrics(
	defaultLatency, fallbackLatency float64,
	isDefaultFailing bool,
) {
	ar.defaultLatency.Store(math.Float64bits(defaultLatency))
	ar.fallbackLatency.Store(math.Float64bits(fallbackLatency))

	if isDefaultFailing && CircuitState(ar.cbState.Load()) != StateOpen {
		ar.cbMutex.Lock()
		ar.openCircuit()
		ar.cbMutex.Unlock()
	}
}

func (ar *AdaptiveRouter) Start(ctx context.Context) {
	for range ar.workers {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case p := <-ar.PayloadChan:
					targetFunc, processorName := ar.chooseProcessor()

					success := targetFunc(p)

					ar.updateCircuitState(processorName, success)

					if !success {
						ar.PayloadChan <- p
					}
				}
			}
		}()
	}
}

func (ar *AdaptiveRouter) chooseProcessor() (target func(*payments.PaymentsPayload) bool, processorName string) {
	state := CircuitState(ar.cbState.Load())

	if state == StateOpen {
		ar.cbMutex.RLock()
		lastOpenTime := ar.cbLastOpenTime
		ar.cbMutex.RUnlock()
		if time.Since(lastOpenTime) > openStateTimeout {
			ar.cbState.CompareAndSwap(int32(StateOpen), int32(StateHalfOpen))
		} else {
			return ar.sendToFallback, payments.FallbackProcessor
		}
	}

	state = CircuitState(ar.cbState.Load())
	if state == StateHalfOpen {
		return ar.sendToDefault, payments.DefaultProcessor
	}

	defLatency := math.Float64frombits(ar.defaultLatency.Load())
	fallLatency := math.Float64frombits(ar.fallbackLatency.Load())

	if fallLatency > 0 && defLatency > (4*fallLatency) {
		return ar.sendToFallback, payments.FallbackProcessor
	}

	return ar.sendToDefault, payments.DefaultProcessor
}

func (ar *AdaptiveRouter) updateCircuitState(processorName string, success bool) {
	if processorName == payments.FallbackProcessor {
		return
	}

	state := CircuitState(ar.cbState.Load())
	if state == StateHalfOpen {
		if !success {
			ar.cbMutex.Lock()
			ar.openCircuit()
			ar.cbMutex.Unlock()
		} else {
			ar.resetCircuit()
		}

		return
	}

	if !success {
		newFailures := ar.cbFailures.Add(1)
		if newFailures >= failureThreshold {
			ar.cbMutex.Lock()
			ar.openCircuit()
			ar.cbMutex.Unlock()
		}
	} else if success {
		ar.resetCircuit()
	}
}

func (ar *AdaptiveRouter) resetCircuit() {
	ar.cbState.Store(int32(StateClosed))
	ar.cbFailures.Store(0)
}

func (ar *AdaptiveRouter) openCircuit() {
	ar.cbState.Store(int32(StateOpen))
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
		return false
	}
	req.SetBody(body)

	err = ar.client.Do(req, resp)
	if err != nil {
		return false
	}

	if resp.StatusCode() >= 200 && resp.StatusCode() < 300 {
		ar.agg.Update(context.Background(), processorName, payload)
		return true
	}

	return false
}

func (ar *AdaptiveRouter) Stop() {
	close(ar.done)
}
