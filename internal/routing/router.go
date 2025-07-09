package routing

import (
	"context"
	"math/rand"
	"net/http"
	"time"
)

type AdaptiveRouter struct {
	Kalman  *KalmanFilter
	PID     *PIDController
	Client  *http.Client
	Workers int
	Queue   *PriorityQueue
	Done    chan struct{}
}

func NewAdaptiveRouter(k *KalmanFilter, p *PIDController, workers int) *AdaptiveRouter {
	return &AdaptiveRouter{
		Kalman:  k,
		PID:     p,
		Client:  &http.Client{Timeout: 2 * time.Second},
		Workers: workers,
		Queue:   NewPriorityQueue(),
		Done:    make(chan struct{}),
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
	health := ar.Kalman.Estimate()
	frac := ar.PID.Compute(health)
	target := ar.sendToFallback
	if rand.Float64() < frac {
		target = ar.sendToDefault
	}
	for _, req := range br.Requests {
		target(req)
	}
}

func (ar *AdaptiveRouter) Stop() {
	close(ar.Done)
}

func (ar *AdaptiveRouter) EnqueueBatch(reqs []*http.Request, priority int) {
	br := &batchRequest{Requests: reqs, Priority: priority}
	ar.Queue.Push(br)
}

func (ar *AdaptiveRouter) sendToDefault(req *http.Request) {
	req.URL.Host = "default-processor:8080"
	req.URL.Scheme = "http"
	ar.Client.Do(req)
}

func (ar *AdaptiveRouter) sendToFallback(req *http.Request) {
	req.URL.Host = "fallback-processor:8080"
	req.URL.Scheme = "http"
	ar.Client.Do(req)
}
