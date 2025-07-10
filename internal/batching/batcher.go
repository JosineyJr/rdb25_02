package batching

import (
	"context"
	"time"

	"github.com/JosineyJr/rdb25_02/internal/routing"
	"github.com/JosineyJr/rdb25_02/pkg/payments"
)

const (
	defaultBatchSize    = 512
	defaultBatchTimeout = 98 * time.Millisecond
)

type Batcher struct {
	buffer chan *payments.PaymentsPayload
	router *routing.AdaptiveRouter
}

func NewBatcher(router *routing.AdaptiveRouter) *Batcher {
	return &Batcher{
		buffer: make(chan *payments.PaymentsPayload, defaultBatchSize*2),
		router: router,
	}
}

func (b *Batcher) Add(p *payments.PaymentsPayload) {
	b.buffer <- p
}

func (b *Batcher) Start(ctx context.Context) {
	go func() {
		batch := make([]*payments.PaymentsPayload, 0, defaultBatchSize)
		ticker := time.NewTicker(defaultBatchTimeout)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				if len(batch) > 0 {
					b.router.EnqueueBatch(batch, 0)
				}
				return
			case p := <-b.buffer:
				batch = append(batch, p)
				if len(batch) >= defaultBatchSize {
					b.router.EnqueueBatch(batch, 0)
					batch = make([]*payments.PaymentsPayload, 0, defaultBatchSize)
					ticker.Reset(defaultBatchTimeout)
				}
			case <-ticker.C:
				if len(batch) > 0 {
					b.router.EnqueueBatch(batch, 0)
					batch = make([]*payments.PaymentsPayload, 0, defaultBatchSize)
				}
			}
		}
	}()
}
