package storage

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Payment struct {
	ID          string
	Amount      int64
	Processor   string
	RequestedAt time.Time
}

type Writer struct {
	Pool   *pgxpool.Pool
	Buffer chan Payment
}

func NewWriter(pool *pgxpool.Pool) *Writer {
	return &Writer{
		Pool:   pool,
		Buffer: make(chan Payment, 10000),
	}
}

func (w *Writer) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		var batch []Payment

		for {
			select {
			case <-ctx.Done():
				return
			case p := <-w.Buffer:
				batch = append(batch, p)
				if len(batch) >= 1000 {
					w.flush(ctx, batch)
					batch = nil
				}
			case <-ticker.C:
				if len(batch) > 0 {
					w.flush(ctx, batch)
					batch = nil
				}
			}
		}
	}()
}

func (w *Writer) flush(ctx context.Context, payments []Payment) {
	batch := &pgx.Batch{}
	for _, p := range payments {
		batch.Queue(
			"INSERT INTO payments (id, amount, processor, requested_at) VALUES ($1, $2, $3, $4)",
			p.ID, p.Amount, p.Processor, p.RequestedAt,
		)
	}
	br := w.Pool.SendBatch(ctx, batch)
	if err := br.Close(); err != nil {
		log.Printf("failed to write batch: %v", err)
	}
}
