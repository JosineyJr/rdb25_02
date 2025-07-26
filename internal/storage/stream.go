package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

type RedisStream struct {
	client       *redis.Client
	payloadChan  chan []byte
	batchIndex   uint16
	batchTicker  time.Ticker
	batch        [][]byte
	maxBatchSize uint16
	maxBatchWait time.Duration
}

func NewRedisStream(
	redisURL, redisSocket string,
	maxBatchSize uint16,
	maxBatchWait time.Duration,
) (*RedisStream, error) {
	if redisURL == "" {
		return nil, fmt.Errorf("REDIS_URL must be set")
	}
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	client := redis.NewClient(opts)

	r := &RedisStream{
		client:       client,
		maxBatchSize: maxBatchSize,
		maxBatchWait: maxBatchWait,
		batch:        make([][]byte, maxBatchSize),
		batchTicker:  *time.NewTicker(maxBatchWait),
		payloadChan:  make(chan []byte, maxBatchSize*2),
	}

	return r, nil
}

func (a *RedisStream) Add(b []byte) {
	a.payloadChan <- b
}

func (a *RedisStream) StartBatchWorker(ctx context.Context, logger *zerolog.Logger) {
	go func() {
		defer a.batchTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				if a.batchIndex > 0 {
					if err := a.sendBatch(ctx); err != nil {
						logger.Error().
							Err(err).
							Int("batch_size", len(a.batch)).
							Msg("Failed to execute Redis pipeline")
					}
				}
				return
			case payload := <-a.payloadChan:
				a.batch[a.batchIndex] = payload
				a.batchIndex++
				if a.batchIndex == a.maxBatchSize {
					if err := a.sendBatch(ctx); err != nil {
						logger.Error().
							Err(err).
							Int("batch_size", len(a.batch)).
							Msg("Failed to execute Redis pipeline")
						continue
					}
					a.batchIndex = 0
				}
			case <-a.batchTicker.C:
				if a.batchIndex > 0 {
					if err := a.sendBatch(ctx); err != nil {
						logger.Error().
							Err(err).
							Int("batch_size", len(a.batch)).
							Msg("Failed to execute Redis pipeline")
						continue
					}
					a.batchIndex = 0
				}
			}
		}
	}()
}

func (a *RedisStream) sendBatch(ctx context.Context) error {
	pipe := a.client.Pipeline()
	for _, payload := range a.batch {
		if len(payload) == 0 {
			break
		}

		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: "payments_stream",
			Approx: true,
			Values: map[string]interface{}{"payload": payload},
		})
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}

	return nil
}
