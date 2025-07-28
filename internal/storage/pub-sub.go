package storage

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type RedisPubSub struct {
	client *redis.Client
}

func NewRedisPubSub(redisURL string) (*RedisPubSub, error) {
	if redisURL == "" {
		return nil, fmt.Errorf("REDIS_URL must be set")
	}
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	client := redis.NewClient(opts)

	return &RedisPubSub{
		client: client,
	}, nil
}

func (ps *RedisPubSub) Publish(ctx context.Context, channel string, message []byte) {
	ps.client.Publish(ctx, channel, message)
}

func (ps *RedisPubSub) Subscribe(ctx context.Context, channel string) <-chan *redis.Message {
	pubsub := ps.client.Subscribe(ctx, channel)
	return pubsub.Channel()
}
