package storage

import (
	"context"
	"strconv"

	"github.com/JosineyJr/rdb25_02/pkg/payments"
	"github.com/redis/go-redis/v9"
)

type RedisAggregator struct {
	client *redis.Client
	key    string
}

func NewRedisAggregator(redisURL, key string) (*RedisAggregator, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	client := redis.NewClient(opts)
	return &RedisAggregator{client: client, key: key}, nil
}

func (a *RedisAggregator) Update(ctx context.Context, processor string, amount float64) {
	a.client.HIncrBy(ctx, a.key, processor+"_count", 1)
	a.client.HIncrByFloat(ctx, a.key, processor+"_total", amount)
}

func (a *RedisAggregator) GetSummary(
	ctx context.Context,
) (payments.SummaryData, payments.SummaryData, error) {
	data, err := a.client.HGetAll(ctx, a.key).Result()
	if err != nil {
		return payments.SummaryData{}, payments.SummaryData{}, err
	}

	defaultSummary := payments.SummaryData{
		Count: parseInt(data["default_count"]),
		Total: parseFloat(data["default_total"]),
	}
	fallbackSummary := payments.SummaryData{
		Count: parseInt(data["fallback_count"]),
		Total: parseFloat(data["fallback_total"]),
	}

	return defaultSummary, fallbackSummary, nil
}

func (a *RedisAggregator) PurgeSummary(ctx context.Context) error {
	return a.client.Del(ctx, a.key).Err()
}

func parseInt(s string) int64 {
	i, _ := strconv.ParseInt(s, 10, 64)
	return i
}

func parseFloat(s string) float64 {
	i, _ := strconv.ParseFloat(s, 64)
	return i
}
