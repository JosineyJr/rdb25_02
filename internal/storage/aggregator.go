package storage

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/JosineyJr/rdb25_02/pkg/payments"
	"github.com/redis/go-redis/v9"
)

type RedisAggregator struct {
	client *redis.Client
	key    string
}

const (
	defaultTimeSeriesKey  = "ts:default"
	fallbackTimeSeriesKey = "ts:fallback"
)

func NewRedisAggregator(redisURL, redisSocket, key string) (*RedisAggregator, error) {
	var client *redis.Client

	if redisSocket != "" {
		client = redis.NewClient(&redis.Options{
			Network: "unix",
			Addr:    redisSocket,
		})
	} else {
		if redisURL == "" {
			return nil, fmt.Errorf("REDIS_SOCKET or REDIS_URL must be set")
		}

		opts, err := redis.ParseURL(redisURL)
		if err != nil {
			return nil, err
		}
		client = redis.NewClient(opts)
	}

	return &RedisAggregator{client: client, key: key}, nil
}

func (a *RedisAggregator) Update(
	ctx context.Context,
	processor string,
	payload *payments.PaymentsPayload,
) {
	pipe := a.client.Pipeline()

	amountInCents := int64(math.Round(payload.Amount * 100))

	pipe.HIncrBy(ctx, a.key, processor+"_total_cents", amountInCents)

	var tsKey string
	if processor == payments.DefaultProcessor {
		tsKey = defaultTimeSeriesKey
	} else {
		tsKey = fallbackTimeSeriesKey
	}

	score := float64(payload.RequestedAt.UnixMilli())
	var sb strings.Builder
	sb.WriteString(payload.CorrelationID)
	sb.WriteString(":")
	sb.WriteString(strconv.Itoa(int(amountInCents)))
	pipe.ZAdd(ctx, tsKey, redis.Z{Score: score, Member: sb.String()})

	_, err := pipe.Exec(ctx)
	if err != nil {
		fmt.Printf("failed to update redis aggregator: %v\n", err)
		return
	}
}

func (a *RedisAggregator) GetSummary(
	ctx context.Context, from, to *time.Time,
) (payments.SummaryData, payments.SummaryData, error) {
	if from == nil || to == nil {
		data, err := a.client.HGetAll(ctx, a.key).Result()
		if err != nil {
			return payments.SummaryData{}, payments.SummaryData{}, err
		}

		defaultCents := parseInt(data["default_total_cents"])
		fallbackCents := parseInt(data["fallback_total_cents"])

		defaultSummary := payments.SummaryData{
			Count: parseInt(data["default_count"]),
			Total: float64(defaultCents) / 100.0,
		}
		fallbackSummary := payments.SummaryData{
			Count: parseInt(data["fallback_count"]),
			Total: float64(fallbackCents) / 100.0,
		}
		return defaultSummary, fallbackSummary, nil
	}

	min := strconv.FormatInt(from.UnixMilli(), 10)
	max := strconv.FormatInt(to.UnixMilli(), 10)

	pipe := a.client.Pipeline()
	defaultResult := pipe.ZRangeByScore(
		ctx,
		defaultTimeSeriesKey,
		&redis.ZRangeBy{Min: min, Max: max},
	)
	fallbackResult := pipe.ZRangeByScore(
		ctx,
		fallbackTimeSeriesKey,
		&redis.ZRangeBy{Min: min, Max: max},
	)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return payments.SummaryData{}, payments.SummaryData{}, err
	}

	defaultCents := parseZRangeResultCents(defaultResult.Val())
	fallbackCents := parseZRangeResultCents(fallbackResult.Val())

	defaultSummary := payments.SummaryData{
		Count: int64(len(defaultResult.Val())),
		Total: float64(defaultCents) / 100.0,
	}
	fallbackSummary := payments.SummaryData{
		Count: int64(len(fallbackResult.Val())),
		Total: float64(fallbackCents) / 100.0,
	}

	return defaultSummary, fallbackSummary, nil
}

func parseZRangeResultCents(results []string) int64 {
	var totalCents int64
	for _, member := range results {
		parts := strings.Split(member, ":")
		if len(parts) == 2 {
			cents, _ := strconv.ParseInt(parts[1], 10, 64)
			totalCents += cents
		}
	}
	return totalCents
}

func (a *RedisAggregator) PurgeSummary(ctx context.Context) error {
	return a.client.Del(ctx, a.key, defaultTimeSeriesKey, fallbackTimeSeriesKey).Err()
}

func parseInt(s string) int64 {
	i, _ := strconv.ParseInt(s, 10, 64)
	return i
}
