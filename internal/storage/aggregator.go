package storage

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/JosineyJr/rdb25_02/pkg/payments"
	"github.com/redis/go-redis/v9"
)

type RedisDB struct {
	client *redis.Client
}

const (
	tsDefaultAmountKey   = "ts:amount:default"
	tsDefaultCountKey    = "ts:count:default"
	tsFallbackAmountKey  = "ts:amount:fallback"
	tsFallbackCountKey   = "ts:count:fallback"
	processedPaymentsKey = "payments:processed"
)

func NewRedisDB(redisURL, redisSocket, key string) (*RedisDB, error) {
	if redisURL == "" {
		return nil, fmt.Errorf("REDIS_URL must be set")
	}
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	opts.Protocol = 2
	client := redis.NewClient(opts)
	ctx := context.Background()

	r := &RedisDB{
		client: client,
	}
	if err = r.createKeys(ctx); err != nil {
		return nil, err
	}

	return r, nil
}

func splitLabels(labelStr string) []string {
	return strings.Fields(labelStr)
}

func splitKeyVal(s string) [2]string {
	parts := strings.SplitN(s, "=", 2)
	return [2]string{parts[0], parts[1]}
}

func isAlreadyExistsError(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "ERR TSDB: key already exists") ||
		strings.Contains(err.Error(), "BUSYKEY Target key name already exists"))
}

func (a *RedisDB) PaymentIsProcessed(ctx context.Context, id string) bool {
	val, err := a.client.SIsMember(ctx, processedPaymentsKey, id).Result()
	if err != nil {
		fmt.Println(err)
		return false
	}

	return val
}

func (a *RedisDB) MarkAsProcessed(ctx context.Context, id string) {
	_, err := a.client.SAdd(ctx, processedPaymentsKey, id).Result()
	if err != nil {
		fmt.Println(err)
		return
	}
}

func (a *RedisDB) Update(
	ctx context.Context,
	processor string,
	payload *payments.PaymentsPayload,
) {
	pipe := a.client.Pipeline()
	timestamp := payload.RequestedAt.UnixMilli()

	var amountKey, countKey string
	if processor == payments.DefaultProcessor {
		amountKey = tsDefaultAmountKey
		countKey = tsDefaultCountKey
	} else {
		amountKey = tsFallbackAmountKey
		countKey = tsFallbackCountKey
	}

	pipe.Do(ctx, "TS.ADD", amountKey, timestamp, payload.Amount, "ON_DUPLICATE", "SUM")
	pipe.Do(ctx, "TS.ADD", countKey, timestamp, 1, "ON_DUPLICATE", "SUM")

	_, err := pipe.Exec(ctx)
	if err != nil {
		fmt.Printf("failed to update redis aggregator: %v\n", err)
	}
}

func (a *RedisDB) GetSummary(
	ctx context.Context, from, to *time.Time,
) (payments.SummaryData, payments.SummaryData, error) {
	var defaultSummary, fallbackSummary payments.SummaryData

	bucket := to.UnixMilli() - from.UnixMilli()
	if bucket <= 0 {
		bucket = 1
	}

	result, err := a.client.Do(ctx,
		"TS.MRANGE",
		from.UnixMilli(), to.UnixMilli(),
		"AGGREGATION", "sum", bucket,
		"FILTER", "type=amount",
	).Slice()
	if err != nil {
		return defaultSummary, fallbackSummary, fmt.Errorf("failed TS.MRANGE (amount): %w", err)
	}

	for _, serie := range result {
		serieData, ok := serie.([]interface{})
		if !ok || len(serieData) != 3 {
			continue
		}

		key, _ := serieData[0].(string)
		points, ok := serieData[2].([]interface{})
		if !ok {
			continue
		}

		var sum float64
		for _, point := range points {
			entry, ok := point.([]interface{})
			if !ok || len(entry) != 2 {
				continue
			}
			valStr, ok := entry[1].(string)
			if !ok {
				continue
			}
			val, err := parseFloat(valStr)
			if err != nil {
				continue
			}
			sum += val
		}

		switch key {
		case tsDefaultAmountKey:
			defaultSummary.Total = sum
		case tsFallbackAmountKey:
			fallbackSummary.Total = sum
		}
	}

	result, err = a.client.Do(ctx,
		"TS.MRANGE",
		from.UnixMilli(), to.UnixMilli(),
		"AGGREGATION", "sum", bucket,
		"FILTER", "type=count",
	).Slice()
	if err != nil {
		return defaultSummary, fallbackSummary, fmt.Errorf("failed TS.MRANGE (count): %w", err)
	}

	for _, serie := range result {
		serieData, ok := serie.([]interface{})
		if !ok || len(serieData) != 3 {
			continue
		}

		key, _ := serieData[0].(string)
		points, ok := serieData[2].([]interface{})
		if !ok {
			continue
		}

		var sum int64
		for _, point := range points {
			entry, ok := point.([]interface{})
			if !ok || len(entry) != 2 {
				continue
			}
			valStr, ok := entry[1].(string)
			if !ok {
				continue
			}
			val, err := parseInt(valStr)
			if err != nil {
				continue
			}
			sum += val
		}

		switch key {
		case tsDefaultCountKey:
			defaultSummary.Count = sum
		case tsFallbackCountKey:
			fallbackSummary.Count = sum
		}
	}

	return defaultSummary, fallbackSummary, nil
}

func (a *RedisDB) createKeys(ctx context.Context) error {
	labels := map[string]string{
		tsDefaultAmountKey:  "processor=default type=amount",
		tsDefaultCountKey:   "processor=default type=count",
		tsFallbackAmountKey: "processor=fallback type=amount",
		tsFallbackCountKey:  "processor=fallback type=count",
	}

	for key, labelStr := range labels {
		args := []interface{}{"TS.CREATE", key, "DUPLICATE_POLICY", "SUM", "LABELS"}
		for _, label := range splitLabels(labelStr) {
			parts := splitKeyVal(label)
			args = append(args, parts[0], parts[1])
		}
		_, err := a.client.Do(ctx, args...).Result()
		if err != nil && !isAlreadyExistsError(err) {
			return fmt.Errorf("error creating timeseries %s: %w", key, err)
		}
	}

	return nil
}

func (a *RedisDB) PurgeSummary(ctx context.Context) error {
	if err := a.client.Del(ctx,
		tsDefaultAmountKey, tsDefaultCountKey,
		tsFallbackAmountKey, tsFallbackCountKey, processedPaymentsKey,
	).Err(); err != nil {
		return err
	}

	return a.createKeys(ctx)
}

func parseFloat(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}

func parseInt(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}
