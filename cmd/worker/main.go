package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/JosineyJr/rdb25_02/internal/config"
	"github.com/JosineyJr/rdb25_02/internal/health"
	"github.com/JosineyJr/rdb25_02/internal/routing"
	"github.com/JosineyJr/rdb25_02/internal/storage"
	"github.com/JosineyJr/rdb25_02/pkg/payments"
	jsoniter "github.com/json-iterator/go"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

const (
	streamName    = "payments_stream"
	consumerGroup = "payments_processors"
	consumerName  = "worker"
)

var json = jsoniter.ConfigFastest

func init() {
	config.LoadEnv()
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := zerolog.New(os.Stdout).Level(zerolog.InfoLevel).With().Timestamp().Logger()

	summaryAggregator, err := storage.NewRedisDB(
		os.Getenv("REDIS_URL"),
		os.Getenv("REDIS_SOCKET"),
		"payments-summary",
	)
	if err != nil {
		logger.Fatal().Err(err).Msg("Unable to connect to Redis")
	}

	redisOpts, err := redis.ParseURL(os.Getenv("REDIS_URL"))
	if err != nil {
		logger.Fatal().Err(err).Msg("Unable to parse Redis URL")
	}
	redisClient := redis.NewClient(redisOpts)
	_ = redisClient.XGroupCreateMkStream(ctx, streamName, consumerGroup, "0").Err()

	ar := routing.NewAdaptiveRouter(
		4,
		summaryAggregator,
	)
	ar.Start(ctx)

	healthUpdater := health.NewHealthUpdater(ar)
	healthUpdater.Start(ctx)

	logger.Info().Msg("Worker starting...")

	readTicker := time.NewTicker(time.Second)
f:
	for {
		select {
		case <-ctx.Done():
			break f
		case <-readTicker.C:
			streams, err := redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    consumerGroup,
				Consumer: consumerName,
				Streams:  []string{streamName, ">"},
				Count:    3000,
			}).Result()

			if err != nil {
				if err != redis.Nil {
					logger.Error().Err(err).Msg("Failed to get payment from stream")
				}
				continue
			}

		s:
			for _, stream := range streams {
				for _, message := range stream.Messages {
					payloadData, ok := message.Values["payload"].(string)
					if !ok || payloadData == "" {
						break s
					}

					var payload payments.PaymentsPayload
					if err := json.UnmarshalFromString(payloadData, &payload); err != nil {
						logger.Error().Err(err).Msg("Failed to unmarshal payment payload")
						continue
					}

					ar.PayloadChan <- payload

					redisClient.XAck(ctx, streamName, consumerGroup, message.ID)
				}
			}
		}
	}

	logger.Info().Msg("Shutdown signal received. Gracefully stopping worker...")
	logger.Info().Msg("Worker stopped gracefully")
}
