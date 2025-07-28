package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/JosineyJr/rdb25_02/internal/config"
	"github.com/JosineyJr/rdb25_02/internal/health"
	"github.com/JosineyJr/rdb25_02/internal/routing"
	"github.com/JosineyJr/rdb25_02/internal/storage"
	"github.com/JosineyJr/rdb25_02/pkg/payments"
	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog"
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

	pubsub, err := storage.NewRedisPubSub(os.Getenv("REDIS_URL"))
	if err != nil {
		logger.Fatal().Err(err).Msg("Unable to connect to Redis for pubsub")
	}

	ar := routing.NewAdaptiveRouter(
		4,
		summaryAggregator,
	)
	ar.Start(ctx)

	healthUpdater := health.NewHealthUpdater(ar)
	healthUpdater.Start(ctx)

	logger.Info().Msg("Worker starting...")

	ch := pubsub.Subscribe(ctx, "payments_channel")

f:
	for {
		select {
		case <-ctx.Done():
			break f
		case msg := <-ch:
			var payload payments.PaymentsPayload
			if err := json.UnmarshalFromString(msg.Payload, &payload); err != nil {
				logger.Error().Err(err).Msg("Failed to unmarshal payment payload")
				continue
			}

			ar.PayloadChan <- payload
		}
	}

	logger.Info().Msg("Shutdown signal received. Gracefully stopping worker...")
	logger.Info().Msg("Worker stopped gracefully")
}
