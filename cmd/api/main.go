package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/JosineyJr/rdb25_02/internal/config"
	"github.com/JosineyJr/rdb25_02/internal/health"
	"github.com/JosineyJr/rdb25_02/internal/routing"
	"github.com/JosineyJr/rdb25_02/internal/storage"
	"github.com/JosineyJr/rdb25_02/pkg/payments"
	"github.com/rs/zerolog"
	"github.com/valyala/fasthttp"
	_ "go.uber.org/automaxprocs"
)

func init() {
	config.LoadEnv()
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := zerolog.New(
		zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339},
	).Level(zerolog.TraceLevel).With().Timestamp().Caller().Logger()

	redisURL := os.Getenv("REDIS_URL")
	summaryAggregator, err := storage.NewRedisAggregator(redisURL, "payments-summary")
	if err != nil {
		logger.Fatal().Err(err).Msg("Unable to connect to Redis")
	}

	ar := routing.NewAdaptiveRouter(
		16,
		summaryAggregator,
	)
	ar.Start(ctx)

	healthUpdater := health.NewHealthUpdater(ar)
	healthUpdater.Start(ctx)

	requestHandler := func(reqCtx *fasthttp.RequestCtx) {
		switch string(reqCtx.Path()) {
		case "/payments":
			if reqCtx.IsPost() {
				handleCreatePayment(reqCtx, &logger, ar)
			} else {
				reqCtx.Error("Method Not Allowed", fasthttp.StatusMethodNotAllowed)
			}
		case "/payments-summary":
			if reqCtx.IsGet() {
				handlePaymentsSummary(reqCtx, &logger, summaryAggregator)
			} else {
				reqCtx.Error("Method Not Allowed", fasthttp.StatusMethodNotAllowed)
			}
		case "/admin/purge-payments":
			if reqCtx.IsPost() {
				handlePurgePayments(reqCtx, &logger, summaryAggregator)
			} else {
				reqCtx.Error("Method Not Allowed", fasthttp.StatusMethodNotAllowed)
			}
		default:
			reqCtx.Error("Not Found", fasthttp.StatusNotFound)
		}
	}

	server := &fasthttp.Server{
		Handler: requestHandler,
		Name:    "PaymentService",
	}

	go func() {
		logger.Info().Msgf("Server starting on port %s", config.PORT)
		if err := server.ListenAndServe(":" + config.PORT); err != nil {
			logger.Fatal().Err(err).Msg("Server failed to start")
		}
	}()

	<-ctx.Done()
	logger.Info().Msg("Shutdown signal received. Gracefully stopping server...")
	ar.Stop()

	if err := server.Shutdown(); err != nil {
		logger.Error().Err(err).Msg("Server shutdown failed")
	}

	logger.Info().Msg("Server stopped gracefully")
}

func handleCreatePayment(
	ctx *fasthttp.RequestCtx,
	logger *zerolog.Logger,
	router *routing.AdaptiveRouter,
) {
	var payload payments.PaymentsPayload

	if err := json.Unmarshal(ctx.PostBody(), &payload); err != nil {
		ctx.Error("invalid request body", fasthttp.StatusBadRequest)
		logger.Error().Err(err).Send()
		return
	}

	if payload.Amount == 0.0 {
		ctx.Error("missing field 'amount'", fasthttp.StatusBadRequest)
		return
	}
	payload.RequestedAt = time.Now().UTC()

	router.EnqueuePayment(&payload)

	ctx.SetStatusCode(fasthttp.StatusAccepted)
}

func handlePaymentsSummary(
	ctx *fasthttp.RequestCtx,
	logger *zerolog.Logger,
	aggregator *storage.RedisAggregator,
) {
	defaultData, fallbackData, err := aggregator.GetSummary(context.Background())
	if err != nil {
		logger.Error().Err(err).Msg("Failed to get summary from Redis")
		ctx.Error("Internal Server Error", fasthttp.StatusInternalServerError)
		return
	}

	respBody, err := json.Marshal(payments.PaymentsSummary{
		Default:  defaultData,
		Fallback: fallbackData,
	})
	if err != nil {
		logger.Error().Err(err).Msg("Failed to marshal summary response")
		ctx.Error("Internal Server Error", fasthttp.StatusInternalServerError)
		return
	}

	ctx.SetContentType("application/json")
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetBody(respBody)
}

func handlePurgePayments(
	ctx *fasthttp.RequestCtx,
	logger *zerolog.Logger,
	aggregator *storage.RedisAggregator,
) {
	err := aggregator.PurgeSummary(ctx)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to purge payments table")
		ctx.Error("Internal Server Error", fasthttp.StatusInternalServerError)
		return
	}

	ctx.SetStatusCode(fasthttp.StatusNoContent)
}
