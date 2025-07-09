package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/JosineyJr/rdb25_02/internal/batching"
	"github.com/JosineyJr/rdb25_02/internal/config"
	"github.com/JosineyJr/rdb25_02/internal/health"
	"github.com/JosineyJr/rdb25_02/internal/routing"
	"github.com/JosineyJr/rdb25_02/internal/storage"
	"github.com/JosineyJr/rdb25_02/pkg/payments"
	"github.com/rs/zerolog"
	"github.com/valyala/fasthttp"
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

	kfDefault := routing.NewKalmanFilter(
		routing.State{Latency: 0.01, ErrorRate: 0.0},
		[2][2]float64{{1, 0}, {0, 1}},
		[2][2]float64{{0.01, 0}, {0, 0.01}},
		[2][2]float64{{0.1, 0}, {0, 0.1}},
	)
	pidDefault := routing.NewPIDController(
		-1.0,
		-0.5,
		0.0,
		routing.State{Latency: 0.9, ErrorRate: 0.2},
	)
	kfFallback := routing.NewKalmanFilter(
		routing.State{Latency: 0.05, ErrorRate: 0.0},
		[2][2]float64{{1, 0}, {0, 1}},
		[2][2]float64{{0.01, 0}, {0, 0.01}},
		[2][2]float64{{0.1, 0}, {0, 0.1}},
	)
	pidFallback := routing.NewPIDController(
		1.0,
		0.1,
		0.0,
		routing.State{Latency: 0.2, ErrorRate: 0.0},
	)
	ar := routing.NewAdaptiveRouter(
		kfDefault, pidDefault,
		kfFallback, pidFallback,
		10,
		summaryAggregator,
	)
	ar.Start(ctx)

	paymentBatcher := batching.NewBatcher(ar)
	paymentBatcher.Start(ctx)

	healthClient := &http.Client{Timeout: 1 * time.Second}

	getHealthDefaultFunc := func() routing.State {
		return health.GetHealthStatus(
			healthClient,
			config.HEALTH_PROCESSOR_URL_DEFAULT,
		)
	}
	kfDefault.FeedMeasurement(getHealthDefaultFunc)

	getHealthFallbackFunc := func() routing.State {
		return health.GetHealthStatus(
			healthClient,
			config.HEALTH_PROCESSOR_URL_FALLBACK,
		)
	}
	kfFallback.FeedMeasurement(getHealthFallbackFunc)

	requestHandler := func(reqCtx *fasthttp.RequestCtx) {
		switch string(reqCtx.Path()) {
		case "/payments":
			if reqCtx.IsPost() {
				handleCreatePayment(reqCtx, &logger, paymentBatcher)
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

	if err := server.Shutdown(); err != nil {
		logger.Error().Err(err).Msg("Server shutdown failed")
	}

	logger.Info().Msg("Server stopped gracefully")
}

func handleCreatePayment(
	ctx *fasthttp.RequestCtx,
	logger *zerolog.Logger,
	batcher *batching.Batcher,
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

	batcher.Add(&payload)

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
