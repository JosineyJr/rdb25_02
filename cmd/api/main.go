package main

import (
	"context"
	"math"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/JosineyJr/rdb25_02/internal/config"
	"github.com/JosineyJr/rdb25_02/internal/health"
	"github.com/JosineyJr/rdb25_02/internal/routing"
	"github.com/JosineyJr/rdb25_02/internal/storage"
	"github.com/JosineyJr/rdb25_02/pkg/payments"
	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog"
	"github.com/valyala/fasthttp"
)

func init() {
	config.LoadEnv()
}

func main() {
	runtime.GOMAXPROCS(1)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := zerolog.New(
		zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339},
	).Level(zerolog.TraceLevel).With().Timestamp().Caller().Logger()

	summaryAggregator, err := storage.NewRedisDB(
		os.Getenv("REDIS_URL"),
		os.Getenv("REDIS_SOCKET"),
		"payments-summary",
	)
	if err != nil {
		logger.Fatal().Err(err).Msg("Unable to connect to Redis")
	}

	ar := routing.NewAdaptiveRouter(
		6,
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
			now := time.Now()

			defer func(n time.Time) {
				logger.Info().Msgf("get summary took- %s", time.Since(n))
			}(now)

			if reqCtx.IsGet() {
				handlePaymentsSummary(reqCtx, &logger, summaryAggregator)
			} else {
				reqCtx.Error("Method Not Allowed", fasthttp.StatusMethodNotAllowed)
			}
		case "/purge-payments":
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
		// Concurrency: 2048,
		// ReduceMemoryUsage: true,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  10 * time.Second,
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

var json = jsoniter.ConfigCompatibleWithStandardLibrary

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

	ticker := time.NewTicker(45 * time.Millisecond)

	select {
	case router.PayloadChan <- &payload:
		ctx.SetStatusCode(fasthttp.StatusAccepted)
	case <-ticker.C:
		logger.Warn().Msg("worker pool saturated. Rejecting request with 503.")
		ctx.Error("Service Unavailable", fasthttp.StatusServiceUnavailable)
	}

	ctx.SetStatusCode(fasthttp.StatusAccepted)
}

func handlePaymentsSummary(
	ctx *fasthttp.RequestCtx,
	logger *zerolog.Logger,
	aggregator *storage.RedisDB,
) {
	var from, to *time.Time

	fromStr := string(ctx.QueryArgs().Peek("from"))
	if fromStr != "" {
		if t, err := time.Parse(time.RFC3339Nano, fromStr); err == nil {
			from = &t
		}
	}

	toStr := string(ctx.QueryArgs().Peek("to"))
	if toStr != "" {
		if t, err := time.Parse(time.RFC3339Nano, toStr); err == nil {
			to = &t
		}
	}

	defaultData, fallbackData, err := aggregator.GetSummary(ctx, from, to)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to get summary from Redis")
		ctx.Error("Internal Server Error", fasthttp.StatusInternalServerError)
		return
	}

	defaultData.Total = math.Round(defaultData.Total*10) / 10
	fallbackData.Total = math.Round(fallbackData.Total*10) / 10

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
	aggregator *storage.RedisDB,
) {
	err := aggregator.PurgeSummary(ctx)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to purge payments table")
		ctx.Error("Internal Server Error", fasthttp.StatusInternalServerError)
		return
	}

	ctx.SetStatusCode(fasthttp.StatusNoContent)
}
