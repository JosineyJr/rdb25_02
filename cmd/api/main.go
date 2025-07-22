package main

import (
	"context"
	"fmt"
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
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := zerolog.New(
		zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339Nano, NoColor: true},
	).Level(zerolog.TraceLevel).With().Timestamp().Logger()

	summaryAggregator, err := storage.NewRedisDB(
		os.Getenv("REDIS_URL"),
		os.Getenv("REDIS_SOCKET"),
		"payments-summary",
	)
	if err != nil {
		logger.Fatal().Err(err).Msg("Unable to connect to Redis")
	}

	ar := routing.NewAdaptiveRouter(
		runtime.NumCPU()*5,
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
				return
			} else {
				reqCtx.Error("Method Not Allowed", fasthttp.StatusMethodNotAllowed)
			}
		case "/payments-summary":
			if reqCtx.IsGet() {
				handlePaymentsSummary(reqCtx, &logger, summaryAggregator)
				return
			} else {
				reqCtx.Error("Method Not Allowed", fasthttp.StatusMethodNotAllowed)
			}
		case "/purge-payments":
			if reqCtx.IsPost() {
				handlePurgePayments(reqCtx, &logger, summaryAggregator)
				return
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
	ctx.SetStatusCode(fasthttp.StatusAccepted)

	select {
	case router.PayloadChan <- &payload:
		ctx.SetStatusCode(fasthttp.StatusAccepted)
	default:
		logger.Warn().Msg("worker pool saturated. Rejecting request with 503.")
		ctx.Error("Service Unavailable", fasthttp.StatusServiceUnavailable)
	}
}

func handlePaymentsSummary(
	ctx *fasthttp.RequestCtx,
	logger *zerolog.Logger,
	aggregator *storage.RedisDB,
) {
	defer func(n time.Time) {
		logger.Info().
			Int("status_code", ctx.Response.StatusCode()).
			Str("duration", time.Since(n).String()).
			Msg("get summary")
	}(time.Now())

	from, err := parseDate(string(ctx.QueryArgs().Peek("from")), logger)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to parse 'from' date")
		ctx.Error("Internal Server Error", fasthttp.StatusInternalServerError)
		return
	}

	to, err := parseDate(string(ctx.QueryArgs().Peek("to")), logger)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to parse 'to' date")
		ctx.Error("Internal Server Error", fasthttp.StatusInternalServerError)
		return
	}

	defaultData, fallbackData, err := aggregator.GetSummary(ctx, &from, &to)
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

func parseDate(date string, logger *zerolog.Logger) (time.Time, error) {
	if date == "" {
		logger.Warn().Msg("caught empty date")
		date = time.Now().Format(time.RFC3339Nano)
	}

	if t, err := time.Parse(time.RFC3339Nano, date); err == nil {
		return t, nil
	}

	layouts := []string{
		time.RFC3339,
		"2006-01-02",
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02T15:04:05Z07:00",
	}

	for _, layout := range layouts {
		if t, err := time.Parse(layout, date); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("invalid date format: %s", date)
}
