package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/JosineyJr/rdb25_02/internal/routing"
	"github.com/JosineyJr/rdb25_02/pkg/payments"
	"github.com/rs/zerolog"
	"github.com/valyala/fasthttp" // Replaced net/http
)

var (
	PORT, PAYMENT_PROCESSOR_URL_DEFAULT, PAYMENT_PROCESSOR_URL_FALLBACK string
	PAYMENT_PROCESSOR_TAX_DEFAULT, PAYMENT_PROCESSOR_TAX_FALLBACK       float32
)

func init() {
	PORT = os.Getenv("PORT")
	if PORT == "" {
		PORT = "9999"
	}
	PAYMENT_PROCESSOR_URL_DEFAULT = os.Getenv("PAYMENT_PROCESSOR_URL_DEFAULT")
	PAYMENT_PROCESSOR_URL_FALLBACK = os.Getenv("PAYMENT_PROCESSOR_URL_FALLBACK")
	PAYMENT_PROCESSOR_TAX_DEFAULT = 0.05
	PAYMENT_PROCESSOR_TAX_FALLBACK = 0.15
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := zerolog.New(
		zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339},
	).Level(zerolog.TraceLevel).With().Timestamp().Caller().Logger()

	kf := routing.NewKalmanFilter()
	pc := routing.NewPIDController()
	ar := routing.NewAdaptiveRouter(kf, pc, runtime.NumCPU()*2)
	ar.Start(ctx)

	requestHandler := func(ctx *fasthttp.RequestCtx) {
		switch string(ctx.Path()) {
		case "/payments":
			if ctx.IsPost() {
				handleCreatePayment(ctx, &logger)
			} else {
				ctx.Error("Method Not Allowed", fasthttp.StatusMethodNotAllowed)
			}
		case "/payments-summary":
			if ctx.IsGet() {
				ctx.SetStatusCode(fasthttp.StatusOK)
				ctx.SetBodyString("Payments summary endpoint")
			} else {
				ctx.Error("Method Not Allowed", fasthttp.StatusMethodNotAllowed)
			}
		case "/admin/purge-payments":
			if ctx.IsPost() {
				ctx.SetStatusCode(fasthttp.StatusOK)
				ctx.SetBodyString("Purge payments endpoint")
			} else {
				ctx.Error("Method Not Allowed", fasthttp.StatusMethodNotAllowed)
			}
		default:
			ctx.Error("Not Found", fasthttp.StatusNotFound)
		}
	}

	server := &fasthttp.Server{
		Handler: requestHandler,
		Name:    "PaymentService",
	}

	go func() {
		logger.Info().Msgf("Server starting on port %s", PORT)
		if err := server.ListenAndServe(":" + PORT); err != nil {
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

func handleCreatePayment(ctx *fasthttp.RequestCtx, logger *zerolog.Logger) {
	var payload payments.PaymentsPayload

	if err := json.Unmarshal(ctx.PostBody(), &payload); err != nil {
		ctx.Error("invalid request body", fasthttp.StatusBadRequest)
		logger.Error().Err(err).Send()
		return
	}

	if payload.CorrelationID == "" {
		ctx.Error("missing field 'correlationId'", fasthttp.StatusBadRequest)
		return
	}

	if payload.Amount == 0.0 {
		ctx.Error("missing field 'amount'", fasthttp.StatusBadRequest)
		return
	}
	payload.RequestedAt = time.Now().UTC()

	ctx.SetStatusCode(fasthttp.StatusAccepted)
}
