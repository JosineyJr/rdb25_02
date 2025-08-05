package main

import (
	"bufio"
	"bytes"
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/JosineyJr/rdb25_02/internal/config"
	"github.com/JosineyJr/rdb25_02/internal/health"
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

	summaryAggregator, err := storage.NewInMemoryAggregator()
	if err != nil {
		logger.Fatal().Err(err).Msg("unable to create in-memory aggregator")
	}

	setupSocketPath(os.Getenv("PAYMENTS_SOCKET_PATH"), &logger)
	setupSocketPath(os.Getenv("SUMMARY_SOCKET_PATH"), &logger)
	setupSocketPath(os.Getenv("PURGE_SOCKET_PATH"), &logger)
	setupSocketPath(os.Getenv("HEALTH_SOCKET_PATH"), &logger)

	healthBroadcaster := health.NewHealthBroadcaster()
	healthUpdates := make(chan health.HealthReport, 1)
	healthUpdater := health.NewHealthUpdater(healthUpdates)
	healthUpdater.Start(ctx)

	healthListener, err := net.Listen("unix", os.Getenv("HEALTH_SOCKET_PATH"))
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to listen on health socket")
	}
	defer healthListener.Close()

	go func() {
		for {
			conn, err := healthListener.Accept()
			if err != nil {
				logger.Error().Err(err).Msg("failed to accept health subscriber")
				return
			}
			healthBroadcaster.Add(conn)
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case report := <-healthUpdates:
				jsonData, err := json.Marshal(report)
				if err != nil {
					logger.Error().Err(err).Msg("failed to marshal health report")
					continue
				}
				healthBroadcaster.Broadcast(append(jsonData, '\n'))
			}
		}
	}()

	paymentsListener, err := net.Listen("unix", os.Getenv("PAYMENTS_SOCKET_PATH"))
	if err != nil {
		logger.Fatal().Err(err).Msgf("failed to listen on payments socket")
	}
	defer paymentsListener.Close()
	paymentsJobs := make(chan net.Conn, 512)
	go acceptConnections(ctx, paymentsListener, paymentsJobs, &logger)
	startWorkerPool(ctx, config.NUM_PAYMENT_WORKERS, paymentsJobs, func(conn net.Conn) {
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			buf := scanner.Bytes()
			parts := bytes.SplitN(buf, []byte("|"), 2)
			if len(parts) != 2 {
				continue
			}

			processor := string(parts[0])
			ts, err := strconv.ParseInt(string(parts[1]), 10, 64)
			if err != nil {
				continue
			}
			summaryAggregator.Update(ctx, processor, ts)
		}
	})

	summaryListener, err := net.Listen("unix", os.Getenv("SUMMARY_SOCKET_PATH"))
	if err != nil {
		logger.Fatal().Err(err).Msgf("failed to listen on summary socket")
	}
	defer summaryListener.Close()
	summaryJobs := make(chan net.Conn, 4)
	go acceptConnections(ctx, summaryListener, summaryJobs, &logger)
	startWorkerPool(ctx, config.NUM_WORKERS, summaryJobs, func(conn net.Conn) {
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			buf := scanner.Bytes()
			buf = bytes.TrimSpace(buf)

			q, _ := bytes.CutPrefix(buf, []byte("from="))
			fromStr, toStr, _ := bytes.Cut(q, []byte("&to="))
			from, _ := time.Parse(time.RFC3339Nano, string(fromStr))
			to, _ := time.Parse(time.RFC3339Nano, string(toStr))

			defaultData, fallbackData, _ := summaryAggregator.GetSummary(ctx, &from, &to)
			summary := payments.PaymentsSummary{Default: defaultData, Fallback: fallbackData}

			json.NewEncoder(conn).Encode(summary)
		}
	})

	purgeListener, err := net.Listen("unix", os.Getenv("PURGE_SOCKET_PATH"))
	if err != nil {
		logger.Fatal().Err(err).Msgf("failed to listen on purge socket")
	}
	defer purgeListener.Close()
	purgeJobs := make(chan net.Conn, 4)
	go acceptConnections(ctx, purgeListener, purgeJobs, &logger)
	startWorkerPool(ctx, config.NUM_WORKERS, purgeJobs, func(conn net.Conn) {
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			summaryAggregator.PurgeSummary(ctx)

			conn.Write([]byte("OK\n"))
		}
	})

	logger.Info().Msg("worker started successfully, listening on sockets.")
	<-ctx.Done()
	logger.Info().Msg("worker stopped gracefully.")
}

func startWorkerPool(
	ctx context.Context,
	numWorkers int,
	jobs <-chan net.Conn,
	handler func(net.Conn),
) {
	for range numWorkers {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case conn, ok := <-jobs:
					if !ok {
						return
					}

					func() {
						defer conn.Close()
						handler(conn)
					}()
				}
			}
		}()
	}
}

func acceptConnections(
	ctx context.Context,
	listener net.Listener,
	jobs chan<- net.Conn,
	logger *zerolog.Logger,
) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			conn, err := listener.Accept()
			if err != nil {
				logger.Error().Err(err).Msg("failed to accept new connection")
				continue
			}
			jobs <- conn
		}
	}
}

func setupSocketPath(path string, logger *zerolog.Logger) {
	if path == "" {
		logger.Fatal().Msgf("socket path environment variable not set for %s", path)
	}
	socketDir := filepath.Dir(path)
	if _, err := os.Stat(socketDir); os.IsNotExist(err) {
		if err := os.MkdirAll(socketDir, 0755); err != nil {
			log.Fatalf("failed to create socket dir %s: %v", socketDir, err)
		}
	}
	if _, err := os.Stat(path); err == nil {
		if err := os.Remove(path); err != nil {
			logger.Fatal().Err(err).Msgf("failed to remove old socket file at %s", path)
		}
	}
}
