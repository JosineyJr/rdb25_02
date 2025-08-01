package main

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/JosineyJr/rdb25_02/internal/config"
	"github.com/JosineyJr/rdb25_02/internal/storage"
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
		logger.Fatal().Err(err).Msg("Unable to create in-memory aggregator")
	}

	setupSocketPath(os.Getenv("PAYMENTS_SOCKET_PATH"), &logger)
	setupSocketPath(os.Getenv("SUMMARY_SOCKET_PATH"), &logger)
	setupSocketPath(os.Getenv("PURGE_SOCKET_PATH"), &logger)

	paymentsListener, err := net.Listen("unix", os.Getenv("PAYMENTS_SOCKET_PATH"))
	if err != nil {
		logger.Fatal().Err(err).Msgf("Failed to listen on payments socket")
	}
	defer paymentsListener.Close()

	go handleConnections(ctx, paymentsListener, &logger, func(conn net.Conn) {
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
		logger.Fatal().Err(err).Msgf("Failed to listen on summary socket")
	}
	defer summaryListener.Close()

	go handleConnections(ctx, summaryListener, &logger, func(conn net.Conn) {
		reader := bufio.NewReader(conn)
		for {
			buf, err := reader.ReadBytes('\n')
			if err != nil {
				if err != io.EOF {
					logger.Error().Err(err).Msg("Error reading from summary socket")
				}
				return
			}
			buf = bytes.TrimSpace(buf)

			q, _ := bytes.CutPrefix(buf, []byte("from="))
			fromStr, toStr, _ := bytes.Cut(q, []byte("&to="))
			from, _ := time.Parse(time.RFC3339Nano, string(fromStr))
			to, _ := time.Parse(time.RFC3339Nano, string(toStr))

			summary, err := summaryAggregator.GetSummary(ctx, &from, &to)
			if err != nil {
				logger.Error().Err(err).Msg("Failed to get summary")
				continue
			}

			json.NewEncoder(conn).Encode(summary)
		}
	})

	purgeListener, err := net.Listen("unix", os.Getenv("PURGE_SOCKET_PATH"))
	if err != nil {
		logger.Fatal().Err(err).Msgf("Failed to listen on purge socket")
	}
	defer purgeListener.Close()

	go handleConnections(ctx, purgeListener, &logger, func(conn net.Conn) {
		buf := make([]byte, 1)
		conn.Read(buf)
		summaryAggregator.PurgeSummary(ctx)
	})

	logger.Info().Msg("Worker started successfully, listening on sockets.")
	<-ctx.Done()
	logger.Info().Msg("Worker stopped gracefully.")
}

func setupSocketPath(path string, logger *zerolog.Logger) {
	if path == "" {
		logger.Fatal().Msgf("Socket path environment variable not set for %s", path)
	}
	socketDir := filepath.Dir(path)
	if _, err := os.Stat(socketDir); os.IsNotExist(err) {
		if err := os.MkdirAll(socketDir, 0755); err != nil {
			log.Fatalf("Failed to create socket dir %s: %v", socketDir, err)
		}
	}
	if _, err := os.Stat(path); err == nil {
		if err := os.Remove(path); err != nil {
			logger.Fatal().Err(err).Msgf("Failed to remove old socket file at %s", path)
		}
	}
}

func handleConnections(
	ctx context.Context,
	listener net.Listener,
	logger *zerolog.Logger,
	handler func(net.Conn),
) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			conn, err := listener.Accept()
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok &&
					opErr.Err.Error() == "use of closed network connection" {
					return
				}
				logger.Error().Err(err).Msg("Failed to accept new connection")
				continue
			}
			go func() {
				defer conn.Close()
				handler(conn)
			}()
		}
	}
}
