package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
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
	"github.com/panjf2000/gnet/v2"
	"github.com/rs/zerolog"
)

var json = jsoniter.ConfigFastest

type HTTPServer struct {
	gnet.BuiltinEventEngine
	aggregator *storage.RingBufferAggregator
	logger     *zerolog.Logger
}

var (
	http200OK         = []byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
	http204NoContent  = []byte("HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
	http404NotFound   = []byte("HTTP/1.1 404 Not Found\r\n\r\n")
	http405NotAllowed = []byte("HTTP/1.1 405 Method Not Allowed\r\n\r\n")
)

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

	ar := routing.NewAdaptiveRouter(
		2,
		summaryAggregator,
	)
	ar.Start(ctx)

	healthUpdater := health.NewHealthUpdater(ar)
	healthUpdater.Start(ctx)

	go runUDPListener(ctx, ar, &logger)

	httpServer := &HTTPServer{
		aggregator: summaryAggregator,
		logger:     &logger,
	}
	go func() {
		logger.Info().Msg("Worker starting gnet HTTP server on port 9995...")
		err := gnet.Run(
			httpServer,
			"tcp://:9995",
			gnet.WithReusePort(true),
			gnet.WithMulticore(true),
			gnet.WithTCPNoDelay(gnet.TCPNoDelay),
			gnet.WithLockOSThread(true),
		)
		if err != nil {
			logger.Fatal().Err(err).Msg("gnet HTTP server failed to start")
		}
	}()

	<-ctx.Done()
	logger.Info().Msg("Worker stopped gracefully")
}

func runUDPListener(ctx context.Context, ar *routing.AdaptiveRouter, logger *zerolog.Logger) {
	logger.Info().Msg("Worker starting UDP listener on port 9996...")
	addr, err := net.ResolveUDPAddr("udp", ":9996")
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to resolve UDP address")
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to start UDP listener")
	}
	defer conn.Close()

	buf := make([]byte, 2048)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			n, _, err := conn.ReadFromUDP(buf)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				logger.Error().Err(err).Msg("Failed to read from UDP connection")
				continue
			}

			var payload payments.PaymentsPayload
			if err := json.Unmarshal(buf[:n], &payload); err != nil {
				logger.Error().Err(err).Msg("Failed to unmarshal payment payload from UDP")
				continue
			}
			ar.PayloadChan <- payload
		}
	}
}

func (s *HTTPServer) OnTraffic(c gnet.Conn) (action gnet.Action) {
	buf, err := c.Next(-1)
	if err != nil {
		return gnet.Close
	}

	requestLineEnd := bytes.Index(buf, []byte("\r\n"))
	if requestLineEnd == -1 {
		return gnet.Close
	}

	requestLineParts := bytes.Split(buf[:requestLineEnd], []byte(" "))
	if len(requestLineParts) < 2 {
		return gnet.Close
	}

	fullPath := string(requestLineParts[1])
	path, query, _ := bytes.Cut([]byte(fullPath), []byte("?"))

	switch string(path) {
	case "/payments-summary":
		q, _ := bytes.CutPrefix(query, []byte("from="))
		fromStr, toStr, _ := bytes.Cut(q, []byte("&to="))
		from, _ := time.Parse(time.RFC3339Nano, string(fromStr))
		to, err := time.Parse(time.RFC3339Nano, string(toStr))
		if err != nil {
			from, to = time.Now(), time.Now()
		}

		defaultData, fallbackData, _ := s.aggregator.GetSummary(context.Background(), &from, &to)
		summary := payments.PaymentsSummary{Default: defaultData, Fallback: fallbackData}
		respBody, _ := json.Marshal(summary)

		response := fmt.Sprintf(
			"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s",
			len(respBody),
			respBody,
		)
		c.Write([]byte(response))

	case "/purge-payments":
		s.aggregator.PurgeSummary(context.Background())
		c.Write(http204NoContent)

	default:
		c.Write(http404NotFound)
	}

	return gnet.None
}
