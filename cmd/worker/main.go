package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
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
	ar         *routing.AdaptiveRouter
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

	httpServer := &HTTPServer{
		aggregator: summaryAggregator,
		logger:     &logger,
		ar:         ar,
	}

	socketPath := os.Getenv("SOCKET_PATH")
	if socketPath == "" {
		log.Fatal("SOCKET_PATH environment variable not set")
	}

	socketDir := filepath.Dir(socketPath)
	if _, err := os.Stat(socketDir); os.IsNotExist(err) {
		if err := os.MkdirAll(socketDir, 0777); err != nil {
			log.Fatalf("Failed to create socket dir %s: %v", socketDir, err)
		}
	}

	if _, err := os.Stat(socketPath); err == nil {
		os.Remove(socketPath)
	}

	httpAddr := fmt.Sprintf("unix://%s", socketPath)

	go func() {
		logger.Info().Msgf("Worker starting gnet HTTP server on %s", httpAddr)
		err := gnet.Run(
			httpServer,
			httpAddr,
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
	case "/payments":
		c.Write(http200OK)
		idx := bytes.Index(buf, []byte("\r\n\r\n"))
		if idx == -1 {
			c.Close()
			return
		}
		body := make([]byte, len(buf[idx+4:]))
		copy(body, buf[idx+4:])
		var payload payments.PaymentsPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			s.logger.Error().Err(err).Msg("Failed to unmarshal payment payload from UDP")
			return
		}
		s.ar.PayloadChan <- payload
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
