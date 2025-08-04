package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sync/atomic"
	"syscall"

	"github.com/JosineyJr/rdb25_02/internal/config"
	"github.com/JosineyJr/rdb25_02/internal/health"
	"github.com/JosineyJr/rdb25_02/internal/routing"
	"github.com/panjf2000/gnet/v2"
	"github.com/rs/zerolog"
)

// HTTP Responses
var (
	http200Ok        = []byte("HTTP/1.1 200 Ok\r\nContent-Length: 0\r\n\r\n")
	http204NoContent = []byte("HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
	http404NotFound  = []byte("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
	http500Error     = []byte("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n")
)

type paymentServer struct {
	gnet.BuiltinEventEngine
	logger           *zerolog.Logger
	ctx              context.Context
	socketPath       string
	paymentsConn     *net.UnixConn
	summaryConn      *net.UnixConn
	purgeConn        *net.UnixConn
	ar               *routing.AdaptiveRouter
	re               *regexp.Regexp
	paymentBackends  []*net.UnixConn
	nextBackendIndex atomic.Uint64
	processPayment   func(c gnet.Conn, buf []byte)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	config.LoadEnv()

	logger := zerolog.New(os.Stdout).Level(zerolog.InfoLevel).With().Timestamp().Logger()

	paymentsAddr, err := net.ResolveUnixAddr("unix", os.Getenv("PAYMENTS_SOCKET_PATH"))
	if err != nil {
		logger.Fatal().Err(err).Msg("Unable to create in-memory aggregator")
	}
	paymentsConn, err := net.DialUnix("unix", nil, paymentsAddr)
	if err != nil {
		log.Fatalf("Failed to dial socket: %v", err)
	}
	defer paymentsConn.Close()

	summaryAddr, err := net.ResolveUnixAddr("unix", os.Getenv("SUMMARY_SOCKET_PATH"))
	if err != nil {
		logger.Fatal().Err(err).Msg("Unable to create in-memory aggregator")
	}
	summaryConn, err := net.DialUnix("unix", nil, summaryAddr)
	if err != nil {
		log.Fatalf("Failed to dial socket: %v", err)
	}
	defer summaryConn.Close()

	purgeAddr, err := net.ResolveUnixAddr("unix", os.Getenv("PURGE_SOCKET_PATH"))
	if err != nil {
		logger.Fatal().Err(err).Msg("Unable to create in-memory aggregator")
	}
	purgeConn, err := net.DialUnix("unix", nil, purgeAddr)
	if err != nil {
		log.Fatalf("Failed to dial socket: %v", err)
	}
	defer summaryConn.Close()

	ar := routing.NewAdaptiveRouter(
		2,
		paymentsConn,
	)
	ar.Start(ctx)

	healthUpdater := health.NewHealthUpdater(ar)
	healthUpdater.Start(ctx)

	ps := &paymentServer{
		logger:       &logger,
		ctx:          context.Background(),
		socketPath:   os.Getenv("SOCKET_PATH"),
		paymentsConn: paymentsConn,
		summaryConn:  summaryConn,
		purgeConn:    purgeConn,
		ar:           ar,
		re:           regexp.MustCompile(`"correlationId"\s*:\s*"([^"]+)"`),
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

	var addr string
	if os.Getenv("LB") == "1" {
		ps.paymentBackends = make([]*net.UnixConn, 0, 2)
		for i := 2; i <= 2; i++ {
			socketPath := fmt.Sprintf("/tmp/api%d.sock", i)
			addr, err := net.ResolveUnixAddr("unix", socketPath)
			if err != nil {
				logger.Fatal().Err(err).Msgf("Failed to resolve unix addr for %s", socketPath)
			}
			conn, err := net.DialUnix("unix", nil, addr)
			if err != nil {
				logger.Fatal().Err(err).Msgf("Failed to dial socket %s", socketPath)
			}
			defer conn.Close()
			ps.paymentBackends = append(ps.paymentBackends, conn)
			logger.Info().Msgf("Connected to payment backend: %s", socketPath)
		}
		ps.processPayment = func(c gnet.Conn, buf []byte) {
			c.Write(http200Ok)
			idx := (ps.nextBackendIndex.Add(1) - 1) % uint64(len(ps.paymentBackends)+1)
			matches := ps.re.FindSubmatch(buf)
			if idx == 0 {
				ps.ar.PayloadChan <- string(matches[1])
				return
			}

			_, err := ps.paymentBackends[idx-1].Write(buf)
			if err != nil {
				ps.logger.Error().
					Err(err).
					Msg("Failed to write to payment backend")
			}
		}
		addr = "tcp4://" + config.PORT
	} else {
		ps.processPayment = func(c gnet.Conn, buf []byte) {
			c.Write(http200Ok)
			matches := ps.re.FindSubmatch(buf)
			ps.ar.PayloadChan <- string(matches[1])
		}
		addr = fmt.Sprintf("unix://%s", ps.socketPath)
	}

	log.Printf("Gnet server starting on %s", addr)
	err = gnet.Run(ps, addr,
		gnet.WithMulticore(true),
		gnet.WithReusePort(true),
	)
	if err != nil {
		log.Fatalf("Gnet server failed to start: %v", err)
	}
}

func (ps *paymentServer) OnBoot(eng gnet.Engine) (action gnet.Action) {
	ps.logger.Info().Msgf("Gnet server started on port %s", config.PORT)
	return
}

func (ps *paymentServer) OnShutdown(eng gnet.Engine) {

}

func (ps *paymentServer) OnTraffic(c gnet.Conn) (action gnet.Action) {
	buf, err := c.Next(-1)
	if err != nil {
		return gnet.Close
	}

	requestLineEnd := bytes.Index(buf, []byte("\r\n"))
	if requestLineEnd == -1 {
		fmt.Println("Requisição inválida")
		return
	}
	requestLine := buf[:requestLineEnd]

	pathStart := bytes.IndexByte(requestLine, ' ')
	if pathStart == -1 {
		fmt.Println("Espaçamento inválido")
		return
	}
	pathStart++

	pathEnd := bytes.LastIndexByte(requestLine, ' ')
	if pathEnd == -1 || pathEnd <= pathStart {
		fmt.Println("Formato de requisição inválido")
		return
	}

	path, query, _ := bytes.Cut(requestLine[pathStart:pathEnd], []byte("?"))

	switch string(path) {
	case "/payments":
		ps.processPayment(c, buf)
	case "/payments-summary":
		_, err := ps.summaryConn.Write(append(query, '\n'))
		if err != nil {
			ps.logger.Error().Err(err).Msg("Failed to write to summary socket")
			c.Write(http500Error)
			return
		}

		reader := bufio.NewReader(ps.summaryConn)
		s, err := reader.ReadBytes('\n')
		if err != nil {
			ps.logger.Error().Err(err).Msg("Failed to read from summary socket")
			c.Write(http500Error)
			return
		}
		s = bytes.TrimSpace(s)

		response := fmt.Sprintf(
			"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s",
			len(s),
			s,
		)
		c.Write([]byte(response))
	case "/purge-payments":
		c.Write(http204NoContent)
		ps.purgeConn.Write([]byte("1"))
	default:
		c.Write(http404NotFound)
	}
	return
}
