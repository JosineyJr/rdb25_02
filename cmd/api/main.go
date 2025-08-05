package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/JosineyJr/rdb25_02/internal/config"
	"github.com/JosineyJr/rdb25_02/internal/health"
	"github.com/JosineyJr/rdb25_02/internal/routing"
	"github.com/panjf2000/gnet/v2"
	"github.com/rs/zerolog"
)

var (
	http200Ok        = []byte("HTTP/1.1 200 Ok\r\nContent-Length: 0\r\n\r\n")
	http204NoContent = []byte("HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
	http404NotFound  = []byte("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
	http500Error     = []byte("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n")
)

type UnixConnPool struct {
	mu      sync.Mutex
	conns   chan *net.UnixConn
	factory func() (*net.UnixConn, error)
}

func NewUnixConnPool(
	initialSize, maxSize int,
	factory func() (*net.UnixConn, error),
) (*UnixConnPool, error) {
	if initialSize < 0 || maxSize <= 0 || initialSize > maxSize {
		return nil, errors.New("invalid capacity settings")
	}

	pool := &UnixConnPool{
		conns:   make(chan *net.UnixConn, maxSize),
		factory: factory,
	}

	for range initialSize {
		conn, err := factory()
		if err != nil {
			close(pool.conns)
			for c := range pool.conns {
				c.Close()
			}
			return nil, err
		}
		pool.conns <- conn
	}

	return pool, nil
}

func (p *UnixConnPool) Get() (*net.UnixConn, error) {
	select {
	case conn := <-p.conns:
		if conn == nil {
			return nil, errors.New("connection is nil from pool")
		}
		return conn, nil
	default:
		return p.factory()
	}
}

func (p *UnixConnPool) Put(conn *net.UnixConn) {
	if conn == nil {
		return
	}

	select {
	case p.conns <- conn:
	default:
		conn.Close()
	}
}

func (p *UnixConnPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conns == nil {
		return
	}

	close(p.conns)
	for conn := range p.conns {
		conn.Close()
	}
	p.conns = nil
}

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
	backendPool      *UnixConnPool
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
	defer purgeConn.Close()

	ar := routing.NewAdaptiveRouter(
		1,
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
		backendSockets := []string{"/sockets/api2.sock", "/sockets/api3.sock"}
		var currentSocket uint64

		factory := func() (*net.UnixConn, error) {
			sockPath := backendSockets[atomic.AddUint64(&currentSocket, 1)%uint64(len(backendSockets))]
			addr, err := net.ResolveUnixAddr("unix", sockPath)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve unix addr for %s: %w", sockPath, err)
			}
			return net.DialUnix("unix", nil, addr)
		}

		pool, err := NewUnixConnPool(len(backendSockets)*5, 50, factory)
		if err != nil {
			logger.Fatal().Err(err).Msg("Failed to create connection pool")
		}
		ps.backendPool = pool

		totalProcessingNodes := uint64(len(backendSockets) + 1)

		ps.processPayment = func(c gnet.Conn, buf []byte) {
			c.Write(http200Ok)
			nodeIndex := (ps.nextBackendIndex.Add(1) - 1) % totalProcessingNodes

			matches := ps.re.FindSubmatch(buf)
			if len(matches) < 2 {
				return
			}

			if nodeIndex == 0 {
				ps.logger.Info().Msg("LB is processing the request locally")
				ps.ar.PayloadChan <- string(matches[1])
				return
			}

			logger.Info().Msgf("Forwarding request to a backend worker")

			conn, err := ps.backendPool.Get()
			if err != nil {
				ps.logger.Error().Err(err).Msg("Failed to get connection from pool")
				ps.ar.PayloadChan <- string(matches[1])
				return
			}

			_, err = conn.Write(buf)
			if err != nil {
				ps.logger.Error().Err(err).Msg("Failed to write to payment backend")
				conn.Close()
			} else {
				ps.backendPool.Put(conn)
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

	logger.Info().Msgf("Gnet server starting on %s", addr)
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
	if ps.backendPool != nil {
		ps.backendPool.Close()
	}
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
		ps.purgeConn.Write([]byte("1\n"))
	default:
		c.Write(http404NotFound)
	}
	return
}
