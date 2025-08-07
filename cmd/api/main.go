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
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/JosineyJr/rdb25_02/internal/config"
	jsoniter "github.com/json-iterator/go"
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
	backendPool      *UnixConnPool
	nextBackendIndex atomic.Uint64
	processPayment   func(c gnet.Conn, buf []byte)
}

func main() {
	_, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	config.LoadEnv()

	logger := zerolog.New(os.Stdout).Level(zerolog.InfoLevel).With().Timestamp().Logger()

	paymentsAddr, err := net.ResolveUnixAddr("unix", os.Getenv("PAYMENTS_SOCKET_PATH"))
	if err != nil {
		logger.Fatal().Err(err).Msg("unable to create in-memory aggregator")
	}
	paymentsConn, err := net.DialUnix("unix", nil, paymentsAddr)
	if err != nil {
		log.Fatalf("failed to dial socket: %v", err)
	}
	defer paymentsConn.Close()

	summaryAddr, err := net.ResolveUnixAddr("unix", os.Getenv("SUMMARY_SOCKET_PATH"))
	if err != nil {
		logger.Fatal().Err(err).Msg("unable to create in-memory aggregator")
	}
	summaryConn, err := net.DialUnix("unix", nil, summaryAddr)
	if err != nil {
		log.Fatalf("failed to dial socket: %v", err)
	}
	defer summaryConn.Close()

	purgeAddr, err := net.ResolveUnixAddr("unix", os.Getenv("PURGE_SOCKET_PATH"))
	if err != nil {
		logger.Fatal().Err(err).Msg("unable to create in-memory aggregator")
	}
	purgeConn, err := net.DialUnix("unix", nil, purgeAddr)
	if err != nil {
		log.Fatalf("failed to dial socket: %v", err)
	}
	defer purgeConn.Close()

	ps := &paymentServer{
		logger:       &logger,
		ctx:          context.Background(),
		socketPath:   os.Getenv("SOCKET_PATH"),
		paymentsConn: paymentsConn,
		summaryConn:  summaryConn,
		purgeConn:    purgeConn,
	}

	socketPath := os.Getenv("SOCKET_PATH")
	if socketPath == "" {
		log.Fatal("SOCKET_PATH environment variable not set")
	}

	socketDir := filepath.Dir(socketPath)
	if _, err := os.Stat(socketDir); os.IsNotExist(err) {
		if err := os.MkdirAll(socketDir, 0777); err != nil {
			log.Fatalf("failed to create socket dir %s: %v", socketDir, err)
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
			logger.Fatal().Err(err).Msg("failed to create connection pool")
		}
		ps.backendPool = pool

		totalProcessingNodes := uint64(len(backendSockets) + 1)

		ps.processPayment = func(c gnet.Conn, buf []byte) {
			bodyStart := bytes.Index(buf, []byte("\r\n\r\n"))
			if bodyStart == -1 {
				return
			}
			jsonBody := buf[bodyStart+4:]
			correlationId := jsoniter.Get(jsonBody, "correlationId").ToString()
			if correlationId == "" {
				return
			}

			nodeIndex := (ps.nextBackendIndex.Add(1) - 1) % totalProcessingNodes

			if nodeIndex == 0 {
				ps.paymentsConn.Write(
					append([]byte(correlationId), '\n'),
				)
				return
			}

			conn, err := ps.backendPool.Get()
			if err != nil {
				ps.logger.Error().Err(err).Msg("failed to get connection from pool")
				ps.paymentsConn.Write(
					append([]byte(correlationId), '\n'),
				)
				return
			}

			_, err = conn.Write(buf)
			if err != nil {
				ps.logger.Error().Err(err).Msg("failed to write to payment backend")
				conn.Close()
			} else {
				ps.backendPool.Put(conn)
			}
		}
		addr = "tcp4://" + config.PORT
	} else {
		ps.processPayment = func(c gnet.Conn, buf []byte) {
			bodyStart := bytes.Index(buf, []byte("\r\n\r\n"))
			if bodyStart == -1 {
				return
			}
			jsonBody := buf[bodyStart+4:]
			correlationId := jsoniter.Get(jsonBody, "correlationId").ToString()
			if correlationId == "" {
				return
			}

			ps.paymentsConn.Write(
				append([]byte(correlationId), '\n'),
			)
		}
		addr = fmt.Sprintf("unix://%s", ps.socketPath)
	}

	logger.Info().Msgf("gnet server starting on %s", addr)
	err = gnet.Run(ps, addr,
		gnet.WithMulticore(true),
		gnet.WithReusePort(true),
		gnet.WithLockOSThread(true),
	)
	if err != nil {
		log.Fatalf("gnet server failed to start: %v", err)
	}
}

func (ps *paymentServer) OnBoot(eng gnet.Engine) (action gnet.Action) {
	ps.logger.Info().Msgf("gnet server started on port %s", config.PORT)
	return
}

func (ps *paymentServer) OnShutdown(eng gnet.Engine) {
	if ps.backendPool != nil {
		ps.backendPool.Close()
	}
}

func (ps *paymentServer) OnTraffic(c gnet.Conn) (action gnet.Action) {
	n := time.Now()

	buf, err := c.Next(-1)
	if err != nil {
		return gnet.Close
	}

	requestLineEnd := bytes.Index(buf, []byte("\r\n"))
	if requestLineEnd == -1 {
		return
	}
	requestLine := buf[:requestLineEnd]

	pathStart := bytes.IndexByte(requestLine, ' ')
	if pathStart == -1 {
		return
	}
	pathStart++

	pathEnd := bytes.LastIndexByte(requestLine, ' ')
	if pathEnd == -1 || pathEnd <= pathStart {
		return
	}

	path, query, _ := bytes.Cut(requestLine[pathStart:pathEnd], []byte("?"))

	switch string(path) {
	case "/payments":
		defer func(t time.Time) {
			d := time.Since(t)
			if d >= time.Millisecond {
				ps.logger.Info().Str("duration", d.String()).Msg("payments")
			}
		}(n)
		c.Write(http200Ok)
		ps.processPayment(c, buf)
		return
	case "/payments-summary":
		defer func(t time.Time) {
			d := time.Since(t)
			if d >= time.Millisecond {
				ps.logger.Info().Str("duration", d.String()).Msg("summary")
			}
		}(n)

		c.Write([]byte("HTTP/1.1 200 Ok\r\n"))
		_, err := ps.summaryConn.Write(append(query, '\n'))
		if err != nil {
			ps.logger.Error().Err(err).Msg("failed to write to summary socket")
			c.Write(http500Error)
			return
		}

		reader := bufio.NewReader(ps.summaryConn)
		s, err := reader.ReadBytes('\n')
		if err != nil {
			ps.logger.Error().Err(err).Msg("failed to read from summary socket")
			c.Write(http500Error)
			return
		}
		s = bytes.TrimSpace(s)

		response := fmt.Sprintf(
			"Content-Type: application/json\r\nContent-Length: %d\r\n\r\n%s",
			len(s),
			s,
		)
		c.Write([]byte(response))
		return
	case "/purge-payments":
		defer func(t time.Time) {
			d := time.Since(t)
			if d >= time.Millisecond {
				ps.logger.Info().Str("duration", d.String()).Msg("purge")
			}
		}(n)

		c.Write(http204NoContent)
		ps.purgeConn.Write([]byte("1\n"))
	default:
		c.Write(http404NotFound)
	}
	return
}
