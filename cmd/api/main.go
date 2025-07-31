package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/JosineyJr/rdb25_02/internal/config"
	jsoniter "github.com/json-iterator/go"
	"github.com/panjf2000/gnet/v2"
	"github.com/rs/zerolog"
)

// HTTP Responses
var (
	http200Ok         = []byte("HTTP/1.1 200 Ok\r\nContent-Length: 0\r\n\r\n")
	http204NoContent  = []byte("HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
	http404NotFound   = []byte("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
	http405NotAllowed = []byte("HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n")
	http500Error      = []byte("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n")
	bufferPool        = sync.Pool{
		New: func() interface{} {
			b := make([]byte, 32*1024)
			return &b
		},
	}
)

var json = jsoniter.ConfigFastest

type paymentServer struct {
	gnet.BuiltinEventEngine
	logger     *zerolog.Logger
	ctx        context.Context
	workerPath string
	socketPath string
}

func main() {
	runtime.GOMAXPROCS(4)
	config.LoadEnv()

	logger := zerolog.New(os.Stdout).Level(zerolog.InfoLevel).With().Timestamp().Logger()

	ps := &paymentServer{
		logger:     &logger,
		ctx:        context.Background(),
		workerPath: os.Getenv("WORKER_SOCKET_PATH"),
		socketPath: os.Getenv("SOCKET_PATH"),
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

	addr := fmt.Sprintf("unix://%s", socketPath)

	log.Printf("Gnet server starting on %s", addr)
	err := gnet.Run(ps, addr,
		gnet.WithMulticore(true),
		gnet.WithLockOSThread(true),
		gnet.WithNumEventLoop(4),
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
		return gnet.Close
	}
	requestLineParts := bytes.Split(buf[:requestLineEnd], []byte(" "))
	if len(requestLineParts) != 3 {
		return gnet.Close
	}
	path, _, _ := bytes.Cut(requestLineParts[1], []byte("?"))

	switch string(path) {
	case "/payments":
		c.Write(http200Ok)
		go ps.handleCreatePayment(buf)
		return
	case "/payments-summary":
		conn, err := net.Dial("unix", ps.workerPath)
		if err != nil {
			ps.logger.Error().Err(err).Msg("Failed to connect to worker via Unix socket")
			return
		}
		defer conn.Close()

		_, err = conn.Write(buf)
		if err != nil {
			ps.logger.Error().Err(err).Msg("Failed to send payment data to worker")
		}

		bufPtr := bufferPool.Get().(*[]byte)
		defer bufferPool.Put(bufPtr)

		n, _ := conn.Read(*bufPtr)
		if n > 0 {
			data := make([]byte, n)
			copy(data, (*bufPtr)[:n])
			c.Write(data)
		}

	case "/purge-payments":
		conn, err := net.Dial("unix", ps.workerPath)
		if err != nil {
			ps.logger.Error().Err(err).Msg("Failed to connect to worker via Unix socket")
			return
		}
		defer conn.Close()

		_, err = conn.Write(buf)
		if err != nil {
			ps.logger.Error().Err(err).Msg("Failed to send payment data to worker")
		}
		c.Write(http204NoContent)
	default:
		c.Write(http404NotFound)
	}
	return
}

func (ps *paymentServer) handleCreatePayment(buf []byte) {
	conn, err := net.Dial("unix", ps.workerPath)
	if err != nil {
		ps.logger.Error().Err(err).Msg("Failed to connect to worker via Unix socket")
		return
	}
	defer conn.Close()

	_, err = conn.Write(buf)
	if err != nil {
		ps.logger.Error().Err(err).Msg("Failed to send payment data to worker")
	}
}
