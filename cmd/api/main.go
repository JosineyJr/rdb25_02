package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"os"

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
)

var json = jsoniter.ConfigFastest

type paymentServer struct {
	gnet.BuiltinEventEngine
	logger  *zerolog.Logger
	ctx     context.Context
	udpConn *net.UDPConn
}

func main() {
	config.LoadEnv()

	logger := zerolog.New(os.Stdout).Level(zerolog.InfoLevel).With().Timestamp().Logger()

	workerAddr, err := net.ResolveUDPAddr("udp", "worker1:9996")
	if err != nil {
		log.Fatalf("Failed to resolve worker address: %v", err)
	}

	conn, err := net.DialUDP("udp", nil, workerAddr)
	if err != nil {
		log.Fatalf("Failed to dial worker via UDP: %v", err)
	}

	ps := &paymentServer{
		logger:  &logger,
		ctx:     context.Background(),
		udpConn: conn,
	}

	addr := fmt.Sprintf("tcp://:%s", config.PORT)
	log.Printf("Gnet server starting on %s", addr)
	err = gnet.Run(ps, addr,
		gnet.WithMulticore(true),
		gnet.WithReusePort(true),
		gnet.WithTCPNoDelay(gnet.TCPNoDelay),
		gnet.WithLockOSThread(true),
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
	if ps.udpConn != nil {
		ps.udpConn.Close()
	}
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
		idx := bytes.Index(buf, []byte("\r\n\r\n"))
		if idx == -1 {
			c.Close()
			return
		}
		body := make([]byte, len(buf[idx+4:]))
		copy(body, buf[idx+4:])
		go ps.handleCreatePayment(body)
		return
	default:
		c.Write(http404NotFound)
	}
	return
}

func (ps *paymentServer) handleCreatePayment(buf []byte) {
	_, err := ps.udpConn.Write(buf)
	if err != nil {
		ps.logger.Error().Err(err).Msg("Failed to send payment data to worker via UDP")
	}
}
