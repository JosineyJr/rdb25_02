package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/JosineyJr/rdb25_02/internal/config"
	"github.com/JosineyJr/rdb25_02/internal/storage"
	"github.com/JosineyJr/rdb25_02/pkg/payments"
	jsoniter "github.com/json-iterator/go"
	"github.com/panjf2000/gnet/v2"
	"github.com/rs/zerolog"
)

// HTTP Responses
var (
	http200Ok         = []byte("HTTP/1.1 200 Ok\r\nContent-Length: 0\r\n\r\n")
	http202Accepted   = []byte("HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n")
	http204NoContent  = []byte("HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
	http404NotFound   = []byte("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
	http405NotAllowed = []byte("HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n")
	http500Error      = []byte("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n")
)

var json = jsoniter.ConfigFastest

type paymentServer struct {
	gnet.BuiltinEventEngine
	db     *storage.RedisDB
	stream *storage.RedisStream
	logger *zerolog.Logger
}

func main() {
	config.LoadEnv()

	logger := zerolog.New(os.Stdout).Level(zerolog.InfoLevel).With().Timestamp().Logger()

	redisURL := os.Getenv("REDIS_URL")
	redisSocket := os.Getenv("REDIS_SOCKET")

	db, err := storage.NewRedisDB(
		redisURL,
		redisSocket,
		"payments-summary",
	)
	if err != nil {
		logger.Fatal().Err(err).Msg("Unable to connect to Redis for aggregator")
	}

	stream, err := storage.NewRedisStream(
		redisURL,
		redisSocket,
		750,
		500*time.Millisecond,
	)
	if err != nil {
		logger.Fatal().Err(err).Msg("Unable to connect to Redis for aggregator")
	}
	stream.StartBatchWorker(context.Background(), &logger)

	ps := &paymentServer{
		db:     db,
		stream: stream,
		logger: &logger,
	}

	addr := fmt.Sprintf("tcp://:%s", config.PORT)
	log.Printf("Gnet server starting on %s", addr)
	err = gnet.Run(ps, addr,
		gnet.WithMulticore(true),
		gnet.WithReusePort(true),
		gnet.WithTCPNoDelay(gnet.TCPNoDelay),
	)
	if err != nil {
		log.Fatalf("Gnet server failed to start: %v", err)
	}
}

func (ps *paymentServer) OnBoot(eng gnet.Engine) (action gnet.Action) {
	ps.logger.Info().Msgf("Gnet server started on port %s", config.PORT)
	return
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
	method := string(requestLineParts[0])
	fullPath := string(requestLineParts[1])
	path, query, _ := bytes.Cut([]byte(fullPath), []byte("?"))

	switch string(path) {
	case "/payments":
		if method == "POST" {
			ps.handleCreatePayment(c, buf)
		} else {
			c.Write(http405NotAllowed)
		}
	case "/payments-summary":
		if method == "GET" {
			ps.handlePaymentsSummary(c, string(query))
		} else {
			c.Write(http405NotAllowed)
		}
	case "/purge-payments":
		if method == "POST" {
			ps.handlePurgePayments(c)
		} else {
			c.Write(http405NotAllowed)
		}
	default:
		c.Write(http404NotFound)
	}
	return
}

func (ps *paymentServer) handleCreatePayment(c gnet.Conn, buf []byte) {
	idx := bytes.Index(buf, []byte("\r\n\r\n"))
	if idx == -1 {
		c.Close()
		return
	}

	body := make([]byte, len(buf[idx+4:]))
	copy(body, buf[idx+4:])

	go ps.stream.Add(body)
	c.Write(http200Ok)
}

func (ps *paymentServer) handlePaymentsSummary(c gnet.Conn, query string) {
	q, err := url.ParseQuery(query)
	if err != nil {
		c.Write(http500Error)
		return
	}

	from, err := parseDate(q.Get("from"), ps.logger)
	if err != nil {
		c.Write(http500Error)
		return
	}

	to, err := parseDate(q.Get("to"), ps.logger)
	if err != nil {
		c.Write(http500Error)
		return
	}

	defaultData, fallbackData, err := ps.db.GetSummary(context.Background(), &from, &to)
	if err != nil {
		ps.logger.Error().Err(err).Msg("Failed to get summary from Redis")
		c.Write(http500Error)
		return
	}

	defaultData.Total = math.Round(defaultData.Total*10) / 10
	fallbackData.Total = math.Round(fallbackData.Total*10) / 10

	respBody, err := json.Marshal(payments.PaymentsSummary{
		Default:  defaultData,
		Fallback: fallbackData,
	})
	if err != nil {
		ps.logger.Error().Err(err).Msg("Failed to marshal summary response")
		c.Write(http500Error)
		return
	}

	var response bytes.Buffer
	response.WriteString("HTTP/1.1 200 OK\r\n")
	response.WriteString("Content-Type: application/json\r\n")
	response.WriteString("Content-Length: " + strconv.Itoa(len(respBody)) + "\r\n")
	response.WriteString("\r\n")
	response.Write(respBody)

	c.Write(response.Bytes())
}

func (ps *paymentServer) handlePurgePayments(c gnet.Conn) {
	err := ps.db.PurgeSummary(context.Background())
	if err != nil {
		ps.logger.Error().Err(err).Msg("Failed to purge payments table")
		c.Write(http500Error)
		return
	}
	c.Write(http204NoContent)
}

func parseDate(date string, logger *zerolog.Logger) (time.Time, error) {
	if date == "" {
		return time.Now(), nil
	}

	if t, err := time.Parse(time.RFC3339Nano, date); err == nil {
		return t, nil
	}
	if t, err := time.Parse(time.RFC3339, date); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("invalid date format: %s", date)
}
