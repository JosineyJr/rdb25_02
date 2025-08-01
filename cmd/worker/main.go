package main

import (
	"bytes"
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/JosineyJr/rdb25_02/internal/config"
	"github.com/JosineyJr/rdb25_02/internal/storage"
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
	runtime.GOMAXPROCS(4)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := zerolog.New(os.Stdout).Level(zerolog.InfoLevel).With().Timestamp().Logger()

	summaryAggregator, err := storage.NewInMemoryAggregator()
	if err != nil {
		logger.Fatal().Err(err).Msg("Unable to create in-memory aggregator")
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

	if _, err := os.Stat(os.Getenv("PAYMENTS_SOCKET_PATH")); err == nil {
		os.Remove(os.Getenv("PAYMENTS_SOCKET_PATH"))
	}

	if _, err := os.Stat(os.Getenv("SUMMARY_SOCKET_PATH")); err == nil {
		os.Remove(os.Getenv("SUMMARY_SOCKET_PATH"))
	}

	if _, err := os.Stat(os.Getenv("PURGE_SOCKET_PATH")); err == nil {
		os.Remove(os.Getenv("PURGE_SOCKET_PATH"))
	}

	paymentsListener, err := net.Listen("unix", os.Getenv("PAYMENTS_SOCKET_PATH"))
	if err != nil {
		log.Fatalf("Failed to listen on socket: %v", err)
	}
	defer func() {
		paymentsListener.Close()
		os.RemoveAll(os.Getenv("PAYMENTS_SOCKET_PATH"))
	}()

	summaryListener, err := net.Listen("unix", os.Getenv("SUMMARY_SOCKET_PATH"))
	if err != nil {
		log.Fatalf("Failed to listen on socket: %v", err)
	}
	defer func() {
		summaryListener.Close()
		os.RemoveAll(os.Getenv("SUMMARY_SOCKET_PATH"))
	}()

	purgeListener, err := net.Listen("unix", os.Getenv("PURGE_SOCKET_PATH"))
	if err != nil {
		log.Fatalf("Failed to listen on socket: %v", err)
	}
	defer func() {
		purgeListener.Close()
		os.RemoveAll(os.Getenv("PURGE_SOCKET_PATH"))
	}()

	go func() {
		for {
			conn, err := paymentsListener.Accept()
			if err != nil {
				log.Printf("Failed to accept connection: %v", err)
				return
			}

			go func() {
				for {
					buf := make([]byte, 27)
					_, err := conn.Read(buf)
					if err != nil {
						return
					}
					n := bytes.IndexByte(buf, 0)
					if n == -1 {
						n = len(buf)
					}
					buf = buf[:n]
					s := bytes.IndexByte(buf, '|')
					if s == -1 {
						s = len(buf)
					}
					ts, _ := strconv.Atoi(string(buf[s+1:]))
					summaryAggregator.Update(ctx, string(buf[:s]), int64(ts))
				}
			}()
		}
	}()

	go func() {
		for {
			conn, err := summaryListener.Accept()
			if err != nil {
				log.Printf("Failed to accept connection: %v", err)
				return
			}

			go func() {
				for {
					buf := make([]byte, 57)
					_, err := conn.Read(buf)
					if err != nil {
						return
					}
					n := bytes.IndexByte(buf, 0)
					if n == -1 {
						n = len(buf)
					}

					q, _ := bytes.CutPrefix(buf[:n], []byte("from="))
					fromStr, toStr, _ := bytes.Cut(q, []byte("&to="))
					from, _ := time.Parse(time.RFC3339Nano, string(fromStr))
					to, err := time.Parse(time.RFC3339Nano, string(toStr))
					if err != nil {
						from, to = time.Now(), time.Now()
					}

					summary, err := summaryAggregator.GetSummary(ctx, &from, &to)
					if err != nil {
						return
					}
					jsoniter.ConfigFastest.NewEncoder(conn).Encode(summary)
				}
			}()
		}
	}()

	go func() {
		for {
			conn, err := purgeListener.Accept()
			if err != nil {
				log.Printf("Failed to accept connection: %v", err)
				return
			}

			go func() {
				for {
					buf := make([]byte, 1)
					_, err := conn.Read(buf)
					if err != nil {
						return
					}
					summaryAggregator.PurgeSummary(ctx)
				}
			}()
		}
	}()

	<-ctx.Done()
	logger.Info().Msg("Worker stopped gracefully")
}
