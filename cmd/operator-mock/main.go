// Command operator-mock is a fake SMS operator for local runs and load tests.
// Right now it only serves /healthz. The send endpoint with configurable
// latency and failure rate comes in a later step. Its env vars are already
// read, so the compose file won't need to change.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/farima-sedghi/sms-gateway/internal/platform/config"
	"github.com/farima-sedghi/sms-gateway/internal/platform/httpserver"
	"github.com/farima-sedghi/sms-gateway/internal/platform/logging"
)

type appConfig struct {
	HTTPAddr        string
	Latency         time.Duration
	FailureRate     float64
	LogLevel        slog.Level
	ShutdownTimeout time.Duration
}

func loadConfig() (appConfig, error) {
	r := config.NewReader(os.LookupEnv)
	cfg := appConfig{
		HTTPAddr:        r.String("HTTP_ADDR", ":8090"),
		Latency:         r.Duration("OPERATOR_LATENCY", 50*time.Millisecond),
		FailureRate:     r.Float("OPERATOR_FAILURE_RATE", 0),
		LogLevel:        r.LogLevel("LOG_LEVEL", slog.LevelInfo),
		ShutdownTimeout: r.Duration("SHUTDOWN_TIMEOUT", 5*time.Second),
	}
	if err := r.Err(); err != nil {
		return cfg, err
	}
	if cfg.FailureRate < 0 || cfg.FailureRate > 1 {
		return cfg, fmt.Errorf("OPERATOR_FAILURE_RATE must be between 0 and 1, got %v", cfg.FailureRate)
	}
	return cfg, nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "operator-mock:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	log := logging.New(os.Stdout, "operator-mock", cfg.LogLevel)
	defer log.Info("operator-mock stopped")

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", httpserver.Healthz)
	handler := httpserver.Chain(mux, httpserver.RequestID, httpserver.Recover(log), httpserver.AccessLog(log))

	log.Info("operator-mock started", "latency", cfg.Latency, "failure_rate", cfg.FailureRate)
	return httpserver.Run(ctx, cfg.HTTPAddr, handler, log, cfg.ShutdownTimeout)
}
