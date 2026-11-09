// Command operator-mock is a fake SMS operator for local runs and load
// tests: POST /send dedupes by ref and fails on demand (temporary,
// permanent, timeout); GET /stats returns how many messages it "sent".
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

	"github.com/farima-sedghi/sms-gateway/internal/operator/operatormock"
	"github.com/farima-sedghi/sms-gateway/internal/platform/config"
	"github.com/farima-sedghi/sms-gateway/internal/platform/httpserver"
	"github.com/farima-sedghi/sms-gateway/internal/platform/logging"
)

type appConfig struct {
	HTTPAddr        string
	Mock            operatormock.Config
	LogLevel        slog.Level
	ShutdownTimeout time.Duration
}

func loadConfig() (appConfig, error) {
	r := config.NewReader(os.LookupEnv)
	cfg := appConfig{
		HTTPAddr: r.String("HTTP_ADDR", ":8090"),
		Mock: operatormock.Config{
			Latency:         r.Duration("OPERATOR_LATENCY", 50*time.Millisecond),
			TempFailureRate: r.Float("OPERATOR_TEMP_FAILURE_RATE", 0),
			PermFailureRate: r.Float("OPERATOR_PERM_FAILURE_RATE", 0),
			TimeoutRate:     r.Float("OPERATOR_TIMEOUT_RATE", 0),
			TimeoutDelay:    r.Duration("OPERATOR_TIMEOUT_DELAY", 30*time.Second),
		},
		LogLevel:        r.LogLevel("LOG_LEVEL", slog.LevelInfo),
		ShutdownTimeout: r.Duration("SHUTDOWN_TIMEOUT", 5*time.Second),
	}
	if err := r.Err(); err != nil {
		return cfg, err
	}
	m := cfg.Mock
	for name, v := range map[string]float64{
		"OPERATOR_TEMP_FAILURE_RATE": m.TempFailureRate,
		"OPERATOR_PERM_FAILURE_RATE": m.PermFailureRate,
		"OPERATOR_TIMEOUT_RATE":      m.TimeoutRate,
	} {
		if v < 0 || v > 1 {
			return cfg, fmt.Errorf("%s must be between 0 and 1, got %v", name, v)
		}
	}
	if sum := m.TempFailureRate + m.PermFailureRate + m.TimeoutRate; sum > 1 {
		return cfg, fmt.Errorf("failure rates add up to %v; they must add up to at most 1", sum)
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
	operatormock.New(cfg.Mock, log).Register(mux)
	handler := httpserver.Chain(mux, httpserver.RequestID, httpserver.Recover(log), httpserver.AccessLog(log))

	m := cfg.Mock
	log.Info("operator-mock started", "latency", m.Latency, "temp_failure_rate", m.TempFailureRate,
		"perm_failure_rate", m.PermFailureRate, "timeout_rate", m.TimeoutRate, "timeout_delay", m.TimeoutDelay)
	return httpserver.Run(ctx, cfg.HTTPAddr, handler, log, cfg.ShutdownTimeout)
}
