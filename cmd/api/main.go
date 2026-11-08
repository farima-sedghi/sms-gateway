// Command api is the public REST API: wallet top-up and balance, message
// acceptance, and health endpoints. Report routes come in a later step.
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
	"github.com/farima-sedghi/sms-gateway/internal/platform/kafka"
	"github.com/farima-sedghi/sms-gateway/internal/platform/logging"
	"github.com/farima-sedghi/sms-gateway/internal/platform/postgres"
	"github.com/farima-sedghi/sms-gateway/internal/sms"
	"github.com/farima-sedghi/sms-gateway/internal/wallet"
)

type appConfig struct {
	HTTPAddr        string
	DatabaseURL     string
	KafkaBrokers    []string
	LogLevel        slog.Level
	ShutdownTimeout time.Duration
	SMSPrice        int64 // minor units per message
}

func loadConfig() (appConfig, error) {
	r := config.NewReader(os.LookupEnv)
	cfg := appConfig{
		HTTPAddr:        r.String("HTTP_ADDR", ":8080"),
		DatabaseURL:     r.Required("DATABASE_URL"),
		KafkaBrokers:    r.RequiredList("KAFKA_BROKERS"),
		LogLevel:        r.LogLevel("LOG_LEVEL", slog.LevelInfo),
		ShutdownTimeout: r.Duration("SHUTDOWN_TIMEOUT", 15*time.Second),
		SMSPrice:        r.Int64("SMS_PRICE", 0),
	}
	if err := r.Err(); err != nil {
		return cfg, err
	}
	if cfg.SMSPrice <= 0 {
		return cfg, fmt.Errorf("SMS_PRICE must be a positive integer (minor units), got %d", cfg.SMSPrice)
	}
	return cfg, nil
}

func main() {
	// main only wires up the signal context and the exit code. Everything else
	// is in run, which returns an error, so deferred cleanup always runs
	// (os.Exit skips defers).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "api:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	log := logging.New(os.Stdout, "api", cfg.LogLevel)
	defer log.Info("api stopped") // registered first, so it runs last (defers are LIFO)

	db, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()

	kc, err := kafka.NewClient(ctx, cfg.KafkaBrokers)
	if err != nil {
		return err
	}
	defer kc.Close()

	walletSvc := wallet.NewService(db)
	smsSvc := sms.NewService(db, cfg.SMSPrice)

	mux := http.NewServeMux()
	wallet.NewHandler(walletSvc, log).Register(mux)
	sms.NewHandler(smsSvc, log).Register(mux)
	mux.HandleFunc("GET /healthz", httpserver.Healthz)
	mux.Handle("GET /readyz", httpserver.Readyz(map[string]httpserver.Check{
		"postgres": db.Ping,
		"kafka":    func(ctx context.Context) error { return kafka.Ping(ctx, kc) },
	}))

	handler := httpserver.Chain(mux,
		httpserver.RequestID,
		httpserver.Recover(log),
		httpserver.AccessLog(log),
	)

	log.Info("api started")
	return httpserver.Run(ctx, cfg.HTTPAddr, handler, log, cfg.ShutdownTimeout)
}
