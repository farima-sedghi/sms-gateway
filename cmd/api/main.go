// Command api is the public REST API: wallet top-up and balance, message
// acceptance, message reports, and health endpoints.
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
	"github.com/farima-sedghi/sms-gateway/internal/ratelimit"
	"github.com/farima-sedghi/sms-gateway/internal/report"
	"github.com/farima-sedghi/sms-gateway/internal/sms"
	"github.com/farima-sedghi/sms-gateway/internal/wallet"
)

type appConfig struct {
	HTTPAddr        string
	DatabaseURL     string
	KafkaBrokers    []string
	LogLevel        slog.Level
	ShutdownTimeout time.Duration
	SMSPrice        int64         // minor units per message
	ExpressTTL      time.Duration // express must reach the operator within this

	// Rate limits are GLOBAL per customer; each instance enforces
	// global / APIInstances (see docs/adr/0007).
	RateNormal   ratelimit.Limit
	RateExpress  ratelimit.Limit
	APIInstances int
	RateIdleTTL  time.Duration
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
		ExpressTTL:      r.Duration("EXPRESS_TTL", 2*time.Minute),
		RateNormal:      ratelimit.Limit{Rate: r.Float("RATE_LIMIT_NORMAL_RPS", 200), Burst: r.Int("RATE_LIMIT_NORMAL_BURST", 400)},
		RateExpress:     ratelimit.Limit{Rate: r.Float("RATE_LIMIT_EXPRESS_RPS", 50), Burst: r.Int("RATE_LIMIT_EXPRESS_BURST", 100)},
		APIInstances:    r.Int("API_INSTANCES", 1),
		RateIdleTTL:     r.Duration("RATE_LIMIT_IDLE_TTL", 10*time.Minute),
	}
	if err := r.Err(); err != nil {
		return cfg, err
	}
	if cfg.SMSPrice <= 0 {
		return cfg, fmt.Errorf("SMS_PRICE must be a positive integer (minor units), got %d", cfg.SMSPrice)
	}
	if cfg.ExpressTTL <= 0 {
		return cfg, fmt.Errorf("EXPRESS_TTL must be positive, got %s", cfg.ExpressTTL)
	}
	if cfg.APIInstances <= 0 {
		return cfg, fmt.Errorf("API_INSTANCES must be positive, got %d", cfg.APIInstances)
	}
	for name, l := range map[string]ratelimit.Limit{"NORMAL": cfg.RateNormal, "EXPRESS": cfg.RateExpress} {
		if l.Rate <= 0 || l.Burst <= 0 {
			return cfg, fmt.Errorf("RATE_LIMIT_%s_RPS and RATE_LIMIT_%s_BURST must be positive", name, name)
		}
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

	// Per-instance share of the global per-customer limits. The burst is
	// divided too, but never below 1, or nobody could ever send.
	perInstance := func(l ratelimit.Limit) ratelimit.Limit {
		n := cfg.APIInstances
		return ratelimit.Limit{Rate: l.Rate / float64(n), Burst: max(l.Burst/n, 1)}
	}
	limiter := ratelimit.NewMemory(ratelimit.Config{
		Classes: map[string]ratelimit.Limit{
			string(sms.PriorityNormal):  perInstance(cfg.RateNormal),
			string(sms.PriorityExpress): perInstance(cfg.RateExpress),
		},
		IdleTTL: cfg.RateIdleTTL,
	}, log)
	go limiter.Run(ctx, time.Minute) // evicts idle buckets; stops on shutdown

	walletSvc := wallet.NewService(db)
	smsSvc := sms.NewService(db, cfg.SMSPrice, cfg.ExpressTTL, limiter)

	mux := http.NewServeMux()
	wallet.NewHandler(walletSvc, log).Register(mux)
	sms.NewHandler(smsSvc, log).Register(mux)
	report.NewHandler(report.NewService(db), log).Register(mux)
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
