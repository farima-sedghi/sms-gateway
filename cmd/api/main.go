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

	// Per-customer plans (customer_limits) are cached in memory and
	// refreshed every 10s, so no request queries them. Customers without a
	// plan get the env defaults. Every limit is global per customer; each
	// instance enforces its share (global / API_INSTANCES).
	plans := ratelimit.NewPlanCache(db, log)
	if err := plans.Refresh(ctx); err != nil {
		return fmt.Errorf("load rate limit plans: %w", err)
	}
	go plans.Run(ctx, 10*time.Second)
	limiter := ratelimit.NewMemory(ratelimit.Config{
		Limits:  plans.LimitFunc(ratelimit.Defaults{Normal: cfg.RateNormal, Express: cfg.RateExpress}, cfg.APIInstances),
		IdleTTL: cfg.RateIdleTTL,
	}, log)
	go limiter.Run(ctx, time.Minute) // evicts idle buckets; stops on shutdown

	walletSvc := wallet.NewService(db)
	smsSvc := sms.NewService(db, cfg.SMSPrice, cfg.ExpressTTL, limiter)

	mux := http.NewServeMux()
	wallet.NewHandler(walletSvc, log).Register(mux)
	sms.NewHandler(smsSvc, log).Register(mux)
	report.NewHandler(report.NewService(db), log).Register(mux)
	ratelimit.NewHandler(db, plans, log).Register(mux)
	mux.HandleFunc("GET /healthz", httpserver.Healthz)
	// Readiness checks Postgres only. The API never talks to Kafka: accepted
	// messages go to the outbox table, so during a Kafka outage the API keeps
	// accepting and must stay in the load balancer. Kafka health is the
	// relay's and dispatcher's concern (their lag shows it).
	mux.Handle("GET /readyz", httpserver.Readyz(map[string]httpserver.Check{
		"postgres": db.Ping,
	}))

	handler := httpserver.Chain(mux,
		httpserver.RequestID,
		httpserver.Recover(log),
		httpserver.AccessLog(log),
	)

	log.Info("api started")
	return httpserver.Run(ctx, cfg.HTTPAddr, handler, log, cfg.ShutdownTimeout)
}
