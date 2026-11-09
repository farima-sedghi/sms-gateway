// Command outbox-relay publishes outbox rows to Kafka. It runs one
// independent lane per topic (express, normal), so express never waits
// behind a normal backlog. Several replicas can run at once (SKIP LOCKED).
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/farima-sedghi/sms-gateway/internal/outbox"
	"github.com/farima-sedghi/sms-gateway/internal/platform/config"
	"github.com/farima-sedghi/sms-gateway/internal/platform/kafka"
	"github.com/farima-sedghi/sms-gateway/internal/platform/logging"
	"github.com/farima-sedghi/sms-gateway/internal/platform/postgres"
	"github.com/farima-sedghi/sms-gateway/internal/sms"
)

const serviceName = "outbox-relay"

type appConfig struct {
	DatabaseURL     string
	KafkaBrokers    []string
	LogLevel        slog.Level
	BatchSize       int
	ExpressIdle     time.Duration
	NormalIdle      time.Duration
	Retention       time.Duration
	CleanupInterval time.Duration
	LagInterval     time.Duration
}

func loadConfig() (appConfig, error) {
	r := config.NewReader(os.LookupEnv)
	cfg := appConfig{
		DatabaseURL:     r.Required("DATABASE_URL"),
		KafkaBrokers:    r.RequiredList("KAFKA_BROKERS"),
		LogLevel:        r.LogLevel("LOG_LEVEL", slog.LevelInfo),
		BatchSize:       r.Int("OUTBOX_BATCH_SIZE", 500),
		ExpressIdle:     r.Duration("OUTBOX_EXPRESS_IDLE", 20*time.Millisecond),
		NormalIdle:      r.Duration("OUTBOX_NORMAL_IDLE", 100*time.Millisecond),
		Retention:       r.Duration("OUTBOX_RETENTION", time.Hour),
		CleanupInterval: r.Duration("OUTBOX_CLEANUP_INTERVAL", time.Minute),
		LagInterval:     r.Duration("OUTBOX_LAG_INTERVAL", 10*time.Second),
	}
	if err := r.Err(); err != nil {
		return cfg, err
	}
	if cfg.BatchSize <= 0 {
		return cfg, fmt.Errorf("OUTBOX_BATCH_SIZE must be positive, got %d", cfg.BatchSize)
	}
	return cfg, nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, serviceName+":", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	log := logging.New(os.Stdout, serviceName, cfg.LogLevel)
	// Defers run in reverse order (LIFO), like nested using blocks in C#.
	// This one is registered first, so it runs last.
	defer log.Info(serviceName + " stopped")

	db, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()

	// One Kafka client per lane: clients share buffers and broker
	// connections, so a big normal batch must not sit in front of express.
	expressProducer, err := kafka.NewClient(ctx, cfg.KafkaBrokers, outbox.ProducerOptions(true)...)
	if err != nil {
		return err
	}
	defer expressProducer.Close()

	normalProducer, err := kafka.NewClient(ctx, cfg.KafkaBrokers, outbox.ProducerOptions(false)...)
	if err != nil {
		return err
	}
	defer normalProducer.Close()

	relay := outbox.NewRelay(db, log,
		outbox.Config{
			Retention:       cfg.Retention,
			CleanupInterval: cfg.CleanupInterval,
			LagInterval:     cfg.LagInterval,
		},
		lane(sms.TopicExpress, cfg.BatchSize, cfg.ExpressIdle, expressProducer),
		lane(sms.TopicNormal, cfg.BatchSize, cfg.NormalIdle, normalProducer),
	)

	log.Info(serviceName + " started")
	relay.Run(ctx) // blocks until SIGINT/SIGTERM; in-flight batches finish first
	log.Info(serviceName + " shutting down")
	return nil
}

func lane(topic string, batchSize int, idle time.Duration, producer *kgo.Client) outbox.Lane {
	return outbox.Lane{Topic: topic, BatchSize: batchSize, IdleInterval: idle, Producer: producer}
}
