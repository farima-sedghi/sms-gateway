// Command dispatcher consumes sms.express, sms.normal and sms.retry and hands
// messages to the operator. Each topic is its own lane: own consumer group,
// own worker pool, so express never waits for normal traffic.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/farima-sedghi/sms-gateway/internal/dispatch"
	"github.com/farima-sedghi/sms-gateway/internal/operator"
	"github.com/farima-sedghi/sms-gateway/internal/outbox"
	"github.com/farima-sedghi/sms-gateway/internal/platform/config"
	"github.com/farima-sedghi/sms-gateway/internal/platform/kafka"
	"github.com/farima-sedghi/sms-gateway/internal/platform/logging"
	"github.com/farima-sedghi/sms-gateway/internal/platform/postgres"
	"github.com/farima-sedghi/sms-gateway/internal/sms"
)

const serviceName = "dispatcher"

// Topic names (decision 6). Express and normal come from the sms module,
// which produces them; retry and DLQ are the dispatcher's own.
const (
	topicRetry = "sms.retry"
	topicDLQ   = "sms.dlq"
)

type laneConfig struct {
	Workers   int
	BatchSize int
}

type appConfig struct {
	DatabaseURL     string
	KafkaBrokers    []string
	LogLevel        slog.Level
	OperatorURL     string
	OperatorTimeout time.Duration
	MaxAttempts     int
	RetryBaseDelay  time.Duration
	RetryMaxDelay   time.Duration
	Express         laneConfig
	Normal          laneConfig
	Retry           laneConfig
}

func loadConfig() (appConfig, error) {
	r := config.NewReader(os.LookupEnv)
	cfg := appConfig{
		DatabaseURL:     r.Required("DATABASE_URL"),
		KafkaBrokers:    r.RequiredList("KAFKA_BROKERS"),
		LogLevel:        r.LogLevel("LOG_LEVEL", slog.LevelInfo),
		OperatorURL:     r.Required("OPERATOR_URL"),
		OperatorTimeout: r.Duration("OPERATOR_TIMEOUT", 5*time.Second),
		MaxAttempts:     r.Int("MAX_ATTEMPTS", 5),
		RetryBaseDelay:  r.Duration("RETRY_BASE_DELAY", time.Second),
		RetryMaxDelay:   r.Duration("RETRY_MAX_DELAY", 30*time.Second),
		Express:         laneConfig{r.Int("EXPRESS_WORKERS", 32), r.Int("EXPRESS_BATCH_SIZE", 64)},
		Normal:          laneConfig{r.Int("NORMAL_WORKERS", 64), r.Int("NORMAL_BATCH_SIZE", 256)},
		Retry:           laneConfig{r.Int("RETRY_WORKERS", 16), r.Int("RETRY_BATCH_SIZE", 64)},
	}
	if err := r.Err(); err != nil {
		return cfg, err
	}
	for name, l := range map[string]laneConfig{"EXPRESS": cfg.Express, "NORMAL": cfg.Normal, "RETRY": cfg.Retry} {
		if l.Workers <= 0 || l.BatchSize <= 0 {
			return cfg, fmt.Errorf("%s_WORKERS and %s_BATCH_SIZE must be positive", name, name)
		}
	}
	if cfg.MaxAttempts <= 0 {
		return cfg, fmt.Errorf("MAX_ATTEMPTS must be positive, got %d", cfg.MaxAttempts)
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
	// Defers run in reverse order (LIFO): this one is registered first, so it runs last.
	defer log.Info(serviceName + " stopped")

	db, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()

	// One producer for retry and DLQ events (acks=all, idempotent).
	producer, err := kafka.NewClient(ctx, cfg.KafkaBrokers, outbox.ProducerOptions(false)...)
	if err != nil {
		return err
	}
	defer producer.Close()

	// One operator client = one circuit breaker, shared by all lanes: the
	// operator is one system; when it is down, it is down for everyone.
	opClient := operator.NewClient(operator.Config{
		BaseURL:         cfg.OperatorURL,
		Timeout:         cfg.OperatorTimeout,
		BreakerFailures: 5,
		BreakerOpenFor:  30 * time.Second,
	}, log)

	proc := dispatch.NewProcessor(sms.NewLifecycle(db), opClient, producer, dispatch.Config{
		RetryTopic:     topicRetry,
		DLQTopic:       topicDLQ,
		MaxAttempts:    cfg.MaxAttempts,
		RetryBaseDelay: cfg.RetryBaseDelay,
		RetryMaxDelay:  cfg.RetryMaxDelay,
		BreakerWait:    time.Second,
		DBTimeout:      10 * time.Second,
	}, log)

	lanes := []dispatch.Lane{
		{Name: "express", Topic: sms.TopicExpress, Group: "dispatcher-express", Workers: cfg.Express.Workers, BatchSize: cfg.Express.BatchSize},
		{Name: "normal", Topic: sms.TopicNormal, Group: "dispatcher-normal", Workers: cfg.Normal.Workers, BatchSize: cfg.Normal.BatchSize},
		{Name: "retry", Topic: topicRetry, Group: "dispatcher-retry", Workers: cfg.Retry.Workers, BatchSize: cfg.Retry.BatchSize},
	}

	// Each lane gets its own Kafka client (= own consumer group membership
	// and fetch loop), so a normal backlog never delays express fetches.
	var consumers []*dispatch.Consumer
	for _, lane := range lanes {
		client, err := kafka.NewClient(ctx, cfg.KafkaBrokers, dispatch.ConsumerOptions(lane)...)
		if err != nil {
			return fmt.Errorf("lane %s: %w", lane.Name, err)
		}
		defer client.Close() // leaves the group cleanly on shutdown
		consumers = append(consumers, dispatch.NewConsumer(client, lane, proc, log))
	}

	log.Info(serviceName+" started", "operator_url", cfg.OperatorURL, "max_attempts", cfg.MaxAttempts)
	var wg sync.WaitGroup
	for _, c := range consumers {
		wg.Add(1)
		go func(c *dispatch.Consumer) {
			defer wg.Done()
			c.Run(ctx)
		}(c)
	}
	wg.Wait() // returns after SIGTERM, once every lane finished its batch
	log.Info(serviceName + " shutting down")
	return nil
}
