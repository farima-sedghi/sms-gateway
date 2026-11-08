// Command dispatcher consumes sms.express, sms.normal and sms.retry and hands
// messages to the operator. Right now it only connects, logs and waits for
// shutdown. Consumers come in a later step.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/farima-sedghi/sms-gateway/internal/platform/config"
	"github.com/farima-sedghi/sms-gateway/internal/platform/kafka"
	"github.com/farima-sedghi/sms-gateway/internal/platform/logging"
	"github.com/farima-sedghi/sms-gateway/internal/platform/postgres"
)

const serviceName = "dispatcher"

type appConfig struct {
	DatabaseURL  string
	KafkaBrokers []string
	LogLevel     slog.Level
}

func loadConfig() (appConfig, error) {
	r := config.NewReader(os.LookupEnv)
	cfg := appConfig{
		DatabaseURL:  r.Required("DATABASE_URL"),
		KafkaBrokers: r.RequiredList("KAFKA_BROKERS"),
		LogLevel:     r.LogLevel("LOG_LEVEL", slog.LevelInfo),
	}
	return cfg, r.Err()
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
	// This one is registered first, so it runs last, after Kafka and Postgres
	// are closed.
	defer log.Info(serviceName + " stopped")

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

	log.Info(serviceName + " started")
	<-ctx.Done() // blocks until SIGINT/SIGTERM
	log.Info(serviceName + " shutting down")
	return nil
}
