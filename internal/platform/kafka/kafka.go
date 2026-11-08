// Package kafka creates franz-go clients and provides a readiness check.
package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// NewClient creates a franz-go client and checks that a broker answers.
// Callers add role-specific options (consumer group, topics, producer
// settings) through opts. Those come in later steps.
func NewClient(ctx context.Context, brokers []string, opts ...kgo.Opt) (*kgo.Client, error) {
	all := append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, opts...)
	cl, err := kgo.NewClient(all...)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := cl.Ping(pingCtx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("ping kafka: %w", err)
	}
	return cl, nil
}

// Ping is a readiness check. It asks the cluster for broker metadata, which
// needs a live connection to at least one broker.
func Ping(ctx context.Context, cl *kgo.Client) error {
	brokers, err := kadm.NewClient(cl).ListBrokers(ctx)
	if err != nil {
		return err
	}
	if len(brokers) == 0 {
		return fmt.Errorf("no kafka brokers available")
	}
	return nil
}
