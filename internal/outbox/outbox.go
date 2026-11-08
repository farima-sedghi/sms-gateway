package outbox

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Insert adds one event to the outbox inside the caller's transaction, so the
// event is stored if and only if the business change commits.
//
//   - topic: the Kafka topic the relay will publish to.
//   - key: the Kafka message key (message_id for SMS, see docs/adr/0002).
//   - payload: JSON, marshalled by the caller, so this package does not
//     depend on any business types.
func Insert(ctx context.Context, tx pgx.Tx, topic, key string, payload []byte) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO outbox (topic, key, payload) VALUES ($1, $2, $3)`,
		topic, key, payload,
	)
	if err != nil {
		return fmt.Errorf("insert outbox row: %w", err)
	}
	return nil
}
