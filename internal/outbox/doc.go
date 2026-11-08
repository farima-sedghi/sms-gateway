// Package outbox owns the transactional outbox.
//
//   - Enqueue: writers (sms) call it inside their own transaction, so the
//     outbox row commits together with the business change.
//   - Relay: polls unsent rows with SELECT ... FOR UPDATE SKIP LOCKED,
//     publishes them to Kafka keyed by message_id, and marks them sent.
//
// Delivery is at-least-once: a crash between publish and mark-sent republishes
// the row, so consumers must be idempotent (see dispatch). SKIP LOCKED lets
// several relay instances run in parallel without double-claiming rows.
//
// Owns tables: outbox.
//
// Dependencies: platform.
package outbox
