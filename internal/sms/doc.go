// Package sms accepts send requests. It validates and prices them, then in ONE
// transaction it debits the wallet, inserts the message row and inserts an
// outbox row. The relay later publishes that outbox row to Kafka.
//
// Owns tables: messages. Status transitions are exported functions here, so
// dispatch changes status without touching the table directly.
//
// Idempotency: POST /messages takes an Idempotency-Key header, and the unique
// constraint (customer_id, idempotency_key) stops client retries from charging
// twice.
//
// Dependencies: wallet and outbox. Both receive this package's pgx.Tx, so the
// debit, the message and the outbox row commit atomically (see docs/adr/0001).
// Also ratelimit and platform.
package sms
