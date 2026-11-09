package sms

import (
	"time"

	"github.com/google/uuid"
)

// Event is the Kafka message for one SMS. It is the contract between the
// producers (sms.Accept via the outbox, and the dispatcher for retries/DLQ)
// and the dispatcher. It carries everything needed to send, so the
// dispatcher never has to read the message row first.
type Event struct {
	MessageID  uuid.UUID `json:"message_id"`
	CustomerID int64     `json:"customer_id"`
	Recipient  string    `json:"recipient"`
	Body       string    `json:"body"`
	Priority   Priority  `json:"priority"`
	Cost       int64     `json:"cost"`
	// CreatedAt is part of the message's primary key (id, created_at), so
	// the dispatcher's status updates hit one partition.
	CreatedAt time.Time `json:"created_at"`
	// ExpiresAt is set for express messages only: after it, do not send.
	ExpiresAt *time.Time `json:"expires_at,omitempty"`

	// Retry fields, set by the dispatcher.

	// Attempt is the number of attempts that already failed (0 on first delivery).
	Attempt int `json:"attempt"`
	// NextAttemptAt: the retry consumer waits until this time before sending.
	NextAttemptAt *time.Time `json:"next_attempt_at,omitempty"`
	// Uncertain is true once an attempt ended with an Unknown outcome (the
	// operator may have delivered it). A later definitive answer clears it.
	Uncertain bool   `json:"uncertain,omitempty"`
	LastError string `json:"last_error,omitempty"`
}

// Expired reports whether an express message passed its deadline at now.
func (e Event) Expired(now time.Time) bool {
	return e.ExpiresAt != nil && now.After(*e.ExpiresAt)
}
