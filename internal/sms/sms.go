package sms

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

// Domain errors, mapped to HTTP status codes by the handler.
var (
	// ErrValidation wraps every input problem (400).
	ErrValidation = errors.New("validation failed")
	// ErrIdempotencyConflict: the Idempotency-Key was already used with a
	// different request body (409).
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different request")
)

// Priority decides the topic and worker pool a message goes through.
type Priority string

const (
	PriorityNormal  Priority = "normal"
	PriorityExpress Priority = "express"
)

// Kafka topics per priority (see docs/adr/0002).
const (
	TopicExpress = "sms.express"
	TopicNormal  = "sms.normal"
)

// TopicFor returns the Kafka topic for a priority.
func TopicFor(p Priority) string {
	if p == PriorityExpress {
		return TopicExpress
	}
	return TopicNormal
}

// Status is the message lifecycle state:
//
//	queued -> sending -> sent
//	   |         |
//	   |         +----> retrying -> sending ...
//	   +--------------> failed (also from sending / retrying)
//
// sent and failed are terminal.
type Status string

const (
	StatusQueued   Status = "queued"
	StatusSending  Status = "sending"
	StatusRetrying Status = "retrying"
	StatusSent     Status = "sent"
	StatusFailed   Status = "failed"
)

// allowedFrom is the allow-list of source states for each target state.
// Every status UPDATE uses it in its WHERE clause, so a duplicate or late
// event (e.g. "fail" for a message that is already sent) matches 0 rows and
// changes nothing. Terminal states (sent, failed) appear in no list.
func allowedFrom(to Status) []Status {
	switch to {
	case StatusSending:
		return []Status{StatusQueued, StatusRetrying}
	case StatusRetrying:
		return []Status{StatusSending}
	case StatusSent:
		return []Status{StatusSending}
	case StatusFailed:
		return []Status{StatusQueued, StatusSending, StatusRetrying}
	default:
		return nil
	}
}

// Single-page limits. GSM-7 fits 160 characters per SMS; anything outside it
// (e.g. Persian) is sent as UCS-2, which fits 70.
const (
	maxGSMChars  = 160
	maxUCS2Chars = 70
)

// MaxIdempotencyKeyLen bounds the Idempotency-Key header.
const MaxIdempotencyKeyLen = 255

// SendRequest is what a customer asks to send.
type SendRequest struct {
	Recipient string
	Body      string
	Priority  Priority
}

// Normalize trims input and applies the default priority.
func (r SendRequest) Normalize() SendRequest {
	r.Recipient = strings.TrimSpace(r.Recipient)
	if r.Priority == "" {
		r.Priority = PriorityNormal
	}
	return r
}

// Validate checks a normalized request. Errors wrap ErrValidation.
func (r SendRequest) Validate() error {
	if !validRecipient(r.Recipient) {
		return fmt.Errorf("%w: recipient must be 8-15 digits, optionally starting with +", ErrValidation)
	}
	if r.Body == "" {
		return fmt.Errorf("%w: body must not be empty", ErrValidation)
	}
	if !fitsOnePage(r.Body) {
		return fmt.Errorf("%w: body must fit one SMS page (%d characters, or %d for non-Latin text)",
			ErrValidation, maxGSMChars, maxUCS2Chars)
	}
	if r.Priority != PriorityNormal && r.Priority != PriorityExpress {
		return fmt.Errorf("%w: priority must be %q or %q", ErrValidation, PriorityNormal, PriorityExpress)
	}
	return nil
}

// Hash fingerprints the request for idempotency: a retry with the same key
// must carry the same content. Marshalling a struct gives a fixed field
// order, so the hash is stable.
func (r SendRequest) Hash() string {
	b, _ := json.Marshal(struct {
		Recipient string   `json:"recipient"`
		Body      string   `json:"body"`
		Priority  Priority `json:"priority"`
	}{r.Recipient, r.Body, r.Priority}) // cannot fail for strings
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// ValidateIdempotencyKey checks the optional header. Empty means "no key".
func ValidateIdempotencyKey(key string) error {
	if len(key) > MaxIdempotencyKeyLen {
		return fmt.Errorf("%w: Idempotency-Key must be at most %d characters", ErrValidation, MaxIdempotencyKeyLen)
	}
	return nil
}

// Message is an accepted message as stored in the messages table.
type Message struct {
	ID         uuid.UUID
	CustomerID int64
	Recipient  string
	Body       string
	Priority   Priority
	Status     Status
	Cost       int64
	CreatedAt  time.Time
}

// validRecipient accepts an optional leading + and 8-15 digits (E.164 length).
func validRecipient(s string) bool {
	s = strings.TrimPrefix(s, "+")
	if len(s) < 8 || len(s) > 15 {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// fitsOnePage reports whether body fits a single SMS page.
// Approximation: printable ASCII (plus \n, \r) is treated as GSM-7. The few
// GSM extension characters such as { } [ ] ~ that cost 2 septets are counted
// as 1; exact GSM accounting belongs to the operator integration.
func fitsOnePage(body string) bool {
	n := utf8.RuneCountInString(body)
	if isGSMLike(body) {
		return n <= maxGSMChars
	}
	return n <= maxUCS2Chars
}

func isGSMLike(s string) bool {
	for _, c := range s {
		if (c < 0x20 || c > 0x7E) && c != '\n' && c != '\r' {
			return false
		}
	}
	return true
}
