package sms

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		req     SendRequest
		wantErr bool
	}{
		{"valid normal", SendRequest{Recipient: "+989121234567", Body: "hello"}, false},
		{"valid express without plus", SendRequest{Recipient: "989121234567", Body: "hi", Priority: PriorityExpress}, false},
		{"160 latin chars fit", SendRequest{Recipient: "989121234567", Body: strings.Repeat("a", 160)}, false},
		{"161 latin chars too long", SendRequest{Recipient: "989121234567", Body: strings.Repeat("a", 161)}, true},
		{"70 persian chars fit", SendRequest{Recipient: "989121234567", Body: strings.Repeat("س", 70)}, false},
		{"71 persian chars too long", SendRequest{Recipient: "989121234567", Body: strings.Repeat("س", 71)}, true},
		{"empty body", SendRequest{Recipient: "989121234567", Body: ""}, true},
		{"recipient too short", SendRequest{Recipient: "1234567", Body: "x"}, true},
		{"recipient too long", SendRequest{Recipient: "1234567890123456", Body: "x"}, true},
		{"recipient with letters", SendRequest{Recipient: "98912abc4567", Body: "x"}, true},
		{"unknown priority", SendRequest{Recipient: "989121234567", Body: "x", Priority: "urgent"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.req.Normalize().Validate()
			if tt.wantErr && !errors.Is(err, ErrValidation) {
				t.Errorf("want ErrValidation, got %v", err)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("want no error, got %v", err)
			}
		})
	}
}

func TestNormalizeDefaultsToNormalPriority(t *testing.T) {
	if p := (SendRequest{}).Normalize().Priority; p != PriorityNormal {
		t.Errorf("default priority = %q, want %q", p, PriorityNormal)
	}
}

func TestHashIsStableAndContentSensitive(t *testing.T) {
	a := SendRequest{Recipient: "989121234567", Body: "hello", Priority: PriorityNormal}
	if a.Hash() != a.Hash() {
		t.Error("same request must hash the same")
	}
	b := a
	b.Body = "hello!"
	if a.Hash() == b.Hash() {
		t.Error("different body must hash differently")
	}
	c := a
	c.Priority = PriorityExpress
	if a.Hash() == c.Hash() {
		t.Error("different priority must hash differently")
	}
}

func TestIdempotencyKeyLength(t *testing.T) {
	if err := ValidateIdempotencyKey(strings.Repeat("k", MaxIdempotencyKeyLen)); err != nil {
		t.Errorf("key of max length rejected: %v", err)
	}
	if err := ValidateIdempotencyKey(strings.Repeat("k", MaxIdempotencyKeyLen+1)); !errors.Is(err, ErrValidation) {
		t.Errorf("too-long key: got %v, want ErrValidation", err)
	}
}

// Terminal states must never be a source of any transition.
func TestTerminalStatesHaveNoOutgoingTransitions(t *testing.T) {
	for _, to := range []Status{StatusQueued, StatusSending, StatusRetrying, StatusSent, StatusFailed} {
		from := allowedFrom(to)
		if slices.Contains(from, StatusSent) || slices.Contains(from, StatusFailed) {
			t.Errorf("transition to %q allowed from a terminal state: %v", to, from)
		}
	}
}
