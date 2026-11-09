package operatormock

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/farima-sedghi/sms-gateway/internal/operator"
)

// newServer runs the mock behind httptest and returns a real operator
// client pointed at it, so these tests also exercise the client contract.
func newServer(t *testing.T, cfg Config, clientTimeout time.Duration) (*Handler, *operator.Client) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := New(cfg, log)
	mux := http.NewServeMux()
	h.Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := operator.NewClient(operator.Config{
		BaseURL: srv.URL, Timeout: clientTimeout, BreakerFailures: 100, BreakerOpenFor: time.Minute,
	}, log)
	return h, client
}

func fixed(v float64) func() float64 { return func() float64 { return v } }

func TestSameRefIsSentOnce(t *testing.T) {
	h, c := newServer(t, Config{Rand: fixed(0.99)}, time.Second)
	ctx := context.Background()

	id1, err := c.Send(ctx, "msg-1", "989121234567", "hi")
	if err != nil {
		t.Fatal(err)
	}
	id2, err := c.Send(ctx, "msg-1", "989121234567", "hi")
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Errorf("same ref got different ids: %s, %s", id1, id2)
	}
	if s := h.Stats(); s.Sends != 1 || s.Duplicates != 1 {
		t.Errorf("stats = %+v, want 1 send, 1 duplicate", s)
	}
}

func TestFailureRates(t *testing.T) {
	_, temp := newServer(t, Config{TempFailureRate: 1, Rand: fixed(0.5)}, time.Second)
	if _, err := temp.Send(context.Background(), "a", "989121234567", "hi"); !errors.Is(err, operator.ErrTemporary) {
		t.Errorf("temp rate 1: error = %v, want ErrTemporary", err)
	}
	_, perm := newServer(t, Config{PermFailureRate: 1, Rand: fixed(0.5)}, time.Second)
	if _, err := perm.Send(context.Background(), "b", "989121234567", "hi"); !errors.Is(err, operator.ErrPermanent) {
		t.Errorf("perm rate 1: error = %v, want ErrPermanent", err)
	}
}

// A timeout is "delivered, answer lost": the client sees Unknown, and a
// retry with the same ref is answered as a duplicate, not sent again.
func TestTimeoutDeliversThenRetryIsDuplicate(t *testing.T) {
	h, c := newServer(t, Config{TimeoutRate: 1, TimeoutDelay: 2 * time.Second, Rand: fixed(0.5)}, 100*time.Millisecond)
	ctx := context.Background()

	if _, err := c.Send(ctx, "msg-1", "989121234567", "hi"); !errors.Is(err, operator.ErrUnknown) {
		t.Fatalf("first send: error = %v, want ErrUnknown", err)
	}
	if _, err := c.Send(ctx, "msg-1", "989121234567", "hi"); err != nil {
		t.Fatalf("retry: %v, want success from the dedupe map", err)
	}
	if s := h.Stats(); s.Sends != 1 || s.Duplicates != 1 {
		t.Errorf("stats = %+v, want 1 send, 1 duplicate", s)
	}
}
