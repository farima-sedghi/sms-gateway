package operator

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newClient(url string, timeout, openFor time.Duration) *Client {
	return NewClient(Config{
		BaseURL:         url,
		Timeout:         timeout,
		BreakerFailures: 5,
		BreakerOpenFor:  openFor,
	}, quietLogger())
}

// server answers every request with status (and a JSON id on 2xx), counting hits.
func server(t *testing.T, status int) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(status)
		if status < 300 {
			_, _ = w.Write([]byte(`{"operator_msg_id":"op-42"}`))
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func TestSendClassifiesResponses(t *testing.T) {
	cases := []struct {
		name   string
		status int
		want   error // nil = success
	}{
		{"200 ok", 200, nil},
		{"400 permanent", 400, ErrPermanent},
		{"404 permanent", 404, ErrPermanent},
		{"429 temporary", 429, ErrTemporary},
		{"500 temporary", 500, ErrTemporary},
		{"503 temporary", 503, ErrTemporary},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv, _ := server(t, c.status)
			id, err := newClient(srv.URL, time.Second, time.Minute).Send(context.Background(), "ref-1", "989121234567", "hi")
			if c.want == nil {
				if err != nil || id != "op-42" {
					t.Fatalf("got (%q, %v), want (op-42, nil)", id, err)
				}
				return
			}
			if !errors.Is(err, c.want) {
				t.Fatalf("error = %v, want %v", err, c.want)
			}
		})
	}
}

// The request reached the server but the answer did not come back in time:
// the operator may have delivered it.
func TestTimeoutAfterRequestWrittenIsUnknown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(2 * time.Second):
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)

	_, err := newClient(srv.URL, 100*time.Millisecond, time.Minute).Send(context.Background(), "ref-1", "989121234567", "hi")
	if !errors.Is(err, ErrUnknown) {
		t.Fatalf("error = %v, want ErrUnknown", err)
	}
}

// Nothing listening: the request was never written, so it is safe to retry.
func TestConnectionRefusedIsTemporary(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // port is now closed

	_, err = newClient("http://"+addr, time.Second, time.Minute).Send(context.Background(), "ref-1", "989121234567", "hi")
	if !errors.Is(err, ErrTemporary) || errors.Is(err, ErrUnknown) {
		t.Fatalf("error = %v, want ErrTemporary (not Unknown)", err)
	}
}

func TestBreakerOpensAfterFiveConsecutiveFailures(t *testing.T) {
	srv, hits := server(t, 503)
	c := newClient(srv.URL, time.Second, time.Minute)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if _, err := c.Send(ctx, "ref", "989121234567", "hi"); !errors.Is(err, ErrTemporary) || errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("call %d: error = %v, want a plain ErrTemporary", i+1, err)
		}
	}
	_, err := c.Send(ctx, "ref", "989121234567", "hi")
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("6th call: error = %v, want ErrCircuitOpen", err)
	}
	if !errors.Is(err, ErrTemporary) {
		t.Error("ErrCircuitOpen must also be an ErrTemporary")
	}
	if hits.Load() != 5 {
		t.Errorf("server hits = %d, want 5 (open breaker must not call the operator)", hits.Load())
	}
}

func TestClientErrorsDoNotTripBreaker(t *testing.T) {
	for _, status := range []int{400, 429} {
		srv, hits := server(t, status)
		c := newClient(srv.URL, time.Second, time.Minute)
		for i := 0; i < 10; i++ {
			if _, err := c.Send(context.Background(), "ref", "989121234567", "hi"); errors.Is(err, ErrCircuitOpen) {
				t.Fatalf("status %d: breaker opened after %d calls", status, i)
			}
		}
		if hits.Load() != 10 {
			t.Errorf("status %d: server hits = %d, want 10", status, hits.Load())
		}
	}
}

func TestBreakerHalfOpenTrialClosesIt(t *testing.T) {
	var healthy atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !healthy.Load() {
			w.WriteHeader(503)
			return
		}
		_, _ = w.Write([]byte(`{"operator_msg_id":"op-1"}`))
	}))
	t.Cleanup(srv.Close)

	c := newClient(srv.URL, time.Second, 200*time.Millisecond)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_, _ = c.Send(ctx, "ref", "989121234567", "hi")
	}
	if _, err := c.Send(ctx, "ref", "989121234567", "hi"); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("breaker not open: %v", err)
	}

	healthy.Store(true)
	time.Sleep(250 * time.Millisecond) // past BreakerOpenFor -> half-open
	if _, err := c.Send(ctx, "ref", "989121234567", "hi"); err != nil {
		t.Fatalf("half-open trial failed: %v", err)
	}
	if _, err := c.Send(ctx, "ref", "989121234567", "hi"); err != nil {
		t.Fatalf("breaker did not close after a successful trial: %v", err)
	}
}
