package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptrace"
	"sync/atomic"
	"time"

	"github.com/sony/gobreaker"
)

// Config configures the operator client.
type Config struct {
	BaseURL string        // e.g. http://operator-mock:8090
	Timeout time.Duration // per Send call
	// Circuit breaker (decision 17).
	BreakerFailures uint32        // consecutive failures that open the breaker
	BreakerOpenFor  time.Duration // how long it stays open before a half-open trial
}

// Client sends SMS to the operator over HTTP. It is safe for concurrent use
// by many workers; they share one circuit breaker, because the operator is
// one system: if it is down, it is down for every lane.
type Client struct {
	cfg     Config
	http    *http.Client
	breaker *gobreaker.CircuitBreaker
}

// NewClient returns an operator client.
func NewClient(cfg Config, log *slog.Logger) *Client {
	// The default transport keeps only 2 idle connections per host; with
	// dozens of workers that means a new TCP connection for most requests.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 512
	transport.MaxIdleConnsPerHost = 256

	breaker := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "operator",
		MaxRequests: 1, // one trial request while half-open
		Timeout:     cfg.BreakerOpenFor,
		ReadyToTrip: func(c gobreaker.Counts) bool {
			return c.ConsecutiveFailures >= cfg.BreakerFailures
		},
		// What counts as an operator failure: 5xx, timeouts and transport
		// errors do. 4xx (including 429) do not: those are answers from a
		// healthy operator about one request.
		IsSuccessful: func(err error) bool {
			return err == nil || errors.Is(err, ErrPermanent) || errors.Is(err, errRateLimited)
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			log.Warn("circuit breaker state change", "breaker", name, "from", from.String(), "to", to.String())
		},
	})

	return &Client{
		cfg:     cfg,
		http:    &http.Client{Transport: transport}, // timeout comes from the per-call context
		breaker: breaker,
	}
}

type sendRequest struct {
	Ref  string `json:"ref"`
	To   string `json:"to"`
	Text string `json:"text"`
}

type sendResponse struct {
	OperatorMsgID string `json:"operator_msg_id"`
}

// Send hands one SMS to the operator and returns the operator's message id.
//
// ref is our message_id. The operator dedupes by it, so retrying with the
// same ref after an Unknown outcome never delivers twice.
//
// Errors wrap ErrPermanent, ErrTemporary or ErrUnknown (see operator.go).
// While the circuit breaker is open, Send returns ErrCircuitOpen at once.
func (c *Client) Send(ctx context.Context, ref, to, text string) (string, error) {
	res, err := c.breaker.Execute(func() (any, error) {
		return c.send(ctx, ref, to, text)
	})
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		return "", ErrCircuitOpen
	}
	if err != nil {
		return "", err
	}
	return res.(string), nil
}

func (c *Client) send(ctx context.Context, ref, to, text string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	body, err := json.Marshal(sendRequest{Ref: ref, To: to, Text: text})
	if err != nil {
		return "", fmt.Errorf("%w: marshal request: %v", ErrPermanent, err)
	}

	// httptrace tells us whether the request was fully written to the
	// connection. A failure before that (connection refused, DNS) means the
	// operator never saw it: Temporary. A failure after it (timeout, reset)
	// means it may have been processed: Unknown.
	var wrote atomic.Bool
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			if info.Err == nil {
				wrote.Store(true)
			}
		},
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.BaseURL+"/send", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("%w: build request: %v", ErrPermanent, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		if wrote.Load() {
			return "", fmt.Errorf("%w: %v", ErrUnknown, err)
		}
		return "", fmt.Errorf("%w: %v", ErrTemporary, err)
	}
	defer resp.Body.Close()
	respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		var out sendResponse
		if readErr != nil || json.Unmarshal(respBody, &out) != nil || out.OperatorMsgID == "" {
			// The operator said OK but we can't read the id: it was
			// probably delivered, so this is Unknown, not a failure.
			return "", fmt.Errorf("%w: unreadable success response (status %d)", ErrUnknown, resp.StatusCode)
		}
		return out.OperatorMsgID, nil
	case resp.StatusCode == http.StatusTooManyRequests:
		return "", fmt.Errorf("%w: %w: status 429", ErrTemporary, errRateLimited)
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		return "", fmt.Errorf("%w: status %d: %s", ErrPermanent, resp.StatusCode, truncate(respBody))
	case resp.StatusCode >= 500:
		return "", fmt.Errorf("%w: status %d: %s", ErrTemporary, resp.StatusCode, truncate(respBody))
	default:
		// 1xx/3xx: not part of the operator contract; we can't tell what happened.
		return "", fmt.Errorf("%w: unexpected status %d", ErrUnknown, resp.StatusCode)
	}
}

func truncate(b []byte) string {
	const max = 200
	if len(b) > max {
		return string(b[:max]) + "..."
	}
	return string(b)
}
