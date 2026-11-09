// Package operatormock is a fake SMS operator for local runs, load tests and
// integration tests (via httptest). It behaves like a real operator in the
// ways our design relies on:
//
//   - It dedupes by ref: the same ref returns the same operator_msg_id and
//     is never "sent" twice.
//   - It fails in the three ways the client must classify: 5xx (temporary),
//     4xx (permanent), and "delivered but the response never arrives"
//     (timeout -> unknown).
package operatormock

import (
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/farima-sedghi/sms-gateway/internal/platform/httpserver"
)

// Config sets the simulated behaviour. The three rates are probabilities in
// [0, 1] and must add up to at most 1; the rest succeeds.
type Config struct {
	Latency         time.Duration // time to "send" a successful message
	TempFailureRate float64       // -> 503
	PermFailureRate float64       // -> 400
	TimeoutRate     float64       // -> delivered, then hang for TimeoutDelay
	TimeoutDelay    time.Duration
	// Rand returns a number in [0, 1). Nil means math/rand. Tests inject a
	// fixed value to force an outcome.
	Rand func() float64
}

// Handler is the fake operator's HTTP API.
type Handler struct {
	cfg Config
	log *slog.Logger

	mu        sync.Mutex
	delivered map[string]string // ref -> operator_msg_id (grows unbounded: fine for a mock)

	sends      atomic.Int64 // messages actually "sent" (once per ref)
	duplicates atomic.Int64 // requests answered from the dedupe map
}

// New returns a fake operator.
func New(cfg Config, log *slog.Logger) *Handler {
	if cfg.Rand == nil {
		cfg.Rand = rand.Float64
	}
	return &Handler{cfg: cfg, log: log, delivered: make(map[string]string)}
}

// Register adds the routes to mux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /send", h.send)
	mux.HandleFunc("GET /stats", h.stats)
}

// Stats is the GET /stats body.
type Stats struct {
	Sends      int64 `json:"sends"`
	Duplicates int64 `json:"duplicates"`
}

// Stats returns the counters (also used directly by tests).
func (h *Handler) Stats() Stats {
	return Stats{Sends: h.sends.Load(), Duplicates: h.duplicates.Load()}
}

type sendRequest struct {
	Ref  string `json:"ref"`
	To   string `json:"to"`
	Text string `json:"text"`
}

type sendResponse struct {
	OperatorMsgID string `json:"operator_msg_id"`
	Duplicate     bool   `json:"duplicate,omitempty"`
}

func (h *Handler) send(w http.ResponseWriter, r *http.Request) {
	var req sendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Ref == "" || req.To == "" {
		httpserver.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "ref, to and text are required"})
		return
	}

	// Already delivered? Answer from the dedupe map, never "send" again.
	if id, ok := h.lookup(req.Ref); ok {
		h.duplicates.Add(1)
		httpserver.WriteJSON(w, http.StatusOK, sendResponse{OperatorMsgID: id, Duplicate: true})
		return
	}

	roll := h.cfg.Rand()
	temp := h.cfg.TempFailureRate
	perm := temp + h.cfg.PermFailureRate
	timeout := perm + h.cfg.TimeoutRate

	switch {
	case roll < temp:
		httpserver.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "temporarily unavailable"})
	case roll < perm:
		httpserver.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "recipient rejected"})
	case roll < timeout:
		// Delivered, but the answer is lost: the client times out.
		id, dup := h.deliver(req.Ref)
		h.log.Info("operator-mock: delivered, response delayed", "ref", req.Ref, "duplicate", dup)
		h.wait(r, h.cfg.TimeoutDelay)
		httpserver.WriteJSON(w, http.StatusOK, sendResponse{OperatorMsgID: id, Duplicate: dup})
	default:
		h.wait(r, h.cfg.Latency)
		id, dup := h.deliver(req.Ref)
		httpserver.WriteJSON(w, http.StatusOK, sendResponse{OperatorMsgID: id, Duplicate: dup})
	}
}

func (h *Handler) lookup(ref string) (string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	id, ok := h.delivered[ref]
	return id, ok
}

// deliver records ref as sent, exactly once. Two concurrent first requests
// for the same ref both reach here; the second one becomes a duplicate.
func (h *Handler) deliver(ref string) (id string, duplicate bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if id, ok := h.delivered[ref]; ok {
		h.duplicates.Add(1)
		return id, true
	}
	id = "op-" + uuid.NewString()
	h.delivered[ref] = id
	h.sends.Add(1)
	return id, false
}

func (h *Handler) stats(w http.ResponseWriter, _ *http.Request) {
	httpserver.WriteJSON(w, http.StatusOK, h.Stats())
}

// wait sleeps for d, or until the client goes away.
func (h *Handler) wait(r *http.Request, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-r.Context().Done():
	}
}
