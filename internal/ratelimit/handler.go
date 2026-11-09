package ratelimit

import (
	"log/slog"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/farima-sedghi/sms-gateway/internal/platform/httpserver"
)

// Handler exposes plan management over HTTP. Like the rest of the API it
// has no auth (out of scope); in production this is an admin-only route.
type Handler struct {
	pool  *pgxpool.Pool
	cache *PlanCache
	log   *slog.Logger
}

// NewHandler returns the plan admin handler.
func NewHandler(pool *pgxpool.Pool, cache *PlanCache, log *slog.Logger) *Handler {
	return &Handler{pool: pool, cache: cache, log: log}
}

// Register adds the routes to mux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("PUT /customers/{id}/limits", h.put)
}

type planResponse struct {
	CustomerID int64 `json:"customer_id"`
	Plan
}

// put: PUT /customers/{id}/limits {"normal_rps":2000,"express_rps":100,"burst":4000}
func (h *Handler) put(w http.ResponseWriter, r *http.Request) {
	customerID, ok := httpserver.PathInt64(w, r, "id")
	if !ok {
		return
	}
	var p Plan
	if err := httpserver.DecodeJSON(w, r, &p); err != nil {
		httpserver.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := p.Validate(); err != nil {
		httpserver.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := upsertPlan(r.Context(), h.pool, customerID, p); err != nil {
		h.log.ErrorContext(r.Context(), "set plan failed", "customer_id", customerID, "err", err)
		httpserver.WriteError(w, http.StatusInternalServerError, "internal error")
		return
	}
	// Apply on this instance now; the others pick it up on their next refresh.
	h.cache.Set(customerID, p)
	httpserver.WriteJSON(w, http.StatusOK, planResponse{CustomerID: customerID, Plan: p})
}
