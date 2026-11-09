package report

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/farima-sedghi/sms-gateway/internal/platform/httpserver"
	"github.com/farima-sedghi/sms-gateway/internal/sms"
)

// Handler exposes reports over HTTP: parse, call the service, map errors.
type Handler struct {
	svc *Service
	log *slog.Logger
}

// NewHandler returns the report HTTP handler.
func NewHandler(svc *Service, log *slog.Logger) *Handler {
	return &Handler{svc: svc, log: log}
}

// Register adds the report routes to mux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /customers/{id}/messages", h.list)
	mux.HandleFunc("GET /customers/{id}/messages/{message_id}", h.get)
}

// list: GET /customers/{id}/messages?from=&to=&status=&limit=&cursor=
func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	customerID, ok := httpserver.PathInt64(w, r, "id")
	if !ok {
		return
	}
	q, err := parseQuery(customerID, r)
	if err != nil {
		httpserver.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	page, err := h.svc.List(r.Context(), q)
	switch {
	case errors.Is(err, ErrInvalidQuery):
		httpserver.WriteError(w, http.StatusBadRequest, err.Error())
	case err != nil:
		h.log.ErrorContext(r.Context(), "list messages failed", "customer_id", customerID, "err", err)
		httpserver.WriteError(w, http.StatusInternalServerError, "internal error")
	default:
		httpserver.WriteJSON(w, http.StatusOK, page)
	}
}

// get: GET /customers/{id}/messages/{message_id}
func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	customerID, ok := httpserver.PathInt64(w, r, "id")
	if !ok {
		return
	}
	id, err := uuid.Parse(r.PathValue("message_id"))
	if err != nil {
		httpserver.WriteError(w, http.StatusBadRequest, "message_id must be a UUID")
		return
	}

	m, err := h.svc.Get(r.Context(), customerID, id)
	switch {
	case errors.Is(err, ErrNotFound):
		httpserver.WriteError(w, http.StatusNotFound, err.Error())
	case err != nil:
		h.log.ErrorContext(r.Context(), "get message failed", "customer_id", customerID, "message_id", id, "err", err)
		httpserver.WriteError(w, http.StatusInternalServerError, "internal error")
	default:
		httpserver.WriteJSON(w, http.StatusOK, m)
	}
}

// parseQuery reads the query string. Errors wrap ErrInvalidQuery.
func parseQuery(customerID int64, r *http.Request) (Query, error) {
	v := r.URL.Query()
	q := Query{
		CustomerID: customerID,
		Status:     sms.Status(v.Get("status")),
		Cursor:     v.Get("cursor"),
	}
	var err error
	if q.From, err = parseTime(v.Get("from"), "from"); err != nil {
		return q, err
	}
	if q.To, err = parseTime(v.Get("to"), "to"); err != nil {
		return q, err
	}
	if s := v.Get("limit"); s != "" {
		if q.Limit, err = strconv.Atoi(s); err != nil || q.Limit < 1 {
			return q, fmt.Errorf("%w: limit must be between 1 and %d", ErrInvalidQuery, MaxLimit)
		}
	}
	return q, nil
}

func parseTime(s, name string) (time.Time, error) {
	if s == "" {
		return time.Time{}, fmt.Errorf("%w: %s is required (RFC3339, e.g. 2026-11-09T00:00:00Z)", ErrInvalidQuery, name)
	}
	t, err := time.Parse(time.RFC3339, s) // also accepts fractional seconds
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: %s must be RFC3339, e.g. 2026-11-09T00:00:00Z", ErrInvalidQuery, name)
	}
	return t, nil
}
