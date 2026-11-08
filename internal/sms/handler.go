package sms

import (
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/farima-sedghi/sms-gateway/internal/platform/httpserver"
	"github.com/farima-sedghi/sms-gateway/internal/wallet"
)

// Handler exposes message acceptance over HTTP. It parses input, calls the
// service and maps domain errors to status codes. No business logic.
type Handler struct {
	svc *Service
	log *slog.Logger
}

// NewHandler returns the sms HTTP handler.
func NewHandler(svc *Service, log *slog.Logger) *Handler {
	return &Handler{svc: svc, log: log}
}

// Register adds the sms routes to mux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /messages", h.send)
}

type sendRequest struct {
	CustomerID int64    `json:"customer_id"`
	Recipient  string   `json:"recipient"`
	Body       string   `json:"body"`
	Priority   Priority `json:"priority"`
}

type sendResponse struct {
	MessageID uuid.UUID `json:"message_id"`
	Status    Status    `json:"status"`
	Priority  Priority  `json:"priority"`
	Cost      int64     `json:"cost"`
	CreatedAt time.Time `json:"created_at"`
}

func (h *Handler) send(w http.ResponseWriter, r *http.Request) {
	var req sendRequest
	if err := httpserver.DecodeJSON(w, r, &req); err != nil {
		httpserver.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	res, err := h.svc.Accept(r.Context(), req.CustomerID, SendRequest{
		Recipient: req.Recipient,
		Body:      req.Body,
		Priority:  req.Priority,
	}, r.Header.Get("Idempotency-Key"))

	switch {
	case errors.Is(err, ErrValidation):
		httpserver.WriteError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, wallet.ErrInsufficientBalance):
		httpserver.WriteError(w, http.StatusPaymentRequired, err.Error())
	case errors.Is(err, ErrIdempotencyConflict):
		httpserver.WriteError(w, http.StatusConflict, err.Error())
	case err != nil:
		h.log.ErrorContext(r.Context(), "accept message failed", "customer_id", req.CustomerID, "err", err)
		httpserver.WriteError(w, http.StatusInternalServerError, "internal error")
	default:
		if res.Replayed {
			w.Header().Set("Idempotent-Replayed", "true")
		}
		m := res.Message
		// The 202 describes the acceptance, so status is always "queued"
		// (also on replay, so the replayed response matches the original).
		// The current status comes from the reports API.
		httpserver.WriteJSON(w, http.StatusAccepted, sendResponse{
			MessageID: m.ID,
			Status:    StatusQueued,
			Priority:  m.Priority,
			Cost:      m.Cost,
			CreatedAt: m.CreatedAt,
		})
	}
}
