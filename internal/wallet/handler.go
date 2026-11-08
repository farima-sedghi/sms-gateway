package wallet

import (
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/farima-sedghi/sms-gateway/internal/platform/httpserver"
)

// Handler exposes the wallet over HTTP. It only parses input, calls the
// service and maps errors to status codes; no business logic here.
type Handler struct {
	svc *Service
	log *slog.Logger
}

// NewHandler returns the wallet HTTP handler.
func NewHandler(svc *Service, log *slog.Logger) *Handler {
	return &Handler{svc: svc, log: log}
}

// Register adds the wallet routes to mux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /customers/{id}/topup", h.topUp)
	mux.HandleFunc("GET /customers/{id}/balance", h.balance)
}

type topUpRequest struct {
	Amount int64 `json:"amount"`
}

type balanceResponse struct {
	CustomerID int64 `json:"customer_id"`
	Balance    int64 `json:"balance"`
}

func (h *Handler) topUp(w http.ResponseWriter, r *http.Request) {
	customerID, ok := parseCustomerID(w, r)
	if !ok {
		return
	}
	var req topUpRequest
	if err := httpserver.DecodeJSON(w, r, &req); err != nil {
		httpserver.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	balance, err := h.svc.TopUp(r.Context(), customerID, req.Amount)
	switch {
	case errors.Is(err, ErrInvalidAmount):
		httpserver.WriteError(w, http.StatusBadRequest, err.Error())
	case err != nil:
		h.log.ErrorContext(r.Context(), "top up failed", "customer_id", customerID, "err", err)
		httpserver.WriteError(w, http.StatusInternalServerError, "internal error")
	default:
		httpserver.WriteJSON(w, http.StatusOK, balanceResponse{CustomerID: customerID, Balance: balance})
	}
}

func (h *Handler) balance(w http.ResponseWriter, r *http.Request) {
	customerID, ok := parseCustomerID(w, r)
	if !ok {
		return
	}
	balance, err := h.svc.Balance(r.Context(), customerID)
	if err != nil {
		h.log.ErrorContext(r.Context(), "get balance failed", "customer_id", customerID, "err", err)
		httpserver.WriteError(w, http.StatusInternalServerError, "internal error")
		return
	}
	httpserver.WriteJSON(w, http.StatusOK, balanceResponse{CustomerID: customerID, Balance: balance})
}

// parseCustomerID reads {id} from the path. On failure it writes 400 and
// returns false, so handlers can just `return`.
func parseCustomerID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		httpserver.WriteError(w, http.StatusBadRequest, "customer id must be a positive integer")
		return 0, false
	}
	return id, true
}
