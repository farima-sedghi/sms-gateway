package httpserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// maxBodyBytes caps request bodies. A single-page SMS request is well under
// 1 KB, so 1 MB is generous and still stops a client from streaming gigabytes.
const maxBodyBytes = 1 << 20

// WriteJSON writes v as a JSON response with the given status code.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v) // the client may have gone; nothing useful to do on error
}

// ErrorBody is the JSON shape of every error response: {"error": "..."}.
type ErrorBody struct {
	Error string `json:"error"`
}

// WriteError writes {"error": msg} with the given status code.
func WriteError(w http.ResponseWriter, status int, msg string) {
	WriteJSON(w, status, ErrorBody{Error: msg})
}

// DecodeJSON reads one JSON object from the request body into dst.
// It rejects unknown fields (a typo like "recipent" fails loudly instead of
// being ignored), bodies over 1 MB, and trailing data after the object.
// The returned error message is safe to show to the client.
func DecodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		switch {
		case errors.As(err, &maxErr):
			return fmt.Errorf("request body larger than %d bytes", maxErr.Limit)
		case errors.Is(err, io.EOF):
			return errors.New("request body is empty")
		default:
			return fmt.Errorf("invalid JSON: %v", err)
		}
	}
	if dec.More() {
		return errors.New("request body must contain a single JSON object")
	}
	return nil
}
