// Package logging builds the structured JSON logger every binary uses.
package logging

import (
	"io"
	"log/slog"
)

// New returns a JSON slog logger. Every record gets a "service" field, so logs
// from all binaries can be mixed in one place and still filtered.
func New(w io.Writer, service string, level slog.Level) *slog.Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})
	return slog.New(h).With("service", service)
}
