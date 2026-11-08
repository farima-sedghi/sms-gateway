// Package config reads configuration from environment variables.
//
// Each binary needs a different set of settings (operator-mock has no
// database, for example), so there is no single big Config struct. Each
// cmd/*/main.go declares its own small struct and fills it with a Reader.
// The Reader collects every problem, so a misconfigured container reports all
// missing or invalid variables at once instead of one per restart.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

// Reader reads typed values from an env lookup function and records errors.
// The lookup is injected (normally os.LookupEnv), so tests can pass a map
// instead of touching the process environment.
type Reader struct {
	lookup func(string) (string, bool)
	errs   []error
}

// NewReader returns a Reader that uses lookup (e.g. os.LookupEnv).
func NewReader(lookup func(string) (string, bool)) *Reader {
	return &Reader{lookup: lookup}
}

func (r *Reader) get(key string) (string, bool) {
	v, ok := r.lookup(key)
	v = strings.TrimSpace(v)
	return v, ok && v != ""
}

// Required returns the value of key and records an error if it is missing.
func (r *Reader) Required(key string) string {
	v, ok := r.get(key)
	if !ok {
		r.errs = append(r.errs, fmt.Errorf("%s is required", key))
	}
	return v
}

// String returns the value of key, or def if it is unset.
func (r *Reader) String(key, def string) string {
	if v, ok := r.get(key); ok {
		return v
	}
	return def
}

// RequiredList returns a comma-separated value as a slice, and records an error if it is missing.
func (r *Reader) RequiredList(key string) []string {
	raw := r.Required(key)
	var out []string
	for _, p := range strings.Split(raw, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Duration parses a Go duration such as "10s" or "250ms".
func (r *Reader) Duration(key string, def time.Duration) time.Duration {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: invalid duration %q", key, v))
		return def
	}
	return d
}

// Float parses a float, e.g. a failure rate between 0 and 1.
func (r *Reader) Float(key string, def float64) float64 {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: invalid number %q", key, v))
		return def
	}
	return f
}

// LogLevel parses debug|info|warn|error.
func (r *Reader) LogLevel(key string, def slog.Level) slog.Level {
	v, ok := r.get(key)
	if !ok {
		return def
	}
	var l slog.Level
	if err := l.UnmarshalText([]byte(v)); err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: invalid log level %q", key, v))
		return def
	}
	return l
}

// Err returns every recorded problem joined into one error, or nil.
func (r *Reader) Err() error {
	return errors.Join(r.errs...)
}
