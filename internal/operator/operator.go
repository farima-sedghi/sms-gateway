package operator

import (
	"errors"
	"fmt"
)

// Error classes (decision 16). Every error from Send wraps exactly one of
// ErrPermanent, ErrTemporary or ErrUnknown; callers branch with errors.Is.
var (
	// ErrPermanent: the operator definitely did NOT deliver and never will
	// (4xx, e.g. invalid number). Fail and refund.
	ErrPermanent = errors.New("operator: permanent failure")

	// ErrTemporary: the operator definitely did NOT deliver, but a retry may
	// work (5xx, 429, connection refused, circuit breaker open).
	ErrTemporary = errors.New("operator: temporary failure")

	// ErrUnknown: the request reached the operator but we got no answer
	// (timeout, connection reset after the request was written). It MAY have
	// been delivered: retry with the same ref, never refund on this alone.
	ErrUnknown = errors.New("operator: unknown outcome")
)

// ErrCircuitOpen is returned without calling the operator while the circuit
// breaker is open. It wraps ErrTemporary (nothing was sent), so
// errors.Is(err, ErrTemporary) is true; callers that want back-pressure
// check for ErrCircuitOpen first.
var ErrCircuitOpen = fmt.Errorf("%w: circuit breaker open", ErrTemporary)

// errRateLimited marks a 429: Temporary for the caller, but not an operator
// failure for the circuit breaker (the operator is healthy, just busy).
var errRateLimited = errors.New("rate limited")
