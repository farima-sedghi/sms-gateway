package httpserver

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// Check is one readiness dependency, such as a Postgres ping.
type Check func(ctx context.Context) error

// Healthz is the liveness probe: the process is up and serving HTTP. It must
// not check dependencies. If it did, a Postgres outage would make the
// orchestrator restart every API pod, and restarts don't fix Postgres.
func Healthz(w http.ResponseWriter, _ *http.Request) {
	WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Readyz is the readiness probe: can this instance do useful work right now?
// It runs all checks in parallel with one timeout and returns 503 if any check
// fails, so a load balancer stops sending traffic to this instance.
func Readyz(checks map[string]Check) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		var (
			mu      sync.Mutex
			wg      sync.WaitGroup
			results = make(map[string]string, len(checks))
			healthy = true
		)
		for name, check := range checks {
			wg.Add(1)
			go func(name string, check Check) {
				defer wg.Done()
				status := "ok"
				if err := check(ctx); err != nil {
					status = err.Error()
				}
				mu.Lock()
				defer mu.Unlock()
				results[name] = status
				if status != "ok" {
					healthy = false
				}
			}(name, check)
		}
		wg.Wait()

		code := http.StatusOK
		if !healthy {
			code = http.StatusServiceUnavailable
		}
		WriteJSON(w, code, results) // encoding/json sorts map keys, so output is stable
	}
}
