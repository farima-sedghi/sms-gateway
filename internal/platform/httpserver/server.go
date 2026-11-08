// Package httpserver runs an http.Server with sane timeouts and graceful
// shutdown, and provides shared middleware and health handlers.
package httpserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// Run serves handler on addr until ctx is cancelled (SIGTERM), then stops
// accepting connections and waits up to shutdownTimeout for in-flight
// requests to finish. It returns nil on a clean shutdown.
func Run(ctx context.Context, addr string, handler http.Handler, log *slog.Logger, shutdownTimeout time.Duration) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second, // slowloris protection
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelError),
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("http server listening", "addr", addr)
		// ListenAndServe always returns an error; ErrServerClosed means Shutdown was called.
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	case <-ctx.Done():
	}

	log.Info("http server shutting down", "timeout", shutdownTimeout)
	// ctx is already cancelled, so a fresh context bounds the shutdown.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("http server shutdown: %w", err)
	}
	return <-errCh
}
