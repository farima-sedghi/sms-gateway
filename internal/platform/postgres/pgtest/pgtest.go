// Package pgtest gives tests a real, migrated, throw-away Postgres database.
//
// Tests that touch SQL run against real Postgres, not mocks: the behaviour we
// rely on (row locks, ON CONFLICT, CHECK constraints, partition routing) only
// exists in the database. Each call creates a fresh database, so tests and
// test packages never see each other's data and can run in parallel.
//
// Set TEST_DATABASE_URL to a server where the user may CREATE DATABASE, e.g.
// the compose Postgres: postgres://sms:sms@localhost:5432/sms?sslmode=disable
// Without it, DB tests are skipped (`make test` sets it).
package pgtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// New creates a fresh database, applies every migrations/*.up.sql in order,
// and returns a pool to it. The database is dropped when the test ends.
func New(t testing.TB) *pgxpool.Pool {
	t.Helper()
	baseURL := os.Getenv("TEST_DATABASE_URL")
	if baseURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping Postgres test (run `make test` or start compose)")
	}
	ctx := context.Background()

	admin, err := pgx.Connect(ctx, baseURL)
	if err != nil {
		t.Fatalf("connect to test server: %v", err)
	}

	name := "sms_test_" + randomSuffix(t)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		admin.Close(ctx)
		t.Fatalf("create test database: %v", err)
	}

	cfg, err := pgxpool.ParseConfig(baseURL)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_URL: %v", err)
	}
	cfg.ConnConfig.Database = name
	// Enough connections for the concurrency tests to really run in
	// parallel, while staying well under Postgres' default max_connections (100).
	cfg.MaxConns = 30

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("open test pool: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		// WITH (FORCE) closes any connection still open (Postgres 13+).
		_, _ = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
		admin.Close(ctx)
	})

	applyMigrations(t, ctx, pool)
	return pool
}

// applyMigrations runs the same SQL files the migrate container runs.
// Exec without arguments uses Postgres' simple protocol, which accepts a
// whole file of statements (including DO blocks) in one call.
func applyMigrations(t testing.TB, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(migrationsDir(t), "*.up.sql"))
	if err != nil || len(files) == 0 {
		t.Fatalf("find migrations: %v (found %d)", err, len(files))
	}
	sort.Strings(files) // 0001_..., 0002_..., in order
	for _, f := range files {
		sql, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply %s: %v", filepath.Base(f), err)
		}
	}
}

// migrationsDir finds <repo>/migrations from this source file's location, so
// it works no matter which package's directory `go test` runs in.
func migrationsDir(t testing.TB) string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate pgtest source file")
	}
	// file = <repo>/internal/platform/postgres/pgtest/pgtest.go
	return filepath.Join(filepath.Dir(file), "..", "..", "..", "..", "migrations")
}

func randomSuffix(t testing.TB) string {
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("random suffix: %v", err)
	}
	return hex.EncodeToString(b)
}

// QueryInt runs a query that returns one integer (e.g. COUNT or SUM) and
// fails the test on error. Handy for asserting on table contents.
func QueryInt(t testing.TB, pool *pgxpool.Pool, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	return n
}

// Must is a tiny helper for setup steps that should never fail.
func Must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(fmt.Errorf("setup: %w", err))
	}
}
