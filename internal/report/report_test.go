package report

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/farima-sedghi/sms-gateway/internal/platform/postgres/pgtest"
	"github.com/farima-sedghi/sms-gateway/internal/sms"
	"github.com/farima-sedghi/sms-gateway/internal/wallet"
)

// --- fixtures ---------------------------------------------------------------

// base is a fixed point in the past; fixture rows are spread over ~60h
// before it (they land in the DEFAULT partition, which is fine for lists).
var base = time.Date(2026, 1, 10, 12, 0, 0, 0, time.UTC)

const customer = 1

// seed inserts 120 messages for customer 1 (118 at 30-minute steps, plus two
// rows sharing one created_at to exercise the id tie-break), every third one
// 'failed', and 10 messages for customer 2 in the same range.
func seed(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
		INSERT INTO messages (id, customer_id, recipient, body, priority, status, cost, created_at)
		SELECT gen_random_uuid(), 1, '989120000000', 'b', 'normal',
		       CASE WHEN i % 3 = 0 THEN 'failed' ELSE 'sent' END, 100,
		       $1::timestamptz - i * interval '30 minutes'
		FROM generate_series(1, 118) AS i`, base)
	pgtest.Must(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO messages (id, customer_id, recipient, body, priority, status, cost, created_at)
		SELECT gen_random_uuid(), 1, '989120000000', 'tie', 'express', 'sent', 100,
		       $1::timestamptz - interval '45 minutes'
		FROM generate_series(1, 2)`, base)
	pgtest.Must(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO messages (id, customer_id, recipient, body, priority, status, cost, created_at)
		SELECT gen_random_uuid(), 2, '989120000000', 'other', 'normal', 'sent', 100,
		       $1::timestamptz - i * interval '1 hour'
		FROM generate_series(1, 10) AS i`, base)
	pgtest.Must(t, err)
}

// expectedIDs is the full answer computed independently, in keyset order.
func expectedIDs(t *testing.T, pool *pgxpool.Pool, status string) []uuid.UUID {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT id FROM messages WHERE customer_id = 1 AND ($1 = '' OR status = $1)
		ORDER BY created_at DESC, id DESC`, status)
	pgtest.Must(t, err)
	ids, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
	pgtest.Must(t, err)
	return ids
}

func newServer(t *testing.T, pool *pgxpool.Pool) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	NewHandler(NewService(pool), slog.New(slog.NewTextHandler(io.Discard, nil))).Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func get(t *testing.T, srv *httptest.Server, path string, params url.Values) (int, []byte) {
	t.Helper()
	u := srv.URL + path
	if params != nil {
		u += "?" + params.Encode()
	}
	resp, err := http.Get(u)
	pgtest.Must(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, body
}

func rangeParams() url.Values {
	return url.Values{
		"from": {base.Add(-7 * 24 * time.Hour).Format(time.RFC3339)},
		"to":   {base.Add(time.Minute).Format(time.RFC3339)},
	}
}

// pageAll follows next_cursor until the end and returns every page.
func pageAll(t *testing.T, srv *httptest.Server, params url.Values) []Page {
	t.Helper()
	var pages []Page
	for i := 0; i < 20; i++ {
		code, body := get(t, srv, "/customers/1/messages", params)
		if code != http.StatusOK {
			t.Fatalf("page %d: status %d: %s", i+1, code, body)
		}
		var p Page
		pgtest.Must(t, json.Unmarshal(body, &p))
		pages = append(pages, p)
		if p.NextCursor == nil {
			return pages
		}
		params.Set("cursor", *p.NextCursor)
	}
	t.Fatal("pagination did not end")
	return nil
}

func flatten(pages []Page) []uuid.UUID {
	var ids []uuid.UUID
	for _, p := range pages {
		for _, m := range p.Messages {
			ids = append(ids, m.ID)
		}
	}
	return ids
}

func sameIDs(a, b []uuid.UUID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// --- tests --------------------------------------------------------------------

// 120 rows at limit 50 -> pages of 50, 50, 20, in exact keyset order, with
// no duplicate and no gap (including the two rows that share created_at).
func TestPaginationWalksEveryRowOnceInOrder(t *testing.T) {
	pool := pgtest.New(t)
	seed(t, pool)
	srv := newServer(t, pool)

	params := rangeParams()
	params.Set("limit", "50")
	pages := pageAll(t, srv, params)

	if len(pages) != 3 || len(pages[0].Messages) != 50 || len(pages[1].Messages) != 50 || len(pages[2].Messages) != 20 {
		sizes := []int{}
		for _, p := range pages {
			sizes = append(sizes, len(p.Messages))
		}
		t.Fatalf("page sizes = %v, want [50 50 20]", sizes)
	}
	if got, want := flatten(pages), expectedIDs(t, pool, ""); !sameIDs(got, want) {
		t.Fatalf("paged ids differ from the full ordered result (%d vs %d rows)", len(got), len(want))
	}
	for _, p := range pages {
		for _, m := range p.Messages {
			if m.Recipient == "" || m.CreatedAt.IsZero() || m.Cost != 100 {
				t.Fatalf("incomplete row: %+v", m)
			}
		}
	}
}

func TestDefaultLimitIs50AndStatusFilterWorks(t *testing.T) {
	pool := pgtest.New(t)
	seed(t, pool)
	srv := newServer(t, pool)

	pages := pageAll(t, srv, rangeParams())
	if len(pages[0].Messages) != DefaultLimit {
		t.Errorf("first page = %d rows, want default %d", len(pages[0].Messages), DefaultLimit)
	}

	params := rangeParams()
	params.Set("status", "failed")
	failed := flatten(pageAll(t, srv, params))
	if want := expectedIDs(t, pool, "failed"); !sameIDs(failed, want) || len(want) == 0 {
		t.Errorf("failed filter returned %d rows, want %d", len(failed), len(want))
	}
}

// from is inclusive, to is exclusive.
func TestRangeBounds(t *testing.T) {
	pool := pgtest.New(t)
	seed(t, pool)
	srv := newServer(t, pool)

	// Rows exist exactly at base-1h and base-2h. [base-2h, base-1h) holds
	// base-2h and base-90m, but not base-1h.
	params := url.Values{
		"from": {base.Add(-2 * time.Hour).Format(time.RFC3339)},
		"to":   {base.Add(-time.Hour).Format(time.RFC3339)},
	}
	page := pageAll(t, srv, params)[0]
	var times []time.Time
	for _, m := range page.Messages {
		times = append(times, m.CreatedAt)
	}
	if len(times) != 2 || !times[0].Equal(base.Add(-90*time.Minute)) || !times[1].Equal(base.Add(-2*time.Hour)) {
		t.Errorf("created_at in [base-2h, base-1h) = %v, want [base-90m, base-2h]", times)
	}
}

func TestOtherCustomersRowsAreNeverReturned(t *testing.T) {
	pool := pgtest.New(t)
	seed(t, pool)
	srv := newServer(t, pool)

	code, body := get(t, srv, "/customers/2/messages", rangeParams())
	var p Page
	pgtest.Must(t, json.Unmarshal(body, &p))
	if code != 200 || len(p.Messages) != 10 {
		t.Fatalf("customer 2: status %d, %d rows, want 200 and 10", code, len(p.Messages))
	}
}

func TestEmptyResultIsEmptyArrayAndNoCursor(t *testing.T) {
	pool := pgtest.New(t)
	srv := newServer(t, pool)

	_, body := get(t, srv, "/customers/99/messages", rangeParams())
	if !strings.Contains(string(body), `"messages":[]`) || !strings.Contains(string(body), `"next_cursor":null`) {
		t.Errorf("body = %s, want messages [] and next_cursor null", body)
	}
}

func TestInvalidQueriesAre400(t *testing.T) {
	pool := pgtest.New(t)
	seed(t, pool)
	srv := newServer(t, pool)

	// A real cursor from an unfiltered query, to reuse with another filter.
	first := pageAll(t, srv, rangeParams())[0]

	from := base.Add(-time.Hour).Format(time.RFC3339)
	to := base.Format(time.RFC3339)
	cases := map[string]url.Values{
		"missing from":         {"to": {to}},
		"missing to":           {"from": {from}},
		"bad time format":      {"from": {"yesterday"}, "to": {to}},
		"from after to":        {"from": {to}, "to": {from}},
		"range over 31 days":   {"from": {base.Add(-32 * 24 * time.Hour).Format(time.RFC3339)}, "to": {to}},
		"limit 0":              {"from": {from}, "to": {to}, "limit": {"0"}},
		"limit 101":            {"from": {from}, "to": {to}, "limit": {"101"}},
		"unknown status":       {"from": {from}, "to": {to}, "status": {"delivered"}},
		"malformed cursor":     {"from": {from}, "to": {to}, "cursor": {"not-a-cursor"}},
		"cursor, other filter": merge(rangeParams(), url.Values{"status": {"sent"}, "cursor": {*first.NextCursor}}),
	}
	for name, params := range cases {
		t.Run(name, func(t *testing.T) {
			if code, body := get(t, srv, "/customers/1/messages", params); code != http.StatusBadRequest {
				t.Errorf("status = %d (%s), want 400", code, body)
			}
		})
	}
	if code, _ := get(t, srv, "/customers/abc/messages", rangeParams()); code != http.StatusBadRequest {
		t.Errorf("non-numeric customer id: status %d, want 400", code)
	}
}

func merge(a, b url.Values) url.Values {
	for k, v := range b {
		a[k] = v
	}
	return a
}

// Get by id: found for its owner, 404 for anyone else and for non-v7 ids.
func TestGetMessageByID(t *testing.T) {
	pool := pgtest.New(t)
	srv := newServer(t, pool)
	ctx := context.Background()

	_, err := wallet.NewService(pool).TopUp(ctx, 5, 1000)
	pgtest.Must(t, err)
	res, err := sms.NewService(pool, 100, 2*time.Minute, nil).Accept(ctx, 5,
		sms.SendRequest{Recipient: "989121234567", Body: "hi", Priority: sms.PriorityExpress}, "")
	pgtest.Must(t, err)
	id := res.Message.ID

	code, body := get(t, srv, "/customers/5/messages/"+id.String(), nil)
	var m MessageView
	pgtest.Must(t, json.Unmarshal(body, &m))
	if code != 200 || m.ID != id || m.Status != sms.StatusQueued || m.Cost != 100 || m.HandedAt != nil {
		t.Fatalf("owner: status %d, body %s", code, body)
	}

	for name, path := range map[string]string{
		"other customer": "/customers/6/messages/" + id.String(),
		"not a v7 id":    "/customers/5/messages/" + uuid.NewString(), // v4
	} {
		if code, _ := get(t, srv, path, nil); code != http.StatusNotFound {
			t.Errorf("%s: status %d, want 404", name, code)
		}
	}
	if code, _ := get(t, srv, "/customers/5/messages/not-a-uuid", nil); code != http.StatusBadRequest {
		t.Errorf("malformed id: status %d, want 400", code)
	}
}

// The created_at window from the UUIDv7 lets Postgres skip every partition
// except the one (or two, at midnight) that can hold the message.
func TestGetByIDPrunesPartitions(t *testing.T) {
	pool := pgtest.New(t)
	ctx := context.Background()
	id, err := uuid.NewV7()
	pgtest.Must(t, err)
	at, err := TimeFromUUIDv7(id)
	pgtest.Must(t, err)

	rows, err := pool.Query(ctx, "EXPLAIN "+getMessageSQL, id, int64(1), at.Add(-idWindow), at.Add(idWindow))
	pgtest.Must(t, err)
	lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
	pgtest.Must(t, err)

	total := pgtest.QueryInt(t, pool, `SELECT COUNT(*) FROM pg_inherits WHERE inhparent = 'messages'::regclass`)
	scanned := 0
	for _, l := range lines {
		if strings.Contains(l, " on messages_") {
			scanned++
		}
	}
	if scanned == 0 || scanned > 2 {
		t.Errorf("plan scans %d of %d partitions, want 1-2:\n%s", scanned, total, strings.Join(lines, "\n"))
	}
	t.Logf("get-by-id scans %d of %d partitions", scanned, total)
}

func TestTimeFromUUIDv7(t *testing.T) {
	before := time.Now().Add(-time.Millisecond)
	id, err := uuid.NewV7()
	pgtest.Must(t, err)
	got, err := TimeFromUUIDv7(id)
	pgtest.Must(t, err)
	if got.Before(before.Truncate(time.Millisecond)) || got.After(time.Now()) {
		t.Errorf("time = %v, want about now", got)
	}
	if _, err := TimeFromUUIDv7(uuid.New()); err == nil {
		t.Error("v4 uuid accepted")
	}
}

// The cursor lands exactly on one of two rows that share created_at: with
// limit 2 the order is [base-30m, tieA] | [tieB, base-60m]. Without the id
// in the keyset comparison, tieB would be skipped or repeated.
func TestCursorBetweenRowsWithTheSameCreatedAt(t *testing.T) {
	pool := pgtest.New(t)
	seed(t, pool)
	srv := newServer(t, pool)

	params := rangeParams()
	params.Set("limit", "2")
	var got []uuid.UUID
	for i := 0; i < 2; i++ {
		code, body := get(t, srv, "/customers/1/messages", params)
		if code != 200 {
			t.Fatalf("page %d: %d %s", i+1, code, body)
		}
		var p Page
		pgtest.Must(t, json.Unmarshal(body, &p))
		for _, m := range p.Messages {
			got = append(got, m.ID)
		}
		params.Set("cursor", *p.NextCursor)
	}
	want := expectedIDs(t, pool, "")[:4]
	if !sameIDs(got, want) {
		t.Errorf("first two pages = %v, want %v", got, want)
	}
}
