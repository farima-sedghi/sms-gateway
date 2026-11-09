package report

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/farima-sedghi/sms-gateway/internal/sms"
)

// Domain errors, mapped to HTTP status codes by the handler.
var (
	ErrInvalidQuery = errors.New("invalid query") // 400
	ErrNotFound     = errors.New("message not found")
)

const (
	DefaultLimit = 50
	MaxLimit     = 100
	// MaxRange bounds one list query so Postgres only scans the daily
	// partitions inside it (partition pruning), never the whole history.
	MaxRange = 31 * 24 * time.Hour
	// idWindow is how far created_at may be from the time inside a UUIDv7:
	// the id is made by the API's clock, created_at by Postgres' clock.
	idWindow = 5 * time.Minute
)

// Query is one page request of a customer's messages in [From, To).
type Query struct {
	CustomerID int64
	From, To   time.Time
	Status     sms.Status // "" = any status
	Limit      int        // 0 = DefaultLimit
	Cursor     string     // opaque, from the previous page's next_cursor
}

// Cursor is the keyset position: the last row of the previous page.
// Filter binds it to the query it came from.
type Cursor struct {
	CreatedAt time.Time `json:"t"`
	ID        uuid.UUID `json:"id"`
	Filter    string    `json:"f"`
}

// validStatuses are the statuses a client may filter on.
func validStatus(s sms.Status) bool {
	switch s {
	case sms.StatusQueued, sms.StatusSending, sms.StatusRetrying,
		sms.StatusSent, sms.StatusFailed, sms.StatusUnknown:
		return true
	}
	return false
}

// normalize validates q, applies the default limit and decodes the cursor
// (nil on the first page).
func (q *Query) normalize() (*Cursor, error) {
	switch {
	case q.CustomerID <= 0:
		return nil, fmt.Errorf("%w: customer id must be positive", ErrInvalidQuery)
	case q.From.IsZero() || q.To.IsZero():
		return nil, fmt.Errorf("%w: from and to are required (RFC3339)", ErrInvalidQuery)
	case !q.From.Before(q.To):
		return nil, fmt.Errorf("%w: from must be before to", ErrInvalidQuery)
	case q.To.Sub(q.From) > MaxRange:
		return nil, fmt.Errorf("%w: range must be at most 31 days", ErrInvalidQuery)
	case q.Status != "" && !validStatus(q.Status):
		return nil, fmt.Errorf("%w: unknown status %q", ErrInvalidQuery, q.Status)
	}
	if q.Limit == 0 {
		q.Limit = DefaultLimit
	}
	if q.Limit < 1 || q.Limit > MaxLimit {
		return nil, fmt.Errorf("%w: limit must be between 1 and %d", ErrInvalidQuery, MaxLimit)
	}
	if q.Cursor == "" {
		return nil, nil
	}
	c, err := decodeCursor(q.Cursor)
	if err != nil {
		return nil, fmt.Errorf("%w: malformed cursor", ErrInvalidQuery)
	}
	if c.Filter != q.filterHash() {
		return nil, fmt.Errorf("%w: cursor belongs to a different query (customer, from, to or status changed)", ErrInvalidQuery)
	}
	return &c, nil
}

// filterHash fingerprints everything that defines the result set, so a
// cursor cannot be replayed against a different query.
func (q Query) filterHash() string {
	s := fmt.Sprintf("%d|%s|%s|%s", q.CustomerID,
		q.From.UTC().Format(time.RFC3339Nano), q.To.UTC().Format(time.RFC3339Nano), q.Status)
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:8])
}

// encodeCursor makes the opaque next_cursor: base64url(JSON). Opaque means
// clients pass it back unchanged and we can change its contents later.
func encodeCursor(c Cursor) string {
	b, _ := json.Marshal(c) // cannot fail for this struct
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(s string) (Cursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return Cursor{}, err
	}
	var c Cursor
	if err := json.Unmarshal(b, &c); err != nil {
		return Cursor{}, err
	}
	if c.CreatedAt.IsZero() || c.ID == uuid.Nil {
		return Cursor{}, errors.New("incomplete cursor")
	}
	return c, nil
}

// MessageView is one message in a report. The body is left out: reports are
// about delivery, and bodies are large and may be sensitive.
type MessageView struct {
	ID        uuid.UUID    `json:"message_id"`
	Recipient string       `json:"recipient"`
	Priority  sms.Priority `json:"priority"`
	Status    sms.Status   `json:"status"`
	Cost      int64        `json:"cost"`
	Attempts  int          `json:"attempts"`
	CreatedAt time.Time    `json:"created_at"`
	HandedAt  *time.Time   `json:"handed_at"`
	LastError *string      `json:"last_error"`
}

// Page is one page of results. NextCursor is nil on the last page.
type Page struct {
	Messages   []MessageView `json:"messages"`
	NextCursor *string       `json:"next_cursor"`
}

// TimeFromUUIDv7 returns the creation time stored in a UUIDv7: its first 48
// bits are Unix milliseconds (RFC 9562). Our message ids are v7, so the id
// alone tells us which daily partition the message is in.
func TimeFromUUIDv7(id uuid.UUID) (time.Time, error) {
	if id.Version() != 7 {
		return time.Time{}, fmt.Errorf("not a UUIDv7 (version %d)", id.Version())
	}
	var b [8]byte
	copy(b[2:], id[:6]) // 48-bit big-endian millisecond timestamp
	return time.UnixMilli(int64(binary.BigEndian.Uint64(b[:]))).UTC(), nil
}
