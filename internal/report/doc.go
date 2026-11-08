// Package report serves read-only queries over a customer's sent messages.
//
// Reads from: messages (range-partitioned by day on created_at, indexed on
// (customer_id, created_at DESC)). Pagination is keyset (created_at, id) and
// never uses OFFSET, so deep pages cost the same as the first page.
//
// Never writes. The future path for heavy analytics is ClickHouse (see
// docs/architecture.md).
//
// Dependencies: platform only.
package report
