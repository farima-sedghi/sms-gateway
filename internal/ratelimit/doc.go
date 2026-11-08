// Package ratelimit gives each customer a token bucket, so a few very heavy
// senders cannot starve everyone else. A customer over the limit gets
// 429 Too Many Requests.
//
// The in-memory implementation is per API instance:
// per-instance limit = global limit / number of API instances.
// The production path is a shared Redis-backed bucket. It is documented, not built.
//
// Dependencies: none.
package ratelimit
