// Package dispatch consumes the sms.express, sms.normal and sms.retry topics
// and hands messages to the operator.
//
// Responsibilities:
//   - A bounded worker pool per topic. Express never shares workers with normal.
//   - Offsets are committed only after a message is fully processed.
//   - Status changes (queued -> sending -> sent | failed, with retrying in
//     between) are guarded in SQL, so a duplicate Kafka delivery never resends.
//   - On failure the message goes to sms.retry with an attempt count and
//     backoff. After N attempts it goes to sms.dlq, is marked failed, and the
//     customer gets an idempotent wallet refund.
//
// Owns tables: none. Message status is changed through sms, money through wallet.
//
// Dependencies: wallet (refunds), operator, sms (status transitions), platform.
package dispatch
