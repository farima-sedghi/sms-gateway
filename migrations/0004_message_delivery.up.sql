-- 0004_message_delivery: columns the dispatcher writes.
--   handed_at       when the operator accepted the message (latency = handed_at - created_at)
--   operator_msg_id the operator's id for the message
--   last_error      last failure reason, for support and reconciliation
--   expires_at      express only: after this, do not send (fail + refund, or unknown)
-- ALTER on a partitioned table adds the columns to every partition.
ALTER TABLE messages
  ADD COLUMN handed_at       TIMESTAMPTZ,
  ADD COLUMN operator_msg_id TEXT,
  ADD COLUMN last_error      TEXT,
  ADD COLUMN expires_at      TIMESTAMPTZ;
