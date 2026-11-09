-- Delivery latency of one load-test run, per priority.
-- Variables (psql -v): since (timestamp), cust_min, cust_max (the run's customer ids).
--
-- latency = handed_at - created_at: from the moment the API accepted the
-- message (inside its transaction) to the moment the operator accepted it.
-- Both timestamps come from the Postgres clock.
\set ON_ERROR_STOP on
\pset footer off

SELECT
  priority,
  count(*)                                                        AS accepted,
  count(*) FILTER (WHERE status = 'sent')                         AS sent,
  count(*) FILTER (WHERE status = 'failed')                       AS failed,
  count(*) FILTER (WHERE status = 'unknown')                      AS unknown,
  count(*) FILTER (WHERE status IN ('queued','sending','retrying')) AS in_flight,
  round(count(*) / nullif(extract(epoch FROM max(created_at) - min(created_at)), 0))  AS accepted_per_s,
  round(count(handed_at) / nullif(extract(epoch FROM max(handed_at) - min(handed_at)), 0)) AS handed_per_s,
  round((percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM handed_at - created_at)))::numeric * 1000) AS p50_ms,
  round((percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM handed_at - created_at)))::numeric * 1000) AS p95_ms,
  round((percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM handed_at - created_at)))::numeric * 1000) AS p99_ms,
  round((max(extract(epoch FROM handed_at - created_at)))::numeric * 1000)         AS max_ms
FROM messages
WHERE created_at >= :'since'::timestamptz
  AND customer_id BETWEEN :cust_min AND :cust_max
GROUP BY priority
ORDER BY priority;
