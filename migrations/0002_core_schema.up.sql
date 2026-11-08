-- 0002_core_schema: wallet, ledger, messages, idempotency keys, outbox.
-- Owners: wallets + ledger_entries -> wallet, messages + idempotency_keys -> sms,
-- outbox -> outbox.

-- Wallet balance: fast-path cache of SUM(ledger_entries.amount).
CREATE TABLE wallets (
  customer_id BIGINT PRIMARY KEY,
  balance     BIGINT NOT NULL DEFAULT 0 CHECK (balance >= 0),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Ledger: source of truth for every money movement.
-- amount is signed: topup and refund > 0, debit < 0.
CREATE TABLE ledger_entries (
  id          BIGINT GENERATED ALWAYS AS IDENTITY,
  customer_id BIGINT NOT NULL,
  amount      BIGINT NOT NULL,
  kind        TEXT NOT NULL CHECK (kind IN ('topup','debit','refund')),
  message_id  UUID,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);
CREATE INDEX ON ledger_entries (customer_id, created_at DESC);

-- Messages: partitioned by day so old days can be detached/dropped cheaply
-- and report queries prune to the days they ask for.
CREATE TABLE messages (
  id          UUID NOT NULL,
  customer_id BIGINT NOT NULL,
  recipient   TEXT NOT NULL,
  body        TEXT NOT NULL,
  priority    TEXT NOT NULL CHECK (priority IN ('express','normal')),
  status      TEXT NOT NULL,
  cost        BIGINT NOT NULL,
  attempts    INT NOT NULL DEFAULT 0,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);
CREATE INDEX ON messages (customer_id, created_at DESC);

-- Idempotency keys: separate table so the unique key works even though
-- messages is partitioned (a unique index on a partitioned table must
-- include the partition key).
CREATE TABLE idempotency_keys (
  customer_id  BIGINT NOT NULL,
  key          TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  message_id   UUID NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (customer_id, key)
);

-- Transactional outbox, drained by outbox-relay.
CREATE TABLE outbox (
  id           BIGSERIAL PRIMARY KEY,
  topic        TEXT NOT NULL,
  key          TEXT NOT NULL,
  payload      JSONB NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ
);
-- Partial index: only unpublished rows, so it stays small however large outbox grows.
CREATE INDEX ON outbox (id) WHERE published_at IS NULL;

-- Initial partitions (UTC boundaries). Later partitions are created by a
-- maintenance job (later step). Rows outside the window land in DEFAULT.
DO $$
DECLARE
  today date := (now() AT TIME ZONE 'UTC')::date;
  month date := date_trunc('month', now() AT TIME ZONE 'UTC')::date;
  d     date;
BEGIN
  -- messages: today + next 7 days
  FOR i IN 0..7 LOOP
    d := today + i;
    EXECUTE format(
      'CREATE TABLE %I PARTITION OF messages FOR VALUES FROM (%L) TO (%L)',
      'messages_p' || to_char(d, 'YYYYMMDD'),
      d::timestamp AT TIME ZONE 'UTC',
      (d + 1)::timestamp AT TIME ZONE 'UTC');
  END LOOP;

  -- ledger_entries: current month + next month
  FOR i IN 0..1 LOOP
    d := (month + make_interval(months => i))::date;
    EXECUTE format(
      'CREATE TABLE %I PARTITION OF ledger_entries FOR VALUES FROM (%L) TO (%L)',
      'ledger_entries_p' || to_char(d, 'YYYYMM'),
      d::timestamp AT TIME ZONE 'UTC',
      (d + interval '1 month')::timestamp AT TIME ZONE 'UTC');
  END LOOP;
END $$;

CREATE TABLE messages_default PARTITION OF messages DEFAULT;
CREATE TABLE ledger_entries_default PARTITION OF ledger_entries DEFAULT;
