-- 0005_customer_limits: per-customer rate-limit plans (owned by ratelimit).
-- A customer without a row gets the defaults from the API's environment.
-- Values are GLOBAL per customer; each API instance enforces value / API_INSTANCES.
-- burst is the bucket size used for both classes of a planned customer.
CREATE TABLE customer_limits (
  customer_id BIGINT PRIMARY KEY,
  normal_rps  INTEGER NOT NULL CHECK (normal_rps > 0),
  express_rps INTEGER NOT NULL CHECK (express_rps > 0),
  burst       INTEGER NOT NULL CHECK (burst > 0),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
