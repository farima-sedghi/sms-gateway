-- Dropping a partitioned table drops all its partitions.
DROP TABLE IF EXISTS outbox;
DROP TABLE IF EXISTS idempotency_keys;
DROP TABLE IF EXISTS messages;
DROP TABLE IF EXISTS ledger_entries;
DROP TABLE IF EXISTS wallets;
