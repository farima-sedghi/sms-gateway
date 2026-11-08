-- 0001_init: placeholder so the migration pipeline (migrate container ->
-- schema_migrations table) is proven end to end in step 1.
-- Real tables (balances, ledger_entries, messages, outbox) come in later
-- migrations, one per module, so each module's schema can be reviewed on its own.
SELECT 1;
