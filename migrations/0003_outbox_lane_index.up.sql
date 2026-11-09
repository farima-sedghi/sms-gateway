-- 0003_outbox_lane_index: one index section per relay lane.
--
-- The old index (id) WHERE published_at IS NULL forced the express lane's
-- "WHERE topic = 'sms.express' ORDER BY id" to walk past every unpublished
-- sms.normal row: a normal backlog slowed express down inside Postgres.
-- With (topic, id), each lane reads only its own rows, in id order.
--
-- Plain CREATE INDEX (not CONCURRENTLY): golang-migrate runs the file in one
-- implicit transaction, and the table is small now. On a large live table,
-- use CREATE INDEX CONCURRENTLY in a migration file of its own.
CREATE INDEX outbox_unpublished_lane_idx ON outbox (topic, id) WHERE published_at IS NULL;
DROP INDEX outbox_id_idx;
