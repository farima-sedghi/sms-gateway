CREATE INDEX outbox_id_idx ON outbox (id) WHERE published_at IS NULL;
DROP INDEX outbox_unpublished_lane_idx;
