ALTER TABLE messages
  DROP COLUMN expires_at,
  DROP COLUMN last_error,
  DROP COLUMN operator_msg_id,
  DROP COLUMN handed_at;
