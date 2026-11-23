-- Primary key is (day, key_id, ...); this lets per-key range queries seek on key_id.
CREATE INDEX IF NOT EXISTS usage_daily_key_day ON usage_daily (key_id, day);
