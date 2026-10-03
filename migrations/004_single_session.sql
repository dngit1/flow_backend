-- Changes the device limit from 2 to 1.
--
-- Two separate things, both needed: the column default only affects users
-- created AFTER this runs, so existing users (who already have 2 stored
-- explicitly on their row from signup) need to be updated directly too.
--
-- Run once:
--   psql "$DATABASE_URL" -f migrations/004_single_session.sql

ALTER TABLE users ALTER COLUMN max_sessions SET DEFAULT 1;

-- Only touches rows still at the OLD default (2) - if you'd already
-- manually granted someone a higher limit (3+), this leaves that alone
-- rather than silently overwriting a deliberate exception.
UPDATE users SET max_sessions = 1 WHERE max_sessions = 2;
