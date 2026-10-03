-- Adds password-based sign-in as an option alongside Google and magic-link,
-- which both remain unchanged and fully working.
--
-- Run once:
--   psql "$DATABASE_URL" -f migrations/005_password_auth.sql

-- NULL for any account that has never set a password (Google-only or
-- magic-link-only users) - password sign-in is opt-in, not required.
ALTER TABLE users ADD COLUMN IF NOT EXISTS password_hash TEXT;

-- Same shape and reasoning as magic_link_tokens: a short-lived, one-time
-- token. Kept as its OWN table rather than reusing magic_link_tokens so a
-- magic sign-in link can never accidentally double as a password-reset
-- link, or vice versa.
CREATE TABLE IF NOT EXISTS password_reset_tokens (
  token           TEXT PRIMARY KEY,
  email           TEXT NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at      TIMESTAMPTZ NOT NULL
);
