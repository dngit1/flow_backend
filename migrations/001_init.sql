-- Initial auth schema: users + sessions.
--
-- Run this once against your database (local or Render) before starting
-- the server with auth enabled. Locally:
--   psql "$DATABASE_URL" -f migrations/001_init.sql
-- On Render: open the database's "Shell" tab in the dashboard, or connect
-- with psql using the External Database URL shown there, and run the same
-- command.

CREATE TABLE IF NOT EXISTS users (
  id              BIGSERIAL PRIMARY KEY,
  email           TEXT NOT NULL UNIQUE,
  google_id       TEXT UNIQUE,                    -- NULL if they've only ever used magic-link sign-in
  plan_status     TEXT NOT NULL DEFAULT 'free',    -- 'free' | 'active' | 'past_due' | 'canceled' (payment enforcement comes later; every value is fully allowed access for now)
  max_sessions    INTEGER NOT NULL DEFAULT 2,      -- how many devices can be logged in at once - per-user so a future plan tier can grant more without a schema change
  stripe_customer_id     TEXT,
  stripe_subscription_id TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sessions (
  id              TEXT PRIMARY KEY,                -- random token, also what's stored in the browser's cookie
  user_id         BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  user_agent      TEXT
);

CREATE INDEX IF NOT EXISTS sessions_user_id_idx ON sessions(user_id);

-- One-time-use tokens for magic-link email sign-in. Short-lived (see
-- MAGIC_LINK_TTL_MS in lib/auth.js) and deleted once used.
CREATE TABLE IF NOT EXISTS magic_link_tokens (
  token           TEXT PRIMARY KEY,
  email           TEXT NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at      TIMESTAMPTZ NOT NULL
);
