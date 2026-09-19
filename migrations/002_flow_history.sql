-- Flow history: every qualifying flow event (option, stock/shares, or
-- futures) that cleared its live threshold, saved for same-day replay.
-- Independent of the auth migration (001_init.sql) - this only needs
-- DATABASE_URL, not the rest of the auth setup.
--
-- Run once:
--   psql "$DATABASE_URL" -f migrations/002_flow_history.sql

CREATE TABLE IF NOT EXISTS flow_events (
  id                BIGSERIAL PRIMARY KEY,
  symbol            TEXT NOT NULL,        -- the ticker this event is filed under (already resolved for futures sibling combining - see sourceSymbol)
  asset_type        TEXT NOT NULL,        -- 'option' | 'stock' | 'futures'
  side              TEXT NOT NULL,        -- 'BOUGHT' | 'SOLD' (options) or 'BUY' | 'SELL' (stock/futures)
  size              NUMERIC,              -- contracts (options/futures) or shares (stock) - standard-equivalent for futures
  value             NUMERIC NOT NULL,     -- dollar value: premium (options) or price*size (stock/futures)
  option_type       TEXT,                 -- 'CALL' | 'PUT' - NULL for stock/futures
  strike            NUMERIC,              -- NULL for stock/futures
  expiration        TEXT,                 -- NULL for stock/futures
  source_symbol     TEXT,                 -- futures sibling tag ("via MESZ6") - NULL otherwise
  event_time        TIMESTAMPTZ NOT NULL, -- when the trade actually happened (not when it was saved)
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Every lookup is "this symbol, today" - covers that access pattern
-- directly rather than a separate symbol-only and time-only index.
CREATE INDEX IF NOT EXISTS flow_events_symbol_time_idx ON flow_events(symbol, event_time);
