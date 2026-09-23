-- Price history: completed 1-minute bars, starting with futures only
-- (Massive already pushes a completed aggregate bar per minute via the
-- AM.<TICKER> channel - this just also saves that same data instead of
-- only using it to drive the live chart). Stocks aren't covered yet -
-- Alpaca never sends a "here's the completed bar" signal the way Massive
-- does, so stock bar-tracking would need to be built server-side from
-- scratch if/when that's wanted.
--
-- Independent of both other migrations - only needs DATABASE_URL.
--
-- Run once:
--   psql "$DATABASE_URL" -f migrations/003_price_history.sql

CREATE TABLE IF NOT EXISTS price_bars (
  id          BIGSERIAL PRIMARY KEY,
  symbol      TEXT NOT NULL,
  timeframe   TEXT NOT NULL DEFAULT '1min',
  bar_time    TIMESTAMPTZ NOT NULL,   -- the bar's own start time, not when it was saved
  open        NUMERIC NOT NULL,
  high        NUMERIC NOT NULL,
  low         NUMERIC NOT NULL,
  close       NUMERIC NOT NULL,
  volume      NUMERIC,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (symbol, timeframe, bar_time)  -- Massive resends the same minute's bar as it develops; this lets us upsert (keep the latest/final version) instead of inserting duplicates
);

-- Every lookup is "this symbol, this timeframe, over some time range" -
-- covers that access pattern directly.
CREATE INDEX IF NOT EXISTS price_bars_symbol_time_idx ON price_bars(symbol, timeframe, bar_time);
