const db = require('./db');

// Retention: 1 day. Nothing older than this is ever returned or kept -
// purgeOldFlowEvents() is called on a timer (see server.js) to actually
// delete it, since Postgres has no built-in row expiration.
const RETENTION_INTERVAL = "1 day";

// Records one qualifying flow event (already past whatever live
// threshold applies - this is NOT a raw/unfiltered trade log, it only
// ever receives events that already made it into the live sidebar).
// Fire-and-forget from the caller's perspective: failures are logged,
// never thrown, so a database hiccup can't break the live flow feature
// itself - saving history is secondary to showing it live.
async function recordFlowEvent(event) {
  try {
    await db.query(
      `INSERT INTO flow_events
         (symbol, asset_type, side, size, value, option_type, strike, expiration, source_symbol, event_time)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, to_timestamp($10::double precision / 1000))`,
      [
        event.symbol,
        event.assetType,
        event.side,
        event.size ?? null,
        event.value,
        event.optionType ?? null,
        event.strike ?? null,
        event.expiration ?? null,
        event.sourceSymbol ?? null,
        event.timeMs,
      ]
    );
  } catch (err) {
    console.error('[flowHistory] failed to record event:', err.message);
  }
}

// ---- what "today" means for Replay ----
// Retention is a rolling 24 hours, so on its own it keeps showing YESTERDAY's
// session for most of the next day (at 10 AM, "the last 24 hours" still includes
// yesterday 10 AM - 4 PM). Replay is meant to show the CURRENT trading session, so
// it also cuts off at the start of that session:
//   stocks / options / SPX : 4:00 AM Eastern (US pre-market opens)
//   futures                : 6:00 PM Eastern (Globex reopens after the daily break),
//                            so ES / NQ / GC keep their whole overnight session
// Both are "the most recent such time at or before now", so before 4 AM Eastern the
// session being replayed is still the previous day's - it rolls over at 4 AM, not at midnight.
const STANDARD_SESSION_START_HOUR_ET = 4;
const FUTURES_SESSION_START_HOUR_ET = 18;

const ET_CLOCK = new Intl.DateTimeFormat('en-US', {
  timeZone: 'America/New_York', hourCycle: 'h23', year: 'numeric', month: 'numeric', day: 'numeric', hour: 'numeric', minute: 'numeric', second: 'numeric',
});

// Wall-clock time in New York at a UTC instant.
function etWallClock(ms) {
  const parts = {};
  for (const p of ET_CLOCK.formatToParts(ms)) if (p.type !== 'literal') parts[p.type] = Number(p.value);
  return { year: parts.year, month: parts.month, day: parts.day, hour: parts.hour, minute: parts.minute, second: parts.second };
}

// The UTC instant at which New York's clock reads year-month-day hour:00 (daylight saving handled: the
// guess is corrected by however far New York's clock is from what was asked for).
function etWallToUtcMs(year, month, day, hour) {
  const wanted = Date.UTC(year, month - 1, day, hour, 0, 0);
  let guess = wanted;
  for (let i = 0; i < 3; i++) {
    const w = etWallClock(guess);
    guess -= Date.UTC(w.year, w.month - 1, w.day, w.hour, w.minute, w.second) - wanted;
  }
  return guess;
}

// The most recent time, at or before nowMs, when New York's clock read `hour`:00.
function mostRecentEtHour(nowMs, hour) {
  const w = etWallClock(nowMs);
  const today = etWallToUtcMs(w.year, w.month, w.day, hour);
  if (today <= nowMs) return today;
  const yesterday = new Date(Date.UTC(w.year, w.month - 1, w.day - 1));
  return etWallToUtcMs(yesterday.getUTCFullYear(), yesterday.getUTCMonth() + 1, yesterday.getUTCDate(), hour);
}

function sessionStarts(nowMs = Date.now()) {
  return {
    standard: new Date(mostRecentEtHour(nowMs, STANDARD_SESSION_START_HOUR_ET)),
    futures: new Date(mostRecentEtHour(nowMs, FUTURES_SESSION_START_HOUR_ET)),
  };
}

// Returns the CURRENT SESSION's saved events for one symbol (see "what today
// means" above), oldest first - the order a replay should draw them in, same
// as how they'd have arrived live. Capped defensively - confirmed in
// production that a bug (since fixed - see tradierHub.js's
// recentlyRecordedTrades) could inflate a single symbol's daily count
// into six figures (SPY hit 102,000+ from duplicate recording across
// reconnects), which froze the browser trying to render it all. Lowered
// from an earlier 5000 to 300, specifically to keep Replay's history
// view focused and manageable rather than dumping the whole day - initial
// page load shows only recent/live flow (a separate, smaller mechanism -
// see flowBuffer), and this cap governs what Replay itself shows when
// clicked.
const MAX_HISTORY_EVENTS = 300;

async function getFlowHistory(symbol, nowMs = Date.now()) {
  const start = sessionStarts(nowMs);
  // Grabs the MOST RECENT N events (DESC + LIMIT), then re-sorts that
  // subset back into chronological order for the caller - a plain
  // ASC + LIMIT would instead grab the EARLIEST N events of the day,
  // which was harmless when the cap was 5000 (most symbols have far
  // fewer events than that in a day) but would be actively wrong now
  // that it's 300 - the oldest 300 trades of the day, not the most
  // recent 300, which isn't what "recent history" should mean.
  const result = await db.query(
    `SELECT * FROM (
       SELECT symbol, asset_type, side, size, value, option_type, strike, expiration, source_symbol,
              EXTRACT(EPOCH FROM event_time) * 1000 AS time_ms
       FROM flow_events
       WHERE symbol = $1 AND event_time > now() - interval '${RETENTION_INTERVAL}'
         AND ((asset_type = 'futures' AND event_time >= $2) OR (asset_type <> 'futures' AND event_time >= $3))
       ORDER BY event_time DESC
       LIMIT ${MAX_HISTORY_EVENTS}
     ) recent
     ORDER BY time_ms ASC`,
    [symbol, start.futures, start.standard]
  );
  return result.rows.map((row) => ({
    symbol: row.symbol,
    assetType: row.asset_type,
    side: row.side,
    size: row.size != null ? parseFloat(row.size) : null,
    value: parseFloat(row.value),
    optionType: row.option_type,
    strike: row.strike != null ? parseFloat(row.strike) : null,
    expiration: row.expiration,
    sourceSymbol: row.source_symbol,
    timeMs: Math.round(parseFloat(row.time_ms)),
  }));
}

// Same shape as getFlowHistory, but across several symbols at once and
// restricted to a specific assetType - used for Background Flow, which
// spans BACKGROUND_FLOW_SYMBOLS (15 tickers) and must stay separate from
// each of those tickers' own regular flow_events entries (recorded at a
// much lower $25k threshold by the normal per-ticker watch), even though
// both live in this same table under the same symbol.
async function getFlowHistoryForSymbols(symbols, assetType) {
  const result = await db.query(
    `SELECT * FROM (
       SELECT symbol, asset_type, side, size, value, option_type, strike, expiration, source_symbol,
              EXTRACT(EPOCH FROM event_time) * 1000 AS time_ms
       FROM flow_events
       WHERE symbol = ANY($1) AND asset_type = $2 AND event_time > now() - interval '${RETENTION_INTERVAL}'
       ORDER BY event_time DESC
       LIMIT ${MAX_HISTORY_EVENTS}
     ) recent
     ORDER BY time_ms ASC`,
    [symbols, assetType]
  );
  return result.rows.map((row) => ({
    symbol: row.symbol,
    assetType: row.asset_type,
    side: row.side,
    size: row.size != null ? parseFloat(row.size) : null,
    value: parseFloat(row.value),
    optionType: row.option_type,
    strike: row.strike != null ? parseFloat(row.strike) : null,
    expiration: row.expiration,
    sourceSymbol: row.source_symbol,
    timeMs: Math.round(parseFloat(row.time_ms)),
  }));
}

async function purgeOldFlowEvents() {
  try {
    const result = await db.query(`DELETE FROM flow_events WHERE event_time <= now() - interval '${RETENTION_INTERVAL}'`);
    if (result.rowCount > 0) console.log(`[flowHistory] purged ${result.rowCount} event(s) past the ${RETENTION_INTERVAL} retention window`);
  } catch (err) {
    console.error('[flowHistory] purge failed:', err.message);
  }
}

module.exports = { recordFlowEvent, getFlowHistory, getFlowHistoryForSymbols, purgeOldFlowEvents, mostRecentEtHour, sessionStarts };
