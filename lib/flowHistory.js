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

// Returns today's (within the retention window) saved events for one
// symbol, oldest first - the order a replay should draw them in, same
// as how they'd have arrived live.
// Returns today's (within the retention window) saved events for one
// symbol, oldest first - the order a replay should draw them in, same
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

async function getFlowHistory(symbol) {
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
       ORDER BY event_time DESC
       LIMIT ${MAX_HISTORY_EVENTS}
     ) recent
     ORDER BY time_ms ASC`,
    [symbol]
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

module.exports = { recordFlowEvent, getFlowHistory, purgeOldFlowEvents };
