const db = require('./db');

// Longer than flow history's 1-day retention - the whole point of this
// table is accumulating enough history to analyze patterns (like the
// flow-vs-subsequent-price-movement question that prompted building this)
// over more than a single day. Still bounded, not kept forever.
const RETENTION_INTERVAL = "30 days";

// Records one completed bar (upserts on symbol+timeframe+bar_time - see
// the migration's UNIQUE constraint - since the source resends the same
// minute's bar repeatedly as it develops, and we only want the latest/
// final version kept, not a new row every resend). Fire-and-forget from
// the caller's perspective: failures are logged, never thrown, so a
// database hiccup can't break live price broadcasting - saving history
// is secondary to showing it live.
async function recordPriceBar(bar) {
  try {
    await db.query(
      `INSERT INTO price_bars (symbol, timeframe, bar_time, open, high, low, close, volume)
       VALUES ($1, $2, to_timestamp($3::double precision / 1000), $4, $5, $6, $7, $8)
       ON CONFLICT (symbol, timeframe, bar_time)
       DO UPDATE SET open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                     close = EXCLUDED.close, volume = EXCLUDED.volume`,
      [bar.symbol, bar.timeframe || '1min', bar.barTimeMs, bar.open, bar.high, bar.low, bar.close, bar.volume ?? null]
    );
  } catch (err) {
    console.error('[priceHistory] failed to record bar:', err.message);
  }
}

// Returns saved bars for one symbol+timeframe, oldest first. sinceMs is
// optional - omit it to get everything within the retention window.
async function getPriceHistory(symbol, timeframe = '1min', sinceMs) {
  const params = [symbol, timeframe];
  let whereClause = `symbol = $1 AND timeframe = $2 AND bar_time > now() - interval '${RETENTION_INTERVAL}'`;
  if (sinceMs) {
    params.push(new Date(sinceMs).toISOString());
    whereClause += ` AND bar_time >= $${params.length}`;
  }

  const result = await db.query(
    `SELECT symbol, timeframe, EXTRACT(EPOCH FROM bar_time) * 1000 AS time_ms, open, high, low, close, volume
     FROM price_bars
     WHERE ${whereClause}
     ORDER BY bar_time ASC`,
    params
  );
  return result.rows.map((row) => ({
    symbol: row.symbol,
    timeframe: row.timeframe,
    timeMs: Math.round(parseFloat(row.time_ms)),
    open: parseFloat(row.open),
    high: parseFloat(row.high),
    low: parseFloat(row.low),
    close: parseFloat(row.close),
    volume: row.volume != null ? parseFloat(row.volume) : null,
  }));
}

async function purgeOldPriceBars() {
  try {
    const result = await db.query(`DELETE FROM price_bars WHERE bar_time <= now() - interval '${RETENTION_INTERVAL}'`);
    if (result.rowCount > 0) console.log(`[priceHistory] purged ${result.rowCount} bar(s) past the ${RETENTION_INTERVAL} retention window`);
  } catch (err) {
    console.error('[priceHistory] purge failed:', err.message);
  }
}

module.exports = { recordPriceBar, getPriceHistory, purgeOldPriceBars };
