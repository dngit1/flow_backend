'use strict';

// Candles for timeframes that no data provider offers directly (2m and 3m),
// built from 1-minute candles.
//
// Why this exists: Twelve Data, Tradier's time & sales and Massive's futures
// history all offer 1 / 5 / 15 / 30-minute candles, but not 2 or 3. The live
// candle on the page is already bucketed by clock time (floor(time / interval)),
// so history has to be bucketed the same way or the last history candle and
// the first live candle would not line up.
//
// Alignment: a candle starts at a multiple of its length counted from the Unix
// epoch. For US markets that is the same grid TradingView draws, because every
// session boundary (4:00 AM and 9:30 AM ET for stocks, 6:00 PM ET for futures)
// is a whole number of minutes after midnight that divides evenly by 2 and by
// 3, and US UTC offsets are whole hours.

const MAX_BASE_BARS = 5000; // Twelve Data's per-request ceiling

const DERIVED_INTERVALS = {
  '2min': { base: '1min', factor: 2, seconds: 120 },
  '3min': { base: '1min', factor: 3, seconds: 180 },
};

function isDerivedInterval(interval) {
  return Object.prototype.hasOwnProperty.call(DERIVED_INTERVALS, interval);
}

// Combines 1-minute candles into candles `bucketSeconds` long.
//  - open = first candle's open, close = last candle's close,
//    high / low = the extremes, volume = the sum.
//  - A bucket built from fewer candles than it could hold (a minute with no
//    trades has no candle) is kept as is, not dropped.
//  - Input may be unsorted or contain a repeated timestamp (the later row
//    wins); candles with missing / non-numeric prices are skipped.
function aggregateBars(bars, bucketSeconds) {
  const byTime = new Map();
  for (const b of bars) {
    if (!b || !Number.isFinite(b.time)) continue;
    if (![b.open, b.high, b.low, b.close].every(Number.isFinite)) continue;
    byTime.set(b.time, b);
  }
  const sorted = [...byTime.values()].sort((a, b) => a.time - b.time);

  const out = [];
  let cur = null;
  for (const b of sorted) {
    const volume = Number.isFinite(b.volume) ? b.volume : 0;
    const bucket = Math.floor(b.time / bucketSeconds) * bucketSeconds;
    if (!cur || cur.time !== bucket) {
      cur = { time: bucket, open: b.open, high: b.high, low: b.low, close: b.close, volume };
      out.push(cur);
    } else {
      cur.high = Math.max(cur.high, b.high);
      cur.low = Math.min(cur.low, b.low);
      cur.close = b.close;
      cur.volume += volume;
    }
  }
  return out;
}

// Returns the newest `count` candles of a derived interval, using
// `fetchBase(baseInterval, n)` to get the newest `n` 1-minute candles
// (oldest first) from whichever provider the caller is using.
async function fetchDerivedBars(interval, count, fetchBase) {
  const spec = DERIVED_INTERVALS[interval];
  if (!spec) throw new Error(`${interval} is not a derived interval`);

  const wanted = Math.max(1, Math.floor(count));
  // Two extra candles' worth: one for the oldest candle (dropped below) and
  // one of slack.
  const baseCount = Math.min(MAX_BASE_BARS, (wanted + 2) * spec.factor);
  const base = await fetchBase(spec.base, baseCount);
  const agg = aggregateBars(base, spec.seconds);

  // If the provider had more history than we asked for, the cut-off likely
  // landed mid-candle, so the oldest candle may be missing its first
  // minute(s). Drop it rather than show a falsely short candle. If we got
  // everything the provider has, that oldest candle is genuine - keep it.
  // (In ordinary sizes the two extra candles requested above already push a
  // cut-off candle out of the back of the result; this matters when the
  // request hit the provider ceiling and there is no spare candle to lose.)
  const cutOff = base.length >= baseCount;
  const usable = cutOff && agg.length > 1 ? agg.slice(1) : agg;
  return usable.slice(-wanted);
}

module.exports = { DERIVED_INTERVALS, isDerivedInterval, aggregateBars, fetchDerivedBars, MAX_BASE_BARS };
