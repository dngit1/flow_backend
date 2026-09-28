// Tradier-backed data for INDEX symbols (see indexSymbols.js for why
// indexes can't use the stock feed). Per Tradier's own documentation
// (Market Data page, updated Feb 2026), indices are real-time on the
// Brokerage API.

const { isIndexSymbol } = require('./indexSymbols');

const AUTH = (token) => ({ Authorization: `Bearer ${token}`, Accept: 'application/json' });

// URL for Tradier's option-expirations list. For an index this MUST ask for
// all option roots: per Tradier's docs, some underlyings use a different
// symbol for their weekly options (SPX/SPXW, RUT/RUTW), and without
// includeAllRoots only the standard root's expirations come back. For SPX
// that meant ONLY the monthly contracts (Oct 16, Nov 20, Dec 18...) - the
// daily SPXW expirations, where most SPX volume trades, never appeared in
// the dropdown, and Big Flow/premium tracking never saw them either.
// Other symbols get the plain URL, exactly as before.
function expirationsUrl(symbol) {
  const base = `https://api.tradier.com/v1/markets/options/expirations?symbol=${encodeURIComponent(symbol)}`;
  return isIndexSymbol(symbol) ? `${base}&includeAllRoots=true` : base;
}

// The index's current level, plus its previous close (for % change) and
// the time of its last update when Tradier supplies one.
async function fetchIndexQuote(token, symbol) {
  const url = `https://api.tradier.com/v1/markets/quotes?symbols=${encodeURIComponent(symbol)}`;
  const res = await fetch(url, { headers: AUTH(token) });
  if (res.status === 429) {
    const err = new Error('Tradier rate limit hit (429)');
    err.rateLimited = true;
    throw err;
  }
  const data = await res.json().catch(() => null);
  if (!res.ok) throw new Error(`quote request failed (status ${res.status})`);

  const quote = data?.quotes?.quote;
  const q = Array.isArray(quote) ? quote[0] : quote;
  const last = parseFloat(q?.last);
  if (!Number.isFinite(last) || last <= 0) throw new Error('no last price in quote response');

  const prevclose = parseFloat(q?.prevclose);
  const tradeDate = Number(q?.trade_date); // epoch milliseconds
  return {
    last,
    prevclose: Number.isFinite(prevclose) && prevclose > 0 ? prevclose : null,
    tradeDateMs: Number.isFinite(tradeDate) && tradeDate > 0 ? tradeDate : null,
  };
}

// Tradier's time & sales supports only these bar sizes. How far back to
// ask for each is chosen so ~260 bars are comfortably available, and stays
// well inside Tradier's stated availability (1min: 20 days, 5min/15min: 40).
const TRADIER_NATIVE_INTERVALS = {
  '1min': { interval: '1min', lookbackDays: 6 },
  '5min': { interval: '5min', lookbackDays: 10 },
  '15min': { interval: '15min', lookbackDays: 20 },
};

const ET_FORMAT_OPTIONS = {
  timeZone: 'America/New_York',
  hour12: false,
  year: 'numeric', month: '2-digit', day: '2-digit',
  hour: '2-digit', minute: '2-digit', second: '2-digit',
};

// "YYYY-MM-DD HH:MM" in US Eastern time - the format Tradier's start/end
// parameters expect.
function formatEtMinute(date) {
  const parts = Object.fromEntries(
    new Intl.DateTimeFormat('en-US', ET_FORMAT_OPTIONS).formatToParts(date).map((p) => [p.type, p.value]),
  );
  const hour = parts.hour === '24' ? '00' : parts.hour; // some runtimes render midnight as 24
  return `${parts.year}-${parts.month}-${parts.day} ${hour}:${parts.minute}`;
}

// Converts a US-Eastern wall-clock time ("2026-09-28 09:30:00" or with a
// "T") into UTC epoch seconds, DST-aware. Only a FALLBACK: Tradier's rows
// normally carry their own epoch `timestamp`, which is used when present.
function etWallTimeToEpochSeconds(str) {
  const m = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?/.exec(String(str));
  if (!m) return null;
  const [, Y, M, D, h, mi, s] = m;
  const wallAsUtc = Date.UTC(+Y, +M - 1, +D, +h, +mi, +(s || 0));

  const offsetAt = (utcMs) => {
    const parts = Object.fromEntries(
      new Intl.DateTimeFormat('en-US', ET_FORMAT_OPTIONS).formatToParts(new Date(utcMs)).map((p) => [p.type, p.value]),
    );
    const shown = Date.UTC(+parts.year, +parts.month - 1, +parts.day, +parts.hour % 24, +parts.minute, +parts.second);
    return shown - utcMs; // e.g. -4h in summer, -5h in winter
  };

  // Two passes so a wall time near a daylight-saving change resolves with
  // the offset that actually applies at the resulting instant.
  const firstGuess = wallAsUtc - offsetAt(wallAsUtc);
  return Math.floor((wallAsUtc - offsetAt(firstGuess)) / 1000);
}

// Candle history for an index from Tradier time & sales, in the same
// {time, open, high, low, close, volume} shape the chart already uses.
// Indexes have no volume, so that field is 0.
async function fetchTradierBars(token, symbol, interval, count = 260) {
  const spec = TRADIER_NATIVE_INTERVALS[interval];
  if (!spec) {
    throw new Error(`${symbol} candles are only available at ${Object.keys(TRADIER_NATIVE_INTERVALS).join(', ')} from Tradier`);
  }

  const start = formatEtMinute(new Date(Date.now() - spec.lookbackDays * 86_400_000));
  const url = `https://api.tradier.com/v1/markets/timesales?symbol=${encodeURIComponent(symbol)}` +
    `&interval=${spec.interval}&start=${encodeURIComponent(start)}&session_filter=open`;
  const res = await fetch(url, { headers: AUTH(token) });
  const data = await res.json().catch(() => null);
  if (!res.ok) throw new Error(`time & sales request failed (status ${res.status})`);

  let rows = data?.series?.data;
  if (rows && !Array.isArray(rows)) rows = [rows]; // a lone row comes back as an object, not an array
  if (!rows || rows.length === 0) throw new Error(`Tradier returned no candles for ${symbol}`);

  const byTime = new Map();
  for (const r of rows) {
    const stamp = Number(r.timestamp);
    const time = Number.isFinite(stamp) && stamp > 0 ? stamp : etWallTimeToEpochSeconds(r.time);
    const open = parseFloat(r.open), high = parseFloat(r.high), low = parseFloat(r.low), close = parseFloat(r.close);
    if (time == null || ![open, high, low, close].every(Number.isFinite)) continue; // skip empty/partial rows
    byTime.set(time, { time, open, high, low, close, volume: 0 });
  }

  const bars = [...byTime.values()].sort((a, b) => a.time - b.time);
  if (bars.length === 0) throw new Error(`Tradier returned no usable candles for ${symbol}`);
  return bars.slice(-count);
}

module.exports = {
  expirationsUrl,
  fetchIndexQuote,
  fetchTradierBars,
  TRADIER_NATIVE_INTERVALS,
  formatEtMinute,
  etWallTimeToEpochSeconds,
};
