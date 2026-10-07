const express = require('express');
const { cached } = require('./cache');
const { isFuturesTicker } = require('./futuresHub');
const { isIndexSymbol } = require('./indexSymbols');
const { isDerivedInterval, fetchDerivedBars } = require('./barAggregation');
const { fetchIndexQuote, fetchTradierBars, fetchExpirationDates, etWallTimeToEpochSeconds } = require('./indexData');

// A partial upload (an older lib/indexData.js next to a newer dataProxy.js)
// otherwise shows up only as a mysterious "Failed to load" in the browser -
// say so plainly, at startup, in the logs.
if (typeof fetchExpirationDates !== 'function') {
  console.error('[startup] lib/indexData.js is out of date: fetchExpirationDates() is missing. Upload the latest lib/indexData.js.');
}

function createDataProxyRouter({ twelveDataKey, tradierToken, lastPriceOf, futuresHub }) {
  const router = express.Router();

  // Previous close (for computing % change) - cached 60s, since it barely
  // changes intraday. Fails gracefully: returns null instead of throwing,
  // so a hiccup here doesn't break the whole /bars response - the frontend
  // just won't show a % change for that load.
  async function fetchPreviousClose(symbol) {
    try {
      return await cached(`prevclose:${symbol}`, 60_000, async () => {
        const url = `https://api.twelvedata.com/quote?symbol=${encodeURIComponent(symbol)}&apikey=${twelveDataKey}`;
        const response = await fetch(url);
        if (!response.ok) throw new Error(`status ${response.status}`);
        const data = await response.json();
        if (data.status === 'error' || data.code) throw new Error(data.message || data.code);
        const prevClose = parseFloat(data.previous_close);
        if (!prevClose || isNaN(prevClose)) throw new Error('no previous_close in response');
        return prevClose;
      });
    } catch (err) {
      console.error(`[dataProxy] previousClose failed for ${symbol}:`, err.message);
      return null;
    }
  }

  // Allowed timeframes. Twelve Data and Massive natively offer all of these
  // except 2min and 3min (Twelve Data's interval list has no 2 or 3), so those
  // two are built from 1-minute candles - see lib/barAggregation.js. Every
  // source (stocks, indexes, futures) does that same derivation at the
  // lowest level, so each caller just asks for "3min" like any other timeframe.
  const ALLOWED_INTERVALS = new Set(['1min', '2min', '3min', '5min', '15min', '30min', '1h', '1day']);

  // ---------------------------------------------------------------
  // GET /api/futures-resolve?root=NQ
  // Resolves a bare futures root (ES, NQ, etc.) to the specific contract
  // currently the active front month (e.g. "NQU6") - so the frontend can
  // let someone type just "NQ" without needing to know which month is
  // currently trading.
  // ---------------------------------------------------------------
  router.get('/futures-resolve', async (req, res) => {
    const root = String(req.query.root || '').toUpperCase();
    if (!root) return res.status(400).json({ error: 'root is required' });

    try {
      const ticker = await futuresHub.resolveFrontMonth(root);
      res.json({ ticker });
    } catch (err) {
      console.error(`[dataProxy] futures-resolve failed for ${root}:`, err.message);
      res.status(502).json({ error: err.message });
    }
  });

  // Twelve Data candles for one symbol/interval, in the chart's own
  // {time, open, high, low, close, volume} shape. Pulled out of the /bars
  // route so the index path below can reuse it - the stock path's
  // behavior is unchanged.
  async function fetchTwelveDataBars(symbol, interval, outputsizeOverride) {
    // 2min / 3min: Twelve Data has no such interval, so combine 1-minute
    // candles (same bar count as any other timeframe).
    if (isDerivedInterval(interval)) {
      return fetchDerivedBars(interval, outputsizeOverride || 260, (baseInterval, n) => fetchTwelveDataBars(symbol, baseInterval, n));
    }
    // outputsize=260: MA200 needs 200 bars just to produce its first
    // value, so we fetch well beyond a minimal window to leave a
    // meaningful stretch of visible MA200 line, not just a single point.
    // Same 260 count at every interval - 260 hourly bars is still
    // plenty of history, just spanning a wider calendar range.
    //
    // outputsizeOverride exists for VWAP, which needs the full session's
    // bars (often more than 260) to compute correctly, independent of
    // the visible chart's own bar count.
    const outputsize = outputsizeOverride || 260;
    const url = `https://api.twelvedata.com/time_series` +
      `?symbol=${encodeURIComponent(symbol)}&interval=${interval}&outputsize=${outputsize}` +
      `&timezone=UTC&apikey=${twelveDataKey}`;

    const response = await fetch(url);
    // Read the body regardless of status - a non-2xx response often
    // still has a useful JSON error message (e.g. rate limit details),
    // which a status-code-only error was previously discarding.
    const data = await response.json().catch(() => null);
    if (!response.ok) {
      throw new Error(`Price data request failed (status ${response.status}): ${data?.message || data?.code || 'no further detail in response'}`);
    }
    if (data.status === 'error' || data.code) {
      throw new Error(`Price data error: ${data.message || data.code}`);
    }

    const values = data.values || [];
    if (values.length === 0) throw new Error('No price history available for this symbol');

    return values
      .slice()
      .reverse()
      .map((v) => ({
        time: Math.floor(Date.parse(`${v.datetime.replace(' ', 'T')}Z`) / 1000),
        open: parseFloat(v.open),
        high: parseFloat(v.high),
        low: parseFloat(v.low),
        close: parseFloat(v.close),
        volume: parseFloat(v.volume) || 0,
      }));
  }

  // Candle history for an INDEX (SPX etc.). Twelve Data is tried first,
  // but it's sent the bare symbol "SPX", which may resolve to a completely
  // different instrument (the stock feed's "SPX" trades around $0.10) - so
  // its result is checked against Tradier's live index level and only used
  // if the two roughly agree. Otherwise, Tradier's own candles are used
  // (1/5/15-minute only - all Tradier's time & sales offers). What
  // happened is logged either way, so which source is in play is visible.
  const INDEX_SANITY_TOLERANCE = 0.05; // last candle must be within 5% of the live index level
  async function getIndexBars(symbol, interval) {
    // Authoritative current level. Throws (surfacing a clear error) if
    // Tradier has no price - better than charting something unverified.
    const quote = await fetchIndexQuote(tradierToken, symbol);

    let bars = null;
    let source = null;
    try {
      const tdBars = await fetchTwelveDataBars(symbol, interval);
      const lastClose = tdBars[tdBars.length - 1].close;
      const apart = Math.abs(lastClose - quote.last) / quote.last;
      console.log(`[dataProxy] ${symbol} ${interval}: Twelve Data returned ${tdBars.length} bars (first close ${tdBars[0].close}, last close ${lastClose}); Tradier index level ${quote.last} - ${(apart * 100).toFixed(2)}% apart`);
      if (apart <= INDEX_SANITY_TOLERANCE) {
        bars = tdBars;
        source = 'twelvedata';
      } else {
        console.warn(`[dataProxy] ${symbol} ${interval}: Twelve Data's "${symbol}" doesn't match the real index level - probably a different instrument. Using Tradier candles instead.`);
      }
    } catch (err) {
      console.warn(`[dataProxy] ${symbol} ${interval}: Twelve Data failed (${err.message}) - trying Tradier candles`);
    }

    if (!bars) {
      bars = await fetchTradierBars(tradierToken, symbol, interval, 260);
      source = 'tradier-timesales';
      console.log(`[dataProxy] ${symbol} ${interval}: using ${bars.length} Tradier candles (last close ${bars[bars.length - 1].close})`);
    }

    return { bars, previousClose: quote.prevclose, source };
  }

  // ---------------------------------------------------------------
  // GET /api/bars?symbol=AAPL&interval=5min (or a futures ticker like ESZ5)
  // Historical bars for seeding the candlestick chart, at whichever
  // timeframe is requested. Stock tickers -> Twelve Data (cached 20s).
  // Futures tickers -> Massive, via futuresHub (which has its own caching).
  // ---------------------------------------------------------------
  router.get('/bars', async (req, res) => {
    const symbol = String(req.query.symbol || '').toUpperCase();
    if (!symbol) return res.status(400).json({ error: 'symbol is required' });

    const interval = String(req.query.interval || '1min');
    if (!ALLOWED_INTERVALS.has(interval)) {
      return res.status(400).json({ error: `Unsupported interval - use one of: ${[...ALLOWED_INTERVALS].join(', ')}` });
    }

    if (isFuturesTicker(symbol)) {
      try {
        const bars = await futuresHub.getHistoricalBars(symbol, interval);
        const previousClose = await futuresHub.getPreviousClose(symbol);
        return res.json({ bars, previousClose });
      } catch (err) {
        console.error(`[dataProxy] futures bars failed for ${symbol}:`, err.message);
        return res.status(502).json({ error: 'Unable to load price history right now' });
      }
    }

    if (isIndexSymbol(symbol)) {
      try {
        const result = await cached(`indexbars:${symbol}:${interval}`, 20_000, () => getIndexBars(symbol, interval));
        return res.json(result);
      } catch (err) {
        console.error(`[dataProxy] index bars failed for ${symbol}:`, err.message);
        return res.status(502).json({ error: err.message });
      }
    }

    try {
      const bars = await cached(`bars:${symbol}:${interval}`, 20_000, () => fetchTwelveDataBars(symbol, interval));

      res.json({ bars, previousClose: await fetchPreviousClose(symbol) });
    } catch (err) {
      console.error(`[dataProxy] bars failed for ${symbol}:`, err.message);
      res.status(502).json({ error: err.message });
    }
  });

  // Minutes per bar, for turning "how far back is the session start" into
  // "how many bars do we need to request".
  const INTERVAL_MINUTES = { '1min': 1, '2min': 2, '3min': 3, '5min': 5, '15min': 15, '30min': 30, '1h': 60 };

  // Epoch ms of the most recent session-start boundary (hourET:minuteET,
  // US Eastern) that has already passed - today's if we're past it,
  // yesterday's otherwise, so VWAP still shows the last complete session
  // right up until the new one begins (standard VWAP behavior).
  function mostRecentSessionStartMs(hourET, minuteET, anchorMs = Date.now()) {
    const parts = Object.fromEntries(
      new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit' })
        .formatToParts(new Date(anchorMs)).map((p) => [p.type, p.value])
    );
    const y = parseInt(parts.year, 10), m = parseInt(parts.month, 10), d = parseInt(parts.day, 10);
    const hh = String(hourET).padStart(2, '0'), mm = String(minuteET).padStart(2, '0');

    let startMs = etWallTimeToEpochSeconds(`${y}-${String(m).padStart(2, '0')}-${String(d).padStart(2, '0')} ${hh}:${mm}:00`) * 1000;
    if (startMs > anchorMs) {
      // The anchor's own day's boundary hasn't happened yet (relative to
      // the anchor) - step back one CALENDAR day (safe via Date.UTC's own
      // normalization, e.g. day 0 rolls into the previous month) and
      // re-resolve against that date, so a DST transition is still
      // handled correctly rather than just subtracting a fixed 24h in
      // milliseconds.
      const prev = new Date(Date.UTC(y, m - 1, d - 1));
      const py = prev.getUTCFullYear(), pm = prev.getUTCMonth() + 1, pd = prev.getUTCDate();
      startMs = etWallTimeToEpochSeconds(`${py}-${String(pm).padStart(2, '0')}-${String(pd).padStart(2, '0')} ${hh}:${mm}:00`) * 1000;
    }
    return startMs;
  }

  // ---------------------------------------------------------------
  // GET /api/vwap-bars?symbol=AAPL&interval=1min
  // A SEPARATE, larger bar batch covering the full session, specifically
  // for VWAP's own calculation - independent of whatever bar count the
  // visible chart uses (see the limitOverride/outputsizeOverride comments
  // in futuresHub.js and this file's fetchTwelveDataBars).
  // ---------------------------------------------------------------
  router.get('/vwap-bars', async (req, res) => {
    const symbol = String(req.query.symbol || '').toUpperCase();
    if (!symbol) return res.status(400).json({ error: 'symbol is required' });

    const interval = String(req.query.interval || '1min');
    if (!(interval in INTERVAL_MINUTES)) {
      return res.status(400).json({ error: `VWAP is intraday only - use one of: ${Object.keys(INTERVAL_MINUTES).join(', ')}` });
    }

    // Indexes carry no volume at all (confirmed separately - their feed
    // has nothing to report), and VWAP is undefined without it. Refusing
    // here is a clear, direct error instead of silently returning a flat
    // or meaningless line.
    if (isIndexSymbol(symbol)) {
      return res.status(400).json({ error: `VWAP isn't available for ${symbol} - index symbols have no volume data` });
    }

    const isFutures = isFuturesTicker(symbol);
    // 6 PM ET = the start of the full near-24h futures trading session
    // (standard CME Globex convention). 9:30 AM ET = the regular stock
    // session open. Both are the user's own explicit choice, not a guess.
    const hourET = isFutures ? 18 : 9, minuteET = isFutures ? 0 : 30;
    const fetchBarsFor = (count) => (isFutures
      ? futuresHub.getHistoricalBars(symbol, interval, count)
      : fetchTwelveDataBars(symbol, interval, count));

    // Initial estimate, sized from wall-clock "now" - correct whenever the
    // market is currently open. Can undershoot when it's NOT (e.g. futures
    // on a weekend, between Friday's close and Sunday's reopen): "now" then
    // resolves to a session boundary that's after trading actually
    // stopped, which gets corrected below once we see the real data.
    let sessionStartMs = mostRecentSessionStartMs(hourET, minuteET);
    const initialBarsNeeded = Math.ceil((Date.now() - sessionStartMs) / 60_000 / INTERVAL_MINUTES[interval]) + 10;

    console.log(`[vwap-bars] ${symbol} ${interval}: estimated session start ${new Date(sessionStartMs).toISOString()}, requesting ${initialBarsNeeded} bars (${isFutures ? 'futures' : 'stock'})`);
    try {
      let bars = await fetchBarsFor(initialBarsNeeded);
      console.log(`[vwap-bars] ${symbol} ${interval}: got ${bars.length} bars back (requested ${initialBarsNeeded})`);

      if (bars.length > 0) {
        const newestBarMs = bars[bars.length - 1].time * 1000; // bars are oldest-first
        const correctedSessionStartMs = mostRecentSessionStartMs(hourET, minuteET, newestBarMs);
        if (correctedSessionStartMs !== sessionStartMs) {
          const oldestBarMs = bars[0].time * 1000;
          if (correctedSessionStartMs < oldestBarMs) {
            // The real session the data belongs to started earlier than
            // our wall-clock estimate reached - re-fetch sized for the
            // ACTUAL gap (newest bar back to the corrected start).
            const neededBars = Math.ceil((newestBarMs - correctedSessionStartMs) / 60_000 / INTERVAL_MINUTES[interval]) + 10;
            console.log(`[vwap-bars] ${symbol} ${interval}: re-anchoring to newest bar's own session (${new Date(correctedSessionStartMs).toISOString()}, likely market-closed period) - refetching ${neededBars} bars`);
            bars = await fetchBarsFor(neededBars);
          }
          sessionStartMs = correctedSessionStartMs;
        }
      }

      const inSession = bars.filter((b) => b.time * 1000 >= sessionStartMs).length;
      console.log(`[vwap-bars] ${symbol} ${interval}: final session start ${new Date(sessionStartMs).toISOString()}, ${bars.length} bars total, ${inSession} fall within the session`);
      res.json({ bars, sessionStartMs });
    } catch (err) {
      console.error(`[dataProxy] vwap-bars failed for ${symbol}:`, err.message);
      res.status(502).json({ error: err.message });
    }
  });

  // ---------------------------------------------------------------
  // GET /api/expirations?symbol=AAPL
  // Cached 5 min - expiration lists don't change intraday.
  // ---------------------------------------------------------------
  router.get('/expirations', async (req, res) => {
    const symbol = String(req.query.symbol || '').toUpperCase();
    if (!symbol) return res.status(400).json({ error: 'symbol is required' });

    try {
      const dates = await cached(`expirations:${symbol}`, 5 * 60_000, () => fetchExpirationDates(tradierToken, symbol));

      res.json({ expirations: dates });
    } catch (err) {
      console.error(`[dataProxy] expirations failed for ${symbol}:`, err.message);
      res.status(502).json({ error: err.message });
    }
  });

  // ---------------------------------------------------------------
  // GET /api/chain?symbol=AAPL&expiration=2026-09-11
  // Full chain, cached 15s. The frontend filters to near-the-money
  // strikes itself so the backend doesn't need to know NEAR_MONEY_STRIKES.
  // ---------------------------------------------------------------
  router.get('/chain', async (req, res) => {
    const symbol = String(req.query.symbol || '').toUpperCase();
    const expiration = String(req.query.expiration || '');
    if (!symbol || !expiration) return res.status(400).json({ error: 'symbol and expiration are required' });

    try {
      const options = await cached(`chain:${symbol}:${expiration}`, 15_000, async () => {
        const url = `https://api.tradier.com/v1/markets/options/chains` +
          `?symbol=${encodeURIComponent(symbol)}&expiration=${expiration}&greeks=true`;
        const res2 = await fetch(url, {
          headers: { Authorization: `Bearer ${tradierToken}`, Accept: 'application/json' },
        });
        const data = await res2.json();
        if (!res2.ok) throw new Error(`Option data request failed (status ${res2.status})`);

        let opts = data.options?.option || [];
        if (!Array.isArray(opts)) opts = [opts];
        return opts;
      });

      res.json({ options, lastPrice: lastPriceOf(symbol) || null });
    } catch (err) {
      console.error(`[dataProxy] chain failed for ${symbol}/${expiration}:`, err.message);
      res.status(502).json({ error: err.message });
    }
  });

  return router;
}

module.exports = { createDataProxyRouter };
