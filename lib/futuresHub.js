// Handles futures tickers (ES, NQ, etc.) via Massive's API.
//
// IMPORTANT LIMITATION #1: on the free "Basic" tier, Massive has NO
// WebSocket access at all - only REST.
//
// IMPORTANT LIMITATION #2 (bigger than #1): per Massive's own docs, the
// Basic tier's data recency is capped at "8-hour historical" - meaning the
// underlying data itself can be up to 8 hours stale, REGARDLESS of how
// often we poll. Polling faster does not make this fresher. So on the free
// tier, treat this as "historical with occasional refresh," not "live."
// Starter/Developer tiers improve this to "10-minute delayed" (still not
// real-time); only Advanced ($199/mo) gives true real-time data.
//
// Rate limit constraint: free tier allows only 5 API calls/minute TOTAL.
// With a 25s poll interval per distinct watched symbol, 2 symbols costs
// ~4.8 calls/min - safely under budget, but don't lower this much further
// if more than 2 futures symbols might ever be watched at once.
//
// When you upgrade to a paid tier with WebSocket + Trades access, this
// polling approach should be replaced with a real streaming connection
// (mirroring how alpacaHub.js works) for both lower latency and to stop
// wasting the increased-but-still-finite call budget on polling.

const FUTURES_TICKER_PATTERN = /^[A-Z]{1,3}[FGHJKMNQUVXZ]\d$/; // e.g. ESZ5, NQZ5
const POLL_INTERVAL_MS = 25_000;

function isFuturesTicker(symbol) {
  return FUTURES_TICKER_PATTERN.test(symbol);
}

function createFuturesHub({ apiKey, onPrice }) {
  const bufferedCache = new Map(); // ticker -> { value, expiresAt }
  const watchers = new Map(); // ticker -> Set<clientId>
  const pollTimers = new Map(); // ticker -> intervalId
  const lastBarTime = new Map(); // ticker -> last broadcast bar's timestamp, to avoid duplicate broadcasts

  function getCached(key, ttlMs, fn) {
    const entry = bufferedCache.get(key);
    if (entry && Date.now() < entry.expiresAt) return Promise.resolve(entry.value);
    return fn().then((value) => {
      bufferedCache.set(key, { value, expiresAt: Date.now() + ttlMs });
      return value;
    });
  }

  async function fetchBars(ticker) {
    return getCached(`bars:${ticker}`, 20_000, async () => {
      // Correct futures-specific endpoint (NOT the generic /v2/aggs/ path,
      // which is Polygon/Massive's stocks endpoint and doesn't recognize
      // futures contract tickers - confirmed via massive.com/docs/rest/futures/aggregates).
      // Omitting window_start returns the most recent candles automatically.
      const url = `https://api.massive.com/futures/v1/aggs/${encodeURIComponent(ticker)}` +
        `?resolution=1min&limit=60&sort=window_start.desc&apiKey=${apiKey}`;

      const res = await fetch(url);
      const data = await res.json();
      if (!res.ok || data.status !== 'OK') {
        console.error(`[futures] Massive request failed for ${ticker}:`, data.error || data.status || res.status);
        throw new Error(`Price data request failed for ${ticker}`);
      }

      const results = data.results || [];
      if (results.length === 0) throw new Error(`No price history available for ${ticker}`);

      // window_start is NANOSECOND epoch, not milliseconds - divide by 1e9,
      // not 1000. Results come back newest-first (per our sort param), so
      // reverse to ascending order for the chart.
      return results
        .slice()
        .reverse()
        .map((bar) => ({
          time: Math.floor(bar.window_start / 1e9),
          open: bar.open,
          high: bar.high,
          low: bar.low,
          close: bar.close,
        }));
    });
  }

  async function pollLatestBar(ticker) {
    try {
      const bars = await fetchBars(ticker); // reuses/refreshes the same 20s cache
      const last = bars[bars.length - 1];
      if (!last) return;

      // Only broadcast if this is actually a new bar we haven't sent yet -
      // avoids spamming identical "updates" every poll when nothing changed.
      if (lastBarTime.get(ticker) === last.time) return;
      lastBarTime.set(ticker, last.time);

      onPrice(ticker, { price: last.close, timeMs: last.time * 1000 });
    } catch (err) {
      console.error(`[futures] poll failed for ${ticker}:`, err.message);
    }
  }

  function startPolling(ticker) {
    if (pollTimers.has(ticker)) return;
    pollLatestBar(ticker); // fire once immediately, don't wait for the first interval tick
    const id = setInterval(() => pollLatestBar(ticker), POLL_INTERVAL_MS);
    pollTimers.set(ticker, id);
  }

  function stopPolling(ticker) {
    const id = pollTimers.get(ticker);
    if (id) clearInterval(id);
    pollTimers.delete(ticker);
    lastBarTime.delete(ticker);
  }

  return {
    isFuturesTicker,

    async getHistoricalBars(ticker) {
      return fetchBars(ticker);
    },

    subscribe(clientId, ticker) {
      let set = watchers.get(ticker);
      if (!set) {
        set = new Set();
        watchers.set(ticker, set);
      }
      set.add(clientId);
      startPolling(ticker);
    },

    unsubscribe(clientId, ticker) {
      const set = watchers.get(ticker);
      if (!set) return;
      set.delete(clientId);
      if (set.size === 0) {
        watchers.delete(ticker);
        stopPolling(ticker);
      }
    },

    unsubscribeAll(clientId) {
      for (const [ticker, set] of watchers) {
        if (set.delete(clientId) && set.size === 0) {
          watchers.delete(ticker);
          stopPolling(ticker);
        }
      }
    },

    watchersOf(ticker) {
      return watchers.get(ticker) || new Set();
    },
  };
}

module.exports = { createFuturesHub, isFuturesTicker };
