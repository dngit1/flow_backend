// Handles futures tickers (ES, NQ, etc.) via Massive's API.
//
// UPGRADED to Massive's paid real-time tier - this now uses a real shared
// WebSocket connection instead of the old 25s REST polling the free tier
// required. Historical bars (getHistoricalBars) still use the REST
// endpoint, unchanged - only the LIVE price mechanism changed.
//
// Confirmed connection details (massive.com/docs/websocket/futures/aggregates-per-minute
// and the official Go client library source, which lists the real hostnames):
//   wss://socket.massive.com/futures   <- REAL-TIME (what we use, paid tier)
//   wss://delayed.massive.com/futures  <- DELAYED demo tier, NOT what we want
//
// Auth:      {"action":"auth","params":"<API_KEY>"}
// Subscribe: {"action":"subscribe","params":"AM.<TICKER>,AM.<TICKER2>"}
// Message:   {"ev":"AM","sym":"6CH5","v":91,"o":6994.5,"c":6995,"h":6995,
//             "l":6994.5,"n":10,"s":<start ms>,"e":<end ms>}
// (AM = Aggregate per-Minute channel - matches our existing 1-minute
// candle granularity, and is what Massive's own docs recommend for
// charting specifically, as opposed to the raw tick-level Trades channel.)
//
// NOTE: as of when this was built, Massive's futures Trades WebSocket
// channel was still marked "beta/coming soon" in their docs - the
// Aggregates channel used here was the one confirmed generally available.
// If large-print detection on futures is ever wanted (mirroring the
// stock share-flow feature), check whether Trades has since gone GA.

const WebSocket = require('ws');

const FUTURES_TICKER_PATTERN = /^[A-Z]{1,3}[FGHJKMNQUVXZ]\d$/; // e.g. ESZ5, NQZ5
const MASSIVE_WS_URL = 'wss://socket.massive.com/futures';
const RECONNECT_DELAY_MS = 2000;

function isFuturesTicker(symbol) {
  return FUTURES_TICKER_PATTERN.test(symbol);
}

function createFuturesHub({ apiKey, onPrice }) {
  const bufferedCache = new Map(); // ticker -> { value, expiresAt } - unchanged, still used by getHistoricalBars
  const watchers = new Map(); // ticker -> Set<clientId>

  let socket = null;
  let authed = false;
  let reconnectTimer = null;

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
      const url = `https://api.massive.com/futures/v1/aggs/${encodeURIComponent(ticker)}` +
        `?resolution=1min&limit=260&sort=window_start.desc&apiKey=${apiKey}`;

      const res = await fetch(url);
      const data = await res.json();
      if (!res.ok || data.status !== 'OK') {
        console.error(`[futures] Massive request failed for ${ticker}:`, data.error || data.status || res.status);
        throw new Error(`Price data request failed for ${ticker}`);
      }

      const results = data.results || [];
      if (results.length === 0) throw new Error(`No price history available for ${ticker}`);

      return results
        .slice()
        .reverse()
        .map((bar) => ({
          time: Math.floor(bar.window_start / 1e9),
          open: bar.open,
          high: bar.high,
          low: bar.low,
          close: bar.close,
          volume: bar.volume || 0,
        }));
    });
  }

  function desiredSymbols() {
    return new Set(watchers.keys());
  }

  function sendSubscribe(symbols) {
    if (!symbols.length || !(socket && socket.readyState === WebSocket.OPEN && authed)) return;
    socket.send(JSON.stringify({ action: 'subscribe', params: symbols.map((s) => `AM.${s}`).join(',') }));
  }

  function sendUnsubscribe(symbols) {
    if (!symbols.length || !(socket && socket.readyState === WebSocket.OPEN && authed)) return;
    socket.send(JSON.stringify({ action: 'unsubscribe', params: symbols.map((s) => `AM.${s}`).join(',') }));
  }

  function ensureConnected() {
    if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) return;

    socket = new WebSocket(MASSIVE_WS_URL);
    authed = false;

    socket.on('open', () => {
      console.log('[futures] WebSocket connected, authenticating...');
      socket.send(JSON.stringify({ action: 'auth', params: apiKey }));
    });

    socket.on('message', (raw) => {
      let messages;
      try { messages = JSON.parse(raw); } catch (e) { return; }
      if (!Array.isArray(messages)) messages = [messages];

      for (const msg of messages) {
        if (msg.ev === 'status' && msg.status === 'auth_success') {
          authed = true;
          console.log('[futures] WebSocket authenticated');
          sendSubscribe([...desiredSymbols()]); // resubscribe everything on (re)connect
        } else if (msg.ev === 'status' && msg.status === 'auth_failed') {
          console.error('[futures] WebSocket auth failed - check MASSIVE_API_KEY');
        } else if (msg.ev === 'AM') {
          // Aggregate-minute bar: use its close price + end timestamp as
          // the live price tick, same shape as the old polling delivered.
          onPrice(msg.sym, { price: msg.c, timeMs: msg.e });
        }
      }
    });

    socket.on('close', () => {
      authed = false;
      console.warn('[futures] WebSocket closed, reconnecting in 2s...');
      clearTimeout(reconnectTimer);
      reconnectTimer = setTimeout(ensureConnected, RECONNECT_DELAY_MS);
    });

    socket.on('error', (err) => {
      console.error('[futures] WebSocket error:', err.message);
    });
  }

  return {
    isFuturesTicker,

    async getHistoricalBars(ticker) {
      return fetchBars(ticker);
    },

    subscribe(clientId, ticker) {
      let set = watchers.get(ticker);
      const isNewTicker = !set;
      if (!set) {
        set = new Set();
        watchers.set(ticker, set);
      }
      set.add(clientId);

      ensureConnected();
      if (isNewTicker) sendSubscribe([ticker]);
    },

    unsubscribe(clientId, ticker) {
      const set = watchers.get(ticker);
      if (!set) return;
      set.delete(clientId);
      if (set.size === 0) {
        watchers.delete(ticker);
        sendUnsubscribe([ticker]);
      }
    },

    unsubscribeAll(clientId) {
      for (const [ticker, set] of watchers) {
        if (set.delete(clientId) && set.size === 0) {
          watchers.delete(ticker);
          sendUnsubscribe([ticker]);
        }
      }
    },

    watchersOf(ticker) {
      return watchers.get(ticker) || new Set();
    },
  };
}

module.exports = { createFuturesHub, isFuturesTicker };
