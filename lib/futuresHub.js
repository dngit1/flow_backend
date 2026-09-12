// Handles futures tickers (ES, NQ, etc.) via Massive's API.
//
// UPGRADED to Massive's paid real-time tier - uses a real shared
// WebSocket connection instead of the old 25s REST polling the free tier
// required. Historical bars (getHistoricalBars) still use the REST
// endpoint, unchanged.
//
// Confirmed connection details:
//   wss://socket.massive.com/futures   <- REAL-TIME (what we use, paid tier)
//   wss://delayed.massive.com/futures  <- DELAYED demo tier, NOT what we want
//
// Channels subscribed over the one shared connection:
//   AM.<TICKER> - aggregate per-minute bars -> drives the live price/candle
//                 (ONLY for tickers a client is DIRECTLY watching - never
//                 sent for a sibling pulled in just for flow purposes)
//   T.<TICKER>  - tick-level trades -> drives large-print detection
//   Q.<TICKER>  - best bid/ask -> used to classify a trade as BUY or SELL
//
// BIDIRECTIONAL MICRO/STANDARD FLOW COMBINING: standard contracts (ES) and
// their micro counterpart (MES) are treated as one "family" for flow
// purposes. Watching EITHER one directly pulls in the OTHER's trades+quotes
// in the background (flow-only, no candle impact), and a large trade on
// EITHER gets converted to real dollar value (using each contract's own
// multiplier) and reported to whichever member(s) of the family are
// currently being directly watched. If both ESZ5 and MESZ5 happen to be
// watched by different clients at once, a single trade on either one gets
// reported to both - each sees the same combined flow, correctly attributed.
//
// Per Massive's changelog, futures market data (including trades and
// quotes) was "promoted from public beta to stable v1" - at the time the
// price-only upgrade was built, the individual Trades/Quotes doc pages
// still showed stale "beta/coming soon" text, so if large-print detection
// doesn't seem to work, double check those channels are genuinely live.
//
// Trade message field names are inferred from Massive's consistent pattern
// across asset classes (stocks/options both use this shape for "T"
// events) since a futures-specific JSON example wasn't available at
// build time - {"ev":"T","sym":...,"x":...,"p":price,"s":size,"c":[...],
// "t":timestamp_ms,"q":sequence}. The raw message is logged once per
// symbol on first receipt so this can be corrected quickly if wrong.

const WebSocket = require('ws');

const FUTURES_TICKER_PATTERN = /^[A-Z]{1,3}[FGHJKMNQUVXZ]\d$/; // e.g. ESZ5, NQZ5
const MASSIVE_WS_URL = 'wss://socket.massive.com/futures';
const RECONNECT_DELAY_MS = 2000;

// Dollar value per 1.00 move in price, per contract. Futures notional
// value is huge relative to stocks (even a single ES contract is ~$300k+
// notional), so this threshold is set much higher than the stock feature's
// $500k. Unrecognized roots fall back to a multiplier of 1 (i.e. treated
// as raw price*size "notional units," not a guaranteed accurate dollar
// figure) - add more roots here as needed.
const CONTRACT_MULTIPLIERS = {
  ES: 50,     // E-mini S&P 500
  NQ: 20,     // E-mini Nasdaq-100
  YM: 5,      // E-mini Dow
  RTY: 50,    // E-mini Russell 2000
  GC: 100,    // Gold (100 troy oz)
  CL: 1000,   // Crude Oil (1,000 barrels)
  // Micro versions - 1/10th their corresponding E-mini/standard contract.
  MES: 5,     // Micro E-mini S&P 500
  MNQ: 2,     // Micro E-mini Nasdaq-100
  MYM: 0.5,   // Micro E-mini Dow
  M2K: 5,     // Micro E-mini Russell 2000
  MGC: 10,    // Micro Gold
  MCL: 100,   // Micro Crude Oil
};

// Standard root -> micro root, and its reverse - used to find either
// member of a family regardless of which one you start from.
const MICRO_FOR_ROOT = {
  ES: 'MES',
  NQ: 'MNQ',
  YM: 'MYM',
  RTY: 'M2K',
  GC: 'MGC',
  CL: 'MCL',
};
const STANDARD_FOR_MICRO_ROOT = Object.fromEntries(
  Object.entries(MICRO_FOR_ROOT).map(([standardRoot, microRoot]) => [microRoot, standardRoot])
);

const FUTURES_BLOCK_TRADE_THRESHOLD = 2_000_000; // dollar value - a starting guess, adjust once you see real print sizes

function isFuturesTicker(symbol) {
  return FUTURES_TICKER_PATTERN.test(symbol);
}

function getRoot(ticker) {
  const match = ticker.match(/^([A-Z]{1,3})[FGHJKMNQUVXZ]\d$/);
  return match ? match[1] : null;
}

function getSuffix(ticker) {
  // month code + year digit, e.g. "Z5" from "ESZ5"
  const match = ticker.match(/^[A-Z]{1,3}([FGHJKMNQUVXZ]\d)$/);
  return match ? match[1] : null;
}

function getMultiplier(ticker) {
  return CONTRACT_MULTIPLIERS[getRoot(ticker)] || 1;
}

// Returns the OTHER member of a ticker's micro/standard family, working in
// either direction - "ESZ5" -> "MESZ5", or "MESZ5" -> "ESZ5". Returns null
// if this root has no known family counterpart.
function getSiblingTicker(ticker) {
  const root = getRoot(ticker);
  const suffix = getSuffix(ticker);
  if (!root || !suffix) return null;
  if (MICRO_FOR_ROOT[root]) return `${MICRO_FOR_ROOT[root]}${suffix}`;
  if (STANDARD_FOR_MICRO_ROOT[root]) return `${STANDARD_FOR_MICRO_ROOT[root]}${suffix}`;
  return null;
}

function createFuturesHub({ apiKey, onPrice, onBigTrade }) {
  const bufferedCache = new Map(); // ticker -> { value, expiresAt } - unchanged, still used by getHistoricalBars
  const watchers = new Map(); // ticker -> Set<clientId> - tickers DIRECTLY watched by a client (get AM/candle too)
  // siblingTicker -> Set of primary tickers whose watchers pulled it in for
  // flow-only purposes. Ref-counted since more than one primary ticker
  // could theoretically request the same sibling.
  const flowOnlyRequestedBy = new Map();
  const lastQuote = new Map(); // ticker -> { bid, ask } (keyed by whichever ticker actually traded)
  const lastTradePrice = new Map(); // ticker -> price, for uptick/downtick fallback
  const loggedTradeSample = new Set(); // tickers we've already logged one raw trade message for

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

  function channelParams(symbols, includeAggregates) {
    const parts = [];
    for (const s of symbols) {
      if (includeAggregates) parts.push(`AM.${s}`);
      parts.push(`T.${s}`, `Q.${s}`);
    }
    return parts.join(',');
  }

  function sendSubscribe(symbols, includeAggregates) {
    if (!symbols.length || !(socket && socket.readyState === WebSocket.OPEN && authed)) return;
    socket.send(JSON.stringify({ action: 'subscribe', params: channelParams(symbols, includeAggregates) }));
  }

  function sendUnsubscribe(symbols, includeAggregates) {
    if (!symbols.length || !(socket && socket.readyState === WebSocket.OPEN && authed)) return;
    socket.send(JSON.stringify({ action: 'unsubscribe', params: channelParams(symbols, includeAggregates) }));
    for (const s of symbols) {
      lastQuote.delete(s);
      lastTradePrice.delete(s);
      loggedTradeSample.delete(s);
    }
  }

  // Ensures `sibling`'s trades+quotes are flowing, attributed to
  // `primaryTicker`'s request. No-ops if the sibling is already someone's
  // OWN directly-watched ticker (it already gets T+Q, no need to duplicate).
  function addFlowOnlySub(primaryTicker, sibling) {
    if (watchers.has(sibling)) return;
    let requestors = flowOnlyRequestedBy.get(sibling);
    const isNew = !requestors || requestors.size === 0;
    if (!requestors) {
      requestors = new Set();
      flowOnlyRequestedBy.set(sibling, requestors);
    }
    requestors.add(primaryTicker);
    if (isNew) sendSubscribe([sibling], false);
  }

  function removeFlowOnlySub(primaryTicker, sibling) {
    const requestors = flowOnlyRequestedBy.get(sibling);
    if (!requestors) return;
    requestors.delete(primaryTicker);
    if (requestors.size === 0) {
      flowOnlyRequestedBy.delete(sibling);
      if (!watchers.has(sibling)) sendUnsubscribe([sibling], false);
    }
  }

  function handleTrade(msg) {
    const tradedSymbol = msg.sym;

    if (!loggedTradeSample.has(tradedSymbol)) {
      loggedTradeSample.add(tradedSymbol);
      console.log(`[futures] Sample raw trade message for ${tradedSymbol} (verify field names look right):`, JSON.stringify(msg));
    }

    const price = msg.p;
    const size = msg.s;
    const prevPrice = lastTradePrice.get(tradedSymbol);
    lastTradePrice.set(tradedSymbol, price);

    // Always use the ACTUALLY TRADED contract's own multiplier - a micro
    // fill must be converted using the micro multiplier, not the
    // standard's, regardless of which ticker(s) it ends up displayed under.
    const multiplier = getMultiplier(tradedSymbol);
    const dollarValue = price * size * multiplier;
    if (dollarValue < FUTURES_BLOCK_TRADE_THRESHOLD) return;

    const quote = lastQuote.get(tradedSymbol);
    let side;
    if (quote?.bid > 0 && quote?.ask > 0) {
      if (price >= quote.ask) side = 'BUY';
      else if (price <= quote.bid) side = 'SELL';
      else side = (prevPrice != null && price < prevPrice) ? 'SELL' : 'BUY';
    } else {
      side = (prevPrice != null && price < prevPrice) ? 'SELL' : 'BUY';
    }

    // Report to whichever member(s) of this family are DIRECTLY watched -
    // could be the traded ticker itself, its sibling, or both at once if
    // different clients are watching each side of the pair.
    const sibling = getSiblingTicker(tradedSymbol);
    const targets = [];
    if (watchers.has(tradedSymbol)) targets.push(tradedSymbol);
    if (sibling && watchers.has(sibling)) targets.push(sibling);
    if (targets.length === 0) targets.push(tradedSymbol); // safety net, shouldn't normally happen

    for (const target of targets) {
      onBigTrade?.(target, { price, size, dollarValue, side, timeMs: msg.t });
    }
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
          // Resubscribe everything on (re)connect - directly-watched
          // tickers with their candle-driving AM channel, sibling
          // flow-only subs without it.
          sendSubscribe([...watchers.keys()], true);
          sendSubscribe([...flowOnlyRequestedBy.keys()], false);
        } else if (msg.ev === 'status' && msg.status === 'auth_failed') {
          console.error('[futures] WebSocket auth failed - check MASSIVE_API_KEY');
        } else if (msg.ev === 'AM') {
          onPrice(msg.sym, { price: msg.c, timeMs: msg.e });
        } else if (msg.ev === 'Q') {
          lastQuote.set(msg.sym, { bid: msg.bp, ask: msg.ap });
        } else if (msg.ev === 'T') {
          handleTrade(msg);
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
      if (isNewTicker) {
        sendSubscribe([ticker], true);

        const sibling = getSiblingTicker(ticker);
        if (sibling) addFlowOnlySub(ticker, sibling);
      }
    },

    unsubscribe(clientId, ticker) {
      const set = watchers.get(ticker);
      if (!set) return;
      set.delete(clientId);
      if (set.size === 0) {
        watchers.delete(ticker);
        sendUnsubscribe([ticker], true);

        const sibling = getSiblingTicker(ticker);
        if (sibling) removeFlowOnlySub(ticker, sibling);
      }
    },

    unsubscribeAll(clientId) {
      for (const [ticker, set] of watchers) {
        if (set.delete(clientId) && set.size === 0) {
          watchers.delete(ticker);
          sendUnsubscribe([ticker], true);

          const sibling = getSiblingTicker(ticker);
          if (sibling) removeFlowOnlySub(ticker, sibling);
        }
      }
    },

    watchersOf(ticker) {
      return watchers.get(ticker) || new Set();
    },
  };
}

module.exports = { createFuturesHub, isFuturesTicker };
