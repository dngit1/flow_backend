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

// Permanent background watch: these roots stay subscribed at all times,
// under a fixed pseudo-client (never unsubscribed by a real browser
// disconnecting), so price_bars and flow_events keep accumulating for
// them even with nobody's browser open - the same problem Background
// Flow solves for stocks, but for futures. ES added alongside NQ/GC for
// a complete "other major index" comparison point, not just the two
// symbols that originally prompted this.
const FUTURES_BACKGROUND_ROOTS = ['ES', 'NQ', 'GC'];
const FUTURES_BACKGROUND_CLIENT_ID = '__background__';
const FUTURES_BACKGROUND_REFRESH_MS = 6 * 60 * 60_000; // re-check front months periodically - contracts roll every few months, this catches it well within that window without checking needlessly often

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

const FUTURES_BLOCK_TRADE_THRESHOLD = 3_000_000; // dollar value - what shows up in the Live Flow sidebar at all

// Micro contracts use a multiplier roughly 1/10th their standard sibling's
// (see CONTRACT_MULTIPLIERS above), so a genuinely large MICRO print in
// contract-count terms (comparable to what clears the standard threshold)
// works out to only about 1/10th the dollar notional. Using the standard
// $3M bar for a DIRECTLY-traded micro contract meant it almost never
// qualified (MES needs ~90+ contracts, MGC ~70+, just to reach $3M) - this
// separate, lower threshold reflects an actually-large print for the
// micro product itself, rather than silencing it entirely.
const MICRO_BLOCK_TRADE_THRESHOLD = 2_000_000;

function isMicroTicker(ticker) {
  return Boolean(STANDARD_FOR_MICRO_ROOT[getRoot(ticker)]);
}

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
  const loggedAMSample = new Set(); // tickers we've already logged one AM (aggregate-minute) message for
  const loggedQuoteSample = new Set(); // tickers we've already logged one raw quote message for
  const backgroundWatchedTicker = new Map(); // root -> the specific contract currently held by the permanent background watch, e.g. "NQ" -> "NQZ6"

  let socket = null;
  let authed = false;
  let openedAt = null; // TEMP DIAGNOSTIC: set when 'open' fires, used to compute exact uptime before the next 'close' - added because this connection has been observed cycling every 4-7s, far more aggressively than Tradier's connection (which showed a healthy ~131s uptime before its one observed drop) - remove once resolved
  let reconnectTimer = null;

  function getCached(key, ttlMs, fn) {
    const entry = bufferedCache.get(key);
    if (entry && Date.now() < entry.expiresAt) return Promise.resolve(entry.value);
    return fn().then((value) => {
      bufferedCache.set(key, { value, expiresAt: Date.now() + ttlMs });
      return value;
    });
  }

  // Resolves a bare root symbol (e.g. "NQ") to whichever specific contract
  // is currently the active front month (e.g. "NQU6"), using Massive's
  // Contracts reference endpoint rather than computing a roll schedule
  // ourselves. Cached 24h - the front month only changes every few months
  // for quarterly-cycle products, so this doesn't need frequent refreshing.
  // NOTE: the exact endpoint path/params below are inferred from Massive's
  // documentation (product_code, active, sort params on a /contracts
  // endpoint), not a confirmed working example - if this errors, check
  // Render logs for the exact response Massive returns.
  async function resolveFrontMonth(root) {
    return getCached(`frontmonth:${root}`, 24 * 60 * 60_000, async () => {
      // "sort" only accepts date/product_code/ticker (confirmed via a real
      // error response) - none of those reliably identify the
      // nearest-to-expiry contract on their own (sorting by ticker text
      // isn't safe across year boundaries, e.g. "NQH7" < "NQZ6"
      // alphabetically even though March 2027 is later than Dec 2026), so
      // fetch all active contracts and pick the earliest last_trade_date
      // ourselves instead of relying on the API to order them for us.
      // Explicitly pass today's date - without it, "active=true" appears
      // to not filter against the current date at all (a real response
      // returned NQH0, a March 2020 contract, when this param was
      // omitted). "date" matches one of the confirmed-valid sort field
      // names, which is a strong signal it's also the real query param
      // name for point-in-time filtering here.
      const today = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
      const url = `https://api.massive.com/futures/v1/contracts` +
        `?product_code=${encodeURIComponent(root)}&active=true&date=${today}&limit=50&apiKey=${apiKey}`;

      const res = await fetch(url);
      const data = await res.json();
      if (!res.ok) {
        console.error(`[futures] Massive front-month lookup failed for ${root}:`, JSON.stringify(data));
        throw new Error(`Couldn't resolve the current contract for ${root}`);
      }

      const results = data.results || data.contracts || [];
      if (results.length === 0) {
        console.error(`[futures] Massive front-month lookup returned no active contracts for ${root}:`, JSON.stringify(data));
        throw new Error(`No active contract found for ${root}`);
      }

      // TEMP DIAGNOSTIC: GC specifically resolved to GCF7 (Jan 2027)
      // instead of GCZ6 (Dec 2026), despite GCZ6 showing genuine active
      // live trading all day - logs every candidate's ticker and
      // last_trade_date so we can see directly whether GCZ6 was missing
      // from these results entirely, present with a missing/null
      // last_trade_date (which the reduce below would silently skip),
      // or present with a later last_trade_date than GCF7 (a Massive-
      // side data quality issue rather than a bug in this logic).
      // Remove once resolved.
      if (root === 'GC') {
        console.log(`[futures] GC candidates: ${JSON.stringify(results.map((c) => ({ ticker: c.ticker, last_trade_date: c.last_trade_date })))}`);
      }

      const nearest = results.reduce((soonest, c) => {
        if (!c.last_trade_date) return soonest;
        if (!soonest || c.last_trade_date < soonest.last_trade_date) return c;
        return soonest;
      }, null);

      if (!nearest?.ticker) {
        console.error(`[futures] Massive front-month lookup - couldn't determine nearest contract for ${root}:`, JSON.stringify(data));
        throw new Error(`No active contract found for ${root}`);
      }
      console.log(`[futures] Resolved ${root} -> ${nearest.ticker} (last_trade_date: ${nearest.last_trade_date})`);
      return nearest.ticker;
    });
  }

  // Maps our frontend-facing interval strings to Massive's resolution
  // format. The 1h case uses "60min" rather than "1h"/"1hour" - Massive's
  // docs describe resolution as a flexible "number + unit" string, but
  // only "1min" was ever directly confirmed working, so staying within
  // the same "min" unit sidesteps guessing at their exact hour spelling.
  const INTERVAL_TO_RESOLUTION = {
    '1min': '1min',
    '5min': '5min',
    '15min': '15min',
    '30min': '30min',
    '1h': '60min',
    // Confirmed against Massive's docs: resolution is a number + unit
    // string (sec/min/hour/session/week/month/quarter/year), and the
    // number is REQUIRED even for session-based units - bare "session"
    // returns "Invalid resolution". "1session" is correct.
    '1day': '1session',
  };

  async function fetchBars(ticker, interval) {
    const resolution = INTERVAL_TO_RESOLUTION[interval] || '1min';
    return getCached(`bars:${ticker}:${resolution}`, 20_000, async () => {
      const url = `https://api.massive.com/futures/v1/aggs/${encodeURIComponent(ticker)}` +
        `?resolution=${resolution}&limit=260&sort=window_start.desc&apiKey=${apiKey}`;

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

  // Previous session's close, for the futures % change display (mirrors
  // what stocks get from Twelve Data's dedicated previousClose field -
  // Massive's aggs endpoint has no such field, so we derive it from daily
  // ("session") bars instead). The most recent bar is normally today's
  // still-forming session, so the one just before it is the last COMPLETE
  // session's close. Cached 60s - this barely changes intraday, same TTL
  // as the stock version. Fails gracefully (returns null) so a hiccup
  // here just means % change doesn't show, same as stocks.
  async function fetchPreviousClose(ticker) {
    try {
      return await getCached(`prevclose:${ticker}`, 60_000, async () => {
        const url = `https://api.massive.com/futures/v1/aggs/${encodeURIComponent(ticker)}` +
          `?resolution=1session&limit=5&sort=window_start.desc&apiKey=${apiKey}`;

        const res = await fetch(url);
        const data = await res.json();
        if (!res.ok || data.status !== 'OK') {
          throw new Error(data.error || data.status || `status ${res.status}`);
        }

        const results = data.results || [];
        if (results.length < 2) throw new Error('not enough session bars to determine previous close');

        // Results are sorted newest-first (sort=window_start.desc), so
        // index 0 is today's (forming) session and index 1 is the last
        // completed one.
        return results[1].close;
      });
    } catch (err) {
      console.error(`[futures] previousClose failed for ${ticker}:`, err.message);
      return null;
    }
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
      loggedAMSample.delete(s);
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

    const price = Number(msg.p);
    const size = Number(msg.s);
    const prevPrice = lastTradePrice.get(tradedSymbol);
    lastTradePrice.set(tradedSymbol, price);

    // Drive live price/candle updates from every trade tick (not just
    // large ones) - the Aggregates channel only delivers a bar once its
    // full minute has completed, which put the chart consistently ~1
    // minute behind real time. Raw trades give genuine tick-by-tick
    // freshness, the same approach stocks already use via Alpaca. Only
    // for symbols DIRECTLY watched - a sibling pulled in just for flow
    // purposes (e.g. MNQ while watching NQ) should never drive NQ's own
    // candle, since the two contracts' last-traded prices aren't identical.
    if (watchers.has(tradedSymbol)) {
      onPrice(tradedSymbol, { price, size, timeMs: msg.t });
    }

    // Always use the ACTUALLY TRADED contract's own multiplier - a micro
    // fill must be converted using the micro multiplier, not the
    // standard's, regardless of which ticker(s) it ends up displayed under.
    const multiplier = getMultiplier(tradedSymbol);
    const dollarValue = price * size * multiplier;

    const quote = lastQuote.get(tradedSymbol);
    let side;
    if (quote?.bid > 0 && quote?.ask > 0) {
      if (price >= quote.ask) side = 'BUY';
      else if (price <= quote.bid) side = 'SELL';
      else side = (prevPrice != null && price < prevPrice) ? 'SELL' : 'BUY';
    } else {
      side = (prevPrice != null && price < prevPrice) ? 'SELL' : 'BUY';
    }

    // TEMP DIAGNOSTIC: logs the RAW feed message for every trade at or
    // above the lower (micro) threshold, so an unusually large print can
    // be checked against exactly what Massive actually sent - now also
    // includes the quote it was classified against and the resulting
    // side, added specifically to verify GC's BUY/SELL classification
    // given a reported pattern of BOUGHT flow correlating with price
    // moving down. Remove once large-print verification is no longer
    // needed.
    if (dollarValue >= MICRO_BLOCK_TRADE_THRESHOLD) {
      console.log(
        `[futures][large-trade] ${tradedSymbol}: raw=${JSON.stringify(msg)} ` +
        `parsedPrice=${price} parsedSize=${size} multiplier=${multiplier} dollarValue=$${Math.round(dollarValue).toLocaleString()} ` +
        `bid=${quote?.bid} ask=${quote?.ask} prevPrice=${prevPrice} side=${side}`
      );
    }

    const threshold = isMicroTicker(tradedSymbol) ? MICRO_BLOCK_TRADE_THRESHOLD : FUTURES_BLOCK_TRADE_THRESHOLD;
    if (dollarValue < threshold) return;

    // Report to whichever member(s) of this family are DIRECTLY watched -
    // could be the traded ticker itself, its sibling, or both at once if
    // different clients are watching each side of the pair.
    const sibling = getSiblingTicker(tradedSymbol);
    const targets = [];
    if (watchers.has(tradedSymbol)) targets.push(tradedSymbol);
    if (sibling && watchers.has(sibling)) targets.push(sibling);
    if (targets.length === 0) targets.push(tradedSymbol); // safety net, shouldn't normally happen

    for (const target of targets) {
      // Standard-equivalent contract count - converts a micro fill onto
      // the same scale as the standard contract (e.g. 11 MNQ contracts
      // -> 1.1 "standard NQ equivalent"), computed from each contract's
      // actual multiplier ratio rather than a hardcoded 1/10, so it stays
      // correct even if a future/mini pair with a different ratio is added.
      const standardEquivalentSize = size * (getMultiplier(tradedSymbol) / getMultiplier(target));

      // sourceSymbol lets the frontend show "via MNQU6" etc. when the
      // actual traded contract differs from the one being watched -
      // without this, a micro-contract print combined into the standard
      // contract's flow looks like inconsistent per-contract dollar math.
      onBigTrade?.(target, { price, size, standardEquivalentSize, dollarValue, side, timeMs: msg.t, sourceSymbol: tradedSymbol !== target ? tradedSymbol : undefined });
    }
  }

  function ensureConnected() {
    if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) return;

    socket = new WebSocket(MASSIVE_WS_URL);
    authed = false;

    socket.on('open', () => {
      openedAt = Date.now();
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
          const watchedList = [...watchers.keys()];
          const flowOnlyList = [...flowOnlyRequestedBy.keys()];
          // TEMP DIAGNOSTIC: checking whether an unusually large
          // subscribe list (possibly accumulated from today's repeated
          // reconnects) is what's triggering the 1008 policy violation
          // that consistently follows ~7-9s after this point. Remove
          // once resolved.
          console.log(`[futures] Subscribing after auth: ${watchedList.length} watched (${watchedList.join(', ')}), ${flowOnlyList.length} flow-only (${flowOnlyList.join(', ')})`);
          sendSubscribe(watchedList, true);
          sendSubscribe(flowOnlyList, false);
        } else if (msg.ev === 'status' && msg.status === 'auth_failed') {
          console.error('[futures] WebSocket auth failed - check MASSIVE_API_KEY');
        } else if (msg.ev === 'AM') {
          // Bucket by the aggregate's START time (msg.s), not its end
          // time (msg.e). If Massive re-sends updates for the SAME
          // still-forming minute with a shifting end timestamp, bucketing
          // by "e" would compute a different bucket each time - creating
          // a new sliver candle on every update instead of updating the
          // one candle for that minute. The start time should stay fixed
          // for a given bar regardless of how many times it's updated.
          if (!loggedAMSample.has(msg.sym)) {
            loggedAMSample.add(msg.sym);
            console.log(`[futures] Sample AM message for ${msg.sym} (checking s/e stability across updates):`, JSON.stringify(msg));
          }
          onPrice(msg.sym, { price: Number(msg.c), open: Number(msg.o), high: Number(msg.h), low: Number(msg.l), close: Number(msg.c), volume: Number(msg.v) || 0, timeMs: msg.s });
        } else if (msg.ev === 'Q') {
          // TEMP DIAGNOSTIC: quotes never got the same "verify field
          // names look right" check trades already have (see
          // loggedTradeSample above) - added specifically to check
          // whether GC's bid/ask are being read correctly, given a
          // reported pattern of GC's BOUGHT flow correlating with price
          // moving DOWN (the opposite of what correct classification
          // should produce). Remove once that's confirmed one way or the
          // other.
          if (!loggedQuoteSample.has(msg.sym)) {
            loggedQuoteSample.add(msg.sym);
            console.log(`[futures] Sample raw quote message for ${msg.sym} (verify bid<ask and field names look right):`, JSON.stringify(msg));
          }
          lastQuote.set(msg.sym, { bid: msg.bp, ask: msg.ap });
        } else if (msg.ev === 'T') {
          handleTrade(msg);
        }
      }
    });

    socket.on('close', (code, reasonBuf) => {
      const reason = reasonBuf ? reasonBuf.toString() : '';
      const uptimeMs = openedAt ? Date.now() - openedAt : null; // null means it closed before ever successfully opening
      console.warn(`[futures] WebSocket CLOSED: code=${code} reason="${reason}" uptimeMs=${uptimeMs} - reconnecting in 2s...`);
      openedAt = null;
      authed = false;
      clearTimeout(reconnectTimer);
      reconnectTimer = setTimeout(ensureConnected, RECONNECT_DELAY_MS);
    });

    socket.on('error', (err) => {
      console.error('[futures] WebSocket error:', err.message);
    });
  }

  // Resolves each FUTURES_BACKGROUND_ROOTS root to its current front-month
  // contract and subscribes under the permanent pseudo-client - reusing
  // hub.subscribe exactly as a real browser client would, so this gets
  // the same AM bars (-> price_bars) and large-trade flow detection
  // (-> flow_events) for free. Safe to call repeatedly: if a root's
  // resolved contract hasn't changed since last time, subscribe() is a
  // no-op for an already-watched ticker.
  async function refreshBackgroundWatch() {
    for (const root of FUTURES_BACKGROUND_ROOTS) {
      try {
        const ticker = await resolveFrontMonth(root);
        const previous = backgroundWatchedTicker.get(root);
        if (previous === ticker) continue; // still the same contract, nothing to do

        if (previous) {
          console.log(`[futures] Background watch: ${root} rolled from ${previous} to ${ticker}`);
          hub.unsubscribe(FUTURES_BACKGROUND_CLIENT_ID, previous);
        } else {
          console.log(`[futures] Background watch: ${root} -> ${ticker}`);
        }
        hub.subscribe(FUTURES_BACKGROUND_CLIENT_ID, ticker);
        backgroundWatchedTicker.set(root, ticker);
      } catch (err) {
        console.error(`[futures] Background watch setup failed for ${root}:`, err.message);
      }
    }
  }

  async function initBackgroundWatch() {
    console.log(`[futures] Setting up permanent background watch for ${FUTURES_BACKGROUND_ROOTS.length} roots (${FUTURES_BACKGROUND_ROOTS.join(', ')})...`);
    await refreshBackgroundWatch();
    setInterval(() => refreshBackgroundWatch(), FUTURES_BACKGROUND_REFRESH_MS);
  }

  const hub = {
    isFuturesTicker,

    async getHistoricalBars(ticker, interval) {
      return fetchBars(ticker, interval);
    },

    async getPreviousClose(ticker) {
      return fetchPreviousClose(ticker);
    },

    resolveFrontMonth,

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

    initBackgroundWatch,
  };

  return hub;
}

module.exports = { createFuturesHub, isFuturesTicker };
