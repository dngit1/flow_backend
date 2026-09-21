require('dotenv').config();

const http = require('http');
const crypto = require('crypto');
const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');

const { createFinnhubHub } = require('./lib/finnhubHub');
const { createAlpacaHub } = require('./lib/alpacaHub'); // kept as a fallback - see STOCK_DATA_PROVIDER below
const { createTradierHub } = require('./lib/tradierHub');
const { createFuturesHub, isFuturesTicker } = require('./lib/futuresHub');
const { createDataProxyRouter } = require('./lib/dataProxy');
const { createFlowBuffer } = require('./lib/flowBuffer');
const cache = require('./lib/cache');
const flowHistory = require('./lib/flowHistory');
// NOTE: auth (lib/auth.js, lib/mailer.js) is intentionally NOT wired in
// here - this is the deploy-now version, auth requires a working
// Postgres + Google OAuth setup that hasn't been done yet. The
// auth-complete version of this file is preserved separately; see
// server-with-auth.js for reintegration once that setup is ready.

const {
  FINNHUB_API_KEY,
  ALPACA_KEY,
  ALPACA_SECRET,
  TWELVE_DATA_KEY,
  TRADIER_TOKEN,
  MASSIVE_API_KEY,
  // Which live stock data provider to use - 'finnhub' (default, free,
  // no brokerage account) or 'alpaca' (kept as a fallback; requires
  // ALPACA_KEY + ALPACA_SECRET and a working brokerage login - see the
  // git history around the Alpaca MFA lockout for why this defaulted
  // away from Alpaca). Flip back by setting this env var - no code
  // change or redeploy-of-different-code needed, both hubs ship in
  // every deploy either way.
  STOCK_DATA_PROVIDER = 'finnhub',
  PORT = 3000,
} = process.env;

const usingAlpaca = STOCK_DATA_PROVIDER === 'alpaca';

const requiredEnvVars = { TWELVE_DATA_KEY, TRADIER_TOKEN, MASSIVE_API_KEY };
if (usingAlpaca) {
  requiredEnvVars.ALPACA_KEY = ALPACA_KEY;
  requiredEnvVars.ALPACA_SECRET = ALPACA_SECRET;
} else {
  requiredEnvVars.FINNHUB_API_KEY = FINNHUB_API_KEY;
}
for (const [name, val] of Object.entries(requiredEnvVars)) {
  if (!val) console.warn(`[startup] Warning: ${name} is not set - check your .env file`);
}
console.log(`[startup] Stock data provider: ${STOCK_DATA_PROVIDER}`);

const app = express();
// Allow the frontend to call this backend from any origin (localhost file,
// Live Server, or wherever it's hosted) - without this, browsers block the
// fetch() calls before they even reach the server, and it looks
// indistinguishable from "server isn't running."
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

app.use(express.static('public')); // serve the frontend HTML/JS from here, see README

// Track connected browser clients so hubs can push data back to them.
// clientId -> WebSocket
const browserClients = new Map();

function sendToClient(clientId, message) {
  const ws = browserClients.get(clientId);
  // TEMP DIAGNOSTIC: shows exactly what happens at the final delivery
  // point for flow messages - whether the client is even found in
  // browserClients, and what readyState its socket is in. Remove once the
  // cross-client flow-delivery investigation is resolved.
  if (message.type === 'flow') {
    console.log(`[server] DELIVER flow to ${clientId.slice(0, 8)}: found=${!!ws} readyState=${ws ? ws.readyState : 'N/A'}`);
  }
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(message));
  }
}

function broadcastPrice(symbol, payload) {
  // Every client currently subscribed to this symbol gets the tick.
  // (Reconciliation of "who wants what" happens in stockHub; here we just
  // fan out to any client whose active symbol matches.)
  for (const [clientId, ws] of browserClients) {
    if (ws.readyState === WebSocket.OPEN && ws.watchedSymbol === symbol) {
      ws.send(JSON.stringify({ type: 'price', symbol, ...payload }));
    }
  }
}

function broadcastStockFlow(symbol, payload) {
  for (const [clientId, ws] of browserClients) {
    if (ws.readyState === WebSocket.OPEN && ws.watchedSymbol === symbol) {
      ws.send(JSON.stringify({ type: 'stock_flow', symbol, ...payload }));
    }
  }
}

// Unlike broadcastPrice/broadcastStockFlow above, this goes to EVERY
// connected client regardless of which ticker they're currently
// watching - background flow is a fixed 15-symbol watchlist independent
// of whatever's loaded in either chart panel, so every browser should
// see the same feed.
function broadcastBackgroundFlow(payload) {
  for (const [clientId, ws] of browserClients) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'background_flow', ...payload }));
    }
  }
}

// Filtered server-side (not client-adjustable like the option flow
// thresholds) - without this, every single trade on a liquid stock would
// get broadcast to every watching client, which is far too much traffic.
const STOCK_BLOCK_TRADE_THRESHOLD = 1_000_000; // dollar value

let currentStockFeedStatus = { status: 'disconnected', detail: null };

// Shared rolling buffer of recent flow events (option flow, big flow, and
// stock/futures share-flow all record into this) so a refreshed page or a
// brand-new connection immediately sees recent activity instead of
// starting blank. Purely in-memory, no extra API calls to any provider -
// see lib/flowBuffer.js for the tradeoffs of this approach.
const flowBuffer = createFlowBuffer();

const createStockHub = usingAlpaca ? createAlpacaHub : createFinnhubHub;
const stockHub = createStockHub({
  apiKey: usingAlpaca ? ALPACA_KEY : FINNHUB_API_KEY,
  apiSecret: ALPACA_SECRET, // only read by createAlpacaHub - createFinnhubHub ignores extra fields it doesn't destructure
  onTrade: ({ symbol, price, size, prevPrice, bid, ask, timeMs }) => {
    broadcastPrice(symbol, { price, size, timeMs });

    const dollarValue = price * (size || 0);
    if (dollarValue >= STOCK_BLOCK_TRADE_THRESHOLD) {
      let side;
      if (bid > 0 && ask > 0) {
        // Real bid/ask comparison - same approach option flow already
        // uses. A trade between the bid and ask (inside the spread) is
        // genuinely ambiguous, so fall back to uptick/downtick just for
        // that case rather than a third "NEUTRAL" side the frontend
        // would need to handle separately.
        if (price >= ask) side = 'BUY';
        else if (price <= bid) side = 'SELL';
        else side = (prevPrice != null && price < prevPrice) ? 'SELL' : 'BUY';
      } else {
        // No quote seen yet for this symbol (e.g. right after subscribing) -
        // fall back to simple uptick/downtick.
        side = (prevPrice != null && price < prevPrice) ? 'SELL' : 'BUY';
      }
      const flowPayload = { price, size, dollarValue, side, timeMs };
      flowBuffer.record(`stockflow:${symbol}`, flowPayload);
      broadcastStockFlow(symbol, flowPayload);
      flowHistory.recordFlowEvent({ symbol, assetType: 'stock', side, size, value: dollarValue, timeMs });
    }
  },
  onStatus: (status, detail) => {
    currentStockFeedStatus = { status, detail };
    for (const ws of browserClients.values()) {
      if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: 'price_feed_status', status, detail }));
    }
  },
});

const tradierHub = createTradierHub({
  token: TRADIER_TOKEN,
  onFlow: (clientId, flow) => sendToClient(clientId, { type: 'flow', ...flow }),
  flowBuffer,
  onFlowEvent: (event) => flowHistory.recordFlowEvent(event),
  onBackgroundFlow: (flow) => broadcastBackgroundFlow(flow),
});

const futuresHub = createFuturesHub({
  apiKey: MASSIVE_API_KEY,
  onPrice: (ticker, payload) => broadcastPrice(ticker, payload),
  onBigTrade: (ticker, payload) => {
    flowBuffer.record(`stockflow:${ticker}`, payload);
    broadcastStockFlow(ticker, payload);
    flowHistory.recordFlowEvent({
      symbol: ticker,
      assetType: 'futures',
      side: payload.side,
      size: payload.standardEquivalentSize,
      value: payload.dollarValue,
      sourceSymbol: payload.sourceSymbol,
      timeMs: payload.timeMs,
    });
  }, // same "large print on the underlying" concept as stocks, same broadcast function
});

app.use('/api', createDataProxyRouter({
  twelveDataKey: TWELVE_DATA_KEY,
  tradierToken: TRADIER_TOKEN,
  lastPriceOf: (symbol) => stockHub.lastPriceOf(symbol),
  futuresHub,
}));

// GET /api/premium-leaderboard - curated-symbol ranking by cumulative
// option premium traded today. See tradierHub.js for the curated list
// and the "no full-market scan" limitation.
app.get('/api/premium-leaderboard', async (req, res) => {
  const leaderboard = tradierHub.getLeaderboard();
  if (!leaderboard.ready || !leaderboard.rankings.length) return res.json(leaderboard);

  // Attach each ranked symbol's daily % change - only fetched for the
  // (already top-10-limited) ranked symbols, not the full curated list,
  // and cached briefly so refreshing the panel every 30s (see the
  // frontend's leaderboard poll) doesn't multiply Twelve Data usage.
  try {
    const symbols = leaderboard.rankings.map((r) => r.symbol);
    const changes = await cache.cached(`leaderboard-changes:${symbols.join(',')}`, 30_000, async () => {
      const url = `https://api.twelvedata.com/quote?symbol=${symbols.join(',')}&apikey=${TWELVE_DATA_KEY}`;
      const response = await fetch(url);
      const data = await response.json();
      // Twelve Data's batch response is keyed by symbol when MULTIPLE
      // symbols are requested; a single-symbol request instead returns
      // one flat object - normalize both shapes to the same lookup.
      const quotes = symbols.length > 1 ? data : { [symbols[0]]: data };
      const result = {};
      for (const sym of symbols) {
        const pct = quotes[sym]?.percent_change;
        result[sym] = pct != null ? parseFloat(pct) : null;
      }
      return result;
    });

    leaderboard.rankings = leaderboard.rankings.map((r) => ({ ...r, percentChange: changes[r.symbol] ?? null }));
  } catch (err) {
    console.error('[dataProxy] leaderboard percent-change fetch failed:', err.message);
    // Fall back to the leaderboard without percentChange rather than
    // failing the whole panel over this one extra field.
  }

  res.json(leaderboard);
});

// GET /api/premium?symbol=XYZ - on-demand call/put premium tracking for
// ANY ticker, not just the curated leaderboard list. First check on a new
// symbol sets up tracking (starts at $0) and returns immediately; premium
// accumulates from that point forward on future checks.
app.get('/api/premium', async (req, res) => {
  const symbol = String(req.query.symbol || '').toUpperCase();
  if (!symbol) return res.status(400).json({ error: 'symbol is required' });

  try {
    const spotPrice = stockHub.lastPriceOf(symbol) || null;
    const result = await tradierHub.trackSymbol(symbol, spotPrice);
    res.json(result);
  } catch (err) {
    console.error(`[premium] failed for ${symbol}:`, err.message);
    res.status(502).json({ error: 'Unable to check premium for this ticker right now' });
  }
});

// GET /api/flow-history?symbol=XYZ - every saved qualifying flow event
// (option, stock, or futures) for this symbol within the retention
// window (1 day - see lib/flowHistory.js), oldest first. Powers the
// "Replay" feature: backfills a symbol's full day of flow onto its
// chart, not just whatever streamed in live while the tab was open.
app.get('/api/flow-history', async (req, res) => {
  const symbol = String(req.query.symbol || '').toUpperCase();
  if (!symbol) return res.status(400).json({ error: 'symbol is required' });

  try {
    const events = await flowHistory.getFlowHistory(symbol);
    res.json({ events });
  } catch (err) {
    console.error(`[flow-history] failed for ${symbol}:`, err.message);
    res.status(502).json({ error: 'Unable to load flow history right now' });
  }
});

// Fire-and-forget at startup - fetches near-the-money contracts for every
// curated leaderboard symbol once. Doesn't block the server from starting;
// the leaderboard just reports ready:false until this finishes.
tradierHub.initLeaderboard().catch((err) => {
  console.error('[startup] Leaderboard init failed:', err.message);
});

// Same fire-and-forget pattern - sets up the permanent 15-symbol
// background flow watch. Independent of the leaderboard (different
// symbol list, different purpose), so it's fine if one fails without
// affecting the other.
tradierHub.initBackgroundFlow().catch((err) => {
  console.error('[startup] Background flow init failed:', err.message);
});

// 1-day retention for saved flow history - nothing here ever deletes
// itself automatically, so this has to run on a schedule. Once at
// startup (catches anything that piled up while the server was down),
// then hourly - frequent enough that the table never grows far past a
// day's worth of data, without running a DELETE on every single event.
flowHistory.purgeOldFlowEvents();
setInterval(() => flowHistory.purgeOldFlowEvents(), 60 * 60_000);

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws) => {
  const clientId = crypto.randomUUID();
  ws.watchedSymbol = null;
  ws.isAlive = true;
  browserClients.set(clientId, ws);

  ws.on('pong', () => { ws.isAlive = true; });

  // Tell this new client the CURRENT status right away - onStatus above
  // only fires on future changes, so without this, anyone who connects
  // after the upstream connection already succeeded would never
  // hear about it and stay stuck showing "Connecting..." forever.
  ws.send(JSON.stringify({ type: 'price_feed_status', status: currentStockFeedStatus.status, detail: currentStockFeedStatus.detail }));

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch (e) { return; }

    // ---- price subscription (Finnhub for stocks, Massive for futures) ----
    if (msg.type === 'subscribe_price' && msg.symbol) {
      const symbol = msg.symbol.toUpperCase();
      if (ws.watchedSymbol) {
        if (isFuturesTicker(ws.watchedSymbol)) futuresHub.unsubscribe(clientId, ws.watchedSymbol);
        else stockHub.unsubscribe(clientId, ws.watchedSymbol);
      }
      ws.watchedSymbol = symbol;
      if (isFuturesTicker(symbol)) {
        futuresHub.subscribe(clientId, symbol);
      } else {
        stockHub.subscribe(clientId, symbol);
        // Re-confirm the CURRENT status directly to this client - not just
        // a broadcast on future changes. Without this, a client whose badge
        // got stuck on an unrelated error (e.g. a failed load for a
        // different symbol) never gets corrected back to the true state,
        // since a successful load intentionally doesn't touch the badge.
        ws.send(JSON.stringify({ type: 'price_feed_status', status: currentStockFeedStatus.status, detail: currentStockFeedStatus.detail }));
      }
      // Replay recent stock/futures share-flow history for this symbol,
      // same "don't start blank" treatment as option flow gets.
      flowBuffer.getRecent(`stockflow:${symbol}`).forEach((event) => {
        sendToClient(clientId, { type: 'stock_flow', symbol, ...event });
      });
      return;
    }

    if (msg.type === 'unsubscribe_price') {
      if (ws.watchedSymbol) {
        if (isFuturesTicker(ws.watchedSymbol)) futuresHub.unsubscribe(clientId, ws.watchedSymbol);
        else stockHub.unsubscribe(clientId, ws.watchedSymbol);
      }
      ws.watchedSymbol = null;
      return;
    }

    // ---- option flow subscription (Tradier) ----
    if (msg.type === 'watch_flow' && msg.symbol && msg.expiration) {
      try {
        // Prefer the price the client just fetched (guaranteed fresh, sent
        // right after a successful historical-bars load) over our own
        // cached lastPriceOf() - that cache can be stale or empty at this
        // exact moment (a timing race after reconnects/resubscribes),
        // which previously caused near-the-money strikes to silently fall
        // back to an arbitrary, unrelated strike.
        const spotPrice = msg.spotPrice || stockHub.lastPriceOf(msg.symbol.toUpperCase()) || 0;
        const { count, history } = await tradierHub.watch(clientId, msg.symbol.toUpperCase(), msg.expiration, spotPrice);
        // Replay recent history oldest-first, so the frontend's prepend
        // logic ends up with newest-on-top, same as if these had arrived
        // live one at a time.
        history.forEach((event) => sendToClient(clientId, { type: 'flow', ...event }));
        sendToClient(clientId, { type: 'flow_watching', symbol: msg.symbol, expiration: msg.expiration, contractCount: count });
      } catch (err) {
        sendToClient(clientId, { type: 'flow_error', error: err.message });
      }
      return;
    }

    if (msg.type === 'unwatch_flow') {
      tradierHub.unwatch(clientId);
      return;
    }

    // "Big flow, any expiration" - watches several near-term expirations
    // at once instead of just the one selected in the normal dropdown.
    if (msg.type === 'watch_big_flow' && msg.symbol) {
      try {
        const spotPrice = msg.spotPrice || stockHub.lastPriceOf(msg.symbol.toUpperCase()) || 0;
        const { count, history } = await tradierHub.watchBigFlow(clientId, msg.symbol.toUpperCase(), spotPrice);
        history.forEach((event) => sendToClient(clientId, { type: 'flow', ...event }));
        sendToClient(clientId, { type: 'big_flow_watching', symbol: msg.symbol, contractCount: count });
      } catch (err) {
        sendToClient(clientId, { type: 'flow_error', error: err.message });
      }
      return;
    }

    if (msg.type === 'unwatch_big_flow') {
      tradierHub.unwatchBigFlow(clientId);
      return;
    }
  });

  ws.on('close', () => {
    if (ws.watchedSymbol) {
      if (isFuturesTicker(ws.watchedSymbol)) futuresHub.unsubscribe(clientId, ws.watchedSymbol);
      else stockHub.unsubscribe(clientId, ws.watchedSymbol);
    }
    tradierHub.unwatch(clientId);
    tradierHub.unwatchBigFlow(clientId);
    browserClients.delete(clientId);
  });
});

// Some network drops (proxy timeouts, laptop sleep, etc.) never send a
// proper close frame, which would otherwise leave a "zombie" subscription
// behind - still registered with stockHub/tradierHub and still receiving
// (and duplicating) broadcasts, even though nothing is really listening.
// This sweep pings every client every 30s; anyone that didn't respond to
// the PREVIOUS ping gets forcibly terminated, which fires the 'close'
// handler above and cleans up their subscriptions properly.
const heartbeatInterval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30_000);

wss.on('close', () => clearInterval(heartbeatInterval));

server.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT}`);
  console.log(`Browser WebSocket endpoint: ws://localhost:${PORT}/ws`);
});
