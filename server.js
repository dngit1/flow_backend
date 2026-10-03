require('dotenv').config();

const http = require('http');
const crypto = require('crypto');
const express = require('express');
const cookieParser = require('cookie-parser');
const { WebSocketServer, WebSocket } = require('ws');

const { createFinnhubHub } = require('./lib/finnhubHub');
const { createAlpacaHub } = require('./lib/alpacaHub'); // kept as a fallback - see STOCK_DATA_PROVIDER below
const { createTradierHub } = require('./lib/tradierHub');
const { createFuturesHub, isFuturesTicker } = require('./lib/futuresHub');
const { createDataProxyRouter } = require('./lib/dataProxy');
const { createIndexHub } = require('./lib/indexHub');
const { isIndexSymbol } = require('./lib/indexSymbols');
const { createFlowBuffer } = require('./lib/flowBuffer');
const cache = require('./lib/cache');
const flowHistory = require('./lib/flowHistory');
const priceHistory = require('./lib/priceHistory');
const auth = require('./lib/auth');
const { sendMagicLinkEmail } = require('./lib/mailer');

const {
  FINNHUB_API_KEY,
  ALPACA_KEY,
  ALPACA_SECRET,
  TWELVE_DATA_KEY,
  TRADIER_TOKEN,
  MASSIVE_API_KEY,
  DATABASE_URL,
  GOOGLE_CLIENT_ID,
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

const APP_URL = process.env.APP_URL || `http://localhost:${PORT}`;
const usingAlpaca = STOCK_DATA_PROVIDER === 'alpaca';

const requiredEnvVars = { TWELVE_DATA_KEY, TRADIER_TOKEN, MASSIVE_API_KEY, DATABASE_URL, GOOGLE_CLIENT_ID };
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

app.use(express.json());
app.use(cookieParser());

app.use(express.static('public')); // serve the frontend HTML/JS from here, see README

// ---------------------------------------------------------------
// Auth routes. The frontend itself (static files above) is reachable
// without signing in - that's where the login screen lives. Only the
// /api routes and the WebSocket connection below actually require it.
// ---------------------------------------------------------------

// POST /auth/google - body: { idToken } (from Google Identity Services on
// the frontend). Verifies the token SERVER-SIDE against Google - the
// frontend's claimed email is never trusted directly.
app.post('/auth/google', async (req, res) => {
  try {
    const { idToken } = req.body || {};
    if (!idToken) return res.status(400).json({ error: 'idToken is required' });

    const { email, googleId } = await auth.verifyGoogleIdToken(idToken);
    const user = await auth.findOrCreateUserByEmail(email, googleId);
    const { sessionId, evictedSessionIds } = await auth.createSession(user.id, req.headers['user-agent']);
    evictedSessionIds.forEach(evictSessionConnections);

    auth.setSessionCookie(res, sessionId);
    res.json({ user: { email: user.email, planStatus: user.plan_status } });
  } catch (err) {
    console.error('[auth] Google sign-in failed:', err.message);
    res.status(401).json({ error: 'Google sign-in failed' });
  }
});

// POST /auth/magic-link/request - body: { email }. Always responds ok
// regardless of whether the email has an existing account, so this
// endpoint can't be used to probe which emails are registered.
app.post('/auth/magic-link/request', async (req, res) => {
  try {
    const email = String(req.body?.email || '').trim().toLowerCase();
    if (!email || !email.includes('@')) return res.status(400).json({ error: 'A valid email is required' });

    const token = await auth.createMagicLinkToken(email);
    const link = `${APP_URL}/auth/magic-link/verify?token=${token}`;
    await sendMagicLinkEmail(email, link);

    res.json({ ok: true });
  } catch (err) {
    console.error('[auth] magic link request failed:', err.message);
    res.status(500).json({ error: 'Unable to send sign-in link right now' });
  }
});

// GET /auth/magic-link/verify?token=... - the link the user clicks from
// their email. One-time use (see auth.consumeMagicLinkToken).
app.get('/auth/magic-link/verify', async (req, res) => {
  try {
    const token = String(req.query.token || '');
    const email = await auth.consumeMagicLinkToken(token);
    if (!email) {
      return res.status(400).send('This sign-in link is invalid or has expired. Please request a new one.');
    }

    const user = await auth.findOrCreateUserByEmail(email, null);
    const { sessionId, evictedSessionIds } = await auth.createSession(user.id, req.headers['user-agent']);
    evictedSessionIds.forEach(evictSessionConnections);

    auth.setSessionCookie(res, sessionId);
    res.redirect('/');
  } catch (err) {
    console.error('[auth] magic link verify failed:', err.message);
    res.status(500).send('Something went wrong signing you in. Please try again.');
  }
});

// GET /auth/me - the frontend calls this on load to decide whether to show
// the app or the login screen. Never errors on "not signed in" - that's a
// normal, expected response ({ user: null }), not a failure.
app.get('/auth/me', async (req, res) => {
  try {
    const sessionId = req.cookies?.[auth.SESSION_COOKIE_NAME];
    const session = await auth.getSessionFromCookieValue(sessionId);
    if (!session) return res.json({ user: null });
    res.json({ user: { email: session.user.email, planStatus: session.user.plan_status } });
  } catch (err) {
    console.error('[auth] /auth/me failed:', err.message);
    res.status(500).json({ error: 'Auth check failed' });
  }
});

app.post('/auth/logout', async (req, res) => {
  try {
    const sessionId = req.cookies?.[auth.SESSION_COOKIE_NAME];
    if (sessionId) await auth.deleteSession(sessionId);
    auth.clearSessionCookie(res);
    res.json({ ok: true });
  } catch (err) {
    console.error('[auth] logout failed:', err.message);
    res.status(500).json({ error: 'Logout failed' });
  }
});

// Track connected browser clients so hubs can push data back to them.
// clientId -> WebSocket
const browserClients = new Map();

// Separate from browserClients/clientId above - clientId identifies a
// single WebSocket connection for hub-subscription bookkeeping (unchanged,
// pre-dates auth); sessionId identifies a logged-in DEVICE and can span
// reconnects. One session can briefly have multiple live sockets (e.g. two
// tabs on the same device), so this maps to a Set. Used only to force-close
// a device's connection(s) when its session gets evicted by a new login
// elsewhere (see auth.createSession's device-limit eviction).
const sessionConnections = new Map(); // sessionId -> Set<WebSocket>

function evictSessionConnections(sessionId) {
  const conns = sessionConnections.get(sessionId);
  if (!conns) return;
  for (const ws of conns) {
    try {
      ws.send(JSON.stringify({ type: 'session_evicted', reason: 'Logged in from another device' }));
    } catch (err) { /* socket may already be closing - the terminate() below is what actually matters */ }
    ws.close(4001, 'Session evicted');
  }
  sessionConnections.delete(sessionId);
}

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
  // A SPECIFIC symbol was rejected by the feed (e.g. not a real ticker) -
  // scoped to just the client(s) watching it, unlike onStatus above. This
  // used to not exist: every per-symbol rejection fell through to onStatus
  // instead, flipping EVERY connected browser's badge to "error" even
  // though their own symbol was working fine - all because of one bad
  // symbol from an unrelated client.
  onSymbolRejected: (clientId, symbol) => {
    sendToClient(clientId, { type: 'price_feed_status', status: 'error', detail: `${symbol} is not a supported symbol` });
  },
});

// Live prices for INDEXES (SPX etc.). The stock feed can't supply these -
// Alpaca has no index data and reports an unrelated ~$0.10 security under
// "SPX" - so they come from Tradier's index quote instead (see
// lib/indexSymbols.js). size is 0: an index has no trade size, so this
// never triggers the block-trade logic in the stock hub's onTrade above.
const indexHub = createIndexHub({
  token: TRADIER_TOKEN,
  onTick: ({ symbol, price, timeMs }) => broadcastPrice(symbol, { price, size: 0, timeMs }),
  // Per-client, deliberately NOT the global stock-feed status: that one
  // reflects Alpaca's connection, which an index doesn't use.
  onClientStatus: (clientId, status, detail) => sendToClient(clientId, { type: 'price_feed_status', status, detail }),
});

// Which hub serves live prices for a given symbol.
function priceHubFor(symbol) {
  if (isFuturesTicker(symbol)) return futuresHub;
  if (isIndexSymbol(symbol)) return indexHub;
  return stockHub;
}

// Last known price, from whichever hub owns the symbol.
function lastPriceOf(symbol) {
  return isIndexSymbol(symbol) ? indexHub.lastPriceOf(symbol) : stockHub.lastPriceOf(symbol);
}

const tradierHub = createTradierHub({
  token: TRADIER_TOKEN,
  onFlow: (clientId, flow) => sendToClient(clientId, { type: 'flow', ...flow }),
  flowBuffer,
  onFlowEvent: (event) => flowHistory.recordFlowEvent(event),
  onBackgroundFlow: (flow) => broadcastBackgroundFlow(flow),
});

const futuresHub = createFuturesHub({
  apiKey: MASSIVE_API_KEY,
  onPrice: (ticker, payload) => {
    broadcastPrice(ticker, payload);
    // Only AM aggregate messages (completed 1-min bars) carry open/high/
    // low - raw trade ticks are price-only and drive live price alone,
    // never saved as bar history.
    if (payload.open != null) {
      priceHistory.recordPriceBar({
        symbol: ticker,
        timeframe: '1min',
        barTimeMs: payload.timeMs,
        open: payload.open,
        high: payload.high,
        low: payload.low,
        close: payload.close,
        volume: payload.volume,
      });
    }
  },
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

app.use('/api', auth.requireAuth, createDataProxyRouter({
  twelveDataKey: TWELVE_DATA_KEY,
  tradierToken: TRADIER_TOKEN,
  lastPriceOf,
  futuresHub,
}));

// GET /api/premium-leaderboard - curated-symbol ranking by cumulative
// option premium traded today. See tradierHub.js for the curated list
// and the "no full-market scan" limitation.
app.get('/api/premium-leaderboard', auth.requireAuth, async (req, res) => {
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
app.get('/api/premium', auth.requireAuth, async (req, res) => {
  const symbol = String(req.query.symbol || '').toUpperCase();
  if (!symbol) return res.status(400).json({ error: 'symbol is required' });

  try {
    const spotPrice = lastPriceOf(symbol) || null;
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
app.get('/api/flow-history', auth.requireAuth, async (req, res) => {
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

// GET /api/price-history?symbol=XYZ&timeframe=1min&sinceMs=... - saved
// completed bars for this symbol (futures only for now - see
// migrations/003_price_history.sql), oldest first, within the 30-day
// retention window. sinceMs is optional.
app.get('/api/price-history', auth.requireAuth, async (req, res) => {
  const symbol = String(req.query.symbol || '').toUpperCase();
  const timeframe = String(req.query.timeframe || '1min');
  const sinceMs = req.query.sinceMs ? Number(req.query.sinceMs) : undefined;
  if (!symbol) return res.status(400).json({ error: 'symbol is required' });

  try {
    const bars = await priceHistory.getPriceHistory(symbol, timeframe, sinceMs);
    res.json({ bars });
  } catch (err) {
    console.error(`[price-history] failed for ${symbol}:`, err.message);
    res.status(502).json({ error: 'Unable to load price history right now' });
  }
});

// Fire-and-forget at startup - fetches near-the-money contracts for every
// curated leaderboard symbol once. Doesn't block the server from starting;
// the leaderboard just reports ready:false until this finishes.
tradierHub.initLeaderboard().catch((err) => {
  console.error('[startup] Leaderboard init failed:', err.message);
});

// Permanent background watch for ES/NQ/GC - keeps price_bars and
// flow_events accumulating for these even with nobody's browser open.
// Much lighter than the (currently disabled) stock version: 3 roots, one
// contract each, reusing the same subscribe() path a real browser client
// uses rather than a separate mechanism - see futuresHub.js.
futuresHub.initBackgroundWatch().catch((err) => {
  console.error('[startup] Futures background watch init failed:', err.message);
});

// TEMPORARILY DISABLED - suspected contributor to overall server load
// (an additional ~1080 watched contracts, plus ~90 chain fetches at
// startup) around the time stock chart updates and normal flow started
// acting up. Uncomment to re-enable once that's confirmed resolved and
// stable; nothing else needs to change; tradierHub.js's implementation
// is untouched.
// tradierHub.initBackgroundFlow().catch((err) => {
//   console.error('[startup] Background flow init failed:', err.message);
// });

// 1-day retention for saved flow history - nothing here ever deletes
// itself automatically, so this has to run on a schedule. Once at
// startup (catches anything that piled up while the server was down),
// then hourly - frequent enough that the table never grows far past a
// day's worth of data, without running a DELETE on every single event.
flowHistory.purgeOldFlowEvents();
setInterval(() => flowHistory.purgeOldFlowEvents(), 60 * 60_000);

// Same pattern, 30-day retention instead of 1-day - see priceHistory.js.
priceHistory.purgeOldPriceBars();
setInterval(() => priceHistory.purgeOldPriceBars(), 60 * 60_000);

const server = http.createServer(app);
const wss = new WebSocketServer({
  server,
  path: '/ws',
  // Rejects the upgrade BEFORE the WebSocket handshake completes, rather
  // than accepting the connection and closing it immediately after - same
  // cookie-based session check as the REST routes' requireAuth.
  verifyClient: (info, callback) => {
    const sessionId = auth.getSessionIdFromCookieHeader(info.req.headers.cookie);
    auth.getSessionFromCookieValue(sessionId)
      .then((session) => {
        if (!session) return callback(false, 401, 'Not signed in');
        info.req.sessionInfo = session; // read back in the 'connection' handler below - same req object throughout the upgrade
        callback(true);
      })
      .catch((err) => {
        console.error('[auth] WS verifyClient failed:', err.message);
        callback(false, 500, 'Auth check failed');
      });
  },
});

wss.on('connection', (ws, req) => {
  const clientId = crypto.randomUUID();
  ws.watchedSymbol = null;
  ws.isAlive = true;
  browserClients.set(clientId, ws);

  // Track this connection under its session, so a device-limit eviction
  // (see evictSessionConnections) can find and force-close it.
  const { sessionId } = req.sessionInfo;
  ws.sessionId = sessionId;
  let sessionConns = sessionConnections.get(sessionId);
  if (!sessionConns) {
    sessionConns = new Set();
    sessionConnections.set(sessionId, sessionConns);
  }
  sessionConns.add(ws);

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
      if (ws.watchedSymbol) priceHubFor(ws.watchedSymbol).unsubscribe(clientId, ws.watchedSymbol);
      ws.watchedSymbol = symbol;
      if (isFuturesTicker(symbol)) {
        futuresHub.subscribe(clientId, symbol);
      } else if (isIndexSymbol(symbol)) {
        // The index hub reports its own status to this client as soon as
        // its first quote arrives (or fails) - no stock-feed status here.
        indexHub.subscribe(clientId, symbol);
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
      if (ws.watchedSymbol) priceHubFor(ws.watchedSymbol).unsubscribe(clientId, ws.watchedSymbol);
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
        const spotPrice = msg.spotPrice || lastPriceOf(msg.symbol.toUpperCase()) || 0;
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
        const spotPrice = msg.spotPrice || lastPriceOf(msg.symbol.toUpperCase()) || 0;
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
    if (ws.watchedSymbol) priceHubFor(ws.watchedSymbol).unsubscribe(clientId, ws.watchedSymbol);
    tradierHub.unwatch(clientId);
    tradierHub.unwatchBigFlow(clientId);
    browserClients.delete(clientId);

    const conns = sessionConnections.get(ws.sessionId);
    if (conns) {
      conns.delete(ws);
      if (conns.size === 0) sessionConnections.delete(ws.sessionId);
    }
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
