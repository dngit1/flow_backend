require('dotenv').config();

const http = require('http');
const crypto = require('crypto');
const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');

const { createAlpacaHub } = require('./lib/alpacaHub');
const { createTradierHub } = require('./lib/tradierHub');
const { createFuturesHub, isFuturesTicker } = require('./lib/futuresHub');
const { createDataProxyRouter } = require('./lib/dataProxy');

const {
  ALPACA_KEY,
  ALPACA_SECRET,
  TWELVE_DATA_KEY,
  TRADIER_TOKEN,
  MASSIVE_API_KEY,
  PORT = 3000,
} = process.env;

for (const [name, val] of Object.entries({ ALPACA_KEY, ALPACA_SECRET, TWELVE_DATA_KEY, TRADIER_TOKEN, MASSIVE_API_KEY })) {
  if (!val) console.warn(`[startup] Warning: ${name} is not set - check your .env file`);
}

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
  // (Reconciliation of "who wants what" happens in alpacaHub; here we just
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

// Filtered server-side (not client-adjustable like the option flow
// thresholds) - without this, every single trade on a liquid stock would
// get broadcast to every watching client, which is far too much traffic.
const STOCK_BLOCK_TRADE_THRESHOLD = 500_000; // dollar value

let currentAlpacaStatus = { status: 'disconnected', detail: null };

const alpacaHub = createAlpacaHub({
  apiKey: ALPACA_KEY,
  apiSecret: ALPACA_SECRET,
  onTrade: ({ symbol, price, size, prevPrice, bid, ask, timeMs }) => {
    broadcastPrice(symbol, { price, timeMs });

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
      broadcastStockFlow(symbol, { price, size, dollarValue, side, timeMs });
    }
  },
  onStatus: (status, detail) => {
    currentAlpacaStatus = { status, detail };
    for (const ws of browserClients.values()) {
      if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: 'alpaca_status', status, detail }));
    }
  },
});

const tradierHub = createTradierHub({
  token: TRADIER_TOKEN,
  onFlow: (clientId, flow) => sendToClient(clientId, { type: 'flow', ...flow }),
});

const futuresHub = createFuturesHub({
  apiKey: MASSIVE_API_KEY,
  onPrice: (ticker, payload) => broadcastPrice(ticker, payload),
  onBigTrade: (ticker, payload) => broadcastStockFlow(ticker, payload), // same "large print on the underlying" concept as stocks, same broadcast function
});

app.use('/api', createDataProxyRouter({
  twelveDataKey: TWELVE_DATA_KEY,
  tradierToken: TRADIER_TOKEN,
  lastPriceOf: (symbol) => alpacaHub.lastPriceOf(symbol),
  futuresHub,
}));

// GET /api/premium-leaderboard - curated-symbol ranking by cumulative
// option premium traded today. See tradierHub.js for the curated list
// and the "no full-market scan" limitation.
app.get('/api/premium-leaderboard', (req, res) => {
  res.json(tradierHub.getLeaderboard());
});

// GET /api/premium?symbol=XYZ - on-demand call/put premium tracking for
// ANY ticker, not just the curated leaderboard list. First check on a new
// symbol sets up tracking (starts at $0) and returns immediately; premium
// accumulates from that point forward on future checks.
app.get('/api/premium', async (req, res) => {
  const symbol = String(req.query.symbol || '').toUpperCase();
  if (!symbol) return res.status(400).json({ error: 'symbol is required' });

  try {
    const spotPrice = alpacaHub.lastPriceOf(symbol) || null;
    const result = await tradierHub.trackSymbol(symbol, spotPrice);
    res.json(result);
  } catch (err) {
    console.error(`[premium] failed for ${symbol}:`, err.message);
    res.status(502).json({ error: 'Unable to check premium for this ticker right now' });
  }
});

// Fire-and-forget at startup - fetches near-the-money contracts for every
// curated leaderboard symbol once. Doesn't block the server from starting;
// the leaderboard just reports ready:false until this finishes.
tradierHub.initLeaderboard().catch((err) => {
  console.error('[startup] Leaderboard init failed:', err.message);
});

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
  // after the upstream Alpaca connection already succeeded would never
  // hear about it and stay stuck showing "Connecting..." forever.
  ws.send(JSON.stringify({ type: 'alpaca_status', status: currentAlpacaStatus.status, detail: currentAlpacaStatus.detail }));

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch (e) { return; }

    // ---- price subscription (Alpaca for stocks, Massive for futures) ----
    if (msg.type === 'subscribe_price' && msg.symbol) {
      const symbol = msg.symbol.toUpperCase();
      if (ws.watchedSymbol) {
        if (isFuturesTicker(ws.watchedSymbol)) futuresHub.unsubscribe(clientId, ws.watchedSymbol);
        else alpacaHub.unsubscribe(clientId, ws.watchedSymbol);
      }
      ws.watchedSymbol = symbol;
      if (isFuturesTicker(symbol)) {
        futuresHub.subscribe(clientId, symbol);
      } else {
        alpacaHub.subscribe(clientId, symbol);
        // Re-confirm the CURRENT status directly to this client - not just
        // a broadcast on future changes. Without this, a client whose badge
        // got stuck on an unrelated error (e.g. a failed load for a
        // different symbol) never gets corrected back to the true state,
        // since a successful load intentionally doesn't touch the badge.
        ws.send(JSON.stringify({ type: 'alpaca_status', status: currentAlpacaStatus.status, detail: currentAlpacaStatus.detail }));
      }
      return;
    }

    if (msg.type === 'unsubscribe_price') {
      if (ws.watchedSymbol) {
        if (isFuturesTicker(ws.watchedSymbol)) futuresHub.unsubscribe(clientId, ws.watchedSymbol);
        else alpacaHub.unsubscribe(clientId, ws.watchedSymbol);
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
        const spotPrice = msg.spotPrice || alpacaHub.lastPriceOf(msg.symbol.toUpperCase()) || 0;
        const count = await tradierHub.watch(clientId, msg.symbol.toUpperCase(), msg.expiration, spotPrice);
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
        const spotPrice = msg.spotPrice || alpacaHub.lastPriceOf(msg.symbol.toUpperCase()) || 0;
        const count = await tradierHub.watchBigFlow(clientId, msg.symbol.toUpperCase(), spotPrice);
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
      else alpacaHub.unsubscribe(clientId, ws.watchedSymbol);
    }
    tradierHub.unwatch(clientId);
    tradierHub.unwatchBigFlow(clientId);
    browserClients.delete(clientId);
  });
});

// Some network drops (proxy timeouts, laptop sleep, etc.) never send a
// proper close frame, which would otherwise leave a "zombie" subscription
// behind - still registered with alpacaHub/tradierHub and still receiving
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
