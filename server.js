require('dotenv').config();

const http = require('http');
const crypto = require('crypto');
const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');

const { createAlpacaHub } = require('./lib/alpacaHub');
const { createTradierHub } = require('./lib/tradierHub');
const { createDataProxyRouter } = require('./lib/dataProxy');

const {
  ALPACA_KEY,
  ALPACA_SECRET,
  TWELVE_DATA_KEY,
  TRADIER_TOKEN,
  PORT = 3000,
} = process.env;

for (const [name, val] of Object.entries({ ALPACA_KEY, ALPACA_SECRET, TWELVE_DATA_KEY, TRADIER_TOKEN })) {
  if (!val) console.warn(`[startup] Warning: ${name} is not set - check your .env file`);
}

const app = express();
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

const alpacaHub = createAlpacaHub({
  apiKey: ALPACA_KEY,
  apiSecret: ALPACA_SECRET,
  onTrade: ({ symbol, price, timeMs }) => broadcastPrice(symbol, { price, timeMs }),
  onStatus: (status, detail) => {
    for (const ws of browserClients.values()) {
      if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: 'alpaca_status', status, detail }));
    }
  },
});

const tradierHub = createTradierHub({
  token: TRADIER_TOKEN,
  onFlow: (clientId, flow) => sendToClient(clientId, { type: 'flow', ...flow }),
});

app.use('/api', createDataProxyRouter({
  twelveDataKey: TWELVE_DATA_KEY,
  tradierToken: TRADIER_TOKEN,
  lastPriceOf: (symbol) => alpacaHub.lastPriceOf(symbol),
}));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws) => {
  const clientId = crypto.randomUUID();
  ws.watchedSymbol = null;
  browserClients.set(clientId, ws);

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch (e) { return; }

    // ---- price subscription (Alpaca) ----
    if (msg.type === 'subscribe_price' && msg.symbol) {
      if (ws.watchedSymbol) alpacaHub.unsubscribe(clientId, ws.watchedSymbol);
      ws.watchedSymbol = msg.symbol.toUpperCase();
      alpacaHub.subscribe(clientId, ws.watchedSymbol);
      return;
    }

    if (msg.type === 'unsubscribe_price') {
      if (ws.watchedSymbol) alpacaHub.unsubscribe(clientId, ws.watchedSymbol);
      ws.watchedSymbol = null;
      return;
    }

    // ---- option flow subscription (Tradier) ----
    if (msg.type === 'watch_flow' && msg.symbol && msg.expiration) {
      try {
        const spotPrice = alpacaHub.lastPriceOf(msg.symbol.toUpperCase()) || msg.spotPrice || 0;
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
  });

  ws.on('close', () => {
    if (ws.watchedSymbol) alpacaHub.unsubscribe(clientId, ws.watchedSymbol);
    tradierHub.unwatch(clientId);
    browserClients.delete(clientId);
  });
});

server.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT}`);
  console.log(`Browser WebSocket endpoint: ws://localhost:${PORT}/ws`);
});
