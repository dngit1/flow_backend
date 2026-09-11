const WebSocket = require('ws');

// Manages exactly ONE upstream connection to Alpaca, no matter how many
// browser clients are connected to this server. Each browser client
// subscribes/unsubscribes to symbols by clientId; this hub reconciles the
// union of everyone's symbols against the single upstream connection.
function createAlpacaHub({ apiKey, apiSecret, onTrade, onStatus }) {
  let socket = null;
  let authed = false;

  // clientId -> Set<symbol>
  const clientSymbols = new Map();
  // last known trade price per symbol, so the data proxy can answer
  // "what's the current price" without a second data source
  const lastPrice = new Map();
  // last known bid/ask per symbol, for real buy-vs-sell classification
  // (trade at/above ask = BUY, at/below bid = SELL) instead of just
  // guessing from the previous trade's price direction.
  const lastQuote = new Map();

  function desiredSymbols() {
    const set = new Set();
    for (const symbols of clientSymbols.values()) {
      for (const s of symbols) set.add(s);
    }
    return set;
  }

  function ensureConnected() {
    if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) return;

    socket = new WebSocket('wss://stream.data.alpaca.markets/v2/iex');

    socket.on('open', () => {
      socket.send(JSON.stringify({ action: 'auth', key: apiKey, secret: apiSecret }));
    });

    socket.on('message', (raw) => {
      let messages;
      try { messages = JSON.parse(raw.toString()); } catch (e) { return; }

      for (const msg of messages) {
        if (msg.T === 'error') {
          console.error('[alpaca] error:', msg.msg || msg.code);
          onStatus?.('error', msg.msg || msg.code);
        }

        if (msg.T === 'success' && msg.msg === 'authenticated') {
          authed = true;
          const symbols = [...desiredSymbols()];
          if (symbols.length) socket.send(JSON.stringify({ action: 'subscribe', trades: symbols, quotes: symbols }));
          onStatus?.('connected');
        }

        if (msg.T === 'q') {
          lastQuote.set(msg.S, { bid: msg.bp, ask: msg.ap });
        }

        if (msg.T === 't') {
          const prevPrice = lastPrice.get(msg.S);
          lastPrice.set(msg.S, msg.p);
          const quote = lastQuote.get(msg.S);
          onTrade({
            symbol: msg.S,
            price: msg.p,
            size: msg.s,
            prevPrice, // fallback for buy-vs-sell classification if no quote is available yet
            bid: quote?.bid,
            ask: quote?.ask,
            timeMs: new Date(msg.t).getTime(),
          });
        }
      }
    });

    socket.on('error', (err) => console.error('[alpaca] ws error:', err.message));
    socket.on('close', () => {
      authed = false;
      socket = null;
      onStatus?.('disconnected');
    });
  }

  function reconcile(previousSymbols, newSymbols) {
    ensureConnected();
    if (!(socket && socket.readyState === WebSocket.OPEN && authed)) return; // onauth will send the full set

    const desired = desiredSymbols();
    const toUnsub = [...(previousSymbols || [])].filter((s) => !desired.has(s));
    const toSub = [...(newSymbols || [])].filter((s) => desired.has(s)); // only actually-still-desired

    if (toUnsub.length) socket.send(JSON.stringify({ action: 'unsubscribe', trades: toUnsub, quotes: toUnsub }));
    if (toSub.length) socket.send(JSON.stringify({ action: 'subscribe', trades: toSub, quotes: toSub }));
  }

  return {
    subscribe(clientId, symbol) {
      const before = new Set(clientSymbols.get(clientId) || []);
      const set = clientSymbols.get(clientId) || new Set();
      set.add(symbol);
      clientSymbols.set(clientId, set);
      reconcile(before, new Set([symbol]));
    },

    unsubscribe(clientId, symbol) {
      const set = clientSymbols.get(clientId);
      if (!set) return;
      const before = new Set(set);
      set.delete(symbol);
      reconcile(before, new Set());
    },

    unsubscribeAll(clientId) {
      const set = clientSymbols.get(clientId);
      if (!set) return;
      const before = new Set(set);
      clientSymbols.delete(clientId);
      reconcile(before, new Set());
    },

    lastPriceOf(symbol) {
      return lastPrice.get(symbol);
    },
  };
}

module.exports = { createAlpacaHub };
