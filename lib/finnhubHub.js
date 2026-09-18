const WebSocket = require('ws');

// Manages exactly ONE upstream connection to Finnhub, no matter how many
// browser clients are connected to this server - same shared-connection
// design as alpacaHub.js, and the same public interface (subscribe /
// unsubscribe / unsubscribeAll / lastPriceOf), so this is a drop-in swap.
//
// Key differences from Alpaca, worth knowing if debugging:
// - Auth is just a `token` query param on the connection URL - no
//   separate auth handshake/success message to wait for like Alpaca's.
//   Subscriptions can be sent as soon as the socket opens.
// - Finnhub's free tier WebSocket only streams TRADES, not quotes/NBBO -
//   there is no bid/ask stream to classify buy-vs-sell against, so every
//   trade always falls back to the uptick/downtick heuristic (comparing
//   against the previous trade's price). That fallback already existed
//   in the onTrade consumer for exactly this "no quote available yet"
//   case - it's just permanently in effect now for stocks.
// - Free tier caps at 50 subscribed WebSocket symbols total.
function createFinnhubHub({ apiKey, onTrade, onStatus }) {
  let socket = null;
  let socketOpen = false;

  // clientId -> Set<symbol>
  const clientSymbols = new Map();
  // last known trade price per symbol, so the data proxy can answer
  // "what's the current price" without a second data source
  const lastPrice = new Map();
  // Symbols we've already logged one raw trade sample for, to verify
  // field names/shapes match what's expected without logging every tick.
  const loggedTradeSample = new Set();

  function desiredSymbols() {
    const set = new Set();
    for (const symbols of clientSymbols.values()) {
      for (const s of symbols) set.add(s);
    }
    return set;
  }

  function ensureConnected() {
    if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) return;

    socket = new WebSocket(`wss://ws.finnhub.io?token=${apiKey}`);

    socket.on('open', () => {
      socketOpen = true;
      const symbols = [...desiredSymbols()];
      for (const s of symbols) socket.send(JSON.stringify({ type: 'subscribe', symbol: s }));
      onStatus?.('connected');
    });

    socket.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw.toString()); } catch (e) { return; }

      if (msg.type === 'error') {
        console.error('[finnhub] error:', msg.msg || JSON.stringify(msg));
        onStatus?.('error', msg.msg);
        return;
      }

      if (msg.type !== 'trade' || !Array.isArray(msg.data)) return;

      for (const trade of msg.data) {
        const symbol = trade.s;
        if (!symbol) continue;

        if (!loggedTradeSample.has(symbol)) {
          loggedTradeSample.add(symbol);
          console.log(`[finnhub] Sample raw trade message for ${symbol} (verify field names look right):`, JSON.stringify(trade));
        }

        const price = Number(trade.p);
        const prevPrice = lastPrice.get(symbol);
        lastPrice.set(symbol, price);

        onTrade({
          symbol,
          price,
          size: trade.v,
          prevPrice, // only signal available for buy-vs-sell classification - see file header, no quote stream on this feed
          bid: undefined,
          ask: undefined,
          timeMs: Number(trade.t),
        });
      }
    });

    socket.on('error', (err) => console.error('[finnhub] ws error:', err.message));
    socket.on('close', () => {
      socketOpen = false;
      socket = null;
      onStatus?.('disconnected');
    });
  }

  function reconcile(previousSymbols, newSymbols) {
    ensureConnected();
    if (!(socket && socket.readyState === WebSocket.OPEN && socketOpen)) return; // onopen will send the full set

    const desired = desiredSymbols();
    const toUnsub = [...(previousSymbols || [])].filter((s) => !desired.has(s));
    const toSub = [...(newSymbols || [])].filter((s) => desired.has(s)); // only actually-still-desired

    for (const s of toUnsub) socket.send(JSON.stringify({ type: 'unsubscribe', symbol: s }));
    for (const s of toSub) socket.send(JSON.stringify({ type: 'subscribe', symbol: s }));
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

module.exports = { createFinnhubHub };
