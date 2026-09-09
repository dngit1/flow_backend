const WebSocket = require('ws');

const NEAR_MONEY_STRIKES = 10;

function filterNearTheMoney(options, spotPrice, strikesEachSide) {
  const uniqueStrikes = [...new Set(options.map((o) => o.strike))]
    .sort((a, b) => Math.abs(a - spotPrice) - Math.abs(b - spotPrice))
    .slice(0, strikesEachSide * 2);
  const keep = new Set(uniqueStrikes);
  return options.filter((o) => keep.has(o.strike));
}

// Manages exactly ONE Tradier streaming session/socket, no matter how many
// browser clients are watching option flow. Each browser client watches
// one (symbol, expiration) pair at a time; this hub fetches that client's
// near-the-money contracts, merges everyone's contracts into one
// subscription, and routes incoming trades back to the right client(s).
function createTradierHub({ token, onFlow }) {
  let socket = null;
  let sessionId = null;
  let sessionPromise = null;

  // clientId -> { symbol, expiration, contracts: Map<occSymbol, {strike, type}> }
  const clientState = new Map();
  const quoteCache = new Map(); // occSymbol -> { bid, ask }

  function allContracts() {
    const merged = new Map(); // occSymbol -> { strike, type }
    for (const state of clientState.values()) {
      for (const [sym, info] of state.contracts) merged.set(sym, info);
    }
    return merged;
  }

  function resolve(occSymbol) {
    for (const [clientId, state] of clientState) {
      const info = state.contracts.get(occSymbol);
      if (info) return { clientId, info };
    }
    return null;
  }

  async function ensureSession() {
    if (sessionId && socket && socket.readyState === WebSocket.OPEN) return sessionId;
    if (sessionPromise) return sessionPromise;

    sessionPromise = (async () => {
      const res = await fetch('https://api.tradier.com/v1/markets/events/session', {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' },
      });
      const data = await res.json();
      if (!res.ok || !data.stream?.sessionid) {
        throw new Error(`Tradier session error: ${data.fault?.faultstring || res.status}`);
      }
      sessionId = data.stream.sessionid;
      sessionPromise = null;
      return sessionId;
    })();

    return sessionPromise;
  }

  function handleMessage(raw) {
    raw.toString().split('\n').forEach((line) => {
      const trimmed = line.trim();
      if (!trimmed) return;
      let msg;
      try { msg = JSON.parse(trimmed); } catch (e) { return; }

      if (msg.type === 'quote') {
        quoteCache.set(msg.symbol, { bid: parseFloat(msg.bid), ask: parseFloat(msg.ask) });
        return;
      }

      if (msg.type === 'trade') {
        const resolved = resolve(msg.symbol);
        if (!resolved) return;

        const price = parseFloat(msg.price);
        const size = parseFloat(msg.size);
        const premium = price * size * 100;

        const q = quoteCache.get(msg.symbol);
        let side = 'BOUGHT';
        if (q && !isNaN(q.bid) && price <= q.bid) side = 'SOLD';

        onFlow(resolved.clientId, {
          optionType: resolved.info.type,
          strike: resolved.info.strike,
          premium,
          side,
          timeMs: msg.date ? parseInt(msg.date, 10) : Date.now(),
        });
      }
    });
  }

  async function rebuildSubscription() {
    const symbols = [...allContracts().keys()];
    if (symbols.length === 0) return;

    try {
      const sid = await ensureSession();

      if (!socket || socket.readyState === WebSocket.CLOSED) {
        socket = new WebSocket('wss://ws.tradier.com/v1/markets/events');

        socket.on('open', () => {
          socket.send(JSON.stringify({
            symbols: [...allContracts().keys()], // freshest list, not a stale closure value
            sessionid: sid,
            filter: ['trade', 'quote'],
            linebreak: true,
          }));
        });
        socket.on('message', handleMessage);
        socket.on('error', (err) => console.error('[tradier] ws error:', err.message));
        socket.on('close', () => {
          socket = null;
          sessionId = null;
        });
      } else if (socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify({ symbols, sessionid: sid, filter: ['trade', 'quote'], linebreak: true }));
      }
      // else CONNECTING: onopen above will send the latest list
    } catch (err) {
      console.error('[tradier] failed to rebuild subscription:', err.message);
    }
  }

  return {
    // Called when a browser client wants flow for a symbol/expiration.
    // spotPrice comes from the Alpaca hub's lastPriceOf() so the backend
    // doesn't need a second round trip to get a reference price.
    async watch(clientId, symbol, expiration, spotPrice) {
      const url = `https://api.tradier.com/v1/markets/options/chains?symbol=${encodeURIComponent(symbol)}&expiration=${expiration}`;
      const res = await fetch(url, { headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' } });
      const data = await res.json();
      if (!res.ok) throw new Error(`Tradier chain error: ${data.fault?.faultstring || res.status}`);

      let options = data.options?.option || [];
      if (!Array.isArray(options)) options = [options];

      const nearMoney = filterNearTheMoney(options, spotPrice || options[0]?.strike || 0, NEAR_MONEY_STRIKES);
      const contracts = new Map();
      nearMoney.forEach((o) => contracts.set(o.symbol, { strike: o.strike, type: (o.option_type || '').toUpperCase() }));

      clientState.set(clientId, { symbol, expiration, contracts });
      await rebuildSubscription();

      return contracts.size;
    },

    unwatch(clientId) {
      if (!clientState.has(clientId)) return;
      clientState.delete(clientId);
      rebuildSubscription();
    },
  };
}

module.exports = { createTradierHub };
