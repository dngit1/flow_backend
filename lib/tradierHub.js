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

  function resolveAll(occSymbol) {
    const matches = [];
    for (const [clientId, state] of clientState) {
      const info = state.contracts.get(occSymbol);
      if (info) matches.push({ clientId, info });
    }
    return matches;
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
        const matches = resolveAll(msg.symbol);
        if (matches.length === 0) return;

        const price = parseFloat(msg.price);
        const size = parseFloat(msg.size);
        const premium = price * size * 100;

        const q = quoteCache.get(msg.symbol);
        let side = 'BOUGHT';
        if (q && !isNaN(q.bid) && price <= q.bid) side = 'SOLD';

        const timeMs = msg.date ? parseInt(msg.date, 10) : Date.now();

        // Every client watching this contract gets it - not just whichever
        // one happened to register first.
        matches.forEach(({ clientId, info }) => {
          onFlow(clientId, {
            optionType: info.type,
            strike: info.strike,
            premium,
            side,
            timeMs,
          });
        });
      }
    });
  }

  // TEMP DIAGNOSTIC: summarize a symbol list by underlying root ticker so
  // we can confirm the merge across multiple watching clients is actually
  // correct. Remove once the SPY/QQQ investigation is done.
  function summarizeByRoot(symbols) {
    const counts = {};
    symbols.forEach((s) => {
      const m = s.match(/^([A-Z]+)\d{6}[CP]\d{8}$/);
      const root = m ? m[1] : 'UNKNOWN';
      counts[root] = (counts[root] || 0) + 1;
    });
    return Object.entries(counts).map(([root, n]) => `${root}:${n}`).join(', ');
  }

  async function rebuildSubscription() {
    const symbols = [...allContracts().keys()];
    if (symbols.length === 0) return;

    try {
      const sid = await ensureSession();

      if (!socket || socket.readyState === WebSocket.CLOSED) {
        socket = new WebSocket('wss://ws.tradier.com/v1/markets/events');

        socket.on('open', () => {
          const freshSymbols = [...allContracts().keys()];
          console.log(`[tradier] NEW socket subscribing: ${freshSymbols.length} total (${summarizeByRoot(freshSymbols)})`);
          socket.send(JSON.stringify({
            symbols: freshSymbols,
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
        console.log(`[tradier] RESENDING on open socket: ${symbols.length} total (${summarizeByRoot(symbols)})`);
        socket.send(JSON.stringify({ symbols, sessionid: sid, filter: ['trade', 'quote'], linebreak: true }));
      } else {
        console.log(`[tradier] socket still CONNECTING - skipping send, onopen will use freshest list`);
      }
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
