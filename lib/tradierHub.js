const WebSocket = require('ws');

const NEAR_MONEY_STRIKES = 10;

// Curated list of the most liquid, well-known names for the premium
// leaderboard. Not a full-market scan (Tradier can't do that - see
// createTradierHub's comment below) but these names carry the large
// majority of all option premium traded on any given day anyway.
const LEADERBOARD_SYMBOLS = [
  'SPY', 'QQQ', 'IWM', 'DIA',
  'AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL', 'META', 'TSLA', 'AMD', 'NFLX', 'AVGO',
  'JPM', 'BAC', 'XOM', 'CVX', 'WMT', 'COST', 'HD', 'DIS', 'BA',
  'INTC', 'CRM', 'ORCL', 'ADBE', 'PYPL', 'UBER', 'COIN', 'PLTR', 'MU', 'SMCI', 'ARM',
];

function filterNearTheMoney(options, spotPrice, strikesEachSide) {
  const uniqueStrikes = [...new Set(options.map((o) => o.strike))]
    .sort((a, b) => Math.abs(a - spotPrice) - Math.abs(b - spotPrice))
    .slice(0, strikesEachSide * 2);
  const keep = new Set(uniqueStrikes);
  return options.filter((o) => keep.has(o.strike));
}

function todayDateString() {
  return new Date().toISOString().slice(0, 10); // "YYYY-MM-DD", UTC-based
}

// Real last-traded price via Tradier's quotes endpoint. Used at server
// startup for the leaderboard's curated symbols, where there's no client
// yet to supply a live price - without this, near-the-money selection
// fell back to the chain's median strike, which can be far from the real
// price and cause the leaderboard's own tracking to watch the wrong
// strikes (see the SPY/QQQ inflated-contract-count investigation).
async function fetchQuotePrice(token, symbol) {
  const url = `https://api.tradier.com/v1/markets/quotes?symbols=${encodeURIComponent(symbol)}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' } });
  const data = await res.json();
  if (!res.ok) throw new Error(`quote request failed (status ${res.status})`);
  const quote = data.quotes?.quote;
  const q = Array.isArray(quote) ? quote[0] : quote;
  const price = parseFloat(q?.last);
  if (!price || isNaN(price)) throw new Error('no last price in quote response');
  return price;
}

// Manages exactly ONE Tradier streaming session/socket, no matter how many
// browser clients are watching option flow, AND regardless of the
// always-on premium leaderboard tracking below - Tradier explicitly
// disallows more than one session per account, so everything rides on
// this single shared connection.
//
// IMPORTANT LIMITATION on the leaderboard specifically: Tradier (like most
// broker APIs) only streams data for symbols you explicitly subscribe to -
// there's no "scan the whole market" endpoint. LEADERBOARD_SYMBOLS above is
// a fixed, curated list, not a true full-market ranking. Adding/removing
// names means editing that array and redeploying.
function createTradierHub({ token, onFlow }) {
  let socket = null;
  let sessionId = null;
  let sessionPromise = null;

  // clientId -> { symbol, expiration, contracts: Map<occSymbol, {strike, type}> }
  const clientState = new Map();
  const quoteCache = new Map(); // occSymbol -> { bid, ask }

  // Leaderboard tracking - separate from per-client state, always active
  // regardless of whether any browser is watching these specific symbols.
  const leaderboardContracts = new Map(); // occSymbol -> { underlying, strike, type }
  const premiumTotals = new Map(); // underlying -> { call: number, put: number, total: number }
  let leaderboardResetDate = todayDateString();
  let leaderboardReady = false;

  function allContracts() {
    const merged = new Map(); // occSymbol -> { strike, type }
    for (const state of clientState.values()) {
      for (const [sym, info] of state.contracts) merged.set(sym, info);
    }
    for (const [sym, info] of leaderboardContracts) {
      if (!merged.has(sym)) merged.set(sym, info);
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
        const price = parseFloat(msg.price);
        const size = parseFloat(msg.size);
        const premium = price * size * 100;
        const timeMs = msg.date ? parseInt(msg.date, 10) : Date.now();

        // ---- per-client flow routing (existing behavior) ----
        const matches = resolveAll(msg.symbol);
        if (matches.length > 0) {
          const q = quoteCache.get(msg.symbol);
          let side = 'BOUGHT';
          if (q && !isNaN(q.bid) && price <= q.bid) side = 'SOLD';

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

        // ---- leaderboard premium accumulation ----
        const lbInfo = leaderboardContracts.get(msg.symbol);
        if (lbInfo) {
          const today = todayDateString();
          if (today !== leaderboardResetDate) {
            premiumTotals.clear();
            leaderboardResetDate = today;
          }

          const entry = premiumTotals.get(lbInfo.underlying) || { call: 0, put: 0, total: 0 };
          if (lbInfo.type === 'CALL') entry.call += premium;
          else entry.put += premium;
          entry.total += premium;
          premiumTotals.set(lbInfo.underlying, entry);
        }
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

  // Fetches nearest-expiration near-the-money contracts for one symbol and
  // registers them into leaderboardContracts. Shared by initLeaderboard
  // (curated list, at startup) and trackSymbol (any ticker, on demand).
  // spotPrice is optional - if omitted, uses the chain's median strike as
  // a rough center point (used at startup, before any client has loaded
  // a real price yet).
  async function registerSymbolForTracking(symbol, spotPrice) {
    const expUrl = `https://api.tradier.com/v1/markets/options/expirations?symbol=${encodeURIComponent(symbol)}`;
    const expRes = await fetch(expUrl, { headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' } });
    const expData = await expRes.json();
    if (!expRes.ok) throw new Error(`expirations request failed (status ${expRes.status})`);

    let dates = expData.expirations?.date || [];
    if (!Array.isArray(dates)) dates = [dates];
    if (dates.length === 0) throw new Error('no expirations found');
    const expiration = dates[0];

    const chainUrl = `https://api.tradier.com/v1/markets/options/chains?symbol=${encodeURIComponent(symbol)}&expiration=${expiration}`;
    const chainRes = await fetch(chainUrl, { headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' } });
    const chainData = await chainRes.json();
    if (!chainRes.ok) throw new Error(`chain request failed (status ${chainRes.status})`);

    let options = chainData.options?.option || [];
    if (!Array.isArray(options)) options = [options];
    if (options.length === 0) throw new Error('no option chain returned');

    let center = spotPrice;
    if (!center) {
      const strikes = options.map((o) => o.strike).sort((a, b) => a - b);
      center = strikes[Math.floor(strikes.length / 2)] || 0;
    }

    const nearMoney = filterNearTheMoney(options, center, NEAR_MONEY_STRIKES);
    nearMoney.forEach((o) => {
      leaderboardContracts.set(o.symbol, {
        underlying: symbol,
        strike: o.strike,
        type: (o.option_type || '').toUpperCase(),
      });
    });

    return nearMoney.length;
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

      let centerUsed = spotPrice;
      let centerSource = 'client-supplied';
      if (!centerUsed) {
        // Both the client's own price AND the server's cached price were
        // empty - rather than fall back to an arbitrary (often wildly
        // wrong) chain-order strike, get a real quote directly. This is
        // what was actually causing two devices to watch completely
        // different strikes for the "same" symbol - whichever one hit
        // this gap landed on a nonsense center like 500 instead of ~757.
        try {
          centerUsed = await fetchQuotePrice(token, symbol);
          centerSource = 'quote-fallback';
        } catch (quoteErr) {
          centerUsed = options[0]?.strike || 0;
          centerSource = 'chain-order (last resort)';
          console.warn(`[tradier] Quote fallback also failed for ${symbol}, using chain order:`, quoteErr.message);
        }
      }

      const nearMoney = filterNearTheMoney(options, centerUsed, NEAR_MONEY_STRIKES);
      const contracts = new Map();
      nearMoney.forEach((o) => contracts.set(o.symbol, { strike: o.strike, type: (o.option_type || '').toUpperCase() }));

      // TEMP DIAGNOSTIC: shows exactly what spot price and strikes each
      // client's watch() call actually used, so we can directly compare
      // two devices watching the "same" symbol+expiration instead of
      // guessing whether their windows overlap. Remove once the
      // cross-device flow-mismatch investigation is resolved.
      const strikeList = [...new Set(nearMoney.map((o) => o.strike))].sort((a, b) => a - b);
      console.log(`[tradier] WATCH ${symbol} ${expiration} client=${clientId.slice(0, 8)} source=${centerSource} center=${centerUsed} strikes=[${strikeList.join(',')}]`);

      clientState.set(clientId, { symbol, expiration, contracts });
      await rebuildSubscription();

      return contracts.size;
    },

    unwatch(clientId) {
      if (!clientState.has(clientId)) return;
      clientState.delete(clientId);
      rebuildSubscription();
    },

    // Fetches near-the-money contracts for every LEADERBOARD_SYMBOLS name
    // and registers them into the shared subscription. Call once at server
    // startup - it's a one-time setup, not per-client.
    async initLeaderboard() {
      console.log(`[tradier] Setting up premium leaderboard for ${LEADERBOARD_SYMBOLS.length} symbols...`);

      for (const symbol of LEADERBOARD_SYMBOLS) {
        try {
          let spotPrice = null;
          try {
            spotPrice = await fetchQuotePrice(token, symbol);
          } catch (quoteErr) {
            console.warn(`[tradier] Could not get a real quote for ${symbol}, falling back to chain median:`, quoteErr.message);
          }
          await registerSymbolForTracking(symbol, spotPrice);
        } catch (err) {
          console.error(`[tradier] leaderboard setup failed for ${symbol}:`, err.message);
        }
      }

      leaderboardReady = true;
      console.log(`[tradier] Premium leaderboard ready: watching ${leaderboardContracts.size} contracts across ${LEADERBOARD_SYMBOLS.length} symbols`);
      await rebuildSubscription();
    },

    // On-demand tracking for ANY ticker, not just the curated leaderboard
    // list. If already tracked (curated, or a previous check), this is a
    // no-op and just returns the current running totals - premium keeps
    // accumulating for the rest of the day regardless of how many times
    // it's checked. If not yet tracked, sets it up now: totals start at
    // zero and grow from this point forward - there's no way to backfill
    // premium that traded before tracking started (see module comment).
    async trackSymbol(symbol, spotPrice) {
      const alreadyTracked = [...leaderboardContracts.values()].some((c) => c.underlying === symbol);
      if (!alreadyTracked) {
        let center = spotPrice;
        if (!center) {
          try {
            center = await fetchQuotePrice(token, symbol);
          } catch (quoteErr) {
            console.warn(`[tradier] Could not get a real quote for ${symbol}, falling back to chain median:`, quoteErr.message);
          }
        }
        await registerSymbolForTracking(symbol, center);
        await rebuildSubscription();
      }

      const today = todayDateString();
      if (today !== leaderboardResetDate) {
        premiumTotals.clear();
        leaderboardResetDate = today;
      }

      const totals = premiumTotals.get(symbol) || { call: 0, put: 0, total: 0 };
      return { symbol, ...totals, newlyTracked: !alreadyTracked };
    },

    getLeaderboard() {
      const today = todayDateString();
      if (today !== leaderboardResetDate) {
        premiumTotals.clear();
        leaderboardResetDate = today;
      }

      return {
        ready: leaderboardReady,
        resetDate: leaderboardResetDate,
        rankings: [...premiumTotals.entries()]
          .map(([symbol, totals]) => ({ symbol, ...totals }))
          .sort((a, b) => b.total - a.total),
      };
    },
  };
}

module.exports = { createTradierHub };
