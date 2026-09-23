const WebSocket = require('ws');

const NEAR_MONEY_STRIKES = 10;
const MAX_BIGFLOW_EXPIRATIONS = 6; // nearest N expirations watched for the "big flow, any expiration" feature - keeps subscription size sane instead of watching literally every listed expiration

// Background flow: watches a fixed curated list of 15 tickers across the
// same 6-expiration window as Big Flow, but trimmed to fewer strikes per
// side (3 vs Big Flow's 10) - genuinely large prints cluster near-the-
// money, so this trades strike-range depth for being able to cover many
// tickers simultaneously, permanently, regardless of what any browser is
// actively viewing. ~36 contracts/symbol x 15 symbols ~= 540 total. Raise
// BACKGROUND_FLOW_STRIKES_EACH_SIDE toward NEAR_MONEY_STRIKES later (after
// upgrading Render - see the git history around this feature for why) if
// server load proves comfortable with room to spare.
const BACKGROUND_FLOW_SYMBOLS = [
  'SPY', 'QQQ', 'IWM',
  'AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL', 'META', 'TSLA', 'AMD', 'NFLX', 'AVGO',
  'COIN', 'PLTR',
];
const BACKGROUND_FLOW_STRIKES_EACH_SIDE = 3;

// Safety net - Tradier's streaming session almost certainly has a max
// symbol count, and nothing here previously stopped total subscribed
// contracts from silently growing past it (stale/abandoned "big flow"
// watches - 240 contracts each - are the single biggest contributor,
// and only get removed when their owning connection cleanly closes).
// When a new watch would push the total over this cap, the OLDEST
// big-flow entries get evicted first (largest and least essential -
// normal single-expiration watches, the curated leaderboard list, and
// the permanent background-flow watch are never evicted) until there's
// room. Raised from 900 to 1500 to make room for background flow
// (~540, permanent) alongside per-client big-flow watches (~240 each).
const MAX_TOTAL_CONTRACTS = 4000; // raised from 1500 after production logs showed Leaderboard + Background Flow alone reaching ~2428-2468 contracts - the original estimate for their combined footprint was too low, causing a resubscribe-thrashing loop (repeatedly evicting, still over cap, resubscribing, still over cap...) that was itself a real source of server load on top of the raw contract count. This leaves headroom above the observed baseline for active per-client watches too.

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
function createTradierHub({ token, onFlow, flowBuffer, onFlowEvent, onBackgroundFlow }) {
  let socket = null;
  let sessionId = null;
  let sessionPromise = null;
  let reconnectTimer = null;

  // Updated on EVERY message from Tradier - used by the staleness
  // watchdog below to detect a connection that's still open but has
  // silently stopped delivering data (observed directly in production:
  // socket stayed OPEN, no error, no close event, zero trades for hours
  // until a manual restart). Without this, a stall like that goes
  // completely unnoticed.
  let lastMessageAt = Date.now();
  const STALE_THRESHOLD_MS = 60_000; // no message in 60s while contracts are watched = assume dead
  const STALE_CHECK_INTERVAL_MS = 20_000;
  let connectingSince = null; // set when a new WebSocket is created, cleared once it resolves (open or close) - lets the watchdog detect a handshake that never resolves either way
  const CONNECTING_TIMEOUT_MS = 20_000; // a real handshake should resolve in low single-digit seconds; this long stuck in CONNECTING means something's actually hung, not just slow

  // clientId -> { symbol, expiration, contracts: Map<occSymbol, {strike, type}> }
  const clientState = new Map();
  const quoteCache = new Map(); // occSymbol -> { bid, ask }

  // Leaderboard tracking - separate from per-client state, always active
  // regardless of whether any browser is watching these specific symbols.
  const leaderboardContracts = new Map(); // occSymbol -> { underlying, strike, type }
  const premiumTotals = new Map(); // underlying -> { call: number, put: number, total: number }
  let leaderboardResetDate = todayDateString();
  let leaderboardReady = false;

  // Background flow tracking - same "always active" shape as the
  // leaderboard above, for a completely separate purpose (surfacing large
  // prints on BACKGROUND_FLOW_SYMBOLS regardless of what any browser is
  // actively viewing, not accumulating a premium total).
  const backgroundFlowContracts = new Map(); // occSymbol -> { underlying, strike, type }

  function allContracts() {
    const merged = new Map(); // occSymbol -> { strike, type }
    for (const state of clientState.values()) {
      for (const [sym, info] of state.contracts) merged.set(sym, info);
    }
    for (const [sym, info] of leaderboardContracts) {
      if (!merged.has(sym)) merged.set(sym, info);
    }
    for (const [sym, info] of backgroundFlowContracts) {
      if (!merged.has(sym)) merged.set(sym, info);
    }
    return merged;
  }

  // Evicts the OLDEST big-flow watches (":bigflow" keys, 240 contracts
  // each - by far the largest single contributor when they accumulate)
  // until the total is back under MAX_TOTAL_CONTRACTS. clientState is a
  // Map, which iterates in insertion order, so the first ":bigflow" key
  // encountered is genuinely the oldest still-registered one. Normal
  // single-expiration client watches and the curated leaderboard list
  // are never evicted - only ever the big-flow entries, since those are
  // the ones that leak when a connection doesn't close cleanly.
  function evictIfOverCap() {
    let total = allContracts().size;
    if (total <= MAX_TOTAL_CONTRACTS) return;

    for (const [key, state] of clientState) {
      if (total <= MAX_TOTAL_CONTRACTS) break;
      if (!key.endsWith(':bigflow')) continue;
      console.warn(`[tradier] EVICTING stale big-flow watch ${key} (${state.symbol}, ${state.contracts.size} contracts) - total was ${total}, cap is ${MAX_TOTAL_CONTRACTS}`);
      clientState.delete(key);
      total = allContracts().size;
    }

    if (total > MAX_TOTAL_CONTRACTS) {
      console.warn(`[tradier] still over cap after evicting all big-flow watches: ${total} contracts (cap ${MAX_TOTAL_CONTRACTS}) - likely too many DISTINCT normal watches or leaderboard symbols`);
    }
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
      try {
        const res = await fetch('https://api.tradier.com/v1/markets/events/session', {
          method: 'POST',
          headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' },
        });
        const data = await res.json();
        if (!res.ok || !data.stream?.sessionid) {
          throw new Error(`Tradier session error: ${data.fault?.faultstring || res.status}`);
        }
        sessionId = data.stream.sessionid;
        return sessionId;
      } finally {
        // Reset regardless of success or failure - previously this only
        // happened on success, so a single failed fetch (a transient
        // network blip, a momentary Tradier API error) would leave
        // sessionPromise permanently pointing at that same rejected
        // promise. Every future reconnect attempt - including the
        // staleness watchdog's forced reconnects - would immediately get
        // that same stale rejection back without ever actually retrying
        // the fetch, staying broken until the whole process restarted.
        sessionPromise = null;
      }
    })();

    return sessionPromise;
  }

  function handleMessage(raw) {
    lastMessageAt = Date.now();
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

          // TEMP DIAGNOSTIC: shows exactly which clientIds get notified for
          // each matching trade, so we can trace whether a SPECIFIC client
          // ever gets included here at all. Remove once the cross-client
          // flow-delivery investigation is resolved.
          console.log(`[tradier] TRADE ${msg.symbol} $${premium.toFixed(0)} -> notifying ${matches.length} client(s): ${matches.map((m) => m.clientId.slice(0, 8)).join(', ')}`);

          // Every client watching this contract gets it - not just whichever
          // one happened to register first.
          const recordedKeys = new Set(); // dedupe - multiple clients can watch the same symbol+expiration
          matches.forEach(({ clientId, info }) => {
            // "bigflow" watches are stored under a derived key
            // (`${realClientId}:bigflow`) so they can reuse all the same
            // subscription/routing machinery as a normal single-expiration
            // watch, without colliding with that same client's normal
            // watch entry. Strip the suffix to deliver to the real
            // connection, and flag it so the frontend knows to show it in
            // the big-flow UI (with its expiration) rather than the normal
            // sidebar.
            const isBigFlow = clientId.endsWith(':bigflow');
            const realClientId = isBigFlow ? clientId.slice(0, -':bigflow'.length) : clientId;

            const flowEvent = {
              optionType: info.type,
              strike: info.strike,
              premium,
              side,
              timeMs,
              ...(isBigFlow ? { kind: 'bigflow', expiration: info.expiration } : {}),
            };

            // Record once per unique symbol+expiration, even if several
            // clients are watching it - the event is identical for all of
            // them, no need to store duplicates.
            if (flowBuffer) {
              const watcherState = clientState.get(clientId);
              if (watcherState) {
                const bufferKey = isBigFlow ? `${watcherState.symbol}:bigflow` : `${watcherState.symbol}:${watcherState.expiration}`;
                if (!recordedKeys.has(bufferKey)) {
                  recordedKeys.add(bufferKey);
                  flowBuffer.record(bufferKey, flowEvent);
                  // Same one-per-unique-event dedup as flowBuffer above -
                  // history only needs one row per real trade, regardless
                  // of how many clients happen to be watching it. Skipped
                  // for bigflow entries (kind === 'bigflow') since those
                  // are the SAME underlying trade as the normal watch
                  // already records, just relabeled for a different UI -
                  // recording both would duplicate every event in history.
                  if (onFlowEvent && !isBigFlow) {
                    onFlowEvent({
                      symbol: watcherState.symbol,
                      assetType: 'option',
                      side,
                      size: null,
                      value: premium,
                      optionType: info.type,
                      strike: info.strike,
                      expiration: watcherState.expiration,
                      timeMs,
                    });
                  }
                }
              }
            }

            onFlow(realClientId, flowEvent);
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

        // ---- background flow (curated 15-ticker watchlist) ----
        // No premium threshold applied here - every matching trade gets
        // reported, and the threshold is applied client-side (same
        // MAJOR/OTHER split Big Flow already uses), consistent with how
        // Big Flow already works rather than inventing a second filtering
        // point.
        if (onBackgroundFlow) {
          const bgInfo = backgroundFlowContracts.get(msg.symbol);
          if (bgInfo) {
            const q = quoteCache.get(msg.symbol);
            let side = 'BOUGHT';
            if (q && !isNaN(q.bid) && price <= q.bid) side = 'SOLD';
            onBackgroundFlow({
              symbol: bgInfo.underlying,
              optionType: bgInfo.type,
              strike: bgInfo.strike,
              premium,
              side,
              timeMs,
            });
          }
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
    evictIfOverCap();
    const symbols = [...allContracts().keys()];
    if (symbols.length === 0) return;

    try {
      const sid = await ensureSession();

      if (!socket || socket.readyState === WebSocket.CLOSED) {
        socket = new WebSocket('wss://ws.tradier.com/v1/markets/events');
        connectingSince = Date.now();

        socket.on('open', () => {
          connectingSince = null;
          const freshSymbols = [...allContracts().keys()];
          console.log(`[tradier] NEW socket subscribing: ${freshSymbols.length} total (${summarizeByRoot(freshSymbols)})`);
          socket.send(JSON.stringify({
            symbols: freshSymbols,
            sessionid: sid,
            filter: ['trade', 'quote'],
            linebreak: true,
          }));
          lastMessageAt = Date.now();
        });
        socket.on('message', handleMessage);
        socket.on('error', (err) => console.error('[tradier] ws error:', err.message));
        socket.on('close', () => {
          connectingSince = null;
          socket = null;
          sessionId = null;
          // Reconnect on our own initiative rather than waiting for some
          // client to happen to call watch/unwatch next - without this, a
          // dropped connection could sit dead indefinitely with no one
          // watching for it. Only bother if anyone still actually wants
          // data.
          if (allContracts().size > 0) {
            clearTimeout(reconnectTimer);
            reconnectTimer = setTimeout(() => rebuildSubscription(), 2000);
          }
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

  // Periodically checks for three failure modes: (1) a connection that
  // LOOKS open but has gone quiet (see lastMessageAt above - observed
  // directly in production: no error, no close event, the socket just
  // stopped delivering trades until a manual restart), (2) NO connection
  // at all while contracts are still watched - the 'close' handler above
  // schedules one reconnect attempt on its own, but if THAT ever fails
  // for any reason (the sessionPromise bug fixed above was exactly this
  // kind of gap), nothing was left to notice and retry, and (3) a socket
  // stuck CONNECTING that never resolves to open OR close - also
  // observed directly in production (7+ minutes stuck, "onopen will use
  // freshest list" repeating with nothing ever arriving). Modes 1 and 2
  // only ever covered a socket that was OPEN or altogether absent; a
  // hung handshake in between was invisible to both.
  setInterval(() => {
    // TEMP DIAGNOSTIC: unconditional, fires on EVERY tick regardless of
    // what happens below - added because the CONNECTING-timeout branch
    // (a few lines down) wasn't firing despite being confirmed deployed
    // during a real multi-minute stuck-CONNECTING incident. This will
    // show directly whether the watchdog is even running during that
    // window, and exactly what state it sees each time. Remove once
    // that's resolved.
    console.log(`[tradier][watchdog] tick: socket=${socket ? socket.readyState : 'null'} connectingSince=${connectingSince} contracts=${allContracts().size} msAgo=${connectingSince ? Date.now() - connectingSince : 'n/a'}`);

    if (allContracts().size === 0) return;

    if (!socket) {
      console.warn(`[tradier] no active connection with ${allContracts().size} contracts watched - forcing reconnect`);
      rebuildSubscription();
      return;
    }

    if (socket.readyState === WebSocket.CONNECTING) {
      if (connectingSince && Date.now() - connectingSince > CONNECTING_TIMEOUT_MS) {
        console.warn(`[tradier] socket stuck CONNECTING for ${Math.round((Date.now() - connectingSince) / 1000)}s (handshake never resolved) - forcing reconnect`);
        socket.terminate(); // fires 'close' above, which clears connectingSince/socket and schedules the reconnect
      }
      return;
    }

    if (socket.readyState !== WebSocket.OPEN) return;
    if (Date.now() - lastMessageAt < STALE_THRESHOLD_MS) return;

    console.warn(`[tradier] connection appears stale (no message in ${Math.round((Date.now() - lastMessageAt) / 1000)}s with ${allContracts().size} contracts watched) - forcing reconnect`);
    socket.terminate(); // fires 'close' above, which schedules the reconnect
  }, STALE_CHECK_INTERVAL_MS);

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

  // Fetches near-the-money contracts across the nearest MAX_BIGFLOW_
  // EXPIRATIONS expirations for one symbol and registers them into
  // backgroundFlowContracts. Same multi-expiration shape as watchBigFlow,
  // but writes into the permanent background map instead of a per-client
  // watch, and uses the trimmed BACKGROUND_FLOW_STRIKES_EACH_SIDE instead
  // of NEAR_MONEY_STRIKES - see the constant comments for why.
  async function registerSymbolForBackgroundFlow(symbol, spotPrice) {
    const expUrl = `https://api.tradier.com/v1/markets/options/expirations?symbol=${encodeURIComponent(symbol)}`;
    const expRes = await fetch(expUrl, { headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' } });
    const expData = await expRes.json();
    if (!expRes.ok) throw new Error(`expirations request failed (status ${expRes.status})`);

    let dates = expData.expirations?.date || [];
    if (!Array.isArray(dates)) dates = [dates];
    if (dates.length === 0) throw new Error('no expirations found');
    const targetExpirations = dates.slice(0, MAX_BIGFLOW_EXPIRATIONS);

    let centerUsed = spotPrice;
    if (!centerUsed) {
      try {
        centerUsed = await fetchQuotePrice(token, symbol);
      } catch (quoteErr) {
        console.warn(`[tradier] Background-flow quote fallback failed for ${symbol}:`, quoteErr.message);
        centerUsed = 0;
      }
    }

    let registered = 0;
    for (const expiration of targetExpirations) {
      try {
        const chainUrl = `https://api.tradier.com/v1/markets/options/chains?symbol=${encodeURIComponent(symbol)}&expiration=${expiration}`;
        const chainRes = await fetch(chainUrl, { headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' } });
        const chainData = await chainRes.json();
        if (!chainRes.ok) throw new Error(`chain request failed (status ${chainRes.status})`);

        let options = chainData.options?.option || [];
        if (!Array.isArray(options)) options = [options];
        if (options.length === 0) continue;

        const nearMoney = filterNearTheMoney(options, centerUsed || options[0].strike, BACKGROUND_FLOW_STRIKES_EACH_SIDE);
        nearMoney.forEach((o) => {
          backgroundFlowContracts.set(o.symbol, {
            underlying: symbol,
            strike: o.strike,
            type: (o.option_type || '').toUpperCase(),
          });
          registered += 1;
        });
      } catch (err) {
        console.error(`[tradier] Background-flow chain fetch failed for ${symbol} ${expiration}:`, err.message);
      }
    }
    return registered;
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

      const history = flowBuffer ? flowBuffer.getRecent(`${symbol}:${expiration}`) : [];
      return { count: contracts.size, history };
    },

    unwatch(clientId) {
      const state = clientState.get(clientId);
      if (!state) return;
      // TEMP DIAGNOSTIC: confirms disconnect cleanup is actually firing
      // and removing contracts, rather than having to infer it indirectly
      // from total subscription size. Remove once the stale-subscription
      // investigation is resolved.
      console.log(`[tradier] UNWATCH ${state.symbol} client=${clientId.slice(0, 8)} (${state.contracts.size} contracts) - total before removal: ${allContracts().size}`);
      clientState.delete(clientId);
      rebuildSubscription();
    },

    // "Big flow, any expiration": watches near-the-money contracts across
    // the nearest MAX_BIGFLOW_EXPIRATIONS expirations for one symbol,
    // instead of just whichever single expiration is selected in the
    // normal dropdown. Stored under a derived clientId
    // (`${clientId}:bigflow`) so it coexists with that same client's
    // normal single-expiration watch without overwriting it.
    async watchBigFlow(clientId, symbol, spotPrice) {
      const expUrl = `https://api.tradier.com/v1/markets/options/expirations?symbol=${encodeURIComponent(symbol)}`;
      const expRes = await fetch(expUrl, { headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' } });
      const expData = await expRes.json();
      if (!expRes.ok) throw new Error(`expirations request failed (status ${expRes.status})`);

      let dates = expData.expirations?.date || [];
      if (!Array.isArray(dates)) dates = [dates];
      if (dates.length === 0) throw new Error('no expirations found');
      const targetExpirations = dates.slice(0, MAX_BIGFLOW_EXPIRATIONS);

      let centerUsed = spotPrice;
      if (!centerUsed) {
        try {
          centerUsed = await fetchQuotePrice(token, symbol);
        } catch (quoteErr) {
          console.warn(`[tradier] Big-flow quote fallback failed for ${symbol}:`, quoteErr.message);
          centerUsed = 0;
        }
      }

      const contracts = new Map();
      for (const expiration of targetExpirations) {
        try {
          const chainUrl = `https://api.tradier.com/v1/markets/options/chains?symbol=${encodeURIComponent(symbol)}&expiration=${expiration}`;
          const chainRes = await fetch(chainUrl, { headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' } });
          const chainData = await chainRes.json();
          if (!chainRes.ok) throw new Error(`chain request failed (status ${chainRes.status})`);

          let options = chainData.options?.option || [];
          if (!Array.isArray(options)) options = [options];
          if (options.length === 0) continue;

          const nearMoney = filterNearTheMoney(options, centerUsed || options[0].strike, NEAR_MONEY_STRIKES);
          nearMoney.forEach((o) => {
            contracts.set(o.symbol, { strike: o.strike, type: (o.option_type || '').toUpperCase(), expiration });
          });
        } catch (err) {
          console.error(`[tradier] Big-flow chain fetch failed for ${symbol} ${expiration}:`, err.message);
        }
      }

      const key = `${clientId}:bigflow`;
      clientState.set(key, { symbol, expiration: 'MULTI', contracts });
      await rebuildSubscription();

      console.log(`[tradier] BIG FLOW WATCH ${symbol} client=${clientId.slice(0, 8)} across ${targetExpirations.length} expirations, ${contracts.size} contracts`);
      const history = flowBuffer ? flowBuffer.getRecent(`${symbol}:bigflow`) : [];
      return { count: contracts.size, history };
    },

    unwatchBigFlow(clientId) {
      const key = `${clientId}:bigflow`;
      const state = clientState.get(key);
      if (!state) return;
      // TEMP DIAGNOSTIC: same purpose as the unwatch() log above - big-flow
      // watches are the largest contributor (240 contracts each) when
      // something doesn't clean up properly. Remove once the
      // stale-subscription investigation is resolved.
      console.log(`[tradier] UNWATCH BIG FLOW ${state.symbol} client=${clientId.slice(0, 8)} (${state.contracts.size} contracts) - total before removal: ${allContracts().size}`);
      clientState.delete(key);
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

    // Fetches near-the-money contracts (trimmed depth - see
    // BACKGROUND_FLOW_STRIKES_EACH_SIDE) across the nearest 6 expirations
    // for every BACKGROUND_FLOW_SYMBOLS name and registers them into the
    // shared subscription. Call once at server startup, alongside
    // initLeaderboard - this is permanent background watching, not tied
    // to any specific browser client.
    async initBackgroundFlow() {
      console.log(`[tradier] Setting up background flow watch for ${BACKGROUND_FLOW_SYMBOLS.length} symbols...`);

      for (const symbol of BACKGROUND_FLOW_SYMBOLS) {
        try {
          let spotPrice = null;
          try {
            spotPrice = await fetchQuotePrice(token, symbol);
          } catch (quoteErr) {
            console.warn(`[tradier] Could not get a real quote for ${symbol}, falling back to chain median:`, quoteErr.message);
          }
          const count = await registerSymbolForBackgroundFlow(symbol, spotPrice);
          console.log(`[tradier] Background flow: ${symbol} - ${count} contracts across up to ${MAX_BIGFLOW_EXPIRATIONS} expirations`);
        } catch (err) {
          console.error(`[tradier] Background flow setup failed for ${symbol}:`, err.message);
        }
      }

      console.log(`[tradier] Background flow ready: watching ${backgroundFlowContracts.size} contracts across ${BACKGROUND_FLOW_SYMBOLS.length} symbols`);
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

      const LEADERBOARD_DISPLAY_LIMIT = 10;
      return {
        ready: leaderboardReady,
        resetDate: leaderboardResetDate,
        rankings: [...premiumTotals.entries()]
          .map(([symbol, totals]) => ({ symbol, ...totals }))
          .sort((a, b) => b.total - a.total)
          .slice(0, LEADERBOARD_DISPLAY_LIMIT),
      };
    },
  };
}

module.exports = { createTradierHub };
