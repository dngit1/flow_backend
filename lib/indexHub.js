const { isIndexSymbol } = require('./indexSymbols');
const { fetchIndexQuote } = require('./indexData');

// Live prices for INDEX symbols (SPX etc.), which the stock feed can't
// supply - see indexSymbols.js. Polls Tradier's index quote (real-time per
// Tradier's docs) on a fixed interval.
//
// Budget: Tradier allows 120 market-data requests/minute per token, shared
// by EVERYTHING (chains, expirations, quotes, candles). One shared poll per
// index at ~2.5s is ~24 requests/minute no matter how many browsers are
// watching it, and at most MAX_POLLED_SYMBOLS run at once.
const POLL_INTERVAL_MS = 2500;
const MAX_BACKOFF_MS = 20_000;
const FAILURES_BEFORE_ERROR_STATUS = 3;
const MAX_POLLED_SYMBOLS = 4;
const MIN_GAP_BETWEEN_KICKS_MS = 1000;

function createIndexHub({ token, onTick, onClientStatus, pollIntervalMs = POLL_INTERVAL_MS }) {
  const subscribers = new Map(); // symbol -> Set<clientId>
  const pollers = new Map();     // symbol -> poller state
  const lastPrice = new Map();   // symbol -> last known level (kept after a poller stops)

  function notify(symbol, status, detail) {
    for (const clientId of subscribers.get(symbol) || []) onClientStatus?.(clientId, status, detail);
  }

  async function pollOnce(symbol, state) {
    if (state.stopped || state.inFlight) return;
    state.inFlight = true;
    state.lastPollAt = Date.now();
    let delay = pollIntervalMs;

    try {
      const quote = await fetchIndexQuote(token, symbol);
      if (state.stopped) return;

      state.failures = 0;
      lastPrice.set(symbol, quote.last);

      if (state.inError || state.announcePending) {
        state.inError = false;
        state.announcePending = false;
        notify(symbol, 'connected');
      }

      // Only tick when the level actually changed: an index that isn't
      // moving (market closed, quiet moment) shouldn't spam the chart
      // with identical prices - and outside market hours that would
      // otherwise keep stamping flat "new" candles at the current time.
      if (quote.last !== state.lastEmitted) {
        state.lastEmitted = quote.last;
        // Prefer the index's own last-update time; fall back to now if it's
        // missing or implausibly far in the future.
        const usable = quote.tradeDateMs && quote.tradeDateMs <= Date.now() + 60_000;
        onTick({ symbol, price: quote.last, timeMs: usable ? quote.tradeDateMs : Date.now() });
      }
    } catch (err) {
      state.failures++;
      // Back off progressively, and much harder if Tradier says we're going too fast.
      delay = Math.min(MAX_BACKOFF_MS, pollIntervalMs * (1 + state.failures) * (err.rateLimited ? 4 : 1));
      if (state.failures === 1 || state.failures % 10 === 0) {
        console.warn(`[indexHub] ${symbol} quote failed (${state.failures} in a row): ${err.message} - retrying in ${Math.round(delay / 1000)}s`);
      }
      if (state.failures >= FAILURES_BEFORE_ERROR_STATUS && !state.inError) {
        state.inError = true;
        notify(symbol, 'error', `no live price for ${symbol} from Tradier`);
      }
    } finally {
      state.inFlight = false;
    }

    if (!state.stopped) state.timer = setTimeout(() => pollOnce(symbol, state), delay);
  }

  function startPoller(symbol) {
    const state = {
      timer: null, stopped: false, inFlight: false,
      failures: 0, inError: false, lastEmitted: null, lastPollAt: 0,
      announcePending: true,
    };
    pollers.set(symbol, state);
    console.log(`[indexHub] polling ${symbol} every ${pollIntervalMs}ms`);
    pollOnce(symbol, state);
  }

  return {
    subscribe(clientId, symbol) {
      if (!isIndexSymbol(symbol)) return; // this hub only serves indexes
      let set = subscribers.get(symbol);
      if (!set) {
        if (pollers.size >= MAX_POLLED_SYMBOLS) {
          console.warn(`[indexHub] Ignoring subscribe for ${symbol} - already polling ${MAX_POLLED_SYMBOLS} indexes`);
          return;
        }
        set = new Set();
        subscribers.set(symbol, set);
      }
      set.add(clientId);

      const state = pollers.get(symbol);
      if (!state) { startPoller(symbol); return; }

      // A poller already exists (another browser is watching this index).
      // The newcomer would otherwise wait for the price to next CHANGE -
      // possibly a long time if the market is closed - so re-arm the
      // emit-on-change check and poll right away (rate-limited).
      state.lastEmitted = null;
      state.announcePending = true;
      if (!state.inFlight && Date.now() - state.lastPollAt > MIN_GAP_BETWEEN_KICKS_MS) {
        clearTimeout(state.timer);
        pollOnce(symbol, state);
      }
    },

    unsubscribe(clientId, symbol) {
      const set = subscribers.get(symbol);
      if (!set) return;
      set.delete(clientId);
      if (set.size > 0) return;

      subscribers.delete(symbol);
      const state = pollers.get(symbol);
      if (state) {
        state.stopped = true;
        clearTimeout(state.timer);
        pollers.delete(symbol);
        console.log(`[indexHub] stopped polling ${symbol} (no watchers left)`);
      }
    },

    unsubscribeAll(clientId) {
      for (const symbol of [...subscribers.keys()]) this.unsubscribe(clientId, symbol);
    },

    lastPriceOf(symbol) {
      return lastPrice.get(String(symbol).toUpperCase());
    },
  };
}

module.exports = { createIndexHub };
