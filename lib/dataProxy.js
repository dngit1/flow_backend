const express = require('express');
const { cached } = require('./cache');
const { isFuturesTicker } = require('./futuresHub');

function createDataProxyRouter({ twelveDataKey, tradierToken, lastPriceOf, futuresHub }) {
  const router = express.Router();

  // ---------------------------------------------------------------
  // GET /api/bars?symbol=AAPL (or a futures ticker like ESZ5)
  // Historical 1-min bars for seeding the candlestick chart.
  // Stock tickers -> Twelve Data (cached 20s). Futures tickers -> Massive,
  // via futuresHub (which has its own caching).
  // ---------------------------------------------------------------
  router.get('/bars', async (req, res) => {
    const symbol = String(req.query.symbol || '').toUpperCase();
    if (!symbol) return res.status(400).json({ error: 'symbol is required' });

    if (isFuturesTicker(symbol)) {
      try {
        const bars = await futuresHub.getHistoricalBars(symbol);
        return res.json({ bars });
      } catch (err) {
        console.error(`[dataProxy] futures bars failed for ${symbol}:`, err.message);
        return res.status(502).json({ error: 'Unable to load price history right now' });
      }
    }

    try {
      const bars = await cached(`bars:${symbol}`, 20_000, async () => {
        const url = `https://api.twelvedata.com/time_series` +
          `?symbol=${encodeURIComponent(symbol)}&interval=1min&outputsize=60` +
          `&timezone=UTC&apikey=${twelveDataKey}`;

        const response = await fetch(url);
        if (!response.ok) throw new Error(`Price data request failed (status ${response.status})`);

        const data = await response.json();
        if (data.status === 'error' || data.code) {
          throw new Error(`Price data error: ${data.message || data.code}`);
        }

        const values = data.values || [];
        if (values.length === 0) throw new Error('No price history available for this symbol');

        return values
          .slice()
          .reverse()
          .map((v) => ({
            time: Math.floor(Date.parse(`${v.datetime.replace(' ', 'T')}Z`) / 1000),
            open: parseFloat(v.open),
            high: parseFloat(v.high),
            low: parseFloat(v.low),
            close: parseFloat(v.close),
          }));
      });

      res.json({ bars });
    } catch (err) {
      console.error(`[dataProxy] bars failed for ${symbol}:`, err.message);
      res.status(502).json({ error: err.message });
    }
  });

  // ---------------------------------------------------------------
  // GET /api/expirations?symbol=AAPL
  // Cached 5 min - expiration lists don't change intraday.
  // ---------------------------------------------------------------
  router.get('/expirations', async (req, res) => {
    const symbol = String(req.query.symbol || '').toUpperCase();
    if (!symbol) return res.status(400).json({ error: 'symbol is required' });

    try {
      const dates = await cached(`expirations:${symbol}`, 5 * 60_000, async () => {
        const url = `https://api.tradier.com/v1/markets/options/expirations?symbol=${encodeURIComponent(symbol)}`;
        const res2 = await fetch(url, {
          headers: { Authorization: `Bearer ${tradierToken}`, Accept: 'application/json' },
        });
        const data = await res2.json();
        if (!res2.ok) throw new Error(`Option data request failed (status ${res2.status})`);

        let d = data.expirations?.date || [];
        if (!Array.isArray(d)) d = [d];
        if (d.length === 0) throw new Error(`No option expirations found for ${symbol}`);
        return d;
      });

      res.json({ expirations: dates });
    } catch (err) {
      console.error(`[dataProxy] expirations failed for ${symbol}:`, err.message);
      res.status(502).json({ error: err.message });
    }
  });

  // ---------------------------------------------------------------
  // GET /api/chain?symbol=AAPL&expiration=2026-09-11
  // Full chain, cached 15s. The frontend filters to near-the-money
  // strikes itself so the backend doesn't need to know NEAR_MONEY_STRIKES.
  // ---------------------------------------------------------------
  router.get('/chain', async (req, res) => {
    const symbol = String(req.query.symbol || '').toUpperCase();
    const expiration = String(req.query.expiration || '');
    if (!symbol || !expiration) return res.status(400).json({ error: 'symbol and expiration are required' });

    try {
      const options = await cached(`chain:${symbol}:${expiration}`, 15_000, async () => {
        const url = `https://api.tradier.com/v1/markets/options/chains` +
          `?symbol=${encodeURIComponent(symbol)}&expiration=${expiration}`;
        const res2 = await fetch(url, {
          headers: { Authorization: `Bearer ${tradierToken}`, Accept: 'application/json' },
        });
        const data = await res2.json();
        if (!res2.ok) throw new Error(`Option data request failed (status ${res2.status})`);

        let opts = data.options?.option || [];
        if (!Array.isArray(opts)) opts = [opts];
        return opts;
      });

      res.json({ options, lastPrice: lastPriceOf(symbol) || null });
    } catch (err) {
      console.error(`[dataProxy] chain failed for ${symbol}/${expiration}:`, err.message);
      res.status(502).json({ error: err.message });
    }
  });

  return router;
}

module.exports = { createDataProxyRouter };
