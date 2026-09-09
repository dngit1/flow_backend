# Flow backend

Holds your Alpaca, Twelve Data, and Tradier credentials server-side, and
gives any number of browsers a shared, key-free way to get:
- historical + live prices (Alpaca, one shared connection)
- historical option chains (Tradier REST, cached)
- real-time option flow (Tradier, one shared session)

No API keys are ever sent to the browser.

## Setup

```bash
npm install
cp .env.example .env
# edit .env and fill in your real ALPACA_KEY, ALPACA_SECRET,
# TWELVE_DATA_KEY, and TRADIER_TOKEN
npm start
```

Server listens on `http://localhost:3000` by default (set `PORT` in `.env`
to change it). Put your frontend's `index.html` (the two-panel chart you
already have, adapted to talk to this server - see "Frontend changes"
below) in a `public/` folder next to `server.js` and it'll be served
automatically at `http://localhost:3000/`.

## REST endpoints

- `GET /api/bars?symbol=AAPL` → `{ bars: [{ time, open, high, low, close }, ...] }`
- `GET /api/expirations?symbol=AAPL` → `{ expirations: ["2026-09-11", ...] }`
- `GET /api/chain?symbol=AAPL&expiration=2026-09-11` → `{ options: [...], lastPrice }`

All three are cached briefly server-side, so ten browsers loading AAPL at
once only costs one upstream request, not ten.

## Browser WebSocket protocol

Connect to `ws://localhost:3000/ws`. Messages are JSON.

**Client → server:**
```jsonc
{ "type": "subscribe_price", "symbol": "AAPL" }
{ "type": "unsubscribe_price" }
{ "type": "watch_flow", "symbol": "AAPL", "expiration": "2026-09-11" }
{ "type": "unwatch_flow" }
```

**Server → client:**
```jsonc
{ "type": "price", "symbol": "AAPL", "price": 316.02, "timeMs": 1234567890000 }
{ "type": "flow", "type_": "CALL", "strike": 315, "premium": 47000, "side": "BOUGHT", "timeMs": 1234567890000 }
{ "type": "flow_watching", "symbol": "AAPL", "expiration": "2026-09-11", "contractCount": 18 }
{ "type": "flow_error", "error": "..." }
{ "type": "alpaca_status", "status": "connected" }
```

Note: each browser connection can watch ONE price symbol and ONE flow
symbol/expiration at a time in this version, matching how your two-panel
frontend works (each panel = its own WebSocket connection to this server).

## Frontend changes needed

Your existing `stock_chart.html` currently:
1. Calls Twelve Data directly for bars → change to `fetch('/api/bars?symbol=...')`
2. Opens its own Alpaca WebSocket → change to sending `subscribe_price` over
   one shared `ws://.../ws` connection per panel, and reading `type: "price"`
   messages instead of raw Alpaca trade messages
3. Calls Tradier directly for expirations/chain → change to
   `/api/expirations` and `/api/chain`
4. Opens its own Tradier WebSocket → change to sending `watch_flow` and
   reading `type: "flow"` messages

I can make these edits directly once you confirm the backend runs
correctly on your machine - want me to do that next?

## Deploying

This needs a host that supports long-lived WebSocket connections and a
persistent Node process - a static host like Netlify/GitHub Pages won't
work for this part. Render, Fly.io, Railway, or a small VPS all work.
Set the four env vars in your host's dashboard (never commit `.env`).
