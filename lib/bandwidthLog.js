'use strict';

// Counts the bytes this server sends out, per route, and logs a one-line
// summary once an hour:
//   [bandwidth] last 60 min: 14.2 MB total | /api/chain 9.1 MB (412) | websocket 3.0 MB (51,200 msgs) | ...
//
// Why: Render bills outbound bandwidth past the plan's monthly allowance, and
// its graph only shows a total. This shows WHICH route the bytes go to, and
// whether a change (like trimming / compressing the option chains) worked.
//
// HTTP bytes are counted as they leave (so after compression, if any);
// WebSocket bytes by wrapping ws's send(). Message headers are not counted
// (a few hundred bytes each) - this is for finding the big spender, not billing.

let httpStats = new Map(); // route -> { bytes, responses }
let wsBytes = 0;
let wsMessages = 0;
let since = Date.now();

function routeKey(req) {
  const path = String(req.originalUrl || req.url || '').split('?')[0];
  const m = path.match(/^\/(api|auth|admin)\/([^/]+)/);
  if (m) return `/${m[1]}/${m[2]}`;
  if (path === '/' || path.endsWith('.html')) return 'page';
  return 'other';
}

// Distinct route names are capped so a flood of made-up URLs (/api/aaa1, /api/aaa2 ...) cannot grow this table
// without limit between reports; anything past the cap is counted under "other". Real routes are well under it.
const MAX_ROUTES = 40;

function record(route, bytes) {
  if (!httpStats.has(route) && httpStats.size >= MAX_ROUTES) route = 'other';
  const s = httpStats.get(route) || { bytes: 0, responses: 0 };
  s.bytes += bytes;
  s.responses += 1;
  httpStats.set(route, s);
}

function httpMiddleware(req, res, next) {
  let bytes = 0;
  const origWrite = res.write;
  const origEnd = res.end;
  const count = (chunk, enc) => {
    if (chunk == null || typeof chunk === 'function') return;
    bytes += Buffer.isBuffer(chunk) ? chunk.length : Buffer.byteLength(String(chunk), typeof enc === 'string' ? enc : 'utf8');
  };
  res.write = function (chunk, enc) { count(chunk, enc); return origWrite.apply(this, arguments); };
  res.end = function (chunk, enc) { count(chunk, enc); return origEnd.apply(this, arguments); };
  res.on('finish', () => record(routeKey(req), bytes));
  next();
}

function installWsCounter(WebSocketClass) {
  if (!WebSocketClass || !WebSocketClass.prototype || typeof WebSocketClass.prototype.send !== 'function') return false;
  if (WebSocketClass.prototype.send.__counted) return true;
  const orig = WebSocketClass.prototype.send;
  const wrapped = function (data, ...rest) {
    wsBytes += typeof data === 'string' ? Buffer.byteLength(data) : ((data && data.length) || 0);
    wsMessages += 1;
    return orig.call(this, data, ...rest);
  };
  wrapped.__counted = true;
  WebSocketClass.prototype.send = wrapped;
  return true;
}

function fmtBytes(n) {
  if (n >= 1024 * 1024 * 1024) return `${(n / 1024 / 1024 / 1024).toFixed(2)} GB`;
  if (n >= 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)} MB`;
  return `${(n / 1024).toFixed(0)} KB`;
}

// Returns the summary line and starts a fresh count, or null if nothing was sent.
function report(now = Date.now()) {
  const entries = [...httpStats.entries()].map(([route, s]) => ({ label: route, bytes: s.bytes, detail: `${s.responses.toLocaleString('en-US')}` }));
  if (wsMessages > 0) entries.push({ label: 'websocket', bytes: wsBytes, detail: `${wsMessages.toLocaleString('en-US')} msgs` });
  const total = entries.reduce((sum, e) => sum + e.bytes, 0);
  const minutes = Math.max(1, Math.round((now - since) / 60000));
  httpStats = new Map(); wsBytes = 0; wsMessages = 0; since = now;
  if (total === 0) return null;
  entries.sort((a, b) => b.bytes - a.bytes);
  const top = entries.slice(0, 6).map((e) => `${e.label} ${fmtBytes(e.bytes)} (${e.detail})`).join(' | ');
  const rest = entries.slice(6);
  const more = rest.length ? ` | ${rest.length} more (${fmtBytes(rest.reduce((sum, e) => sum + e.bytes, 0))})` : '';
  return `[bandwidth] last ${minutes} min: ${fmtBytes(total)} total | ${top}${more}`;
}

function startHourlyReport(log = console.log, everyMs = 60 * 60_000) {
  const timer = setInterval(() => { const line = report(); if (line) log(line); }, everyMs);
  if (timer.unref) timer.unref();
  return timer;
}

module.exports = { httpMiddleware, installWsCounter, report, startHourlyReport, routeKey, fmtBytes };
