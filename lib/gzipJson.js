'use strict';

// Compresses large JSON replies (gzip) for browsers that accept it.
//
// Why: the page downloads option chains (hundreds of contracts each) several
// times a minute. As plain text that was the bulk of the server's outbound
// bandwidth, which Render bills past the plan's monthly allowance. JSON
// compresses roughly 7-8x because every record repeats the same field names.
//
// Only res.json() replies are touched: smaller than MIN_BYTES are sent as
// normal (compressing them saves nothing worth the CPU), and so is anything
// sent to a client that did not say it accepts gzip. Uses Node's built-in zlib.

const zlib = require('zlib');

const MIN_BYTES = 1024;

function addVary(res, value) {
  const existing = res.getHeader('Vary');
  if (!existing) { res.setHeader('Vary', value); return; }
  const parts = String(existing).split(',').map((s) => s.trim().toLowerCase());
  if (!parts.includes(value.toLowerCase()) && !parts.includes('*')) res.setHeader('Vary', `${existing}, ${value}`);
}

function gzipJson(req, res, next) {
  const originalJson = res.json.bind(res);
  res.json = function (body) {
    const acceptsGzip = /\bgzip\b/i.test(req.headers['accept-encoding'] || '');
    if (!acceptsGzip || res.headersSent) return originalJson(body);
    let text;
    try { text = JSON.stringify(body); } catch (e) { return originalJson(body); }
    if (text === undefined || Buffer.byteLength(text) < MIN_BYTES) return originalJson(body);

    zlib.gzip(text, (err, compressed) => {
      if (res.headersSent) return;
      if (err) { originalJson(body); return; }
      res.setHeader('Content-Type', 'application/json; charset=utf-8');
      res.setHeader('Content-Encoding', 'gzip');
      addVary(res, 'Accept-Encoding');
      res.setHeader('Content-Length', compressed.length);
      res.end(compressed);
    });
    return res;
  };
  next();
}

module.exports = { gzipJson, MIN_BYTES };
