// A simple rolling buffer of recent flow events, keyed by whatever the
// caller wants (symbol, symbol:expiration, symbol:bigflow, etc). Lets a
// brand-new or refreshed connection immediately see recent activity
// instead of starting blank and waiting for the next live event.
//
// Purely in-memory - no database, no extra API calls to any provider.
// Entries age out automatically (both by time and by count) so this
// can't grow unbounded on a very active ticker.
function createFlowBuffer({ maxAgeMs = 60 * 60_000, maxEntries = 100 } = {}) {
  const buffers = new Map(); // key -> array of { event, recordedAt }

  function trim(arr) {
    const cutoff = Date.now() - maxAgeMs;
    while (arr.length && arr[0].recordedAt < cutoff) arr.shift();
    if (arr.length > maxEntries) arr.splice(0, arr.length - maxEntries);
  }

  function record(key, event) {
    let arr = buffers.get(key);
    if (!arr) {
      arr = [];
      buffers.set(key, arr);
    }
    arr.push({ event, recordedAt: Date.now() });
    trim(arr);
  }

  function getRecent(key) {
    const arr = buffers.get(key);
    if (!arr) return [];
    trim(arr);
    return arr.map((e) => e.event);
  }

  function clear(key) {
    buffers.delete(key);
  }

  return { record, getRecent, clear };
}

module.exports = { createFlowBuffer };
