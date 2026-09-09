// Minimal in-memory cache with per-entry TTL. Good enough for a
// single-process deployment - if you ever scale to multiple server
// instances, swap this for Redis, but the interface stays the same.

const store = new Map(); // key -> { value, expiresAt }

function get(key) {
  const entry = store.get(key);
  if (!entry) return undefined;
  if (Date.now() > entry.expiresAt) {
    store.delete(key);
    return undefined;
  }
  return entry.value;
}

function set(key, value, ttlMs) {
  store.set(key, { value, expiresAt: Date.now() + ttlMs });
}

// Wraps an async function: if a fresh value exists in cache, return it
// without calling fn(); otherwise call fn(), cache the result, return it.
// Concurrent calls for the same key while a fetch is in-flight share the
// same promise instead of firing duplicate upstream requests.
const inFlight = new Map();

async function cached(key, ttlMs, fn) {
  const existing = get(key);
  if (existing !== undefined) return existing;

  if (inFlight.has(key)) return inFlight.get(key);

  const promise = (async () => {
    try {
      const value = await fn();
      set(key, value, ttlMs);
      return value;
    } finally {
      inFlight.delete(key);
    }
  })();

  inFlight.set(key, promise);
  return promise;
}

module.exports = { get, set, cached };
