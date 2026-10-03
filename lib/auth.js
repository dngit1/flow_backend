const crypto = require('crypto');
const bcrypt = require('bcryptjs');
const { OAuth2Client } = require('google-auth-library');
const db = require('./db');

const SESSION_COOKIE_NAME = 'xnlflow_session';
const SESSION_COOKIE_MAX_AGE_MS = 30 * 24 * 60 * 60 * 1000; // 30 days
const MAGIC_LINK_TTL_MS = 15 * 60 * 1000; // 15 minutes
const PASSWORD_RESET_TTL_MS = 15 * 60 * 1000; // 15 minutes - same window as magic links
const PASSWORD_MIN_LENGTH = 8;
const BCRYPT_ROUNDS = 10;
// How often a session's last_seen_at gets updated. Writing on literally
// every request would hammer the database for no real benefit - this
// throttle still gives "which device has been idle longest" a meaningful
// signal for eviction, just without a write on every single request.
const LAST_SEEN_THROTTLE_MS = 5 * 60 * 1000; // 5 minutes

const googleClient = new OAuth2Client(process.env.GOOGLE_CLIENT_ID);

function isProduction() {
  return process.env.NODE_ENV === 'production';
}

// --- Cookie helpers -------------------------------------------------

// A session's id IS its secret - 32 random bytes (256 bits), the same
// approach Rails/Django use for session tokens. No separate HMAC signing
// needed on top: the token is only ever valid if it exists as a row in
// the sessions table (see getSessionFromRequest), so a guessed/forged
// value simply won't match anything.
function generateToken() {
  return crypto.randomBytes(32).toString('hex');
}

function setSessionCookie(res, sessionId) {
  res.cookie(SESSION_COOKIE_NAME, sessionId, {
    httpOnly: true,
    secure: isProduction(), // browsers reject a Secure cookie over plain http://localhost, so this must follow the environment
    sameSite: 'lax',
    maxAge: SESSION_COOKIE_MAX_AGE_MS,
    path: '/',
  });
}

function clearSessionCookie(res) {
  res.clearCookie(SESSION_COOKIE_NAME, { path: '/' });
}

function getSessionIdFromCookieHeader(cookieHeader) {
  if (!cookieHeader) return null;
  for (const part of cookieHeader.split(';')) {
    const [key, ...rest] = part.trim().split('=');
    if (key === SESSION_COOKIE_NAME) return decodeURIComponent(rest.join('='));
  }
  return null;
}

// --- Users ------------------------------------------------------------

async function findOrCreateUserByEmail(email, googleId) {
  const normalizedEmail = email.trim().toLowerCase();

  const existing = await db.query('SELECT * FROM users WHERE email = $1', [normalizedEmail]);
  if (existing.rows.length) {
    const user = existing.rows[0];
    // Backfill google_id if they originally signed up via magic link and
    // are now using Google for the first time - same account either way.
    if (googleId && !user.google_id) {
      await db.query('UPDATE users SET google_id = $1 WHERE id = $2', [googleId, user.id]);
      user.google_id = googleId;
    }
    return user;
  }

  const inserted = await db.query(
    'INSERT INTO users (email, google_id) VALUES ($1, $2) RETURNING *',
    [normalizedEmail, googleId || null]
  );
  return inserted.rows[0];
}

// --- Password sign-in ------------------------------------------------
//
// Fully optional, alongside Google and magic-link, which are both
// unchanged. password_hash is NULL for any account that hasn't set one.

async function hashPassword(password) {
  return bcrypt.hash(password, BCRYPT_ROUNDS);
}

// Creates a brand-new account with a password, OR - if an account with
// this email already exists from Google/magic-link sign-in with no
// password set yet - adds a password to that SAME account rather than
// erroring or creating a duplicate. Matches how Google sign-in already
// backfills google_id onto an existing magic-link account (see
// findOrCreateUserByEmail above): one person, one row, regardless of
// which method they used first.
async function signUpWithPassword(email, password) {
  const normalizedEmail = email.trim().toLowerCase();
  if (password.length < PASSWORD_MIN_LENGTH) {
    throw new Error(`Password must be at least ${PASSWORD_MIN_LENGTH} characters`);
  }

  const existing = await db.query('SELECT * FROM users WHERE email = $1', [normalizedEmail]);
  if (existing.rows.length) {
    const user = existing.rows[0];
    if (user.password_hash) {
      throw new Error('An account with this email already has a password set - log in instead');
    }
    const passwordHash = await hashPassword(password);
    await db.query('UPDATE users SET password_hash = $1 WHERE id = $2', [passwordHash, user.id]);
    user.password_hash = passwordHash;
    return user;
  }

  const passwordHash = await hashPassword(password);
  try {
    const inserted = await db.query(
      'INSERT INTO users (email, password_hash) VALUES ($1, $2) RETURNING *',
      [normalizedEmail, passwordHash]
    );
    return inserted.rows[0];
  } catch (err) {
    // Two signups for the same brand-new email arriving at once - the
    // database's own UNIQUE constraint on email is what actually
    // prevents the duplicate; this just turns that race into the same
    // clean error a sequential attempt would have gotten above.
    if (err.code === '23505') throw new Error('An account with this email already exists - log in instead');
    throw err;
  }
}

// Verifies email+password and returns the user row on success. Throws a
// distinct, user-facing message for each failure reason - no account,
// account exists but has no password set (Google/magic-link only), or
// the password itself is wrong - rather than one generic "invalid
// credentials" for all three, since the right next step differs (sign up
// vs. use Google/magic-link vs. just retype the password).
async function verifyPasswordLogin(email, password) {
  const normalizedEmail = email.trim().toLowerCase();
  const result = await db.query('SELECT * FROM users WHERE email = $1', [normalizedEmail]);
  const user = result.rows[0];
  if (!user) throw new Error('No account found with this email');
  if (!user.password_hash) throw new Error("This account doesn't have a password set - use Google or email sign-in link instead");

  const matches = await bcrypt.compare(password, user.password_hash);
  if (!matches) throw new Error('Incorrect password');
  return user;
}

async function createPasswordResetToken(email) {
  const token = generateToken();
  const expiresAt = new Date(Date.now() + PASSWORD_RESET_TTL_MS);
  await db.query(
    'INSERT INTO password_reset_tokens (token, email, expires_at) VALUES ($1, $2, $3)',
    [token, email.trim().toLowerCase(), expiresAt]
  );
  return token;
}

// One-time use, same pattern as consumeMagicLinkToken: deletes the token
// as part of verifying it, so a second attempt (or a replay) always
// fails even before it would have expired naturally. Returns the updated
// user row, or null for an invalid/expired/already-used token.
async function resetPasswordWithToken(token, newPassword) {
  if (newPassword.length < PASSWORD_MIN_LENGTH) {
    throw new Error(`Password must be at least ${PASSWORD_MIN_LENGTH} characters`);
  }

  const result = await db.query(
    'DELETE FROM password_reset_tokens WHERE token = $1 AND expires_at > now() RETURNING email',
    [token]
  );
  const email = result.rows[0]?.email;
  if (!email) return null;

  const passwordHash = await hashPassword(newPassword);
  const updated = await db.query('UPDATE users SET password_hash = $1 WHERE email = $2 RETURNING *', [passwordHash, email]);
  return updated.rows[0] || null;
}

// --- Sessions (with per-user device-count eviction) --------------------

// Creates a new session for this user, evicting their OLDEST (by
// last_seen_at) session first if they're already at their device limit.
// Returns { sessionId, evictedSessionIds } - the caller (server.js) is
// responsible for force-closing any live WebSocket tied to an evicted
// session id, since this module has no visibility into live connections.
async function createSession(userId, userAgent) {
  const userResult = await db.query('SELECT max_sessions FROM users WHERE id = $1', [userId]);
  const maxSessions = userResult.rows[0]?.max_sessions ?? 1;

  const existing = await db.query(
    'SELECT id FROM sessions WHERE user_id = $1 ORDER BY last_seen_at ASC',
    [userId]
  );

  const evictedSessionIds = [];
  const overflow = existing.rows.length - (maxSessions - 1); // -1 to make room for the new one
  if (overflow > 0) {
    const toEvict = existing.rows.slice(0, overflow).map((r) => r.id);
    await db.query('DELETE FROM sessions WHERE id = ANY($1)', [toEvict]);
    evictedSessionIds.push(...toEvict);
  }

  const sessionId = generateToken();
  await db.query(
    'INSERT INTO sessions (id, user_id, user_agent) VALUES ($1, $2, $3)',
    [sessionId, userId, userAgent || null]
  );

  return { sessionId, evictedSessionIds };
}

async function touchSession(sessionId) {
  // Throttled - see LAST_SEEN_THROTTLE_MS above.
  await db.query(
    `UPDATE sessions SET last_seen_at = now()
     WHERE id = $1 AND last_seen_at < now() - $2 * interval '1 millisecond'`,
    [sessionId, LAST_SEEN_THROTTLE_MS]
  );
}

async function deleteSession(sessionId) {
  await db.query('DELETE FROM sessions WHERE id = $1', [sessionId]);
}

// Looks up the session + user for a request's cookie. Returns null if
// there's no cookie, or it doesn't match a live session (expired,
// evicted, or logged out elsewhere) - callers treat null as "not logged
// in", they don't need to distinguish why.
async function getSessionFromCookieValue(sessionId) {
  if (!sessionId) return null;

  const result = await db.query(
    `SELECT s.id AS session_id, u.*
     FROM sessions s
     JOIN users u ON u.id = s.user_id
     WHERE s.id = $1`,
    [sessionId]
  );
  if (!result.rows.length) return null;

  touchSession(sessionId).catch((err) => console.error('[auth] touchSession failed:', err.message));

  const row = result.rows[0];
  const { session_id, ...user } = row;
  return { sessionId: session_id, user };
}

// --- Express middleware -------------------------------------------------

async function requireAuth(req, res, next) {
  try {
    const sessionId = req.cookies?.[SESSION_COOKIE_NAME];
    const session = await getSessionFromCookieValue(sessionId);
    if (!session) return res.status(401).json({ error: 'Not signed in' });
    req.user = session.user;
    req.sessionId = session.sessionId;
    next();
  } catch (err) {
    console.error('[auth] requireAuth failed:', err.message);
    res.status(500).json({ error: 'Auth check failed' });
  }
}

// --- Google ID token verification ---------------------------------------

async function verifyGoogleIdToken(idToken) {
  const ticket = await googleClient.verifyIdToken({
    idToken,
    audience: process.env.GOOGLE_CLIENT_ID,
  });
  const payload = ticket.getPayload();
  if (!payload?.email || !payload.email_verified) {
    throw new Error('Google account has no verified email');
  }
  return { email: payload.email, googleId: payload.sub };
}

// --- Magic link tokens ----------------------------------------------

async function createMagicLinkToken(email) {
  const token = generateToken();
  const expiresAt = new Date(Date.now() + MAGIC_LINK_TTL_MS);
  await db.query(
    'INSERT INTO magic_link_tokens (token, email, expires_at) VALUES ($1, $2, $3)',
    [token, email.trim().toLowerCase(), expiresAt]
  );
  return token;
}

// One-time use: deletes the token as part of verifying it, so a second
// attempt with the same link (or a replay) always fails, even before it
// would have expired naturally.
async function consumeMagicLinkToken(token) {
  const result = await db.query(
    'DELETE FROM magic_link_tokens WHERE token = $1 AND expires_at > now() RETURNING email',
    [token]
  );
  return result.rows[0]?.email || null;
}

// --- Admin -------------------------------------------------------------

// Comma-separated allowlist, e.g. "you@example.com,cofounder@example.com".
// Deny-by-default: an unset or empty env var means nobody is an admin,
// rather than accidentally opening the session list to every signed-in
// user if this is ever forgotten during setup.
function adminEmails() {
  return new Set(
    (process.env.ADMIN_EMAILS || '')
      .split(',')
      .map((e) => e.trim().toLowerCase())
      .filter(Boolean)
  );
}

// Must run AFTER requireAuth (reads req.user, which that sets).
function requireAdmin(req, res, next) {
  if (!req.user || !adminEmails().has(req.user.email.toLowerCase())) {
    return res.status(403).json({ error: 'Not authorized' });
  }
  next();
}

// Every currently-active session (a row here = a valid, non-evicted,
// non-logged-out login), newest activity first - this is exactly the
// query from the earlier manual psql check, now reusable from the route.
async function getAllSessions() {
  const result = await db.query(
    `SELECT u.email, s.created_at, s.last_seen_at, s.user_agent
     FROM sessions s
     JOIN users u ON u.id = s.user_id
     ORDER BY s.last_seen_at DESC`
  );
  return result.rows;
}

module.exports = {
  SESSION_COOKIE_NAME,
  setSessionCookie,
  clearSessionCookie,
  getSessionIdFromCookieHeader,
  findOrCreateUserByEmail,
  createSession,
  deleteSession,
  getSessionFromCookieValue,
  requireAuth,
  requireAdmin,
  getAllSessions,
  verifyGoogleIdToken,
  createMagicLinkToken,
  consumeMagicLinkToken,
  signUpWithPassword,
  verifyPasswordLogin,
  createPasswordResetToken,
  resetPasswordWithToken,
};
