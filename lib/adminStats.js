// Numbers for the admin page (public/admin.html).

// ws's readyState for a fully open connection (WebSocket.OPEN). Written as
// a literal so this module has no dependency on the ws package.
const SOCKET_OPEN = 1;

// How many DIFFERENT PEOPLE have a live, signed-in connection right now.
//
// Counted from the server's own record of open WebSockets rather than from
// sessions.last_seen_at: last_seen_at is only refreshed when a page request
// or a connection handshake passes the sign-in check (and is throttled), so
// someone who has charts open and is receiving live data over an existing
// connection can look "inactive" in it for hours. An open connection is the
// real signal, and the server's 30s heartbeat terminates dead ones.
//
// Counted by user, not by connection: each browser tab holds a connection
// per chart panel, and one person can have several tabs or sessions, but
// they are still one person.
function countOnlineUsers(sessionConnections) {
  const userIds = new Set();
  for (const sockets of sessionConnections.values()) {
    for (const ws of sockets) {
      if (ws.readyState === SOCKET_OPEN && ws.userId != null) userIds.add(ws.userId);
    }
  }
  return userIds.size;
}

// Everything /admin/sessions returns, in one snapshot so the numbers on the
// page are consistent with each other. Rejects if either database lookup
// fails (the route turns that into a clean error rather than showing
// partial or misleading figures).
async function buildAdminOverview({ auth, sessionConnections }) {
  const [sessions, totalUsers] = await Promise.all([auth.getAllSessions(), auth.getUserCount()]);
  return {
    sessions,
    stats: {
      totalUsers,
      onlineUsers: countOnlineUsers(sessionConnections),
    },
  };
}

module.exports = { countOnlineUsers, buildAdminOverview };
