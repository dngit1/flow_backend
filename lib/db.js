// Thin wrapper around a single shared `pg` Pool. Every other module talks
// to the database only through `query()` here, never by importing `pg`
// directly - keeps connection handling (and any future change to it, like
// SSL config) in one place.
const { Pool } = require('pg');

const isProduction = process.env.NODE_ENV === 'production';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  // Render's managed Postgres requires SSL in production; a local Docker
  // Postgres doesn't speak SSL at all, so this can't be a fixed setting -
  // it has to follow which environment we're actually running in.
  ssl: isProduction ? { rejectUnauthorized: false } : false,
});

pool.on('error', (err) => {
  // Fires for errors on IDLE clients in the pool (e.g. the DB restarting
  // under us) - without this handler, that crashes the whole process.
  console.error('[db] unexpected error on idle client:', err.message);
});

function query(text, params) {
  return pool.query(text, params);
}

module.exports = { query, pool };
