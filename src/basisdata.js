// Pool koneksi PostgreSQL untuk proyek A04. Satu Pool bersama per proses,
// DATABASE_URL dibaca dari .env milik proyek ini sendiri.
'use strict';
const { Pool } = require('pg');

let pool;
function getPool() {
  if (!pool) pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 5 });
  return pool;
}
async function end() {
  if (pool) { const p = pool; pool = undefined; await p.end(); }
}

module.exports = { getPool, end };
