// Terapkan skema (idempoten) ke database yang ditunjuk DATABASE_URL.
// Pola sama dengan tools/siapkan-db.js di simpel-lab, disederhanakan untuk
// satu file skema karena proyek ini berdiri sendiri.
'use strict';
const { readFileSync } = require('node:fs');
const { join } = require('node:path');
const { Pool } = require('pg');

async function main() {
  const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 1 });
  try {
    await pool.query(readFileSync(join(__dirname, '../db/skema.sql'), 'utf8'));
    console.log('PASS: tabel file_results dan file_rejections tersedia; baris yang sudah ada tidak diubah.');
  } finally { await pool.end(); }
}

main().catch(e => { console.error('Gagal menyiapkan database. Periksa DATABASE_URL dan container postgres.', e.message); process.exitCode = 1; });
