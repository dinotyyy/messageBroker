#!/usr/bin/env node
// Verifikasi & bukti untuk skenario uji U1-U4 kasus A04 (PostgreSQL +
// RabbitMQ Management API). Pengiriman event dan penghentian/penyalaan
// worker dilakukan MANUAL lewat src/produsen.js dan terminal worker terpisah
// -- lihat README §6; script ini hanya membaca hasil dan menulis bukti.
//
// Kebijakan bukti (lihat README "Bukti yang dicatat"): setiap `verifikasi`
// dengan ekspektasi jumlah akan MENUNGGU (polling tiap 2 detik) sampai
// BATAS_TUNGGU_MS (60 detik) sebelum menyerah -- queue kosong saja tidak
// dianggap bukti keberhasilan, hanya jumlah baris ledger yang cocok dengan
// himpunan ID input yang dianggap bukti. Bila target tidak tercapai dalam
// batas waktu, itu dicatat apa adanya (`tercapaiDalamWaktu: false`), bukan
// disembunyikan.
'use strict';
const path = require('node:path');
const { setTimeout: delay } = require('node:timers/promises');
const { writeFileSync, mkdirSync, existsSync } = require('node:fs');
const { getPool, end } = require('../src/basisdata');
const { queueSnapshot } = require('../src/manajemen');
const { QUEUE_KERJA, QUEUE_PENOLAKAN, QUEUE_TANPA_RUTE } = require('../src/topologi');

const BATAS_TUNGGU_MS = 60_000;
const INTERVAL_POLL_MS = 2_000;

const BUKTI_DIR = path.join(__dirname, '..', 'bukti');
if (!existsSync(BUKTI_DIR)) mkdirSync(BUKTI_DIR, { recursive: true });

function simpanBukti(nama, data) {
  const file = path.join(BUKTI_DIR, `${nama}.json`);
  writeFileSync(file, JSON.stringify(data, null, 2));
  console.log(`Bukti disimpan: ${file}`);
  return file;
}

async function idHasil(pool, run) {
  const { rows } = await pool.query(
    'SELECT event_id FROM file_results WHERE event_id LIKE $1 ORDER BY event_id',
    [`${run}-%`],
  );
  return rows.map(r => r.event_id);
}

// Polling dengan batas waktu eksplisit -- lihat kebijakan bukti di atas.
async function tungguHasil(pool, run, ekspektasi) {
  const mulai = Date.now();
  let hasil = await idHasil(pool, run);
  const target = ekspektasi != null && ekspektasi !== '' ? Number(ekspektasi) : null;
  while (target != null && hasil.length < target && Date.now() - mulai < BATAS_TUNGGU_MS) {
    await delay(INTERVAL_POLL_MS);
    hasil = await idHasil(pool, run);
  }
  const waktuTungguMs = Date.now() - mulai;
  const tercapaiDalamWaktu = target == null || hasil.length >= target;
  if (target != null && !tercapaiDalamWaktu) {
    console.warn(`PERINGATAN: target ${target} hasil TIDAK tercapai dalam batas waktu ${BATAS_TUNGGU_MS / 1000} detik (baru ${hasil.length}).`);
  }
  return { hasil, waktuTungguMs, tercapaiDalamWaktu, batasTungguMs: BATAS_TUNGGU_MS };
}

// Ledger/receipt lengkap per event -- inilah yang membuat hitungan (20 -> 25
// -> 25 -> 26) bisa dibuktikan, bukan sekadar diklaim. Diekspor terpisah dari
// laporan verifikasi supaya bisa dilampirkan apa adanya (hasil query mentah).
async function eksporLedger(pool, run) {
  const hasil = (await pool.query(
    `SELECT event_id, job_id, file_id, operation, ukuran_bytes, jumlah_kata,
            durasi_proses_ms, status, worker_id, occurred_at, diproses_pada
       FROM file_results WHERE event_id LIKE $1 ORDER BY event_id`,
    [`${run}-%`],
  )).rows;
  const ditolak = (await pool.query(
    'SELECT event_id, alasan, payload_mentah, ditolak_pada FROM file_rejections WHERE event_id LIKE $1 ORDER BY ditolak_pada',
    [`${run}-%`],
  )).rows;
  return simpanBukti(`${run}-ledger`, {
    run, dieksporPada: new Date().toISOString(),
    sumber: 'SELECT langsung ke tabel file_results dan file_rejections (ekspor data mentah, bukan hasil olahan)',
    jumlahHasilBisnis: hasil.length, hasilBisnis: hasil,
    jumlahDitolak: ditolak.length, ditolak,
  });
}

async function verifikasi(run, label, ekspektasi) {
  const pool = getPool();
  try {
    const { hasil, waktuTungguMs, tercapaiDalamWaktu, batasTungguMs } = await tungguHasil(pool, run, ekspektasi);
    const penolakan = (await pool.query(
      'SELECT event_id, alasan, ditolak_pada FROM file_rejections WHERE event_id LIKE $1 ORDER BY ditolak_pada',
      [`${run}-%`],
    )).rows;
    const [antreanKerja, antreanPenolakan, antreanTanpaRute] = await Promise.all([
      queueSnapshot(QUEUE_KERJA).catch(e => ({ error: e.message })),
      queueSnapshot(QUEUE_PENOLAKAN).catch(e => ({ error: e.message })),
      queueSnapshot(QUEUE_TANPA_RUTE).catch(e => ({ error: e.message })),
    ]);
    const laporan = {
      run, label, diverifikasiPada: new Date().toISOString(),
      perintahUji: `node uji/skenario.js verifikasi ${run} ${label}${ekspektasi ? ' ' + ekspektasi : ''}`,
      jumlahHasilBisnis: hasil.length, idHasilBisnis: hasil,
      jumlahDitolak: penolakan.length, ditolak: penolakan,
      antrean: { kerja: antreanKerja, penolakan: antreanPenolakan, tanpaRute: antreanTanpaRute },
    };
    if (ekspektasi != null && ekspektasi !== '') {
      laporan.ekspektasi = Number(ekspektasi);
      laporan.sesuaiEkspektasi = hasil.length === Number(ekspektasi);
      laporan.batasTungguMs = batasTungguMs;
      laporan.waktuTungguMs = waktuTungguMs;
      laporan.tercapaiDalamWaktu = tercapaiDalamWaktu;
    }
    simpanBukti(`${run}-${label}`, laporan);
    console.log(JSON.stringify(laporan, null, 2));
  } finally { await end(); }
}

async function antrean() {
  for (const q of [QUEUE_KERJA, QUEUE_PENOLAKAN, QUEUE_TANPA_RUTE]) {
    console.log(JSON.stringify(await queueSnapshot(q)));
  }
}

async function ledger(run) {
  const pool = getPool();
  try { await eksporLedger(pool, run); } finally { await end(); }
}

async function main() {
  const [perintah, run, a, b] = process.argv.slice(2);
  if (perintah === 'verifikasi') return verifikasi(run, a, b);
  if (perintah === 'ledger') return ledger(run);
  if (perintah === 'antrean') return antrean();
  throw new Error('Gunakan: verifikasi <run> <label> [ekspektasiJumlah] | ledger <run> | antrean');
}

if (require.main === module) main().catch(e => { console.error(e.message); process.exitCode = 1; });
