#!/usr/bin/env node
// Producer CLI kasus A04 - Pengolahan Berkas (pola W / Work Queue).
//
// REUSE: openPublisher() di src/broker.js adalah adaptasi dari
// layanan/messaging.js pada repo simpel-lab (confirm channel, publish
// mandatory+persistent, deteksi 'return', penutupan bersih). Topologi
// (topologi.js) dan kontrak (kontrak.js) BARU dan spesifik kasus berkas.
//
// Perintah:
//   siapkan                                   deklarasikan topologi lalu keluar
//   kirim   --run <run> --ids <A01-A20>        kirim N event valid (contoh: N01-N20)
//   ulang   --run <run> --ids <A01-A05>        kirim ULANG event_id+payload PERSIS semula
//   invalid --run <run> --id <X01>             kirim satu event dengan file_id tidak terdaftar
'use strict';
const { writeFileSync, readFileSync, existsSync, mkdirSync } = require('node:fs');
const path = require('node:path');
const { openPublisher } = require('./broker');
const { declareTopology, ROUTING_KEY } = require('./topologi');
const { EVENT_TYPE } = require('./kontrak');
const { FIXTURE_FILES } = require('./berkas');

const FIXTURE_IDS = Object.keys(FIXTURE_FILES);

const BUKTI_DIR = path.join(__dirname, '..', 'bukti');
if (!existsSync(BUKTI_DIR)) mkdirSync(BUKTI_DIR, { recursive: true });

function catatanPath(run) { return path.join(BUKTI_DIR, `${run}-terkirim.json`); }
function muatCatatan(run) {
  const file = catatanPath(run);
  return existsSync(file) ? JSON.parse(readFileSync(file, 'utf8')) : {};
}
function simpanCatatan(run, catatan) {
  writeFileSync(catatanPath(run), JSON.stringify(catatan, null, 2));
}

// "N01-N20" atau "N01-20" -> ['N01', 'N02', ..., 'N20']; lebar padding mengikuti sisi kiri.
function rentang(spec) {
  const m = /^([A-Za-z]+)(\d+)-([A-Za-z]+)?(\d+)$/.exec(spec || '');
  if (!m) throw new Error('Format rentang harus <prefix><angkaAwal>-[<prefix>]<angkaAkhir>, misal N01-N20');
  const [, prefix, awalStr, prefix2, akhirStr] = m;
  if (prefix2 && prefix2 !== prefix) throw new Error('Prefix awal dan akhir rentang harus sama');
  const lebar = awalStr.length;
  const awal = Number(awalStr), akhir = Number(akhirStr);
  if (akhir < awal) throw new Error('Angka akhir rentang harus >= angka awal');
  const hasil = [];
  for (let i = awal; i <= akhir; i++) hasil.push(prefix + String(i).padStart(lebar, '0'));
  return hasil;
}

// job_id SENGAJA dibuat berbeda dari event_id (job_id = label bisnis job,
// event_id = identitas kejadian pengiriman + kunci dedup -- lihat README §2
// dan §9). file_id dirotasi lewat fixture terdaftar supaya satu berkas yang
// sama terbukti bisa dipakai oleh beberapa job berbeda.
function buatEventValid(run, id, waktu) {
  const angka = Number(id.replace(/^[A-Za-z]+/, '')) || 1;
  const fileId = FIXTURE_IDS[(angka - 1) % FIXTURE_IDS.length];
  return {
    event_id: `${run}-${id}`,
    event_type: EVENT_TYPE,
    occurred_at: waktu || new Date().toISOString(),
    payload: {
      job_id: `JOB-${id}`,
      file_id: fileId,
      operation: 'word_count',
    },
  };
}

// Sengaja tidak valid: file_id TIDAK terdaftar di fixture lokal, melanggar
// kontrak (consumer hanya menerima file_id terdaftar, lihat src/berkas.js).
// event_type & envelope tetap benar supaya pesan ini terbukti ter-route ke
// queue kerja yang sama (bukan gagal routing).
function buatEventTidakValid(run, id, waktu) {
  return {
    event_id: `${run}-${id}`,
    event_type: EVENT_TYPE,
    occurred_at: waktu || new Date().toISOString(),
    payload: {
      job_id: `JOB-${id}`,
      file_id: 'sample-tidak-terdaftar',
      operation: 'word_count',
    },
  };
}

async function kirimSemua(events) {
  const p = await openPublisher(declareTopology);
  try {
    for (const event of events) {
      await p.publish(event, ROUTING_KEY);
      console.log(JSON.stringify({ terkirim: event.event_id, job_id: event.payload.job_id, occurred_at: event.occurred_at }));
    }
  } finally { await p.close(); }
}

function ambilOpsi(argv) {
  const opsi = {};
  for (let i = 0; i < argv.length; i += 2) opsi[String(argv[i]).replace(/^--/, '')] = argv[i + 1];
  return opsi;
}

async function main() {
  const [perintah, ...sisa] = process.argv.slice(2);
  const opsi = ambilOpsi(sisa);

  if (perintah === 'siapkan') {
    const p = await openPublisher(declareTopology);
    await p.close();
    console.log('Topologi A04 siap: exchange, queue, binding, dan DLX sudah dideklarasikan (idempoten).');
    return;
  }

  if (perintah === 'kirim') {
    const run = opsi.run; if (!run) throw new Error('--run wajib diisi, misal run01');
    const ids = rentang(opsi.ids || opsi.rentang);
    const events = ids.map(id => buatEventValid(run, id, opsi.waktu));
    await kirimSemua(events);
    const catatan = muatCatatan(run);
    for (const e of events) catatan[e.event_id] = e;
    simpanCatatan(run, catatan);
    console.log(JSON.stringify({ run, jumlahTerkirim: events.length, ids }));
    return;
  }

  if (perintah === 'ulang') {
    const run = opsi.run; if (!run) throw new Error('--run wajib diisi');
    const ids = rentang(opsi.ids || opsi.rentang);
    const catatan = muatCatatan(run);
    const events = ids.map(id => {
      const key = `${run}-${id}`;
      const asli = catatan[key];
      if (!asli) throw new Error(`Event ${key} belum pernah dikirim pada run ini; replay membutuhkan event_id dan payload semula`);
      return asli; // event_id dan payload BENAR-BENAR identik dengan pengiriman semula
    });
    await kirimSemua(events);
    console.log(JSON.stringify({ run, diulang: events.map(e => e.event_id) }));
    return;
  }

  if (perintah === 'invalid') {
    const run = opsi.run; const id = opsi.id || 'X01';
    if (!run) throw new Error('--run wajib diisi');
    const event = buatEventTidakValid(run, id, opsi.waktu);
    await kirimSemua([event]);
    const catatan = muatCatatan(run); catatan[event.event_id] = event; simpanCatatan(run, catatan);
    console.log(JSON.stringify({ run, dikirimTidakValid: event.event_id }));
    return;
  }

  throw new Error('Gunakan: siapkan | kirim --run <run> --ids <A01-A20> | ulang --run <run> --ids <A01-A05> | invalid --run <run> --id <X01>');
}

if (require.main === module) main().catch(e => { console.error(e.message); process.exitCode = 1; });
module.exports = { rentang, buatEventValid, buatEventTidakValid, catatanPath, muatCatatan };
