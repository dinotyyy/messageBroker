// Registri berkas teks sintetis (fixture lokal) kasus A04 - Pengolahan Berkas.
//
// KEAMANAN: consumer TIDAK PERNAH menerima path atau URL berkas dari pesan.
// payload.file_id hanya dipakai sebagai KUNCI lookup ke peta di bawah ini --
// nilai file_id yang tidak terdaftar ditolak (lihat validateEvent() di
// src/kontrak.js), dan nama file fisik yang dibaca selalu berasal dari peta
// ini, bukan dari input pesan.
'use strict';
const path = require('node:path');
const { readFileSync, statSync } = require('node:fs');

const FIXTURE_DIR = path.join(__dirname, '..', 'fixtures');

const FIXTURE_FILES = {
  'sample-a': 'sample-a.txt',
  'sample-b': 'sample-b.txt',
  'sample-c': 'sample-c.txt',
};

function berkasTerdaftar(fileId) {
  return Object.prototype.hasOwnProperty.call(FIXTURE_FILES, fileId);
}

// Ukuran byte diambil dari ukuran berkas SUNGGUHAN di disk (fs.statSync),
// bukan dari payload pesan -- berbeda dari implementasi kasus A04 sebelumnya
// yang mempercayai ukuran_bytes dari producer.
function bacaBerkas(fileId) {
  const nama = FIXTURE_FILES[fileId];
  if (!nama) throw new Error(`file_id "${fileId}" tidak terdaftar di fixture lokal`);
  const lokasi = path.join(FIXTURE_DIR, nama);
  const ukuranBytes = statSync(lokasi).size;
  const isi = readFileSync(lokasi, 'utf8');
  return { ukuranBytes, isi };
}

// Aturan hitung kata (didokumentasikan juga di README §2): token dipisahkan
// oleh satu atau lebih whitespace (spasi/tab/newline) setelah trim awal-akhir;
// token kosong diabaikan. Berkas kosong/hanya-whitespace -> 0 kata.
function hitungKata(isi) {
  const rapi = isi.trim();
  return rapi === '' ? 0 : rapi.split(/\s+/).length;
}

module.exports = { FIXTURE_DIR, FIXTURE_FILES, berkasTerdaftar, bacaBerkas, hitungKata };
