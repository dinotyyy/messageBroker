// Kontrak event kasus A04 - Pengolahan Berkas (pola Work Queue).
//
// Format envelope: { event_id, event_type, occurred_at, payload: {...} },
// sesuai lembar penugasan Action Learning (envelope halaman 4). event_id
// mengidentifikasi SATU permintaan pemrosesan berkas (satu job); job_id di
// payload adalah identitas job itu sendiri, dipakai sebagai label bisnis --
// dedup tetap berbasis event_id, BUKAN job_id (lihat README §2 dan §9).
'use strict';
const { berkasTerdaftar } = require('./berkas');

const EVENT_TYPE = 'file.processing.requested';
const ID_PATTERN = /^[A-Za-z0-9_-]{1,160}$/;
const OPERASI_DIIZINKAN = new Set(['word_count']);

// Dipakai worker (pekerja.js) untuk membedakan "pesan gagal validasi kontrak"
// (jalur penolakan, U4) dari "kegagalan tak terduga" (mis. DB mati) yang tidak
// boleh di-ack maupun di-nack tanpa requeue.
class KontrakError extends Error {}

function validateEvent(event) {
  const gagal = pesan => { throw new KontrakError(pesan); };

  if (!event || typeof event !== 'object' || Array.isArray(event)) gagal('Event kosong atau bukan objek JSON');
  if (event.event_type !== EVENT_TYPE) gagal(`payload.event_type harus "${EVENT_TYPE}"`);
  if (typeof event.event_id !== 'string' || !ID_PATTERN.test(event.event_id)) gagal('event_id tidak valid (wajib string alfanumerik, maks 160 karakter)');
  if (typeof event.occurred_at !== 'string' || !Number.isFinite(Date.parse(event.occurred_at))) gagal('occurred_at tidak valid (wajib ISO 8601)');

  const p = event.payload;
  if (!p || typeof p !== 'object' || Array.isArray(p)) gagal('payload kosong atau bukan objek');
  if (typeof p.job_id !== 'string' || !ID_PATTERN.test(p.job_id)) gagal('payload.job_id tidak valid (wajib string alfanumerik, maks 160 karakter)');
  if (typeof p.file_id !== 'string' || !p.file_id.trim()) gagal('payload.file_id tidak valid');
  // Consumer hanya menerima file_id yang TERDAFTAR di fixture lokal -- tidak
  // pernah menerima path atau URL bebas dari pesan (lihat src/berkas.js).
  if (!berkasTerdaftar(p.file_id)) gagal(`payload.file_id "${p.file_id}" tidak terdaftar di fixture lokal`);
  if (typeof p.operation !== 'string' || !OPERASI_DIIZINKAN.has(p.operation)) gagal(`payload.operation tidak dikenali (salah satu dari: ${[...OPERASI_DIIZINKAN].join(', ')})`);

  return event;
}

module.exports = { EVENT_TYPE, ID_PATTERN, OPERASI_DIIZINKAN, KontrakError, validateEvent };
