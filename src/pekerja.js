#!/usr/bin/env node
// Consumer/worker CLI kasus A04 - Pengolahan Berkas (pola W, satu worker aktif;
// worker kedua adalah pengembangan opsional -- lihat README bagian Batasan).
//
// REUSE: openPublisher() (src/broker.js, adaptasi dari layanan/messaging.js
// pada repo simpel-lab) dipakai untuk membuka koneksi + channel + deklarasi
// topologi yang SAMA persis dengan producer (idempoten). Idiom graceful-
// shutdown (cancel consumer, tunggu pekerjaan aktif selesai, lalu tutup
// koneksi) meniru pola dari tools/kasus.js dan tools/routing.js pada repo
// simpel-lab.
//
// BARU: validasi kontrak, pemrosesan berkas sungguhan (ukuran byte + jumlah
// kata dari fixture lokal lewat src/berkas.js), dan penulisan hasil idempoten
// (event_id sebagai kunci) ke PostgreSQL di bawah ini.
'use strict';
const { randomInt } = require('node:crypto');
const { setTimeout: delay } = require('node:timers/promises');
const { openPublisher } = require('./broker');
const { declareTopology, QUEUE_KERJA } = require('./topologi');
const { validateEvent, KontrakError } = require('./kontrak');
const { bacaBerkas, hitungKata } = require('./berkas');
const { getPool, end } = require('./basisdata');

const WORKER_ID = process.env.A04_WORKER_ID || `pekerja-${process.pid}`;
const KERJA_MIN_MS = Number(process.env.A04_KERJA_MIN_MS ?? 40);
const KERJA_MAX_MS = Number(process.env.A04_KERJA_MAX_MS ?? 160);

// "Efek bisnis": ukuran byte AKTUAL (fs.statSync) + jumlah kata (aturan di
// README §2) dari berkas fixture yang dirujuk file_id. file_id hanya dipakai
// sebagai kunci lookup lewat src/berkas.js -- tidak pernah sebagai path
// langsung, sehingga payload pesan tidak bisa memaksa worker membaca berkas
// arbitrer di luar fixtures/.
function prosesBerkas(fileId) {
  const { ukuranBytes, isi } = bacaBerkas(fileId);
  const jumlahKata = hitungKata(isi);
  return { ukuranBytes, jumlahKata };
}

async function simpanHasil(pool, event, meta, durasiMs) {
  // Pemeriksaan duplikat + penulisan efek bisnis dalam SATU statement atomik:
  // event_id adalah primary key, ON CONFLICT DO NOTHING mencegah baris kedua
  // walau job_id berbeda (mis. replay event_id yang sama dengan job_id yang
  // sama pula, karena event_id dan job_id selalu dikirim berpasangan).
  const r = await pool.query(
    `INSERT INTO file_results
       (event_id, job_id, file_id, operation, ukuran_bytes, jumlah_kata,
        durasi_proses_ms, status, worker_id, occurred_at, payload_asli)
     VALUES ($1,$2,$3,$4,$5,$6,$7,'selesai',$8,$9,$10)
     ON CONFLICT (event_id) DO NOTHING
     RETURNING event_id`,
    [event.event_id, event.payload.job_id, event.payload.file_id, event.payload.operation,
     meta.ukuranBytes, meta.jumlahKata, durasiMs, WORKER_ID, event.occurred_at, event.payload],
  );
  return r.rowCount > 0; // true = baris baru; false = duplikat idempoten (sudah pernah diproses)
}

async function catatPenolakan(pool, eventMentah, alasan) {
  await pool.query(
    'INSERT INTO file_rejections (event_id, alasan, payload_mentah) VALUES ($1,$2,$3)',
    [eventMentah && typeof eventMentah.event_id === 'string' ? eventMentah.event_id : null, alasan, eventMentah ?? null],
  );
}

async function main() {
  const pool = getPool();
  const p = await openPublisher(declareTopology);
  const ch = p.channel;
  await ch.prefetch(1); // satu worker, satu pesan diproses pada satu waktu -- memudahkan pengamatan saat uji gangguan
  let active = 0, stopping = false;

  const { consumerTag } = await ch.consume(QUEUE_KERJA, async msg => {
    if (!msg) return;
    active++;
    let mentah;
    try {
      mentah = JSON.parse(msg.content.toString());
      const event = validateEvent(mentah);
      const meta = prosesBerkas(event.payload.file_id);
      const durasiMs = randomInt(KERJA_MIN_MS, KERJA_MAX_MS + 1);
      await delay(durasiMs); // simulasi kerja pengolahan berkas (I/O lokal, tanpa layanan pihak ketiga)

      const baru = await simpanHasil(pool, event, meta, durasiMs);
      ch.ack(msg); // ACK HANYA setelah efek bisnis tersimpan di PostgreSQL
      console.log(JSON.stringify({
        workerId: WORKER_ID, event_id: event.event_id, job_id: event.payload.job_id,
        hasil: baru ? 'baru' : 'duplikat-diabaikan', durasiMs,
        redelivered: msg.fields.redelivered,
      }));
    } catch (error) {
      if (error instanceof KontrakError || error instanceof SyntaxError) {
        // Jalur penolakan (U4): dokumentasikan di tabel file_rejections, lalu
        // nack TANPA requeue -- queue kerja akan mengarahkannya ke DLX
        // (files.penolakan) sesuai konfigurasi di topologi.js. Tidak ada baris
        // baru di file_results untuk pesan ini.
        await catatPenolakan(pool, mentah, error.message);
        ch.nack(msg, false, false);
        console.log(JSON.stringify({ workerId: WORKER_ID, event_id: mentah && mentah.event_id, hasil: 'ditolak', alasan: error.message }));
      } else {
        // Kegagalan tak terduga (mis. PostgreSQL tidak terjangkau, atau
        // fixture hilang dari disk): JANGAN ack, JANGAN nack. Pesan tetap
        // unacked di broker dan akan dikirim ulang begitu koneksi ini
        // ditutup -- tidak ada efek bisnis yang hilang diam-diam, tapi
        // worker berhenti agar operator memeriksa dulu.
        console.error(JSON.stringify({ workerId: WORKER_ID, event_id: mentah && mentah.event_id, error: error.message, aksi: 'berhenti-tanpa-ack' }));
        process.exitCode = 1;
        await p.close(); await end();
      }
    } finally { active--; }
  }, { noAck: false });

  console.log(JSON.stringify({ siap: true, workerId: WORKER_ID, queue: QUEUE_KERJA }));

  await new Promise(resolve => {
    p.connection.once('close', () => { stopping = true; resolve(); });
    for (const signal of ['SIGINT', 'SIGTERM']) {
      process.once(signal, async () => {
        if (stopping) return; stopping = true;
        console.log(JSON.stringify({ workerId: WORKER_ID, berhenti: 'menunggu pekerjaan aktif selesai, pesan belum diambil akan tetap di queue' }));
        await ch.cancel(consumerTag).catch(() => {});
        while (active) await delay(25);
        await p.close(); await end(); resolve();
      });
    }
  });
}

if (require.main === module) main().catch(e => { console.error(e.message); process.exitCode = 1; });
module.exports = { prosesBerkas, simpanHasil, catatPenolakan };
