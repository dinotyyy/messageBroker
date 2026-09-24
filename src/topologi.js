// Topologi RabbitMQ kasus A04 - Pengolahan Berkas (pola W / Work Queue,
// competing consumers, satu worker aktif).
//
// Nama exchange `files`, routing key `file.process`, dan queue kerja
// `filejobs` mengikuti lembar penugasan. Pola yang dipakai ulang dari lab
// simpel-lab (lihat README.md bagian "Reuse dari lab SIMPEL"): alternate-
// exchange untuk pesan tak ter-route dan dead-letter-exchange di queue kerja
// untuk jalur penolakan kontrak. Nama exchange/queue pendukung (tanpa-rute,
// penolakan) dibuat baru untuk kasus ini.
'use strict';

const EXCHANGE_UTAMA = 'files';
const EXCHANGE_TANPA_RUTE = 'files.tanpa_rute';
const EXCHANGE_PENOLAKAN = 'files.penolakan';

const QUEUE_KERJA = 'filejobs';
const QUEUE_TANPA_RUTE = 'files.tanpa_rute.q';
const QUEUE_PENOLAKAN = 'files.penolakan.q';

const ROUTING_KEY = 'file.process';
const ROUTING_KEY_PENOLAKAN = 'invalid';

async function declareTopology(channel) {
  // 1) Jalur pesan tak ter-route. Dipasang sebagai alternate-exchange dari
  //    exchange utama: publisher confirm yang sukses TIDAK membuktikan pesan
  //    sampai ke queue tujuan (bisa saja routing key salah/queue belum ada).
  //    Dengan alternate-exchange, pesan yang gagal cocok binding apa pun tetap
  //    ditangkap di sini -- bisa diperiksa lewat `npm run uji -- antrean` --
  //    alih-alih hilang diam-diam.
  await channel.assertExchange(EXCHANGE_TANPA_RUTE, 'fanout', { durable: true });
  await channel.assertQueue(QUEUE_TANPA_RUTE, { durable: true });
  await channel.bindQueue(QUEUE_TANPA_RUTE, EXCHANGE_TANPA_RUTE, '');

  // 2) Jalur penolakan kontrak (U4). Worker mem-validasi skema + file_id
  //    terdaftar; pesan yang gagal di-nack TANPA requeue, dan queue kerja
  //    mengarahkannya ke sini lewat dead-letter-exchange/-routing-key. Tanpa
  //    efek bisnis apa pun.
  await channel.assertExchange(EXCHANGE_PENOLAKAN, 'direct', { durable: true });
  await channel.assertQueue(QUEUE_PENOLAKAN, { durable: true });
  await channel.bindQueue(QUEUE_PENOLAKAN, EXCHANGE_PENOLAKAN, ROUTING_KEY_PENOLAKAN);

  // 3) Jalur kerja utama. Direct exchange dipilih karena hanya ada SATU
  //    routing key (satu jenis pekerjaan, satu queue, work-queue klasik) --
  //    topic/fanout tidak diperlukan untuk pola W di kasus ini.
  await channel.assertExchange(EXCHANGE_UTAMA, 'direct', { durable: true, alternateExchange: EXCHANGE_TANPA_RUTE });
  await channel.assertQueue(QUEUE_KERJA, {
    durable: true,
    deadLetterExchange: EXCHANGE_PENOLAKAN,
    deadLetterRoutingKey: ROUTING_KEY_PENOLAKAN,
  });
  await channel.bindQueue(QUEUE_KERJA, EXCHANGE_UTAMA, ROUTING_KEY);

  return { exchange: EXCHANGE_UTAMA };
}

module.exports = {
  declareTopology,
  EXCHANGE_UTAMA, EXCHANGE_TANPA_RUTE, EXCHANGE_PENOLAKAN,
  QUEUE_KERJA, QUEUE_TANPA_RUTE, QUEUE_PENOLAKAN,
  ROUTING_KEY, ROUTING_KEY_PENOLAKAN,
};
