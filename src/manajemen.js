// Klien tipis untuk RabbitMQ Management HTTP API, dipakai untuk MEMBUKTIKAN
// routing (bukan cuma percaya publisher confirm): mengecek jumlah pesan ready/
// unacked pada queue tujuan secara langsung. Pola diadaptasi dari
// tools/operasi.js pada repo simpel-lab (lihat README.md bagian "Reuse"),
// disederhanakan untuk satu vhost dan kredensial proyek ini.
'use strict';

function konfigurasi() {
  return {
    base: process.env.RABBITMQ_MGMT_URL || 'http://127.0.0.1:15682',
    user: process.env.RABBITMQ_USER || 'a04',
    pass: process.env.RABBITMQ_PASS || 'a04pengolahan',
    vhost: process.env.RABBITMQ_VHOST || '/',
  };
}

async function api(resource) {
  const { base, user, pass } = konfigurasi();
  const res = await fetch(base + '/api' + resource, {
    headers: { authorization: 'Basic ' + Buffer.from(`${user}:${pass}`).toString('base64') },
    signal: AbortSignal.timeout(10000),
  });
  if (!res.ok) throw new Error(`Management API ${resource} -> HTTP ${res.status}`);
  return res.json();
}

async function queueSnapshot(nama) {
  const { vhost } = konfigurasi();
  const q = await api(`/queues/${encodeURIComponent(vhost)}/${encodeURIComponent(nama)}`);
  return {
    queue: q.name,
    ready: q.messages_ready,
    unacked: q.messages_unacknowledged,
    total: q.messages,
    consumers: q.consumers,
    diambilPada: new Date().toISOString(),
  };
}

module.exports = { api, queueSnapshot, konfigurasi };
