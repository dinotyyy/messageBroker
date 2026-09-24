// Boundary AMQP generik: buka koneksi + confirm channel, deklarasikan
// topologi lewat fungsi declare() yang diberikan pemanggil, publish dengan
// mandatory+persistent, dan tutup dengan bersih.
//
// REUSE: fungsi openPublisher() di file ini adalah salinan-yang-diadaptasi
// dari layanan/messaging.js pada repo simpel-lab (lihat README.md bagian
// "Reuse dari lab SIMPEL"). Divendorkan ke sini (bukan di-import lintas-repo)
// supaya proyek ini berdiri sendiri, tidak bergantung pada lokasi/keberadaan
// repo simpel-lab di komputer lain. Mekanisme confirm/mandatory/return TIDAK
// diubah; satu-satunya penyesuaian adalah kunci pelacakan pending-publish
// memakai event.event_id (kontrak kasus ini), menggantikan event.messageId
// pada kontrak SIMPEL asli.
'use strict';
const amqp = require('amqplib');

async function openPublisher(declare, url = process.env.AMQP_URL || 'amqp://a04:a04pengolahan@localhost:5682') {
  const connection = await amqp.connect(url);
  connection.on('error', () => {}); // Close rejects pending work; never print connection URLs.
  let channel, spec;
  try {
    channel = await connection.createConfirmChannel();
    channel.on('error', () => {}); // Topology errors may arrive before setup finishes.
    spec = await declare(channel);
  } catch (error) {
    await connection.close().catch(() => {});
    throw error;
  }
  const pending = new Map();
  let ready = true;
  let buffered = false;
  function failPending(reason) {
    ready = false;
    for (const job of pending.values()) job.finish(new Error(reason));
  }
  channel.on('error', () => failPending('Publisher channel failed; outcome may be unknown'));
  channel.on('close', () => {
    failPending('Publisher channel closed; outcome may be unknown');
    void connection.close().catch(() => {});
  });
  connection.on('close', () => failPending('Broker connection closed; outcome may be unknown'));
  channel.on('drain', () => { buffered = false; });
  channel.on('return', message => {
    const job = pending.get(message.properties.messageId);
    if (job) job.returned = true;
  });
  return {
    channel, connection, spec,
    isReady: () => ready && !buffered,
    publish(event, routingKey, exchange = spec.exchange, headers = {}) {
      if (!ready || buffered) return Promise.reject(new Error('Publisher is not ready'));
      if (pending.has(event.event_id)) return Promise.reject(new Error('The same event_id is already in flight'));
      return new Promise((resolve, reject) => {
        const job = { returned: false, timer: null, finish(error) {
          if (!pending.has(event.event_id)) return;
          pending.delete(event.event_id);
          clearTimeout(job.timer);
          error ? reject(error) : resolve();
        } };
        pending.set(event.event_id, job);
        job.timer = setTimeout(() => job.finish(new Error('Confirm timed out; outcome may be unknown')), 10000);
        try {
          const writable = channel.publish(exchange, routingKey, Buffer.from(JSON.stringify(event)), {
            persistent: true, mandatory: true, contentType: 'application/json',
            messageId: event.event_id, headers,
          }, error => job.finish(error || (job.returned ? new Error('Unroutable publication') : null)));
          // publish()'s boolean is local buffer pressure, not broker acceptance.
          if (!writable) buffered = true;
        } catch (error) { job.finish(error); }
      });
    },
    async close() {
      ready = false;
      await channel.close().catch(() => {});
      await connection.close().catch(() => {});
    },
  };
}

module.exports = { openPublisher };
