# Laporan Proyek Action Learning — A04: Pengolahan Berkas (Metadata Hasil Pengolahan Tiap Job)

**Pola integrasi:** W (Work Queue) — satu worker.
**Panduan Pengerjaan Minggu Kedua (21–25 September 2026).**
Diisi mengikuti struktur template resmi pelatihan (`lab/action-learning/TEMPLATE.md`
pada repo `simpel-lab`). Kode & konfigurasi: [`../README.md`](../README.md).
Identitas & pembagian kontribusi tim: [README §0](../README.md#0-identitas--kontribusi-tim).
Bukti mentah lengkap (lampiran terpisah, tidak diulang di laporan ini):
[`../bukti/`](../bukti/). Diagram satu halaman: [`../diagram/topologi.svg`](../diagram/topologi.svg).

> **Branch `setup-awal`.** Checkpoint ini merekam kondisi proyek setelah
> desain & implementasi (topologi, kontrak event, producer/consumer) selesai
> tapi **sebelum** skenario uji U1–U4 dijalankan — folder `bukti/` masih
> kosong. Untuk laporan dengan hasil pengujian sungguhan, lihat branch `main`.

---

## 1. Identifikasi Masalah, Ruang Lingkup, & Kriteria Keberhasilan

**Latar belakang masalah.** Pada alur perizinan SIMPEL, pemohon mengunggah
berkas pendukung yang perlu **diproses** (mis. dianalisis isinya) sebelum
bisa dipakai proses berikutnya (mis. verifikasi kelengkapan). Bila pemrosesan
dilakukan sinkron di dalam request pengunggahan, lonjakan jumlah job atau
gangguan pada layanan pemroses akan langsung menahan pengguna dan berisiko
kehilangan permintaan saat proses pemroses down. Dibutuhkan decoupling:
permintaan pemrosesan berkas (satu **job**) dipublikasikan sebagai event,
diproses oleh worker terpisah, dan hasilnya (metadata hasil pemrosesan)
tersimpan permanen terlepas dari kapan worker sempat memprosesnya.

**Pengguna & peran sistem.**
- **Pemicu event:** proses permintaan pemrosesan berkas (disimulasikan oleh CLI producer `src/produsen.js`, mewakili API/CLI yang menerima permintaan job).
- **Message broker:** RabbitMQ (exchange `files`, routing key `file.process`, queue `filejobs`, lihat README §3).
- **Consumer:** satu worker (`src/pekerja.js`) yang membaca berkas fixture lokal (lihat README §2), menjalankan `operation: word_count`, dan menulis metadata hasil ke PostgreSQL.

**Batasan prototipe.** Berkas yang diproses adalah **tiga berkas teks
sintetis tetap** di `fixtures/` (bukan hasil unggahan sungguhan); `file_id`
pada payload hanya kunci lookup ke berkas-berkas ini, tidak pernah path/URL
bebas. Hanya `operation: "word_count"` yang didukung. Detail lengkap batasan
ada di README §8.

**Kriteria keberhasilan yang terukur** (dipetakan langsung ke skenario uji bersama U1–U4, run `run01`):
1. 20 event valid (`N01`–`N20`) menghasilkan **20 baris metadata unik** di `file_results`, himpunan `event_id` input = output.
2. Saat worker dihentikan, 5 event baru (`G01`–`G05`) **tertahan di queue** (`messages_ready = 5`, `consumers = 0`); setelah worker dipulihkan, kelima event selesai **tanpa pengiriman ulang manual**.
3. Mengirim ulang `N01`–`N05` dengan `event_id` dan payload semula **tidak menambah baris baru** — jumlah total efek bisnis tetap sama.
4. Payload tidak valid (`X01`, `file_id` tidak terdaftar) **tidak menghasilkan efek bisnis** dan masuk jalur penolakan yang terdokumentasi (tabel `file_rejections` + queue `files.penolakan.q`); event valid berikutnya (`V01`) **tetap selesai**, tidak tertahan oleh `X01`.

---

## 2. Arsitektur Sistem & Spesifikasi Kontrak Event

Diagram topologi, tabel exchange/queue/binding, dan penjelasan lengkap ada di
[README §3](../README.md#3-topologi-rabbitmq) — tidak diulang di sini agar
satu sumber kebenaran.

**Pola integrasi: Work Queue (W).** Dipilih karena kasus ini adalah "satu
jenis pekerjaan (pemrosesan berkas) yang harus dikerjakan tepat oleh satu
pemroses per event", bukan "banyak service independen butuh salinan event
yang sama" (itu pola Pub-Sub). Satu queue kerja dengan competing consumers
memungkinkan penambahan worker kedua di kemudian hari tanpa mengubah topologi
(lihat README §8).

**Janji layanan (acceptance vs completion).** Producer CLI ini bersifat
"fire-and-forget setelah publisher confirm": `src/produsen.js` mengembalikan
kendali ke pemanggil segera setelah broker mengonfirmasi penerimaan pesan
(padanan HTTP 202 pada producer berbentuk API sungguhan), **bukan** setelah
efek bisnis tersimpan. Proses bisnis dinyatakan selesai tuntas ketika baris
muncul di `file_results` — dibuktikan lewat `npm run uji -- verifikasi`.
Celah antara "diterima" dan "ter-publish" (tanpa outbox) didokumentasikan di
README §8.

**Kontrak event** — lihat README §2 untuk skema lengkap dan aturan validasi
(`src/kontrak.js`). Ringkasnya: `event_id`, `event_type`
(`file.processing.requested`), `occurred_at` (ISO 8601), `payload`
(`job_id`, `file_id`, `operation`). `file_id` divalidasi terhadap peta fixture
terdaftar ([`src/berkas.js`](../src/berkas.js)) — bukan sekadar format string.

**Strategi idempotensi.** `event_id` adalah primary key tabel efek bisnis;
pemeriksaan-dan-tulis dilakukan atomik lewat satu statement
`INSERT ... ON CONFLICT (event_id) DO NOTHING RETURNING event_id`. Consumer
ack setelah statement ini selesai, sehingga redelivery (baik dari restart
consumer maupun replay manual) tidak pernah menghasilkan baris efek bisnis
kedua. `job_id` (label bisnis job) sengaja **bukan** kunci dedup — lihat
README §9 untuk pembuktian `job_id` berbeda-beda sementara `file_id` dipakai
berulang. Detail di README §4.

---

## 3. Petunjuk Menjalankan dan Menghentikan Sistem

Lihat [README §5](../README.md#5-menjalankan-sistem) untuk instruksi CLI
lengkap (prasyarat, `.env`, `docker compose up`, `npm run db:siapkan`,
menjalankan producer & consumer sebagai proses terpisah, dan cara berhenti).
Proyek ini berdiri sendiri — infrastrukturnya sendiri (RabbitMQ port
5682/15682, PostgreSQL port 5442, kredensial `a04`/`a04pengolahan`), tidak
memakai atau mengganggu container lab lain. Tidak ada kredensial produksi di
repositori.

---

## 4. Konfigurasi Routing dan Kontrol Akses

Deklarasi exchange/queue/binding ada di [`src/topologi.js`](../src/topologi.js)
dan direkap di [README §3](../README.md#3-topologi-rabbitmq). Proyek ini
memakai satu user RabbitMQ (`a04`, tag administrator, dibuat otomatis oleh
`docker-compose.yml`) tanpa user non-admin tambahan — cakupan kontrol akses
per-user (permission regex) berada di luar cakupan capstone ini, sesuai
arahan penugasan yang memfokuskan pola W pada pembagian pekerjaan dan
pemulihan consumer.

---

## 5. Bukti Hasil Pengujian (Evidence Matrix)

**Status pada checkpoint ini: belum dilaksanakan.** Topologi, kontrak event,
dan kode producer/consumer sudah siap dan bisa dijalankan (lihat §2–§4), tapi
skenario uji U1–U4 belum dieksekusi pada branch ini — folder
[`../bukti/`](../bukti/) masih kosong. Rencana skenario dan cara
menjalankannya ada di [README §6](../README.md#6-skenario-uji-u1u4); cara
menghitung hasil (kenapa nanti akan jadi 20→25→25→26, dan bedanya dengan
pola P) dijelaskan di [README §9](../README.md#9-cara-menghitung-hasil).
Kebijakan bukti (batas tunggu 60 detik, apa yang direkam) ada di
[README §10](../README.md#10-bukti-yang-dicatat). Tabel di bawah ini akan
diisi kolom "Hasil Aktual"/"Waktu tunggu"/"Bukti"/"Status" setelah pengujian
sungguhan dijalankan (lihat branch `main` untuk hasil yang sudah diuji).

| Uji | Langkah yang direncanakan | Hasil yang Diharapkan | Status |
|---|---|---|---|
| **U1** | Kirim 20 event valid `run01-N01`..`run01-N20` (`file_id` dirotasi `sample-a`/`sample-b`/`sample-c`) | 20 hasil unik, himpunan ID input = output | Belum dijalankan |
| **U2** | Setelah U1, worker dihentikan paksa (mensimulasikan crash proses); kirim 5 event baru `G01`–`G05` | 5 pesan menunggu di queue; setelah worker pulih, ke-5 ID selesai tanpa kirim ulang manual | Belum dijalankan |
| **U3** | Kirim ulang `run01-N01`..`run01-N05` dengan `event_id` + payload **persis semula** | Efek bisnis tidak bertambah; jumlah hasil tetap 25 | Belum dijalankan |
| **U4** | Kirim `run01-X01` (payload `file_id: "sample-tidak-terdaftar"`, tidak valid), lalu `run01-V01` (valid) | `X01` masuk jalur penolakan terdokumentasi tanpa efek bisnis; `V01` selesai, tidak tertahan | Belum dijalankan |

Setelah dijalankan, ringkasan akhir yang diharapkan: 26 baris efek bisnis
(20 + 5 + 0 dari replay + 1 dari `V01`), 1 baris penolakan (`X01`), 0 pesan
tak ter-route sepanjang pengujian (`files.tanpa_rute.q` tetap 0). Ekspor
mentah ledger + bukti tiap uji akan disimpan ke `bukti/run01-*.json` supaya
hitungan bisa dibuktikan ulang, bukan sekadar diklaim.

---

## 6. Laporan Investigasi Troubleshooting

**Status pada checkpoint ini: belum ada insiden tercatat.** Pengujian U1–U4
belum dijalankan pada branch ini (lihat §5), sehingga belum ada gangguan
nyata yang bisa diinvestigasi. Bagian ini akan diisi mengikuti struktur
gejala → hipotesis → pembuktian → solusi → verifikasi pemulihan begitu
skenario U2 (worker dihentikan paksa lalu dipulihkan, lihat
[README §6](../README.md#6-skenario-uji-u1u4)) dijalankan dan ada temuan
yang perlu didokumentasikan — lihat branch `main` untuk laporan investigasi
yang sudah diisi dari pengujian sungguhan.

---

## 7. Batasan Sistem dan Pengembangan Berikutnya

Lihat [README §8](../README.md#8-batasan-prototipe) untuk daftar lengkap.
Ringkasnya: satu worker (worker kedua = pengembangan opsional belum diuji),
fixture berkas sintetis tetap dengan satu operasi (`word_count`) yang
didukung, tidak ada transactional outbox pada producer (celah
acceptance-vs-publish), broker & database single-node tanpa TLS/mTLS, dan
**tidak ada klaim "zero message loss" atau "exactly-once" menyeluruh** —
hasil U1–U4 hanya membuktikan idempotensi consumer-side dan pemulihan dari
gangguan proses consumer, bukan dari kegagalan infrastruktur (broker/DB)
itu sendiri.

Pengembangan berikutnya yang disarankan: worker kedua untuk kasus W
(memverifikasi tidak ada duplikasi efek bisnis lintas-consumer berkat
idempotensi berbasis `event_id`), operasi tambahan selain `word_count` (mis.
`checksum`, `line_count`), retry/backoff otomatis dengan TTL queue untuk
kegagalan sementara, dan transactional outbox pada sisi producer bila kasus
ini dikembangkan menjadi API HTTP sungguhan.

---

## 8. Kontribusi dan Rujukan

Rincian lengkap ada di [README §7](../README.md#7-bagian-yang-dipakai-ulang-dari-lab-simpel).
Ringkasnya: `openPublisher()` divendorkan dan diadaptasi dari
`layanan/messaging.js` (repo `simpel-lab`) menjadi [`src/broker.js`](../src/broker.js);
pola alternate-exchange, dead-letter-exchange, graceful shutdown,
`INSERT ... ON CONFLICT`, dan Management API client diadaptasi dari
`layanan/messaging.js`, `tools/kasus.js`, `tools/operasi.js`, dan
`layanan/alur.js` pada repo yang sama. Kontrak event, skema tabel `file_*`,
fixture berkas lokal + pemrosesan `word_count` sungguhan, seluruh CLI
producer/consumer/orkestrasi uji, serta infrastruktur `docker-compose.yml`
mandiri adalah pengembangan baru untuk kasus A04.

---

## Status Checkpoint

| Hari / Tanggal | Target Milestone | Status |
|---|---|---|
| Senin, 21 September | Rumusan masalah, diagram topologi, kontrak event | Selesai (bagian 1–2 di atas) |
| Selasa, 22 September | Implementasi dasar producer, exchange, queue, consumer aktif | Selesai — proyek dipisah menjadi folder mandiri |
| Rabu, 23 September | Routing lengkap + bukti pengujian | Kontrak & topologi sudah disesuaikan ke spesifikasi job/file_id/operation (kode siap dijalankan); **U1–U4 belum dijalankan pada branch ini** — lihat bagian 5 |
| Kamis, 24 September | Pengujian failure/recovery + laporan | Belum dimulai pada branch ini — menunggu eksekusi skenario uji (lihat branch `main`) |
| Jumat, 25 September | Presentasi & sesi feedback | Menunggu jadwal sesi sinkron; lihat `../presentasi/` |
