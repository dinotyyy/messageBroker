# Laporan Proyek Action Learning — A04: Pengolahan Berkas (Metadata Hasil Pengolahan Tiap Job)

**Pola integrasi:** W (Work Queue) — satu worker.
**Panduan Pengerjaan Minggu Kedua (21–25 September 2026).**
Diisi mengikuti struktur template resmi pelatihan (`lab/action-learning/TEMPLATE.md`
pada repo `simpel-lab`). Kode & konfigurasi: [`../README.md`](../README.md).
Identitas & pembagian kontribusi tim: [README §0](../README.md#0-identitas--kontribusi-tim).
Bukti mentah lengkap (lampiran terpisah, tidak diulang di laporan ini):
[`../bukti/`](../bukti/). Diagram satu halaman: [`../diagram/topologi.svg`](../diagram/topologi.svg).

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

Run: **`run01`**. Dijalankan 23 September 2026 pada infrastruktur mandiri
proyek ini, worker tunggal (`A04_WORKER_ID` otomatis dari PID). Cara
menghitung hasil (kenapa 20→25→25→26, dan bedanya dengan pola P) dijelaskan
di [README §9](../README.md#9-cara-menghitung-hasil). Kebijakan bukti
(batas tunggu 60 detik, apa yang direkam) ada di
[README §10](../README.md#10-bukti-yang-dicatat). File bukti mentah (JSON,
keluaran `uji/skenario.js verifikasi`/`ledger`, log worker) ada di
[`../bukti/`](../bukti/).

| Uji | Langkah | Hasil yang Diharapkan | Hasil Aktual | Waktu tunggu (batas 60s) | Bukti | Status |
|---|---|---|---|---|---|---|
| **U1** | Kirim 20 event valid `run01-N01`..`run01-N20` (`file_id` dirotasi `sample-a`/`sample-b`/`sample-c`) | 20 hasil unik, himpunan ID input = output | 20 baris baru di `file_results`; `idHasilBisnis` cocok persis 20 ID yang dikirim; `ukuran_bytes`/`jumlah_kata` sesuai fixture (61/10, 212/29, 43/5) | 142 ms | `bukti/run01-u1.json` | **LULUS** |
| **U2** | Setelah U1, worker dihentikan paksa (`taskkill //F`, mensimulasikan crash proses); kirim 5 event baru `G01`–`G05` | 5 pesan menunggu di queue; setelah worker pulih, ke-5 ID selesai tanpa kirim ulang manual | Snapshot saat worker mati: `ready=5, unacked=0, consumers=0` (`bukti/run01-u2-antrean-tertahan.json`). Setelah worker dinyalakan kembali: kelima `G01`–`G05` otomatis diproses (log `"hasil":"baru"`), total hasil bisnis run naik dari 20 → **25** | 221 ms | `bukti/run01-u2-antrean-tertahan.json`, `bukti/run01-u2.json` | **LULUS** |
| **U3** | Kirim ulang `run01-N01`..`run01-N05` dengan `event_id` + payload **persis semula** (`occurred_at` sama dengan pengiriman U1) | Efek bisnis tidak bertambah; jumlah hasil tetap 25 | Log worker: kelima event bertanda `"hasil":"duplikat-diabaikan"`. Jumlah baris tetap **25** (sebelum 25, sesudah 25) | 176 ms | `bukti/run01-u3.json` | **LULUS** |
| **U4** | Kirim `run01-X01` (payload `file_id: "sample-tidak-terdaftar"`, tidak valid), lalu `run01-V01` (valid) | `X01` masuk jalur penolakan terdokumentasi tanpa efek bisnis; `V01` selesai, tidak tertahan | `X01` tercatat di `file_rejections` (alasan: *"payload.file_id ... tidak terdaftar di fixture lokal"*) dan `files.penolakan.q` (`ready=1` via dead-letter); **tidak ada** baris `run01-X01` di `file_results`. `V01` selesai (`"hasil":"baru"`) segera setelah `X01` ditolak — total hasil bisnis run naik 25 → **26** | 133 ms | `bukti/run01-u4.json` | **LULUS** |

Kolom "Waktu tunggu" adalah `waktuTungguMs` aktual dari polling
`verifikasi` (lihat README §10) — jauh di bawah batas 60 detik pada keempat
uji, sehingga tidak ada kasus `tercapaiDalamWaktu: false` yang perlu dicatat.

**Ringkasan akhir run01:** 26 baris efek bisnis (20 + 5 + 0 dari replay + 1
dari `V01`), 1 baris penolakan (`X01`), 0 pesan tak ter-route sepanjang
pengujian (`files.tanpa_rute.q` selalu 0) — membuktikan seluruh publish
selama U1–U4 ter-route ke queue tujuan yang benar. Ekspor mentah seluruh 26
baris ledger + 1 baris penolakan (bukan ringkasan), lengkap dengan `job_id`,
`file_id`, `ukuran_bytes`, dan `jumlah_kata` per baris, ada di
`bukti/run01-ledger.json` — sehingga hitungan di atas bisa dibuktikan ulang
oleh pengajar, bukan sekadar diklaim. Verifikasi silang: `job_id` pada
seluruh 26 baris **berbeda satu sama lain** (`JOB-N01`..`JOB-V01`), sementara
`file_id` `sample-a`/`sample-b`/`sample-c` masing-masing dipakai berulang
oleh banyak `job_id` — membuktikan kriteria "berkas yang sama boleh dipakai
beberapa job, dedup berbasis `event_id`" (README §9).

---

## 6. Laporan Investigasi Troubleshooting

**Gejala masalah.** Pada percobaan pertama skenario U2, worker dihentikan
lewat `kill -SIGINT <PID>` dari shell Git Bash (MSYS) sebelum mengirim 5
event baru `G01`–`G05`. Snapshot queue sesaat sesudahnya menunjukkan
`ready:0, consumers:1` — bukan `ready:5, consumers:0` seperti yang
diharapkan bila worker benar-benar mati. Log worker kemudian mengonfirmasi
kelima event **sudah diproses** (`"hasil":"baru"`) meski perintah stop sudah
dijalankan sebelum event dikirim.

**Dua hipotesis penyebab.**
1. **Sinyal SIGINT tidak pernah sampai ke worker** — proses Node.js pada
   Windows yang dijalankan lewat `npm run worker &` di dalam sesi Git Bash
   mungkin tidak menerima `SIGINT` yang dikirim `kill` dari shell MSYS lain,
   karena Windows tidak memiliki sinyal POSIX asli (`kill` MSYS hanya
   mengonversi ke `CTRL_C_EVENT` bila proses berada di console yang sama).
2. **PID yang di-*kill* salah** — PID yang dipakai berasal dari kolom
   `WINPID` hasil `ps aux` MSYS, yang bisa tidak sinkron dengan PID
   sesungguhnya dari proses `node.exe` yang terlihat oleh Windows.

**Langkah pembuktian.** Kedua hipotesis dibedakan dengan memeriksa proses
`node.exe` lewat `tasklist` (alat Windows native, bukan `ps` MSYS) segera
setelah perintah stop dijalankan, dan membandingkan PID itu dengan `workerId`
yang tercetak di log worker (`pekerja-<PID>`):
- Bila hipotesis 2 benar, `tasklist` akan menunjukkan `node.exe` **masih
  berjalan** dengan PID yang **berbeda** dari PID yang dipakai pada perintah
  `kill`.
- Bila hipotesis 1 benar (PID sudah benar tapi sinyal tidak sampai),
  `tasklist` tetap menunjukkan proses yang sama masih hidup walau PID-nya
  cocok dengan yang dikirimi `kill -SIGINT`.

Hasil: `tasklist //FI "IMAGENAME eq node.exe"` menunjukkan `node.exe` dengan
PID **17836** (cocok dengan `workerId: "pekerja-17836"` di log) masih
berjalan, sedangkan PID yang dipakai pada perintah `kill -SIGINT` sebelumnya
adalah **27860** (dari kolom `WINPID` `ps aux`, ternyata tidak sinkron).
Ini mengonfirmasi **hipotesis 2** (PID salah) sebagai penyebab utama —
dan sekaligus mengonfirmasi keterbatasan `kill -SIGINT` lintas MSYS/Windows
dari hipotesis 1 tetap relevan sebagai alasan untuk tidak mengandalkan
`SIGINT` sama sekali pada platform ini.

**Solusi yang diterapkan.** Penghentian worker untuk pengujian U2 diganti
memakai `tasklist //FI "IMAGENAME eq node.exe"` untuk mendapatkan PID
Windows yang benar (dicocokkan dengan `workerId` di log), lalu
`taskkill //F //PID <pid>` untuk mematikan proses secara paksa — pendekatan
yang juga sekaligus lebih representatif untuk skenario "worker crash" yang
ingin dibuktikan U2 (mati mendadak, bukan graceful shutdown). Lima baris
`file_results` yang terlanjur tercatat dari percobaan pertama (`run01-G01`..
`run01-G05`) dihapus (`DELETE ... WHERE event_id IN (...)`, ditargetkan
hanya ke 5 baris tersebut) sebelum U2 diulang dari kondisi bersih.

**Hasil akhir pasca perbaikan.** Snapshot U2 (`bukti/run01-u2-antrean-tertahan.json`)
menunjukkan `ready:5, unacked:0, consumers:0` secara akurat, dan
`npm run uji -- verifikasi run01 u2 25` mengonfirmasi seluruh 5 pesan
diproses tuntas setelah worker dipulihkan — sistem berfungsi normal sesuai
desain; masalah sepenuhnya ada pada cara menghentikan proses worker di
platform Windows/Git Bash, bukan pada topologi atau kode consumer/producer.

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
| **Rabu, 23 September** | **Routing lengkap + bukti pengujian** | **Selesai — kontrak & topologi disesuaikan ke spesifikasi job/file_id/operation, U1–U4 dijalankan ulang penuh dan lulus (bagian 5), termasuk satu insiden operasional yang terdokumentasi (bagian 6)** |
| Kamis, 24 September | Pengujian failure/recovery + laporan | Draft laporan ini disusun; perlu direview ulang & dilengkapi refleksi tim sebelum dikumpulkan |
| Jumat, 25 September | Presentasi & sesi feedback | Menunggu jadwal sesi sinkron; lihat `../presentasi/` |
