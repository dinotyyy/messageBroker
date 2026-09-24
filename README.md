# A04-W — Pengolahan Berkas (Metadata Hasil Pengolahan)

Proyek Action Learning Minggu Kedua, PJJ *Implementasi dan Pengelolaan Message
Broker untuk Arsitektur Microservices* (BPPK Kemenkeu). Kasus **A04**, pola
**W (Work Queue)** — satu worker, fokus pembagian pekerjaan dan pemulihan
consumer.

**Proyek ini berdiri sendiri** (RabbitMQ + PostgreSQL + Node.js miliknya
sendiri, port dan kredensial terpisah) — tidak lagi bergantung pada repositori
`simpel-lab`. Beberapa pola desainnya diadaptasi dari lab tersebut; lihat §7.

## 0. Identitas & Kontribusi Tim

|---|---|
| Kode kasus | A04-W — Pengolahan Berkas, pola Work Queue |
| Pelatihan | PJJ Implementasi dan Pengelolaan Message Broker untuk Arsitektur Microservices — BPPK Kemenkeu, 14–25 September 2026 |

**Anggota kelompok & pembagian kontribusi:**

| Nama | NIP | Kontribusi |
|---|---|---|
| Laeila Mardhatilla | 198906262024212004 | Topologi broker, Pengujian U1–U4, serta penulisan laporan dan bahan presentasi |
| Dhiya Nida Ulayya | 199606062024212003 | Topologi broker, kode producer/worker termasuk idempotensi di sisi worker, skema database |


## 1. Ringkasan kasus

Produsen mempublikasikan kejadian *"permintaan pemrosesan berkas"* (satu
*job*). Satu worker (pola Work Queue — competing consumers, satu instance
aktif; worker kedua adalah pengembangan opsional, lihat §8) mengambil pesan
dari satu antrean, membaca **berkas teks sintetis lokal** yang dirujuk oleh
`file_id` pada payload, menghitung **ukuran byte sungguhan + jumlah kata**
(`operation: word_count`), dan menyimpan hasilnya ke PostgreSQL sebagai efek
bisnis. Producer dan consumer berjalan sebagai proses Node.js terpisah.

## 2. Kontrak event

```json
{
  "event_id": "run01-N01",
  "event_type": "file.processing.requested",
  "occurred_at": "2026-09-23T07:38:12.312Z",
  "payload": {
    "job_id": "JOB-N01",
    "file_id": "sample-a",
    "operation": "word_count"
  }
}
```

`event_id` mengidentifikasi **satu permintaan pemrosesan** (satu pengiriman
job) dan menjadi **kunci dedup** — replay dengan `event_id` yang sama tidak
pernah menghasilkan efek bisnis kedua (lihat §9). `job_id` adalah **label
bisnis job itu sendiri**: untuk 26 event valid pada run ini, seluruh `job_id`
berbeda satu sama lain, tapi `file_id` yang sama boleh dipakai berulang oleh
banyak job (lihat tabel rotasi fixture di bawah). Validasi lengkap ada di
[`src/kontrak.js`](src/kontrak.js).

### Fixture berkas & pemetaan `file_id`

Consumer **tidak pernah** menerima path atau URL berkas dari pesan.
`payload.file_id` hanya dipakai sebagai **kunci lookup** ke peta berikut
([`src/berkas.js`](src/berkas.js)), yang menunjuk ke tiga berkas teks
sintetis lokal di [`fixtures/`](fixtures/). `file_id` yang tidak terdaftar di
peta ini **ditolak** oleh `validateEvent()` (jalur penolakan kontrak, lihat
U4 di §6) — worker tidak akan pernah membaca berkas arbitrer di luar peta ini.

| `file_id` | Berkas | Ukuran (bytes) | Jumlah kata |
|---|---|---|---|
| `sample-a` | [`fixtures/sample-a.txt`](fixtures/sample-a.txt) | 61 | 10 |
| `sample-b` | [`fixtures/sample-b.txt`](fixtures/sample-b.txt) | 212 | 29 |
| `sample-c` | [`fixtures/sample-c.txt`](fixtures/sample-c.txt) | 43 | 5 |

Producer ([`src/produsen.js`](src/produsen.js)) merotasi `file_id` di antara
ketiga fixture ini saat membuat event `kirim` (`N01`→`sample-a`,
`N02`→`sample-b`, `N03`→`sample-c`, `N04`→`sample-a`, dst.) — membuktikan
satu berkas yang sama dipakai oleh beberapa `job_id` berbeda.

### Aturan hitung kata (`operation: word_count`)

Didokumentasikan di sini karena dipakai langsung oleh worker
([`src/berkas.js`](src/berkas.js) fungsi `hitungKata()`):

- **`ukuran_bytes`** = ukuran berkas **sungguhan di disk** (`fs.statSync`),
  bukan angka dari payload pesan (payload tidak pernah membawa ukuran).
- **`jumlah_kata`** = isi berkas di-*trim* (buang whitespace di awal/akhir),
  lalu dipecah oleh satu atau lebih karakter whitespace (spasi, tab, baris
  baru); token kosong diabaikan. Berkas kosong atau hanya berisi whitespace
  → 0 kata.
- Operasi yang didukung saat ini **hanya `word_count`**; nilai `operation`
  lain ditolak oleh `validateEvent()`.

## 3. Topologi RabbitMQ

```
                                   ┌─ (alternate-exchange) ─┐
                                   ▼                         │
produsen.js ──publish──▶     files (direct)  ────────▶ files.tanpa_rute (fanout)
                 rk: file.process │                        │
                                   ▼                        ▼
                               filejobs                files.tanpa_rute.q
                                   │                  (pesan tak ter-route;
                          consume (prefetch=1)         harus tetap 0 selama
                                   ▼                    binding benar)
                             pekerja.js (worker)
                          ├─ valid  → INSERT ... ON CONFLICT DO NOTHING
                          │           (file_results) → ack
                          └─ invalid → INSERT (file_rejections) → nack(requeue:false)
                                        │
                                        ▼ (dead-letter-exchange queue kerja)
                                files.penolakan (direct) ──▶ files.penolakan.q
```

| Exchange | Tipe | Durable | Peran |
|---|---|---|---|
| `files` | direct | ya | Exchange utama; `alternateExchange: files.tanpa_rute` |
| `files.tanpa_rute` | fanout | ya | Menangkap pesan yang gagal cocok binding apa pun |
| `files.penolakan` | direct | ya | Tujuan dead-letter dari queue kerja |

| Queue | Binding | Durable | Peran |
|---|---|---|---|
| `filejobs` | `files` rk=`file.process` | ya, persistent | Queue kerja (work queue, satu worker) |
| `files.tanpa_rute.q` | `files.tanpa_rute` rk=`` | ya | Bukti pesan tak ter-route |
| `files.penolakan.q` | `files.penolakan` rk=`invalid` | ya | Jalur penolakan kontrak (DLQ) |

**Kenapa direct, bukan topic/fanout?** Kasus ini pola W (satu jenis pekerjaan,
satu queue kerja, banyak konsumen bersaing) — hanya perlu satu routing key
(`file.process`). Topic/fanout baru relevan untuk pola Pub-Sub / selective
routing, di luar cakupan kasus ini.

**Publisher confirm saja tidak cukup.** Confirm channel + `mandatory:true`
hanya membuktikan broker *menerima* pesan, bukan pesan *sampai* ke queue
tujuan. Dua lapis pembuktian dipakai di sini:
1. `alternateExchange` — pesan yang gagal cocok binding tetap tertangkap di
   `files.tanpa_rute.q`, tidak pernah hilang diam-diam.
2. `uji/skenario.js antrean` memeriksa `messages_ready` queue tujuan lewat
   RabbitMQ Management API secara langsung (lihat [`src/manajemen.js`](src/manajemen.js)),
   dipakai U2 untuk membuktikan 5 pesan benar-benar mengantre saat worker mati.

## 4. Idempotensi & acknowledgment

- Efek bisnis: tabel `file_results`, `event_id` sebagai **PRIMARY KEY**.
- Pemeriksaan duplikat + penulisan dilakukan **atomik dalam satu statement**:
  `INSERT ... ON CONFLICT (event_id) DO NOTHING RETURNING event_id`
  (lihat `simpanHasil()` di [`src/pekerja.js`](src/pekerja.js)).
- Consumer **ack setelah** query INSERT selesai (efek bisnis sudah tersimpan),
  baik untuk baris baru maupun duplikat yang diabaikan — keduanya berarti
  "pesan ini sudah punya efek bisnis di database", sehingga aman di-ack.
- Payload tidak valid — termasuk `file_id` yang **tidak terdaftar** di fixture
  lokal — dicatat ke `file_rejections`, lalu `nack(msg, false, false)`
  (requeue **false**) sehingga broker mem-buang pesan ke DLX `files.penolakan`
  — jalur penolakan yang terdokumentasi di broker maupun di database, tanpa menyentuh tabel efek bisnis.
- Kegagalan tak terduga (mis. PostgreSQL putus) → pesan **tidak** di-ack/nack;
  worker menutup koneksi dan berhenti. Pesan tetap unacked di broker dan akan
  dikirim ulang ke consumer berikutnya begitu koneksi lama ditutup broker.

## 5. Menjalankan sistem

**Prasyarat:** Node.js >= 20.6, Docker Desktop.

**Setup** (sekali di awal, atau setelah `docker compose down -v`):
```bash
cd a04-w-pengolahan-berkas
cp .env.contoh .env        # salin konfigurasi contoh; TIDAK ada kredensial produksi
npm install

# Infrastruktur mandiri (RabbitMQ di port 5682/15682, Postgres di 5442 --
# beda dari default 5672/15672/5432 supaya tidak bentrok kalau ada stack lain)
docker compose up -d

# Skema tabel + topologi RabbitMQ (idempoten, aman diulang)
npm run db:siapkan
npm run topologi:siapkan
```

**Start** (producer dan consumer sebagai proses TERPISAH, dua terminal):
```bash
npm run worker                                        # terminal A — consumer, biarkan berjalan
```

**Publish** (kirim event dari terminal lain, worker di terminal A harus tetap hidup):
```bash
npm run produsen -- kirim --run run01 --ids N01-N20    # terminal B
```

**Test** (jalankan skenario uji U1–U4 secara manual — lihat §6 untuk urutan lengkap):
```bash
npm run produsen -- kirim --run run01 --ids N01-N20
npm run uji -- verifikasi run01 u1 20
```

**Stop:**
```bash
# Consumer: Ctrl+C di terminal worker (graceful -- menunggu pekerjaan aktif selesai)
docker compose down       # infrastruktur berhenti, data tetap ada di volume a04-*-data
docker compose down -v    # atau reset total (volume ikut terhapus)
```

### Cara memeriksa hasil

Cara paling langsung (tidak lewat script apa pun) — query database yang
menyimpan efek bisnis:
```bash
docker exec -it a04-postgres psql -U a04 -d a04 -c \
  "SELECT event_id, job_id, file_id, operation, ukuran_bytes, jumlah_kata, diproses_pada FROM file_results ORDER BY event_id;"

docker exec -it a04-postgres psql -U a04 -d a04 -c \
  "SELECT event_id, alasan, ditolak_pada FROM file_rejections ORDER BY ditolak_pada;"

docker exec -it a04-postgres psql -U a04 -d a04 -c \
  "SELECT count(*) FROM file_results WHERE event_id LIKE 'run01-%';"
```

Cara lewat script proyek (menghasilkan file JSON di `bukti/`, lihat §10):
```bash
npm run uji -- verifikasi run01 <label> [jumlahEkspektasi]   # DB + status queue
npm run uji -- ledger run01                                  # ekspor mentah seluruh baris (ledger/receipt)
npm run uji -- antrean                                       # status ready/unacked/consumers ketiga queue
```

Status queue juga bisa dilihat langsung di RabbitMQ Management UI:
`http://localhost:15682` (user `a04`, password sesuai `.env`).

## 6. Skenario uji U1–U4

Pengiriman event untuk tiap skenario dilakukan **manual**, langsung lewat CLI
producer ([`src/produsen.js`](src/produsen.js)), termasuk menghentikan/
menyalakan proses worker di terminal terpisah — supaya gangguan yang diuji
adalah gangguan proses sungguhan, bukan simulasi lewat script orkestrasi.
Verifikasi hasil tetap memakai `npm run uji -- verifikasi`/`ledger` karena
dua perintah inilah yang membaca DB + RabbitMQ Management API dan menulis
file bukti JSON ke `bukti/` (lihat §10). Topologi sudah dideklarasikan sekali
di §5 (`npm run topologi:siapkan`) — tidak perlu diulang di sini.

**U1 — 20 event valid (`N01`–`N20`):**
```bash
npm run produsen -- kirim --run run01 --ids N01-N20
# tunggu worker selesai memproses semua, lalu:
npm run uji -- verifikasi run01 u1 20
```

**U2 — worker mati saat event baru masuk, lalu pulih:**
```bash
# 1. HENTIKAN WORKER DULU: Ctrl+C di terminal worker, tunggu proses benar-benar berhenti
npm run produsen -- kirim --run run01 --ids G01-G05
# 2. selagi worker masih mati, buktikan pesan mengantre (ready harus 5, consumers 0):
npm run uji -- antrean
#    (atau lihat langsung di Management UI: http://localhost:15682)
# 3. nyalakan kembali worker (npm run worker), tunggu selesai memproses, lalu:
npm run uji -- verifikasi run01 u2 25
```

**U3 — replay `N01`–`N05` dengan `event_id` + payload identik (uji idempotensi):**
```bash
npm run produsen -- ulang --run run01 --ids N01-N05
# tunggu worker memproses ulang (tidak boleh menambah baris baru), lalu:
npm run uji -- verifikasi run01 u3 25
```

**U4 — satu event tidak valid (`X01`, `file_id` tidak terdaftar) + satu event valid (`V01`):**
```bash
npm run produsen -- invalid --run run01 --id X01
npm run produsen -- kirim --run run01 --ids V01-V01
# tunggu worker memproses keduanya, lalu:
npm run uji -- verifikasi run01 u4 26
```

**Ekspor ledger** (dijalankan sekali di akhir seluruh rangkaian U1–U4, bukti hitung mentah):
```bash
npm run uji -- ledger run01
```

Setiap `verifikasi` dengan angka ekspektasi akan **menunggu (polling) sampai
60 detik** sebelum menyerah — lihat kebijakan lengkap di §10. Jalankan worker
dengan output diarahkan ke file bukti supaya log ikut tersimpan sebagai
lampiran, bukan hanya tampil di layar:
```bash
npm run worker > bukti/run01-log-worker.txt 2>&1     # pertama kali
npm run worker >> bukti/run01-log-worker.txt 2>&1     # setiap dinyalakan ulang (append)
```

Hasil pengujian aktual (dijalankan 23 September 2026, di infrastruktur
mandiri ini) ada di [`bukti/`](bukti/) dan direkap di
[`laporan/LAPORAN.md`](laporan/LAPORAN.md) §5.

## 7. Bagian yang dipakai ulang dari lab SIMPEL

Proyek ini dikembangkan dari lab praktik `simpel-lab` (RabbitMQ + PostgreSQL
di lab MP-06 s.d. MP-08), kemudian dipisah menjadi folder mandiri. Pola-pola
berikut dipakai ulang (sebagian diadaptasi, bukan diimpor langsung, karena
proyek ini tidak lagi berbagi repositori):

| Komponen | Sumber pola | Status di proyek ini |
|---|---|---|
| `openPublisher()` — confirm channel, publish mandatory+persistent, deteksi `return`, penutupan bersih | `layanan/messaging.js` (simpel-lab) | Divendorkan ke [`src/broker.js`](src/broker.js); logika confirm/mandatory/return tidak diubah, hanya kunci pending-publish disesuaikan ke `event_id` |
| Pola `alternateExchange` untuk pesan tak ter-route | `layanan/messaging.js`, `tools/kasus.js` (simpel-lab) | Pola dipakai ulang, exchange baru khusus kasus ini |
| Pola `deadLetterExchange`/`deadLetterRoutingKey` untuk jalur penolakan/DLQ | `layanan/messaging.js`, `lab/lab4-routing` (simpel-lab) | Pola dipakai ulang, exchange/queue baru |
| Idiom graceful shutdown (cancel consumer → tunggu pekerjaan aktif selesai → tutup koneksi) | `tools/kasus.js worker()`, `tools/routing.js worker` (simpel-lab) | Pola dipakai ulang, disesuaikan untuk satu worker |
| `INSERT ... ON CONFLICT DO NOTHING RETURNING` untuk idempotensi atomik | `layanan/alur.js` (inbox pattern MP-08, simpel-lab) | Pola dipakai ulang, tabel & kolom baru |
| Klien RabbitMQ Management API (snapshot queue) | `tools/operasi.js` (simpel-lab) | Pola disederhanakan menjadi [`src/manajemen.js`](src/manajemen.js) |
| Pemisahan skrip "kirim/verifikasi" vs proses worker jangka panjang | `tools/kasus.js` (Lab 7, simpel-lab) | Pola dipakai ulang untuk `uji/skenario.js` vs `src/pekerja.js` |
| Kontrak event (`event_id`/`event_type`/`occurred_at`/`payload`), validasi skema, skema tabel `file_*`, CLI producer/consumer, fixture berkas lokal + pemrosesan `word_count` sungguhan, docker-compose mandiri | — | **Baru**, spesifik proyek ini |

## 8. Batasan prototipe

- **Satu worker** dijalankan pada pengujian ini sesuai arahan kasus W
  ("Satu worker cukup"); kode sudah kompatibel dengan >1 worker (prefetch=1,
  efek bisnis idempoten) tapi belum diuji dengan >1 consumer paralel — itu
  pengembangan opsional.
- **Fixture berkas bersifat sintetis dan tetap.** Tiga berkas teks di
  `fixtures/` dibuat manual untuk proyek ini (bukan hasil unggahan
  sungguhan), dan hanya `operation: "word_count"` yang didukung saat ini —
  tidak ada integrasi penyimpanan berkas pihak ketiga (S3, dsb.) maupun
  operasi lain (mis. OCR, konversi format).
- **Celah acceptance vs publish**: producer di sini adalah CLI yang langsung
  mem-publish (tanpa lapis API/HTTP dan tanpa transactional outbox). Antara
  "permintaan diterima" dan "pesan benar-benar ter-publish" ada jendela
  kegagalan yang tidak dijamin oleh prototipe ini (mis. proses producer mati
  tepat setelah validasi tapi sebelum publish) — celah ini hanya bisa ditutup
  dengan pola outbox (seperti dilatih di `layanan/gateway`, MP-08 simpel-lab),
  yang sengaja tidak direplikasi di sini agar cakupan proyek tetap pada pola
  W dan pemulihan consumer.
- **Broker & database standalone single-node** (bukan cluster/HA), tanpa TLS/mTLS.
- **Tidak ada klaim "tanpa kehilangan pesan" atau "exactly-once" secara
  menyeluruh.** Pengujian U1–U4 membuktikan idempotensi consumer-side dan
  pemulihan setelah downtime consumer TERBATAS pada skenario yang diuji
  (gangguan proses consumer, bukan kegagalan broker/database itu sendiri).

## 9. Cara menghitung hasil

Kasus ini pola **W** (Work Queue, satu worker), jadi setiap event hanya
diproses **satu kali oleh satu worker** — bukan pola **P** (Pub-Sub) yang
membagi salinan event yang sama ke banyak subscriber independen.

- Setelah **U1** (20 event valid `N01`–`N20`): **20** hasil unik.
- Setelah **U2** (worker mati, 5 event baru `G01`–`G05`, lalu pulih): **25**
  hasil (20 + 5) — bertambah, bukan bertambah dua kali, karena `G01`–`G05`
  adalah event **baru**, bukan redelivery dari U1.
- Setelah **U3** (replay `N01`–`N05` dengan `event_id`+payload semula):
  **tetap 25** — replay tidak menambah baris, hanya membuktikan idempotensi.
- Setelah **U4** (`X01` invalid + `V01` valid): **26** — `V01` menambah satu
  baris; **`X01` TIDAK dihitung sebagai hasil bisnis** (masuk tabel
  `file_rejections`, bukan `file_results`).

**`job_id` vs `event_id`.** Dedup selalu berbasis `event_id` (primary key
tabel `file_results`), **bukan** `job_id`. `job_id` cuma label bisnis job —
pada run ini seluruh 26 `job_id` event valid berbeda satu sama lain
(`JOB-N01`..`JOB-N20`, `JOB-G01`..`JOB-G05`, `JOB-V01`), sementara `file_id`
yang sama (`sample-a`/`sample-b`/`sample-c`) dipakai berulang oleh banyak
`job_id` — dibuktikan lewat `bukti/run01-ledger.json` (lihat §10).

**Kalau proyek ini dikembangkan ke pola P** (dua subscriber independen,
masing-masing dengan queue sendiri yang di-bind ke exchange yang sama):
target menjadi **26 hasil per subscriber** (dedup per-subscriber, bukan
global), atau **52 catatan pemrosesan** bila dihitung gabungan kedua
subscriber. Banyaknya *attempt*/*delivery* (termasuk redelivery yang di-nack
atau di-retry) boleh lebih besar dari angka ini — yang dihitung adalah
**baris efek bisnis final**, bukan jumlah pengiriman.

**Dua worker pada satu queue kerja (pengembangan opsional §8) membagi
pekerjaan** — jumlah hasil TETAP 26 total (bukan 26 per worker), karena
keduanya bersaing mengambil pesan dari queue yang sama; idempotensi
berbasis `event_id` memastikan tidak ada duplikasi efek bisnis lintas-worker
walau kedua worker bisa saja sama-sama mengambil percobaan pengiriman yang
sama pada kasus redelivery.

**Bukti yang bisa dihitung ulang:** tabel `file_results` adalah
*ledger*/*receipt* per event (satu baris = satu event_id yang sudah tuntas
diproses) — hitungannya bisa diverifikasi ulang kapan saja lewat
`npm run uji -- ledger run01` atau query SQL langsung (§5), bukan
sekadar diklaim di laporan.

## 10. Bukti yang dicatat

Setiap pengujian di `bukti/run01-*.json` mencakup:
- **Daftar ID input** yang dikirim (`bukti/run01-terkirim.json`, payload asli
  — termasuk `job_id`/`file_id`/`operation` — tersimpan untuk keperluan
  replay U3).
- **Hasil query/ekspor data** — daftar `event_id` yang sudah punya efek
  bisnis (`idHasilBisnis`), dan ekspor mentah penuh termasuk `job_id`,
  `file_id`, `ukuran_bytes`, `jumlah_kata` per baris (`bukti/run01-ledger.json`).
- **Status `ready`/`unacked`/`consumers`** ketiga queue (`kerja`, `penolakan`,
  `tanpaRute`) yang benar-benar teramati lewat RabbitMQ Management API saat
  verifikasi dijalankan — **bukan asumsi**.
- **Perintah uji** yang dipakai (`perintahUji` di tiap file bukti).
- **Log worker** (`bukti/run01-log-worker.txt`) sebagai bukti pendukung
  console log, mendampingi bukti utama di database.

**Batas waktu tunggu:** setiap `verifikasi <run> <label> <ekspektasi>`
menunggu (polling tiap 2 detik) **maksimum 60 detik** sampai jumlah hasil
mencapai ekspektasi sebelum menyerah (`BATAS_TUNGGU_MS` di
[`uji/skenario.js`](uji/skenario.js)). **Queue yang kosong saja tidak
dianggap bukti keberhasilan** — yang dianggap bukti adalah jumlah baris
ledger yang cocok dengan himpunan ID input. Bila target belum tercapai
dalam batas waktu, itu dicatat apa adanya di field `tercapaiDalamWaktu:
false` beserta `waktuTungguMs` aktual, tidak disembunyikan.

Hasil nyata run01 (semua tercapai jauh di bawah batas 60 detik):

| Uji | Ekspektasi | Waktu tunggu aktual | Tercapai? |
|---|---|---|---|
| U1 | 20 | 142 ms | ya |
| U2 | 25 | 221 ms | ya |
| U3 | 25 | 176 ms | ya |
| U4 | 26 | 133 ms | ya |

## 11. Diagram satu halaman

[`diagram/topologi.svg`](diagram/topologi.svg) — peran layanan (producer/
worker), exchange, routing key, queue, penyimpanan PostgreSQL, dan alur
acknowledgment dalam satu gambar. Buka langsung di browser, atau konversi ke
PDF/PNG bila format itu yang diminta pengajar.

## 12. Berkas yang dikumpulkan

| Yang diminta | Lokasi di repo ini |
|---|---|
| README (identitas, kode kasus, kontribusi, prasyarat, konfigurasi, urutan perintah, cara memeriksa hasil) | Berkas ini — §0, §5 |
| Kode producer & consumer, konfigurasi topology, skema penyimpanan, fixture berkas, data uji sintetis | [`src/`](src/), [`db/skema.sql`](db/skema.sql), [`fixtures/`](fixtures/); data uji dibuat deterministik oleh `src/produsen.js` (lihat §2) |
| Diagram satu halaman | [`diagram/topologi.svg`](diagram/topologi.svg) |
| Laporan ringkas (masalah & cakupan, alasan desain, hasil U1–U4, satu diagnosis gangguan, batas prototipe, kontribusi) | [`laporan/LAPORAN.md`](laporan/LAPORAN.md) |
| Folder bukti (input, output persisten, perbandingan ID, log, observasi broker, per uji) | [`bukti/`](bukti/) |

Seluruh isi tabel di atas ada dalam satu folder proyek ini
(`a04-w-pengolahan-berkas/`) — cukup diarsipkan (mis. `.zip`) atau dibagikan
lewat tautan repositori yang dapat diakses pengajar; repo publik tidak
diwajibkan.
