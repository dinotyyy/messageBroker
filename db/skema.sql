-- Skema PostgreSQL kasus A04 (Pengolahan Berkas, pola Work Queue).
-- Dijalankan otomatis lewat docker-entrypoint-initdb.d saat volume Postgres
-- baru dibuat, dan lewat `npm run db:siapkan` (idempoten) untuk volume yang
-- sudah ada.

-- Hasil bisnis: hasil pemrosesan tiap job (operation "word_count" atas
-- berkas fixture lokal yang dirujuk file_id). event_id adalah kunci
-- idempotensi -- satu event_id hanya boleh menghasilkan satu baris efek
-- bisnis, meski job_id boleh berbeda untuk file_id yang sama. Pemeriksaan
-- duplikat + penulisan dilakukan atomik lewat satu statement
-- INSERT ... ON CONFLICT (event_id) DO NOTHING di src/pekerja.js.
CREATE TABLE IF NOT EXISTS file_results (
    event_id          TEXT PRIMARY KEY,
    job_id            TEXT        NOT NULL,
    file_id           TEXT        NOT NULL,
    operation         TEXT        NOT NULL,
    ukuran_bytes      BIGINT      NOT NULL,
    jumlah_kata       INTEGER     NOT NULL,
    durasi_proses_ms  INTEGER     NOT NULL,
    status            TEXT        NOT NULL DEFAULT 'selesai',
    worker_id         TEXT        NOT NULL,
    occurred_at       TIMESTAMPTZ NOT NULL,
    diproses_pada     TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload_asli      JSONB       NOT NULL
);
CREATE INDEX IF NOT EXISTS file_results_job_id_idx ON file_results(job_id);
CREATE INDEX IF NOT EXISTS file_results_file_id_idx ON file_results(file_id);

-- Bukti pendukung jalur penolakan (U4): payload yang gagal validasi kontrak
-- (termasuk file_id yang tidak terdaftar di fixture lokal). BUKAN efek
-- bisnis -- baris di sini tidak pernah menambah baris di tabel di atas.
-- Dilengkapi bukti jalur broker: pesan yang sama juga berakhir di queue
-- files.penolakan.q lewat dead-letter-exchange (lihat src/topologi.js).
CREATE TABLE IF NOT EXISTS file_rejections (
    id             SERIAL PRIMARY KEY,
    event_id       TEXT,
    alasan         TEXT        NOT NULL,
    payload_mentah JSONB,
    ditolak_pada   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS file_rejections_event_id_idx ON file_rejections(event_id);
