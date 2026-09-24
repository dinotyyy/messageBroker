# Outline Presentasi — A04-W: Pengolahan Berkas

Target: 3 JP sesi sinkron Jumat 25 September (135 menit, dibagi seluruh
peserta). Alokasikan realistis 8–10 menit presentasi per kelompok + tanya
jawab. Outline ini siap dipindah ke slide (mis. lewat Artifact tipe Slides).

1. **Masalah & ruang lingkup** (1 slide)
   Kendala pengolahan berkas sinkron pada SIMPEL, batas prototipe (data
   sintetis, simulasi lokal) — dari `laporan/LAPORAN.md` §1.

2. **Arsitektur & topologi** (1–2 slide)
   Diagram producer → exchange `files` → queue `filejobs` → worker →
   PostgreSQL (README §3). Jelaskan kenapa direct exchange +
   alternate-exchange, kenapa pola W.

3. **Kontrak event & idempotensi** (1 slide)
   Contoh JSON event_id/event_type (`file.processing.requested`)/occurred_at/
   payload (`job_id`/`file_id`/`operation`). Jelaskan `file_id` hanya kunci
   lookup ke fixture lokal (tidak pernah path/URL bebas), dan bedanya
   `job_id` (label bisnis, boleh berulang pakai `file_id` yang sama) vs
   `event_id` (kunci dedup). Strategi `ON CONFLICT DO NOTHING` +
   ack-setelah-tersimpan (README §4).

4. **Demo / bukti pengujian U1–U4** (2 slide + demo langsung bila waktu cukup)
   Tabel evidence matrix dari `laporan/LAPORAN.md` §5 — tunjukkan angka
   nyata: 20 → 25 (setelah recovery) → tetap 25 (replay) → 26 (setelah X01
   ditolak + V01 selesai). Tunjukkan cuplikan
   `bukti/run01-u2-antrean-tertahan.json` sebagai bukti backlog nyata.

5. **Insiden & investigasi** (1 slide)
   Ringkas dari `laporan/LAPORAN.md` §6: gejala (worker tidak benar-benar
   berhenti saat U2), dua hipotesis (SIGINT tidak sampai vs PID salah di
   Windows/Git Bash), pembuktian lewat `tasklist`, perbaikan (`taskkill //F`
   dengan PID yang dicocokkan ke `workerId` di log).

6. **Batasan & pengembangan lanjut** (1 slide)
   Satu worker (opsional: worker kedua), tanpa outbox di producer, tanpa
   klaim zero-loss/exactly-once menyeluruh — dari `laporan/LAPORAN.md` §7.

7. **Reuse & kontribusi** (1 slide, opsional digabung dengan slide penutup)
   Komponen lab simpel-lab yang dipakai ulang/diadaptasi vs bagian baru —
   `laporan/LAPORAN.md` §8. Sebutkan juga bahwa proyek sengaja dipisah dari
   repo lab menjadi folder mandiri agar mudah dinilai/dijalankan independen.

**Catatan:** sudah dibuat sebagai `A04-W-Presentasi.pptx` (12 slide, di folder
ini) — lengkapi placeholder nama/NIP kelompok di slide judul sebelum dipakai.
