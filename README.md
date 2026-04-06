# Cantex Autoswap Bot

Bot autoswap multi-account untuk Cantex yang dibangun di atas SDK lokal pada `cantex_sdk-0.3.0`.

## Fitur

- Multi-account dengan mode `sequential` atau `concurrent`
- 9 strategi swap sesuai daftar kebutuhan
- Validasi balance, estimasi fee, dan reserve minimal `5 CC`
- Route optimizer yang membandingkan route langsung vs 1-hop intermediate
- Prompt interaktif saat balance tidak cukup
- Logging per account dan ringkasan akhir
- Best-effort fetch activity user dari endpoint web `v1/account/activity`

## Menjalankan

```bash
python -m pip install -r requirements.txt
copy config\accounts.example.toml config\accounts.toml
python run_bot.py --config config\accounts.toml
```

## Catatan Konfigurasi

- Gunakan `env:VAR_NAME` agar private key tidak disimpan langsung di file config.
- Gunakan `[defaults]` untuk menetapkan `strategy`, `rounds`, dan `amounts` bawaan ke semua account aktif.
- Account tetap bisa override nilai dari `[defaults]` secara per-account bila diperlukan.
- `full_24h_mode = true` akan mengaktifkan sesi scheduler 24 jam penuh berbasis UTC untuk semua account aktif.
- Saat `full_24h_mode = true`, `swap_delay_seconds` diabaikan sepenuhnya.
- `max_network_fee_cc_per_execution` membatasi network fee per round dalam satuan `CC`. Jika estimasi fee saat itu lebih tinggi, bot akan menunggu sampai fee turun.
- `network_fee_poll_seconds` mengatur interval cek ulang fee saat bot sedang menunggu network fee turun.
- `max_retries` dan `retry_base_delay` juga dipakai untuk retry swap hop di level bot. Jika seluruh retry gagal, round tersebut di-skip.
- `full_24h_min_gap_minutes` mengatur jarak minimum antar jadwal swap dalam mode 24 jam agar pola tidak terlalu rapat.
- `full_24h_auto_restart = true` akan memulai sesi 24 jam berikutnya otomatis setelah sesi sebelumnya selesai. Cocok untuk VPS yang ingin jalan terus.
- `full_24h_schedule_log_limit` membatasi berapa banyak item jadwal random yang ditampilkan di log agar tidak terlalu panjang.
- `random_seed` opsional untuk membuat randomisasi bisa direproduksi saat debugging. Hapus atau kosongkan jika ingin pola benar-benar berubah tiap run.
- `telegram_enabled = true` akan mengaktifkan monitoring Telegram per account.
- `telegram_bot_token` menerima token bot Telegram, disarankan lewat `env:TELEGRAM_BOT_TOKEN`.
- `telegram_chat_id` adalah chat / group / channel ID tujuan monitor.
- `telegram_update_min_interval_seconds` membatasi frekuensi edit pesan agar tidak kena rate limit Telegram.
- `telegram_latest_logs_limit` menentukan jumlah log terbaru yang disimpan di kartu Telegram.
- Kegagalan swap hop sekarang akan dicoba ulang sampai batas `max_retries`. Jika masih gagal, round itu di-skip dan account lanjut ke round berikutnya.
- `strategy` menerima nilai `1-9`. Mapping strateginya:
- `1`: `CC -> USDCx`
- `2`: `USDCx -> CC`
- `3`: `CC -> CBTC`
- `4`: `CBTC -> CC`
- `5`: `USDCx -> CBTC`
- `6`: `CBTC -> USDCx`
- `7`: siklik `CC -> USDCx`, lalu `USDCx -> CBTC`, lalu `CBTC -> CC`
- `8`: siklik `USDCx -> CBTC`, lalu `CBTC -> CC`, lalu `CC -> USDCx`
- `9`: siklik `CBTC -> CC`, lalu `CC -> USDCx`, lalu `USDCx -> CBTC`
- `amounts` diisi per simbol aset dan nilainya dipakai saat aset itu menjadi token `sell`.
- `amounts` bisa diisi nilai tetap atau range acak `min/max`.
- Contoh: `amounts = { CC = "10", USDCx = "10", CBTC = "0.001" }`
- Contoh range: `amounts = { CC = { min = "8", max = "12" }, USDCx = { min = "8", max = "12" }, CBTC = { min = "0.0004", max = "0.0008" } }`
- Artinya:
- saat strategi perlu `CC` sebagai aset jual, bot akan mencoba swap `10 CC`
- saat strategi perlu `USDCx` sebagai aset jual, bot akan mencoba swap `10 USDCx`
- saat strategi perlu `CBTC` sebagai aset jual, bot akan mencoba swap `0.001 CBTC`
- Jika memakai range, bot akan mengambil nominal random dalam range itu pada setiap langkah swap.
- Untuk strategi `1-6`, `rounds` berarti jumlah pengulangan swap yang sama.
- Untuk strategi `7-9`, `rounds` berarti jumlah langkah swap total, bukan jumlah siklus penuh.
- `rounds` bisa diisi angka tetap atau range `{ min = x, max = y }`. Jika range dipakai, jumlah putaran diacak sekali di awal run account.
- Contoh:
- `strategy = "7"` dan `rounds = 3` akan menjalankan 1 siklus penuh
- `strategy = "7"` dan `rounds = 6` akan menjalankan 2 siklus penuh
- `strategy = "7"` dan `rounds = 5` akan menjalankan 5 langkah berurutan sesuai pola siklik
- Khusus `CC`, bot tidak akan memakai seluruh balance karena selalu menyisakan minimal `5 CC` sebagai reserve fee.
- Bila `amounts.CC` terlalu besar, bot akan menyesuaikan nominal aktual agar reserve `5 CC` tetap aman.
- Jika `allow_continue_on_low_balance = true`, bot akan mencoba lanjut semampunya dan dapat melakukan swap recovery dari aset sisa bila diperlukan.
- Jika `allow_continue_on_low_balance = false`, bot akan berhenti saat putaran tidak bisa dipenuhi dan tidak akan mencoba recovery otomatis.
- Saat prompt `balance kurang apakah anda akan tetap melanjutkan?(y/n/i)` muncul:
- `y` = lanjut semampunya dan boleh recovery aset sisa bila perlu
- `i` = tetap mulai jalan, tetapi begitu ada balance kurang di tengah proses bot langsung stop
- `n` = hentikan proses
- `route_mode = "auto"` akan membandingkan route langsung dengan route 1-hop intermediate antar token yang tersedia.
- `route_mode = "direct"` hanya memakai route langsung.
- Untuk wallet baru, `auto_create_intent_account = true` disarankan agar intent account swap dibuat otomatis jika belum ada.
- `swap_delay_seconds` bisa diisi angka tetap atau range `{ min = x, max = y }`. Jika range dipakai, delay antar transaksi akan diacak setiap kali eksekusi.
- `proxy_label` per account hanya untuk label tampilan monitor Telegram, misalnya `No proxy` atau `SG Residential`.
- Dalam mode 24 jam:
- bot membuat target penyelesaian harian sampai `00:00 UTC` berikutnya
- setiap account mendapat jadwal swap acak yang berbeda
- jadwal dibagi menyebar sepanjang 24 jam, bukan delay statis per swap
- bot menyisihkan buffer eksekusi di akhir window agar seluruh `rounds` tetap terjadwal selesai sebelum pergantian hari UTC
- setelah melewati `00:00 UTC`, sesi account berhenti otomatis
- jika bot dijalankan di tengah hari UTC, sesi pertama memakai sisa waktu sampai `00:00 UTC` terdekat
- jika `full_24h_auto_restart = true`, sesi berikutnya akan berjalan per hari UTC penuh, dari `00:00 UTC` ke `00:00 UTC` hari berikutnya

## Telegram Monitor

- Bot akan membuat 1 pesan yang diedit terus untuk setiap account aktif.
- Formatnya dibuat seperti kartu ringkas berisi:
- status account dan progres round
- uptime sesi
- balance utama
- total swap tx dan progres pair
- proxy label
- reward / volume / tx / rank bila data activity tersedia
- delta tx dan reward dari baseline sesi
- latest logs terbaru

Contoh aktivasi:

```toml
[settings]
telegram_enabled = true
telegram_bot_token = "env:TELEGRAM_BOT_TOKEN"
telegram_chat_id = "-1001234567890"
telegram_update_min_interval_seconds = 5
telegram_latest_logs_limit = 6
```

## Sumber Activity

Saat inspeksi frontend `https://www.cantex.io/app/activity`, route tanpa login diarahkan ke `/signup`, tetapi bundle frontend memuat referensi endpoint:

- `https://api.cantex.io/v1/account/activity`
- `https://api.cantex.io/v1/account/reward_activity`

Bot ini mencoba endpoint tersebut secara best-effort dan akan fallback dengan log yang aman bila respons tidak tersedia.
