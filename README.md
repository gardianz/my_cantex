# Cantex Autoswap Bot

Bot autoswap multi-account untuk Cantex yang dibangun di atas SDK lokal pada `cantex_sdk-4.0`.

## Ringkasan

Fitur utama:

- Multi-account
- Strategi swap `1`, `2`, `3`, dan `4`
- Validasi balance, minimum protocol, dan fee sebelum submit swap
- Reserve `CC` berbasis `reserve_fee` per account
- Optimasi route `direct` vs `1-hop`
- `1 swap sukses = 1 round selesai`
- Bot selalu berjalan dalam mode 24 jam berbasis UTC
- Saat start bot selalu meminta pilihan mode startup `1-4`
- Monitor Telegram gabungan dalam 1 pesan
- Dashboard terminal live berbentuk tabel per account
- Best-effort fetch activity user dari endpoint web Cantex
- Konfirmasi swap via WebSocket (`swap_and_confirm`) dari SDK 4.0

## Struktur File Penting

- Config contoh: `config/accounts.example.toml`
- Config utama: `config/accounts.toml`
- Entry point: `run_bot.py`
- Core bot: `src/autoswap_bot/bot.py`

## Kebutuhan

- Python 3.11 atau lebih baru
- `pip`
- Internet yang stabil
- Private key / credential account Cantex

## Quick Start

Alur umum:

1. Buat virtual environment
2. Install dependency
3. Copy `.env.example` menjadi `.env`
4. Copy config contoh
5. Isi credential dan pengaturan
6. Jalankan bot

## Menjalankan di Windows

Masuk ke folder project lalu jalankan:

```powershell
py -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
Copy-Item .env.example .env
Copy-Item config\accounts.example.toml config\accounts.toml
```

Isi secret di `.env`, isi pengaturan di `config\accounts.toml`, lalu jalankan:

```powershell
py run_bot.py --config config\accounts.toml
```

Contoh isi `.env`:

```powershell
TELEGRAM_BOT_TOKEN="isi_token_bot"
TELEGRAM_CHAT_ID="isi_chat_id"
CANTEX_OPERATOR_KEY_1="isi_operator_key_1"
CANTEX_TRADING_KEY_1="isi_trading_key_1"
CANTEX_OPERATOR_KEY_2="isi_operator_key_2"
CANTEX_TRADING_KEY_2="isi_trading_key_2"
```

## Menjalankan di VPS Ubuntu

Masuk ke folder project lalu jalankan:

```bash
python3 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
cp .env.example .env
cp config/accounts.example.toml config/accounts.toml
```

Isi secret di `.env`, isi pengaturan di `config/accounts.toml`, lalu jalankan:

```bash
python run_bot.py --config config/accounts.toml
```

Penting:

- Di Linux gunakan slash `/`, bukan `\`
- Command ini salah di Linux: `python run_bot.py --config config\accounts.toml`
- Command yang benar di Linux: `python run_bot.py --config config/accounts.toml`

Contoh isi `.env`:

```bash
TELEGRAM_BOT_TOKEN="isi_token_bot"
TELEGRAM_CHAT_ID="isi_chat_id"
CANTEX_OPERATOR_KEY_1="isi_operator_key_1"
CANTEX_TRADING_KEY_1="isi_trading_key_1"
CANTEX_OPERATOR_KEY_2="isi_operator_key_2"
CANTEX_TRADING_KEY_2="isi_trading_key_2"
```

Bot akan otomatis membaca file `.env` dari root project saat start.

## Format Config

Lihat contoh lengkap di `config/accounts.example.toml`.

Struktur dasarnya:

```toml
[settings]
swap_delay_seconds = { min = 20.0, max = 100.0 }
max_network_fee_cc_per_execution = "0.12"
network_fee_poll_seconds = { min = 20.0, max = 40.0 }
full_24h_auto_restart = true
telegram_enabled = false
terminal_dashboard_enabled = true
terminal_dashboard_logs_limit = 20
terminal_dashboard_min_interval_seconds = 0.25
default_continue_on_low_balance = true
max_retries = 3
retry_base_delay = 5.0

[defaults]
strategy = "3"
rounds = { min = 70, max = 72 }
amounts = { CC = { min = "11", max = "13" }, USDCx = { min = "1.5", max = "1.7" }, CBTC = { min = "0.000023", max = "0.000025" } }
reserve_fee = "5"

[[accounts]]
name = "wallet-1"
enabled = true
operator_key = "env:CANTEX_OPERATOR_KEY_1"
trading_key = "env:CANTEX_TRADING_KEY_1"
proxy_label = "No proxy"
auto_create_intent_account = true
```

## Penjelasan Setting Penting

### `[settings]`

- `swap_delay_seconds`
  - Delay antar swap pada mode normal
  - Bisa angka tetap atau range `{ min, max }`

- `max_network_fee_cc_per_execution`
  - Batas maksimum network fee untuk setiap transaksi swap / hop dalam satuan `CC`
  - Bot akan cek quote terlebih dahulu sebelum submit transaksi
  - Jika ada hop yang network fee quote-nya lebih tinggi dari batas ini, transaksi tidak dikirim
  - Bot akan menunggu sesuai `network_fee_poll_seconds`, lalu quote ulang sampai fee turun
  - Jika saat menunggu muncul quote error sementara seperti `HTTP 502`, bot tidak dianggap gagal; bot tetap hidup dan akan quote ulang lagi
  - Setting ini hanya membatasi network fee, bukan swap fee admin/liquidity
  - Aturan ini berlaku untuk swap normal, recovery, dan refill
  - Cantex saat ini memberi `3x free fee swap` per account per hari UTC
  - Bot otomatis mencoba memakai jatah ini mulai `01:00 UTC`
  - Hanya hop pertama yang benar-benar memakai jatah free swap harian boleh bypass batas fee ini
  - Bot akan mencoba menyinkronkan pemakaian jatah ini dari endpoint history trading lebih dulu
  - Jika history hari ini sudah menunjukkan `3` swap, bot akan menganggap jatah free swap hari itu sudah habis
  - State lokal tetap disimpan sebagai fallback agar restart bot tidak mengulang jatah yang sudah terpakai walaupun endpoint history sedang tidak tersedia
  - Contoh:
    - jika nilai setting `0.12`
    - lalu quote menunjukkan network fee `0.15 CC`
    - maka bot tidak swap, tetapi menunggu dan cek ulang

- `network_fee_poll_seconds`
  - Interval tunggu antar pengecekan ulang fee saat fee masih di atas batas
  - Bisa angka tetap atau range `{ min, max }`
  - Jika memakai range, bot akan memilih jeda random baru pada setiap percobaan
  - Contoh:
    - `30` berarti bot akan cek ulang setiap 30 detik
    - `{ min = 20.0, max = 40.0 }` berarti bot akan cek ulang dengan jeda acak antara 20 sampai 40 detik

- `full_24h_mode`
  - Secara praktik bot sekarang selalu memakai mode 24 jam berbasis UTC
  - Setting ini dipertahankan untuk kompatibilitas config lama

- `full_24h_startup_mode`
  - Dipertahankan untuk kompatibilitas config lama
  - Sumber utama perilaku startup sekarang adalah prompt pilihan mode saat bot dijalankan

- `full_24h_auto_restart`
  - Jika `true`, setelah sesi harian selesai bot akan lanjut ke hari UTC berikutnya

- `full_24h_min_gap_minutes`
  - Jarak minimum antar jadwal swap pada mode 24 jam

- `random_seed`
  - Isi angka jika ingin pola random bisa direproduksi saat debugging

- `telegram_enabled`
  - Jika `true`, bot akan mengirim 1 pesan Telegram gabungan untuk semua account

- `telegram_chat_id`
  - Disarankan diisi lewat `.env` dengan `env:TELEGRAM_CHAT_ID`

- `terminal_dashboard_enabled`
  - Jika `true` dan bot dijalankan di terminal interaktif, output console akan berubah menjadi dashboard tabel live per account
  - Log mentah biasa disembunyikan agar tampilan tidak campur

- `terminal_dashboard_logs_limit`
  - Jumlah log terakhir yang ditampilkan di bagian bawah dashboard terminal

- `terminal_dashboard_min_interval_seconds`
  - Jeda minimum refresh dashboard terminal agar layar tidak terlalu sering redraw

- `default_continue_on_low_balance`
  - Default perilaku untuk semua account jika balance kurang

- `max_retries`
  - Jumlah retry swap hop
  - Jika seluruh retry gagal, round di-skip dan bot lanjut ke round berikutnya

- `retry_base_delay`
  - Delay dasar antar retry

### `[defaults]`

Dipakai sebagai default untuk semua account aktif, kecuali di-override pada `[[accounts]]`.

- `strategy`
- `rounds`
- `amounts`
- `reserve_fee`
- `reserve_kritis`

`reserve_fee` adalah reserve `CC` utama per account untuk semua strategi.
`reserve_kritis` hanya wajib diisi jika account memakai strategi `4`.

Contoh:

```toml
[defaults]
strategy = "3"
rounds = { min = 5, max = 7 }
amounts = { CC = { min = "9", max = "12" }, USDCx = { min = "8", max = "11" }, CBTC = { min = "0.0005", max = "0.0010" } }
reserve_fee = "5"
```

### `[[accounts]]`

Setiap account minimal berisi:

- `name`
- `enabled`
- `operator_key`
- `trading_key`

Setting yang sering dipakai per account:

- `strategy`
- `rounds`
- `amounts`
- `reserve_fee`
- `reserve_kritis`
- `allow_continue_on_low_balance`
- `auto_create_intent_account`
- `proxy_label`

## Override Default

Aturannya:

- Jika field tidak ada di `[[accounts]]`, bot pakai nilai dari `[defaults]` atau `[settings]`
- Jika field ada di `[[accounts]]`, nilai account akan override default

Contoh:

```toml
[settings]
default_continue_on_low_balance = true

[[accounts]]
name = "wallet-2"
enabled = true
operator_key = "env:CANTEX_OPERATOR_KEY_2"
trading_key = "env:CANTEX_TRADING_KEY_2"
allow_continue_on_low_balance = false
```

Artinya account `wallet-2` tetap memakai `false`, walaupun default global `true`.

## Strategi

`strategy` sekarang menerima nilai `1`, `2`, `3`, atau `4`:

1. `CC -> USDCx`
2. `CC -> CBTC`
3. `CC -> USDCx -> CBTC`
4. `CC -> USDCx -> CBTC`

Catatan:

- `rounds` adalah jumlah swap sukses yang ingin dicapai
- `1 swap sukses = 1 round selesai`
- Strategi `1` akan memprioritaskan refill token luar strategi ke `CC`, lalu `CC -> USDCx`, lalu unwind `USDCx -> CC` saat `CC` tidak cukup
- Strategi `2` akan memprioritaskan refill token luar strategi ke `CC`, lalu `CC -> CBTC`, lalu unwind `CBTC -> CC` saat `CC` tidak cukup
- Strategi `3` memakai static round robin dinamis:
  - selama `CC` masih cukup, bot bergantian `CC -> USDCx` lalu `CC -> CBTC`
  - saat `CC` tidak cukup untuk swap keluar, bot masuk fase recycle: `USDCx -> CBTC (50%)`, `CBTC -> USDCx (50%)`, `CBTC -> CC (max)`, `USDCx -> CC (max)`
- Strategi `4` memakai mode reserve:
  - bot swap `CC -> USDCx` memakai saldo maksimum sambil menyisakan `reserve_fee`
  - setelah itu bot bolak-balik `USDCx -> CBTC` dan `CBTC -> USDCx` dengan `amount max`
  - jika `CC <= reserve_kritis`, bot masuk fase recovery dan mengosongkan `USDCx` serta `CBTC` kembali ke `CC`
  - setelah saldo token luar habis, flow kembali ke langkah `CC -> USDCx`
- Langkah strategi hanya maju jika swap pada langkah saat ini benar-benar sukses
- Constraint sementara seperti fee tinggi, minimum ticket protocol, atau source token belum cukup tidak mengurangi `rounds`
- Alias lama `strategy = "7"` masih diterima dan dipetakan ke strategi `3` agar config lama tetap jalan

Contoh:

- `strategy = "1"` akan terus mengulang flow `refill luar strategi -> CC -> USDCx -> USDCx -> CC`
- `strategy = "2"` akan terus mengulang flow `refill luar strategi -> CC -> CBTC -> CBTC -> CC`
- `strategy = "3"` akan terus memakai round robin dinamis antara fase spend dan fase recycle sampai target `rounds` sukses terpenuhi
- `strategy = "4"` akan mengulang flow `CC -> USDCx (sisakan reserve_fee) -> USDCx <-> CBTC (max) -> recovery ke CC saat reserve_kritis tersentuh`

Contoh config untuk strategi `4`:

```toml
[defaults]
strategy = "4"
rounds = { min = 70, max = 72 }
reserve_fee = "5"
reserve_kritis = "1"
```

## Amount dan Rounds

`amounts` dipakai berdasarkan aset yang sedang menjadi token `sell`.

Catatan penting:

- Strategi `1`, `2`, dan `3` memakai `amounts`
- Strategi `4` tidak memakai `amounts`; strategi ini memakai saldo maksimum berdasarkan `reserve_fee` dan `reserve_kritis`

Contoh nilai tetap:

```toml
amounts = { CC = "10", USDCx = "10", CBTC = "0.001" }
```

Contoh random range:

```toml
amounts = { CC = { min = "8", max = "12" }, USDCx = { min = "8", max = "12" }, CBTC = { min = "0.0004", max = "0.0008" } }
rounds = { min = 5, max = 8 }
```

Artinya:

- nominal swap diacak saat langkah itu benar-benar akan dieksekusi
- token source aktif menentukan range amount yang dipakai
- `rounds` diacak sekali di awal run account

## Perilaku Saat Balance dan Reserve

- `reserve_fee` membatasi saat source token adalah `CC`
- Jika step aktif adalah `CC -> token lain`, bot hanya boleh swap selama balance `CC` masih di atas `reserve_fee`
- Jika balance `CC <= reserve_fee`, bot tidak akan lagi memakai `CC` sebagai source token untuk swap keluar
- Kondisi ini bukan berarti strategi selesai
- Step lain seperti `USDCx -> CBTC`, `CBTC -> CC`, atau `USDCx -> CC` tetap boleh berjalan walaupun `CC <= reserve_fee`
- Pada strategi `4`, `reserve_fee` adalah target saldo `CC` yang disisakan saat langkah `CC -> USDCx`
- Pada strategi `4`, jika `CC <= reserve_kritis`, bot akan memaksa recovery `USDCx` dan `CBTC` ke `CC` sampai saldo token luar habis

Catatan:

- Bot tidak menganggap balance kurang sebagai kondisi selesai
- Jika source token aktif belum memenuhi syarat config user atau protocol, bot hanya menunda attempt dan mencoba lagi pada evaluasi berikutnya
- `allow_continue_on_low_balance` saat ini sebaiknya dianggap sebagai setting kompatibilitas lama, bukan penentu selesai strategi

## Mode 24 Jam

Bot sekarang selalu berjalan dalam mode 24 jam.

Saat bot dijalankan, akan muncul 4 pilihan mode startup:

1. `Mode hanya ambil free swap`
2. `Mode ambil free swap lalu lanjut swap sesuai batas swap dan fee swap yang ditentukan`
3. `Mode swap sesuai batas swap dan fee swap yang ditentukan`
4. `Mode swap sesuai jam plan dan batas fee yang ditentukan`

Arti mode:

- Mode `1`
  - Bot hanya memakai jatah free swap harian
  - Setelah jatah free swap hari itu habis, bot menunggu hari UTC berikutnya jika `full_24h_auto_restart = true`
- Mode `2`
  - Bot memprioritaskan free swap harian lebih dulu
  - Hanya hop free swap yang benar-benar memakai jatah harian boleh bypass fee cap
  - Setelah jatah free swap habis, bot lanjut swap normal dengan fee cap
- Mode `3`
  - Bot langsung swap normal
  - Semua swap tetap patuh pada fee cap
- Mode `4`
  - Bot memakai plan jadwal random dalam window harian UTC
  - Fee cap tetap berlaku

Perilaku umum mode 24 jam:

- semua jadwal memakai acuan UTC
- pada mode `planned`, target sesi adalah selesai sebelum `00:00 UTC`
- pada mode non-plan, bot terus mencoba sampai quota `rounds` sukses terpenuhi; jika quota selesai lebih cepat dan `full_24h_auto_restart = true`, bot akan idle sampai `00:00 UTC` berikutnya
- jika `full_24h_auto_restart = true`, sesi berikutnya dimulai lagi untuk hari UTC berikutnya
- jatah `3x free fee swap` harian per account akan reset saat hari UTC berganti, tetapi baru boleh dipakai mulai `01:00 UTC`
- jadi bot tidak akan memakai free swap tepat setelah `00:00 UTC`, melainkan menunggu `+1 jam`
- `max_network_fee_cc_per_execution` berlaku untuk semua swap normal, recovery, refill, dan hop lanjutan
- pengecualian hanya untuk hop free swap yang benar-benar memakai jatah harian
- jika fee terlalu tinggi, bot akan retry quote pada slot round itu
- jika saat menunggu fee turun muncul quote error sementara seperti `HTTP 502`, bot tetap hidup dan akan mencoba quote ulang lagi
- retry fee dilakukan sampai tersisa `30 detik` menuju jadwal round berikutnya
- jika sampai batas itu fee tidak turun, slot round saat itu dilewati dan bot lanjut ke slot berikutnya
- account tidak dianggap gagal hanya karena fee sedang tinggi

Catatan penting:

- Saat mode 24 jam aktif, account dijalankan paralel di level account
- Di dalam 1 account, transaksi tetap serial
- Saat mode 24 jam aktif, bot selalu memaksa perilaku recovery / continue semampunya
- Jadi `allow_continue_on_low_balance = false` tidak dipakai sebagai stop keras selama mode 24 jam aktif
- Pada `full_24h_startup_mode = "direct"`, bot memakai `swap_delay_seconds` hanya setelah swap yang berhasil
- Di mode normal, bot akan terus menunggu fee turun karena tidak ada jadwal round berikutnya yang menjadi batas deadline

Contoh flow fee cap di mode 24 jam:

1. Round saat ini dijadwalkan pukul `10:00:00 UTC`
2. Round berikutnya dijadwalkan pukul `10:05:00 UTC`
3. Deadline retry fee untuk round saat ini adalah `10:04:30 UTC`
4. Jika sampai `10:04:30 UTC` fee masih di atas `max_network_fee_cc_per_execution`, slot round saat ini dilewati
5. Bot lanjut ke round berikutnya dan tetap hidup

## Retry dan Attempt

Jika swap hop gagal:

1. Bot retry sampai batas `max_retries`
2. Bot menunggu sesuai `retry_base_delay`
3. Jika tetap gagal, attempt saat itu dianggap gagal
4. Bot tetap hidup dan akan mencoba lagi pada evaluasi berikutnya
5. Round baru dianggap selesai jika ada 1 swap sukses

Jadi bot tidak langsung berhenti untuk account hanya karena 1 hop swap gagal, dan `rounds` tidak berkurang hanya karena retry habis.

## Recovery dan Minimum Ticket

- Bot membedakan:
  - minimum amount dari config user
  - minimum ticket size dari protocol / web
- Jika nominal saat itu berada di bawah minimum protocol, bot tidak menganggap strategi selesai
- Bot akan mencoba menyesuaikan amount jika masih memungkinkan
- Jika tetap tidak memenuhi minimum protocol, attempt saat itu dilewati dengan log yang jelas
- Bot tetap hidup dan mencoba lagi sampai round sukses terkumpul sesuai target
- Dust balance kecil tidak dipaksa swap jika akan menghasilkan transaksi invalid
- Jika route optimizer menurunkan amount sampai di bawah minimum config user, bot tidak submit transaksi itu

## Telegram Monitor

Jika `telegram_enabled = true`, bot membuat 1 pesan Telegram gabungan untuk semua account dan terus mengedit pesan yang sama.

Isi pesan gabungan per account meliputi:

- status account
- balance `CC`, `USDCX`, `CBTC`
- statistik 24h
  - `Tx [ok|fail]`
  - `Free swap`
  - `Swap`
  - `Volume`
  - `Fee Spent`
- rewards
  - `Kemarin`
  - `Minggu ini`
- statistik sejak bot start
  - `Tx [ok|fail]`
  - `Swap`
  - `Volume`
  - `Fee Spent`

Contoh config:

```toml
[settings]
telegram_enabled = true
telegram_bot_token = "env:TELEGRAM_BOT_TOKEN"
telegram_chat_id = "env:TELEGRAM_CHAT_ID"
telegram_update_min_interval_seconds = 5
telegram_latest_logs_limit = 6
```

Catatan:

- Saat startup, monitor Telegram tidak lagi mengirim 1 pesan per akun
- Bot akan mempertahankan 1 pesan gabungan dan mengeditnya secara berkala

## Output dan Ringkasan

Di akhir run, bot menampilkan ringkasan per account, termasuk:

- status
- putaran selesai (`swap sukses`)
- `skipped_rounds`
- jumlah tx swap
- estimasi network fee
- network fee terpakai
- swap fee terpakai
- balance akhir
- `stop_reason` jika ada

Catatan:

- `completed_rounds` adalah jumlah swap sukses
- `skipped_rounds` terutama menunjukkan jumlah attempt / slot yang dilewati, bukan jumlah round yang dianggap selesai

## Sumber Activity

Saat inspeksi frontend Cantex secara langsung, data activity akun dan history trading yang dipakai bot mengikuti endpoint berikut:

- `https://api.cantex.io/v1/account/reward_activity`
- `https://api.cantex.io/v1/history/trading`

Bot memakai endpoint ini untuk:

- `24h swaps`
- `24h volume`
- `CC rebates` seperti `Yesterday` dan `This Week`
- sinkronisasi history trading harian untuk jatah `3x free fee swap`

Jika data activity belum tersedia atau belum terindeks, bot tetap lanjut dengan log yang aman.

## File State Lokal

Bot menyimpan state lokal di folder `config/`:

- `.autoswap_bot_runtime_state.json`
  - menyimpan jatah `3x free fee swap` harian per account
  - dipakai sebagai fallback lokal dan cache sinkronisasi
  - saat tersedia, bot akan mencocokkan pemakaian free swap dengan endpoint history trading hari ini
  - reset harian mengikuti UTC
  - window pemakaian free swap tetap baru terbuka pada `01:00 UTC`
- `.autoswap_telegram_state.json`
  - menyimpan statistik pesan Telegram seperti tx ok/fail, swap, dan gas fee harian / lifetime

## Troubleshooting

### File config tidak ditemukan di Ubuntu

Gunakan:

```bash
python run_bot.py --config config/accounts.toml
```

Jangan gunakan:

```bash
python run_bot.py --config config\accounts.toml
```

### Telegram token belum di-set

Jika `telegram_enabled = false`, token Telegram tidak wajib diisi.

Jika `telegram_enabled = true`, pastikan:

- `telegram_bot_token` valid
- `telegram_chat_id` valid
- environment variable sudah di-set jika memakai format `env:...`

### Menghentikan bot secara manual

Saat bot sedang berjalan, tekan `Ctrl + C`.

Bot akan menampilkan:

```text
berhenti? (y/n)
```

Arti pilihan:

- `y`: bot berhenti dengan rapi
- `n`: bot lanjut jalan

### Account nonaktif tetap minta private key

Account dengan `enabled = false` akan di-skip. Jika masih error, cek apakah field account aktif dan nonaktif tertukar saat mengedit config.
