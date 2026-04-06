# Cantex Autoswap Bot

Bot autoswap multi-account untuk Cantex yang dibangun di atas SDK lokal pada `cantex_sdk-0.3.0`.

## Ringkasan

Fitur utama:

- Multi-account
- 9 strategi swap
- Validasi balance sebelum eksekusi
- Reserve minimal `5 CC` per account
- Optimasi route `direct` vs `1-hop`
- Retry swap hop dan skip round jika tetap gagal
- Mode 24 jam berbasis UTC
- Monitor Telegram berbentuk kartu
- Best-effort fetch activity user dari endpoint web Cantex

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
3. Copy config contoh
4. Isi credential dan pengaturan
5. Jalankan bot

## Menjalankan di Windows

Masuk ke folder project lalu jalankan:

```powershell
py -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
Copy-Item config\accounts.example.toml config\accounts.toml
```

Isi config di `config\accounts.toml`, lalu jalankan:

```powershell
py run_bot.py --config config\accounts.toml
```

Jika memakai environment variable untuk key:

```powershell
$env:CANTEX_OPERATOR_KEY_1="isi_operator_key"
$env:CANTEX_TRADING_KEY_1="isi_trading_key"
```

Jika ingin Telegram:

```powershell
$env:TELEGRAM_BOT_TOKEN="isi_token_bot"
```

## Menjalankan di VPS Ubuntu

Masuk ke folder project lalu jalankan:

```bash
python3 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
cp config/accounts.example.toml config/accounts.toml
```

Isi config di `config/accounts.toml`, lalu jalankan:

```bash
python run_bot.py --config config/accounts.toml
```

Penting:

- Di Linux gunakan slash `/`, bukan `\`
- Command ini salah di Linux: `python run_bot.py --config config\accounts.toml`
- Command yang benar di Linux: `python run_bot.py --config config/accounts.toml`

Jika memakai environment variable:

```bash
export CANTEX_OPERATOR_KEY_1="isi_operator_key"
export CANTEX_TRADING_KEY_1="isi_trading_key"
export TELEGRAM_BOT_TOKEN="isi_token_bot"
```

## Format Config

Lihat contoh lengkap di `config/accounts.example.toml`.

Struktur dasarnya:

```toml
[settings]
execution_mode = "sequential"
min_cc_reserve = "5"
swap_delay_seconds = { min = 20.0, max = 100.0 }
max_network_fee_cc_per_execution = "0.12"
network_fee_poll_seconds = 30
full_24h_mode = true
full_24h_auto_restart = true
telegram_enabled = false
default_continue_on_low_balance = true
max_retries = 3
retry_base_delay = 5.0

[defaults]
strategy = "7"
rounds = { min = 70, max = 72 }
amounts = { CC = { min = "11", max = "13" }, USDCx = { min = "1.5", max = "1.7" }, CBTC = { min = "0.000023", max = "0.000025" } }

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

- `execution_mode`
  - `sequential`: account diproses satu per satu
  - `concurrent`: beberapa account diproses bersamaan sesuai `max_concurrency`

- `max_concurrency`
  - Batas account paralel saat `execution_mode = "concurrent"`

- `min_cc_reserve`
  - Reserve minimum `CC` yang selalu disisakan untuk fee
  - Bot tidak akan menghabiskan seluruh `CC`

- `swap_delay_seconds`
  - Delay antar swap pada mode normal
  - Bisa angka tetap atau range `{ min, max }`

- `max_network_fee_cc_per_execution`
  - Batas network fee per round dalam satuan `CC`
  - Jika fee saat itu lebih tinggi dari batas, bot akan menunggu dan cek ulang quote

- `network_fee_poll_seconds`
  - Interval tunggu saat fee masih di atas batas

- `full_24h_mode`
  - Jika `true`, bot memakai scheduler harian berbasis UTC sampai `00:00 UTC` berikutnya

- `full_24h_auto_restart`
  - Jika `true`, setelah sesi harian selesai bot akan lanjut ke hari UTC berikutnya

- `full_24h_min_gap_minutes`
  - Jarak minimum antar jadwal swap pada mode 24 jam

- `random_seed`
  - Isi angka jika ingin pola random bisa direproduksi saat debugging

- `telegram_enabled`
  - Jika `true`, bot akan mengirim 1 kartu Telegram per account

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

Contoh:

```toml
[defaults]
strategy = "3"
rounds = { min = 5, max = 7 }
amounts = { CC = { min = "9", max = "12" }, USDCx = { min = "8", max = "11" }, CBTC = { min = "0.0005", max = "0.0010" } }
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

`strategy` menerima nilai `1-9`:

1. `CC -> USDCx`
2. `USDCx -> CC`
3. `CC -> CBTC`
4. `CBTC -> CC`
5. `USDCx -> CBTC`
6. `CBTC -> USDCx`
7. Siklik `CC -> USDCx -> CBTC -> CC`
8. Siklik `USDCx -> CBTC -> CC -> USDCx`
9. Siklik `CBTC -> CC -> USDCx -> CBTC`

Catatan:

- Untuk strategi `1-6`, `rounds` berarti jumlah pengulangan swap yang sama
- Untuk strategi `7-9`, `rounds` berarti jumlah langkah swap total, bukan jumlah siklus penuh

Contoh:

- `strategy = "7"` dan `rounds = 3` berarti 1 siklus penuh
- `strategy = "7"` dan `rounds = 5` berarti 5 langkah berurutan sesuai pola siklik

## Amount dan Rounds

`amounts` dipakai berdasarkan aset yang sedang menjadi token `sell`.

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

- nominal swap diacak pada setiap langkah
- `rounds` diacak sekali di awal run account

## Perilaku Saat Balance Kurang

Saat balance tidak cukup, bot bisa menampilkan prompt:

```text
balance kurang apakah anda akan tetap melanjutkan?(y/n/i)
```

Arti pilihan:

- `y`: lanjut semampunya dan boleh recovery aset sisa bila perlu
- `i`: tetap mulai jalan, tetapi jika nanti balance kurang di tengah proses bot langsung stop
- `n`: hentikan proses

Catatan:

- `allow_continue_on_low_balance = true` artinya mode default account adalah lanjut semampunya
- `allow_continue_on_low_balance = false` artinya account tidak recovery otomatis

## Mode 24 Jam

Jika `full_24h_mode = true`:

- `swap_delay_seconds` diabaikan
- bot membuat jadwal random per account
- semua jadwal memakai acuan UTC
- target sesi adalah selesai sebelum `00:00 UTC`
- jika `full_24h_auto_restart = true`, sesi berikutnya dimulai lagi untuk hari UTC berikutnya

Catatan penting:

- Saat mode 24 jam aktif, account dijalankan paralel di level account
- Di dalam 1 account, transaksi tetap serial

## Retry dan Skip Round

Jika swap hop gagal:

1. Bot retry sampai batas `max_retries`
2. Bot menunggu sesuai `retry_base_delay`
3. Jika tetap gagal, round di-skip
4. Bot lanjut ke round berikutnya

Jadi bot tidak langsung berhenti untuk account hanya karena 1 hop swap gagal.

## Telegram Monitor

Jika `telegram_enabled = true`, bot membuat 1 kartu Telegram per account dan terus mengedit pesan itu.

Isi kartu meliputi:

- status
- uptime
- balance
- total fee
- progres swap
- proxy label
- reward / volume / tx / rank
- delta tx dan reward
- latest logs

Contoh config:

```toml
[settings]
telegram_enabled = true
telegram_bot_token = "env:TELEGRAM_BOT_TOKEN"
telegram_chat_id = "-1001234567890"
telegram_update_min_interval_seconds = 5
telegram_latest_logs_limit = 6
```

## Output dan Ringkasan

Di akhir run, bot menampilkan ringkasan per account, termasuk:

- status
- putaran selesai
- `skipped_rounds`
- jumlah tx swap
- estimasi network fee
- network fee terpakai
- swap fee terpakai
- balance akhir
- `stop_reason` jika ada

## Sumber Activity

Saat inspeksi frontend `https://www.cantex.io/app/activity`, route tanpa login diarahkan ke `/signup`, tetapi bundle frontend memuat referensi endpoint:

- `https://api.cantex.io/v1/account/activity`
- `https://api.cantex.io/v1/account/reward_activity`

Bot mencoba endpoint tersebut secara best-effort. Jika data activity tidak tersedia atau belum terindeks, bot tetap lanjut dengan log yang aman.

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

### Account nonaktif tetap minta private key

Account dengan `enabled = false` akan di-skip. Jika masih error, cek apakah field account aktif dan nonaktif tertukar saat mengedit config.
