# Cantex Autoswap Bot

Bot autoswap multi-account untuk Cantex yang dibangun di atas SDK lokal pada `cantex_sdk-0.3.0`.

## Ringkasan

Fitur utama:

- Multi-account
- 9 strategi swap
- Validasi balance, minimum protocol, dan fee sebelum submit swap
- Reserve minimal `5 CC` per account
- Optimasi route `direct` vs `1-hop`
- `1 swap sukses = 1 round selesai`
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
execution_mode = "sequential"
min_cc_reserve = "5"
swap_delay_seconds = { min = 20.0, max = 100.0 }
max_network_fee_cc_per_execution = "0.12"
network_fee_poll_seconds = { min = 20.0, max = 40.0 }
full_24h_mode = true
full_24h_startup_mode = "planned"
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
  - Batas maksimum network fee untuk 1 percobaan swap dalam satuan `CC`
  - Bot akan cek quote terlebih dahulu sebelum submit transaksi
  - Jika network fee quote saat itu lebih tinggi dari batas ini, transaksi tidak dikirim
  - Bot akan menunggu sesuai `network_fee_poll_seconds`, lalu quote ulang sampai fee turun
  - Jika saat menunggu muncul quote error sementara seperti `HTTP 502`, bot tidak dianggap gagal; bot tetap hidup dan akan quote ulang lagi
  - Setting ini hanya membatasi network fee, bukan swap fee admin/liquidity
  - Cantex saat ini memberi `3x free fee swap` per account per hari UTC
  - Bot otomatis mencoba memakai jatah ini mulai `01:00 UTC`
  - Untuk 3 swap sukses pertama setelah `01:00 UTC`, bot akan tetap submit swap walaupun quote UI masih menampilkan fee
  - Pemakaian jatah free swap disimpan di file state lokal, jadi jika bot direstart di hari UTC yang sama, jatahnya tidak diulang dari awal
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
  - Jika `true`, bot memakai scheduler harian berbasis UTC sampai `00:00 UTC` berikutnya

- `full_24h_startup_mode`
  - Menentukan perilaku saat sesi 24 jam dimulai
  - Setting ini berlaku pada awal setiap sesi harian, termasuk setelah auto restart
  - `planned`
    - Bot memakai plan jadwal random seperti logic yang sudah dipakai sebelumnya
  - `direct`
    - Bot tidak membuat plan swap di awal sesi
    - Bot langsung mencoba swap terus-menerus sampai `rounds` sukses terpenuhi
    - Delay setelah swap yang berhasil mengikuti `swap_delay_seconds`
    - Jika quota `rounds` sudah tercapai lebih cepat dan `full_24h_auto_restart = true`, bot akan idle sampai `00:00 UTC` berikutnya

- `full_24h_auto_restart`
  - Jika `true`, setelah sesi harian selesai bot akan lanjut ke hari UTC berikutnya

- `full_24h_min_gap_minutes`
  - Jarak minimum antar jadwal swap pada mode 24 jam

- `random_seed`
  - Isi angka jika ingin pola random bisa direproduksi saat debugging

- `telegram_enabled`
  - Jika `true`, bot akan mengirim 1 kartu Telegram per account

- `telegram_chat_id`
  - Disarankan diisi lewat `.env` dengan `env:TELEGRAM_CHAT_ID`

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

`strategy` sekarang hanya menerima nilai `1`, `3`, atau `7`:

1. `CC -> USDCx`
3. `CC -> CBTC`
7. `CC -> USDCx -> CBTC`

Catatan:

- `rounds` adalah jumlah swap sukses yang ingin dicapai
- `1 swap sukses = 1 round selesai`
- Strategi `1` akan memprioritaskan refill token luar strategi ke `CC`, lalu `CC -> USDCx`, lalu unwind `USDCx -> CC` saat `CC` tidak cukup
- Strategi `3` akan memprioritaskan refill token luar strategi ke `CC`, lalu `CC -> CBTC`, lalu unwind `CBTC -> CC` saat `CC` tidak cukup
- Strategi `7` memakai static round robin dinamis:
  - selama `CC` masih cukup, bot bergantian `CC -> USDCx` lalu `CC -> CBTC`
  - saat `CC` tidak cukup untuk swap keluar, bot masuk fase recycle: `USDCx -> CBTC (50%)`, `CBTC -> USDCx (50%)`, `CBTC -> CC (max)`, `USDCx -> CC (max)`
- Langkah strategi hanya maju jika swap pada langkah saat ini benar-benar sukses
- Constraint sementara seperti fee tinggi, minimum ticket protocol, atau source token belum cukup tidak mengurangi `rounds`

Contoh:

- `strategy = "1"` akan terus mengulang flow `refill luar strategi -> CC -> USDCx -> USDCx -> CC`
- `strategy = "3"` akan terus mengulang flow `refill luar strategi -> CC -> CBTC -> CBTC -> CC`
- `strategy = "7"` akan terus memakai round robin dinamis antara fase spend dan fase recycle sampai target `rounds` sukses terpenuhi

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

- nominal swap diacak saat langkah itu benar-benar akan dieksekusi
- token source aktif menentukan range amount yang dipakai
- `rounds` diacak sekali di awal run account

## Perilaku Saat Balance dan Reserve

- `min_cc_reserve` hanya membatasi saat source token adalah `CC`
- Jika step aktif adalah `CC -> token lain`, bot hanya boleh swap selama balance `CC` masih di atas reserve
- Jika balance `CC <= min_cc_reserve`, bot tidak akan lagi memakai `CC` sebagai source token untuk swap keluar
- Kondisi ini bukan berarti strategi selesai
- Step lain seperti `USDCx -> CBTC`, `CBTC -> CC`, atau `USDCx -> CC` tetap boleh berjalan walaupun `CC <= min_cc_reserve`

Catatan:

- Bot tidak menganggap balance kurang sebagai kondisi selesai
- Jika source token aktif belum memenuhi syarat config user atau protocol, bot hanya menunda attempt dan mencoba lagi pada evaluasi berikutnya
- `allow_continue_on_low_balance` saat ini sebaiknya dianggap sebagai setting kompatibilitas lama, bukan penentu selesai strategi

## Mode 24 Jam

Jika `full_24h_mode = true`:

- jika `full_24h_startup_mode = "planned"`, `swap_delay_seconds` diabaikan
- jika `full_24h_startup_mode = "planned"`, saat startup bot hanya membuat plan waktu eksekusi swap
- bot tidak membuat plan swap lengkap di awal
- jika `full_24h_startup_mode = "planned"`, bot membuat jadwal random per account
- jika `full_24h_startup_mode = "direct"`, bot tidak membuat jadwal random di awal dan langsung mencoba swap
- semua jadwal memakai acuan UTC
- pada mode `planned`, target sesi adalah selesai sebelum `00:00 UTC`
- pada mode `direct`, bot terus mencoba sampai quota `rounds` sukses terpenuhi; jika quota selesai lebih cepat dan `full_24h_auto_restart = true`, bot akan idle sampai `00:00 UTC` berikutnya
- jika `full_24h_auto_restart = true`, sesi berikutnya dimulai lagi untuk hari UTC berikutnya
- jatah `3x free fee swap` harian per account akan reset saat hari UTC berganti, tetapi baru dipakai mulai `01:00 UTC`
- `max_network_fee_cc_per_execution` tetap berlaku
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
telegram_chat_id = "env:TELEGRAM_CHAT_ID"
telegram_update_min_interval_seconds = 5
telegram_latest_logs_limit = 6
```

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

Saat inspeksi frontend `https://www.cantex.io/app/activity`, route tanpa login diarahkan ke `/signup`, tetapi bundle frontend memuat referensi endpoint:

- `https://api.cantex.io/v1/account/activity`
- `https://api.cantex.io/v1/account/reward_activity`

Bot mencoba endpoint tersebut secara best-effort. Jika data activity tidak tersedia atau belum terindeks, bot tetap lanjut dengan log yang aman.

## File State Lokal

Bot menyimpan state lokal di folder `config/`:

- `.autoswap_bot_runtime_state.json`
  - menyimpan jatah `3x free fee swap` harian per account
  - dipakai agar restart bot tidak mengulang free swap yang sudah terpakai di hari UTC yang sama
- `.autoswap_telegram_state.json`
  - menyimpan statistik kartu Telegram seperti total swap dan gas fee harian / lifetime

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
