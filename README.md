# 🎬 Netflix Cookie Checker — Telegram Bot

A high-speed Telegram bot for bulk Netflix cookie validation. Extracts full account details (plan, quality, country, billing, profiles), supports per-user proxy isolation, a Telegram Stars token economy, and a live monitoring dashboard.

---

## ✨ Features

- **Bulk cookie checking** — paste text or upload `.txt` / `.json` / `.zip`
- **Race engine** — races up to N proxies simultaneously; fastest result wins
- **EMA latency scoring** — admin proxy pool auto-prioritizes fastest nodes
- **Per-user proxy isolation** — users' own proxies never shared with others
- **Token system** — users buy tokens via Telegram Stars (XTR) to use admin pool
- **Admin panel** — manage proxies, grant tokens, toggle direct mode
- **MongoDB integration** — optional hit persistence with lazy reconnect
- **Output modes** — Basic (compact) or Full Info per user
- **Delivery modes** — ZIP archive or Card-by-Card individual messages
- **Live dashboard** — Flask status page with real-time stats (port 5000)
- **Password changer** — \[BETA\] change Netflix password via ALE key exchange

---

## 📋 Supported Cookie Formats

| Format | Example |
|--------|---------|
| Netscape `.txt` | `.netflix.com TRUE / TRUE 9999 NetflixId ct%3D…` |
| JSON `.json` | `[{"name":"NetflixId","value":"ct%3D…"}]` |
| Pipe-combo | `email:pass \| Country=IN \| NetflixId=ct%3D…` |
| ZIP `.zip` | Each `.txt`/`.json` inside = 1 account |
| Hit-file | `• Cookies : nfvdid=…; NetflixId=…` |

---

## 🚀 Quick Start

### 1 — Prerequisites

- Python 3.12+
- A Telegram bot token from [@BotFather](https://t.me/BotFather)
- (Optional) MongoDB URL for hit persistence

### 2 — Install dependencies

```bash
pip install -r requirements.txt
```

### 3 — Configure environment

```bash
cp .env.example .env
# Edit .env — set at minimum TELEGRAM_BOT_TOKEN and ADMIN_ID
```

### 4 — Run

```bash
python bot.py
```

---

## 🐳 Docker Deploy (Recommended)

```bash
cp .env.example .env
# Fill in TELEGRAM_BOT_TOKEN and ADMIN_ID in .env

docker compose up -d
```

SQLite databases persist in a named Docker volume (`bot_data`). The container runs as a non-root user and exposes the dashboard on port 5000.

To view logs:
```bash
docker compose logs -f
```

To stop:
```bash
docker compose down
```

---

## ⚙️ Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `TELEGRAM_BOT_TOKEN` | ✅ | Bot token from @BotFather |
| `ADMIN_ID` | Recommended | Telegram user ID of the bot owner |
| `PAYMENT_ADMIN_USERNAME` | Optional | Username shown in payment instructions |
| `MONGODB_URL` | Optional | MongoDB connection string for hit persistence |
| `BACKUP_CHAT_ID` | Optional | Group/channel ID for database export backups |
| `RACE_N` | Optional | Proxies raced per check (default: 6) |
| `BULK_CONCURRENCY` | Optional | Concurrent checks in bulk mode (default: 16) |
| `DASHBOARD_TOKEN` | Optional | Secret token to protect `/api/stats` endpoint |
| `DB_DIR` | Optional | Directory for SQLite databases (default: `.`, Docker: `/data`) |

---

## 🤖 Bot Commands

### User Commands

| Command | Description |
|---------|-------------|
| `/start` | Welcome message + quick action buttons |
| `/account` | View token balance and stats |
| `/buy` | Purchase tokens with Telegram Stars |
| `/myproxy` | Manage your personal proxy list |
| `/settings` | Output format, delivery mode, routing preference |
| `/mode` | Toggle Full Info / Basic output |
| `/changepw` | \[BETA\] Change Netflix account password |
| `/cancel` | Cancel an active bulk check |
| `/help` | Supported formats and commands |

### Admin Commands

| Command | Description |
|---------|-------------|
| `/proxy` | Manage admin proxy pool (add, remove, sources, toggle) |
| `/grant` | Grant or set token balance for a user |
| `/givetoken` | Give tokens to a user by Telegram ID |
| `/userlist` | List all users with token balances |
| `/userstatus` | Check a specific user's status |
| `/revoke` | Revoke a user's token balance |
| `/backup` | Export full database as JSON to backup group |
| `/setadmin` | Claim admin role (first use only, or via ADMIN_ID env) |
| `/setqr` | Set QR code image for payment instructions |
| `/info` | Bot stats: proxy pool, active sessions, token economy |

---

## 🪙 Token System

Users buy token packs via Telegram Stars:

| Pack | Stars | Tokens | Bonus |
|------|-------|--------|-------|
| ⚡ Starter | 50 ⭐ | 50 🪙 | — |
| 🔥 Popular | 200 ⭐ | 200 🪙 | — |
| 💎 Pro | 500 ⭐ | 600 🪙 | +100 bonus |

**Token costs:**
- Cookie check (via admin pool): **1 token**
- Password change \[BETA\]: **5 tokens**

Free users can still check cookies using their own proxies at no cost.

---

## 🏗️ Architecture

```
User sends cookie / file
        │
        ▼
  _get_check_params(uid)        ← routing decision
        │
        ├─ Admin + Direct ON  → direct HTTP
        ├─ Admin + Direct OFF → admin proxy pool (EMA-sorted)
        ├─ Token user         → admin proxy pool (costs 1 token)
        ├─ Free + own proxies → user's own proxies (no charge)
        └─ Free + no proxies  → blocked (buy tokens or add proxies)
        │
        ▼
  checker.check_cookie()
        │
        ├─ force_direct   → single direct HTTPS attempt
        ├─ user_proxies   → race user's proxies (no scoring)
        └─ admin pool     → race top-N proxies (EMA latency sorted)
```

### File Structure

```
bot.py               Main entry point — all Telegram handlers
checker.py           Netflix validation, cookie parsing, NFToken generation
proxy_manager.py     Admin proxy pool — EMA latency scoring, SQLite persistence
user_store.py        Per-user proxies, token balances, transaction log (SQLite)
mongodb_store.py     Optional MongoDB persistence for hits
stats.py             Thread-safe in-memory stats counter
dashboard.py         Flask status dashboard (port 5000)
password_changer.py  [BETA] Netflix password changer via ALE key exchange
requirements.txt     Python dependencies
Dockerfile           Production container (Python 3.12-slim, non-root user)
docker-compose.yml   One-command deploy with persistent volume
.env.example         All environment variables documented
```

---

## 🔒 Security

- **Proxy isolation** — user proxies are private, never shared with others
- **Token atomicity** — all balance ops use SQLite transactions + threading locks
- **Dashboard auth** — set `DASHBOARD_TOKEN` to protect stats from public access
- **SSL verification** — always enabled on all outbound HTTPS connections
- **Callback ownership** — all inline buttons verify caller matches embedded user ID
- **No token race conditions** — deduct-then-check pattern with full rollback on failure

---

## ⚡ Performance Tuning

| Setting | Default | Notes |
|---------|---------|-------|
| `RACE_N` | 6 | Higher = faster singles, more proxy load |
| `BULK_CONCURRENCY` | 16 | Concurrent checks per bulk session |
| Thread pool workers | 48 | For blocking I/O operations |
| Health check interval | 90s | Socket-tests entire proxy pool |
| Source refresh interval | 300s | Re-fetches proxy source URLs |

---

## License

Private — all rights reserved.
