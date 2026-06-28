"""
Per-user proxy list, token balances, and config storage.
Storage: SQLite (user_data.db) — persists across restarts.
Backup:  Telegram group (BACKUP_CHAT_ID env var) — survives redeploys.
"""

import os
import sqlite3
import threading
import time
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_DB_DIR = Path(os.environ.get("DB_DIR", "."))
_DB_FILE = _DB_DIR / "user_data.db"
_lock = threading.Lock()

try:
    import mongo_sync as _msync
except ImportError:
    _msync = None  # type: ignore


# ── Token packs — users buy these with Telegram Stars ─────────────────────────
TOKEN_PACKS: dict[str, dict] = {
    "t50":  {"tokens": 50,  "stars": 50,  "label": "50 Tokens",  "emoji": "⚡", "bonus": ""},
    "t200": {"tokens": 200, "stars": 200, "label": "200 Tokens", "emoji": "🔥", "bonus": ""},
    "t500": {"tokens": 600, "stars": 500, "label": "500 Tokens", "emoji": "💎", "bonus": "+100 bonus"},
}

# ── Token cost per feature ─────────────────────────────────────────────────────
TOKEN_COSTS: dict[str, int] = {
    "check":    1,   # 1 token per cookie check using admin proxy pool
    "changepw": 5,   # 5 tokens to change a Netflix password
}

# Keep for any legacy code referencing PLANS
PLANS: dict[str, dict] = {}


# ── DB helpers ─────────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(str(_DB_FILE), check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    return c


def _init() -> None:
    with _conn() as c:
        c.executescript("""
            -- Per-user proxy pool
            CREATE TABLE IF NOT EXISTS user_proxies (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id   INTEGER NOT NULL,
                url       TEXT    NOT NULL,
                added_at  REAL    NOT NULL DEFAULT 0,
                UNIQUE(user_id, url)
            );
            CREATE INDEX IF NOT EXISTS idx_up_uid ON user_proxies(user_id);

            -- Token balances
            CREATE TABLE IF NOT EXISTS token_balances (
                user_id      INTEGER PRIMARY KEY,
                balance      INTEGER NOT NULL DEFAULT 0,
                total_bought INTEGER NOT NULL DEFAULT 0,
                total_spent  INTEGER NOT NULL DEFAULT 0,
                first_seen   REAL    NOT NULL DEFAULT 0,
                updated_at   REAL    NOT NULL DEFAULT 0
            );

            -- Full transaction log (credit/debit)
            CREATE TABLE IF NOT EXISTS token_transactions (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER NOT NULL,
                delta      INTEGER NOT NULL,
                reason     TEXT    NOT NULL DEFAULT '',
                created_at REAL    NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_tt_uid ON token_transactions(user_id);

            -- Bot-wide config (QR path, etc.)
            CREATE TABLE IF NOT EXISTS config_store (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            );

            -- Legacy payments table (kept for history)
            CREATE TABLE IF NOT EXISTS payments (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id  INTEGER NOT NULL,
                stars    INTEGER NOT NULL,
                plan_key TEXT    NOT NULL,
                paid_at  REAL    NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_pay_uid ON payments(user_id);

            -- Legacy subscriptions (kept for export compatibility)
            CREATE TABLE IF NOT EXISTS subscriptions (
                user_id     INTEGER PRIMARY KEY,
                tier        TEXT    NOT NULL DEFAULT 'free',
                paid_until  REAL    NOT NULL DEFAULT 0,
                total_stars INTEGER NOT NULL DEFAULT 0,
                updated_at  REAL    NOT NULL DEFAULT 0
            );
        """)


# ── MongoDB sync helpers ───────────────────────────────────────────────────────

def _sync_balance_to_mongo(user_id: int) -> None:
    """Read current balance from SQLite and mirror it to MongoDB (background thread)."""
    if not _msync or not _msync.is_enabled():
        return
    try:
        with _conn() as c:
            row = c.execute(
                "SELECT balance, total_bought, total_spent, first_seen "
                "FROM token_balances WHERE user_id=?",
                (user_id,)
            ).fetchone()
        if row:
            _msync.sync_user_balance(user_id, row[0], row[1], row[2], row[3])
    except Exception as e:
        logger.warning("_sync_balance_to_mongo error (uid=%s): %s", user_id, e)


def _sync_proxies_to_mongo(user_id: int) -> None:
    """Read current proxy list from SQLite and mirror it to MongoDB (background thread)."""
    if not _msync or not _msync.is_enabled():
        return
    try:
        with _conn() as c:
            rows = c.execute(
                "SELECT url FROM user_proxies WHERE user_id=? ORDER BY added_at ASC",
                (user_id,)
            ).fetchall()
        _msync.sync_user_proxies(user_id, [r[0] for r in rows])
    except Exception as e:
        logger.warning("_sync_proxies_to_mongo error (uid=%s): %s", user_id, e)


def _restore_from_mongo() -> None:
    """Merge MongoDB data into SQLite on startup.

    Always runs regardless of whether SQLite has rows — uses INSERT OR IGNORE
    so existing local rows are never overwritten, but any rows present in
    MongoDB that are missing locally (e.g. after a partial crash) are added.
    This guarantees data is never lost just because SQLite had one stale row.
    """
    if not _msync or not _msync.is_enabled():
        logger.warning("mongo_sync: skipping restore — MongoDB not connected")
        return
    try:
        # ── Token balances ────────────────────────────────────────────────
        rows = _msync.load_all_balances()
        if rows:
            with _conn() as c:
                for r in rows:
                    c.execute(
                        "INSERT OR IGNORE INTO token_balances "
                        "(user_id, balance, total_bought, total_spent, first_seen, updated_at) "
                        "VALUES (?,?,?,?,?,?)",
                        (r["user_id"], r["balance"], r["total_bought"],
                         r["total_spent"], r["first_seen"], r.get("updated_at", 0))
                    )
            logger.warning("mongo_sync: merged %d user balance(s) from MongoDB", len(rows))
        else:
            logger.warning("mongo_sync: no balance records found in MongoDB to restore")

        # ── User proxies ──────────────────────────────────────────────────
        all_proxies = _msync.load_all_user_proxies()
        if all_proxies:
            now = time.time()
            total = 0
            with _conn() as c:
                for uid, proxies in all_proxies.items():
                    for url in proxies:
                        try:
                            c.execute(
                                "INSERT OR IGNORE INTO user_proxies (user_id, url, added_at) "
                                "VALUES (?,?,?)",
                                (uid, url, now)
                            )
                            total += 1
                        except Exception:
                            pass
            logger.warning("mongo_sync: merged %d user proxy record(s) from MongoDB", total)
    except Exception as e:
        logger.warning("_restore_from_mongo error: %s", e)


_init()
_restore_from_mongo()


# ── Config store ───────────────────────────────────────────────────────────────

def get_config(key: str, default: str = "") -> str:
    with _conn() as c:
        row = c.execute("SELECT value FROM config_store WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def set_config(key: str, value: str) -> None:
    with _conn() as c:
        c.execute(
            "INSERT OR REPLACE INTO config_store (key, value) VALUES (?,?)",
            (key, value),
        )


# ── User Proxy CRUD ────────────────────────────────────────────────────────────

def _normalize(raw: str) -> str | None:
    """Normalize proxy URL — mirrors proxy_manager logic."""
    raw = raw.strip()
    if not raw or raw.startswith("#"):
        return None
    if "://" in raw:
        if raw.startswith(("http://", "https://", "socks5://", "socks4://")):
            return raw
        return None
    if "@" in raw:
        return "http://" + raw
    parts = raw.split(":")
    if len(parts) == 2:
        host, port = parts
        if port.isdigit() and 1 <= int(port) <= 65535:
            return f"http://{host}:{port}"
    elif len(parts) == 4:
        if parts[1].isdigit() and 1 <= int(parts[1]) <= 65535:
            host, port, user, pwd = parts
            return f"http://{user}:{pwd}@{host}:{port}"
        elif parts[3].isdigit() and 1 <= int(parts[3]) <= 65535:
            user, pwd, host, port = parts
            return f"http://{user}:{pwd}@{host}:{port}"
    return None


def add_proxies_from_url(user_id: int, url: str, max_proxies: int = 50) -> tuple[int, int, str | None]:
    import requests as _req
    try:
        r = _req.get(url.strip(), timeout=12, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        text = r.text
    except Exception as e:
        return 0, 0, f"Could not fetch URL: {e}"

    added = skipped = 0
    for line in text.splitlines():
        if added >= max_proxies:
            break
        normalized = _normalize(line)
        if not normalized:
            continue
        with _conn() as c:
            try:
                c.execute(
                    "INSERT INTO user_proxies (user_id, url, added_at) VALUES (?,?,?)",
                    (user_id, normalized, time.time()),
                )
                added += 1
            except sqlite3.IntegrityError:
                skipped += 1
    return added, skipped, None


def add_user_proxy(user_id: int, raw: str) -> tuple[bool, str]:
    normalized = _normalize(raw)
    if not normalized:
        return False, (
            "❌ Could not parse that proxy.\n\n"
            "Accepted formats:\n"
            "  • <code>host:port</code>\n"
            "  • <code>user:pass@host:port</code>\n"
            "  • <code>http://user:pass@host:port</code>\n"
            "  • <code>socks5://host:port</code>"
        )
    with _conn() as c:
        try:
            c.execute(
                "INSERT INTO user_proxies (user_id, url, added_at) VALUES (?,?,?)",
                (user_id, normalized, time.time()),
            )
        except sqlite3.IntegrityError:
            return False, f"⚠️ Already added: <code>{normalized}</code>"
    _sync_proxies_to_mongo(user_id)
    return True, normalized


def remove_user_proxy(user_id: int, index: int) -> str | None:
    with _conn() as c:
        rows = c.execute(
            "SELECT id, url FROM user_proxies WHERE user_id=? ORDER BY added_at ASC",
            (user_id,)
        ).fetchall()
    if 0 <= index < len(rows):
        row_id, url = rows[index]
        with _conn() as c:
            c.execute("DELETE FROM user_proxies WHERE id=?", (row_id,))
        _sync_proxies_to_mongo(user_id)
        return url
    return None


def clear_user_proxies(user_id: int) -> int:
    with _conn() as c:
        n = c.execute(
            "SELECT COUNT(*) FROM user_proxies WHERE user_id=?", (user_id,)
        ).fetchone()[0]
        c.execute("DELETE FROM user_proxies WHERE user_id=?", (user_id,))
    if _msync:
        _msync.sync_user_proxies(user_id, [])
    return n


def list_user_proxies(user_id: int) -> list[str]:
    with _conn() as c:
        rows = c.execute(
            "SELECT url FROM user_proxies WHERE user_id=? ORDER BY added_at ASC",
            (user_id,)
        ).fetchall()
    return [r[0] for r in rows]


def count_user_proxies(user_id: int) -> int:
    with _conn() as c:
        return c.execute(
            "SELECT COUNT(*) FROM user_proxies WHERE user_id=?", (user_id,)
        ).fetchone()[0]


# ── Token System ───────────────────────────────────────────────────────────────

def _ensure_balance_row(user_id: int, conn: sqlite3.Connection) -> None:
    """Create a balance row for this user if one doesn't exist yet."""
    conn.execute(
        "INSERT OR IGNORE INTO token_balances "
        "(user_id, balance, total_bought, total_spent, first_seen, updated_at) "
        "VALUES (?,0,0,0,?,?)",
        (user_id, time.time(), time.time()),
    )


def register_user(user_id: int) -> None:
    """Register a user the first time they interact with the bot.
    Safe to call on every interaction — INSERT OR IGNORE makes it a no-op
    for existing users. Ensures every user appears in /userlist even if
    they never buy tokens.
    """
    with _conn() as c:
        _ensure_balance_row(user_id, c)
    _sync_balance_to_mongo(user_id)


def get_balance(user_id: int) -> int:
    """Return current token balance for a user."""
    with _conn() as c:
        row = c.execute(
            "SELECT balance FROM token_balances WHERE user_id=?", (user_id,)
        ).fetchone()
    return row[0] if row else 0


def get_token_stats(user_id: int) -> dict:
    """Return full token stats for a user."""
    with _conn() as c:
        row = c.execute(
            "SELECT balance, total_bought, total_spent, first_seen FROM token_balances WHERE user_id=?",
            (user_id,)
        ).fetchone()
    if row:
        return {
            "balance":      row[0],
            "total_bought": row[1],
            "total_spent":  row[2],
            "first_seen":   row[3],
        }
    return {"balance": 0, "total_bought": 0, "total_spent": 0, "first_seen": time.time()}


def add_tokens(user_id: int, amount: int, reason: str = "credit") -> int:
    """
    Credit tokens to a user. Thread-safe.
    Returns the new balance.
    """
    if amount <= 0:
        return get_balance(user_id)
    now = time.time()
    with _lock:
        with _conn() as c:
            _ensure_balance_row(user_id, c)
            c.execute(
                """UPDATE token_balances
                   SET balance=balance+?, total_bought=total_bought+?, updated_at=?
                   WHERE user_id=?""",
                (amount, amount, now, user_id),
            )
            c.execute(
                "INSERT INTO token_transactions (user_id, delta, reason, created_at) VALUES (?,?,?,?)",
                (user_id, amount, reason, now),
            )
            row = c.execute(
                "SELECT balance FROM token_balances WHERE user_id=?", (user_id,)
            ).fetchone()
    _sync_balance_to_mongo(user_id)
    return row[0] if row else 0


def deduct_tokens(user_id: int, amount: int, reason: str = "debit") -> tuple[bool, int]:
    """
    Deduct tokens from a user atomically. Thread-safe.
    Returns (success, new_balance). success=False if insufficient funds.
    """
    if amount <= 0:
        return True, get_balance(user_id)
    now = time.time()
    with _lock:
        with _conn() as c:
            _ensure_balance_row(user_id, c)
            row = c.execute(
                "SELECT balance FROM token_balances WHERE user_id=?", (user_id,)
            ).fetchone()
            current = row[0] if row else 0
            if current < amount:
                return False, current
            c.execute(
                """UPDATE token_balances
                   SET balance=balance-?, total_spent=total_spent+?, updated_at=?
                   WHERE user_id=?""",
                (amount, amount, now, user_id),
            )
            c.execute(
                "INSERT INTO token_transactions (user_id, delta, reason, created_at) VALUES (?,?,?,?)",
                (user_id, -amount, reason, now),
            )
            new_row = c.execute(
                "SELECT balance FROM token_balances WHERE user_id=?", (user_id,)
            ).fetchone()
    _sync_balance_to_mongo(user_id)
    return True, (new_row[0] if new_row else 0)


def admin_set_tokens(user_id: int, amount: int, reason: str = "admin_set") -> int:
    """Admin: set a user's balance to an exact amount."""
    now = time.time()
    with _lock:
        with _conn() as c:
            _ensure_balance_row(user_id, c)
            old_row = c.execute(
                "SELECT balance FROM token_balances WHERE user_id=?", (user_id,)
            ).fetchone()
            old = old_row[0] if old_row else 0
            delta = amount - old
            c.execute(
                """UPDATE token_balances
                   SET balance=?, updated_at=?
                   WHERE user_id=?""",
                (amount, now, user_id),
            )
            if delta != 0:
                c.execute(
                    "INSERT INTO token_transactions (user_id, delta, reason, created_at) VALUES (?,?,?,?)",
                    (user_id, delta, reason, now),
                )
    _sync_balance_to_mongo(user_id)
    return amount


def get_recent_transactions(user_id: int, limit: int = 5) -> list[dict]:
    """Return the N most recent token transactions for a user."""
    with _conn() as c:
        rows = c.execute(
            "SELECT delta, reason, created_at FROM token_transactions "
            "WHERE user_id=? ORDER BY created_at DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()
    return [{"delta": r[0], "reason": r[1], "created_at": r[2]} for r in rows]


def get_all_balances() -> list[dict]:
    """Admin: return all users with token stats."""
    with _conn() as c:
        rows = c.execute(
            "SELECT user_id, balance, total_bought, total_spent, first_seen "
            "FROM token_balances ORDER BY balance DESC"
        ).fetchall()
    return [
        {
            "user_id":     r[0],
            "balance":     r[1],
            "total_bought": r[2],
            "total_spent": r[3],
            "first_seen":  r[4],
        }
        for r in rows
    ]


# ── Legacy subscription helpers (kept so existing handlers don't break) ────────

def get_subscription(user_id: int) -> dict:
    """Legacy — returns token balance as subscription proxy for backward compat."""
    balance = get_balance(user_id)
    return {
        "tier": "tokens",
        "paid_until": 0,
        "total_stars": 0,
        "is_paid": balance > 0,
        "days_left": 0,
        "hours_left": 0,
        "balance": balance,
    }


def is_paid(user_id: int) -> bool:
    """Legacy — True if user has any tokens."""
    return get_balance(user_id) > 0


def add_subscription(user_id: int, plan_key: str, stars: int) -> dict:
    """Legacy stub — not used in token system."""
    return {"ok": False, "error": "Subscription system replaced by tokens"}


def get_all_paid_users() -> list[dict]:
    """Legacy — returns users with token balance > 0."""
    with _conn() as c:
        rows = c.execute(
            "SELECT user_id, balance FROM token_balances WHERE balance > 0"
        ).fetchall()
    return [{"user_id": r[0], "paid_until": 0, "total_stars": r[1]} for r in rows]


def revoke_subscription(user_id: int) -> bool:
    """Legacy — sets token balance to 0."""
    with _lock:
        with _conn() as c:
            row = c.execute(
                "SELECT balance FROM token_balances WHERE user_id=?", (user_id,)
            ).fetchone()
            if not row or row[0] == 0:
                return False
            c.execute(
                "UPDATE token_balances SET balance=0, updated_at=? WHERE user_id=?",
                (time.time(), user_id),
            )
            c.execute(
                "INSERT INTO token_transactions (user_id, delta, reason, created_at) VALUES (?,?,?,?)",
                (user_id, -row[0], "admin_revoke", time.time()),
            )
    return True


def grant_subscription(user_id: int, plan_key: str) -> dict:
    """Legacy stub — use add_tokens instead."""
    return {"ok": False, "error": "Use /givetoken instead"}


# ── Export / Import for Telegram backup ───────────────────────────────────────

def export_json() -> str:
    with _conn() as c:
        proxies = c.execute(
            "SELECT user_id, url, added_at FROM user_proxies"
        ).fetchall()
        balances = c.execute(
            "SELECT user_id, balance, total_bought, total_spent, first_seen FROM token_balances"
        ).fetchall()
        txns = c.execute(
            "SELECT user_id, delta, reason, created_at FROM token_transactions "
            "ORDER BY created_at DESC LIMIT 5000"
        ).fetchall()
    data = {
        "v": 2,
        "exported_at": time.time(),
        "user_proxies": [
            {"user_id": r[0], "url": r[1], "added_at": r[2]} for r in proxies
        ],
        "token_balances": [
            {"user_id": r[0], "balance": r[1], "total_bought": r[2],
             "total_spent": r[3], "first_seen": r[4]} for r in balances
        ],
        "token_transactions": [
            {"user_id": r[0], "delta": r[1], "reason": r[2], "created_at": r[3]}
            for r in txns
        ],
    }
    return json.dumps(data, indent=2)


def import_json(raw: str) -> tuple[int, int, int]:
    try:
        data = json.loads(raw)
    except Exception:
        return 0, 0, 0
    now = time.time()
    p_n = b_n = t_n = 0
    with _conn() as c:
        for p in data.get("user_proxies", []):
            try:
                c.execute(
                    "INSERT OR IGNORE INTO user_proxies (user_id, url, added_at) VALUES (?,?,?)",
                    (p["user_id"], p["url"], p.get("added_at", now)),
                )
                p_n += c.rowcount
            except Exception:
                pass
        for b in data.get("token_balances", []):
            try:
                c.execute(
                    """INSERT INTO token_balances
                       (user_id, balance, total_bought, total_spent, first_seen, updated_at)
                       VALUES (?,?,?,?,?,?)
                       ON CONFLICT(user_id) DO UPDATE SET
                         balance=excluded.balance, total_bought=excluded.total_bought,
                         total_spent=excluded.total_spent, updated_at=excluded.updated_at""",
                    (b["user_id"], b["balance"], b.get("total_bought", b["balance"]),
                     b.get("total_spent", 0), b.get("first_seen", now), now),
                )
                b_n += 1
            except Exception:
                pass
        for t in data.get("token_transactions", []):
            try:
                c.execute(
                    "INSERT OR IGNORE INTO token_transactions "
                    "(user_id, delta, reason, created_at) VALUES (?,?,?,?)",
                    (t["user_id"], t["delta"], t.get("reason", ""), t.get("created_at", now)),
                )
                t_n += c.rowcount
            except Exception:
                pass
    return p_n, b_n, t_n
