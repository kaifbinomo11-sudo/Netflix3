"""
Proxy Manager — Admin-controlled proxy pool.
State persisted in SQLite (proxy_pool.db).
Default: OFF (direct connection).

Features:
  - Any common proxy format (auto-normalized on add)
  - Source URLs stored; auto-refreshed every 60 seconds in background
  - Dead proxy auto-removal: socket health-check removes unreachable proxies
  - Failure tracking: 3 consecutive failures → permanent removal
  - No proxies hardcoded; admin adds source URLs manually
"""

import os
import re
import socket
import sqlite3
import threading
import time
import logging
import requests
from pathlib import Path
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

try:
    import mongo_sync as _msync
except ImportError:
    _msync = None  # type: ignore

_DB_DIR = Path(os.environ.get("DB_DIR", "."))
_DB_FILE = _DB_DIR / "proxy_pool.db"
_AUTO_REFRESH_INTERVAL = 300       # seconds between source re-fetches (5 min)
_HEALTH_CHECK_INTERVAL = 90        # seconds between full pool health checks
_MAX_FAILURES = 2                  # consecutive failures before permanent removal
_SOCKET_TIMEOUT = 4                # seconds for socket health-check
_FETCH_TIMEOUT = 20                # seconds for HTTP source fetch
_TEST_WORKERS = 80                 # parallel workers for pre-testing fetched proxies
_TEST_ON_FETCH = True              # socket-test proxies before adding from URL


# ---------------------------------------------------------------------------
# Format normalizer
# ---------------------------------------------------------------------------

def normalize_proxy_url(raw: str) -> str | None:
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


def _parse_host_port(proxy_url: str) -> tuple[str, int] | None:
    """Extract (host, port) from a proxy URL for socket health-check."""
    try:
        parsed = urlparse(proxy_url)
        host = parsed.hostname
        port = parsed.port
        if host and port:
            return host, port
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_DB_FILE), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


_EMA_ALPHA = 0.3  # weight of latest sample in the exponential moving average


def _init_db() -> None:
    with _get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS proxies (
                url         TEXT PRIMARY KEY,
                added_at    REAL NOT NULL DEFAULT 0,
                fail_count  INTEGER NOT NULL DEFAULT 0,
                last_fail   REAL NOT NULL DEFAULT 0,
                last_ok     REAL NOT NULL DEFAULT 0,
                avg_latency REAL
            );
            CREATE TABLE IF NOT EXISTS sources (
                url      TEXT PRIMARY KEY,
                added_at REAL NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS config (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            );
        """)
        # Migration: add avg_latency column if it was not present yet
        cols = [r[1] for r in conn.execute("PRAGMA table_info(proxies)").fetchall()]
        if "avg_latency" not in cols:
            conn.execute("ALTER TABLE proxies ADD COLUMN avg_latency REAL")
        conn.execute(
            "INSERT OR IGNORE INTO config (key, value) VALUES ('enabled', '0')"
        )
        conn.execute(
            "INSERT OR IGNORE INTO config (key, value) VALUES ('changepw_proxy', '0')"
        )
        conn.execute(
            "INSERT OR IGNORE INTO config (key, value) VALUES ('admin_direct', '0')"
        )
        conn.execute(
            "INSERT OR IGNORE INTO config (key, value) VALUES ('cf_relay_enabled', '0')"
        )


# ---------------------------------------------------------------------------
# Socket-based health check (no HTTP through proxy — avoids any web bans)
# ---------------------------------------------------------------------------

def _is_proxy_alive(proxy_url: str, timeout: float = _SOCKET_TIMEOUT) -> bool:
    """
    Check if the proxy server is reachable by attempting a TCP connection.
    Does NOT make any HTTP request through the proxy.
    """
    hp = _parse_host_port(proxy_url)
    if not hp:
        return False
    host, port = hp
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, socket.timeout):
        return False


# ---------------------------------------------------------------------------
# ProxyManager
# ---------------------------------------------------------------------------

class ProxyManager:
    def __init__(self):
        _init_db()
        self._lock = threading.Lock()
        self._idx: int = 0
        self._stop_event = threading.Event()
        self._bg_thread = threading.Thread(
            target=self._background_loop, daemon=True, name="proxy-bg"
        )
        self._bg_thread.start()
        self._restore_from_mongo()

    def _restore_from_mongo(self) -> None:
        """Merge proxy pool and sources from MongoDB into SQLite on startup.

        Always runs — uses INSERT OR IGNORE so existing local rows are kept
        but any rows missing locally are added from MongoDB.
        """
        if not _msync or not _msync.is_enabled():
            return
        try:
            rows = _msync.load_proxy_pool()
            if rows:
                with _get_conn() as conn:
                    for r in rows:
                        try:
                            conn.execute(
                                "INSERT OR IGNORE INTO proxies "
                                "(url, added_at, fail_count, last_fail, last_ok, avg_latency) "
                                "VALUES (?,?,?,?,?,?)",
                                (r["url"], r.get("added_at", 0), r.get("fail_count", 0),
                                 r.get("last_fail", 0), r.get("last_ok", 0), r.get("avg_latency"))
                            )
                        except Exception:
                            pass
                logger.warning("mongo_sync: merged %d admin proxy record(s) from MongoDB", len(rows))

            sources = _msync.load_proxy_sources()
            if sources:
                with _get_conn() as conn:
                    for url in sources:
                        try:
                            conn.execute(
                                "INSERT OR IGNORE INTO sources (url, added_at) VALUES (?,?)",
                                (url, time.time())
                            )
                        except Exception:
                            pass
                logger.warning("mongo_sync: merged %d proxy source(s) from MongoDB", len(sources))
        except Exception as e:
            logger.warning("ProxyManager._restore_from_mongo error: %s", e)

    def _sync_pool_to_mongo(self) -> None:
        """Read current proxy pool from SQLite and mirror to MongoDB (fire-and-forget)."""
        if not _msync or not _msync.is_enabled():
            return
        try:
            with _get_conn() as conn:
                rows = conn.execute(
                    "SELECT url, added_at, fail_count, last_fail, last_ok, avg_latency FROM proxies"
                ).fetchall()
            _msync.sync_proxy_pool([
                {"url": r[0], "added_at": r[1], "fail_count": r[2],
                 "last_fail": r[3], "last_ok": r[4], "avg_latency": r[5]}
                for r in rows
            ])
        except Exception as e:
            logger.debug("_sync_pool_to_mongo error: %s", e)

    def _sync_sources_to_mongo(self) -> None:
        """Read current source list from SQLite and mirror to MongoDB (fire-and-forget)."""
        if not _msync or not _msync.is_enabled():
            return
        try:
            _msync.sync_proxy_sources(self.list_sources())
        except Exception as e:
            logger.debug("_sync_sources_to_mongo error: %s", e)

    # ── Background thread ─────────────────────────────────────────────────

    def _background_loop(self) -> None:
        last_refresh = 0.0
        last_health = 0.0
        while not self._stop_event.is_set():
            now = time.time()
            try:
                if now - last_refresh >= _AUTO_REFRESH_INTERVAL:
                    self._auto_refresh_sources()
                    last_refresh = now
                if now - last_health >= _HEALTH_CHECK_INTERVAL:
                    self._health_check_all()
                    last_health = now
            except Exception as exc:
                logger.warning(f"proxy_manager: background error: {exc}")
            self._stop_event.wait(timeout=10)

    def _auto_refresh_sources(self) -> None:
        sources = self.list_sources()
        if not sources:
            return
        added = 0
        for url in sources:
            a, _, _ = self.fetch_from_url(url)
            added += a
        if added:
            logger.info(f"proxy_manager: auto-refresh added {added} proxies")

    def _health_check_all(self) -> None:
        """Socket-check every proxy; permanently remove dead ones."""
        with _get_conn() as conn:
            rows = conn.execute("SELECT url FROM proxies").fetchall()
        urls = [r[0] for r in rows]
        if not urls:
            return
        removed = 0
        for url in urls:
            alive = _is_proxy_alive(url)
            if alive:
                with _get_conn() as conn:
                    conn.execute(
                        "UPDATE proxies SET fail_count=0, last_ok=? WHERE url=?",
                        (time.time(), url),
                    )
            else:
                self._increment_failure(url, force_remove=True)
                removed += 1
        if removed:
            logger.info(f"proxy_manager: health-check removed {removed} dead proxies")

    # ── Persistence helpers ───────────────────────────────────────────────

    def _increment_failure(self, url: str, force_remove: bool = False) -> None:
        with _get_conn() as conn:
            conn.execute(
                "UPDATE proxies SET fail_count = fail_count + 1, last_fail = ? WHERE url = ?",
                (time.time(), url),
            )
            row = conn.execute(
                "SELECT fail_count FROM proxies WHERE url = ?", (url,)
            ).fetchone()
        if row and (row[0] >= _MAX_FAILURES or force_remove):
            self._permanently_remove(url)

    def mark_failure(self, url: str, hard: bool = False) -> None:
        """
        Record a failure for a proxy.
        hard=True  → immediately remove (use for 429, timeout, connection error).
        hard=False → increment counter; remove after _MAX_FAILURES (use for login-page ambiguity).
        """
        if not url:
            return
        self._increment_failure(url, force_remove=hard)

    def mark_success(self, url: str, latency: float | None = None) -> None:
        """Reset failure counter, record last successful use, and update latency EMA."""
        if not url:
            return
        with _get_conn() as conn:
            if latency is not None and latency > 0:
                row = conn.execute(
                    "SELECT avg_latency FROM proxies WHERE url = ?", (url,)
                ).fetchone()
                if row and row[0] is not None:
                    new_avg = _EMA_ALPHA * latency + (1 - _EMA_ALPHA) * row[0]
                else:
                    new_avg = latency
                conn.execute(
                    "UPDATE proxies SET fail_count = 0, last_ok = ?, avg_latency = ? WHERE url = ?",
                    (time.time(), new_avg, url),
                )
            else:
                conn.execute(
                    "UPDATE proxies SET fail_count = 0, last_ok = ? WHERE url = ?",
                    (time.time(), url),
                )

    def _permanently_remove(self, url: str) -> None:
        with _get_conn() as conn:
            conn.execute("DELETE FROM proxies WHERE url = ?", (url,))
        logger.info(f"proxy_manager: removed dead proxy {url[:40]}…")

    # ── Properties ───────────────────────────────────────────────────────

    @property
    def count(self) -> int:
        with _get_conn() as conn:
            row = conn.execute("SELECT COUNT(*) FROM proxies").fetchone()
        return row[0] if row else 0

    @property
    def available_count(self) -> int:
        return self.count

    @property
    def enabled(self) -> bool:
        with _get_conn() as conn:
            row = conn.execute(
                "SELECT value FROM config WHERE key='enabled'"
            ).fetchone()
        return bool(int(row[0])) if row else False

    # ── Toggle ───────────────────────────────────────────────────────────

    def toggle(self) -> bool:
        new_val = not self.enabled
        with _get_conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO config (key, value) VALUES ('enabled', ?)",
                ("1" if new_val else "0",),
            )
        return new_val

    def set_enabled(self, val: bool) -> None:
        with _get_conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO config (key, value) VALUES ('enabled', ?)",
                ("1" if val else "0",),
            )

    # ── Add / remove individual proxies ──────────────────────────────────

    def add_proxy_raw(self, raw: str) -> tuple[bool, str]:
        normalized = normalize_proxy_url(raw)
        if not normalized:
            return False, (
                "Could not parse that as a proxy. Accepted formats:\n"
                "  • <code>host:port</code>\n"
                "  • <code>host:port:user:pass</code>\n"
                "  • <code>user:pass@host:port</code>\n"
                "  • <code>http://user:pass@host:port</code>\n"
                "  • <code>socks5://host:port</code>"
            )
        with _get_conn() as conn:
            existing = conn.execute(
                "SELECT url FROM proxies WHERE url=?", (normalized,)
            ).fetchone()
            if existing:
                return False, f"Already in list: <code>{normalized}</code>"
            conn.execute(
                "INSERT INTO proxies (url, added_at) VALUES (?, ?)",
                (normalized, time.time()),
            )
        self._sync_pool_to_mongo()
        return True, normalized

    def remove_proxy(self, index: int) -> str | None:
        with _get_conn() as conn:
            rows = conn.execute(
                "SELECT url FROM proxies ORDER BY added_at ASC"
            ).fetchall()
        if 0 <= index < len(rows):
            url = rows[index][0]
            with _get_conn() as conn:
                conn.execute("DELETE FROM proxies WHERE url=?", (url,))
            self._sync_pool_to_mongo()
            return url
        return None

    def clear_proxies(self) -> int:
        with _get_conn() as conn:
            count = conn.execute("SELECT COUNT(*) FROM proxies").fetchone()[0]
            conn.execute("DELETE FROM proxies")
        self._sync_pool_to_mongo()
        return count

    def list_proxies(self) -> list[str]:
        with _get_conn() as conn:
            rows = conn.execute(
                "SELECT url FROM proxies ORDER BY added_at ASC"
            ).fetchall()
        return [r[0] for r in rows]

    def proxies_as_text(self) -> str:
        """Return all proxies as a plain-text string (one per line)."""
        return "\n".join(self.list_proxies())

    # ── Source URLs ───────────────────────────────────────────────────────

    def add_source(self, url: str) -> tuple[bool, str]:
        url = url.strip()
        if not url.startswith(("http://", "https://")):
            return False, "Source URL must start with http:// or https://"
        with _get_conn() as conn:
            existing = conn.execute(
                "SELECT url FROM sources WHERE url=?", (url,)
            ).fetchone()
            if existing:
                return False, "Source URL already saved."
            conn.execute(
                "INSERT INTO sources (url, added_at) VALUES (?, ?)",
                (url, time.time()),
            )
        self._sync_sources_to_mongo()
        return True, url

    def remove_source(self, index: int) -> str | None:
        with _get_conn() as conn:
            rows = conn.execute(
                "SELECT url FROM sources ORDER BY added_at ASC"
            ).fetchall()
        if 0 <= index < len(rows):
            url = rows[index][0]
            with _get_conn() as conn:
                conn.execute("DELETE FROM sources WHERE url=?", (url,))
            self._sync_sources_to_mongo()
            return url
        return None

    def list_sources(self) -> list[str]:
        with _get_conn() as conn:
            rows = conn.execute(
                "SELECT url FROM sources ORDER BY added_at ASC"
            ).fetchall()
        return [r[0] for r in rows]

    def fetch_from_url(self, url: str) -> tuple[int, int, str]:
        """
        Fetch a plain-text proxy list from *url*, socket-test each one in parallel,
        and insert only the live ones into the pool.

        Returns (added, skipped, error_msg).

        Why pre-test?  Free proxy lists contain 80-95 % dead entries.  Adding
        them untested pollutes the pool so that the checker always falls back to
        direct.  By testing on fetch we keep only proxies that are actually
        reachable right now, so they genuinely rotate during checks.
        """
        try:
            resp = requests.get(url, timeout=_FETCH_TIMEOUT, headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                )
            })
            resp.raise_for_status()
            text = resp.text
        except Exception as e:
            return 0, 0, str(e)

        # ── Parse & deduplicate candidate lines ──────────────────────────────
        with _get_conn() as conn:
            existing_set = {
                r[0] for r in conn.execute("SELECT url FROM proxies").fetchall()
            }

        candidates: list[str] = []
        skipped = 0
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                skipped += 1
                continue
            normalized = normalize_proxy_url(line)
            if not normalized:
                skipped += 1
                continue
            if normalized in existing_set:
                skipped += 1
                continue
            candidates.append(normalized)

        if not candidates:
            return 0, skipped, ""

        # ── Parallel socket-test: only keep reachable proxies ────────────────
        if _TEST_ON_FETCH:
            import concurrent.futures as _cf
            live: list[str] = []
            with _cf.ThreadPoolExecutor(max_workers=min(_TEST_WORKERS, len(candidates))) as pool:
                results = pool.map(
                    lambda u: (u, _is_proxy_alive(u, timeout=_SOCKET_TIMEOUT)),
                    candidates,
                )
                for proxy_url, alive in results:
                    if alive:
                        live.append(proxy_url)
                    else:
                        skipped += 1
            logger.info(
                f"proxy_manager: fetch {url[:50]}… → "
                f"{len(candidates)} parsed, {len(live)} live, {skipped} skipped"
            )
        else:
            live = candidates

        # ── Insert survivors (atomic: INSERT OR IGNORE avoids TOCTOU race) ────
        added = 0
        now = time.time()
        with _get_conn() as conn:
            for proxy_url in live:
                cur = conn.execute(
                    "INSERT OR IGNORE INTO proxies (url, added_at) VALUES (?, ?)",
                    (proxy_url, now),
                )
                if cur.rowcount:
                    added += 1
        if added:
            self._sync_pool_to_mongo()
        return added, skipped, ""

    def refresh_all_sources(self) -> tuple[int, int, list[str]]:
        """Re-fetch all saved source URLs. Returns (total_added, total_skipped, errors)."""
        sources = self.list_sources()
        total_added = 0
        total_skipped = 0
        errors = []
        for url in sources:
            added, skipped, err = self.fetch_from_url(url)
            total_added += added
            total_skipped += skipped
            if err:
                errors.append(f"{url[:40]}… → {err}")
        return total_added, total_skipped, errors

    # ── Runtime helpers (used by checker.py) ─────────────────────────────

    def get_proxies_dict(self) -> dict | None:
        """Return a single proxy, round-robin within the pool (latency-sorted)."""
        if not self.enabled:
            return None
        with _get_conn() as conn:
            rows = conn.execute(
                "SELECT url FROM proxies "
                "ORDER BY COALESCE(avg_latency, 9999999) ASC, last_ok DESC, added_at ASC"
            ).fetchall()
        available = [r[0] for r in rows]
        if not available:
            return None
        with self._lock:
            self._idx = (self._idx + 1) % len(available)
            url = available[self._idx]
        return {"http": url, "https": url}

    def get_top_proxies_dicts(self, n: int) -> list[dict]:
        """
        Return up to n proxy dicts for racing, using a rotating window so that
        all proxies in the pool get used over time (true rotation).

        Selection strategy:
          1. Fetch the full pool ordered by latency (best first).
          2. Use a rotating window offset (_race_offset) so each call starts
             at a different position in the pool — this ensures all proxies
             get selected even when n < pool_size.
          3. Always include at least one random unscored proxy (exploration)
             so newly added proxies get sampled quickly.

        This guarantees:
          • With 10 paid proxies and n=6, every proxy gets its turn over ~2 calls.
          • Fast proxies still win more often (sorted by latency), but slow ones
            are also exercised and can improve their score over time.
          • New/unscored proxies are always sampled immediately.
        """
        if not self.enabled or n <= 0:
            return []

        import random as _random

        with _get_conn() as conn:
            # Full pool sorted by latency (NULL latency = unscored, pushed last)
            all_rows = conn.execute(
                "SELECT url, avg_latency IS NULL as unscored FROM proxies "
                "ORDER BY COALESCE(avg_latency, 9999999) ASC, last_ok DESC"
            ).fetchall()
            # Unscored proxies in random order for exploration
            unscored_rows = conn.execute(
                "SELECT url FROM proxies WHERE avg_latency IS NULL "
                "ORDER BY RANDOM() LIMIT ?",
                (max(1, n // 3),),
            ).fetchall()

        if not all_rows:
            return []

        scored_urls = [r[0] for r in all_rows if not r[1]]
        unscored_urls = [r[0] for r in unscored_rows]

        result: list[dict] = []
        seen: set[str] = set()

        # Step 1: rotating window over scored proxies
        if scored_urls:
            pool_size = len(scored_urls)
            with self._lock:
                offset = self._idx % pool_size
                # Advance index by n so next call picks a different window
                self._idx = (self._idx + max(1, n - 1)) % pool_size

            # Pick n-1 slots from the rotated pool (leave 1 slot for exploration)
            exploit_n = max(1, n - 1)
            for i in range(min(exploit_n, pool_size)):
                url = scored_urls[(offset + i) % pool_size]
                if url not in seen:
                    result.append({"http": url, "https": url})
                    seen.add(url)
                    if len(result) >= exploit_n:
                        break

        # Step 2: fill remaining slots with random unscored proxies (exploration)
        for url in unscored_urls:
            if len(result) >= n:
                break
            if url not in seen:
                result.append({"http": url, "https": url})
                seen.add(url)

        # Step 3: if still short, fill from the full pool in random order
        if len(result) < n:
            extras = [r[0] for r in all_rows if r[0] not in seen]
            _random.shuffle(extras)
            for url in extras:
                if len(result) >= n:
                    break
                result.append({"http": url, "https": url})
                seen.add(url)

        return result

    # ── Password-change proxy setting ────────────────────────────────────

    @property
    def changepw_proxy_enabled(self) -> bool:
        with _get_conn() as conn:
            row = conn.execute(
                "SELECT value FROM config WHERE key='changepw_proxy'"
            ).fetchone()
        return bool(int(row[0])) if row else False

    def toggle_changepw_proxy(self) -> bool:
        new_val = not self.changepw_proxy_enabled
        with _get_conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO config (key, value) VALUES ('changepw_proxy', ?)",
                ("1" if new_val else "0",),
            )
        return new_val

    def set_changepw_proxy_enabled(self, val: bool) -> None:
        with _get_conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO config (key, value) VALUES ('changepw_proxy', ?)",
                ("1" if val else "0",),
            )

    def get_changepw_proxy_dict(self) -> dict | None:
        """
        Return a proxy dict for the password-changer flow.
        Only returns a proxy if ChangePW proxy is explicitly enabled.
        Uses the same pool as the main checker but bypasses the main proxy toggle.
        """
        if not self.changepw_proxy_enabled:
            return None
        return self.get_proxies_dict()

    # ── Admin direct-only mode ────────────────────────────────────────────

    @property
    def admin_direct(self) -> bool:
        """When True, admin's own checks always go direct (no proxy)."""
        with _get_conn() as conn:
            row = conn.execute(
                "SELECT value FROM config WHERE key='admin_direct'"
            ).fetchone()
        return bool(int(row[0])) if row else False

    def toggle_admin_direct(self) -> bool:
        new_val = not self.admin_direct
        with _get_conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO config (key, value) VALUES ('admin_direct', ?)",
                ("1" if new_val else "0",),
            )
        return new_val

    # ── Cloudflare Worker relay toggle ────────────────────────────────────

    @property
    def cf_relay_enabled(self) -> bool:
        """Whether the Cloudflare Worker relay is enabled for proxy-pool checks."""
        with _get_conn() as conn:
            row = conn.execute(
                "SELECT value FROM config WHERE key='cf_relay_enabled'"
            ).fetchone()
        return bool(int(row[0])) if row else False

    def toggle_cf_relay(self) -> bool:
        new_val = not self.cf_relay_enabled
        with _get_conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO config (key, value) VALUES ('cf_relay_enabled', ?)",
                ("1" if new_val else "0",),
            )
        if _msync:
            _msync.save_setting("cf_relay_enabled", new_val)
        return new_val

    # ── Display ──────────────────────────────────────────────────────────

    def status_text(self) -> str:
        with _get_conn() as conn:
            total = conn.execute("SELECT COUNT(*) FROM proxies").fetchone()[0]
            src_count = conn.execute("SELECT COUNT(*) FROM sources").fetchone()[0]
        src_note = f" · {src_count} source{'s' if src_count != 1 else ''}" if src_count else ""
        if not self.enabled:
            if total == 0:
                return f"🔴 <b>OFF</b> — direct connection  (no proxies stored{src_note})"
            return f"🔴 <b>OFF</b> — direct connection  ({total} proxies stored{src_note})"
        if total == 0:
            return f"🟡 <b>ON</b> — no proxies yet, falling back to direct{src_note}"
        return f"🟢 <b>ON</b> — {total} proxies available{src_note}"


proxy_manager = ProxyManager()
