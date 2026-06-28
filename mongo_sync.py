"""
mongo_sync.py — MongoDB persistence layer.

Mirrors critical bot state to MongoDB so data survives restarts and
redeploys on any platform (Render, Replit, Docker, etc.).

Behaviour:
  • All saves run in non-daemon background threads so they complete even
    during a graceful shutdown (SIGTERM on Render gives ~30 s grace).
  • Restores run once on startup and populate SQLite from MongoDB — merging
    any missing rows so a partial local DB is always topped up from the cloud.
  • Entire module degrades gracefully when MONGODB_URL is not set.

Collections used:
  settings       — key/value config (admin_id, cf_relay state, ...)
  token_balances — per-user token balances
  user_proxies   — per-user proxy lists
  admin_proxies  — admin proxy pool with latency scores
  proxy_sources  — admin proxy source URLs
"""

import os
import re
import threading
import logging
import time
from urllib.parse import quote

logger = logging.getLogger(__name__)

_client = None
_db     = None
_enabled = False


# ── Connection ────────────────────────────────────────────────────────────────

def _try_connect() -> bool:
    global _client, _db, _enabled
    url = os.environ.get("MONGODB_URL", "").strip()
    if not url:
        logger.warning("mongo_sync: MONGODB_URL is not set — MongoDB disabled")
        return False

    # Redact credentials for safe logging: show only scheme + host
    try:
        _safe = re.sub(r'(mongodb(?:\+srv)?://)([^@]+@)', r'\1***:***@', url)
    except Exception:
        _safe = "<url>"
    logger.warning("mongo_sync: connecting to %s …", _safe)

    try:
        import pymongo
        m = re.match(r'(mongodb(?:\+srv)?://[^:]+:)(.+?)(@[^@]+$)', url)
        if m:
            url = m.group(1) + quote(m.group(2), safe="") + m.group(3)
        client = pymongo.MongoClient(
            url,
            serverSelectionTimeoutMS=8000,
            connectTimeoutMS=8000,
            socketTimeoutMS=10000,
            retryWrites=True,
        )
        client.server_info()
        try:
            db_name = pymongo.uri_parser.parse_uri(url).get("database") or "netflix_checker"
        except Exception:
            db_name = "netflix_checker"
        _client  = client
        _db      = client[db_name]
        _enabled = True
        logger.warning("mongo_sync: connected — db=%s", db_name)
        return True
    except ImportError:
        logger.warning("mongo_sync: pymongo is not installed — run: pip install pymongo")
        return False
    except Exception as e:
        err = str(e)
        logger.warning("mongo_sync: connection failed — %s", err)
        if "Authentication failed" in err or "AuthenticationFailed" in err:
            logger.warning("mongo_sync: hint — wrong username or password in MONGODB_URL")
        elif "timed out" in err.lower() or "ServerSelectionTimeoutError" in err:
            logger.warning(
                "mongo_sync: hint — connection timed out. "
                "Most likely cause: your MongoDB Atlas cluster has IP Access List restrictions. "
                "Go to Atlas → Network Access → Add IP Address → Allow access from anywhere (0.0.0.0/0)"
            )
        elif "SSL" in err or "ssl" in err:
            logger.warning("mongo_sync: hint — SSL/TLS error. Check if your cluster requires TLS.")
        return False


def is_enabled() -> bool:
    return _enabled


def _col(name: str):
    return _db[name] if _db is not None else None


def _bg(fn):
    """Run fn in a non-daemon background thread so it survives SIGTERM grace period."""
    threading.Thread(target=fn, daemon=False).start()


# ── Settings (key/value) ──────────────────────────────────────────────────────

def save_setting(key: str, value) -> None:
    """Async: persist a key/value setting to MongoDB."""
    if not _enabled:
        return
    def _do():
        try:
            _col("settings").replace_one(
                {"_id": key},
                {"_id": key, "value": value},
                upsert=True,
            )
        except Exception as e:
            logger.warning("mongo_sync.save_setting error: %s", e)
    _bg(_do)


def get_setting(key: str, default=None):
    """Blocking: read one setting from MongoDB."""
    if not _enabled:
        return default
    try:
        doc = _col("settings").find_one({"_id": key})
        return doc["value"] if doc else default
    except Exception:
        return default


# ── Token balances ────────────────────────────────────────────────────────────

def sync_user_balance(user_id: int, balance: int, total_bought: int,
                      total_spent: int, first_seen: float) -> None:
    """Async: mirror one user's token balance to MongoDB."""
    if not _enabled:
        return
    def _do():
        try:
            _col("token_balances").replace_one(
                {"_id": user_id},
                {
                    "_id":         user_id,
                    "balance":     balance,
                    "total_bought": total_bought,
                    "total_spent": total_spent,
                    "first_seen":  first_seen,
                    "updated_at":  time.time(),
                },
                upsert=True,
            )
        except Exception as e:
            logger.warning("mongo_sync.sync_user_balance error (uid=%s): %s", user_id, e)
    _bg(_do)


def load_all_balances() -> list:
    """Blocking: return all token balance docs from MongoDB."""
    if not _enabled:
        return []
    try:
        return [
            {
                "user_id":      doc["_id"],
                "balance":      doc.get("balance", 0),
                "total_bought": doc.get("total_bought", 0),
                "total_spent":  doc.get("total_spent", 0),
                "first_seen":   doc.get("first_seen", 0),
                "updated_at":   doc.get("updated_at", 0),
            }
            for doc in _col("token_balances").find({})
        ]
    except Exception as e:
        logger.warning("mongo_sync.load_all_balances: %s", e)
        return []


# ── User proxies ──────────────────────────────────────────────────────────────

def sync_user_proxies(user_id: int, proxies: list) -> None:
    """Async: mirror one user's proxy list to MongoDB."""
    if not _enabled:
        return
    def _do():
        try:
            _col("user_proxies").replace_one(
                {"_id": user_id},
                {"_id": user_id, "proxies": proxies, "updated_at": time.time()},
                upsert=True,
            )
        except Exception as e:
            logger.warning("mongo_sync.sync_user_proxies error (uid=%s): %s", user_id, e)
    _bg(_do)


def load_all_user_proxies() -> dict:
    """Blocking: return {user_id: [proxy_url, ...]} from MongoDB."""
    if not _enabled:
        return {}
    try:
        return {
            doc["_id"]: doc.get("proxies", [])
            for doc in _col("user_proxies").find({})
        }
    except Exception as e:
        logger.warning("mongo_sync.load_all_user_proxies: %s", e)
        return {}


# ── Admin proxy pool ──────────────────────────────────────────────────────────

def sync_proxy_pool(proxies: list) -> None:
    """Async: replace the full admin proxy pool in MongoDB."""
    if not _enabled:
        return
    def _do():
        try:
            col = _col("admin_proxies")
            col.delete_many({})
            if proxies:
                col.insert_many(proxies)
        except Exception as e:
            logger.warning("mongo_sync.sync_proxy_pool error: %s", e)
    _bg(_do)


def load_proxy_pool() -> list:
    """Blocking: return admin proxy pool from MongoDB."""
    if not _enabled:
        return []
    try:
        return [
            {
                "url":         doc["url"],
                "added_at":    doc.get("added_at", 0),
                "fail_count":  doc.get("fail_count", 0),
                "last_fail":   doc.get("last_fail", 0),
                "last_ok":     doc.get("last_ok", 0),
                "avg_latency": doc.get("avg_latency"),
            }
            for doc in _col("admin_proxies").find({})
        ]
    except Exception as e:
        logger.warning("mongo_sync.load_proxy_pool: %s", e)
        return []


# ── Proxy sources ─────────────────────────────────────────────────────────────

def sync_proxy_sources(sources: list) -> None:
    """Async: replace proxy source URL list in MongoDB."""
    if not _enabled:
        return
    def _do():
        try:
            col = _col("proxy_sources")
            col.delete_many({})
            if sources:
                col.insert_many([{"url": s} for s in sources])
        except Exception as e:
            logger.warning("mongo_sync.sync_proxy_sources error: %s", e)
    _bg(_do)


def load_proxy_sources() -> list:
    """Blocking: return proxy source URLs from MongoDB."""
    if not _enabled:
        return []
    try:
        return [doc["url"] for doc in _col("proxy_sources").find({})]
    except Exception as e:
        logger.warning("mongo_sync.load_proxy_sources: %s", e)
        return []


# ── Auto-connect on import ────────────────────────────────────────────────────
_try_connect()
