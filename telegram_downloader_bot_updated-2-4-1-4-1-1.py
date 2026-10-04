# -*- coding: utf-8 -*-
# =====================================================================
# telegram_downloader_bot.py
# نسخهٔ اصلاح‌شده: شامل دو تغییر خواسته‌شده:
# 1) برای هر لینک (از جمله اینستاگرام، X، فیس‌بوک و ...) ابتدا کیفیت‌ها شناسایی می‌شود و سپس از کاربر پرسیده می‌شود کجا می‌خواهد ذخیره کند.
# 2) انتخاب مسیر آپلود بر اساس اندازه فایل: زیر 30 مگابایت -> Bot API (توکن)، بالای 30 مگابایت -> Telethon.
# توجه: تاکید شده بود که سایر بخش‌ها دست‌نخورده باقی بمانند؛ فقط همین دو تغییر دقیق اعمال شده‌اند.
# =====================================================================

import os
import re
import time
import uuid
import json
import threading
import traceback
import shutil
import zipfile
import ssl
import random
import sqlite3
from queue import Queue, PriorityQueue
import itertools
from urllib.parse import urlparse
from pathlib import Path
from threading import Lock
from datetime import datetime, timedelta

import yt_dlp
import psutil
import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder, MultipartEncoderMonitor

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("telegram_downloader")

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InlineQueryResultArticle, InputTextMessageContent, ParseMode
from telegram.ext import Updater, MessageHandler, Filters, CallbackQueryHandler, CommandHandler, InlineQueryHandler
from telegram.error import NetworkError

# Telethon optional
try:
    from telethon import TelegramClient, errors as telethon_errors
except Exception:
    TelegramClient = None
    telethon_errors = None

# Google Drive optional (pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib)
try:
    from google.oauth2 import service_account as gdrive_service_account
    from google.oauth2.credentials import Credentials as GoogleUserCredentials
    from google_auth_oauthlib.flow import InstalledAppFlow as GoogleInstalledAppFlow
    from google.auth.transport.requests import Request as GoogleAuthRequest
    from googleapiclient.discovery import build as gdrive_build
    from googleapiclient.http import MediaFileUpload as GoogleMediaFileUpload
    from googleapiclient.errors import HttpError as GoogleHttpError
    GDRIVE_LIBS_AVAILABLE = True
except Exception:
    GDRIVE_LIBS_AVAILABLE = False

# Try to import urllib3 SSLError type for robust exception checks
try:
    from telegram.vendor.ptb_urllib3.urllib3.exceptions import SSLError as Urllib3SSLError
except Exception:
    try:
        from urllib3.exceptions import SSLError as Urllib3SSLError
    except Exception:
        Urllib3SSLError = None

# Try to detect cryptg (optional speedup for Telethon)
try:
    import cryptg  # type: ignore
    HAS_CRYPTG = True
except Exception:
    HAS_CRYPTG = False

# -------------------------
# پیکربندی (این مقادیر را در صورت نیاز تغییر بده)
# -------------------------
# توکن ربات را اینجا قرار بده یا از متغیر محیطی استفاده کن
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "8460981737:AAFVLyZbSkv6eIqVPXWnEuhjVJYy9TyCCUA")

# Telethon config (اختیاری)
TELETHON_API_ID = os.environ.get("TELETHON_API_ID", "39999874")
TELETHON_API_HASH = os.environ.get("TELETHON_API_HASH", "f6c320a19abd4975daaaa2f9d61601ff")
TELETHON_SESSION = os.environ.get("TELETHON_SESSION", "user_session")

# Force Telethon usage for uploads (set env FORCE_TELETHON_ALWAYS=1 to enable)
FORCE_TELETHON_ALWAYS = os.environ.get("FORCE_TELETHON_ALWAYS", "0") == "1"

# ادمین‌ها (با کاما جدا کن) — برای /stats
ADMIN_IDS = set()
for _aid in os.environ.get("ADMIN_IDS", "").replace(" ", "").split(","):
    if _aid.isdigit():
        ADMIN_IDS.add(int(_aid))
# ادمین پیش‌فرض (آیدی تلگرام)
ADMIN_IDS.add(7821647091)

# پروکسی اختیاری برای yt-dlp (مثال: http://127.0.0.1:7890 یا socks5://...)
YTDLP_PROXY = os.environ.get("YTDLP_PROXY", "").strip() or None

# محدودیت هر کاربر
MAX_CONCURRENT_PER_USER = int(os.environ.get("MAX_CONCURRENT_PER_USER", "2"))
MAX_DAILY_DOWNLOADS_PER_USER = int(os.environ.get("MAX_DAILY_DOWNLOADS_PER_USER", "50"))
MAX_DAILY_BYTES_PER_USER = int(os.environ.get("MAX_DAILY_BYTES_PER_USER", str(5 * 1024 * 1024 * 1024)))  # 5 GB

# پاکسازی فایل‌های موقت قدیمی‌تر از این ساعت
CLEANUP_MAX_AGE_HOURS = int(os.environ.get("CLEANUP_MAX_AGE_HOURS", "6"))
CLEANUP_INTERVAL_SECONDS = int(os.environ.get("CLEANUP_INTERVAL_SECONDS", "1800"))  # هر ۳۰ دقیقه

# مسیرها
DOWNLOAD_ROOT = os.path.join(os.getcwd(), "telegram_downloader")
os.makedirs(DOWNLOAD_ROOT, exist_ok=True)

USERS_ROOT = os.path.join(DOWNLOAD_ROOT, "users")
os.makedirs(USERS_ROOT, exist_ok=True)

# -------------------------
# پایگاه‌داده SQLite (به‌جای فایل‌های JSON پراکنده برای prefs/history)
# -------------------------
DB_PATH = os.environ.get("BOT_DB_PATH", os.path.join(DOWNLOAD_ROOT, "bot_data.sqlite3"))
DB_LOCK = Lock()


def get_db_conn():
    conn = sqlite3.connect(DB_PATH, timeout=15)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
    except Exception:
        pass
    return conn


def init_db():
    with DB_LOCK:
        conn = get_db_conn()
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS user_prefs (
                    user_id INTEGER PRIMARY KEY,
                    data TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS user_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    ts INTEGER NOT NULL,
                    record TEXT NOT NULL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_history_user ON user_history(user_id)")
            conn.commit()
        finally:
            conn.close()


init_db()

COOKIES_ROOT = os.path.join(DOWNLOAD_ROOT, "cookies")
os.makedirs(COOKIES_ROOT, exist_ok=True)

# -------------------------
# پیکربندی Google Drive
# -------------------------
# استخراج‌شده از لینک پوشهٔ گوگل درایو که کاربر تأیید کرده:
# https://drive.google.com/drive/folders/1h8X65wKdo-qAu1V_i1HO6aRSZvJx56_J
GOOGLE_DRIVE_ENABLED = os.environ.get("GOOGLE_DRIVE_ENABLED", "1") == "1"
GOOGLE_DRIVE_FOLDER_ID = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "1h8X65wKdo-qAu1V_i1HO6aRSZvJx56_J")
# چند پوشه درایو: youtube=ID1,instagram=ID2,tiktok=ID3,default=ID0
_DRIVE_FOLDER_MAP = {}
for _part in os.environ.get("GOOGLE_DRIVE_FOLDERS", "").split(","):
    _part = _part.strip()
    if "=" in _part:
        _k, _v = _part.split("=", 1)
        _DRIVE_FOLDER_MAP[_k.strip().lower()] = _v.strip()
# مدت پیش‌فرض ضبط لایو (ثانیه)
LIVE_RECORD_SECONDS = int(os.environ.get("LIVE_RECORD_SECONDS", "600"))
# حالت کم‌مصرف: اگر CPU یا RAM از این درصد بالاتر رفت، ورکر کمتر می‌گیرد
LOW_RESOURCE_MODE = os.environ.get("LOW_RESOURCE_MODE", "1") == "1"
LOW_RESOURCE_CPU_PCT = float(os.environ.get("LOW_RESOURCE_CPU_PCT", "85"))
LOW_RESOURCE_RAM_PCT = float(os.environ.get("LOW_RESOURCE_RAM_PCT", "85"))
# فایل اعتبارنامه: یا کلید Service Account (json) یا client_secret برای OAuth کاربر
GOOGLE_DRIVE_CREDENTIALS_FILE = os.environ.get(
    "GOOGLE_DRIVE_CREDENTIALS_FILE", os.path.join(DOWNLOAD_ROOT, "gdrive_credentials.json")
)
# در حالت OAuth کاربر، توکن دریافتی اینجا کش می‌شود تا هر بار نیاز به ورود مجدد نباشد
GOOGLE_DRIVE_TOKEN_FILE = os.environ.get(
    "GOOGLE_DRIVE_TOKEN_FILE", os.path.join(DOWNLOAD_ROOT, "gdrive_token.json")
)
# drive (کامل) پایدارتر از drive.file است و خطای invalid_scope را کمتر می‌دهد
GOOGLE_DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]
GOOGLE_DRIVE_CHUNK_SIZE = int(os.environ.get("GOOGLE_DRIVE_CHUNK_SIZE", str(8 * 1024 * 1024)))  # 8MB

_drive_service = None
_drive_service_lock = Lock()

LOG_ROOT = Path(os.getcwd()) / "telegram_bot_logs"
LOG_ROOT.mkdir(parents=True, exist_ok=True)
LOG_LOCK = Lock()

BACKUP_ROOT = Path(os.getcwd()) / "telegram_bot_backups"
BACKUP_ROOT.mkdir(parents=True, exist_ok=True)
BACKUP_LOCK = Lock()

# آمار سراسری (در حافظه)
GLOBAL_STATS = {
    "downloads_ok": 0,
    "downloads_fail": 0,
    "bytes_uploaded": 0,
    "started_at": time.time(),
    "avg_task_seconds": 25.0,  # میانگین متحرک مدت‌زمان هر کار، برای تخمین زمان صف
}
GLOBAL_STATS_LOCK = Lock()

# تاریخچه دانلود هر کاربر (در حافظه + فایل)
HISTORY_MAX = int(os.environ.get("HISTORY_MAX", "30"))
USER_HISTORY = {}  # user_id -> list of records
USER_HISTORY_LOCK = Lock()

# کاربران مسدود‌شده (ادمین)
BANNED_USERS = set()
BANNED_LOCK = Lock()

# پیگیری دانلود چندلینکی (batch)
# batch_id -> {user_id, chat_id, total, done, ok, fail, titles, created}
BATCHES = {}
BATCHES_LOCK = Lock()

# حالت تعمیرات و لیست سفید
MAINTENANCE_MODE = os.environ.get("MAINTENANCE_MODE", "0") == "1"
MAINTENANCE_LOCK = Lock()

WHITELIST_ENABLED = os.environ.get("WHITELIST_ENABLED", "0") == "1"
WHITELIST_USERS = set()
for _wid in os.environ.get("WHITELIST_IDS", "").replace(" ", "").split(","):
    if _wid.isdigit():
        WHITELIST_USERS.add(int(_wid))
WHITELIST_LOCK = Lock()

# کاربران منتظر خالی شدن صف
QUEUE_WAITERS = {}  # user_id -> chat_id
QUEUE_WAITERS_LOCK = Lock()

# دانلود زمان‌بندی‌شده: list of {run_at, task dict}
SCHEDULED_JOBS = []
SCHEDULED_LOCK = Lock()

# نظارت کانال: list of {user_id, chat_id, channel_url, last_ids set, quality}
CHANNEL_WATCHES = []
CHANNEL_WATCH_LOCK = Lock()

# پروکسی‌های چرخشی (اختیاری، با کاما)
PROXY_LIST = [p.strip() for p in os.environ.get("PROXY_LIST", "").split(",") if p.strip()]
PROXY_INDEX = 0
PROXY_LOCK = Lock()

# پارامترهای اجرایی
MAX_CONCURRENT_DOWNLOADS = int(os.environ.get("MAX_CONCURRENT_DOWNLOADS", "6"))
MIN_FREE_DISK_GB = 1
PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "8"))
REQUEST_TTL_SECONDS = int(os.environ.get("REQUEST_TTL_SECONDS", "300"))
YTDLP_SOCKET_TIMEOUT = int(os.environ.get("YTDLP_SOCKET_TIMEOUT", "60"))
YTDLP_RETRIES = int(os.environ.get("YTDLP_RETRIES", "5"))
YTDLP_FRAGMENT_RETRIES = int(os.environ.get("YTDLP_FRAGMENT_RETRIES", "5"))
# اندازه هر chunk و تعداد دانلود همزمان فرگمنت‌ها (تأثیر مستقیم روی سرعت دانلود)
YTDLP_HTTP_CHUNK_SIZE = int(os.environ.get("YTDLP_HTTP_CHUNK_SIZE", str(5 * 1024 * 1024)))  # 5 MB (قبلاً 1 MB)
YTDLP_CONCURRENT_FRAGMENTS = int(os.environ.get("YTDLP_CONCURRENT_FRAGMENTS", "6"))  # قبلاً 1 بود
# استفاده از aria2c در صورت نصب بودن (سرعت خیلی بالاتر)
YTDLP_USE_ARIA2 = os.environ.get("YTDLP_USE_ARIA2", "1") == "1"

HEAD_TIMEOUT = int(os.environ.get("HEAD_TIMEOUT", "4"))
MAX_HEAD_REQUESTS_PER_PARSE = int(os.environ.get("MAX_HEAD_REQUESTS_PER_PARSE", "1"))
USER_AGENT_HEAD = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"

MAIN_RESOLUTIONS = [144, 240, 360, 480, 720, 1080, 1440, 2160]

ANIMATION_INTERVAL = 0.20

NETWORK_MAX_RETRIES = 6
NETWORK_BACKOFF_BASE = 1.5
NETWORK_RETRY_SLEEP_MIN = 1.0
NETWORK_RETRY_SLEEP_MAX = 30.0

# Keep CHUNK_SIZE for Bot API multipart uploads (safe under 50 MiB)
CHUNK_SIZE = 48 * 1024 * 1024
MAX_SINGLE_UPLOAD_BYTES = 2 * 1024 * 1024 * 1024
# اندازه بخش آپلود Telethon (KB) — مقادیر بالاتر = آپلود سریع‌تر (باید توان ۲ باشند)
TELETHON_MIN_PART_KB = int(os.environ.get("TELETHON_MIN_PART_KB", "512"))

TELETHON_MAX_PART_KB = int(os.environ.get("TELETHON_MAX_PART_KB", "2048"))

# قابلیت‌های جدید
ENABLE_INLINE = os.environ.get("ENABLE_INLINE", "1") == "1"
ENABLE_GROUPS = os.environ.get("ENABLE_GROUPS", "1") == "1"
# فقط ادمین گروه بتواند لینک بفرستد (اگر 1)
GROUP_ADMIN_ONLY = os.environ.get("GROUP_ADMIN_ONLY", "0") == "1"
# کانال آرشیو: بعد از آپلود موفق، یک کپی به این chat_id هم برود
ARCHIVE_CHAT_ID = os.environ.get("ARCHIVE_CHAT_ID", "").strip() or None
if ARCHIVE_CHAT_ID and str(ARCHIVE_CHAT_ID).lstrip("-").isdigit():
    ARCHIVE_CHAT_ID = int(ARCHIVE_CHAT_ID)
else:
    ARCHIVE_CHAT_ID = None
# محدودیت سرعت دانلود yt-dlp (بایت بر ثانیه) — 0 = بدون محدودیت
DEFAULT_RATE_LIMIT = int(os.environ.get("YTDLP_RATE_LIMIT", "0") or 0)
# ازسرگیری دانلود ناقص
YTDLP_CONTINUE = os.environ.get("YTDLP_CONTINUE", "1") == "1"
# ضد سیل: حداقل فاصله بین دو درخواست لینک از یک کاربر (ثانیه)
USER_FLOOD_SECONDS = float(os.environ.get("USER_FLOOD_SECONDS", "2.5"))
USER_LAST_REQUEST = {}  # user_id -> timestamp
USER_LAST_REQUEST_LOCK = Lock()

# لیست سیاه دامنه (با کاما)
DOMAIN_BLACKLIST = set(
    d.strip().lower().lstrip(".")
    for d in os.environ.get("DOMAIN_BLACKLIST", "").split(",")
    if d.strip()
)
# پروکسی جدا برای یوتیوب / اینستاگرام
YOUTUBE_PROXY = os.environ.get("YOUTUBE_PROXY", "").strip() or None
INSTAGRAM_PROXY = os.environ.get("INSTAGRAM_PROXY", "").strip() or None
# هشدار فایل بزرگ (مگابایت)
LARGE_FILE_WARN_MB = int(os.environ.get("LARGE_FILE_WARN_MB", "200"))
# حذف پیام پیشرفت بعد از اتمام
DELETE_PROGRESS_MSG = os.environ.get("DELETE_PROGRESS_MSG", "1") == "1"
# Telethon بدون لاگین تعاملی (برای سرور)
TELETHON_NO_INTERACTIVE = os.environ.get("TELETHON_NO_INTERACTIVE", "1") == "1"
# چند کانال آرشیو: ARCHIVE_CHAT_IDS=-1001,-1002
_archive_ids = []
for _x in os.environ.get("ARCHIVE_CHAT_IDS", "").replace(" ", "").split(","):
    if _x.lstrip("-").isdigit():
        _archive_ids.append(int(_x))
if ARCHIVE_CHAT_ID and ARCHIVE_CHAT_ID not in _archive_ids:
    _archive_ids.append(ARCHIVE_CHAT_ID)
ARCHIVE_CHAT_IDS = _archive_ids
# اعلان ری‌استارت به ادمین
NOTIFY_ADMIN_ON_START = os.environ.get("NOTIFY_ADMIN_ON_START", "1") == "1"

# سقف حجم سخت (مگابایت) — ۰ = بدون سقف سخت
HARD_MAX_FILE_MB = int(os.environ.get("HARD_MAX_FILE_MB", "0") or 0)
# Webhook اختیاری (اگر خالی باشد از polling استفاده می‌شود)
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "").strip() or None
WEBHOOK_PORT = int(os.environ.get("WEBHOOK_PORT", "8443") or 8443)
WEBHOOK_LISTEN = os.environ.get("WEBHOOK_LISTEN", "0.0.0.0")
# Telethon session به‌صورت Base64 (بدون نیاز به فایل)
TELETHON_SESSION_B64 = os.environ.get("TELETHON_SESSION_B64", "").strip() or None
# گزارش روزانه به ادمین (ساعت محلی سرور، 0-23؛ -1 = خاموش)
DAILY_STATS_HOUR = int(os.environ.get("DAILY_STATS_HOUR", "9"))
# فایل لاگ خطاهای اخیر برای /errors
RECENT_ERRORS = []  # list of {ts, error, url, user_id}
RECENT_ERRORS_LOCK = Lock()
RECENT_ERRORS_MAX = 50


# -------------------------
# وضعیت‌ها و صف‌ها
# -------------------------
REQUESTS = {}            # request_id -> {url, formats, info, created, user_id, cancel, error, progress_msg_id}
REQUESTS_LOCK = threading.Lock()

download_queue = PriorityQueue()
_QUEUE_SEQ_COUNTER = itertools.count()
_QUEUE_SEQ_LOCK = Lock()
active_workers = 0
active_workers_lock = threading.Lock()

progress_map = {}        # task_id -> progress info

CANCEL_FLAGS = {}        # task_id -> {"cancel": bool, "owner_id": id}
CANCEL_LOCK = threading.Lock()

USER_MAP = {}
USER_MAP_LOCK = Lock()

# rate-limit: user_id -> {"active": int, "day": "YYYY-MM-DD", "count": int, "bytes": int}
USER_LIMITS = {}
USER_LIMITS_LOCK = Lock()

# صف نمایشی: task metadata برای /status
QUEUE_META = {}  # task_id -> {user_id, url, status, created}
QUEUE_META_LOCK = Lock()

# -------------------------
# Telethon client (lazy init) — با event loop اختصاصی در thread جدا
# -------------------------
telethon_client = None
telethon_loop = None
telethon_lock = threading.Lock()
telethon_ready = threading.Event()

def _telethon_loop_thread_main():
    """Thread دائمی برای event loop تلثون — جلوگیری از خطای asyncio loop must not change"""
    global telethon_client, telethon_loop
    import asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    telethon_loop = loop
    try:
        session_name = TELETHON_SESSION
        if TELETHON_SESSION_B64:
            try:
                import base64
                raw = base64.b64decode(TELETHON_SESSION_B64)
                session_path = os.path.join(os.getcwd(), str(TELETHON_SESSION) + ".session")
                if not os.path.exists(session_path):
                    with open(session_path, "wb") as sf:
                        sf.write(raw)
                    logger.info("Telethon session restored from TELETHON_SESSION_B64")
            except Exception as b64e:
                logger.warning("Failed to restore session from B64: %s", b64e)
        client = TelegramClient(session_name, int(TELETHON_API_ID), TELETHON_API_HASH, loop=loop)
        loop.run_until_complete(client.connect())
        if not loop.run_until_complete(client.is_user_authorized()):
            if TELETHON_NO_INTERACTIVE:
                logger.error(
                    "Telethon session unauthorized. On server interactive login is disabled. "
                    "Generate user_session.session locally, then copy it to the server."
                )
                raise RuntimeError("Telethon session not authorized (TELETHON_NO_INTERACTIVE=1)")
            client.start()
        else:
            try:
                loop.run_until_complete(client.get_me())
            except Exception:
                if TELETHON_NO_INTERACTIVE:
                    raise
                client.start()
        telethon_client = client
        if not HAS_CRYPTG:
            logger.warning("cryptg not available: Telethon will fall back to slower crypto.")
        else:
            logger.info("cryptg detected: Telethon will use optimized crypto for uploads.")
        logger.info("Telethon client started on dedicated loop thread.")
        telethon_ready.set()
        loop.run_forever()
    except Exception as e:
        logger.exception("Telethon loop thread failed: %s", e)
        telethon_client = None
        telethon_ready.set()
    finally:
        try:
            loop.close()
        except Exception:
            pass


def ensure_telethon_client():
    global telethon_client, telethon_loop
    if not TELETHON_API_ID or not TELETHON_API_HASH or TelegramClient is None:
        logger.info("Telethon API credentials not provided or Telethon not installed; Telethon disabled.")
        return None
    with telethon_lock:
        try:
            if telethon_client and getattr(telethon_client, "is_connected", lambda: False)():
                return telethon_client
        except Exception:
            telethon_client = None

        if telethon_client is None and not telethon_ready.is_set():
            t = threading.Thread(target=_telethon_loop_thread_main, name="telethon-loop", daemon=True)
            t.start()
            # wait up to 60s for login/connect
            if not telethon_ready.wait(timeout=60):
                logger.error("Telethon startup timed out")
                return None
        elif telethon_client is None:
            # previous attempt failed; try once more
            telethon_ready.clear()
            t = threading.Thread(target=_telethon_loop_thread_main, name="telethon-loop", daemon=True)
            t.start()
            telethon_ready.wait(timeout=60)

        return telethon_client

# -------------------------
# Google Drive: احراز هویت و آپلود با گزارش پیشرفت
# -------------------------
def ensure_drive_service():
    """
    سرویس Google Drive را می‌سازد (یا از کش برمی‌گرداند).
    از دو حالت پشتیبانی می‌کند:
      1) Service Account: فایل credentials از نوع "service_account" — نیاز به تعامل کاربر ندارد
         و برای اجرا روی سرور مناسب است. کافیست فایل json کلید سرویس‌اکانت را
         در مسیر GOOGLE_DRIVE_CREDENTIALS_FILE قرار دهی و پوشهٔ درایو را با ایمیل
         سرویس‌اکانت به اشتراک بگذاری (Editor).
      2) OAuth کاربر: فایل client_secret را در GOOGLE_DRIVE_CREDENTIALS_FILE بگذار؛
         چون این ربات روی سرور (بدون مرورگر) اجرا می‌شود، توکن (GOOGLE_DRIVE_TOKEN_FILE)
         باید یک‌بار به‌صورت آفلاین/لوکال تولید و روی سرور کپی شود. اگر توکن معتبر
         موجود باشد، به‌صورت خودکار refresh می‌شود.
    """
    global _drive_service
    if not GOOGLE_DRIVE_ENABLED:
        raise RuntimeError("قابلیت Google Drive غیرفعال است (GOOGLE_DRIVE_ENABLED=0).")
    if not GDRIVE_LIBS_AVAILABLE:
        raise RuntimeError(
            "کتابخانه‌های گوگل درایو نصب نیستند. نصب کن:\n"
            "pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib"
        )
    with _drive_service_lock:
        if _drive_service is not None:
            return _drive_service

        if not os.path.exists(GOOGLE_DRIVE_CREDENTIALS_FILE):
            raise RuntimeError(
                f"فایل اعتبارنامهٔ گوگل درایو پیدا نشد: {GOOGLE_DRIVE_CREDENTIALS_FILE}\n"
                "یک فایل Service Account یا OAuth client_secret در این مسیر قرار بده."
            )

        try:
            with open(GOOGLE_DRIVE_CREDENTIALS_FILE, "r", encoding="utf-8") as f:
                cred_data = json.load(f)
        except Exception as e:
            raise RuntimeError(f"خواندن فایل اعتبارنامهٔ گوگل درایو ناموفق بود: {e}")

        creds = None
        try:
            if cred_data.get("type") == "service_account":
                creds = gdrive_service_account.Credentials.from_service_account_file(
                    GOOGLE_DRIVE_CREDENTIALS_FILE, scopes=GOOGLE_DRIVE_SCOPES
                )
            else:
                # OAuth کاربر — اگر توکن قدیمی با scope متفاوت باشد، پاک می‌شود
                if os.path.exists(GOOGLE_DRIVE_TOKEN_FILE):
                    try:
                        creds = GoogleUserCredentials.from_authorized_user_file(
                            GOOGLE_DRIVE_TOKEN_FILE, GOOGLE_DRIVE_SCOPES
                        )
                        # اگر scope توکن با فعلی هم‌خوان نباشد، refresh/استفاده را رد کن
                        if creds and getattr(creds, "scopes", None):
                            if not set(GOOGLE_DRIVE_SCOPES).issubset(set(creds.scopes or [])):
                                creds = None
                                try:
                                    os.remove(GOOGLE_DRIVE_TOKEN_FILE)
                                except Exception:
                                    pass
                    except Exception:
                        creds = None
                if not creds or not creds.valid:
                    if creds and creds.expired and creds.refresh_token:
                        try:
                            creds.refresh(GoogleAuthRequest())
                        except Exception as refresh_err:
                            # invalid_scope و مشابه → توکن را پاک کن تا کاربر دوباره لاگین کند
                            err_s = str(refresh_err).lower()
                            if "invalid_scope" in err_s or "bad request" in err_s:
                                try:
                                    os.remove(GOOGLE_DRIVE_TOKEN_FILE)
                                except Exception:
                                    pass
                                raise RuntimeError(
                                    "توکن گوگل‌درایو با scope فعلی ناسازگار است. "
                                    "فایل gdrive_token.json پاک شد. یک‌بار روی سیستم با مرورگر "
                                    "لاگین کن تا توکن جدید ساخته شود، بعد روی سرور کپی کن."
                                ) from refresh_err
                            raise
                    else:
                        # روی سرور بدون مرورگر: باید یک‌بار محلی اجرا و token.json کپی شود.
                        flow = GoogleInstalledAppFlow.from_client_secrets_file(
                            GOOGLE_DRIVE_CREDENTIALS_FILE, GOOGLE_DRIVE_SCOPES
                        )
                        creds = flow.run_local_server(port=0)
                    try:
                        with open(GOOGLE_DRIVE_TOKEN_FILE, "w", encoding="utf-8") as tf:
                            tf.write(creds.to_json())
                    except Exception:
                        pass
        except RuntimeError:
            raise
        except Exception as e:
            err_s = str(e).lower()
            if "invalid_scope" in err_s:
                try:
                    if os.path.exists(GOOGLE_DRIVE_TOKEN_FILE):
                        os.remove(GOOGLE_DRIVE_TOKEN_FILE)
                except Exception:
                    pass
                raise RuntimeError(
                    "خطای invalid_scope در گوگل‌درایو. توکن قدیمی پاک شد.\n"
                    "اگر Service Account است: در Google Cloud Console اسکوپ Drive را فعال کن "
                    "و ایمیل سرویس‌اکانت را Editor روی فولدر کن.\n"
                    "اگر OAuth است: یک‌بار محلی لاگین کن تا token جدید با scope "
                    "https://www.googleapis.com/auth/drive ساخته شود."
                ) from e
            raise RuntimeError(f"احراز هویت گوگل درایو ناموفق بود: {e}")

        _drive_service = gdrive_build("drive", "v3", credentials=creds, cache_discovery=False)
        return _drive_service


_drive_subfolder_cache = {}
_drive_subfolder_cache_lock = Lock()


def ensure_drive_subfolder(service, parent_id, name):
    """
    یک ساب‌فولدر با نام مشخص زیر parent_id پیدا می‌کند یا در صورت نبود می‌سازد.
    نتیجه کش می‌شود تا برای هر آپلود دوباره جستجو نشود.
    """
    if not name:
        return parent_id
    cache_key = (parent_id, name)
    with _drive_subfolder_cache_lock:
        cached = _drive_subfolder_cache.get(cache_key)
        if cached:
            return cached
    safe_name = name.replace("'", "\\'")
    query = (
        f"name = '{safe_name}' and mimeType = 'application/vnd.google-apps.folder' "
        f"and trashed = false"
    )
    if parent_id:
        query += f" and '{parent_id}' in parents"
    try:
        res = service.files().list(q=query, fields="files(id, name)", pageSize=1).execute()
        files = res.get("files") or []
        if files:
            folder_id = files[0]["id"]
        else:
            meta = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
            if parent_id:
                meta["parents"] = [parent_id]
            created = service.files().create(body=meta, fields="id").execute()
            folder_id = created["id"]
        with _drive_subfolder_cache_lock:
            _drive_subfolder_cache[cache_key] = folder_id
        return folder_id
    except Exception:
        # اگر ساخت/جستجوی ساب‌فولدر ناموفق بود، به همان پوشهٔ اصلی برگرد
        return parent_id


def get_drive_free_space_bytes(service):
    """
    فضای آزاد باقی‌مانده در گوگل‌درایو را برمی‌گرداند (بایت).
    اگر حساب Unlimited/Workspace بدون سقف مشخص باشد یا خطایی رخ دهد، None برمی‌گرداند
    (یعنی بررسی رد شود).
    """
    try:
        about = service.about().get(fields="storageQuota").execute()
        quota = about.get("storageQuota") or {}
        limit = quota.get("limit")
        usage = quota.get("usage")
        if limit is None or usage is None:
            return None
        return max(0, int(limit) - int(usage))
    except Exception:
        return None


def parse_drive_file_id(text):
    """استخراج fileId از یک لینک گوگل‌درایو یا پذیرفتن مستقیم خود fileId."""
    text = (text or "").strip()
    if not text:
        return None
    m = re.search(r"/d/([a-zA-Z0-9_-]{10,})", text)
    if m:
        return m.group(1)
    m = re.search(r"[?&]id=([a-zA-Z0-9_-]{10,})", text)
    if m:
        return m.group(1)
    if re.fullmatch(r"[a-zA-Z0-9_-]{10,}", text):
        return text
    return None


def download_from_drive_with_progress(file_id, dest_path, progress_callback=None, task_id=None):
    """دانلود یک فایل از گوگل‌درایو به مسیر محلی dest_path، با گزارش پیشرفت."""
    from googleapiclient.http import MediaIoBaseDownload
    service = ensure_drive_service()
    meta = service.files().get(fileId=file_id, fields="name, size, mimeType").execute()
    total_size = int(meta.get("size") or 0)

    request = service.files().get_media(fileId=file_id)
    with open(dest_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request, chunksize=GOOGLE_DRIVE_CHUNK_SIZE)
        done = False
        while not done:
            if task_id:
                with CANCEL_LOCK:
                    flag = CANCEL_FLAGS.get(task_id)
                    if flag and flag.get("cancel"):
                        raise Exception("Download cancelled by user")
            status, done = downloader.next_chunk()
            if status and progress_callback:
                sent = int(status.resumable_progress)
                progress_callback(sent, total_size or sent)
    if progress_callback:
        progress_callback(total_size or os.path.getsize(dest_path), total_size or os.path.getsize(dest_path))
    return {"name": meta.get("name"), "size": total_size or os.path.getsize(dest_path)}


def upload_to_drive_with_progress(file_path, folder_id=None, progress_callback=None, task_id=None, mime_type=None, subfolder_name=None):
    """
    آپلود فایل به گوگل درایو به‌صورت resumable با گزارش پیشرفت (sent, total).
    اگر subfolder_name داده شود، فایل داخل یک زیرپوشه با آن نام (زیر folder_id) قرار می‌گیرد.
    قبل از آپلود، فضای آزاد درایو بررسی می‌شود.
    خروجی: dict شامل id و link قابل‌اشتراک‌گذاری فایل.
    """
    service = ensure_drive_service()
    folder_id = folder_id or GOOGLE_DRIVE_FOLDER_ID
    file_name = os.path.basename(file_path)
    total_size = os.path.getsize(file_path)

    free_space = get_drive_free_space_bytes(service)
    if free_space is not None and total_size > free_space:
        raise RuntimeError(
            f"فضای آزاد Google Drive کافی نیست (آزاد: {human_size(free_space)}، نیاز: {human_size(total_size)})."
        )

    if subfolder_name:
        folder_id = ensure_drive_subfolder(service, folder_id, subfolder_name)

    file_metadata = {"name": file_name}
    if folder_id:
        file_metadata["parents"] = [folder_id]

    media = GoogleMediaFileUpload(
        file_path, mimetype=mime_type, resumable=True, chunksize=GOOGLE_DRIVE_CHUNK_SIZE
    )
    request = service.files().create(body=file_metadata, media_body=media, fields="id, webViewLink, webContentLink")

    response = None
    last_exc = None
    attempts = 0
    max_attempts = 6
    while response is None:
        if task_id:
            with CANCEL_LOCK:
                flag = CANCEL_FLAGS.get(task_id)
                if flag and flag.get("cancel"):
                    raise Exception("Upload cancelled by user")
        try:
            status, response = request.next_chunk()
            attempts = 0
            if status and progress_callback:
                sent = int(status.resumable_progress)
                progress_callback(sent, total_size)
        except Exception as e:
            last_exc = e
            attempts += 1
            is_http = GDRIVE_LIBS_AVAILABLE and isinstance(e, GoogleHttpError)
            if attempts >= max_attempts:
                raise
            if is_http and getattr(e, "resp", None) is not None and e.resp.status in (403, 429, 500, 502, 503, 504):
                time.sleep(min(20, 2 ** attempts))
                continue
            if isinstance(e, (requests.exceptions.RequestException, ssl.SSLError)):
                time.sleep(min(20, 2 ** attempts))
                continue
            raise

    if progress_callback:
        progress_callback(total_size, total_size)

    file_id = response.get("id")
    # تلاش برای عمومی/قابل‌مشاهده کردن لینک (در صورت نداشتن دسترسی کافی، بی‌خطر رد می‌شود)
    try:
        service.permissions().create(fileId=file_id, body={"role": "reader", "type": "anyone"}).execute()
    except Exception:
        pass

    link = response.get("webViewLink") or f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"
    return {"id": file_id, "link": link, "name": file_name, "size": total_size}

# -------------------------
# توابع کمکی
# -------------------------
def ensure_user_dir(user_id_or_username):
    if isinstance(user_id_or_username, str) and user_id_or_username:
        safe = re.sub(r'[^0-9A-Za-z_\-@]', '_', user_id_or_username)
        user_dir = Path(USERS_ROOT) / f"user_{safe}"
    else:
        user_dir = Path(USERS_ROOT) / f"user_{user_id_or_username}"
    user_dir.mkdir(parents=True, exist_ok=True)
    (user_dir / "downloads").mkdir(exist_ok=True)
    (user_dir / "logs").mkdir(exist_ok=True)
    (user_dir / "backups").mkdir(exist_ok=True)
    return user_dir


def organized_subpath(info, url=None):
    """
    ساخت مسیر نسبی پوشه‌بندی: Source/Uploader/YYYY-MM
    مثال: YouTube/MrBeast/2026-10
    """
    try:
        net = (urlparse(url or (info or {}).get("webpage_url") or "").netloc or "").lower()
    except Exception:
        net = ""
    if "youtube" in net or "youtu.be" in net:
        source = "YouTube"
    elif "instagram" in net:
        source = "Instagram"
    elif "tiktok" in net:
        source = "TikTok"
    elif "x.com" in net or "twitter" in net:
        source = "X"
    elif "facebook" in net or "fb.watch" in net:
        source = "Facebook"
    else:
        source = "Other"
    uploader = ""
    if info:
        uploader = info.get("uploader") or info.get("channel") or info.get("uploader_id") or info.get("creator") or ""
    uploader = sanitize_name(uploader or "Unknown", max_len=40) or "Unknown"
    month = datetime.now().strftime("%Y-%m")
    return f"{source}/{uploader}/{month}"


def smart_compress_file(src_path, progress_msg_fn=None, timeout=900, force_crf=None):
    """
    فشرده‌سازی با CRF پویا یا اجباری.
    force_crf: اگر ست شود همان استفاده می‌شود.
    """
    import subprocess
    try:
        size = os.path.getsize(src_path)
    except Exception:
        return src_path
    if force_crf is not None:
        crf = str(force_crf)
    else:
        if size < 20 * 1024 * 1024:
            return src_path
        if size >= 500 * 1024 * 1024:
            crf = "30"
        elif size >= 200 * 1024 * 1024:
            crf = "28"
        elif size >= 80 * 1024 * 1024:
            crf = "26"
        else:
            crf = "24"
    ext = os.path.splitext(src_path)[1].lower()
    if ext not in (".mp4", ".mkv", ".webm", ".mov", ".avi", ".m4v"):
        return src_path
    out_path = src_path + f".smart_crf{crf}.mp4"
    if progress_msg_fn:
        try:
            progress_msg_fn(f"🗜 فشرده‌سازی هوشمند (CRF {crf})...")
        except Exception:
            pass
    cmd = [
        "ffmpeg", "-y", "-i", src_path,
        "-c:v", "libx264", "-preset", "veryfast", "-crf", crf,
        "-c:a", "aac", "-b:a", "128k",
        "-movflags", "+faststart",
        out_path,
    ]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=timeout)
        if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
            new_size = os.path.getsize(out_path)
            # فقط اگر واقعاً کوچک‌تر شد جایگزین کن
            if new_size < size * 0.95:
                try:
                    os.remove(src_path)
                except Exception:
                    pass
                return out_path
            else:
                try:
                    os.remove(out_path)
                except Exception:
                    pass
    except Exception as e:
        logger.warning("smart_compress failed: %s", e)
        try:
            if os.path.exists(out_path):
                os.remove(out_path)
        except Exception:
            pass
    return src_path


# -------------------------
# لینک موقت با انقضا (توکن → فایل)
# -------------------------
TEMP_LINKS = {}  # token -> {path, user_id, expires, title, size}
TEMP_LINKS_LOCK = Lock()
TEMP_LINK_TTL_HOURS = int(os.environ.get("TEMP_LINK_TTL_HOURS", "24"))
TEMP_LINKS_ROOT = Path(DOWNLOAD_ROOT) / "temp_links"
TEMP_LINKS_ROOT.mkdir(parents=True, exist_ok=True)


def create_temp_link(file_path, user_id, title="", ttl_hours=None):
    """کپی فایل به temp_links و برگرداندن توکن"""
    ttl = ttl_hours if ttl_hours is not None else TEMP_LINK_TTL_HOURS
    token = uuid.uuid4().hex[:12]
    ext = os.path.splitext(file_path)[1] or ".bin"
    dest = TEMP_LINKS_ROOT / f"{token}{ext}"
    try:
        shutil.copy2(file_path, str(dest))
    except Exception as e:
        raise RuntimeError(f"کپی فایل برای لینک موقت ناموفق: {e}")
    entry = {
        "path": str(dest),
        "user_id": int(user_id),
        "expires": time.time() + ttl * 3600,
        "title": (title or os.path.basename(file_path))[:120],
        "size": os.path.getsize(dest),
        "created": time.time(),
    }
    with TEMP_LINKS_LOCK:
        TEMP_LINKS[token] = entry
    return token, entry


def get_temp_link(token):
    with TEMP_LINKS_LOCK:
        entry = TEMP_LINKS.get(token)
    if not entry:
        return None, "توکن نامعتبر یا منقضی شده."
    if time.time() > entry.get("expires", 0):
        try:
            if os.path.exists(entry["path"]):
                os.remove(entry["path"])
        except Exception:
            pass
        with TEMP_LINKS_LOCK:
            TEMP_LINKS.pop(token, None)
        return None, "این لینک منقضی شده است."
    if not os.path.exists(entry["path"]):
        with TEMP_LINKS_LOCK:
            TEMP_LINKS.pop(token, None)
        return None, "فایل دیگر روی سرور نیست."
    return entry, None


def cleanup_temp_links():
    now = time.time()
    to_del = []
    with TEMP_LINKS_LOCK:
        for tok, ent in list(TEMP_LINKS.items()):
            if now > ent.get("expires", 0):
                to_del.append(tok)
        for tok in to_del:
            ent = TEMP_LINKS.pop(tok, None)
            if ent:
                try:
                    if os.path.exists(ent["path"]):
                        os.remove(ent["path"])
                except Exception:
                    pass
    # پاکسازی فایل‌های یتیم در پوشه
    try:
        for p in TEMP_LINKS_ROOT.iterdir():
            if p.is_file() and time.time() - p.stat().st_mtime > TEMP_LINK_TTL_HOURS * 3600 + 3600:
                try:
                    p.unlink()
                except Exception:
                    pass
    except Exception:
        pass

def append_user_log(user_id_or_username, record: dict):
    try:
        udir = ensure_user_dir(user_id_or_username)
        log_file = udir / "logs" / "events.log"
        record["ts"] = int(time.time())
        record["datetime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with LOG_LOCK:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception:
        pass


# -------------------------
# سیستم امتیاز و سطح
# -------------------------
# سطح ۱: 0+   | سطح ۲: 50+  | سطح ۳: 150+ | سطح ۴: 400+ | سطح ۵: 1000+
LEVEL_THRESHOLDS = [0, 50, 150, 400, 1000, 2500]
# ضریب سقف روزانه بر اساس سطح
LEVEL_DAILY_MULT = {1: 1.0, 2: 1.25, 3: 1.5, 4: 2.0, 5: 3.0, 6: 4.0}
POINTS_PER_SUCCESS = int(os.environ.get("POINTS_PER_SUCCESS", "5"))
POINTS_PER_FAIL = int(os.environ.get("POINTS_PER_FAIL", "0"))


def calc_level(points):
    pts = int(points or 0)
    level = 1
    for i, thr in enumerate(LEVEL_THRESHOLDS):
        if pts >= thr:
            level = i + 1
    return min(level, 6)


def add_user_points(user_id, delta, reason=""):
    try:
        prefs = load_user_prefs(user_id)
        pts = int(prefs.get("points") or 0) + int(delta)
        if pts < 0:
            pts = 0
        old_level = int(prefs.get("level") or 1)
        prefs["points"] = pts
        prefs["level"] = calc_level(pts)
        save_user_prefs(user_id, prefs)
        return prefs["points"], prefs["level"], old_level
    except Exception:
        return 0, 1, 1


def on_download_success(user_id, bot=None, chat_id=None):
    """افزودن امتیاز بعد از دانلود موفق و اعلان ارتقای سطح"""
    try:
        pts, level, old_level = add_user_points(user_id, POINTS_PER_SUCCESS, reason="success")
        if bot and chat_id and level > old_level:
            try:
                bot.send_message(
                    chat_id=chat_id,
                    text=f"🎉 ارتقا سطح! الان سطح {level} هستی\n⭐ امتیاز: {pts}\nسقف روزانه دانلود افزایش یافت.",
                )
            except Exception:
                pass
        return pts, level
    except Exception:
        return 0, 1


def get_user_limits_scaled(user_id):
    """سقف روزانه با ضریب سطح کاربر"""
    prefs = load_user_prefs(user_id)
    level = int(prefs.get("level") or calc_level(prefs.get("points")))
    mult = LEVEL_DAILY_MULT.get(level, 1.0)
    max_dl = int(MAX_DAILY_DOWNLOADS_PER_USER * mult)
    max_bytes = int(MAX_DAILY_BYTES_PER_USER * mult)
    return max_dl, max_bytes, level, mult


def is_instagram_story_url(url):
    try:
        u = (url or "").lower()
        return any(x in u for x in (
            "instagram.com/stories/",
            "instagram.com/s/",
            "/stories/",
            "instagram.com/highlights/",
            "/highlights/",
        ))
    except Exception:
        return False


def make_clip_ffmpeg(src_path, dest_path, start_sec, end_sec, timeout=600):
    """برش بازه و تبدیل به mp4 سبک"""
    import subprocess
    dur = max(0.1, float(end_sec) - float(start_sec))
    cmd = [
        "ffmpeg", "-y", "-ss", str(start_sec), "-i", src_path,
        "-t", str(dur),
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
        "-c:a", "aac", "-b:a", "128k",
        "-movflags", "+faststart",
        dest_path,
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=timeout)
    return dest_path


def make_gif_ffmpeg(src_path, dest_path, start_sec=0, duration=5, width=480, fps=12, timeout=300):
    """ساخت GIF از بازه مشخص"""
    import subprocess
    # palette برای کیفیت بهتر GIF
    palette = dest_path + ".palette.png"
    ss = str(float(start_sec or 0))
    t = str(float(duration or 5))
    scale = f"fps={int(fps)},scale={int(width)}:-1:flags=lanczos"
    try:
        cmd1 = [
            "ffmpeg", "-y", "-ss", ss, "-t", t, "-i", src_path,
            "-vf", f"{scale},palettegen=stats_mode=diff",
            palette,
        ]
        subprocess.run(cmd1, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=timeout)
        cmd2 = [
            "ffmpeg", "-y", "-ss", ss, "-t", t, "-i", src_path, "-i", palette,
            "-lavfi", f"{scale}[x];[x][1:v]paletteuse=dither=bayer:bayer_scale=5",
            dest_path,
        ]
        subprocess.run(cmd2, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=timeout)
    finally:
        try:
            if os.path.exists(palette):
                os.remove(palette)
        except Exception:
            pass
    return dest_path


def translate_subtitle_file(sub_path, target_lang="fa"):
    """
    ترجمه فایل زیرنویس .srt/.vtt
    اگر deep_translator نصب باشد از آن استفاده می‌کند؛ وگرنه خطا برمی‌گرداند.
    """
    try:
        from deep_translator import GoogleTranslator
    except Exception:
        raise RuntimeError(
            "برای ترجمه زیرنویس نصب کن:\npip install deep-translator"
        )
    ext = os.path.splitext(sub_path)[1].lower()
    out_path = sub_path.rsplit(".", 1)[0] + f".{target_lang}{ext}"
    with open(sub_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()
    # بلوک‌های متن را جدا کن (ساده‌سازی برای srt/vtt)
    lines = content.splitlines()
    out_lines = []
    buf = []
    translator = GoogleTranslator(source="auto", target=target_lang)

    def flush_buf():
        nonlocal buf, out_lines
        if not buf:
            return
        text = " ".join(buf).strip()
        if text:
            try:
                # محدودیت طول API
                chunks = [text[i:i+4500] for i in range(0, len(text), 4500)]
                translated = " ".join(translator.translate(c) for c in chunks)
                out_lines.append(translated)
            except Exception:
                out_lines.append(text)
        buf = []

    for line in lines:
        stripped = line.strip()
        # شماره ایندکس یا تایم‌کد را دست نزن
        if not stripped:
            flush_buf()
            out_lines.append(line)
        elif re.match(r"^\d+$", stripped):
            flush_buf()
            out_lines.append(line)
        elif re.match(r"^\d{1,2}:\d{2}", stripped) or "-->" in stripped:
            flush_buf()
            out_lines.append(line)
        elif stripped.startswith("WEBVTT") or stripped.startswith("NOTE") or stripped.startswith("STYLE"):
            flush_buf()
            out_lines.append(line)
        else:
            buf.append(stripped)
    flush_buf()
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(out_lines))
    return out_path


# -------------------------
# تنظیمات کاربر (prefs)
# -------------------------
def _prefs_path(user_id):
    # مسیر فایل JSON قدیمی — فقط برای مهاجرت یک‌باره به SQLite نگه داشته شده
    return ensure_user_dir(user_id) / "prefs.json"

def load_user_prefs(user_id):
    defaults = {
        "default_mode": "telegram",       # telegram | local | drive
        "default_quality": "best",
        "want_subtitles": False,
        "subtitle_langs": "fa,en",
        "compress": False,
        "send_thumbnail": True,
        "quick_mode": False,              # رد کردن تأیید و انتخاب کیفیت
        "preferred_height": 720,          # کیفیت ترجیحی برای auto
        "max_auto_size_mb": 50,           # بهترین زیر این حجم (مگ)
        "audio_format": "mp3",            # mp3 | m4a | opus
        "caption_template": "{title}\n{uploader}\n{url}",
        "send_as_video": True,            # sendVideo وقتی ممکن باشد
        "forward_chat_id": None,          # ارسال کپی به چت دیگر
        "burn_subtitles": False,          # سوزاندن زیرنویس روی ویدیو
        "watermark_text": "",             # واترمارک متنی
        "force_mp4": False,               # تبدیل اجباری به mp4
        "notify_queue_empty": False,      # اعلان وقتی صف خالی شد
        "lang": "fa",                     # fa | en
        "rate_limit_bps": 0,              # محدودیت سرعت دانلود (0=آزاد)
        "mirror_drive": False,            # همزمان تلگرام + Drive
        "zip_batch": False,               # فشرده‌سازی چندفایلی
        "allow_groups": True,
        "auto_forward": False,            # فوروارد خودکار بعد از هر دانلود موفق
        "multi_select": False,            # حالت انتخاب چند کیفیت
        "smart_compress": False,          # فشرده‌سازی خودکار اگر فایل بزرگ باشد
        "smart_compress_mb": 80,          # آستانه فشرده‌سازی هوشمند (مگابایت)
        "organized_folders": True,        # پوشه‌بندی خودکار بر اساس کانال/تاریخ
        "points": 0,                      # امتیاز کاربر
        "level": 1,                       # سطح بر اساس امتیاز
        "translate_subs": False,          # ترجمه خودکار زیرنویس
        "translate_to": "fa",             # زبان مقصد ترجمه
    }
    row = None
    try:
        with DB_LOCK:
            conn = get_db_conn()
            try:
                cur = conn.execute("SELECT data FROM user_prefs WHERE user_id=?", (int(user_id),))
                row = cur.fetchone()
            finally:
                conn.close()
    except Exception:
        row = None
    if row:
        try:
            data = json.loads(row[0])
            defaults.update(data or {})
            return defaults
        except Exception:
            pass
    # مهاجرت یک‌باره از فایل JSON قدیمی در صورت وجود
    try:
        p = _prefs_path(user_id)
        if p.exists():
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
            defaults.update(data or {})
            save_user_prefs(user_id, defaults)
    except Exception:
        pass
    return defaults

def save_user_prefs(user_id, prefs: dict):
    try:
        with DB_LOCK:
            conn = get_db_conn()
            try:
                conn.execute(
                    "INSERT INTO user_prefs(user_id, data) VALUES (?, ?) "
                    "ON CONFLICT(user_id) DO UPDATE SET data=excluded.data",
                    (int(user_id), json.dumps(prefs, ensure_ascii=False))
                )
                conn.commit()
            finally:
                conn.close()
    except Exception:
        pass


def _history_path(user_id):
    # مسیر فایل JSON قدیمی — فقط برای مهاجرت یک‌باره به SQLite نگه داشته شده
    return ensure_user_dir(user_id) / "history.json"


def load_user_history(user_id):
    with USER_HISTORY_LOCK:
        if user_id in USER_HISTORY:
            return list(USER_HISTORY[user_id])
    records = []
    try:
        with DB_LOCK:
            conn = get_db_conn()
            try:
                cur = conn.execute(
                    "SELECT record FROM user_history WHERE user_id=? ORDER BY ts ASC, id ASC",
                    (int(user_id),)
                )
                rows = cur.fetchall()
            finally:
                conn.close()
        records = [json.loads(r[0]) for r in rows]
    except Exception:
        records = []
    if not records:
        # مهاجرت یک‌باره از فایل JSON قدیمی در صورت وجود
        try:
            p = _history_path(user_id)
            if p.exists():
                with open(p, "r", encoding="utf-8") as f:
                    data = json.load(f) or []
                for rec in data:
                    append_user_history(user_id, rec)
                records = data
        except Exception:
            pass
    with USER_HISTORY_LOCK:
        USER_HISTORY[user_id] = list(records)[-HISTORY_MAX:]
    return list(USER_HISTORY[user_id])


def append_user_history(user_id, record: dict):
    """record: url, title, format_id, size, status, mode"""
    try:
        record = dict(record)
        record.setdefault("ts", int(time.time()))
        record.setdefault("datetime", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        with USER_HISTORY_LOCK:
            lst = USER_HISTORY.get(user_id) or []
            lst.append(record)
            USER_HISTORY[user_id] = lst[-HISTORY_MAX:]
        with DB_LOCK:
            conn = get_db_conn()
            try:
                conn.execute(
                    "INSERT INTO user_history(user_id, ts, record) VALUES (?, ?, ?)",
                    (int(user_id), int(record["ts"]), json.dumps(record, ensure_ascii=False))
                )
                conn.commit()
                # نگه‌داشتن حداکثر HISTORY_MAX رکورد به ازای هر کاربر
                cur = conn.execute("SELECT COUNT(*) FROM user_history WHERE user_id=?", (int(user_id),))
                cnt = cur.fetchone()[0]
                if cnt > HISTORY_MAX:
                    conn.execute(
                        "DELETE FROM user_history WHERE id IN ("
                        "  SELECT id FROM user_history WHERE user_id=? ORDER BY ts ASC, id ASC LIMIT ?"
                        ")",
                        (int(user_id), cnt - HISTORY_MAX)
                    )
                    conn.commit()
            finally:
                conn.close()
    except Exception:
        pass


def is_user_banned(user_id):
    with BANNED_LOCK:
        return user_id in BANNED_USERS


def ban_user(user_id):
    with BANNED_LOCK:
        BANNED_USERS.add(int(user_id))


def unban_user(user_id):
    with BANNED_LOCK:
        BANNED_USERS.discard(int(user_id))


def format_caption(prefs, info, url):
    """ساخت کپشن از قالب کاربر"""
    try:
        title = (info.get("title") if info else "") or ""
        uploader = (info.get("uploader") if info else "") or ""
        webpage = (info.get("webpage_url") if info else "") or url or ""
        default_tpl = "{title}\n{uploader}\n{url}"
        tpl = (prefs or {}).get("caption_template") or default_tpl
        # اگر از JSON به صورت \\n ذخیره شده باشد
        if "\\n" in tpl and "\n" not in tpl.replace("\\n", ""):
            tpl = tpl.replace("\\n", "\n")
        elif tpl.count("\\n") and "\n" not in tpl:
            tpl = tpl.replace("\\n", "\n")
        # ساده‌سازی: همیشه \\n را به newline تبدیل کن (اگر هنوز escaped است)
        if "\\" + "n" in repr(tpl):
            pass
        tpl = tpl.replace("\\n", "\n")
        cap = tpl.format(title=title[:200], uploader=uploader[:80], url=webpage[:120], webpage=webpage[:120])
        return (cap or "")[:1024]
    except Exception:
        parts = []
        if info and info.get("title"):
            parts.append("🎬 " + str(info.get("title"))[:200])
        if info and info.get("uploader"):
            parts.append("📺 " + str(info.get("uploader"))[:80])
        if url:
            parts.append(str(url)[:120])
        return "\n".join(parts)[:1024]


# =====================================================================
# قابلیت‌های جدید (هوش مصنوعی + DeepSeek + ادیت + فصل + پردازش فایل کاربر)
# =====================================================================

DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "").strip() or None
DEEPSEEK_BASE_URL = os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com").rstrip("/")
DEEPSEEK_MODEL = os.environ.get("DEEPSEEK_MODEL", "deepseek-chat")

# وضعیت آخرین خطای مالی DeepSeek (برای اطلاع به کاربر)
_DEEPSEEK_BILLING_LOCK = Lock()
_DEEPSEEK_BILLING_MSG = None  # آخرین پیام مالی برای نمایش


class DeepSeekBillingError(Exception):
    """اعتبار تمام شده یا نیاز به پرداخت"""
    pass


def _deepseek_is_billing_error(status_code, body_text):
    """تشخیص خطاهای مربوط به پول / سهمیه / پرداخت"""
    t = (body_text or "").lower()
    keywords = (
        "insufficient", "balance", "quota", "billing", "payment", "pay",
        "credit", "arrears", "exceeded", "limit exceeded", "please top up",
        "top up", "recharge", "overdue", "subscription", "plan",
        "402", "payment required", "spend", "budget",
    )
    if status_code in (402, 403):
        if any(k in t for k in keywords) or status_code == 402:
            return True
    if status_code == 429 and any(k in t for k in ("quota", "billing", "insufficient", "credit")):
        return True
    if any(k in t for k in keywords) and status_code in (400, 401, 402, 403, 429):
        return True
    return False


def deepseek_billing_user_message():
    return (
        "💳 اعتبار DeepSeek تمام شده یا حساب نیاز به شارژ دارد.\n\n"
        "لطفاً به پنل DeepSeek برو و موجودی را شارژ کن:\n"
        "https://platform.deepseek.com\n\n"
        "تا قبل از شارژ، ربات از حالت رایگان (بدون AI پیشرفته) استفاده می‌کند."
    )


def _deepseek_chat(messages, max_tokens=800, temperature=0.7):
    """
    فراخوانی API دیپ‌سیک.
    در صورت خطای مالی: DeepSeekBillingError پرتاب می‌شود.
    اگر کلید نباشد: None
    """
    global _DEEPSEEK_BILLING_MSG
    if not DEEPSEEK_API_KEY:
        return None
    try:
        r = requests.post(
            f"{DEEPSEEK_BASE_URL}/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": DEEPSEEK_MODEL,
                "messages": messages,
                "max_tokens": max_tokens,
                "temperature": temperature,
            },
            timeout=45,
        )
        body = r.text or ""
        if r.status_code != 200:
            logger.warning("DeepSeek API error %s: %s", r.status_code, body[:400])
            if _deepseek_is_billing_error(r.status_code, body):
                with _DEEPSEEK_BILLING_LOCK:
                    _DEEPSEEK_BILLING_MSG = deepseek_billing_user_message()
                raise DeepSeekBillingError(_DEEPSEEK_BILLING_MSG)
            return None
        data = r.json()
        # بعضی پاسخ‌ها error داخل JSON با status 200 ندارند ولی چک می‌کنیم
        if isinstance(data, dict) and data.get("error"):
            err = data.get("error")
            err_s = json.dumps(err, ensure_ascii=False) if not isinstance(err, str) else err
            if _deepseek_is_billing_error(400, err_s):
                with _DEEPSEEK_BILLING_LOCK:
                    _DEEPSEEK_BILLING_MSG = deepseek_billing_user_message()
                raise DeepSeekBillingError(_DEEPSEEK_BILLING_MSG)
            return None
        return (data.get("choices") or [{}])[0].get("message", {}).get("content")
    except DeepSeekBillingError:
        raise
    except Exception as e:
        logger.warning("DeepSeek call failed: %s", e)
        return None


def _clean_text(text):
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def _extract_sentences(text, max_sentences=8):
    if not text:
        return []
    parts = re.split(r"(?<=[.!?؟\n])\s+", text)
    sentences = []
    for p in parts:
        p = _clean_text(p)
        if len(p) > 20:
            sentences.append(p)
        if len(sentences) >= max_sentences * 2:
            break
    return sentences


def generate_smart_caption(info, url=None, max_len=900):
    if not info:
        return format_caption({}, None, url)
    title = _clean_text(info.get("title") or "")
    uploader = _clean_text(info.get("uploader") or info.get("channel") or "")
    description = _clean_text(info.get("description") or "")
    duration = info.get("duration")
    views = info.get("view_count")
    like_count = info.get("like_count")
    upload_date = info.get("upload_date") or ""
    tags = info.get("tags") or []
    webpage = info.get("webpage_url") or url or ""

    # DeepSeek (اگر کلید باشد)
    if DEEPSEEK_API_KEY:
        try:
            meta_bits = []
            if duration:
                try:
                    m, s = divmod(int(duration), 60)
                    h, m = divmod(m, 60)
                    meta_bits.append(f"مدت: {h}:{m:02d}:{s:02d}" if h else f"مدت: {m}:{s:02d}")
                except Exception:
                    pass
            if views:
                meta_bits.append(f"بازدید: {views}")
            prompt = (
                "یک کپشن کوتاه و جذاب به فارسی برای تلگرام بنویس. "
                "شامل عنوان، کانال، یک جمله خلاصه و چند هشتگ. بدون توضیح اضافه.\n\n"
                f"عنوان: {title}\nکانال: {uploader}\n"
                f"{' | '.join(meta_bits)}\n"
                f"توضیحات: {description[:800]}\nلینک: {webpage}"
            )
            ai_cap = _deepseek_chat([
                {"role": "system", "content": "تو نویسنده کپشن حرفه‌ای تلگرام هستی."},
                {"role": "user", "content": prompt},
            ], max_tokens=500)
            if ai_cap and len(ai_cap.strip()) > 20:
                return ai_cap.strip()[:max_len]
        except DeepSeekBillingError as be:
            # به همراه کپشن رایگان، پیام مالی را برمی‌گردانیم
            free = None  # پایین‌تر ساخته می‌شود
            # ادامه به فال‌بک و الصاق پیام
            pass
        except Exception:
            pass

    lines = []
    if title:
        lines.append(f"🎬 {title[:180]}")
    if uploader:
        lines.append(f"📺 {uploader[:80]}")
    meta_parts = []
    if duration:
        try:
            m, s = divmod(int(duration), 60)
            h, m = divmod(m, 60)
            meta_parts.append(f"⏱ {h}:{m:02d}:{s:02d}" if h else f"⏱ {m}:{s:02d}")
        except Exception:
            pass
    if views:
        try:
            v = int(views)
            if v >= 1_000_000:
                meta_parts.append(f"👁 {v/1_000_000:.1f}M")
            elif v >= 1000:
                meta_parts.append(f"👁 {v/1000:.1f}K")
            else:
                meta_parts.append(f"👁 {v}")
        except Exception:
            pass
    if like_count:
        try:
            meta_parts.append(f"❤️ {int(like_count):,}")
        except Exception:
            pass
    if upload_date and len(str(upload_date)) == 8:
        try:
            meta_parts.append(f"📅 {upload_date[0:4]}/{upload_date[4:6]}/{upload_date[6:8]}")
        except Exception:
            pass
    if meta_parts:
        lines.append(" • ".join(meta_parts))
    if description:
        desc_clean = re.sub(r"http\S+|#\w+", "", description)
        sentences = _extract_sentences(desc_clean, 2)
        if sentences:
            summary = " ".join(sentences[:2])
            if len(summary) > 220:
                summary = summary[:217] + "..."
            lines.append("")
            lines.append(f"📝 {summary}")
    if tags:
        clean_tags = [f"#{_clean_text(str(t)).replace(' ', '_')}" for t in tags[:8] if 2 < len(_clean_text(str(t))) < 25][:5]
        if clean_tags:
            lines.append("")
            lines.append(" ".join(clean_tags))
    if webpage:
        lines.append("")
        lines.append(f"🔗 {webpage[:120]}")
    return "\n".join(lines).strip()[:max_len]


def generate_video_summary(info, max_sentences=5, max_len=700):
    title = _clean_text((info or {}).get("title") or "")
    description = _clean_text((info or {}).get("description") or "")
    uploader = _clean_text((info or {}).get("uploader") or "")

    if DEEPSEEK_API_KEY and (description or title):
        try:
            prompt = (
                "این ویدیو را به فارسی در ۳ تا ۵ نکته کوتاه خلاصه کن (لیست شماره‌دار).\n"
                "بدون مقدمه.\n\n"
                f"عنوان: {title}\nکانال: {uploader}\nتوضیحات: {description[:2000]}"
            )
            ai_sum = _deepseek_chat([
                {"role": "system", "content": "تو خلاصه‌نویس دقیق هستی."},
                {"role": "user", "content": prompt},
            ], max_tokens=600)
            if ai_sum and len(ai_sum.strip()) > 30:
                return ("📋 خلاصه ویدیو (AI):\n🎬 " + title[:100] + "\n\n" + ai_sum.strip())[:max_len]
        except DeepSeekBillingError:
            # فال‌بک رایگان؛ پیام مالی از _DEEPSEEK_BILLING_MSG خوانده می‌شود
            pass
        except Exception:
            pass

    if not description or len(description) < 60:
        parts = []
        if title:
            parts.append(f"عنوان: {title}")
        if uploader:
            parts.append(f"کانال: {uploader}")
        return "📋 خلاصه ساده:\n" + "\n".join(parts) if parts else "خلاصه‌ای پیدا نشد."
    source_text = re.sub(r"http\S+|\[.*?\]", "", description)
    sentences = _extract_sentences(source_text, max_sentences + 4)
    if not sentences:
        return generate_smart_caption(info)
    scored = []
    for i, s in enumerate(sentences):
        score = len(s) * 0.6 + (40 if i < 3 else 0)
        if any(kw in s.lower() for kw in ("مهم", "خلاصه", "نتیجه", "آموزش", "بررسی")):
            score += 25
        scored.append((score, s))
    scored.sort(key=lambda x: -x[0])
    selected = [s for _, s in scored[:max_sentences]]
    selected_ordered = [s for s in sentences if s in selected][:max_sentences]
    lines = ["📋 خلاصه ویدیو:", f"🎬 {title[:100]}" if title else "", ""]
    for i, s in enumerate(selected_ordered, 1):
        lines.append(f"{i}. {s[:177] + '...' if len(s) > 180 else s}")
    return "\n".join(l for l in lines if l).strip()[:max_len]


def extract_youtube_chapters(info):
    chapters = (info or {}).get("chapters") or []
    result = []
    for ch in chapters:
        start = ch.get("start_time")
        title = ch.get("title") or "فصل"
        if start is not None:
            result.append({"start": float(start), "title": str(title)[:80]})
    return result


def _info_context(info, url=None, desc_limit=1800):
    title = _clean_text((info or {}).get("title") or "")
    uploader = _clean_text((info or {}).get("uploader") or (info or {}).get("channel") or "")
    description = _clean_text((info or {}).get("description") or "")[:desc_limit]
    webpage = (info or {}).get("webpage_url") or url or ""
    parts = [f"عنوان: {title}", f"کانال: {uploader}"]
    if description:
        parts.append(f"توضیحات: {description}")
    if webpage:
        parts.append(f"لینک: {webpage}")
    return "\n".join(parts)


def ai_generate_hashtags(info, url=None):
    if DEEPSEEK_API_KEY:
        try:
            out = _deepseek_chat([
                {"role": "system", "content": "متخصص هشتگ هستی. فقط هشتگ بده."},
                {"role": "user", "content": "۱۲ تا ۱۸ هشتگ فارسی و انگلیسی:\n" + _info_context(info, url)},
            ], max_tokens=300)
            if out and len(out.strip()) > 10:
                return "🏷 هشتگ‌ها:\n" + out.strip()
        except DeepSeekBillingError:
            raise
        except Exception:
            pass
    tags = (info or {}).get("tags") or []
    if tags:
        return "🏷 هشتگ‌ها:\n" + " ".join(f"#{str(t).replace(' ', '_')}" for t in tags[:15])
    return "هشتگی پیدا نشد."


def ai_generate_styles(info, url=None):
    if not DEEPSEEK_API_KEY:
        return generate_smart_caption(info, url=url)
    out = _deepseek_chat([
        {"role": "system", "content": "نویسنده کپشن هستی."},
        {"role": "user", "content": "۳ کپشن فارسی: ۱)رسمی ۲)خودمونی ۳)کوتاه ریلز\n\n" + _info_context(info, url)},
    ], max_tokens=700)
    if out and len(out.strip()) > 40:
        return "✍️ چند سبک کپشن:\n\n" + out.strip()
    return generate_smart_caption(info, url=url)


def ai_generate_post(info, url=None):
    if not DEEPSEEK_API_KEY:
        return generate_smart_caption(info, url=url)
    out = _deepseek_chat([
        {"role": "system", "content": "ادیتور کانال تلگرام هستی."},
        {"role": "user", "content": "پست کامل تلگرام (تیتر+متن+هشتگ+لینک) به فارسی:\n\n" + _info_context(info, url)},
    ], max_tokens=700)
    if out and len(out.strip()) > 40:
        return "📢 پست کانال:\n\n" + out.strip()
    return generate_smart_caption(info, url=url)


def ai_improve_title(info, url=None):
    if not DEEPSEEK_API_KEY:
        return "عنوان: " + _clean_text((info or {}).get("title") or "")
    out = _deepseek_chat([
        {"role": "system", "content": "عنوان‌نویس یوتیوب هستی."},
        {"role": "user", "content": "۵ عنوان فارسی جذاب:\n\n" + _info_context(info, url, 800)},
    ], max_tokens=400)
    return ("🎯 پیشنهاد عنوان:\n\n" + out.strip()) if out and len(out.strip()) > 20 else "عنوان بهبود نشد."


def ai_content_analysis(info, url=None):
    if not DEEPSEEK_API_KEY:
        return "برای تحلیل به کلید DeepSeek نیاز است."
    out = _deepseek_chat([
        {"role": "system", "content": "تحلیل‌گر محتوا هستی."},
        {"role": "user", "content": "موضوع، نوع محتوا، مخاطب، نقاط قوت، پلتفرم مناسب (فارسی، کوتاه):\n\n" + _info_context(info, url)},
    ], max_tokens=550)
    return ("🔍 تحلیل:\n\n" + out.strip()) if out and len(out.strip()) > 30 else "تحلیل انجام نشد."


def ai_translate_caption(info, url=None, lang="en"):
    base = generate_smart_caption(info, url=url)
    if not DEEPSEEK_API_KEY:
        return base
    out = _deepseek_chat([
        {"role": "system", "content": "مترجم حرفه‌ای هستی. فقط ترجمه را بده."},
        {"role": "user", "content": f"ترجمه به انگلیسی:\n\n{base}"},
    ], max_tokens=500)
    return ("🌐 ترجمه EN:\n\n" + out.strip()) if out and len(out.strip()) > 15 else base


def ai_answer_about_video(info, question, url=None):
    if not DEEPSEEK_API_KEY:
        return "کلید DeepSeek تنظیم نشده."
    out = _deepseek_chat([
        {"role": "system", "content": "بر اساس متادیتای ویدیو جواب بده."},
        {"role": "user", "content": f"{_info_context(info, url)}\n\nسؤال: {question}"},
    ], max_tokens=500)
    return ("💬 پاسخ:\n\n" + out.strip()) if out and len(out.strip()) > 10 else "پاسخی نبود."


def make_ai_keyboard(request_id):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✨ کپشن", callback_data=f"ai:caption:{request_id}"),
            InlineKeyboardButton("📋 خلاصه", callback_data=f"ai:summary:{request_id}"),
        ],
        [
            InlineKeyboardButton("✍️ چند سبک", callback_data=f"ai:styles:{request_id}"),
            InlineKeyboardButton("📢 پست کانال", callback_data=f"ai:post:{request_id}"),
        ],
        [
            InlineKeyboardButton("🏷 هشتگ", callback_data=f"ai:tags:{request_id}"),
            InlineKeyboardButton("🎯 عنوان", callback_data=f"ai:title:{request_id}"),
        ],
        [
            InlineKeyboardButton("🔍 تحلیل", callback_data=f"ai:analyze:{request_id}"),
            InlineKeyboardButton("🌐 ترجمه EN", callback_data=f"ai:tr_en:{request_id}"),
        ],
        [
            InlineKeyboardButton("📑 فصل‌ها", callback_data=f"ai:chapters:{request_id}"),
            InlineKeyboardButton("🖼 تامبنیل", callback_data=f"ai:thumb:{request_id}"),
        ],
        [InlineKeyboardButton("🔙 بازگشت", callback_data=f"ai:back:{request_id}")],
        [InlineKeyboardButton("❌ لغو", callback_data="cancel")],
    ])


def make_chapters_keyboard(request_id, chapters):
    rows = []
    for i, ch in enumerate(chapters[:12]):
        m, s = divmod(int(ch["start"]), 60)
        h, m = divmod(m, 60)
        tstr = f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"
        rows.append([InlineKeyboardButton(f"{tstr} — {ch['title'][:35]}", callback_data=f"chapter:{request_id}:{i}")])
    rows.append([InlineKeyboardButton("📥 دانلود کل ویدیو", callback_data=f"dl:{request_id}:best")])
    rows.append([InlineKeyboardButton("❌ لغو", callback_data="cancel")])
    return InlineKeyboardMarkup(rows)


def add_logo_ffmpeg(src_path, logo_path, out_path, position="topright", timeout=900):
    import subprocess
    pos_map = {"topright": "W-w-20:20", "topleft": "20:20", "bottomright": "W-w-20:H-h-20", "bottomleft": "20:H-h-20"}
    overlay = pos_map.get(position, "W-w-20:20")
    cmd = ["ffmpeg", "-y", "-i", src_path, "-i", logo_path, "-filter_complex", f"[1:v]scale=120:-1[logo];[0:v][logo]overlay={overlay}", "-c:a", "copy", out_path]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=timeout)
    return out_path


def merge_videos_ffmpeg(file_list, out_path, timeout=1800):
    import subprocess
    list_file = out_path + ".txt"
    with open(list_file, "w", encoding="utf-8") as f:
        for p in file_list:
            f.write(f"file '{os.path.abspath(p)}'\n")
    try:
        subprocess.run(["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", list_file, "-c", "copy", out_path], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=timeout)
    finally:
        try:
            os.remove(list_file)
        except Exception:
            pass
    return out_path


# --- تورنت / اسپاتیفای / پردازش فایل کاربر ---
USER_MEDIA_STATE = {}
USER_MEDIA_LOCK = Lock()


def is_magnet_or_torrent(text):
    t = (text or "").strip().lower()
    return t.startswith("magnet:?") or t.endswith(".torrent") or ".torrent?" in t


def is_spotify_or_soundcloud(url):
    u = (url or "").lower()
    return any(x in u for x in ("spotify.com", "soundcloud.com", "open.spotify"))


def is_live_info(info):
    if not info:
        return False
    return bool(info.get("is_live") or info.get("live_status") in ("is_live", "is_upcoming"))


def resolve_drive_folder_for_url(url):
    """انتخاب پوشه گوگل‌درایو بر اساس دامنه لینک"""
    u = (url or "").lower()
    mapping = [
        ("youtube.com", "youtube"), ("youtu.be", "youtube"),
        ("instagram.com", "instagram"), ("tiktok.com", "tiktok"),
        ("twitter.com", "twitter"), ("x.com", "twitter"),
        ("facebook.com", "facebook"), ("fb.watch", "facebook"),
    ]
    for needle, key in mapping:
        if needle in u and key in _DRIVE_FOLDER_MAP:
            return _DRIVE_FOLDER_MAP[key]
    return _DRIVE_FOLDER_MAP.get("default") or GOOGLE_DRIVE_FOLDER_ID


def make_sublang_keyboard(request_id, format_id):
    """انتخاب زبان زیرنویس قبل از ادامه"""
    langs = [
        ("fa", "فارسی"), ("en", "English"), ("ar", "العربية"),
        ("tr", "Türkçe"), ("ru", "Русский"), ("de", "Deutsch"),
    ]
    rows = []
    row = []
    for code, label in langs:
        row.append(InlineKeyboardButton(label, callback_data=f"sublang:{request_id}:{format_id}:{code}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("بدون زیرنویس", callback_data=f"sublang:{request_id}:{format_id}:off")])
    rows.append([InlineKeyboardButton("❌ لغو", callback_data="cancel")])
    return InlineKeyboardMarkup(rows)


def make_live_keyboard(request_id):
    """مدت ضبط لایو"""
    opts = [(120, "۲ دقیقه"), (300, "۵ دقیقه"), (600, "۱۰ دقیقه"), (900, "۱۵ دقیقه"), (1800, "۳۰ دقیقه")]
    rows = []
    for sec, label in opts:
        rows.append([InlineKeyboardButton(f"🔴 ضبط {label}", callback_data=f"live:{request_id}:{sec}")])
    rows.append([InlineKeyboardButton("❌ لغو", callback_data="cancel")])
    return InlineKeyboardMarkup(rows)


def make_compress_keyboard(request_id, format_id):
    """انتخاب دستی CRF برای فشرده‌سازی"""
    rows = [
        [InlineKeyboardButton("کیفیت بالا CRF18", callback_data=f"crf:{request_id}:{format_id}:18")],
        [InlineKeyboardButton("متعادل CRF23", callback_data=f"crf:{request_id}:{format_id}:23")],
        [InlineKeyboardButton("حجم کمتر CRF28", callback_data=f"crf:{request_id}:{format_id}:28")],
        [InlineKeyboardButton("حداکثر فشرده CRF32", callback_data=f"crf:{request_id}:{format_id}:32")],
        [InlineKeyboardButton("❌ لغو", callback_data="cancel")],
    ]
    return InlineKeyboardMarkup(rows)


def system_under_pressure():
    """آیا CPU/RAM بالاست؟"""
    if not LOW_RESOURCE_MODE:
        return False
    try:
        cpu = psutil.cpu_percent(interval=0.3)
        ram = psutil.virtual_memory().percent
        return cpu >= LOW_RESOURCE_CPU_PCT or ram >= LOW_RESOURCE_RAM_PCT
    except Exception:
        return False


def download_live_stream(url, outtmpl, duration_sec=600, user_id=None, task_id=None):
    """ضبط لایو برای مدت مشخص"""
    ydl_opts = {
        "outtmpl": outtmpl,
        "format": "best",
        "quiet": True,
        "no_warnings": True,
        "socket_timeout": 30,
        "nocheckcertificate": True,
    }
    # محدود کردن مدت با ffmpeg
    ydl_opts["external_downloader"] = "ffmpeg"
    ydl_opts["external_downloader_args"] = {
        "ffmpeg_i": ["-t", str(int(duration_sec))],
    }
    try:
        cp = cookies_path_for_user(user_id) if user_id else None
        if cp and os.path.exists(str(cp)):
            ydl_opts["cookiefile"] = str(cp)
    except Exception:
        pass
    if task_id:
        ydl_opts["progress_hooks"] = [ytdl_progress_hook_factory(task_id)]
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])
    # پیدا کردن خروجی
    base = outtmpl.replace("%(ext)s", "").rstrip(".")
    for ext in ("mp4", "mkv", "webm", "ts"):
        for cand in (base + "." + ext, base + ext):
            if os.path.exists(cand):
                return cand
    d = os.path.dirname(outtmpl) or "."
    cands = []
    for f in os.listdir(d):
        if f.endswith((".mp4", ".mkv", ".webm", ".ts")):
            cands.append(os.path.join(d, f))
    if cands:
        cands.sort(key=os.path.getmtime, reverse=True)
        return cands[0]
    raise RuntimeError("فایل لایو پیدا نشد")


async def _telethon_download_private_message(client, chat_ref, message_id, dest_path):
    """دانلود مدیای یک پیام از چت/کانال (خصوصی یا عمومی) با Telethon"""
    entity = await client.get_entity(chat_ref)
    msg = await client.get_messages(entity, ids=int(message_id))
    if not msg:
        raise RuntimeError("پیام پیدا نشد")
    if not msg.media:
        raise RuntimeError("این پیام فایل رسانه‌ای ندارد")
    path = await client.download_media(msg, file=dest_path)
    return path


def download_telegram_private_media(chat_ref, message_id, dest_dir):
    """رابط همزمان برای دانلود از تلگرام خصوصی"""
    client = ensure_telethon_client()
    if not client or not telethon_loop:
        raise RuntimeError("Telethon آماده نیست. session را تنظیم کن.")
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, f"tg_{message_id}")
    import asyncio
    fut = asyncio.run_coroutine_threadsafe(
        _telethon_download_private_message(client, chat_ref, message_id, dest),
        telethon_loop,
    )
    return fut.result(timeout=600)


def parse_telegram_private_link(text):
    """
    پشتیبانی از:
      - https://t.me/c/1234567890/123
      - t.me/username/123
      - tg://private?chat=... (ساده)
    خروجی: (chat_ref, message_id) یا None
    """
    text = (text or "").strip()
    m = re.search(r"t\.me/c/(\d+)/(\d+)", text)
    if m:
        # کانال خصوصی: chat_id = -100{channel_id}
        cid = int(m.group(1))
        mid = int(m.group(2))
        return int("-100" + str(cid)), mid
    m = re.search(r"t\.me/([A-Za-z0-9_]+)/(\d+)", text)
    if m and m.group(1).lower() not in ("c", "s", "joinchat", "addstickers"):
        return m.group(1), int(m.group(2))
    return None


def aria2_download_torrent(magnet_or_url, out_dir, timeout=3600):
    import subprocess
    os.makedirs(out_dir, exist_ok=True)
    cmd = ["aria2c", "--dir", out_dir, "--seed-time=0", "--max-connection-per-server=8",
           "--split=8", "--bt-max-peers=50", "--summary-interval=5", "--console-log-level=warn", magnet_or_url]
    subprocess.run(cmd, check=True, timeout=timeout)
    files = []
    for root, _, names in os.walk(out_dir):
        for n in names:
            p = os.path.join(root, n)
            if os.path.isfile(p) and not n.endswith(".aria2"):
                files.append((os.path.getsize(p), p))
    if not files:
        raise RuntimeError("فایلی از تورنت دانلود نشد")
    files.sort(reverse=True)
    return files[0][1]


def resolve_spotify_to_search(url):
    try:
        info = extract_info_safe(url, user_id=None)
        title = info.get("title") or info.get("track") or ""
        artist = info.get("artist") or info.get("uploader") or ""
        q = f"{artist} {title}".strip()
        if q:
            return f"ytsearch1:{q}"
    except Exception:
        pass
    return url


def make_userfile_keyboard(fpath):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎧 صوت", callback_data=f"uf:audio:{fpath}"),
         InlineKeyboardButton("🗜 فشرده", callback_data=f"uf:compress:{fpath}")],
        [InlineKeyboardButton("🎞 MP4", callback_data=f"uf:mp4:{fpath}"),
         InlineKeyboardButton("🎞 GIF", callback_data=f"uf:gif:{fpath}")],
        [InlineKeyboardButton("✂ ۳۰ث اول", callback_data=f"uf:trim30:{fpath}"),
         InlineKeyboardButton("🖼 فریم", callback_data=f"uf:frame:{fpath}")],
        [InlineKeyboardButton("❌ لغو", callback_data="cancel")],
    ])


def ffmpeg_compress(src, dst, crf=28, preset="fast", timeout=1800):
    import subprocess
    subprocess.run(["ffmpeg", "-y", "-i", src, "-c:v", "libx264", "-crf", str(crf), "-preset", preset, "-c:a", "aac", "-b:a", "128k", dst],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=timeout)
    return dst


def ffmpeg_extract_audio(src, dst, bitrate="192k", timeout=900):
    import subprocess
    subprocess.run(["ffmpeg", "-y", "-i", src, "-vn", "-acodec", "libmp3lame", "-b:a", bitrate, dst],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=timeout)
    return dst


def ffmpeg_to_mp4(src, dst, timeout=1800):
    import subprocess
    subprocess.run(["ffmpeg", "-y", "-i", src, "-c:v", "libx264", "-c:a", "aac", "-movflags", "+faststart", dst],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=timeout)
    return dst


def ffmpeg_gif(src, dst, duration=5, timeout=600):
    import subprocess
    subprocess.run(["ffmpeg", "-y", "-i", src, "-t", str(duration), "-vf", "fps=12,scale=480:-1:flags=lanczos", dst],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=timeout)
    return dst


def ffmpeg_trim(src, dst, start=0, duration=30, timeout=900):
    import subprocess
    subprocess.run(["ffmpeg", "-y", "-ss", str(start), "-i", src, "-t", str(duration), "-c", "copy", dst],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=timeout)
    return dst


def ffmpeg_frame(src, dst, at_sec=1, timeout=120):
    import subprocess
    subprocess.run(["ffmpeg", "-y", "-ss", str(at_sec), "-i", src, "-frames:v", "1", dst],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=timeout)
    return dst


def pick_auto_format(parsed_formats, preferred_height=720, max_size_mb=50):
    """انتخاب هوشمند فرمت: ترجیح ارتفاع، سپس زیر سقف حجم"""
    if not parsed_formats:
        return "best"
    max_bytes = (max_size_mb or 50) * 1024 * 1024
    # فقط muxed یا video-only با height
    cands = [p for p in parsed_formats if p.get("height") and not p.get("is_audio_only")]
    if not cands:
        return "best"
    # 1) نزدیک‌ترین به preferred_height با حجم زیر سقف
    under = [p for p in cands if (p.get("size") or 0) and p["size"] <= max_bytes]
    pool = under if under else cands
    preferred_height = preferred_height or 720
    pool_sorted = sorted(pool, key=lambda p: (abs((p.get("height") or 0) - preferred_height), -(p.get("height") or 0)))
    best = pool_sorted[0]
    return best.get("format_id") or "best"

# -------------------------
# کوکی کاربر
# -------------------------

def create_batch(user_id, chat_id, total, bot=None):
    """
    ایجاد یک دسته (batch) برای دانلود چندتایی/پلی‌لیست.
    اگر bot داده شود، یک پیام پیشرفت زنده ارسال می‌شود که با هر آیتم تمام‌شده به‌روزرسانی می‌شود.
    """
    batch_id = uuid.uuid4().hex[:10]
    progress_msg_id = None
    if bot and total:
        try:
            bar = render_horizontal_ali_bar(0, length=20, left_label="Batch")
            msg = bot.send_message(chat_id=chat_id, text=f"📦 دانلود گروهی شروع شد\n{bar}\n0/{total} انجام شد")
            progress_msg_id = msg.message_id
        except Exception:
            progress_msg_id = None
    with BATCHES_LOCK:
        BATCHES[batch_id] = {
            "user_id": user_id,
            "chat_id": chat_id,
            "total": total,
            "done": 0,
            "ok": 0,
            "fail": 0,
            "titles": [],
            "created": time.time(),
            "progress_msg_id": progress_msg_id,
            "last_edit": 0,
        }
    return batch_id


def batch_item_done(batch_id, ok=True, title=None, bot=None):
    if not batch_id:
        return
    finished = False
    snapshot = None
    should_edit = False
    edit_chat_id = None
    edit_msg_id = None
    edit_text = None
    with BATCHES_LOCK:
        b = BATCHES.get(batch_id)
        if not b:
            return
        b["done"] += 1
        if ok:
            b["ok"] += 1
        else:
            b["fail"] += 1
        if title:
            b["titles"].append(str(title)[:60])
        finished = b["done"] >= b["total"]
        snapshot = dict(b) if finished else None
        progress_msg_id = b.get("progress_msg_id")
        now = time.time()
        should_edit = bool(bot and progress_msg_id and (finished or now - b.get("last_edit", 0) > 1.5))
        if should_edit:
            b["last_edit"] = now
            total = b["total"]
            pct = int(b["done"] * 100 / total) if total else 0
            bar = render_horizontal_ali_bar(pct, length=20, left_label="Batch")
            status_line = "✅ تمام شد" if finished else "در حال پردازش..."
            edit_text = f"📦 دانلود گروهی — {status_line}\n{bar}\n{b['done']}/{total} انجام (موفق: {b['ok']}، ناموفق: {b['fail']})"
            edit_chat_id = b.get("chat_id")
            edit_msg_id = progress_msg_id
        if finished:
            BATCHES.pop(batch_id, None)

    if should_edit:
        try:
            bot.edit_message_text(chat_id=edit_chat_id, message_id=edit_msg_id, text=edit_text)
        except Exception:
            pass

    if finished and bot and snapshot:
        try:
            lines = [
                "✅ گزارش چندلینکی تمام شد",
                f"کل: {snapshot['total']} | موفق: {snapshot['ok']} | ناموفق: {snapshot['fail']}",
            ]
            for t in snapshot.get("titles", [])[:12]:
                lines.append(f"• {t}")
            bot.send_message(chat_id=snapshot["chat_id"], text="\n".join(lines))
        except Exception:
            pass


def split_file_for_upload(file_path, chunk_bytes=48 * 1024 * 1024):
    """تقسیم فایل بزرگ به چند بخش؛ خروجی لیست مسیرها"""
    parts = []
    size = os.path.getsize(file_path)
    if size <= chunk_bytes:
        return [file_path]
    base = file_path + ".part"
    idx = 0
    with open(file_path, "rb") as src:
        while True:
            data = src.read(chunk_bytes)
            if not data:
                break
            idx += 1
            part_path = f"{base}{idx:03d}"
            with open(part_path, "wb") as out:
                out.write(data)
            parts.append(part_path)
    return parts


def trim_media_ffmpeg(src_path, dest_path, start_sec=None, end_sec=None, timeout=600):
    """برش فایل با ffmpeg؛ start/end بر حسب ثانیه"""
    import subprocess
    cmd = ["ffmpeg", "-y"]
    if start_sec is not None and float(start_sec) > 0:
        cmd += ["-ss", str(start_sec)]
    cmd += ["-i", src_path]
    if end_sec is not None and start_sec is not None:
        dur = max(0.1, float(end_sec) - float(start_sec))
        cmd += ["-t", str(dur)]
    elif end_sec is not None:
        cmd += ["-to", str(end_sec)]
    cmd += ["-c", "copy", dest_path]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=timeout)
    return dest_path


def make_playlist_quality_keyboard(playlist_url, request_id):
    rows = [
        [InlineKeyboardButton("🎥 بهترین", callback_data=f"plq:{request_id}:best")],
        [InlineKeyboardButton("720p", callback_data=f"plq:{request_id}:720"),
         InlineKeyboardButton("480p", callback_data=f"plq:{request_id}:480"),
         InlineKeyboardButton("360p", callback_data=f"plq:{request_id}:360")],
        [InlineKeyboardButton("🎧 فقط صدا MP3", callback_data=f"plq:{request_id}:audio")],
        [InlineKeyboardButton("❌ لغو", callback_data="cancel")],
    ]
    return InlineKeyboardMarkup(rows)


def make_playlist_range_keyboard(request_id, quality, total):
    """انتخاب بازه ویدیوها در پلی‌لیست"""
    rows = [
        [InlineKeyboardButton(f"📥 همه ({total})", callback_data=f"plrange:{request_id}:{quality}:all")],
    ]
    # دکمه‌های بازه‌ای ۱۰تایی
    step = 10
    for start in range(1, total + 1, step):
        end = min(start + step - 1, total)
        label = f"{start}–{end}"
        rows.append([InlineKeyboardButton(label, callback_data=f"plrange:{request_id}:{quality}:{start}-{end}")])
        if len(rows) >= 12:  # جلوگیری از کیبورد خیلی بلند
            break
    if total > 5:
        rows.append([
            InlineKeyboardButton("۵ اول", callback_data=f"plrange:{request_id}:{quality}:1-5"),
            InlineKeyboardButton("۱۰ اول", callback_data=f"plrange:{request_id}:{quality}:1-10"),
        ])
    rows.append([InlineKeyboardButton("❌ لغو", callback_data="cancel")])
    return InlineKeyboardMarkup(rows)


def make_trim_keyboard(request_id, format_id, with_subs=False):
    sub_flag = "1" if with_subs else "0"
    rows = [
        [InlineKeyboardButton("⏭ بدون برش — ادامه", callback_data=f"trimskip:{request_id}:{format_id}:{sub_flag}")],
        [InlineKeyboardButton("❌ لغو", callback_data="cancel")],
    ]
    return InlineKeyboardMarkup(rows)


def is_admin(user_id):
    if not ADMIN_IDS:
        return True
    return int(user_id) in ADMIN_IDS


def user_allowed(user_id):
    """بررسی ban / maintenance / whitelist"""
    if is_user_banned(user_id):
        return False, "⛔ دسترسی شما مسدود شده است."
    with MAINTENANCE_LOCK:
        maint = MAINTENANCE_MODE
    if maint and not is_admin(user_id):
        return False, "🛠 ربات در حالت تعمیرات است. بعداً تلاش کن."
    if WHITELIST_ENABLED and not is_admin(user_id):
        with WHITELIST_LOCK:
            if int(user_id) not in WHITELIST_USERS:
                return False, "🔒 فقط کاربران تأییدشده می‌توانند استفاده کنند."
    return True, ""


def _task_priority(user_id):
    """اولویت پردازش در صف: ادمین‌ها (۰) > کاربران لیست‌سفید/VIP (۱) > سایر کاربران (۲)."""
    try:
        uid = int(user_id)
    except Exception:
        return 2
    try:
        if is_admin(uid):
            return 0
    except Exception:
        pass
    try:
        with WHITELIST_LOCK:
            if uid in WHITELIST_USERS:
                return 1
    except Exception:
        pass
    return 2


def enqueue_task(task):
    """
    افزودن یک وظیفه به صف دانلود با در نظر گرفتن اولویت.
    ادمین‌ها و کاربران VIP (لیست‌سفید) زودتر از کاربران عادی پردازش می‌شوند؛
    در داخل هر سطح اولویت، ترتیب FIFO حفظ می‌شود.
    """
    prio = _task_priority(task.get("user_id")) if task else 2
    with _QUEUE_SEQ_LOCK:
        seq = next(_QUEUE_SEQ_COUNTER)
    download_queue.put((prio, seq, task))


def estimate_queue_wait_seconds(ahead_count):
    """تخمین زمان تقریبی شروع بر اساس میانگین مدت‌زمان کارهای اخیر و تعداد ورکرهای فعال."""
    with GLOBAL_STATS_LOCK:
        avg = GLOBAL_STATS.get("avg_task_seconds") or 25.0
    workers = max(1, MAX_CONCURRENT_DOWNLOADS)
    return (max(0, ahead_count) * avg) / workers


def get_rotating_proxy(url=None):
    """پروکسی مناسب سایت: YOUTUBE_PROXY / INSTAGRAM_PROXY / لیست چرخشی / YTDLP_PROXY"""
    global PROXY_INDEX
    try:
        net = (urlparse(url or "").netloc or "").lower()
    except Exception:
        net = ""
    if net and any(x in net for x in ("youtube.com", "youtu.be", "googlevideo.com", "youtube-nocookie.com")):
        if YOUTUBE_PROXY:
            return YOUTUBE_PROXY
    if net and any(x in net for x in ("instagram.com", "cdninstagram.com", "instagr.am")):
        if INSTAGRAM_PROXY:
            return INSTAGRAM_PROXY
    if YTDLP_PROXY:
        return YTDLP_PROXY
    if not PROXY_LIST:
        return None
    with PROXY_LOCK:
        p = PROXY_LIST[PROXY_INDEX % len(PROXY_LIST)]
        PROXY_INDEX += 1
        return p


def is_domain_blacklisted(url):
    try:
        net = (urlparse(url).netloc or "").lower()
        if not net:
            return False
        for d in DOMAIN_BLACKLIST:
            if net == d or net.endswith("." + d):
                return True
    except Exception:
        pass
    return False


def burn_subtitles_ffmpeg(video_path, sub_path, out_path, timeout=900):
    import subprocess
    # escape path for subtitles filter
    sub_esc = sub_path.replace("\\", "\\\\").replace(":", "\\:").replace("'", "\\'")
    cmd = [
        "ffmpeg", "-y", "-i", video_path,
        "-vf", f"subtitles={sub_esc}",
        "-c:a", "copy", out_path,
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=timeout)
    return out_path


def apply_watermark_ffmpeg(video_path, out_path, text, timeout=900):
    import subprocess
    safe = (text or "ALI").replace(":", "\\:").replace("'", "\\'")[:40]
    vf = f"drawtext=text='{safe}':x=20:y=h-th-20:fontsize=24:fontcolor=white@0.7:box=1:boxcolor=black@0.4"
    cmd = ["ffmpeg", "-y", "-i", video_path, "-vf", vf, "-c:a", "copy", out_path]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=timeout)
    return out_path


def force_mp4_ffmpeg(src_path, out_path, height=None, timeout=900):
    import subprocess
    cmd = ["ffmpeg", "-y", "-i", src_path]
    if height:
        cmd += ["-vf", f"scale=-2:{int(height)}"]
    cmd += ["-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-c:a", "aac", "-b:a", "128k", out_path]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=timeout)
    return out_path


def extract_screenshot_ffmpeg(src_path, out_path, at_sec=5, timeout=120):
    import subprocess
    cmd = ["ffmpeg", "-y", "-ss", str(at_sec), "-i", src_path, "-frames:v", "1", "-q:v", "2", out_path]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=timeout)
    return out_path


def notify_queue_waiters(bot):
    with active_workers_lock:
        aw = active_workers
    qsize = download_queue.qsize()
    if aw > 0 or qsize > 0:
        return
    with QUEUE_WAITERS_LOCK:
        waiters = dict(QUEUE_WAITERS)
        QUEUE_WAITERS.clear()
    for uid, cid in waiters.items():
        try:
            bot.send_message(chat_id=cid, text="✅ صف خالی شد؛ می‌توانی دانلود جدید شروع کنی.")
        except Exception:
            pass


def start_scheduler_thread(bot):
    def runner():
        while True:
            try:
                now = time.time()
                due = []
                with SCHEDULED_LOCK:
                    keep = []
                    for job in SCHEDULED_JOBS:
                        if job.get("run_at", 0) <= now:
                            due.append(job)
                        else:
                            keep.append(job)
                    SCHEDULED_JOBS[:] = keep
                for job in due:
                    task = job.get("task") or {}
                    try:
                        enqueue_task(task)
                        chat_id = task.get("chat_id")
                        if chat_id:
                            bot.send_message(chat_id=chat_id, text=f"⏰ دانلود زمان‌بندی‌شده شروع شد:\n{(task.get('url') or '')[:80]}")
                    except Exception as e:
                        logger.warning("scheduled job failed: %s", e)
            except Exception:
                pass
            time.sleep(20)
    t = threading.Thread(target=runner, daemon=True)
    t.start()


def start_channel_watch_thread(bot):
    def runner():
        while True:
            try:
                with CHANNEL_WATCH_LOCK:
                    watches = list(CHANNEL_WATCHES)
                for w in watches:
                    try:
                        url = w.get("channel_url")
                        ydl_opts = {"quiet": True, "no_warnings": True, "extract_flat": True, "playlistend": 5}
                        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                            info = ydl.extract_info(url, download=False)
                        entries = info.get("entries") or []
                        last_ids = set(w.get("last_ids") or [])
                        new_ids = []
                        for e in entries:
                            if not e:
                                continue
                            vid = e.get("id")
                            if not vid or vid in last_ids:
                                continue
                            new_ids.append(vid)
                            vurl = e.get("url") or vid
                            if vurl and not str(vurl).startswith("http"):
                                vurl = f"https://www.youtube.com/watch?v={vurl}"
                            enqueue_task({
                                "user_id": w["user_id"],
                                "username": str(w["user_id"]),
                                "url": vurl,
                                "chat_id": w["chat_id"],
                                "message_id": None,
                                "action": "video" if w.get("quality") != "audio" else "audio",
                                "format_id": w.get("quality") or "best",
                                "mode": "telegram",
                                "request_id": None,
                                "request_info": None,
                            })
                            try:
                                bot.send_message(
                                    chat_id=w["chat_id"],
                                    text=f"🔔 ویدیو جدید کانال شناسایی شد و به صف رفت:\n{(e.get('title') or vurl)[:80]}",
                                )
                            except Exception:
                                pass
                        if new_ids:
                            with CHANNEL_WATCH_LOCK:
                                for ww in CHANNEL_WATCHES:
                                    if ww is w or (ww.get("user_id") == w.get("user_id") and ww.get("channel_url") == url):
                                        ids = set(ww.get("last_ids") or [])
                                        ids.update(new_ids)
                                        ww["last_ids"] = list(ids)[-50:]
                    except Exception as e:
                        logger.warning("channel watch error: %s", e)
            except Exception:
                pass
            time.sleep(300)
    t = threading.Thread(target=runner, daemon=True)
    t.start()


def cookies_path_for_user(user_id):
    return Path(COOKIES_ROOT) / f"{user_id}.txt"

def get_cookiefile(user_id):
    p = cookies_path_for_user(user_id)
    if p.exists() and p.stat().st_size > 10:
        return str(p)
    # کوکی سراسری اختیاری
    global_c = Path(COOKIES_ROOT) / "global.txt"
    if global_c.exists() and global_c.stat().st_size > 10:
        return str(global_c)
    return None

# -------------------------
# محدودیت نرخ کاربر
# -------------------------
def _today_str():
    return datetime.now().strftime("%Y-%m-%d")

def check_user_limits(user_id):
    """برمی‌گرداند (ok: bool, message: str) — سقف با سطح کاربر مقیاس می‌شود"""
    max_dl, max_bytes, level, mult = get_user_limits_scaled(user_id)
    with USER_LIMITS_LOCK:
        entry = USER_LIMITS.get(user_id) or {"active": 0, "day": _today_str(), "count": 0, "bytes": 0}
        if entry.get("day") != _today_str():
            entry = {"active": entry.get("active", 0), "day": _today_str(), "count": 0, "bytes": 0}
            USER_LIMITS[user_id] = entry
        if entry.get("active", 0) >= MAX_CONCURRENT_PER_USER:
            return False, f"حداکثر {MAX_CONCURRENT_PER_USER} دانلود همزمان برای هر کاربر مجاز است. صبر کن تا یکی تمام شود."
        if entry.get("count", 0) >= max_dl:
            return False, f"سقف روزانه دانلود (سطح {level}: {max_dl} عدد) پر شده است."
        if entry.get("bytes", 0) >= max_bytes:
            return False, f"سقف حجم روزانه (سطح {level}: {human_size(max_bytes)}) پر شده است."
        return True, ""

def user_limit_start(user_id):
    with USER_LIMITS_LOCK:
        entry = USER_LIMITS.get(user_id) or {"active": 0, "day": _today_str(), "count": 0, "bytes": 0}
        if entry.get("day") != _today_str():
            entry = {"active": 0, "day": _today_str(), "count": 0, "bytes": 0}
        entry["active"] = entry.get("active", 0) + 1
        entry["count"] = entry.get("count", 0) + 1
        USER_LIMITS[user_id] = entry

def user_limit_finish(user_id, bytes_count=0):
    with USER_LIMITS_LOCK:
        entry = USER_LIMITS.get(user_id)
        if not entry:
            return
        entry["active"] = max(0, entry.get("active", 1) - 1)
        entry["bytes"] = entry.get("bytes", 0) + (bytes_count or 0)
        USER_LIMITS[user_id] = entry

# -------------------------
# پاکسازی فایل‌های موقت
# -------------------------
def cleanup_old_files():
    try:
        cutoff = time.time() - CLEANUP_MAX_AGE_HOURS * 3600
        removed = 0
        for root, dirs, files in os.walk(DOWNLOAD_ROOT):
            # پوشه downloads کاربران و ریشه دانلود
            for name in files:
                fp = os.path.join(root, name)
                try:
                    if os.path.getmtime(fp) < cutoff:
                        # prefs و logs و cookies را پاک نکن
                        if name in ("prefs.json", "events.log", "bot_data.sqlite3") or name.endswith(".session") or name.endswith(".sqlite3"):
                            continue
                        # فایل‌های ناقص را زودتر پاک کن
                        if name.endswith((".part", ".ytdl", ".aria2", ".tmp")) and os.path.getmtime(fp) < time.time() - 3600:
                            try:
                                os.remove(fp)
                                removed += 1
                                continue
                            except Exception:
                                pass
                        if "cookies" in root:
                            continue
                        if "logs" in root:
                            continue
                        os.remove(fp)
                        removed += 1
                except Exception:
                    pass
        if removed:
            logger.info("Cleanup removed %d old files", removed)
    except Exception as e:
        logger.warning("Cleanup error: %s", e)

def start_cleanup_thread():
    def runner():
        while True:
            try:
                cleanup_old_files()
            except Exception:
                pass
            try:
                cleanup_temp_links()
            except Exception:
                pass
            time.sleep(CLEANUP_INTERVAL_SECONDS)
    t = threading.Thread(target=runner, daemon=True)
    t.start()

# -------------------------
# سلامت‌سنجی سرویس‌ها (Telethon و Google Drive) + هشدار به ادمین‌ها
# -------------------------
HEALTH_CHECK_INTERVAL_SECONDS = int(os.environ.get("HEALTH_CHECK_INTERVAL_SECONDS", "180"))
_HEALTH_STATE = {"telethon_ok": None, "drive_ok": None}
_HEALTH_STATE_LOCK = Lock()


def _notify_admins(bot, text):
    for aid in list(ADMIN_IDS):
        try:
            bot.send_message(chat_id=aid, text=text)
        except Exception:
            pass


def _check_telethon_health():
    if not TELETHON_API_ID or not TELETHON_API_HASH or TelegramClient is None:
        return None  # پیکربندی نشده؛ بررسی معنا ندارد
    try:
        client = telethon_client
        if client and getattr(client, "is_connected", lambda: False)():
            return True
        return False
    except Exception:
        return False


def _check_drive_health():
    if not GOOGLE_DRIVE_ENABLED:
        return None  # غیرفعال؛ بررسی معنا ندارد
    if not GDRIVE_LIBS_AVAILABLE or not os.path.exists(GOOGLE_DRIVE_CREDENTIALS_FILE):
        return None  # هنوز پیکربندی نشده
    try:
        service = ensure_drive_service()
        service.about().get(fields="storageQuota").execute()
        return True
    except Exception:
        return False


def start_health_check_thread(bot):
    def runner():
        while True:
            try:
                tg_ok = _check_telethon_health()
                drive_ok = _check_drive_health()
                with _HEALTH_STATE_LOCK:
                    prev_tg = _HEALTH_STATE.get("telethon_ok")
                    prev_drive = _HEALTH_STATE.get("drive_ok")
                    _HEALTH_STATE["telethon_ok"] = tg_ok
                    _HEALTH_STATE["drive_ok"] = drive_ok

                if tg_ok is not None and prev_tg is not None and tg_ok != prev_tg:
                    if tg_ok:
                        _notify_admins(bot, "✅ اتصال Telethon دوباره برقرار شد.")
                    else:
                        _notify_admins(bot, "⚠️ اتصال Telethon قطع شده است؛ آپلود فایل‌های بزرگ ممکن است کار نکند.")

                if drive_ok is not None and prev_drive is not None and drive_ok != prev_drive:
                    if drive_ok:
                        _notify_admins(bot, "✅ اتصال Google Drive دوباره برقرار شد.")
                    else:
                        _notify_admins(bot, "⚠️ اتصال Google Drive قطع شده یا احراز هویت آن ناموفق است.")
            except Exception as e:
                logger.warning("Health check error: %s", e)
            time.sleep(HEALTH_CHECK_INTERVAL_SECONDS)
    t = threading.Thread(target=runner, daemon=True, name="health-check")
    t.start()

def sanitize_name(name, max_len=120):
    return re.sub(r'[<>:"/\\\\|?*]', '_', str(name))[:max_len]

def human_size(size):
    if not size:
        return "—"
    try:
        size = int(size)
        if size >= 1024**3:
            return f"{size/1024/1024/1024:.2f} GB"
        if size >= 1024**2:
            return f"{size/1024/1024:.2f} MB"
        return f"{size/1024:.2f} KB"
    except:
        return "—"

def is_instagram_or_x(url):
    try:
        net = urlparse(url).netloc.lower()
        return any(d in net for d in ("instagram.com", "www.instagram.com", "x.com", "www.x.com", "twitter.com", "www.twitter.com", "t.co", "facebook.com", "fb.watch", "tiktok.com"))
    except:
        return False

def is_youtube_url(url):
    try:
        net = urlparse(url).netloc.lower()
        return "youtube.com" in net or "youtu.be" in net
    except:
        return False

# -------------------------
# safe edit helper
# -------------------------
def safe_edit_message(bot, chat_id, message_id, text, reply_markup=None):
    try:
        bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, reply_markup=reply_markup)
    except:
        try:
            bot.edit_message_caption(chat_id=chat_id, message_id=message_id, caption=text, reply_markup=reply_markup)
        except:
            pass

# -------------------------
# === انیمیشن‌ها: ALI sequence, I Fill و Processing bar ===
# این بخش به‌صورت افزوده به فایل اصلی اضافه شده و تداخلی با توابع موجود ایجاد نمی‌کند.
# توجه: تابع human_size در بالا از قبل تعریف شده؛ بنابراین در این بخش دوباره تعریف نشده است.
# -------------------------

def format_eta(seconds):
    """ثانیه -> H:MM:SS یا MM:SS"""
    try:
        s = int(max(0, int(seconds)))
    except Exception:
        return "—"
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    if h:
        return f"{h:d}:{m:02d}:{sec:02d}"
    return f"{m:02d}:{sec:02d}"

# render I with inner fill (I Fill)
def render_I_with_inner_fill(pct, segments=8, inner_width=5):
    """
    pct: 0..100
    segments: تعداد بخش‌های عمودی داخل I
    inner_width: عرض داخلی (تعداد کاراکتر)
    خروجی: بلوک متنی چندخطی با عنوان ALI و درصد
    """
    try:
        pct = max(0, min(100, int(pct)))
    except:
        pct = 0
    filled = int(round((pct / 100.0) * segments))
    if inner_width % 2 == 0:
        inner_width -= 1
        if inner_width < 1:
            inner_width = 1
    bar_width = inner_width + 4
    top_bar = " " * ((bar_width - 3)//2) + "███" + " " * ((bar_width - 3)//2)
    bottom_bar = top_bar
    middle_lines = []
    for i in range(segments):
        idx_from_bottom = segments - 1 - i
        inner = "█" * inner_width if idx_from_bottom < filled else "░" * inner_width
        line = " " * 2 + inner + " " * 2
        middle_lines.append(line)
    header = "A L I"
    footer = f"{pct}%"
    parts = [header, "", top_bar] + middle_lines + [bottom_bar, "", footer]
    return "\n".join(parts)

# horizontal ALI branded bar
def render_horizontal_ali_bar(pct, length=20, left_label="ALI", fill_char="█", empty_char="░"):
    """
    pct: 0..100
    length: طول نوار
    خروجی: یک خط شامل برچسب، نوار و درصد
    """
    try:
        pct = max(0, min(100, int(pct)))
    except:
        pct = 0
    filled = int(round((pct / 100.0) * length))
    bar = fill_char * filled + empty_char * (length - filled)
    percent_text = f"{pct}%"
    line = f"{left_label} |{bar}| {percent_text}"
    return line

# QualityAnimationALI: ALI sequence frames
class QualityAnimationALI:
    """
    انیمیشن سبک برای مرحله بررسی کیفیت
    فریم‌ها: A -> Al -> ALI -> AL -> A -> AL
    استفاده: start(key, bot, chat_id), stop(key)
    """
    def __init__(self):
        self.map = {}

    def start(self, key, bot, chat_id, title="در حال بررسی کیفیت…", min_interval=0.9):
        msg = bot.send_message(chat_id=chat_id, text=title)
        stop_flag = {"stop": False}
        info = {
            "bot": bot,
            "chat_id": chat_id,
            "msg_id": msg.message_id,
            "title": title,
            "min_interval": min_interval,
            "last_edit": 0,
            "stop_flag": stop_flag,
            "reply_markup": None
        }
        self.map[key] = info
        t = threading.Thread(target=self._runner, args=(key,), daemon=True)
        t.start()
        return msg.message_id

    def stop(self, key, final_text=None):
        info = self.map.get(key)
        if not info:
            return
        info["stop_flag"]["stop"] = True
        try:
            text = final_text or f"{info['title']}\n\n✅ بررسی کیفیت انجام شد"
            info["bot"].edit_message_text(chat_id=info["chat_id"], message_id=info["msg_id"], text=text, reply_markup=info.get("reply_markup"))
        except:
            pass
        self.map.pop(key, None)

    def _runner(self, key):
        info = self.map.get(key)
        if not info:
            return
        frames = ["A", "Al", "ALI", "AL", "A", "AL"]
        i = 0
        while not info["stop_flag"]["stop"]:
            now = time.time()
            if now - info["last_edit"] >= info["min_interval"]:
                frame = frames[i % len(frames)]
                text = f"{info['title']}\n\n{frame}"
                try:
                    info["bot"].edit_message_text(chat_id=info["chat_id"], message_id=info["msg_id"], text=text, reply_markup=info.get("reply_markup"))
                except:
                    pass
                info["last_edit"] = now
                i += 1
            time.sleep(0.12)

# General QualityAnimation with I Fill mode (passive updates)
class QualityAnimation:
    """
    حالت i_fill: نمایش درصد داخل حرف I
    متدها: start, update_i_fill, stop
    """
    def __init__(self):
        self.map = {}

    def start(self, key, bot, chat_id, title="در حال بررسی کیفیت…", min_interval=0.9, segments=8):
        msg = bot.send_message(chat_id=chat_id, text=title)
        info = {
            "bot": bot,
            "chat_id": chat_id,
            "msg_id": msg.message_id,
            "title": title,
            "min_interval": min_interval,
            "last_edit": 0,
            "segments": segments,
            "pct": 0,
            "reply_markup": None
        }
        self.map[key] = info
        return msg.message_id

    def update_i_fill(self, key, pct, extra_text=None, inner_width=5):
        info = self.map.get(key)
        if not info:
            return
        now = time.time()
        if now - info["last_edit"] < info["min_interval"]:
            return
        i_block = render_I_with_inner_fill(pct, segments=info.get("segments", 8), inner_width=inner_width)
        lines = [info["title"], "", i_block]
        if extra_text:
            lines += ["", extra_text]
        text = "\n".join(lines)
        try:
            info["bot"].edit_message_text(chat_id=info["chat_id"], message_id=info["msg_id"], text=text, reply_markup=info.get("reply_markup"))
        except:
            pass
        info["last_edit"] = now
        info["pct"] = pct

    def stop(self, key, final_text=None):
        info = self.map.get(key)
        if not info:
            return
        try:
            text = final_text or f"{info['title']}\n\n✅ بررسی کیفیت انجام شد"
            info["bot"].edit_message_text(chat_id=info["chat_id"], message_id=info["msg_id"], text=text, reply_markup=info.get("reply_markup"))
        except:
            pass
        self.map.pop(key, None)

# ProcessingAnimation: progress bar for processing/upload/download stages
class ProcessingAnimation:
    """
    نمایش پردازش پس از دانلود/آپلود
    متدها: start, update, stop
    update می‌تواند stage, pct, transferred, total, speed_bps را بگیرد
    """
    def __init__(self):
        self.map = {}

    def start(self, key, bot, chat_id, title="در حال پردازش فایل…", initial_stage="شروع", min_interval=0.9, bar_length=20):
        msg = bot.send_message(chat_id=chat_id, text=title)
        info = {
            "bot": bot,
            "chat_id": chat_id,
            "msg_id": msg.message_id,
            "title": title,
            "stage": initial_stage,
            "pct": 0,
            "min_interval": min_interval,
            "bar_length": bar_length,
            "last_edit": 0,
            "start_time": time.time(),
            "last_transferred": None,
            "last_transferred_time": None,
            "reply_markup": None
        }
        self.map[key] = info
        return msg.message_id

    def update(self, key, stage=None, pct=None, transferred=None, total=None, speed_bps=None, extra_text=None):
        info = self.map.get(key)
        if not info:
            return
        now = time.time()
        if now - info["last_edit"] < info["min_interval"]:
            return

        if stage is not None:
            info["stage"] = stage
        if pct is not None:
            info["pct"] = max(0, min(100, int(pct)))

        # محاسبه سرعت اگر داده نشده
        if speed_bps is None and transferred is not None:
            prev = info.get("last_transferred")
            prev_t = info.get("last_transferred_time")
            if prev is not None and prev_t is not None and now - prev_t > 0:
                speed_bps = (transferred - prev) / max(1e-6, (now - prev_t))

        speed_text = f"⚡ {speed_bps/1024/1024:.2f} MB/s" if speed_bps and speed_bps > 0 else ""
        eta_text = ""
        if transferred is not None and total:
            remaining = max(0, total - transferred)
            if speed_bps and speed_bps > 0:
                eta_text = f"⏳ {format_eta(remaining / speed_bps)}"
        size_text = ""
        if transferred is not None and total:
            size_text = f"📦 {human_size(transferred)} / {human_size(total)}"
        elif transferred is not None:
            size_text = f"📦 {human_size(transferred)}"

        extras = "  ".join([t for t in [speed_text, eta_text, size_text, extra_text] if t])

        bar_line = render_horizontal_ali_bar(info.get("pct", 0), length=info.get("bar_length", 20), left_label=info.get("stage", "processing"))
        elapsed = int(now - info.get("start_time", now))
        elapsed_text = f"⏱ {format_eta(elapsed)}"
        lines = [info.get("title"), "", bar_line]
        if extras:
            lines += ["", f"{elapsed_text}  {extras}"]
        else:
            lines += ["", elapsed_text]
        text = "\n".join(lines)

        try:
            info["bot"].edit_message_text(chat_id=info["chat_id"], message_id=info["msg_id"], text=text, reply_markup=info.get("reply_markup"))
        except:
            pass

        info["last_edit"] = now
        if transferred is not None:
            info["last_transferred"] = transferred
            info["last_transferred_time"] = now

    def stop(self, key, final_text=None):
        info = self.map.get(key)
        if not info:
            return
        try:
            text = final_text or f"{info['title']}\n\n✅ پردازش انجام شد"
            info["bot"].edit_message_text(chat_id=info["chat_id"], message_id=info["msg_id"], text=text, reply_markup=info.get("reply_markup"))
        except:
            pass
        self.map.pop(key, None)

# global instances for quick use
quality_ali_anim = QualityAnimationALI()
quality_anim = QualityAnimation()
proc_anim = ProcessingAnimation()

# -------------------------
# Remote size helper
# -------------------------
def get_remote_size(url):
    try:
        if not url or not (url.startswith("http://") or url.startswith("https://")):
            return None
        headers = {"User-Agent": USER_AGENT_HEAD}
        try:
            r = requests.head(url, headers=headers, allow_redirects=True, timeout=HEAD_TIMEOUT)
            if 200 <= r.status_code < 400:
                cl = r.headers.get("Content-Length") or r.headers.get("content-length")
                if cl and cl.isdigit():
                    return int(cl)
        except Exception:
            pass
        try:
            with requests.get(url, headers=headers, stream=True, timeout=HEAD_TIMEOUT) as r2:
                if 200 <= r2.status_code < 400:
                    cl = r2.headers.get("Content-Length") or r2.headers.get("content-length")
                    if cl and cl.isdigit():
                        return int(cl)
        except Exception:
            pass
        return None
    except Exception:
        return None

# -------------------------
# Retry decorator for network operations
# -------------------------
def retry_on_network_errors(max_retries=NETWORK_MAX_RETRIES, base_backoff=NETWORK_BACKOFF_BASE):
    def deco(func):
        def wrapper(*args, **kwargs):
            attempt = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    is_network = False
                    if isinstance(e, NetworkError):
                        is_network = True
                    elif Urllib3SSLError and isinstance(e, Urllib3SSLError):
                        is_network = True
                    elif isinstance(e, ssl.SSLError):
                        is_network = True
                    elif isinstance(e, requests.exceptions.RequestException):
                        is_network = True
                    if not is_network:
                        raise
                    if attempt > max_retries:
                        raise
                    sleep_for = min(NETWORK_RETRY_SLEEP_MAX, (base_backoff ** attempt))
                    sleep_for = max(NETWORK_RETRY_SLEEP_MIN, sleep_for)
                    time.sleep(sleep_for)
        return wrapper
    return deco

# -------------------------
# Improved extract_info_safe
# -------------------------
class ExtractError(Exception):
    pass

# -------------------------
# محدودیت نرخ درخواست به تفکیک دامنه (برای جلوگیری از rate-limit شدن توسط سایت‌ها)
# -------------------------
DOMAIN_MIN_INTERVAL_SECONDS = {
    "instagram.com": 3.0,
    "www.instagram.com": 3.0,
    "tiktok.com": 2.5,
    "www.tiktok.com": 2.5,
    "x.com": 2.0,
    "www.x.com": 2.0,
    "twitter.com": 2.0,
    "www.twitter.com": 2.0,
    "facebook.com": 3.0,
    "www.facebook.com": 3.0,
    "fb.watch": 3.0,
    "youtube.com": 1.0,
    "www.youtube.com": 1.0,
    "youtu.be": 1.0,
}
DEFAULT_DOMAIN_MIN_INTERVAL = float(os.environ.get("DEFAULT_DOMAIN_MIN_INTERVAL", "1.0"))
_DOMAIN_LAST_ACCESS = {}
_DOMAIN_RATE_LOCK = Lock()


def throttle_domain(url):
    """
    قبل از هر extract/دانلود، فاصلهٔ حداقلی بین درخواست‌های همان دامنه را رعایت می‌کند
    تا احتمال بلاک‌شدن یا rate-limit سمت سایت مقصد کمتر شود.
    """
    try:
        net = urlparse(url).netloc.lower()
    except Exception:
        net = ""
    min_gap = DOMAIN_MIN_INTERVAL_SECONDS.get(net, DEFAULT_DOMAIN_MIN_INTERVAL)
    sleep_for = 0.0
    with _DOMAIN_RATE_LOCK:
        now = time.time()
        last = _DOMAIN_LAST_ACCESS.get(net, 0)
        wait = (last + min_gap) - now
        if wait > 0:
            sleep_for = wait
        _DOMAIN_LAST_ACCESS[net] = now + max(0.0, sleep_for)
    if sleep_for > 0:
        time.sleep(sleep_for)

@retry_on_network_errors()
def extract_info_safe(url, user_id=None):
    """
    استخراج اطلاعات ویدیو.
    برای یوتیوب: کوکی عمداً غیرفعال است (باعث خطا و کمبود کیفیت می‌شود).
    برای بقیهٔ سایت‌ها: اول با کوکی (اگر باشد)، بعد بدون کوکی.
    """
    throttle_domain(url)
    is_yt = is_youtube_url(url)

    def _base_opts(extra_extractor_args=None, use_cookies=True):
        opts = {
            "quiet": True,
            "no_warnings": True,
            "socket_timeout": YTDLP_SOCKET_TIMEOUT,
            "retries": YTDLP_RETRIES,
            "fragment_retries": YTDLP_FRAGMENT_RETRIES,
            "http_chunk_size": YTDLP_HTTP_CHUNK_SIZE,
            "http_headers": {"User-Agent": USER_AGENT_HEAD, "Accept-Language": "en-US,en;q=0.9"},
            "skip_download": True,
            "nocheckcertificate": True,
        }
        # کلاینت‌هایی که معمولاً بیشترین لیست فرمت (از جمله 1080/1440/2160) را می‌دهند
        extractor_args = {
            "generic": {"impersonate": "chrome"},
            "youtube": {
                "player_client": ["web", "android", "ios", "mweb", "tv"],
                "player_skip": ["configs"],
            },
        }
        if extra_extractor_args:
            extractor_args.update(extra_extractor_args)
        opts["extractor_args"] = extractor_args
        proxy = get_rotating_proxy()
        if proxy:
            opts["proxy"] = proxy
        # برای یوتیوب هرگز کوکی نفرست
        if use_cookies and user_id and not is_yt:
            cf = get_cookiefile(user_id)
            if cf:
                opts["cookiefile"] = cf
        return opts

    # یوتیوب: فقط بدون کوکی
    if is_yt:
        cookie_modes = [False]
    else:
        cookie_path = get_cookiefile(user_id) if user_id else None
        cookie_modes = ([True, False] if cookie_path else [False])

    # ترتیب تلاش برای گرفتن حداکثر کیفیت‌ها
    client_attempts = [
        None,  # پیش‌فرض android+web+ios+tv
        {"youtube": {"player_client": ["android", "web"], "player_skip": ["webpage", "configs"]}},
        {"youtube": {"player_client": ["tv", "web"]}},
        {"youtube": {"player_client": ["ios", "mweb"]}},
        {"youtube": {"player_client": ["web"]}},
    ]
    last_err = None
    for use_cookies in cookie_modes:
        mode_label = "with cookies" if use_cookies else "without cookies"
        for extra in client_attempts:
            try:
                with yt_dlp.YoutubeDL(_base_opts(extra, use_cookies=use_cookies)) as ydl:
                    info = ydl.extract_info(url, download=False)
                    if not is_yt and use_cookies is False and get_cookiefile(user_id):
                        logger.info("extract_info_safe: succeeded %s", mode_label)
                    return info
            except Exception as e:
                last_err = e
                logger.warning("extract_info_safe failed (%s): %s", mode_label, str(e)[:160])
                continue

    msg = str(last_err or "unknown")
    if "HTTP Error 403" in msg or "Cloudflare" in msg:
        if is_yt:
            raise ExtractError("یوتیوب دسترسی را محدود کرده. yt-dlp را به‌روز کن: pip install -U yt-dlp")
        raise ExtractError("منبع محافظت‌شده است. yt-dlp را به‌روز کن یا کوکی بفرست (/cookies).")
    if "reload" in msg.lower():
        raise ExtractError("یوتیوب: page needs reload. دستور: pip install -U yt-dlp")
    raise ExtractError("خطا در استخراج اطلاعات: %s" % msg)


def parse_formats_from_info(info):
    """
    استخراج و یکتاسازی همهٔ کیفیت‌های موجود.
    برای هر ارتفاع + نوع (muxed / video-only / audio) بهترین گزینه نگه داشته می‌شود.
    """
    formats = info.get("formats", []) or []
    parsed = []
    size_map = {}

    for f in formats:
        try:
            h = f.get("height")
            ext0 = (f.get("ext") or "").lower()
            s = f.get("filesize") or f.get("filesize_approx")
            if h is not None and ext0 and s:
                size_map[(int(h), ext0)] = int(s)
        except Exception:
            continue

    for f in formats:
        try:
            fid = str(f.get("format_id") or "")
            if not fid:
                continue
            ext = (f.get("ext") or "").lower() or "unknown"
            height = f.get("height")
            width = f.get("width")
            res = f.get("resolution") or ""
            size = f.get("filesize") or f.get("filesize_approx") or None
            url = f.get("url") or None
            mime = f.get("mime_type") or ""
            acodec = (f.get("acodec") or "").lower()
            vcodec = (f.get("vcodec") or "").lower()
            abr = f.get("abr")
            tbr = f.get("tbr")
            fps = f.get("fps")
            format_note = f.get("format_note") or ""
            protocol = (f.get("protocol") or "").lower()

            if "storyboard" in (format_note or "").lower():
                continue
            if ext in ("jpg", "png", "webp", "mhtml"):
                continue
            if vcodec in ("images",):
                continue

            if not height and isinstance(res, str):
                m = re.search(r"(\d{3,4})p", res)
                if m:
                    height = int(m.group(1))
                elif "x" in res:
                    try:
                        height = int(str(res).split("x")[-1])
                    except Exception:
                        pass

            if not size and height and ext:
                size = size_map.get((int(height), ext))

            is_video_only = bool(vcodec and vcodec not in ("none", "null", "unknown") and acodec in ("none", "null", "", "unknown"))
            is_audio_only = bool(acodec and acodec not in ("none", "null", "unknown") and vcodec in ("none", "null", "", "unknown"))
            is_muxed = (not is_video_only) and (not is_audio_only) and bool(
                (vcodec and vcodec not in ("none", "null")) or height
            )

            parts = []
            if height:
                parts.append("%sp" % height)
            elif is_audio_only:
                if abr:
                    try:
                        parts.append("%skbps" % int(abr))
                    except Exception:
                        parts.append("audio")
                else:
                    parts.append("audio")
            if fps and height:
                try:
                    if float(fps) >= 50:
                        parts.append("%sfps" % int(float(fps)))
                except Exception:
                    pass
            if is_video_only:
                parts.append("V")
            elif is_muxed:
                parts.append("AV")
            label = " ".join(parts) if parts else (format_note or ext or fid)

            is_preview = False
            if size and int(size) < 80 * 1024 and not height:
                is_preview = True

            parsed.append({
                "format_id": fid,
                "ext": ext,
                "resolution": res or (("%sp" % height) if height else ""),
                "height": height,
                "width": width,
                "size": int(size) if isinstance(size, (int, float)) else size,
                "size_text": human_size(size),
                "label": label,
                "url": url,
                "is_preview": is_preview,
                "mime": mime,
                "note": format_note,
                "acodec": acodec,
                "vcodec": vcodec,
                "is_video_only": is_video_only,
                "is_audio_only": is_audio_only,
                "is_muxed": is_muxed,
                "abr": abr,
                "tbr": tbr,
                "fps": fps,
                "protocol": protocol,
            })
        except Exception:
            continue

    best = {}
    for p in parsed:
        if p.get("is_preview"):
            continue
        h = p.get("height") or 0
        if p.get("is_audio_only"):
            kind = "audio"
            h = int(p.get("abr") or 0)
        elif p.get("is_video_only"):
            kind = "video"
        else:
            kind = "muxed"
        key = (kind, h, p.get("ext") or "")
        prev = best.get(key)
        if not prev:
            best[key] = p
            continue
        prev_score = (prev.get("size") or 0) or int((prev.get("tbr") or 0) * 1000)
        cur_score = (p.get("size") or 0) or int((p.get("tbr") or 0) * 1000)
        if cur_score >= prev_score:
            best[key] = p

    parsed = list(best.values())

    def sort_key(x):
        if x.get("is_audio_only"):
            return (0, x.get("abr") or 0)
        return (1, x.get("height") or 0, 2 if x.get("is_muxed") else 1)

    parsed.sort(key=lambda x: (-sort_key(x)[0], -sort_key(x)[1], -(sort_key(x)[2] if len(sort_key(x)) > 2 else 0)))

    if not parsed and info.get("format_id"):
        parsed.append({
            "format_id": str(info.get("format_id")),
            "ext": (info.get("ext") or "mp4").lower(),
            "resolution": "",
            "height": info.get("height"),
            "size": info.get("filesize") or info.get("filesize_approx"),
            "size_text": human_size(info.get("filesize") or info.get("filesize_approx")),
            "label": "default",
            "url": info.get("url"),
            "is_preview": False,
            "mime": "",
            "note": "",
            "is_video_only": False,
            "is_audio_only": False,
            "is_muxed": True,
        })
    return parsed


def build_category_tabs(request_id, active_category="all"):
    def mark(name, key):
        return ("● " + name) if active_category == key else name
    return [
        InlineKeyboardButton(mark("همه", "all"), callback_data="cat:%s:all:0" % request_id),
        InlineKeyboardButton(mark("MP4", "mp4"), callback_data="cat:%s:mp4:0" % request_id),
        InlineKeyboardButton(mark("WEBM", "webm"), callback_data="cat:%s:webm:0" % request_id),
        InlineKeyboardButton(mark("سایر", "other"), callback_data="cat:%s:other:0" % request_id),
    ]


def make_quality_keyboard(parsed_formats, request_id, category="all", page=0, multi_mode=False, selected=None):
    """نمایش همه کیفیت‌ها با صفحه‌بندی — پشتیبانی حالت چندانتخابی"""
    selected = selected or set()
    videos = [f for f in parsed_formats if not f.get("is_audio_only")]
    if category == "mp4":
        chosen = [f for f in videos if f.get("ext") == "mp4"]
    elif category == "webm":
        chosen = [f for f in videos if f.get("ext") == "webm"]
    elif category == "other":
        chosen = [f for f in videos if f.get("ext") not in ("mp4", "webm")]
    else:
        chosen = list(videos)

    if not chosen and videos:
        chosen = list(videos)

    page_size = max(6, min(12, PAGE_SIZE))
    start = page * page_size
    end = start + page_size
    rows = []
    rows.append(build_category_tabs(request_id, category))

    # دکمه سوییچ حالت چندانتخابی
    if multi_mode:
        rows.append([InlineKeyboardButton("☑ حالت چندانتخابی (فعال)", callback_data=f"multitoggle:{request_id}:0")])
    else:
        rows.append([InlineKeyboardButton("☐ انتخاب چند کیفیت", callback_data=f"multitoggle:{request_id}:1")])

    if not chosen:
        rows.append([InlineKeyboardButton("فرمت ویدیویی پیدا نشد — از بهترین استفاده کن", callback_data="dl:%s:best" % request_id)])
    else:
        for p in chosen[start:end]:
            size_text = p.get("size_text") or "—"
            height = p.get("height")
            ext = (p.get("ext") or "").upper()
            if p.get("is_video_only"):
                text = "%sp • %s • %s • فقط تصویر" % (height or "?", size_text, ext)
            elif p.get("is_muxed"):
                text = "%sp • %s • %s" % (height or "?", size_text, ext)
            else:
                text = "%s • %s • %s" % (p.get("label") or "file", size_text, ext)
            if len(text) > 55:
                text = text[:52] + "..."
            fid = p.get("format_id") or "best"
            if multi_mode:
                mark = "✅ " if str(fid) in selected else "⬜ "
                rows.append([InlineKeyboardButton(mark + text, callback_data=f"multisel:{request_id}:{fid}")])
            else:
                rows.append([InlineKeyboardButton(text, callback_data="dl:%s:%s" % (request_id, fid))])

        total = len(chosen)
        nav = []
        pages = max(1, (total + page_size - 1) // page_size)
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️ قبلی", callback_data="cat:%s:%s:%d" % (request_id, category, page - 1)))
        if end < total:
            nav.append(InlineKeyboardButton("صفحه %d/%d ➡️" % (page + 2, pages), callback_data="cat:%s:%s:%d" % (request_id, category, page + 1)))
        elif total > page_size:
            nav.append(InlineKeyboardButton("صفحه %d/%d" % (page + 1, pages), callback_data="noop:%s" % request_id))
        if nav:
            rows.append(nav)
        rows.append([InlineKeyboardButton("📊 تعداد کیفیت‌ها: %d" % total, callback_data="noop:%s" % request_id)])

    if multi_mode and selected:
        rows.append([InlineKeyboardButton(
            f"⬇️ دانلود {len(selected)} کیفیت انتخاب‌شده",
            callback_data=f"multidl:{request_id}"
        )])

    if not multi_mode:
        rows.append([
            InlineKeyboardButton("🎥 بهترین کیفیت", callback_data="dl:%s:best" % request_id),
            InlineKeyboardButton("⚡ خودکار هوشمند", callback_data="dl:%s:auto" % request_id),
        ])
        rows.append([
            InlineKeyboardButton("⭐ کیفیت‌های محبوب (720+1080+صدا)", callback_data=f"popular:{request_id}"),
        ])
        rows.append([
            InlineKeyboardButton("🎧 MP3 128k", callback_data="dl:%s:audio:mp3:128" % request_id),
            InlineKeyboardButton("🎧 MP3 192k", callback_data="dl:%s:audio:mp3:192" % request_id),
            InlineKeyboardButton("🎧 MP3 320k", callback_data="dl:%s:audio:mp3:320" % request_id),
        ])
        rows.append([
            InlineKeyboardButton("🎧 M4A", callback_data="dl:%s:audio:m4a:0" % request_id),
            InlineKeyboardButton("🎧 OPUS", callback_data="dl:%s:audio:opus:0" % request_id),
            InlineKeyboardButton("🎧 FLAC", callback_data="dl:%s:audio:flac:0" % request_id),
        ])
        rows.append([
            InlineKeyboardButton("📝 زیرنویس + بهترین", callback_data="dl:%s:best:subs" % request_id),
            InlineKeyboardButton("📄 فقط زیرنویس", callback_data="dl:%s:subs_only" % request_id),
        ])
        rows.append([
            InlineKeyboardButton("✨ هوش مصنوعی / فصل / تامبنیل", callback_data=f"ai:menu:{request_id}"),
        ])
        rows.append([
            InlineKeyboardButton("🌐 زبان زیرنویس", callback_data=f"sublangmenu:{request_id}:best"),
            InlineKeyboardButton("🗜 CRF دستی", callback_data=f"crfmenu:{request_id}:best"),
            InlineKeyboardButton("❌ لغو", callback_data="cancel"),
        ])
    else:
        rows.append([InlineKeyboardButton("❌ لغو", callback_data="cancel")])
    return InlineKeyboardMarkup(rows)


def make_confirm_keyboard(request_id, format_id, with_subs=False, large_warn=False):
    """تأیید حجم قبل از انتخاب محل ذخیره"""
    sub_flag = "1" if with_subs else "0"
    rows = []
    if large_warn:
        rows.append([InlineKeyboardButton("⚠️ فایل حجیم — ادامه", callback_data=f"confirm:{request_id}:{format_id}:{sub_flag}")])
    else:
        rows.append([InlineKeyboardButton("✅ تأیید و ادامه", callback_data=f"confirm:{request_id}:{format_id}:{sub_flag}")])
    rows.append([InlineKeyboardButton("❌ لغو", callback_data="cancel")])
    return InlineKeyboardMarkup(rows)

def make_output_mode_keyboard(request_id, format_id, with_subs=False):
    sub_flag = "1" if with_subs else "0"
    # format_id may contain extra audio codec like audio:mp3 — keep as-is in callback
    rows = [
        [InlineKeyboardButton("📤 ارسال به تلگرام", callback_data=f"mode:{request_id}:{format_id}:telegram:{sub_flag}")],
        [InlineKeyboardButton("💾 ذخیره محلی", callback_data=f"mode:{request_id}:{format_id}:local:{sub_flag}")],
        [InlineKeyboardButton("☁️ آپلود در Google Drive", callback_data=f"mode:{request_id}:{format_id}:drive:{sub_flag}")],
        [InlineKeyboardButton("📤+☁️ تلگرام و Drive", callback_data=f"mode:{request_id}:{format_id}:mirror:{sub_flag}")],
        [InlineKeyboardButton("🔗 فقط لینک مستقیم (بدون آپلود)", callback_data=f"mode:{request_id}:{format_id}:link_only:{sub_flag}")],
        [InlineKeyboardButton("🗜 فشرده + ارسال", callback_data=f"mode:{request_id}:{format_id}:telegram_compress:{sub_flag}")],
        [InlineKeyboardButton("📨 ارسال + فوروارد", callback_data=f"mode:{request_id}:{format_id}:telegram_forward:{sub_flag}")],
        [InlineKeyboardButton("⏱ لینک موقت ۲۴ساعته", callback_data=f"mode:{request_id}:{format_id}:templink:{sub_flag}")],
        [InlineKeyboardButton("❌ لغو", callback_data="cancel")],
    ]
    return InlineKeyboardMarkup(rows)


def make_queue_keyboard(user_jobs):
    """user_jobs: list of (task_id, meta)"""
    rows = []
    for task_id, meta in user_jobs[:12]:
        st = meta.get("status", "?")
        url = (meta.get("url") or "")[:35]
        owner = meta.get("user_id")
        rows.append([InlineKeyboardButton(f"❌ {st} | {url}", callback_data=f"cancel_dl:{task_id}:{owner}")])
    if not rows:
        rows.append([InlineKeyboardButton("صف خالی است", callback_data="noop:0")])
    return InlineKeyboardMarkup(rows)


def make_history_keyboard(history_items):
    rows = []
    for i, h in enumerate(reversed(history_items[-10:])):
        title = (h.get("title") or h.get("url") or "item")[:40]
        url = h.get("url") or ""
        if not url:
            continue
        rows.append([InlineKeyboardButton(f"⬇ {title}", callback_data=f"dl_direct:{url}")])
    if not rows:
        rows.append([InlineKeyboardButton("تاریخچه خالی", callback_data="noop:0")])
    return InlineKeyboardMarkup(rows)


def make_redownload_markup(url):
    if not url:
        return None
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 تلاش مجدد", callback_data=f"dl_direct:{url}")],
        [InlineKeyboardButton("📥 دانلود دوباره (بهترین)", callback_data=f"dl_direct:{url}")],
    ])


def friendly_yt_error(err):
    """ترجمه خطاهای رایج yt-dlp به فارسی ساده"""
    s = str(err or "")
    low = s.lower()
    rules = [
        ("sign in", "این ویدیو نیاز به ورود/کوکی دارد. فایل cookies را بفرست."),
        ("login required", "نیاز به ورود است. کوکی مرورگر را آپلود کن."),
        ("private video", "ویدیو خصوصی است و قابل دانلود نیست."),
        ("private", "محتوا خصوصی است."),
        ("members-only", "مخصوص اعضای کانال است."),
        ("premium", "مخصوص کاربران ویژه است."),
        ("copyright", "به‌خاطر حق نشر مسدود شده."),
        ("not available", "در این منطقه در دسترس نیست یا حذف شده."),
        ("geo", "محدودیت جغرافیایی دارد (از پروکسی استفاده کن)."),
        ("region", "محدودیت منطقه‌ای دارد."),
        ("http error 403", "دسترسی رد شد (403). کوکی یا پروکسی امتحان کن."),
        ("http error 404", "لینک پیدا نشد (404)."),
        ("http error 429", "محدودیت تعداد درخواست (کمی صبر کن)."),
        ("too many requests", "درخواست زیاد؛ بعداً تلاش کن."),
        ("unsupported url", "این لینک پشتیبانی نمی‌شود."),
        ("no video formats", "فرمت ویدیویی پیدا نشد."),
        ("requested format is not available", "کیفیت انتخاب‌شده موجود نیست."),
        ("ffmpeg", "مشکل ffmpeg — نصب بودن ffmpeg را بررسی کن."),
        ("aria2", "مشکل aria2؛ بدون آن دوباره تلاش می‌شود."),
        ("ssl", "خطای SSL/اتصال امن."),
        ("timed out", "زمان اتصال تمام شد. دوباره تلاش کن."),
        ("timeout", "زمان اتصال تمام شد."),
        ("connection", "مشکل اتصال اینترنت/شبکه."),
        ("page needs to be reloaded", "یوتیوب نیاز به رفرش داشت؛ دوباره تلاش کن یا yt-dlp را آپدیت کن."),
        ("confirm your age", "محدودیت سنی؛ با کوکی حساب واردشده امتحان کن."),
        ("join this channel", "عضویت در کانال لازم است."),
        ("live event", "پخش زنده هنوز تمام نشده یا در دسترس نیست."),
        ("cancelled", "عملیات لغو شد."),
        ("دانلود توسط کاربر لغو شد", "دانلود توسط شما لغو شد."),
    ]
    for key, msg in rules:
        if key in low or key in s:
            return msg
    # کوتاه‌سازی متن خام
    short = s.strip().replace("\n", " ")
    if len(short) > 180:
        short = short[:177] + "..."
    return short or "خطای ناشناخته"


def record_error(error, url=None, user_id=None):
    try:
        with RECENT_ERRORS_LOCK:
            RECENT_ERRORS.append({
                "ts": time.time(),
                "error": str(error)[:500],
                "url": (url or "")[:200],
                "user_id": user_id,
            })
            while len(RECENT_ERRORS) > RECENT_ERRORS_MAX:
                RECENT_ERRORS.pop(0)
    except Exception:
        pass


def make_retry_error_markup(url):
    if not url:
        return None
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔄 تلاش مجدد", callback_data=f"dl_direct:{url}")]])

def make_cancel_markup(task_id, owner_id):
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ لغو دانلود/آپلود", callback_data=f"cancel_dl:{task_id}:{owner_id}")]])


def make_request_cancel_markup(request_id, owner_id):
    """دکمه لغو برای مرحله انتخاب کیفیت (قبل از صف)"""
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ لغو درخواست", callback_data=f"cancel_req:{request_id}:{owner_id}")]])


def search_youtube_channel(uname, update, context):
    """جستجوی کانال یوتیوب با یوزرنیم (@name) — فراخوانی handle_channel_cmd"""
    try:
        # شبیه‌سازی context.args برای handle_channel_cmd
        class FakeContext:
            def __init__(self, bot, args):
                self.bot = bot
                self.args = args
        fake_ctx = FakeContext(context.bot, [uname])
        return handle_channel_cmd(update, fake_ctx)
    except Exception as e:
        try:
            update.message.reply_text(f"❗ خطا در جستجوی کانال: {str(e)[:200]}")
        except Exception:
            pass


# -------------------------
# progress hooks
# -------------------------
def ytdl_progress_hook_factory(task_id):
    def hook(d):
        d['task_id'] = task_id
        with CANCEL_LOCK:
            flag = CANCEL_FLAGS.get(task_id)
            if flag and flag.get("cancel"):
                raise Exception("دانلود توسط کاربر لغو شد")
        ytdl_progress_hook(d)
    return hook

def ytdl_progress_hook(d):
    task_id = d.get("task_id")
    if not task_id:
        return
    info_map = progress_map.get(task_id)
    if not info_map:
        return
    bot = info_map.get("bot")
    chat_id = info_map.get("chat_id")
    msg_id = info_map.get("msg_id")
    owner_id = info_map.get("owner_id")
    status = d.get("status")

    with CANCEL_LOCK:
        cancel_entry = CANCEL_FLAGS.get(task_id)
    reply_markup = None
    if cancel_entry:
        owner = cancel_entry.get("owner_id")
        reply_markup = make_cancel_markup(task_id, owner)

    try:
        if status == "downloading":
            downloaded = d.get("downloaded_bytes") or 0
            total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0

            if not total:
                info = info_map.get("info") or {}
                total = info.get("filesize") or 0
                if not total:
                    req_fmts = info.get("requested_formats") or info.get("formats") or []
                    ssum = 0
                    for rf in req_fmts:
                        s = rf.get("filesize") or rf.get("filesize_approx") or 0
                        ssum += s or 0
                    if ssum:
                        total = ssum

            now = time.time()
            instant_speed = d.get("speed") or 0

            # سرعت میانگین متحرک (smoothed) برای ETA پایدارتر
            samples = info_map.get("speed_samples") or []
            last_dl = info_map.get("last_downloaded")
            last_t = info_map.get("last_speed_t")
            if last_dl is not None and last_t is not None and now - last_t > 0.3:
                delta_b = downloaded - last_dl
                delta_t = now - last_t
                if delta_t > 0 and delta_b >= 0:
                    calc_speed = delta_b / delta_t
                    samples.append(calc_speed)
                    if len(samples) > 8:
                        samples = samples[-8:]
            info_map["speed_samples"] = samples
            info_map["last_downloaded"] = downloaded
            info_map["last_speed_t"] = now

            if samples:
                avg_speed = sum(samples) / len(samples)
            else:
                avg_speed = instant_speed or 0

            # ترکیب سرعت لحظه‌ای و میانگین برای نمایش
            display_speed = avg_speed if avg_speed > 0 else instant_speed
            if instant_speed and avg_speed:
                display_speed = (0.4 * instant_speed) + (0.6 * avg_speed)

            eta = d.get("eta")
            if total:
                try:
                    pct = int(downloaded * 100 / total)
                except Exception:
                    pct = 0
            else:
                pct = 0

            bar_len = 14
            filled = int(bar_len * pct / 100) if pct else 0
            bar = "█" * filled + "░" * (bar_len - filled)

            speed_mb = (display_speed / 1024 / 1024) if display_speed else 0
            # ETA پایدارتر بر اساس سرعت میانگین
            if display_speed and total and downloaded < total:
                eta_text = format_eta((total - downloaded) / max(display_speed, 1))
            elif eta is not None:
                eta_text = format_eta(eta)
            else:
                eta_text = "—"

            text = (
                f"📥 در حال دانلود\n"
                f"[{bar}] {pct}%\n"
                f"⚡ سرعت: {speed_mb:.2f} MB/s\n"
                f"📦 {human_size(downloaded)} / {human_size(total) if total else '—'}\n"
                f"⏳ باقی‌مانده: {eta_text}"
            )

            last_log = info_map.get("last_log_ts", 0)
            if now - last_log > 5:
                append_user_log(get_log_key_for_user(owner_id), {
                    "event": "download_progress",
                    "task_id": task_id,
                    "downloaded_bytes": downloaded,
                    "total_bytes": total,
                    "speed": display_speed,
                    "eta": eta,
                    "pct": pct
                })
                info_map["last_log_ts"] = now

            if now - info_map.get("last_edit", 0) > 0.9:
                try:
                    if reply_markup:
                        bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=text, reply_markup=reply_markup)
                    else:
                        bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=text)
                except Exception:
                    pass
            info_map["last_edit"] = now
            progress_map[task_id] = info_map

        elif status == "finished":
            try:
                bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text="دانلود تمام شد. در حال آماده‌سازی...")
            except Exception:
                pass
    except Exception:
        pass

# -------------------------
# yt-dlp download wrapper
# -------------------------
def _build_ydl_opts_base(outtmpl, format_spec=None, postprocessors=None, user_id=None, want_subtitles=False, subtitle_langs="fa,en", use_cookies=True, url=None):
    """ساخت گزینه‌های پایه yt-dlp (بدون external_downloader). برای یوتیوب کوکی اعمال نمی‌شود."""
    ydl_opts = {
        "outtmpl": outtmpl,
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,
        "socket_timeout": YTDLP_SOCKET_TIMEOUT,
        "retries": YTDLP_RETRIES,
        "fragment_retries": YTDLP_FRAGMENT_RETRIES,
        "http_chunk_size": YTDLP_HTTP_CHUNK_SIZE,
        "concurrent_fragment_downloads": max(1, YTDLP_CONCURRENT_FRAGMENTS),
        "http_headers": {"User-Agent": USER_AGENT_HEAD},
        "nocheckcertificate": True,
        "buffersize": 1024 * 1024 * 8,  # 8 MB buffer for faster IO
        "writethumbnail": True,
        "continuedl": YTDLP_CONTINUE,
        "nopart": False,
        "extractor_args": {
            "youtube": {
                "player_client": ["web", "android", "ios", "mweb", "tv"],
                "player_skip": ["configs"],
            },
        },
    }
    proxy = get_rotating_proxy()
    if proxy:
        ydl_opts["proxy"] = proxy
    # کوکی فقط برای غیر یوتیوب
    if use_cookies and user_id and not (url and is_youtube_url(url)):
        cf = get_cookiefile(user_id)
        if cf:
            ydl_opts["cookiefile"] = cf
    # محدودیت سرعت از prefs یا env
    try:
        if user_id:
            _prefs = load_user_prefs(user_id)
            rl = int(_prefs.get("rate_limit_bps") or 0) or DEFAULT_RATE_LIMIT
        else:
            rl = DEFAULT_RATE_LIMIT
        if rl and rl > 0:
            ydl_opts["ratelimit"] = rl
    except Exception:
        pass
    if want_subtitles:
        ydl_opts["writesubtitles"] = True
        ydl_opts["writeautomaticsub"] = True
        ydl_opts["subtitleslangs"] = [x.strip() for x in (subtitle_langs or "fa,en").split(",") if x.strip()]
        ydl_opts["subtitlesformat"] = "best"
    if format_spec:
        ydl_opts["format"] = format_spec
    if postprocessors:
        ydl_opts["postprocessors"] = postprocessors
    return ydl_opts


def _aria2_external_args():
    """آرگومان‌های aria2 — check-certificate=false برای رفع SSL/TLS روی ویندوز"""
    return {
        "default": [
            "-x", "8",
            "-s", "8",
            "-k", "1M",
            "--file-allocation=none",
            "--min-split-size=1M",
            "--max-connection-per-server=8",
            "--check-certificate=false",
            "--allow-overwrite=true",
            "--auto-file-renaming=false",
            "--console-log-level=error",
            "--summary-interval=0",
        ]
    }


@retry_on_network_errors()
def yt_dlp_download_with_hook(url, outtmpl, format_spec=None, postprocessors=None, task_id=None, bot=None, chat_id=None, msg_id=None, user_id=None, want_subtitles=False, subtitle_langs="fa,en"):
    """
    دانلود دوگانه (Dual download):
      1) اول با yt-dlp + aria2c (اگر نصب باشد و فعال باشد) — سرعت بالاتر
      2) اگر خطا داد، با yt-dlp خالص (native downloader) دوباره تلاش می‌کند
    در صورت لغو توسط کاربر، خطا را بالا می‌برد و fallback انجام نمی‌شود.
    """
    hook = ytdl_progress_hook_factory(task_id)
    if task_id:
        progress_map[task_id] = progress_map.get(task_id, {})
    throttle_domain(url)

    # برای یوتیوب کوکی غیرفعال است
    if is_youtube_url(url):
        cookie_modes = [False]
    else:
        cookie_path = get_cookiefile(user_id) if user_id else None
        cookie_modes = ([True, False] if cookie_path else [False])
    # یوتیوب/googlevideo روی ویندوز اغلب SSL با aria2 می‌دهد → فقط yt-dlp
    skip_aria2 = False
    try:
        net = (urlparse(url).netloc or "").lower()
        if any(x in net for x in ("youtube.com", "youtu.be", "googlevideo.com", "youtube-nocookie.com")):
            skip_aria2 = True
    except Exception:
        pass
    aria2_available = bool(YTDLP_USE_ARIA2 and shutil.which("aria2c") and not skip_aria2)
    last_error = None

    def _cleanup_partials():
        try:
            base_no_ext = os.path.splitext(outtmpl.replace("%(ext)s", ""))[0]
            for p in Path(DOWNLOAD_ROOT).glob("%s*" % os.path.basename(base_no_ext)):
                if p.is_file() and p.suffix.lower() in (".part", ".aria2", ".tmp", ".ytdl"):
                    try:
                        p.unlink()
                    except Exception:
                        pass
        except Exception:
            pass

    for use_cookies in cookie_modes:
        mode_label = "with cookies" if use_cookies else "without cookies"
        base_opts = _build_ydl_opts_base(
            outtmpl, format_spec=format_spec, postprocessors=postprocessors,
            user_id=user_id, want_subtitles=want_subtitles, subtitle_langs=subtitle_langs,
            use_cookies=use_cookies, url=url,
        )
        base_opts["progress_hooks"] = [hook]

        if aria2_available:
            opts_aria = dict(base_opts)
            opts_aria["external_downloader"] = "aria2c"
            opts_aria["external_downloader_args"] = _aria2_external_args()
            logger.info("Dual-download: aria2 (%s) for %s", mode_label, (url or "")[:80])
            try:
                with yt_dlp.YoutubeDL(opts_aria) as ydl:
                    info = ydl.extract_info(url, download=True)
                    filename = ydl.prepare_filename(info)
                    logger.info("Dual-download: aria2 succeeded (%s)", mode_label)
                    return filename, info
            except Exception as e:
                with CANCEL_LOCK:
                    flag = CANCEL_FLAGS.get(task_id) if task_id else None
                    if flag and flag.get("cancel"):
                        raise
                last_error = e
                logger.warning("Dual-download: aria2 failed (%s): %s", mode_label, str(e)[:200])
                _cleanup_partials()

        opts_native = dict(base_opts)
        opts_native.pop("external_downloader", None)
        opts_native.pop("external_downloader_args", None)
        logger.info("Dual-download: pure yt-dlp (%s) for %s", mode_label, (url or "")[:80])
        try:
            with yt_dlp.YoutubeDL(opts_native) as ydl:
                info = ydl.extract_info(url, download=True)
                filename = ydl.prepare_filename(info)
                logger.info("Dual-download: pure yt-dlp succeeded (%s)", mode_label)
                return filename, info
        except Exception as e:
            with CANCEL_LOCK:
                flag = CANCEL_FLAGS.get(task_id) if task_id else None
                if flag and flag.get("cancel"):
                    raise
            last_error = e
            logger.warning("Dual-download: pure failed (%s): %s", mode_label, str(e)[:200])
            _cleanup_partials()
            continue

    logger.error("Dual-download: all methods failed: %s", str(last_error)[:200] if last_error else "")
    raise last_error if last_error else RuntimeError("download failed")

# -------------------------
# upload with progress (Bot API) - supports sendDocument and sendAnimation
# -------------------------
def upload_file_with_progress(bot_token, chat_id, file_path, caption, progress_callback, timeout=3600, task_id=None, api_method="sendDocument"):
    """
    api_method: "sendDocument" or "sendAnimation"
    بهبودها:
    - session.trust_env = False و proxies=None تا پراکسی سیستم نادیده گرفته شود (رفع ProxyError/SSLEOF)
    - retry با backoff برای خطاهای شبکه
    - keep-alive حفظ شده
    """
    if api_method not in ("sendDocument", "sendAnimation"):
        api_method = "sendDocument"
    url = f"https://api.telegram.org/bot{bot_token}/{api_method}"
    filename = os.path.basename(file_path)
    field_name = "document" if api_method == "sendDocument" else "animation"

    session = requests.Session()
    session.trust_env = False  # مهم: پراکسی سیستم (HTTP_PROXY/HTTPS_PROXY) را کاملاً نادیده بگیر
    session.headers.update({"Connection": "keep-alive", "User-Agent": USER_AGENT_HEAD})

    max_attempts = 4
    last_exc = None

    for attempt in range(1, max_attempts + 1):
        fobj = None
        try:
            fobj = open(file_path, "rb")
            m = MultipartEncoder(fields={
                "chat_id": str(chat_id),
                "caption": caption or "",
                field_name: (filename, fobj, "application/octet-stream")
            })

            def monitor_callback(monitor):
                try:
                    if task_id:
                        with CANCEL_LOCK:
                            flag = CANCEL_FLAGS.get(task_id)
                            if flag and flag.get("cancel"):
                                raise Exception("Upload cancelled by user")
                    progress_callback(monitor.bytes_read, m.len)
                except Exception:
                    raise

            monitor = MultipartEncoderMonitor(m, monitor_callback)
            headers = {"Content-Type": monitor.content_type}

            r = session.post(
                url,
                data=monitor,
                headers=headers,
                timeout=timeout,
                proxies={"http": None, "https": None},
            )
            return r
        except Exception as e:
            last_exc = e
            with CANCEL_LOCK:
                flag = CANCEL_FLAGS.get(task_id)
                if flag and flag.get("cancel"):
                    raise Exception("Upload cancelled by user") from e

            is_network = False
            if isinstance(e, (requests.exceptions.RequestException, ssl.SSLError, NetworkError)):
                is_network = True
            elif Urllib3SSLError and isinstance(e, Urllib3SSLError):
                is_network = True

            if not is_network or attempt >= max_attempts:
                raise

            wait = min(6 * attempt, 20)
            logger.warning(
                "Bot API upload attempt %d/%d failed: %s. Retrying in %ds...",
                attempt, max_attempts, str(e)[:150], wait
            )
            time.sleep(wait)
        finally:
            if fobj:
                try:
                    fobj.close()
                except Exception:
                    pass

    try:
        session.close()
    except Exception:
        pass
    raise last_exc if last_exc else RuntimeError("Upload failed after retries")

# -------------------------
# Telethon send helper (cancellable, improved)
# -------------------------
def telethon_send_file(chat_id, file_path, caption=None, progress_callback=None, task_id=None):
    """
    ارسال فایل با Telethon روی event loop اختصاصی (thread-safe).
    رفع خطای: The asyncio event loop must not change after connection
    """
    import asyncio
    client = ensure_telethon_client()
    if not client:
        raise RuntimeError("Telethon client not configured or not available.")
    loop = telethon_loop or getattr(client, "loop", None)
    if loop is None:
        raise RuntimeError("Telethon event loop is not ready.")

    file_size = None
    try:
        file_size = os.path.getsize(file_path)
    except Exception:
        file_size = None

    def choose_part_size_kb(size_bytes):
        lo = max(256, TELETHON_MIN_PART_KB)
        hi = max(lo, TELETHON_MAX_PART_KB)
        if not size_bytes:
            return lo
        mb = size_bytes / (1024 * 1024)
        if mb <= 30:
            chosen = lo
        elif mb <= 100:
            chosen = max(lo, 1024)
        elif mb <= 500:
            chosen = max(lo, 2048)
        else:
            chosen = hi
        for p in (256, 512, 1024, 2048, 4096):
            if p >= chosen:
                return min(max(p, lo), hi)
        return hi

    part_size_kb = choose_part_size_kb(file_size)
    max_attempts = 5
    attempt = 0
    last_exc = None

    while attempt < max_attempts:
        attempt += 1
        try:
            def wrapped_progress(sent, total):
                if task_id:
                    with CANCEL_LOCK:
                        flag = CANCEL_FLAGS.get(task_id)
                        if flag and flag.get("cancel"):
                            raise Exception("Upload cancelled by user")
                if progress_callback:
                    progress_callback(sent, total)

            async def _do_send():
                return await client.send_file(
                    entity=chat_id,
                    file=file_path,
                    caption=caption or "",
                    progress_callback=wrapped_progress,
                    part_size_kb=part_size_kb,
                )

            # همیشه از loop اختصاصی Telethon استفاده کن
            if loop.is_running():
                fut = asyncio.run_coroutine_threadsafe(_do_send(), loop)
                fut.result(timeout=3600)
            else:
                loop.run_until_complete(_do_send())
            return True
        except Exception as e:
            last_exc = e
            with CANCEL_LOCK:
                flag = CANCEL_FLAGS.get(task_id)
                if flag and flag.get("cancel"):
                    raise
            msg = str(e).lower()
            # خطای loop: یک‌بار reconnect
            if "event loop" in msg or "must not change" in msg:
                logger.warning("Telethon loop error on attempt %s: %s", attempt, e)
                try:
                    # force restart client
                    global telethon_client, telethon_ready
                    with telethon_lock:
                        telethon_client = None
                        telethon_ready.clear()
                    ensure_telethon_client()
                    client = telethon_client
                    loop = telethon_loop or getattr(client, "loop", None)
                except Exception as re:
                    logger.warning("Telethon restart failed: %s", re)
            sleep_for = min(20.0, (1.5 ** attempt) + random.random())
            logger.warning("Telethon upload attempt %s failed: %s. Retrying in %.1fs", attempt, e, sleep_for)
            time.sleep(sleep_for)
            continue
    raise RuntimeError("Telethon upload failed after retries: %s" % last_exc)


def upload_with_smart_choice(bot_token, bot, chat_id, file_path, caption, progress_update_fn=None, task_id=None, as_animation=False):
    """
    انتخاب مسیر آپلود:
    - اگر FORCE_TELETHON_ALWAYS فعال باشد: همیشه از Telethon استفاده کن (تا سقف 2GiB)
    - اگر فایل <= 30MB: اول Bot API؛ در صورت خطای شبکه/پراکسی به Telethon fallback کن
    - اگر فایل > 30MB و <= 2 GiB: از Telethon استفاده کن
    - اگر فایل > 2 GiB: خطا بده
    - as_animation: اگر True و Bot API انتخاب شد، از sendAnimation استفاده کن تا گیف‌ها به‌صورت انیمیشن ارسال شوند
    """
    total_size = os.path.getsize(file_path)

    def telethon_progress(sent, total):
        try:
            if progress_update_fn:
                progress_update_fn(sent, total)
        except Exception:
            raise

    # If forced Telethon usage
    if FORCE_TELETHON_ALWAYS:
        if total_size > MAX_SINGLE_UPLOAD_BYTES:
            raise RuntimeError(f"File size {human_size(total_size)} exceeds 2 GiB limit.")
        return telethon_send_file(chat_id, file_path, caption=caption, progress_callback=telethon_progress, task_id=task_id)

    if total_size > MAX_SINGLE_UPLOAD_BYTES:
        raise RuntimeError(f"File size {human_size(total_size)} exceeds 2 GiB limit.")

    # Threshold: 30 MB
    threshold = 30 * 1024 * 1024
    if total_size <= threshold:
        api_method = "sendAnimation" if as_animation else "sendDocument"
        try:
            resp = upload_file_with_progress(
                bot_token, chat_id, file_path, caption,
                progress_update_fn, timeout=3600, task_id=task_id, api_method=api_method
            )
            if resp is None:
                raise RuntimeError("No response from Telegram Bot API during upload.")
            if resp.status_code != 200:
                raise RuntimeError(f"Upload failed: {resp.status_code} {resp.text[:400]}")
            return resp
        except Exception as e:
            err_str = str(e).lower()
            network_keywords = (
                "proxy", "ssl", "connection", "timeout", "max retries",
                "eof", "ssleof", "unable to connect", "network"
            )
            if any(k in err_str for k in network_keywords):
                logger.warning(
                    "Bot API failed with network/proxy error, falling back to Telethon: %s",
                    str(e)[:150]
                )
                # ادامه به Telethon
            else:
                raise  # خطای غیرشبکه‌ای را بالا بده

    # Telethon (فایل بزرگ یا fallback)
    return telethon_send_file(
        chat_id, file_path,
        caption=caption,
        progress_callback=telethon_progress,
        task_id=task_id
    )

# -------------------------
# Worker logic
# -------------------------
def download_worker_thread(bot):
    global active_workers
    while True:
        _prio, _seq, task = download_queue.get()
        if task is None:
            break
        # حالت کم‌مصرف: اگر سرور تحت فشار است کمی صبر کن
        if system_under_pressure():
            try:
                time.sleep(3)
            except Exception:
                pass
        task_started_at = time.time()
        with active_workers_lock:
            active_workers += 1
        try:
            chat_id = task.get("chat_id")
            url = task.get("url")
            action = task.get("action")
            format_id = task.get("format_id")
            owner_id = task.get("user_id")
            username = task.get("username") or None
            request_id = task.get("request_id")
            request_info = task.get("request_info")
            mode = task.get("mode") or "telegram"  # 'telegram' | 'local' | 'telegram_compress' | 'telegram_forward'
            want_subtitles = bool(task.get("want_subtitles"))
            prefs = load_user_prefs(owner_id)
            subtitle_langs = prefs.get("subtitle_langs") or "fa,en"
            send_thumb = prefs.get("send_thumbnail", True)
            do_forward = mode == "telegram_forward" or bool(prefs.get("auto_forward"))
            if mode == "telegram_forward":
                mode = "telegram"
            batch_id = task.get("batch_id")
            trim_start = task.get("trim_start")
            trim_end = task.get("trim_end")

            ok_acc, acc_msg = user_allowed(owner_id)
            if not ok_acc:
                try:
                    bot.send_message(chat_id=chat_id, text=acc_msg)
                except Exception:
                    pass
                continue

            # rate limit
            ok_limit, limit_msg = check_user_limits(owner_id)
            if not ok_limit:
                try:
                    bot.send_message(chat_id=chat_id, text=f"⛔ {limit_msg}")
                except Exception:
                    pass
                continue
            user_limit_start(owner_id)

            # create a unique task_id early
            task_id = uuid.uuid4().hex[:12]
            with CANCEL_LOCK:
                CANCEL_FLAGS[task_id] = {"cancel": False, "owner_id": owner_id}
            with QUEUE_META_LOCK:
                QUEUE_META[task_id] = {"user_id": owner_id, "url": url, "status": "queued", "created": time.time(), "title": ""}

            # initial progress message with cancel button
            try:
                progress_msg = bot.send_message(chat_id=chat_id, text="در حال آماده‌سازی دانلود...", reply_markup=make_cancel_markup(task_id, owner_id))
                progress_msg_id = progress_msg.message_id
            except Exception:
                progress_msg = bot.send_message(chat_id=chat_id, text="در حال آماده‌سازی دانلود...")
                progress_msg_id = progress_msg.message_id
                try:
                    bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text="در حال آماده‌سازی دانلود...", reply_markup=make_cancel_markup(task_id, owner_id))
                except:
                    pass

            with QUEUE_META_LOCK:
                if task_id in QUEUE_META:
                    QUEUE_META[task_id]["status"] = "downloading"

            append_user_log(get_log_key_for_user(owner_id), {
                "event": "download_request",
                "url": url,
                "format_requested": format_id or "best",
                "request_id": request_id,
                "task_id": task_id,
                "mode": mode
            })

            # check request-level cancel
            if request_id:
                with REQUESTS_LOCK:
                    req = REQUESTS.get(request_id)
                if req and req.get("cancel"):
                    try:
                        bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text="❌ این درخواست قبلاً لغو شده است.")
                    except:
                        pass
                    append_user_log(get_log_key_for_user(owner_id), {"event": "request_cancelled_before_start", "request_id": request_id})
                    with CANCEL_LOCK:
                        CANCEL_FLAGS.pop(task_id, None)
                    user_limit_finish(owner_id)
                    with QUEUE_META_LOCK:
                        QUEUE_META.pop(task_id, None)
                    continue

            # use request_info if provided to avoid re-extract
            info = None
            if request_info:
                info = request_info
            else:
                try:
                    append_user_log(get_log_key_for_user(owner_id), {"event": "extract_start", "url": url, "task_id": task_id})
                    info = extract_info_safe(url, user_id=owner_id)
                    append_user_log(get_log_key_for_user(owner_id), {"event": "extract_ok", "url": url, "title": info.get("title") if info else None, "task_id": task_id})
                except ExtractError as e:
                    try:
                        fe = friendly_yt_error(e)
                        bot.edit_message_text(
                            chat_id=chat_id, message_id=progress_msg_id,
                            text=f"❗ خطا در استخراج اطلاعات:\n{fe}",
                            reply_markup=make_retry_error_markup(url),
                        )
                    except:
                        pass
                    record_error(e, url=url, user_id=owner_id)
                    append_user_log(get_log_key_for_user(owner_id), {"event": "extract_error", "error": str(e), "url": url, "task_id": task_id})
                    with CANCEL_LOCK:
                        CANCEL_FLAGS.pop(task_id, None)
                    user_limit_finish(owner_id)
                    with QUEUE_META_LOCK:
                        QUEUE_META.pop(task_id, None)
                    continue
                except Exception as e:
                    try:
                        fe = friendly_yt_error(e)
                        bot.edit_message_text(
                            chat_id=chat_id, message_id=progress_msg_id,
                            text=f"❗ خطا در استخراج اطلاعات:\n{fe}",
                            reply_markup=make_retry_error_markup(url),
                        )
                    except:
                        pass
                    record_error(e, url=url, user_id=owner_id)
                    append_user_log(get_log_key_for_user(owner_id), {"event": "extract_error", "error": str(e), "url": url, "task_id": task_id})
                    with CANCEL_LOCK:
                        CANCEL_FLAGS.pop(task_id, None)
                    user_limit_finish(owner_id)
                    with QUEUE_META_LOCK:
                        QUEUE_META.pop(task_id, None)
                    continue

            # Determine final format_spec and optional postprocessors
            final_format_spec = None
            postprocessors = None
            shot_at = task.get("shot_at")
            clip_start = task.get("clip_start")
            clip_end = task.get("clip_end")
            gif_start = task.get("gif_start")
            gif_duration = task.get("gif_duration")
            if action == "shot":
                # دانلود بهترین ویدیو سپس اسکرین‌شات
                action = "video"
                format_id = "best"
            if action == "clip":
                action = "video"
                format_id = format_id or "best"
            if action == "gif":
                action = "video"
                format_id = format_id or "best[height<=480]/best"
            if action == "live" or task.get("live_duration"):
                # ضبط لایو
                live_dur = int(task.get("live_duration") or LIVE_RECORD_SECONDS)
                try:
                    if progress_msg_id:
                        safe_edit_message(bot, chat_id, progress_msg_id, f"🔴 ضبط لایو تا {live_dur // 60} دقیقه...")
                except Exception:
                    pass
                outtmpl = os.path.join(user_dir, f"live_{task_id}.%(ext)s")
                try:
                    found = download_live_stream(url, outtmpl, duration_sec=live_dur, user_id=owner_id, task_id=task_id)
                    # بقیه مسیر مثل ویدیو عادی — فایل را به found می‌سپاریم
                    task["_live_file"] = found
                    action = "video"
                    format_id = "best"
                    final_format_spec = "best"
                    # از دانلود دوباره رد شو
                    info = info or {}
                except Exception as e:
                    raise RuntimeError(f"خطا در ضبط لایو: {e}")
            if action == "subs" or format_id == "subs_only":
                # فقط زیرنویس: ویدیو دانلود نمی‌شود، فقط زیرنویس‌ها
                action = "video"
                format_id = "best"
                want_subtitles = True
                # برای جلوگیری از دانلود سنگین ویدیو، از فرمت خیلی سبک استفاده می‌کنیم
                # و بعداً فقط فایل‌های .srt/.vtt ارسال می‌شوند (منطق موجود want_subtitles)
                final_format_spec = "worstvideo/worst"  # سبک‌ترین ممکن
                postprocessors = None
            elif action == "audio" or (isinstance(format_id, str) and str(format_id).startswith("audio")):
                final_format_spec = "bestaudio/best"
                codec = "mp3"
                quality = "192"
                if isinstance(format_id, str) and ":" in str(format_id):
                    pa = str(format_id).split(":")
                    if len(pa) >= 2:
                        codec = pa[1]
                    if len(pa) >= 3 and str(pa[2]).isdigit():
                        quality = str(pa[2])
                    elif codec != "mp3":
                        quality = "0"
                elif prefs.get("audio_format"):
                    codec = prefs.get("audio_format")
                    quality = "192" if codec == "mp3" else "0"
                if codec not in ("mp3", "m4a", "opus", "flac", "wav", "ogg"):
                    codec = "mp3"
                    quality = "192"
                postprocessors = [{
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": codec,
                    "preferredquality": quality,
                }]
                action = "audio"
            else:
                if not format_id or format_id == "best":
                    if is_youtube_url(url):
                        final_format_spec = "bestvideo+bestaudio/best"
                    else:
                        final_format_spec = "best"
                elif isinstance(format_id, str) and "height<=" in str(format_id):
                    # از پلی‌لیست: best[height<=720]/best
                    final_format_spec = str(format_id)
                else:
                    # try to validate format_id exists in parsed formats
                    parsed = parse_formats_from_info(info) if info else []
                    found_fmt = any(p.get("format_id") == format_id for p in parsed)
                    if not found_fmt:
                        if is_youtube_url(url):
                            final_format_spec = "bestvideo+bestaudio/best"
                        else:
                            final_format_spec = "best"
                    else:
                        final_format_spec = f"{format_id}+bestaudio/best"

            safe_base = sanitize_name((info.get("title") if info else "file") or "file")
            outtmpl = os.path.join(DOWNLOAD_ROOT, f"{safe_base}.%(ext)s")
            # حالت فقط لینک: بدون دانلود فایل
            if mode == "link_only":
                try:
                    direct = None
                    if info:
                        for f in (info.get("formats") or []):
                            if format_id and str(f.get("format_id")) == str(format_id) and f.get("url"):
                                direct = f.get("url")
                                break
                        if not direct:
                            # بهترین فرمت با url
                            for f in reversed(info.get("formats") or []):
                                if f.get("url") and (f.get("vcodec") != "none" or f.get("acodec") != "none"):
                                    direct = f.get("url")
                                    break
                        if not direct:
                            direct = info.get("url") or info.get("webpage_url") or url
                    else:
                        direct = url
                    title = (info.get("title") if info else "") or ""
                    bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text="✅ لینک آماده شد")
                    body = "🔗 " + (title[:80] + "\n" if title else "") + (direct or "—")
                    if len(body) > 4000:
                        body = body[:3990] + "..."
                    bot.send_message(chat_id=chat_id, text=body)
                    append_user_log(get_log_key_for_user(owner_id), {"event": "link_only", "url": url})
                    append_user_history(owner_id, {"url": url, "title": title, "status": "link_only", "mode": "link_only"})
                    with GLOBAL_STATS_LOCK:
                        GLOBAL_STATS["downloads_ok"] += 1
                    user_limit_finish(owner_id)
                    with CANCEL_LOCK:
                        CANCEL_FLAGS.pop(task_id, None)
                    with QUEUE_META_LOCK:
                        QUEUE_META.pop(task_id, None)
                    continue
                except Exception as le:
                    try:
                        bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text="❗ " + friendly_yt_error(le))
                    except Exception:
                        pass
                    user_limit_finish(owner_id)
                    continue

            progress_map[task_id] = {
                "bot": bot,
                "chat_id": chat_id,
                "msg_id": progress_msg_id,
                "owner_id": owner_id,
                "username": username,
                "info": info,
                "last_edit": 0,
                "last_log_ts": 0
            }

            # سقف حجم سخت
            if HARD_MAX_FILE_MB and HARD_MAX_FILE_MB > 0 and info:
                est = info.get("filesize") or info.get("filesize_approx") or 0
                try:
                    if not est:
                        for rf in (info.get("requested_formats") or []):
                            est += rf.get("filesize") or rf.get("filesize_approx") or 0
                except Exception:
                    pass
                if est and int(est) > HARD_MAX_FILE_MB * 1024 * 1024:
                    try:
                        bot.edit_message_text(
                            chat_id=chat_id, message_id=progress_msg_id,
                            text=f"🚫 حجم تقریبی فایل ({human_size(est)}) از سقف مجاز ({HARD_MAX_FILE_MB}MB) بیشتر است.",
                        )
                    except Exception:
                        pass
                    append_user_log(get_log_key_for_user(owner_id), {"event": "hard_size_block", "size": est, "limit_mb": HARD_MAX_FILE_MB})
                    user_limit_finish(owner_id)
                    with CANCEL_LOCK:
                        CANCEL_FLAGS.pop(task_id, None)
                    with QUEUE_META_LOCK:
                        QUEUE_META.pop(task_id, None)
                    continue

            try:
                if task.get("_live_file") and os.path.exists(task["_live_file"]):
                    filename, dl_info = task["_live_file"], info
                else:
                    filename, dl_info = yt_dlp_download_with_hook(
                        url, outtmpl,
                        format_spec=final_format_spec,
                        postprocessors=postprocessors,
                        task_id=task_id, bot=bot, chat_id=chat_id, msg_id=progress_msg_id,
                        user_id=owner_id,
                        want_subtitles=want_subtitles,
                        subtitle_langs=subtitle_langs,
                    )
                    if dl_info:
                        info = dl_info
            except Exception as e:
                with CANCEL_LOCK:
                    flag = CANCEL_FLAGS.get(task_id)
                    was_cancelled = flag and flag.get("cancel")
                if was_cancelled:
                    try:
                        bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text="❌ دانلود توسط کاربر لغو شد.")
                    except:
                        pass
                    append_user_log(get_log_key_for_user(owner_id), {"event": "download_cancelled", "task_id": task_id})
                else:
                    try:
                        fe = friendly_yt_error(e)
                        bot.edit_message_text(
                            chat_id=chat_id, message_id=progress_msg_id,
                            text=f"❗ خطا در دانلود:\n{fe}",
                            reply_markup=make_retry_error_markup(url),
                        )
                    except:
                        pass
                    record_error(e, url=url, user_id=owner_id)
                    append_user_log(get_log_key_for_user(owner_id), {"event": "download_error", "error": str(e), "url": url, "task_id": task_id})
                    with GLOBAL_STATS_LOCK:
                        GLOBAL_STATS["downloads_fail"] += 1
                    batch_item_done(batch_id, ok=False, bot=bot)
                with CANCEL_LOCK:
                    CANCEL_FLAGS.pop(task_id, None)
                user_limit_finish(owner_id)
                with QUEUE_META_LOCK:
                    QUEUE_META.pop(task_id, None)
                continue

            # find actual file path
            found = None
            try:
                if os.path.exists(filename):
                    found = filename
                else:
                    base_no_ext = os.path.splitext(os.path.basename(outtmpl))[0]
                    for p in Path(DOWNLOAD_ROOT).glob(f"{base_no_ext}.*"):
                        if p.is_file():
                            found = str(p)
                            break
            except Exception:
                found = None

            if not found:
                try:
                    bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text="❗ فایل دانلود شده پیدا نشد")
                except:
                    pass
                append_user_log(get_log_key_for_user(owner_id), {"event": "file_not_found_after_download", "url": url, "task_id": task_id})
                with CANCEL_LOCK:
                    CANCEL_FLAGS.pop(task_id, None)
                user_limit_finish(owner_id)
                with QUEUE_META_LOCK:
                    QUEUE_META.pop(task_id, None)
                continue

            try:
                file_size = os.path.getsize(found)
            except Exception:
                file_size = None

            # حالت اسکرین‌شات
            if shot_at is not None and found:
                try:
                    bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text=f"🖼 گرفتن اسکرین‌شات در {shot_at}s ...")
                    shot_path = found + f".shot_{int(shot_at)}.jpg"
                    extract_screenshot_ffmpeg(found, shot_path, at_sec=shot_at)
                    with open(shot_path, "rb") as img:
                        bot.send_photo(chat_id=chat_id, photo=img, caption=f"🖼 ثانیه {shot_at}")
                    try:
                        os.remove(shot_path)
                    except Exception:
                        pass
                    try:
                        os.remove(found)
                    except Exception:
                        pass
                    try:
                        bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text="✅ اسکرین‌شات ارسال شد")
                    except Exception:
                        pass
                    append_user_history(owner_id, {"url": url, "title": "screenshot", "format_id": "shot", "size": 0, "status": "ok", "mode": "shot"})
                    batch_item_done(batch_id, ok=True, title="shot", bot=bot) if batch_id else None
                    with GLOBAL_STATS_LOCK:
                        GLOBAL_STATS["downloads_ok"] += 1
                    user_limit_finish(owner_id, bytes_count=0)
                    with CANCEL_LOCK:
                        CANCEL_FLAGS.pop(task_id, None)
                    with QUEUE_META_LOCK:
                        QUEUE_META.pop(task_id, None)
                    notify_queue_waiters(bot)
                    continue
                except Exception as se:
                    try:
                        bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text=f"❗ خطا در اسکرین‌شات: {se}")
                    except Exception:
                        pass
                    batch_item_done(batch_id, ok=False, bot=bot) if batch_id else None
                    user_limit_finish(owner_id)
                    with CANCEL_LOCK:
                        CANCEL_FLAGS.pop(task_id, None)
                    continue

            # کلیپ کوتاه
            if found and clip_start is not None and clip_end is not None:
                try:
                    bot.edit_message_text(
                        chat_id=chat_id, message_id=progress_msg_id,
                        text=f"🎬 ساخت کلیپ ({clip_start}s → {clip_end}s) ...",
                        reply_markup=make_cancel_markup(task_id, owner_id),
                    )
                    clip_path = found + ".clip.mp4"
                    make_clip_ffmpeg(found, clip_path, clip_start, clip_end)
                    if os.path.exists(clip_path) and os.path.getsize(clip_path) > 0:
                        try:
                            os.remove(found)
                        except Exception:
                            pass
                        found = clip_path
                        file_size = os.path.getsize(found)
                except Exception as ce:
                    logger.warning("clip failed: %s", ce)
                    try:
                        bot.send_message(chat_id=chat_id, text=f"⚠ ساخت کلیپ ناموفق؛ فایل کامل ارسال می‌شود.\n{ce}")
                    except Exception:
                        pass

            # ساخت GIF
            if found and gif_duration is not None:
                try:
                    gs = float(gif_start or 0)
                    gd = float(gif_duration or 5)
                    bot.edit_message_text(
                        chat_id=chat_id, message_id=progress_msg_id,
                        text=f"🎞 ساخت GIF از {gs}s به مدت {gd}s ...",
                        reply_markup=make_cancel_markup(task_id, owner_id),
                    )
                    gif_path = found + ".gif"
                    make_gif_ffmpeg(found, gif_path, start_sec=gs, duration=gd)
                    if os.path.exists(gif_path) and os.path.getsize(gif_path) > 0:
                        try:
                            os.remove(found)
                        except Exception:
                            pass
                        found = gif_path
                        file_size = os.path.getsize(found)
                        # ارسال به عنوان animation
                        try:
                            with open(found, "rb") as gf:
                                bot.send_animation(
                                    chat_id=chat_id, animation=gf,
                                    caption=f"🎞 GIF ({gs}s–{gs+gd}s)",
                                    timeout=300,
                                )
                            try:
                                bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text="✅ GIF ارسال شد")
                            except Exception:
                                pass
                            append_user_history(owner_id, {"url": url, "title": "gif", "format_id": "gif", "size": file_size, "status": "ok", "mode": "gif"})
                            batch_item_done(batch_id, ok=True, title="gif", bot=bot) if batch_id else None
                            with GLOBAL_STATS_LOCK:
                                GLOBAL_STATS["downloads_ok"] += 1
                            user_limit_finish(owner_id, bytes_count=file_size or 0)
                            on_download_success(owner_id, bot=bot, chat_id=chat_id)
                            try:
                                os.remove(found)
                            except Exception:
                                pass
                            with CANCEL_LOCK:
                                CANCEL_FLAGS.pop(task_id, None)
                            with QUEUE_META_LOCK:
                                QUEUE_META.pop(task_id, None)
                            notify_queue_waiters(bot)
                            continue
                        except Exception as ge:
                            logger.warning("send gif failed, fallback document: %s", ge)
                except Exception as ge2:
                    logger.warning("gif failed: %s", ge2)
                    try:
                        bot.send_message(chat_id=chat_id, text=f"⚠ ساخت GIF ناموفق.\n{ge2}")
                    except Exception:
                        pass

            # برش ویدیو/صدا در صورت درخواست
            if found and (trim_start is not None or trim_end is not None):
                try:
                    bot.edit_message_text(
                        chat_id=chat_id, message_id=progress_msg_id,
                        text=f"✂ در حال برش ({trim_start or 0}s → {trim_end or 'end'}) ...",
                        reply_markup=make_cancel_markup(task_id, owner_id),
                    )
                    trimmed = found + ".trim" + os.path.splitext(found)[1]
                    trim_media_ffmpeg(found, trimmed, start_sec=trim_start, end_sec=trim_end)
                    if os.path.exists(trimmed) and os.path.getsize(trimmed) > 0:
                        try:
                            os.remove(found)
                        except Exception:
                            pass
                        found = trimmed
                        file_size = os.path.getsize(found)
                except Exception as te:
                    logger.warning("trim failed: %s", te)
                    try:
                        bot.send_message(chat_id=chat_id, text=f"⚠ برش ناموفق بود؛ فایل کامل ارسال می‌شود.\n{te}")
                    except Exception:
                        pass


            # پس‌پردازش: force_mp4 / watermark / burn subs
            try:
                if action != "shot" and found and os.path.exists(found):
                    if prefs.get("force_mp4"):
                        out_mp4 = os.path.splitext(found)[0] + ".forced.mp4"
                        try:
                            bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text="🎞 تبدیل به MP4...", reply_markup=make_cancel_markup(task_id, owner_id))
                            force_mp4_ffmpeg(found, out_mp4)
                            if os.path.exists(out_mp4) and os.path.getsize(out_mp4) > 0:
                                try:
                                    os.remove(found)
                                except Exception:
                                    pass
                                found = out_mp4
                                file_size = os.path.getsize(found)
                        except Exception as e:
                            logger.warning("force_mp4 failed: %s", e)
                    wm = (prefs.get("watermark_text") or "").strip()
                    if wm:
                        out_wm = found + ".wm.mp4"
                        try:
                            bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text="💧 واترمارک...", reply_markup=make_cancel_markup(task_id, owner_id))
                            apply_watermark_ffmpeg(found, out_wm, wm)
                            if os.path.exists(out_wm) and os.path.getsize(out_wm) > 0:
                                try:
                                    os.remove(found)
                                except Exception:
                                    pass
                                found = out_wm
                                file_size = os.path.getsize(found)
                        except Exception as e:
                            logger.warning("watermark failed: %s", e)
                    if prefs.get("burn_subtitles") or want_subtitles:
                        # پیدا کردن زیرنویس کنار فایل
                        base_no_ext = os.path.splitext(found)[0]
                        sub_file = None
                        for sub_ext in (".fa.vtt", ".en.vtt", ".vtt", ".srt", ".ass"):
                            for p in Path(DOWNLOAD_ROOT).glob(f"{os.path.basename(base_no_ext)}*{sub_ext}"):
                                sub_file = str(p)
                                break
                            if sub_file:
                                break
                        if not sub_file:
                            for p in Path(DOWNLOAD_ROOT).glob("*.vtt"):
                                if time.time() - p.stat().st_mtime < 3600:
                                    sub_file = str(p)
                                    break
                        if sub_file and prefs.get("burn_subtitles"):
                            out_burn = found + ".burn.mp4"
                            try:
                                bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text="📝 سوزاندن زیرنویس...", reply_markup=make_cancel_markup(task_id, owner_id))
                                burn_subtitles_ffmpeg(found, sub_file, out_burn)
                                if os.path.exists(out_burn) and os.path.getsize(out_burn) > 0:
                                    try:
                                        os.remove(found)
                                    except Exception:
                                        pass
                                    found = out_burn
                                    file_size = os.path.getsize(found)
                            except Exception as e:
                                logger.warning("burn subs failed: %s", e)
            except Exception as pe:
                logger.warning("postprocess error: %s", pe)

            def upload_progress_cb(sent, total):

                try:
                    pct = int(sent * 100 / total) if total else 0
                except:
                    pct = 0
                bar_len = 10
                filled = int(bar_len * pct / 100) if pct else 0
                bar = "🟩" * filled + "⬜" * (bar_len - filled)

                now = time.time()
                st = progress_map.setdefault(task_id, {})
                prev_sent = st.get("up_prev_sent")
                prev_t = st.get("up_prev_t")
                speed = 0
                if prev_sent is not None and prev_t is not None and now - prev_t > 0:
                    speed = (sent - prev_sent) / max(1e-6, (now - prev_t))
                eta_text = "—"
                if speed > 0 and total:
                    remaining = max(0, total - sent)
                    eta_text = format_eta(remaining / speed)

                text = (
                    f"📤 آپلود:\n"
                    f"{bar} {pct}%\n"
                    f"⚡ سرعت: {speed/1024/1024:.2f} MB/s\n"
                    f"📦 ارسال شده: {human_size(sent)} از {human_size(total)}\n"
                    f"⏳ زمان باقی‌مانده: {eta_text}"
                )
                last = st.get("last_edit", 0)
                if now - last > 1.0:
                    try:
                        bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text=text, reply_markup=make_cancel_markup(task_id, owner_id))
                    except:
                        pass
                    st["last_edit"] = now
                st["up_prev_sent"] = sent
                st["up_prev_t"] = now
                progress_map[task_id] = st

            try:
                if file_size and file_size > MAX_SINGLE_UPLOAD_BYTES:
                    # تلاش برای تقسیم و ارسال چندبخشی
                    try:
                        bot.edit_message_text(
                            chat_id=chat_id, message_id=progress_msg_id,
                            text=f"📦 فایل بزرگ است ({human_size(file_size)}). در حال تقسیم و ارسال چندبخشی...",
                            reply_markup=make_cancel_markup(task_id, owner_id),
                        )
                        parts = split_file_for_upload(found, chunk_bytes=CHUNK_SIZE)
                        for i, part in enumerate(parts, 1):
                            with CANCEL_LOCK:
                                flag = CANCEL_FLAGS.get(task_id)
                                if flag and flag.get("cancel"):
                                    raise Exception("Upload cancelled by user")
                            cap = f"بخش {i}/{len(parts)}\n" + (caption if 'caption' in dir() else "")
                            # caption may not exist yet — build simple
                            part_cap = f"📦 بخش {i}/{len(parts)} — {(info.get('title') if info else 'file') or 'file'}"[:1024]
                            upload_with_smart_choice(
                                TOKEN, bot, chat_id, part, caption=part_cap,
                                progress_update_fn=upload_progress_cb, task_id=task_id, as_animation=False,
                            )
                            if part != found:
                                try:
                                    os.remove(part)
                                except Exception:
                                    pass
                        try:
                            bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text=f"✅ ارسال چندبخشی تمام شد ({len(parts)} بخش)")
                        except Exception:
                            pass
                        append_user_log(get_log_key_for_user(owner_id), {"event": "upload_multipart_ok", "parts": len(parts), "task_id": task_id})
                        append_user_history(owner_id, {"url": url, "title": (info.get("title") if info else "") or "", "format_id": format_id, "size": file_size, "status": "ok_multipart", "mode": mode})
                        batch_item_done(batch_id, ok=True, title=(info.get("title") if info else None), bot=bot)
                        with GLOBAL_STATS_LOCK:
                            GLOBAL_STATS["downloads_ok"] += 1
                            GLOBAL_STATS["bytes_uploaded"] += file_size or 0
                        user_limit_finish(owner_id, bytes_count=file_size or 0)
                        with CANCEL_LOCK:
                            CANCEL_FLAGS.pop(task_id, None)
                        with QUEUE_META_LOCK:
                            QUEUE_META.pop(task_id, None)
                        try:
                            if os.path.exists(found):
                                os.remove(found)
                        except Exception:
                            pass
                        continue
                    except Exception as se:
                        bot.edit_message_text(
                            chat_id=chat_id, message_id=progress_msg_id,
                            text=f"❗ فایل بسیار بزرگ است ({human_size(file_size)}) و تقسیم هم ناموفق بود: {se}",
                            reply_markup=make_cancel_markup(task_id, owner_id),
                        )
                        append_user_log(get_log_key_for_user(owner_id), {"event": "upload_too_large", "file_size": file_size, "limit": MAX_SINGLE_UPLOAD_BYTES, "task_id": task_id, "error": str(se)})
                        batch_item_done(batch_id, ok=False, bot=bot)
                        with CANCEL_LOCK:
                            CANCEL_FLAGS.pop(task_id, None)
                        try:
                            if os.path.exists(found):
                                os.remove(found)
                        except Exception:
                            pass
                        user_limit_finish(owner_id)
                        continue


                # فقط لینک مستقیم (بدون دانلود کامل در صورت امکان)
                if mode == "link_only":
                    try:
                        direct = None
                        if info:
                            # از فرمت انتخابی
                            for f in (info.get("formats") or []):
                                if str(f.get("format_id")) == str(format_id) and f.get("url"):
                                    direct = f.get("url")
                                    break
                            if not direct:
                                direct = info.get("url") or info.get("webpage_url")
                        msg = "🔗 لینک مستقیم/صفحه:\n" + (direct or url or "—")
                        if direct and len(direct) > 3500:
                            msg = "🔗 لینک خیلی طولانی است؛ همان webpage:\n" + (info.get("webpage_url") or url or "")
                        bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text="✅ لینک آماده شد")
                        bot.send_message(chat_id=chat_id, text=msg)
                        append_user_log(get_log_key_for_user(owner_id), {"event": "link_only", "url": url, "direct": (direct or "")[:200]})
                        append_user_history(owner_id, {"url": url, "title": (info.get("title") if info else "") or "", "status": "link_only", "mode": "link_only"})
                        with GLOBAL_STATS_LOCK:
                            GLOBAL_STATS["downloads_ok"] += 1
                        user_limit_finish(owner_id)
                        with CANCEL_LOCK:
                            CANCEL_FLAGS.pop(task_id, None)
                        with QUEUE_META_LOCK:
                            QUEUE_META.pop(task_id, None)
                        # فایل محلی اگر دانلود شده بود پاک شود
                        try:
                            if found and os.path.exists(found):
                                os.remove(found)
                        except Exception:
                            pass
                        continue
                    except Exception as le:
                        try:
                            bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text="❗ خطا در حالت فقط لینک: %s" % str(le)[:150])
                        except Exception:
                            pass

                # لینک موقت با انقضا — فایل روی سرور نگه داشته می‌شود و با /fetch TOKEN دوباره ارسال می‌شود
                if mode == "templink":
                    try:
                        title = (info.get("title") if info else "") or os.path.basename(found)
                        token, entry = create_temp_link(found, owner_id, title=title)
                        exp_h = TEMP_LINK_TTL_HOURS
                        text = (
                            f"⏱ لینک موقت ساخته شد\n"
                            f"🔑 توکن: `{token}`\n"
                            f"📦 {human_size(entry.get('size'))}\n"
                            f"⏳ اعتبار: {exp_h} ساعت\n\n"
                            f"برای دریافت دوباره فایل:\n"
                            f"/fetch {token}"
                        )
                        try:
                            bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text="✅ لینک موقت آماده شد", parse_mode=ParseMode.MARKDOWN)
                        except Exception:
                            pass
                        bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.MARKDOWN)
                        # همچنین یک‌بار فایل را به کاربر بفرست
                        try:
                            upload_with_smart_choice(TOKEN, bot, chat_id, found, caption=title[:1024], progress_update_fn=None, task_id=task_id, as_animation=False)
                        except Exception:
                            pass
                        append_user_log(get_log_key_for_user(owner_id), {"event": "templink_created", "token": token, "size": entry.get("size")})
                        append_user_history(owner_id, {"url": url, "title": title, "status": "templink", "mode": "templink", "token": token})
                        with GLOBAL_STATS_LOCK:
                            GLOBAL_STATS["downloads_ok"] += 1
                        user_limit_finish(owner_id, bytes_count=file_size or 0)
                        with CANCEL_LOCK:
                            CANCEL_FLAGS.pop(task_id, None)
                        with QUEUE_META_LOCK:
                            QUEUE_META.pop(task_id, None)
                        try:
                            if found and os.path.exists(found):
                                os.remove(found)
                        except Exception:
                            pass
                        continue
                    except Exception as te:
                        try:
                            bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text=f"❗ خطا در ساخت لینک موقت: {str(te)[:150]}")
                        except Exception:
                            pass

                # If user chose local save, move file to user's downloads folder
                if mode == "local":
                    try:
                        udir = ensure_user_dir(username or owner_id)
                        dest_dir = udir / "downloads"
                        if prefs.get("organized_folders", True):
                            sub = organized_subpath(info, url)
                            dest_dir = dest_dir / sub
                        dest_dir.mkdir(parents=True, exist_ok=True)
                        dest_name = os.path.basename(found)
                        dest_path = dest_dir / dest_name
                        # if file exists, add suffix
                        if dest_path.exists():
                            base, ext = os.path.splitext(dest_name)
                            dest_path = dest_dir / f"{base}_{int(time.time())}{ext}"
                        shutil.move(found, str(dest_path))
                        bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text=f"💾 فایل در پوشهٔ محلی ذخیره شد: {str(dest_path)}")
                        try:
                            bot.send_message(chat_id=chat_id, text=f"✅ ذخیره محلی انجام شد\n📦 {human_size(file_size)}\n📁 {dest_path}")
                        except Exception:
                            pass
                        append_user_log(get_log_key_for_user(owner_id), {"event": "saved_local", "path": str(dest_path), "size": file_size, "task_id": task_id})
                        append_user_history(owner_id, {
                            "url": url,
                            "title": (info.get("title") if info else "") or "",
                            "format_id": format_id,
                            "size": file_size,
                            "status": "local",
                            "mode": "local",
                        })
                        with GLOBAL_STATS_LOCK:
                            GLOBAL_STATS["downloads_ok"] += 1
                        user_limit_finish(owner_id, bytes_count=file_size or 0)
                        on_download_success(owner_id, bot=bot, chat_id=chat_id)
                        with CANCEL_LOCK:
                            CANCEL_FLAGS.pop(task_id, None)
                        with QUEUE_META_LOCK:
                            QUEUE_META.pop(task_id, None)
                        continue
                    except Exception as e:
                        try:
                            bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text=f"❗ خطا در ذخیره محلی: {str(e)}")
                        except:
                            pass
                        append_user_log(get_log_key_for_user(owner_id), {"event": "save_local_error", "error": str(e), "task_id": task_id})
                        # fall through to attempt upload to telegram as fallback

                # If user chose Google Drive, upload there with a live progress report
                if mode == "drive":
                    try:
                        def drive_progress_cb(sent, total):
                            now = time.time()
                            st = progress_map.setdefault(task_id, {})
                            prev_sent = st.get("gd_prev_sent")
                            prev_t = st.get("gd_prev_t")
                            speed = 0
                            if prev_sent is not None and prev_t is not None and now - prev_t > 0:
                                speed = (sent - prev_sent) / max(1e-6, (now - prev_t))
                            pct = int(sent * 100 / total) if total else 0
                            bar_len = 10
                            filled = int(bar_len * pct / 100) if pct else 0
                            bar = "🟨" * filled + "⬜" * (bar_len - filled)
                            eta_text = "—"
                            if speed > 0 and total:
                                remaining = max(0, total - sent)
                                eta_text = format_eta(remaining / speed)
                            text = (
                                f"☁️ آپلود در Google Drive:\n"
                                f"{bar} {pct}%\n"
                                f"⚡ سرعت: {speed/1024/1024:.2f} MB/s\n"
                                f"📦 حجم: {human_size(sent)} از {human_size(total)}\n"
                                f"⏳ زمان باقی‌مانده: {eta_text}"
                            )
                            if now - st.get("last_edit", 0) > 1.0:
                                try:
                                    bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text=text, reply_markup=make_cancel_markup(task_id, owner_id))
                                except Exception:
                                    pass
                                st["last_edit"] = now
                            st["gd_prev_sent"] = sent
                            st["gd_prev_t"] = now
                            progress_map[task_id] = st

                        bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text="☁️ در حال آماده‌سازی آپلود در Google Drive...", reply_markup=make_cancel_markup(task_id, owner_id))
                        if prefs.get("organized_folders", True):
                            drive_subfolder = organized_subpath(info, url)
                        else:
                            drive_subfolder = sanitize_name(username or str(owner_id), max_len=60)
                        result = upload_to_drive_with_progress(
                            found, folder_id=resolve_drive_folder_for_url(url), progress_callback=drive_progress_cb,
                            task_id=task_id, subfolder_name=drive_subfolder
                        )
                        try:
                            os.remove(found)
                        except Exception:
                            pass
                        drive_link = result.get("link") or ""
                        drive_text = (
                            "✅ در Google Drive آپلود شد\n"
                            "📦 " + human_size(result.get("size")) + "\n"
                            "📁 " + str(result.get("name") or "") + "\n"
                            "🔗 " + drive_link
                        )
                        try:
                            bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text=drive_text)
                        except Exception:
                            pass
                        if drive_link:
                            try:
                                kb = InlineKeyboardMarkup([[InlineKeyboardButton("☁️ باز کردن در Drive", url=drive_link)]])
                                bot.send_message(chat_id=chat_id, text="لینک مستقیم فایل در Google Drive:", reply_markup=kb)
                            except Exception:
                                pass
                        append_user_log(get_log_key_for_user(owner_id), {"event": "uploaded_drive", "link": drive_link, "size": file_size, "task_id": task_id})
                        try:
                            batch_item_done(batch_id, ok=True, title=(info.get("title") if info else None), bot=bot)
                        except Exception:
                            pass
                        append_user_history(owner_id, {
                            "url": url,
                            "title": (info.get("title") if info else "") or "",
                            "format_id": format_id,
                            "size": file_size,
                            "status": "drive",
                            "mode": "drive",
                        })
                        with GLOBAL_STATS_LOCK:
                            GLOBAL_STATS["downloads_ok"] += 1
                        user_limit_finish(owner_id, bytes_count=file_size or 0)
                        on_download_success(owner_id, bot=bot, chat_id=chat_id)
                        with CANCEL_LOCK:
                            CANCEL_FLAGS.pop(task_id, None)
                        with QUEUE_META_LOCK:
                            QUEUE_META.pop(task_id, None)
                        continue
                    except Exception as e:
                        try:
                            bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text=f"❗ خطا در آپلود Google Drive: {str(e)}")
                        except:
                            pass
                        append_user_log(get_log_key_for_user(owner_id), {"event": "upload_drive_error", "error": str(e), "task_id": task_id})
                        with GLOBAL_STATS_LOCK:
                            GLOBAL_STATS["downloads_fail"] += 1
                        user_limit_finish(owner_id)
                        with CANCEL_LOCK:
                            CANCEL_FLAGS.pop(task_id, None)
                        with QUEUE_META_LOCK:
                            QUEUE_META.pop(task_id, None)
                        continue

                # فشرده‌سازی: حالت دستی telegram_compress یا هوشمند بر اساس حجم
                do_compress = mode == "telegram_compress"
                if do_compress:
                    mode = "telegram"
                # فشرده‌سازی هوشمند خودکار
                if not do_compress and prefs.get("smart_compress"):
                    try:
                        thr_mb = int(prefs.get("smart_compress_mb") or 80)
                        if file_size and file_size >= thr_mb * 1024 * 1024:
                            do_compress = True
                    except Exception:
                        pass
                if do_compress:
                    try:
                        def _cmp_msg(txt):
                            try:
                                bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text=txt, reply_markup=make_cancel_markup(task_id, owner_id))
                            except Exception:
                                pass
                        force_crf = task.get("compress_crf")
                        _cmp_msg(f"🗜 فشرده‌سازی{f' CRF{force_crf}' if force_crf else ' هوشمند'}...")
                        found = smart_compress_file(found, progress_msg_fn=_cmp_msg, timeout=900, force_crf=force_crf)
                        file_size = os.path.getsize(found) if os.path.exists(found) else file_size
                    except Exception as ce:
                        logger.warning("Compress failed: %s", ce)
                        # ادامه با فایل اصلی

                # کپشن از قالب کاربر
                title = (info.get("title") if info else "") or ""
                caption = format_caption(prefs, info, url)

                # ارسال تامبنیل (اگر موجود)
                if send_thumb and info:
                    thumb_url = info.get("thumbnail")
                    if thumb_url:
                        try:
                            bot.send_photo(chat_id=chat_id, photo=thumb_url, caption=f"🖼 کاور\n{title[:100]}" if title else "🖼 کاور")
                        except Exception:
                            pass

                # ارسال زیرنویس‌های دانلودشده (در صورت وجود) + ترجمه اختیاری
                if want_subtitles or prefs.get("translate_subs"):
                    base_no_ext = os.path.splitext(found)[0]
                    for sub_ext in (".vtt", ".srt", ".ass"):
                        for p in Path(DOWNLOAD_ROOT).glob(f"{os.path.basename(base_no_ext)}*{sub_ext}"):
                            try:
                                upload_with_smart_choice(TOKEN, bot, chat_id, str(p), caption=f"📝 زیرنویس {p.suffix}", progress_update_fn=None, task_id=task_id, as_animation=False)
                            except Exception:
                                pass
                            if prefs.get("translate_subs"):
                                try:
                                    target = prefs.get("translate_to") or "fa"
                                    bot.edit_message_text(
                                        chat_id=chat_id, message_id=progress_msg_id,
                                        text=f"🌐 در حال ترجمه زیرنویس به {target}...",
                                        reply_markup=make_cancel_markup(task_id, owner_id),
                                    )
                                    translated = translate_subtitle_file(str(p), target_lang=target)
                                    upload_with_smart_choice(
                                        TOKEN, bot, chat_id, translated,
                                        caption=f"🌐 زیرنویس ترجمه‌شده ({target})",
                                        progress_update_fn=None, task_id=task_id, as_animation=False,
                                    )
                                    try:
                                        os.remove(translated)
                                    except Exception:
                                        pass
                                except Exception as tre:
                                    try:
                                        bot.send_message(chat_id=chat_id, text=f"⚠ ترجمه زیرنویس ناموفق: {str(tre)[:120]}")
                                    except Exception:
                                        pass

                # GIF → animation | ویدیو کوچک → sendVideo | بقیه → document/telethon
                ext = os.path.splitext(found)[1].lower()
                is_gif = ext == ".gif"
                is_video_ext = ext in (".mp4", ".mov", ".mkv", ".webm", ".avi")
                send_as_video = bool(prefs.get("send_as_video", True)) and is_video_ext and file_size and file_size <= 50 * 1024 * 1024
                bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text=f"⬆️ در حال آپلود فایل ({human_size(file_size)}) ...", reply_markup=make_cancel_markup(task_id, owner_id))

                with QUEUE_META_LOCK:
                    if task_id in QUEUE_META:
                        QUEUE_META[task_id]["status"] = "uploading"

                uploaded_ok = False
                if is_gif and file_size and file_size <= CHUNK_SIZE:
                    upload_with_smart_choice(TOKEN, bot, chat_id, found, caption=caption, progress_update_fn=upload_progress_cb, task_id=task_id, as_animation=True)
                    uploaded_ok = True
                elif send_as_video and file_size and file_size <= 30 * 1024 * 1024:
                    try:
                        with open(found, "rb") as vf:
                            bot.send_video(chat_id=chat_id, video=vf, caption=caption, supports_streaming=True, timeout=600)
                        uploaded_ok = True
                    except Exception as ve:
                        logger.warning("sendVideo failed, fallback document: %s", ve)
                if not uploaded_ok:
                    upload_with_smart_choice(TOKEN, bot, chat_id, found, caption=caption, progress_update_fn=upload_progress_cb, task_id=task_id, as_animation=False)

                # فوروارد به چت دیگر
                if do_forward:
                    fwd = prefs.get("forward_chat_id")
                    if fwd:
                        try:
                            target = int(fwd) if str(fwd).lstrip("-").isdigit() else fwd
                            upload_with_smart_choice(TOKEN, bot, target, found, caption=caption, progress_update_fn=None, task_id=task_id, as_animation=False)
                        except Exception as fe:
                            logger.warning("forward failed: %s", fe)

                try:
                    if DELETE_PROGRESS_MSG:
                        try:
                            bot.delete_message(chat_id=chat_id, message_id=progress_msg_id)
                        except Exception:
                            bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text="✅ آپلود انجام شد")
                    else:
                        bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text="✅ آپلود انجام شد")
                except Exception:
                    pass
                # آینه به Google Drive
                if mode == "mirror" or prefs.get("mirror_drive"):
                    try:
                        bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text="☁️ در حال آپلود آینه در Google Drive...")
                        if prefs.get("organized_folders", True):
                            drive_subfolder = organized_subpath(info, url)
                        else:
                            drive_subfolder = sanitize_name(username or str(owner_id), max_len=60)
                        result = upload_to_drive_with_progress(
                            found, folder_id=resolve_drive_folder_for_url(url), progress_callback=None,
                            task_id=task_id, subfolder_name=drive_subfolder
                        )
                        link = (result or {}).get("link") or ""
                        bot.send_message(chat_id=chat_id, text="☁️ کپی در Drive ذخیره شد\n" + (link[:200] if link else ""))
                        append_user_log(get_log_key_for_user(owner_id), {"event": "mirror_drive_ok", "link": link, "task_id": task_id})
                    except Exception as me:
                        logger.warning("mirror drive failed: %s", me)
                        try:
                            bot.send_message(chat_id=chat_id, text="⚠️ آپلود تلگرام شد ولی Drive ناموفق: %s" % str(me)[:120])
                        except Exception:
                            pass
                # کانال‌های آرشیو (چندتایی)
                for _arch_id in (ARCHIVE_CHAT_IDS or []):
                    try:
                        upload_with_smart_choice(
                            TOKEN, bot, _arch_id, found,
                            caption=("🗄 آرشیو\n" + (title[:100] if title else "")),
                            progress_update_fn=None, task_id=task_id, as_animation=False,
                        )
                    except Exception as ae:
                        logger.warning("archive upload failed (%s): %s", _arch_id, ae)
                try:
                    bot.send_message(
                        chat_id=chat_id,
                        text=f"✅ دانلود و ارسال تمام شد\n🎬 {title[:120] if title else 'فایل'}\n📦 {human_size(file_size)}",
                        reply_markup=make_redownload_markup(url),
                    )
                except Exception:
                    pass
                append_user_log(get_log_key_for_user(owner_id), {"event": "upload_success", "file": found, "size": file_size, "task_id": task_id})
                append_user_history(owner_id, {
                    "url": url,
                    "title": title,
                    "format_id": format_id,
                    "size": file_size,
                    "status": "ok",
                    "mode": mode,
                })
                batch_item_done(batch_id, ok=True, title=title, bot=bot)
                with GLOBAL_STATS_LOCK:
                    GLOBAL_STATS["downloads_ok"] += 1
                    GLOBAL_STATS["bytes_uploaded"] += file_size or 0
                user_limit_finish(owner_id, bytes_count=file_size or 0)
                on_download_success(owner_id, bot=bot, chat_id=chat_id)
                with QUEUE_META_LOCK:
                    QUEUE_META.pop(task_id, None)
                notify_queue_waiters(bot)
            except Exception as e:
                with CANCEL_LOCK:
                    flag = CANCEL_FLAGS.get(task_id)
                    was_cancelled = flag and flag.get("cancel")
                if was_cancelled:
                    try:
                        bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text="❌ عملیات توسط کاربر لغو شد.")
                    except:
                        pass
                    append_user_log(get_log_key_for_user(owner_id), {"event": "cancelled_by_user", "task_id": task_id})
                else:
                    try:
                        bot.edit_message_text(chat_id=chat_id, message_id=progress_msg_id, text=f"❗ خطا در آپلود: {str(e)}")
                    except:
                        pass
                    append_user_log(get_log_key_for_user(owner_id), {"event": "upload_error", "error": str(e), "file": found, "task_id": task_id})
                    with GLOBAL_STATS_LOCK:
                        GLOBAL_STATS["downloads_fail"] += 1
                with CANCEL_LOCK:
                    CANCEL_FLAGS.pop(task_id, None)
                user_limit_finish(owner_id)
                with QUEUE_META_LOCK:
                    QUEUE_META.pop(task_id, None)
                try:
                    if os.path.exists(found):
                        os.remove(found)
                except:
                    pass
                continue

            try:
                if os.path.exists(found):
                    try:
                        os.remove(found)
                    except:
                        pass
            except:
                pass

            log_entry = {"event": "job_finished", "url": url, "request_id": request_id, "user_id": owner_id, "task_id": task_id}
            append_user_log(get_log_key_for_user(owner_id), log_entry)

            with CANCEL_LOCK:
                CANCEL_FLAGS.pop(task_id, None)

        except Exception as e:
            try:
                with open(LOG_ROOT / "worker_exceptions.log", "a", encoding="utf-8") as f:
                    f.write(f"{datetime.now().isoformat()} - Worker exception: {str(e)}\n")
                    f.write(traceback.format_exc())
                    f.write("\n" + ("-" * 60) + "\n")
            except:
                pass
        finally:
            try:
                elapsed = max(0.5, time.time() - task_started_at)
                with GLOBAL_STATS_LOCK:
                    prev_avg = GLOBAL_STATS.get("avg_task_seconds") or elapsed
                    alpha = 0.25
                    GLOBAL_STATS["avg_task_seconds"] = (alpha * elapsed) + ((1 - alpha) * prev_avg)
            except Exception:
                pass
            with active_workers_lock:
                active_workers -= 1

# -------------------------
# Channel / playlist helpers
# -------------------------
def build_channel_url_from_info(info):
    channel_url = info.get("channel_url") or info.get("webpage_url") or info.get("url")
    if channel_url and isinstance(channel_url, str) and channel_url.startswith("http"):
        return channel_url
    uploader_id = info.get("uploader_id") or info.get("uploader")
    if uploader_id:
        if str(uploader_id).startswith("UC"):
            return f"https://www.youtube.com/channel/{uploader_id}"
        handle = str(uploader_id).lstrip("@")
        return f"https://www.youtube.com/@{handle}"
    return None

def fetch_channel_info(channel_query):
    ydl_opts = {"quiet": True, "no_warnings": True}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            if isinstance(channel_query, str) and (channel_query.startswith("http") or "youtube.com" in channel_query):
                info = ydl.extract_info(channel_query, download=False)
                if info and info.get("extractor"):
                    return info
        except Exception:
            pass
        try:
            search_q = f"ytsearch5:channel {channel_query}"
            res = ydl.extract_info(search_q, download=False)
            entries = res.get("entries", []) or []
            for e in entries:
                if e and e.get("extractor") and "channel" in e.get("extractor"):
                    return e
        except Exception as e:
            raise e

def fetch_channel_videos(channel_url_or_id, max_results=200):
    ydl_opts = {"quiet": True, "no_warnings": True, "extract_flat": True}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            info = ydl.extract_info(channel_url_or_id, download=False)
        except Exception:
            try:
                info = ydl.extract_info(channel_url_or_id.rstrip("/") + "/videos", download=False)
            except Exception:
                try:
                    info = ydl.extract_info(f"ytsearch{max_results}:channel {channel_url_or_id}", download=False)
                except Exception as e:
                    raise e
    entries = info.get("entries", []) or []
    videos = []
    for e in entries:
        vid = e.get("id")
        url = e.get("url") or vid
        if url and not url.startswith("http"):
            url = f"https://www.youtube.com/watch?v={url}"
        videos.append({
            "id": vid,
            "title": e.get("title"),
            "url": url,
            "duration": e.get("duration"),
            "thumbnail": e.get("thumbnail")
        })
        if len(videos) >= max_results:
            break
    return videos

def fetch_channel_playlists(channel_url_or_id):
    ydl_opts = {"quiet": True, "no_warnings": True, "extract_flat": True}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(channel_url_or_id, download=False)
    playlists = []
    for k in ("playlists", "entries"):
        for e in info.get(k, []) or []:
            pid = e.get("id")
            url = e.get("url") or pid
            if url and not url.startswith("http"):
                url = f"https://www.youtube.com/playlist?list={url}"
            playlists.append({
                "id": pid,
                "title": e.get("title"),
                "url": url
            })
    return playlists

# -------------------------
# Handlers and multi-link support
# -------------------------
def start(update, context):
    text = (
        "سلام! 👋\n"
        "لینک ویدیو را بفرست تا کیفیت‌ها را نشان دهم.\n\n"
        "دستورات:\n"
        "/search کلمه — جستجو در یوتیوب\n"
        "/channel نام — اطلاعات کانال\n"
        "/status — وضعیت سیستم\n"
        "/dashboard — داشبورد زنده (ادمین)\n"
        "/queue — صف دانلود شما + لغو\n"
        "/history — تاریخچه دانلودها\n"
        "/quick — روشن/خاموش حالت سریع\n"
        "/prefs — تنظیمات شما\n"
        "/set_subs fa,en — زبان زیرنویس\n"
        "/set_quality 720 — کیفیت ترجیحی auto\n"
        "/set_caption — قالب کپشن\n"
        "/set_forward CHAT_ID — فوروارد به چت\n"
        "/autoforward — روشن/خاموش فوروارد خودکار\n"
        "/smartcompress — فشرده‌سازی هوشمند\n"
        "/fetch TOKEN — دریافت فایل از لینک موقت\n"
        "/clip START END URL — کلیپ کوتاه\n"
        "/gif START DUR URL — ساخت GIF\n"
        "/story URL — استوری/هایلایت اینستاگرام\n"
        "/points — امتیاز و سطح\n"
        "/translatesubs — ترجمه زیرنویس\n"
        "/summarize لینک — خلاصه و کپشن هوشمند\n"
        "/logo — راهنمای اضافه کردن لوگو\n"
        "/merge — راهنمای ادغام ویدیو\n"
        "/trim START END URL — برش ویدیو\n"
        "/cookies — راهنمای کوکی\n"
        "/fromdrive لینک‌یا‌FILE_ID — دریافت فایل از Google Drive و ارسال به تلگرام\n"
        "/admin — پنل ادمین\n"
        "/stats — آمار (ادمین)\n"
        "/help — راهنما"
    )
    update.message.reply_text(text)

def help_cmd(update, context):
    start(update, context)


def queue_cmd(update, context):
    user_id = update.message.from_user.id
    with QUEUE_META_LOCK:
        mine = [(k, v) for k, v in QUEUE_META.items() if v.get("user_id") == user_id]
    if not mine:
        update.message.reply_text("صف شما خالی است.")
        return
    lines = ["📋 صف شما (%d جاب):" % len(mine)]
    for tid, m in mine[:15]:
        lines.append("• %s — %s" % (m.get("status", "?"), (m.get("url") or "")[:50]))
    update.message.reply_text("\n".join(lines), reply_markup=make_queue_keyboard(mine))


def history_cmd(update, context):
    user_id = update.message.from_user.id
    hist = load_user_history(user_id)
    if not hist:
        update.message.reply_text("تاریخچه‌ای ندارید.")
        return
    lines = ["📜 آخرین دانلودها:"]
    for h in reversed(hist[-10:]):
        lines.append("• %s [%s]" % ((h.get("title") or h.get("url") or "")[:50], h.get("status")))
    update.message.reply_text("\n".join(lines), reply_markup=make_history_keyboard(hist))


def quick_cmd(update, context):
    user_id = update.message.from_user.id
    prefs = load_user_prefs(user_id)
    prefs["quick_mode"] = not bool(prefs.get("quick_mode"))
    save_user_prefs(user_id, prefs)
    state = "روشن ⚡" if prefs["quick_mode"] else "خاموش"
    update.message.reply_text("حالت سریع: %s\nدر حالت روشن، بعد از انتخاب کیفیت مستقیم به صف می‌رود." % state)


def set_quality_cmd(update, context):
    user_id = update.message.from_user.id
    args = context.args or []
    if not args or not str(args[0]).isdigit():
        update.message.reply_text("مثال: /set_quality 720")
        return
    prefs = load_user_prefs(user_id)
    prefs["preferred_height"] = int(args[0])
    save_user_prefs(user_id, prefs)
    update.message.reply_text("کیفیت ترجیحی auto روی %sp تنظیم شد." % args[0])


def set_caption_cmd(update, context):
    user_id = update.message.from_user.id
    args = " ".join(context.args).strip() if context.args else ""
    if not args:
        update.message.reply_text("مثال:\n/set_caption {title}\n{uploader}\n{url}")
        return
    prefs = load_user_prefs(user_id)
    prefs["caption_template"] = args
    save_user_prefs(user_id, prefs)
    update.message.reply_text("✅ قالب کپشن ذخیره شد.")


def set_forward_cmd(update, context):
    user_id = update.message.from_user.id
    args = context.args or []
    if not args:
        update.message.reply_text("مثال: /set_forward -1001234567890\nحذف: /set_forward off")
        return
    prefs = load_user_prefs(user_id)
    if str(args[0]).lower() in ("off", "0", "none"):
        prefs["forward_chat_id"] = None
        prefs["auto_forward"] = False
        save_user_prefs(user_id, prefs)
        update.message.reply_text("فوروارد غیرفعال شد.")
        return
    prefs["forward_chat_id"] = args[0]
    save_user_prefs(user_id, prefs)
    update.message.reply_text("فوروارد به %s تنظیم شد.\nبرای فوروارد خودکار هر دانلود: /autoforward" % args[0])


def autoforward_cmd(update, context):
    """روشن/خاموش کردن فوروارد خودکار بعد از هر دانلود موفق"""
    user_id = update.message.from_user.id
    prefs = load_user_prefs(user_id)
    if not prefs.get("forward_chat_id"):
        update.message.reply_text("اول مقصد را با /set_forward CHAT_ID تنظیم کن.")
        return
    prefs["auto_forward"] = not bool(prefs.get("auto_forward"))
    save_user_prefs(user_id, prefs)
    state = "روشن ✅" if prefs["auto_forward"] else "خاموش"
    update.message.reply_text(
        "فوروارد خودکار: %s\nمقصد: %s" % (state, prefs.get("forward_chat_id"))
    )


def smartcompress_cmd(update, context):
    """روشن/خاموش فشرده‌سازی هوشمند + تنظیم آستانه"""
    user_id = update.message.from_user.id
    args = context.args or []
    prefs = load_user_prefs(user_id)
    if args and str(args[0]).isdigit():
        prefs["smart_compress_mb"] = int(args[0])
        prefs["smart_compress"] = True
        save_user_prefs(user_id, prefs)
        update.message.reply_text(
            f"✅ فشرده‌سازی هوشمند روشن شد\nآستانه: {prefs['smart_compress_mb']} MB"
        )
        return
    prefs["smart_compress"] = not bool(prefs.get("smart_compress"))
    save_user_prefs(user_id, prefs)
    state = "روشن ✅" if prefs["smart_compress"] else "خاموش"
    update.message.reply_text(
        f"فشرده‌سازی هوشمند: {state}\n"
        f"آستانه فعلی: {prefs.get('smart_compress_mb', 80)} MB\n"
        f"تغییر آستانه: /smartcompress 100"
    )


def fetch_cmd(update, context):
    """دریافت دوباره فایل از لینک موقت: /fetch TOKEN"""
    user_id = update.message.from_user.id
    ok, msg = user_allowed(user_id)
    if not ok:
        update.message.reply_text(msg)
        return
    args = context.args or []
    if not args:
        update.message.reply_text("مثال:\n/fetch a1b2c3d4e5f6")
        return
    token = str(args[0]).strip()
    entry, err = get_temp_link(token)
    if err:
        update.message.reply_text(f"❌ {err}")
        return
    # فقط صاحب توکن (یا ادمین) بتواند بگیرد
    if entry.get("user_id") != user_id and not is_admin(user_id):
        update.message.reply_text("⛔ این توکن مال شما نیست.")
        return
    path = entry["path"]
    title = entry.get("title") or "file"
    progress = update.message.reply_text(f"📤 در حال ارسال فایل موقت...\n📦 {human_size(entry.get('size'))}")
    try:
        upload_with_smart_choice(
            TOKEN, context.bot, update.message.chat_id, path,
            caption=title[:1024], progress_update_fn=None, task_id=None, as_animation=False,
        )
        remain_h = max(0, (entry.get("expires", 0) - time.time()) / 3600)
        try:
            progress.edit_text(f"✅ ارسال شد\n⏳ باقی‌مانده اعتبار: {remain_h:.1f} ساعت")
        except Exception:
            pass
    except Exception as e:
        try:
            progress.edit_text(f"❗ خطا در ارسال: {str(e)[:150]}")
        except Exception:
            pass



def trim_cmd(update, context):
    """
    /trim START END URL
    مثال: /trim 30 90 https://youtu.be/xxxx
    برش از ثانیه ۳۰ تا ۹۰ و ارسال به تلگرام
    """
    user = update.message.from_user
    user_id = user.id
    if is_user_banned(user_id):
        update.message.reply_text("⛔ دسترسی شما مسدود شده است.")
        return
    args = context.args or []
    if len(args) < 3:
        update.message.reply_text(
            "برش ویدیو:\n"
            "/trim START END URL\n"
            "مثال:\n"
            "/trim 10 60 https://youtu.be/xxxx\n"
            "(از ثانیه ۱۰ تا ۶۰)"
        )
        return
    try:
        start_sec = float(args[0])
        end_sec = float(args[1])
    except Exception:
        update.message.reply_text("START و END باید عدد (ثانیه) باشند.")
        return
    url = args[2]
    if not url.startswith("http"):
        update.message.reply_text("URL نامعتبر است.")
        return
    if end_sec <= start_sec:
        update.message.reply_text("END باید بزرگ‌تر از START باشد.")
        return
    enqueue_task({
        "group_id": None,
        "user_id": user_id,
        "username": user.username or str(user_id),
        "url": url,
        "chat_id": update.message.chat_id,
        "message_id": update.message.message_id,
        "action": "video",
        "format_id": "best",
        "mode": "telegram",
        "request_id": None,
        "request_info": None,
        "trim_start": start_sec,
        "trim_end": end_sec,
    })
    update.message.reply_text(f"✂ به صف اضافه شد — برش {start_sec}s تا {end_sec}s")




def burn_cmd(update, context):
    prefs = load_user_prefs(update.message.from_user.id)
    prefs["burn_subtitles"] = not bool(prefs.get("burn_subtitles"))
    save_user_prefs(update.message.from_user.id, prefs)
    update.message.reply_text("سوزاندن زیرنویس: %s" % ("ON" if prefs["burn_subtitles"] else "OFF"))


def forcemp4_cmd(update, context):
    prefs = load_user_prefs(update.message.from_user.id)
    prefs["force_mp4"] = not bool(prefs.get("force_mp4"))
    save_user_prefs(update.message.from_user.id, prefs)
    update.message.reply_text("تبدیل اجباری MP4: %s" % ("ON" if prefs["force_mp4"] else "OFF"))


def watermark_cmd(update, context):
    args = " ".join(context.args).strip() if context.args else ""
    prefs = load_user_prefs(update.message.from_user.id)
    if not args:
        update.message.reply_text("مثال: /watermark MyChannel\nحذف: /watermark off\nفعلی: %s" % (prefs.get("watermark_text") or "—"))
        return
    if args.lower() in ("off", "0", "none"):
        prefs["watermark_text"] = ""
    else:
        prefs["watermark_text"] = args[:40]
    save_user_prefs(update.message.from_user.id, prefs)
    update.message.reply_text("واترمارک: %s" % (prefs.get("watermark_text") or "خاموش"))


def shot_cmd(update, context):
    """ /shot SECOND URL """
    user = update.message.from_user
    ok, msg = user_allowed(user.id)
    if not ok:
        update.message.reply_text(msg)
        return
    args = context.args or []
    if len(args) < 2:
        update.message.reply_text("مثال:\n/shot 15 https://youtu.be/xxxx")
        return
    try:
        at = float(args[0])
    except Exception:
        update.message.reply_text("ثانیه نامعتبر")
        return
    url = args[1]
    if not url.startswith("http"):
        update.message.reply_text("URL نامعتبر")
        return
    enqueue_task({
        "user_id": user.id,
        "username": user.username or str(user.id),
        "url": url,
        "chat_id": update.message.chat_id,
        "message_id": update.message.message_id,
        "action": "shot",
        "format_id": "best",
        "mode": "telegram",
        "shot_at": at,
        "request_id": None,
        "request_info": None,
    })
    update.message.reply_text(f"🖼 اسکرین‌شات در ثانیه {at} به صف اضافه شد.")


def clip_cmd(update, context):
    """ /clip START END URL — ساخت کلیپ کوتاه """
    user = update.message.from_user
    ok, msg = user_allowed(user.id)
    if not ok:
        update.message.reply_text(msg)
        return
    args = context.args or []
    if len(args) < 3:
        update.message.reply_text(
            "ساخت کلیپ:\n/clip START END URL\nمثال:\n/clip 10 25 https://youtu.be/xxxx"
        )
        return
    try:
        start_sec = float(args[0])
        end_sec = float(args[1])
    except Exception:
        update.message.reply_text("START و END باید عدد (ثانیه) باشند.")
        return
    url = args[2]
    if not url.startswith("http"):
        update.message.reply_text("URL نامعتبر")
        return
    if end_sec <= start_sec:
        update.message.reply_text("END باید بزرگ‌تر از START باشد.")
        return
    if end_sec - start_sec > 300:
        update.message.reply_text("حداکثر طول کلیپ ۳۰۰ ثانیه است.")
        return
    enqueue_task({
        "user_id": user.id,
        "username": user.username or str(user.id),
        "url": url,
        "chat_id": update.message.chat_id,
        "message_id": update.message.message_id,
        "action": "clip",
        "format_id": "best",
        "mode": "telegram",
        "clip_start": start_sec,
        "clip_end": end_sec,
        "request_id": None,
        "request_info": None,
    })
    update.message.reply_text(f"🎬 کلیپ {start_sec}s–{end_sec}s به صف اضافه شد.")


def gif_cmd(update, context):
    """ /gif START DURATION URL — ساخت GIF """
    user = update.message.from_user
    ok, msg = user_allowed(user.id)
    if not ok:
        update.message.reply_text(msg)
        return
    args = context.args or []
    if len(args) < 3:
        update.message.reply_text(
            "ساخت GIF:\n/gif START DURATION URL\nمثال:\n/gif 5 4 https://youtu.be/xxxx\n(از ثانیه ۵ به مدت ۴ ثانیه)"
        )
        return
    try:
        start_sec = float(args[0])
        duration = float(args[1])
    except Exception:
        update.message.reply_text("START و DURATION باید عدد باشند.")
        return
    url = args[2]
    if not url.startswith("http"):
        update.message.reply_text("URL نامعتبر")
        return
    if duration <= 0 or duration > 15:
        update.message.reply_text("مدت GIF باید بین ۰.۵ تا ۱۵ ثانیه باشد.")
        return
    enqueue_task({
        "user_id": user.id,
        "username": user.username or str(user.id),
        "url": url,
        "chat_id": update.message.chat_id,
        "message_id": update.message.message_id,
        "action": "gif",
        "format_id": "best[height<=480]/best",
        "mode": "telegram",
        "gif_start": start_sec,
        "gif_duration": duration,
        "request_id": None,
        "request_info": None,
    })
    update.message.reply_text(f"🎞 GIF از {start_sec}s به مدت {duration}s به صف اضافه شد.")


def story_cmd(update, context):
    """دانلود استوری/هایلایت اینستاگرام — نیاز به کوکی"""
    user = update.message.from_user
    ok, msg = user_allowed(user.id)
    if not ok:
        update.message.reply_text(msg)
        return
    args = context.args or []
    if not args:
        update.message.reply_text(
            "استوری / هایلایت اینستاگرام:\n"
            "/story URL\n\n"
            "مثال:\n"
            "/story https://www.instagram.com/stories/username/...\n"
            "/story https://www.instagram.com/stories/highlights/...\n\n"
            "⚠️ برای استوری خصوصی باید کوکی ارسال کرده باشی (/cookies)"
        )
        return
    url = args[0]
    if not url.startswith("http"):
        update.message.reply_text("URL نامعتبر")
        return
    if not is_instagram_story_url(url) and "instagram.com" not in url.lower():
        update.message.reply_text("این لینک شبیه استوری/هایلایت اینستاگرام نیست. لینک کامل را بفرست.")
        return
    cf = get_cookiefile(user.id)
    if not cf:
        update.message.reply_text(
            "🍪 برای استوری معمولاً کوکی لازم است.\n"
            "فایل cookies.txt را بفرست یا از /cookies راهنما را ببین.\n"
            "با این حال تلاش می‌شود..."
        )
    enqueue_task({
        "user_id": user.id,
        "username": user.username or str(user.id),
        "url": url,
        "chat_id": update.message.chat_id,
        "message_id": update.message.message_id,
        "action": "video",
        "format_id": "best",
        "mode": "telegram",
        "request_id": None,
        "request_info": None,
        "is_story": True,
    })
    update.message.reply_text("📲 استوری/هایلایت به صف اضافه شد.")


def points_cmd(update, context):
    """نمایش امتیاز و سطح کاربر"""
    user_id = update.message.from_user.id
    prefs = load_user_prefs(user_id)
    pts = int(prefs.get("points") or 0)
    level = int(prefs.get("level") or calc_level(pts))
    max_dl, max_bytes, _, mult = get_user_limits_scaled(user_id)
    # امتیاز تا سطح بعدی
    next_thr = None
    for thr in LEVEL_THRESHOLDS:
        if pts < thr:
            next_thr = thr
            break
    if next_thr is None and level < 6:
        next_thr = LEVEL_THRESHOLDS[-1] if pts < LEVEL_THRESHOLDS[-1] else None
    lines = [
        f"⭐ امتیاز تو: {pts}",
        f"🏅 سطح: {level}",
        f"📥 سقف روزانه: {max_dl} دانلود | {human_size(max_bytes)}",
        f"📊 ضریب سطح: ×{mult}",
    ]
    if next_thr:
        lines.append(f"🎯 تا سطح بعد: {next_thr - pts} امتیاز")
    else:
        lines.append("🎯 به بالاترین سطح رسیدی!")
    lines.append(f"\nهر دانلود موفق = +{POINTS_PER_SUCCESS} امتیاز")
    update.message.reply_text("\n".join(lines))


def translatesubs_cmd(update, context):
    """روشن/خاموش ترجمه خودکار زیرنویس + زبان مقصد"""
    user_id = update.message.from_user.id
    args = context.args or []
    prefs = load_user_prefs(user_id)
    if args:
        lang = str(args[0]).lower().strip()
        if lang in ("off", "0", "false"):
            prefs["translate_subs"] = False
            save_user_prefs(user_id, prefs)
            update.message.reply_text("ترجمه زیرنویس خاموش شد.")
            return
        prefs["translate_to"] = lang[:8]
        prefs["translate_subs"] = True
        save_user_prefs(user_id, prefs)
        update.message.reply_text(f"✅ ترجمه زیرنویس روشن شد → {lang}\nنیاز: pip install deep-translator")
        return
    prefs["translate_subs"] = not bool(prefs.get("translate_subs"))
    save_user_prefs(user_id, prefs)
    state = "روشن ✅" if prefs["translate_subs"] else "خاموش"
    update.message.reply_text(
        f"ترجمه زیرنویس: {state}\n"
        f"زبان مقصد: {prefs.get('translate_to', 'fa')}\n"
        f"تغییر زبان: /translatesubs en\n"
        f"خاموش: /translatesubs off"
    )


def summarize_cmd(update, context):
    """خلاصه و کپشن هوشمند رایگان"""
    user = update.message.from_user
    ok, msg = user_allowed(user.id)
    if not ok:
        update.message.reply_text(msg)
        return
    args = context.args or []
    if not args:
        update.message.reply_text("مثال:\n/summarize https://youtu.be/xxxx")
        return
    url = args[0]
    if not url.startswith("http"):
        update.message.reply_text("لینک نامعتبر")
        return
    progress = update.message.reply_text("🔍 در حال ساخت خلاصه و کپشن...")
    try:
        info = extract_info_safe(url, user_id=user.id)
        summary = generate_video_summary(info)
        caption = generate_smart_caption(info, url=url)
        text = summary + "\n\n────────\n✨ کپشن پیشنهادی:\n" + caption
        if len(text) > 4000:
            text = text[:3990] + "..."
        progress.edit_text(text)
    except Exception as e:
        try:
            progress.edit_text(f"❗ {str(e)[:300]}")
        except Exception:
            update.message.reply_text(f"❗ {str(e)[:300]}")


def logo_cmd(update, context):
    """اضافه کردن لوگو به آخرین ویدیوی ذخیره‌شده کاربر"""
    user = update.message.from_user
    ok, msg = user_allowed(user.id)
    if not ok:
        update.message.reply_text(msg)
        return
    with USER_MEDIA_LOCK:
        st = USER_MEDIA_STATE.get(user.id) or {}
        videos = list(st.get("videos") or [])
        logo = st.get("logo")
    if not videos:
        update.message.reply_text("اول ویدیو بفرست، بعد تصویر با کپشن logo، سپس /logo")
        return
    if not logo or not os.path.exists(logo):
        update.message.reply_text("لوگو نیست. تصویر را با کپشن logo بفرست.")
        return
    src = videos[-1]
    if not os.path.exists(src):
        update.message.reply_text("ویدیو پیدا نشد.")
        return
    out = src + "_logo.mp4"
    progress = update.message.reply_text("🖼 در حال اضافه کردن لوگو...")
    try:
        add_logo_ffmpeg(src, logo, out)
        with open(out, "rb") as f:
            context.bot.send_document(chat_id=update.message.chat_id, document=f, caption="✅ لوگو اضافه شد")
        try:
            progress.delete()
        except Exception:
            pass
    except Exception as e:
        try:
            progress.edit_text(f"❗ {str(e)[:300]}")
        except Exception:
            update.message.reply_text(f"❗ {str(e)[:300]}")


def merge_cmd(update, context):
    """ادغام ویدیوهای ذخیره‌شده"""
    user = update.message.from_user
    ok, msg = user_allowed(user.id)
    if not ok:
        update.message.reply_text(msg)
        return
    with USER_MEDIA_LOCK:
        st = USER_MEDIA_STATE.get(user.id) or {}
        videos = [v for v in (st.get("videos") or []) if os.path.exists(v)]
    if len(videos) < 2:
        update.message.reply_text(f"حداقل ۲ ویدیو لازم است (الان {len(videos)}).")
        return
    out = os.path.join(os.path.dirname(videos[0]), f"merged_{int(time.time())}.mp4")
    progress = update.message.reply_text(f"🎬 ادغام {len(videos)} ویدیو...")
    try:
        merge_videos_ffmpeg(videos, out)
        with open(out, "rb") as f:
            context.bot.send_document(chat_id=update.message.chat_id, document=f, caption=f"✅ ادغام {len(videos)} ویدیو")
        try:
            progress.delete()
        except Exception:
            pass
        with USER_MEDIA_LOCK:
            st = USER_MEDIA_STATE.get(user.id) or {}
            st["videos"] = []
            USER_MEDIA_STATE[user.id] = st
    except Exception as e:
        try:
            progress.edit_text(f"❗ {str(e)[:300]}")
        except Exception:
            update.message.reply_text(f"❗ {str(e)[:300]}")


def ask_cmd(update, context):
    """ /ask لینک سؤال """
    user = update.message.from_user
    ok, msg = user_allowed(user.id)
    if not ok:
        update.message.reply_text(msg)
        return
    args = context.args or []
    if len(args) < 2:
        update.message.reply_text("مثال:\n/ask https://youtu.be/xxxx موضوع ویدیو چیست؟")
        return
    url, question = args[0], " ".join(args[1:])
    if not url.startswith("http"):
        update.message.reply_text("لینک نامعتبر")
        return
    if not DEEPSEEK_API_KEY:
        update.message.reply_text("DEEPSEEK_API_KEY تنظیم نشده.")
        return
    progress = update.message.reply_text("🤖 در حال پاسخ...")
    try:
        info = extract_info_safe(url, user_id=user.id)
        ans = ai_answer_about_video(info, question, url=url)
        with _DEEPSEEK_BILLING_LOCK:
            bill = _DEEPSEEK_BILLING_MSG
        if bill:
            ans = bill + "\n\n" + ans
        progress.edit_text(ans[:4000])
    except DeepSeekBillingError as be:
        progress.edit_text(str(be))
    except Exception as e:
        try:
            progress.edit_text(f"❗ {str(e)[:300]}")
        except Exception:
            pass


def schedule_cmd(update, context):
    """ /schedule HH:MM URL """
    user = update.message.from_user
    ok, msg = user_allowed(user.id)
    if not ok:
        update.message.reply_text(msg)
        return
    args = context.args or []
    if len(args) < 2:
        update.message.reply_text("مثال:\n/schedule 18:30 https://youtu.be/xxxx")
        return
    try:
        hh, mm = args[0].split(":")
        hh, mm = int(hh), int(mm)
        now = datetime.now()
        run_at_dt = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if run_at_dt <= now:
            run_at_dt = run_at_dt + timedelta(days=1)
        run_at = run_at_dt.timestamp()
    except Exception:
        update.message.reply_text("ساعت نامعتبر. فرمت: HH:MM")
        return
    url = args[1]
    if not url.startswith("http"):
        update.message.reply_text("URL نامعتبر")
        return
    task = {
        "user_id": user.id,
        "username": user.username or str(user.id),
        "url": url,
        "chat_id": update.message.chat_id,
        "message_id": update.message.message_id,
        "action": "video",
        "format_id": "best",
        "mode": "telegram",
        "request_id": None,
        "request_info": None,
    }
    with SCHEDULED_LOCK:
        SCHEDULED_JOBS.append({"run_at": run_at, "task": task})
    update.message.reply_text(f"⏰ زمان‌بندی شد برای {args[0]}\n{url[:80]}")


def watch_cmd(update, context):
    """ /watch CHANNEL_URL  |  /watch off """
    user = update.message.from_user
    ok, msg = user_allowed(user.id)
    if not ok:
        update.message.reply_text(msg)
        return
    args = context.args or []
    if not args:
        update.message.reply_text("مثال:\n/watch https://www.youtube.com/@channel\n/watch off")
        return
    if args[0].lower() in ("off", "stop", "0"):
        with CHANNEL_WATCH_LOCK:
            CHANNEL_WATCHES[:] = [w for w in CHANNEL_WATCHES if w.get("user_id") != user.id]
        update.message.reply_text("نظارت کانال متوقف شد.")
        return
    url = args[0]
    with CHANNEL_WATCH_LOCK:
        CHANNEL_WATCHES.append({
            "user_id": user.id,
            "chat_id": update.message.chat_id,
            "channel_url": url,
            "last_ids": [],
            "quality": "best",
        })
    update.message.reply_text(f"🔔 نظارت کانال فعال شد:\n{url[:100]}\nهر ۵ دقیقه بررسی می‌شود.")


def waitqueue_cmd(update, context):
    user = update.message.from_user
    ok, msg = user_allowed(user.id)
    if not ok:
        update.message.reply_text(msg)
        return
    with QUEUE_WAITERS_LOCK:
        QUEUE_WAITERS[user.id] = update.message.chat_id
    prefs = load_user_prefs(user.id)
    prefs["notify_queue_empty"] = True
    save_user_prefs(user.id, prefs)
    update.message.reply_text("✅ وقتی صف خالی شد خبرت می‌کنم.")



def make_admin_panel_keyboard():
    rows = [
        [InlineKeyboardButton("📊 آمار", callback_data="adminpanel:stats"),
         InlineKeyboardButton("📋 صف", callback_data="adminpanel:queue")],
        [InlineKeyboardButton("🛠 تعمیرات ON", callback_data="adminpanel:maint_on"),
         InlineKeyboardButton("🛠 تعمیرات OFF", callback_data="adminpanel:maint_off")],
        [InlineKeyboardButton("🔄 آپدیت yt-dlp", callback_data="adminpanel:update_ytdlp"),
         InlineKeyboardButton("💾 بکاپ", callback_data="adminpanel:backup")],
        [InlineKeyboardButton("🔐 Whitelist ON", callback_data="adminpanel:wl_on"),
         InlineKeyboardButton("🔓 Whitelist OFF", callback_data="adminpanel:wl_off")],
    ]
    return InlineKeyboardMarkup(rows)


def zip_files(file_paths, dest_zip):
    """چند فایل را zip می‌کند"""
    with zipfile.ZipFile(dest_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in file_paths:
            if p and os.path.exists(p):
                zf.write(p, arcname=os.path.basename(p))
    return dest_zip



def mystats_cmd(update, context):
    user_id = update.message.from_user.id
    ok, msg = user_allowed(user_id)
    if not ok:
        update.message.reply_text(msg)
        return
    hist = load_user_history(user_id)
    ok_n = sum(1 for h in hist if h.get("status") in ("ok", "drive", "local"))
    bytes_sum = 0
    for h in hist:
        try:
            bytes_sum += int(h.get("size") or 0)
        except Exception:
            pass
    with USER_LIMITS_LOCK:
        lim = dict(USER_LIMITS.get(user_id) or {})
    with QUEUE_META_LOCK:
        qmine = sum(1 for v in QUEUE_META.values() if v.get("user_id") == user_id)
    max_dl, max_bytes, level, mult = get_user_limits_scaled(user_id)
    prefs = load_user_prefs(user_id)
    pts = int(prefs.get("points") or 0)
    update.message.reply_text(
        "📈 آمار شما\n"
        "⭐ امتیاز: %d | 🏅 سطح: %d (×%.2f)\n"
        "تاریخچه: %d | موفق: %d\n"
        "حجم تاریخچه: %s\n"
        "امروز: %s / %s دانلود\n"
        "حجم امروز: %s / %s\n"
        "جاب در صف: %d"
        % (
            pts, level, mult,
            len(hist), ok_n, human_size(bytes_sum),
            lim.get("count", 0), max_dl,
            human_size(lim.get("bytes", 0)), human_size(max_bytes),
            qmine,
        )
    )


def drivestatus_cmd(update, context):
    user_id = update.message.from_user.id
    if not is_admin(user_id):
        update.message.reply_text("فقط ادمین.")
        return
    try:
        service = ensure_drive_service()
        free = get_drive_free_space_bytes(service)
        about = service.about().get(fields="user,storageQuota").execute()
        quota = about.get("storageQuota") or {}
        user = about.get("user") or {}
        lines = ["☁️ وضعیت Google Drive"]
        if user.get("emailAddress"):
            lines.append("اکانت: %s" % user.get("emailAddress"))
        if quota.get("limit") is not None:
            lines.append("سقف: %s" % human_size(int(quota.get("limit"))))
        if quota.get("usage") is not None:
            lines.append("مصرف: %s" % human_size(int(quota.get("usage"))))
        if free is not None:
            lines.append("آزاد: %s" % human_size(free))
        else:
            lines.append("آزاد: نامحدود/نامشخص")
        lines.append("Folder: %s" % (GOOGLE_DRIVE_FOLDER_ID or "—"))
        update.message.reply_text("\n".join(lines))
    except Exception as e:
        update.message.reply_text("خطا در وضعیت Drive:\n%s" % str(e)[:300])



def set_rate_cmd(update, context):
    """محدودیت سرعت دانلود برای خود کاربر: /set_rate 500k یا /set_rate 0"""
    user_id = update.message.from_user.id
    ok, msg = user_allowed(user_id)
    if not ok:
        update.message.reply_text(msg)
        return
    args = context.args or []
    if not args:
        prefs = load_user_prefs(user_id)
        cur = int(prefs.get("rate_limit_bps") or 0)
        update.message.reply_text(
            "محدودیت سرعت فعلی: %s\nمثال:\n/set_rate 0 — بدون محدودیت\n/set_rate 500k — حدود 500KB/s\n/set_rate 2m — حدود 2MB/s"
            % (human_size(cur) + "/s" if cur else "آزاد")
        )
        return
    raw = str(args[0]).strip().lower()
    mult = 1
    if raw.endswith("k"):
        mult = 1024
        raw = raw[:-1]
    elif raw.endswith("m"):
        mult = 1024 * 1024
        raw = raw[:-1]
    try:
        val = int(float(raw) * mult)
    except Exception:
        update.message.reply_text("مقدار نامعتبر")
        return
    if val < 0:
        val = 0
    prefs = load_user_prefs(user_id)
    prefs["rate_limit_bps"] = val
    save_user_prefs(user_id, prefs)
    update.message.reply_text("✅ محدودیت سرعت: %s" % ("آزاد" if not val else human_size(val) + "/s"))


def errors_cmd(update, context):
    """آخرین خطاها — فقط ادمین"""
    if not is_admin(update.message.from_user.id):
        update.message.reply_text("فقط ادمین.")
        return
    with RECENT_ERRORS_LOCK:
        items = list(RECENT_ERRORS)[-15:]
    if not items:
        update.message.reply_text("خطای ثبت‌شده‌ای نیست.")
        return
    lines = ["🧾 آخرین خطاها:"]
    for it in reversed(items):
        ts = datetime.fromtimestamp(it.get("ts") or 0).strftime("%H:%M:%S")
        lines.append("%s | u=%s | %s | %s" % (
            ts, it.get("user_id"), (it.get("url") or "")[:30], friendly_yt_error(it.get("error"))[:80]
        ))
    update.message.reply_text("\n".join(lines)[:3500])


def start_daily_stats_thread(bot):
    if DAILY_STATS_HOUR < 0:
        return
    def runner():
        last_day = None
        while True:
            try:
                now = datetime.now()
                if now.hour == DAILY_STATS_HOUR and last_day != now.date():
                    last_day = now.date()
                    with GLOBAL_STATS_LOCK:
                        s = dict(GLOBAL_STATS)
                    uptime = int(time.time() - (s.get("started_at") or time.time()))
                    text = (
                        "📅 گزارش روزانه\n"
                        "✅ موفق: %s | ❌ ناموفق: %s\n"
                        "📤 حجم: %s\n"
                        "⏱ آپتایم: %s ثانیه\n"
                        "صف الان: %d"
                        % (
                            s.get("downloads_ok", 0), s.get("downloads_fail", 0),
                            human_size(s.get("bytes_uploaded", 0)),
                            uptime, download_queue.qsize(),
                        )
                    )
                    for aid in list(ADMIN_IDS):
                        try:
                            bot.send_message(chat_id=aid, text=text)
                        except Exception:
                            pass
            except Exception:
                pass
            time.sleep(60)
    threading.Thread(target=runner, name="daily-stats", daemon=True).start()


def admin_cmd(update, context):
    global MAINTENANCE_MODE, WHITELIST_ENABLED
    user_id = update.message.from_user.id
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        update.message.reply_text("فقط ادمین.")
        return
    args = context.args or []
    if not args:
        with active_workers_lock:
            aw = active_workers
        qsize = download_queue.qsize()
        with GLOBAL_STATS_LOCK:
            s = dict(GLOBAL_STATS)
        with BANNED_LOCK:
            nb = len(BANNED_USERS)
        with MAINTENANCE_LOCK:
            maint = MAINTENANCE_MODE
        with WHITELIST_LOCK:
            nw = len(WHITELIST_USERS)
        update.message.reply_text(
            "🛠 پنل ادمین\n"
            "ورکر: %d/%d | صف: %d\n"
            "✅ %s | ❌ %s\n"
            "📤 %s\n"
            "🚫 مسدود: %d | 🔒 وایت‌لیست: %d | تعمیرات: %s\n\n"
            "/admin ban USER_ID\n"
            "/admin unban USER_ID\n"
            "/admin clear_queue\n"
            "/admin backup\n"
            "/admin broadcast متن پیام\n"
            "/admin maintenance on|off\n"
            "/admin whitelist on|off|add ID|del ID\n"
            "/admin update_ytdlp"
            % (
                aw, MAX_CONCURRENT_DOWNLOADS, qsize,
                s.get("downloads_ok", 0), s.get("downloads_fail", 0),
                human_size(s.get("bytes_uploaded", 0)),
                nb, nw, "ON" if maint else "OFF",
            ),
            reply_markup=make_admin_panel_keyboard(),
        )
        return
    cmd = str(args[0]).lower()
    if cmd == "ban" and len(args) >= 2 and str(args[1]).lstrip("-").isdigit():
        ban_user(int(args[1]))
        update.message.reply_text("مسدود شد: %s" % args[1])
    elif cmd == "unban" and len(args) >= 2 and str(args[1]).lstrip("-").isdigit():
        unban_user(int(args[1]))
        update.message.reply_text("رفع مسدودیت: %s" % args[1])
    elif cmd == "clear_queue":
        n = 0
        while not download_queue.empty():
            try:
                download_queue.get_nowait()
                n += 1
            except Exception:
                break
        update.message.reply_text("%d جاب از صف حذف شد." % n)
    elif cmd == "backup":
        try:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            dest = BACKUP_ROOT / ("backup_%s" % ts)
            dest.mkdir(parents=True, exist_ok=True)
            if Path(COOKIES_ROOT).exists():
                shutil.copytree(COOKIES_ROOT, dest / "cookies", dirs_exist_ok=True)
            if Path(USERS_ROOT).exists():
                for ud in Path(USERS_ROOT).iterdir():
                    if ud.is_dir():
                        td = dest / "users" / ud.name
                        td.mkdir(parents=True, exist_ok=True)
                        for name in ("prefs.json", "history.json"):
                            src = ud / name
                            if src.exists():
                                shutil.copy2(src, td / name)
            update.message.reply_text("✅ بکاپ: %s" % dest)
        except Exception as e:
            update.message.reply_text("خطا در بکاپ: %s" % e)
    elif cmd == "broadcast":
        msg = " ".join(args[1:]).strip()
        if not msg:
            update.message.reply_text("مثال: /admin broadcast سلام به همه")
            return
        with USER_MAP_LOCK:
            users = list(USER_MAP.keys())
        ok_n = fail_n = 0
        for uid in users:
            try:
                context.bot.send_message(chat_id=uid, text="📢 " + msg)
                ok_n += 1
            except Exception:
                fail_n += 1
            time.sleep(0.05)
        update.message.reply_text("ارسال شد: %d موفق / %d ناموفق" % (ok_n, fail_n))
    elif cmd == "maintenance" and len(args) >= 2:
        with MAINTENANCE_LOCK:
            MAINTENANCE_MODE = str(args[1]).lower() in ("on", "1", "true", "yes")
            state = MAINTENANCE_MODE
        update.message.reply_text("حالت تعمیرات: %s" % ("ON" if state else "OFF"))
    elif cmd == "whitelist":
        if len(args) < 2:
            update.message.reply_text("/admin whitelist on|off|add ID|del ID")
            return
        sub = str(args[1]).lower()
        if sub in ("on", "1", "true"):
            WHITELIST_ENABLED = True
            update.message.reply_text("Whitelist فعال شد")
        elif sub in ("off", "0", "false"):
            WHITELIST_ENABLED = False
            update.message.reply_text("Whitelist خاموش شد")
        elif sub == "add" and len(args) >= 3 and str(args[2]).isdigit():
            with WHITELIST_LOCK:
                WHITELIST_USERS.add(int(args[2]))
            update.message.reply_text("اضافه شد: %s" % args[2])
        elif sub == "del" and len(args) >= 3 and str(args[2]).isdigit():
            with WHITELIST_LOCK:
                WHITELIST_USERS.discard(int(args[2]))
            update.message.reply_text("حذف شد: %s" % args[2])
        else:
            update.message.reply_text("آرگومان نامعتبر")
    elif cmd == "update_ytdlp":
        try:
            import subprocess
            r = subprocess.run(
                [os.environ.get("PYTHON", "python3"), "-m", "pip", "install", "-U", "yt-dlp"],
                capture_output=True, text=True, timeout=180,
            )
            tail = ((r.stdout or "") + (r.stderr or ""))[-500:]
            update.message.reply_text("yt-dlp update:\n%s" % tail)
        except Exception as e:
            update.message.reply_text("خطا: %s" % e)
    elif cmd == "blacklist":
        global DOMAIN_BLACKLIST
        if len(args) < 2:
            update.message.reply_text("لیست سیاه: %s\n/admin blacklist add domain.com\n/admin blacklist del domain.com" % (", ".join(sorted(DOMAIN_BLACKLIST)) or "(خالی)"))
            return
        sub = str(args[1]).lower()
        if sub == "add" and len(args) >= 3:
            d = str(args[2]).lower().lstrip(".")
            DOMAIN_BLACKLIST.add(d)
            update.message.reply_text("اضافه شد: %s" % d)
        elif sub == "del" and len(args) >= 3:
            d = str(args[2]).lower().lstrip(".")
            DOMAIN_BLACKLIST.discard(d)
            update.message.reply_text("حذف شد: %s" % d)
        else:
            update.message.reply_text("آرگومان نامعتبر")
    else:
        update.message.reply_text("دستور ناشناخته. /admin")



def status_cmd(update, context):
    user_id = update.message.from_user.id
    with active_workers_lock:
        aw = active_workers
    qsize = download_queue.qsize()
    with QUEUE_META_LOCK:
        mine = [v for k, v in QUEUE_META.items() if v.get("user_id") == user_id]
        all_jobs = list(QUEUE_META.values())
    eta_txt = format_eta(estimate_queue_wait_seconds(qsize))
    lines = [
        "📊 وضعیت سیستم",
        "👷 ورکر فعال: %d/%d" % (aw, MAX_CONCURRENT_DOWNLOADS),
        "📥 صف انتظار: %d | شروع تقریبی برای کار بعدی: %s" % (qsize, eta_txt),
        "🔄 جاب‌های در حال اجرا: %d" % len(all_jobs),
    ]
    if mine:
        lines.append("")
        lines.append("جاب‌های شما:")
        for j in mine[:10]:
            lines.append("• %s — %s" % (j.get("status", "?"), (j.get("url") or "")[:50]))
    else:
        lines.append("")
        lines.append("جاب فعالی برای شما نیست.")
    with USER_LIMITS_LOCK:
        lim = USER_LIMITS.get(user_id) or {}
    lines.append("")
    lines.append(
        "امروز: %s/%s دانلود | %s / %s"
        % (
            lim.get("count", 0),
            MAX_DAILY_DOWNLOADS_PER_USER,
            human_size(lim.get("bytes", 0)),
            human_size(MAX_DAILY_BYTES_PER_USER),
        )
    )
    update.message.reply_text("\n".join(lines))


def dashboard_cmd(update, context):
    """داشبورد زنده برای ادمین: ورکر، صف، رم، دیسک، شبکه، آمار"""
    user_id = update.message.from_user.id
    if not is_admin(user_id):
        update.message.reply_text("فقط ادمین.")
        return
    with active_workers_lock:
        aw = active_workers
    qsize = download_queue.qsize()
    with QUEUE_META_LOCK:
        running = len(QUEUE_META)
    with GLOBAL_STATS_LOCK:
        s = dict(GLOBAL_STATS)
    uptime = int(time.time() - (s.get("started_at") or time.time()))

    # سیستم
    try:
        mem = psutil.virtual_memory()
        mem_txt = f"{mem.percent:.0f}% ({human_size(mem.used)} / {human_size(mem.total)})"
    except Exception:
        mem_txt = "—"
    try:
        disk = psutil.disk_usage(DOWNLOAD_ROOT if os.path.exists(DOWNLOAD_ROOT) else "/")
        disk_txt = f"{disk.percent:.0f}% ({human_size(disk.used)} / {human_size(disk.total)})"
    except Exception:
        disk_txt = "—"
    try:
        cpu = psutil.cpu_percent(interval=0.3)
        cpu_txt = f"{cpu:.0f}%"
    except Exception:
        cpu_txt = "—"
    try:
        load = os.getloadavg()
        load_txt = f"{load[0]:.2f} / {load[1]:.2f} / {load[2]:.2f}"
    except Exception:
        load_txt = "—"

    with BANNED_LOCK:
        nbanned = len(BANNED_USERS)
    with MAINTENANCE_LOCK:
        maint = MAINTENANCE_MODE
    with WHITELIST_LOCK:
        nwl = len(WHITELIST_USERS)

    tg_ok = "—"
    try:
        if telethon_client and getattr(telethon_client, "is_connected", lambda: False)():
            tg_ok = "✅"
        elif TELETHON_API_ID:
            tg_ok = "❌"
    except Exception:
        tg_ok = "?"

    lines = [
        "🖥 داشبورد زنده",
        "────────────────",
        f"👷 ورکر: {aw}/{MAX_CONCURRENT_DOWNLOADS} | صف: {qsize} | در حال اجرا: {running}",
        f"⏱ آپ‌تایم: {format_eta(uptime)}",
        f"✅ موفق: {s.get('downloads_ok', 0)} | ❌ ناموفق: {s.get('downloads_fail', 0)}",
        f"📤 حجم آپلود: {human_size(s.get('bytes_uploaded', 0))}",
        f"📈 میانگین کار: {float(s.get('avg_task_seconds') or 0):.1f}s",
        "────────────────",
        f"🧠 RAM: {mem_txt}",
        f"💾 Disk: {disk_txt}",
        f"⚙️ CPU: {cpu_txt} | Load: {load_txt}",
        "────────────────",
        f"🔌 Telethon: {tg_ok}",
        f"🚫 مسدود: {nbanned} | 🔒 وایت‌لیست: {nwl} | تعمیرات: {'ON' if maint else 'OFF'}",
        f"👥 کاربران شناخته‌شده: {len(USER_MAP)}",
    ]
    update.message.reply_text("\n".join(lines))


def stats_cmd(update, context):
    user_id = update.message.from_user.id
    if ADMIN_IDS and user_id not in ADMIN_IDS:
        update.message.reply_text("فقط ادمین.")
        return
    with GLOBAL_STATS_LOCK:
        s = dict(GLOBAL_STATS)
    uptime = int(time.time() - s.get("started_at", time.time()))
    update.message.reply_text(
        "📈 آمار ربات\n✅ موفق: %s\n❌ ناموفق: %s\n📤 حجم آپلود: %s\n⏱ آپ‌تایم: %s\n👥 کاربران شناخته‌شده: %s"
        % (
            s.get("downloads_ok", 0),
            s.get("downloads_fail", 0),
            human_size(s.get("bytes_uploaded", 0)),
            format_eta(uptime),
            len(USER_MAP),
        )
    )


def cookies_cmd(update, context):
    update.message.reply_text(
        "🍪 برای سایت‌های محدود (اینستاگرام، یوتیوب اعضا و ...):\n\n"
        "1) از مرورگر با افزونه Get cookies.txt کوکی را export کن\n"
        "2) فایل .txt را همین‌جا برای ربات بفرست\n"
        "3) یا محتویات را با کپشن `cookies` بفرست\n\n"
        "برای حذف: /clear_cookies"
    )

def clear_cookies_cmd(update, context):
    user_id = update.message.from_user.id
    p = cookies_path_for_user(user_id)
    try:
        if p.exists():
            p.unlink()
        update.message.reply_text("کوکی شما حذف شد.")
    except Exception as e:
        update.message.reply_text(f"خطا: {e}")


def fromdrive_cmd(update, context):
    """
    مسیر برعکس: دریافت یک فایل از Google Drive (با لینک یا fileId) و ارسال آن به تلگرام،
    با گزارش زندهٔ پیشرفت دانلود و آپلود.
    """
    chat_id = update.message.chat_id
    user_id = update.message.from_user.id
    username = update.message.from_user.username or str(user_id)
    args = context.args or []
    if not args:
        update.message.reply_text(
            "مثال:\n/fromdrive https://drive.google.com/file/d/FILE_ID/view\nیا مستقیم:\n/fromdrive FILE_ID"
        )
        return
    ok, msg = user_allowed(user_id)
    if not ok:
        update.message.reply_text(msg)
        return
    file_id = parse_drive_file_id(args[0])
    if not file_id:
        update.message.reply_text("لینک/شناسهٔ Google Drive نامعتبر است.")
        return

    bot = context.bot
    task_id = uuid.uuid4().hex[:12]
    with CANCEL_LOCK:
        CANCEL_FLAGS[task_id] = {"cancel": False, "owner_id": user_id}
    try:
        progress_msg = bot.send_message(
            chat_id=chat_id, text="☁️ در حال دریافت اطلاعات فایل از Google Drive...",
            reply_markup=make_cancel_markup(task_id, user_id)
        )
    except Exception:
        progress_msg = bot.send_message(chat_id=chat_id, text="☁️ در حال دریافت اطلاعات فایل از Google Drive...")

    def worker():
        state = {"prev_sent": None, "prev_t": None, "last_edit": 0}
        dest_path = None

        def make_progress_text(icon, stage, sent, total):
            now = time.time()
            speed = 0
            if state["prev_sent"] is not None and state["prev_t"] is not None and now - state["prev_t"] > 0:
                speed = (sent - state["prev_sent"]) / max(1e-6, now - state["prev_t"])
            pct = int(sent * 100 / total) if total else 0
            bar_len = 10
            filled = int(bar_len * pct / 100) if pct else 0
            bar = f"{icon}" * filled + "⬜" * (bar_len - filled)
            eta_text = "—"
            if speed > 0 and total:
                remaining = max(0, total - sent)
                eta_text = format_eta(remaining / speed)
            text = (
                f"{stage}\n"
                f"{bar} {pct}%\n"
                f"⚡ سرعت: {speed/1024/1024:.2f} MB/s\n"
                f"📦 حجم: {human_size(sent)} از {human_size(total)}\n"
                f"⏳ زمان باقی‌مانده: {eta_text}"
            )
            state["prev_sent"] = sent
            state["prev_t"] = now
            return text, now

        try:
            udir = ensure_user_dir(username or user_id)
            tmp_dir = udir / "drive_tmp"
            tmp_dir.mkdir(parents=True, exist_ok=True)
            dest_path = str(tmp_dir / f"{task_id}.bin")

            def dl_progress_cb(sent, total):
                text, now = make_progress_text("🟦", "☁️ دریافت از Google Drive:", sent, total)
                if now - state["last_edit"] > 1.0:
                    try:
                        bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, text=text, reply_markup=make_cancel_markup(task_id, user_id))
                    except Exception:
                        pass
                    state["last_edit"] = now

            meta = download_from_drive_with_progress(file_id, dest_path, progress_callback=dl_progress_cb, task_id=task_id)

            try:
                bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, text="📤 در حال ارسال فایل به تلگرام...", reply_markup=make_cancel_markup(task_id, user_id))
            except Exception:
                pass
            state["prev_sent"] = None
            state["prev_t"] = None

            def up_progress_cb(sent, total):
                text, now = make_progress_text("🟩", "📤 آپلود به تلگرام:", sent, total)
                if now - state["last_edit"] > 1.0:
                    try:
                        bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, text=text, reply_markup=make_cancel_markup(task_id, user_id))
                    except Exception:
                        pass
                    state["last_edit"] = now

            caption = (meta.get("name") or "file")[:1024]
            upload_with_smart_choice(TOKEN, bot, chat_id, dest_path, caption=caption, progress_update_fn=up_progress_cb, task_id=task_id)

            try:
                bot.edit_message_text(
                    chat_id=chat_id, message_id=progress_msg.message_id,
                    text=f"✅ فایل با موفقیت ارسال شد\n📁 {meta.get('name')}\n📦 {human_size(meta.get('size'))}"
                )
            except Exception:
                pass
            append_user_log(get_log_key_for_user(user_id), {"event": "fromdrive_ok", "file_id": file_id, "size": meta.get("size")})
        except Exception as e:
            try:
                bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, text=f"❗ خطا در دریافت/ارسال فایل: {str(e)}")
            except Exception:
                pass
            append_user_log(get_log_key_for_user(user_id), {"event": "fromdrive_error", "error": str(e), "file_id": file_id})
        finally:
            with CANCEL_LOCK:
                CANCEL_FLAGS.pop(task_id, None)
            if dest_path:
                try:
                    if os.path.exists(dest_path):
                        os.remove(dest_path)
                except Exception:
                    pass

    threading.Thread(target=worker, daemon=True, name=f"fromdrive-{task_id}").start()

def prefs_cmd(update, context):
    user_id = update.message.from_user.id
    prefs = load_user_prefs(user_id)
    update.message.reply_text(
        f"⚙️ تنظیمات شما:\n"
        f"• حالت پیش‌فرض: {prefs.get('default_mode')}\n"
        f"• کیفیت پیش‌فرض: {prefs.get('default_quality')}\n"
        f"• زیرنویس: {prefs.get('want_subtitles')}\n"
        f"• زبان زیرنویس: {prefs.get('subtitle_langs')}\n"
        f"• فشرده‌سازی: {prefs.get('compress')}\n"
        f"• ارسال تامبنیل: {prefs.get('send_thumbnail')}\n\n"
        f"برای تغییر، از دکمه‌های دانلود استفاده کن (خودکار ذخیره می‌شود).\n"
        f"تغییر زبان زیرنویس: /set_subs fa,en"
    )

def set_subs_cmd(update, context):
    user_id = update.message.from_user.id
    args = " ".join(context.args).strip() if context.args else ""
    if not args:
        update.message.reply_text("مثال: /set_subs fa,en")
        return
    prefs = load_user_prefs(user_id)
    prefs["subtitle_langs"] = args
    save_user_prefs(user_id, prefs)
    update.message.reply_text(f"زبان زیرنویس تنظیم شد: {args}")

def search_cmd(update, context):
    q = " ".join(context.args).strip() if context.args else ""
    if not q:
        update.message.reply_text("مثال: /search آهنگ سنتی")
        return
    msg = update.message.reply_text("🔍 در حال جستجو...")
    try:
        ydl_opts = {"quiet": True, "no_warnings": True, "extract_flat": True}
        if YTDLP_PROXY:
            ydl_opts["proxy"] = YTDLP_PROXY
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            res = ydl.extract_info(f"ytsearch8:{q}", download=False)
        entries = (res.get("entries") or [])[:8]
        if not entries:
            msg.edit_text("نتیجه‌ای پیدا نشد.")
            return
        buttons = []
        text_lines = [f"نتایج جستجو برای: {q}\n"]
        for i, e in enumerate(entries, 1):
            title = (e.get("title") or "بدون عنوان")[:60]
            vid = e.get("id") or ""
            url = e.get("url") or (f"https://www.youtube.com/watch?v={vid}" if vid else None)
            if not url:
                continue
            text_lines.append(f"{i}. {title}")
            buttons.append([InlineKeyboardButton(f"⬇ {i}. {title[:40]}", callback_data=f"dl_direct:{url}")])
        msg.edit_text("\n".join(text_lines), reply_markup=InlineKeyboardMarkup(buttons) if buttons else None)
    except Exception as e:
        try:
            msg.edit_text(f"خطا در جستجو: {e}")
        except Exception:
            pass

def handle_document(update, context):
    """کوکی یا فایل رسانه‌ای کاربر"""
    user = update.message.from_user
    user_id = user.id
    ok, msg = user_allowed(user_id)
    if not ok:
        update.message.reply_text(msg)
        return
    caption = (update.message.caption or "").strip().lower()
    doc = update.message.document
    video = update.message.video
    photos = update.message.photo
    tg_file, fname, mime, file_size = None, "", "", 0
    if doc:
        tg_file, fname, mime, file_size = doc, (doc.file_name or "").lower(), (doc.mime_type or "").lower(), doc.file_size or 0
    elif video:
        tg_file = video
        fname = (getattr(video, "file_name", None) or "video.mp4").lower()
        mime = (video.mime_type or "video/mp4").lower()
        file_size = video.file_size or 0
    elif photos:
        p = photos[-1]
        tg_file, fname, mime, file_size = p, "photo.jpg", "image/jpeg", p.file_size or 0
    else:
        return

    if "cookie" in fname or caption == "cookies" or (fname.endswith(".txt") and "cookie" in caption):
        try:
            f = tg_file.get_file()
            dest = cookies_path_for_user(user_id)
            f.download(custom_path=str(dest))
            update.message.reply_text(f"✅ کوکی ذخیره شد ({dest.name}).")
        except Exception as e:
            update.message.reply_text(f"خطا: {e}")
        return

    is_video = mime.startswith("video/") or fname.endswith((".mp4", ".mkv", ".webm", ".avi", ".mov"))
    is_audio = mime.startswith("audio/") or fname.endswith((".mp3", ".m4a", ".ogg", ".flac", ".wav"))
    is_image = mime.startswith("image/") or fname.endswith((".png", ".jpg", ".jpeg", ".webp"))
    if not (is_video or is_audio or is_image):
        return
    if file_size and file_size > 50 * 1024 * 1024:
        update.message.reply_text("حداکثر ۵۰ مگابایت.")
        return
    user_dir = os.path.join(USERS_ROOT, str(user_id), "uploads")
    os.makedirs(user_dir, exist_ok=True)
    safe_name = re.sub(r"[^\w.\-]", "_", fname or f"f_{uuid.uuid4().hex[:8]}")
    dest = os.path.join(user_dir, f"{int(time.time())}_{safe_name}")
    try:
        tg_file.get_file().download(custom_path=dest)
    except Exception as e:
        update.message.reply_text(f"خطا در دریافت: {e}")
        return
    if is_image and (caption in ("logo", "لوگو") or "logo" in caption):
        with USER_MEDIA_LOCK:
            st = USER_MEDIA_STATE.get(user_id) or {"videos": [], "logo": None}
            st["logo"] = dest
            USER_MEDIA_STATE[user_id] = st
        update.message.reply_text("✅ لوگو ذخیره شد. ویدیو بفرست و /logo بزن.")
        return
    if is_video:
        with USER_MEDIA_LOCK:
            st = USER_MEDIA_STATE.get(user_id) or {"videos": [], "logo": None}
            st.setdefault("videos", []).append(dest)
            st["videos"] = st["videos"][-10:]
            USER_MEDIA_STATE[user_id] = st
            n = len(st["videos"])
        update.message.reply_text(
            f"✅ ویدیو ذخیره شد ({n}).\n/merge برای ادغام | /logo برای لوگو\nیا پردازش:",
            reply_markup=make_userfile_keyboard(dest),
        )
        return
    update.message.reply_text("✅ ذخیره شد:", reply_markup=make_userfile_keyboard(dest))

def process_single_link(update, context, link):
    """
    تغییرات:
      - انیمیشن ALI sequence یا I Fill را در ابتدای پردازش شروع می‌کنیم (key یکتا)
      - پس از آماده شدن کیبورد یا خطا، انیمیشن را متوقف می‌کنیم
      - بقیهٔ منطق بدون تغییر باقی مانده است
    """
    chat_id = update.message.chat_id
    user = update.message.from_user
    user_id = user.id
    username = user.username or str(user_id)

    with USER_MAP_LOCK:
        USER_MAP[user_id] = username

    # create a unique request id and animation key
    request_id = uuid.uuid4().hex[:12]
    anim_key = f"quality_anim_{request_id}"

    # start ALI animation immediately to show activity
    try:
        quality_ali_anim.start(anim_key, bot=context.bot, chat_id=chat_id, title="در حال بررسی کیفیت…", min_interval=0.9)
    except Exception:
        pass

    # extract once and store
    try:
        info = extract_info_safe(link, user_id=user_id)
    except ExtractError as e:
        try:
            quality_ali_anim.stop(anim_key, final_text=f"❗ خطا در استخراج اطلاعات: {str(e)}")
        except:
            pass
        context.bot.send_message(chat_id=chat_id, text=f"❗ خطا: {str(e)}")
        append_user_log(get_log_key_for_user(user_id), {"event": "extract_error", "url": link, "error": str(e)})
        return
    except Exception as e:
        try:
            quality_ali_anim.stop(anim_key, final_text=f"❗ خطا در استخراج اطلاعات: {str(e)}")
        except:
            pass
        context.bot.send_message(chat_id=chat_id, text=f"❗ خطا در استخراج اطلاعات: {str(e)}")
        append_user_log(get_log_key_for_user(user_id), {"event": "extract_error", "url": link, "error": str(e)})
        return

    parsed_formats = parse_formats_from_info(info)
    title = info.get("title") or "بدون عنوان"
    uploader = info.get("uploader") or info.get("uploader_id") or ""
    duration = info.get("duration")
    views = info.get("view_count")
    live_tag = " 🔴 LIVE" if is_live_info(info) else ""
    meta = f"🎬 {title}{live_tag}\n📺 {uploader}\n⏱ {time.strftime('%M:%S', time.gmtime(duration)) if duration else '—'}  •  👁 {views or '—'}"
    if is_live_info(info):
        meta += f"\n\n⚠️ استریم زنده — مدت ضبط را انتخاب کن (پیش‌فرض {LIVE_RECORD_SECONDS // 60} دقیقه)."

    with REQUESTS_LOCK:
        REQUESTS[request_id] = {
            "type": "formats",
            "url": link,
            "formats": parsed_formats,
            "info": info,
            "created": time.time(),
            "user_id": user_id,
            "cancel": False,
            "error": None,
            "progress_msg_id": None
        }

    # send initial message with cancel button immediately — با تصویر بندانگشتی در صورت وجود
    thumb_url = info.get("thumbnail")
    msg = None
    if thumb_url:
        try:
            msg = context.bot.send_photo(
                chat_id=chat_id,
                photo=thumb_url,
                caption=f"{meta}\n\nدر حال آماده‌سازی کیبورد...",
                reply_markup=make_request_cancel_markup(request_id, user_id),
            )
        except Exception:
            msg = None
    if msg is None:
        try:
            msg = context.bot.send_message(chat_id=chat_id, text=f"{meta}\n\nدر حال آماده‌سازی کیبورد...", reply_markup=make_request_cancel_markup(request_id, user_id))
        except Exception:
            msg = context.bot.send_message(chat_id=chat_id, text=f"{meta}\n\nدر حال آماده‌سازی کیبورد...")
    with REQUESTS_LOCK:
        REQUESTS[request_id]["progress_msg_id"] = msg.message_id

    # build keyboard in background to avoid blocking
    def build_and_attach_keyboard(rid, bot, chat_id, msg_id):
        with REQUESTS_LOCK:
            req = REQUESTS.get(rid)
        if not req:
            try:
                quality_ali_anim.stop(anim_key)
            except:
                pass
            return
        # check cancel before building
        if req.get("cancel"):
            try:
                safe_edit_message(bot, chat_id, msg_id, "❌ این درخواست لغو شد.")
            except:
                pass
            try:
                quality_ali_anim.stop(anim_key, final_text="❌ این درخواست لغو شد.")
            except:
                pass
            return
        parsed = req.get("formats") or []
        default_cat = "all"
        if is_live_info(req.get("info") or {}):
            kb = make_live_keyboard(rid)
        else:
            kb = make_quality_keyboard(parsed, rid, category=default_cat, page=0)
        try:
            bot.edit_message_reply_markup(chat_id=chat_id, message_id=msg_id, reply_markup=kb)
            # stop animation now that keyboard is ready
            try:
                quality_ali_anim.stop(anim_key)
            except:
                pass
        except:
            try:
                bot.send_message(chat_id=chat_id, text="کیبورد آماده شد.", reply_markup=kb)
                try:
                    quality_ali_anim.stop(anim_key)
                except:
                    pass
            except:
                try:
                    quality_ali_anim.stop(anim_key)
                except:
                    pass

    threading.Thread(target=build_and_attach_keyboard, args=(request_id, context.bot, chat_id, msg.message_id), daemon=True).start()

def handle_channel_cmd(update, context):
    q = " ".join(context.args).strip()
    if not q:
        update.message.reply_text("مثال: /channel @honarnewsofficial_ یا /channel soheilprank")
        return
    if q.startswith("@"):
        q = q[1:]
    msg = update.message.reply_text("در حال جستجوی کانال...")
    try:
        info = fetch_channel_info(q)
    except Exception as e:
        msg.edit_text(f"خطا در یافتن کانال: {str(e)}")
        return
    if not info:
        msg.edit_text("کانالی یافت نشد.")
        return

    channel_url = build_channel_url_from_info(info)
    if not channel_url:
        try:
            with yt_dlp.YoutubeDL({"quiet": True}) as ydl:
                search_res = ydl.extract_info(f"ytsearch:channel {info.get('title')}", download=False)
                entries = search_res.get("entries", []) or []
                if entries:
                    channel_url = entries[0].get("webpage_url") or entries[0].get("url")
        except:
            channel_url = None

    title = info.get("title") or info.get("uploader") or "کانال"
    username = info.get("uploader_id") or info.get("webpage_url") or ""
    subs = info.get("subscriber_count") or "—"
    videos_count = info.get("video_count") or "—"
    views = info.get("view_count") or "—"
    created = info.get("upload_date") or ""
    thumb = info.get("thumbnail")
    text = (
        f"📛 {title}\n"
        f"👤 {username}\n"
        f"📊 مشترکین: {subs}  •  ویدیوها: {videos_count}  •  بازدیدها: {views}\n"
        f"🗓 {created}"
    )

    req_id = uuid.uuid4().hex[:12]
    with REQUESTS_LOCK:
        REQUESTS[req_id] = {
            "type": "channel_card",
            "channel_url": channel_url,
            "info": info,
            "created": time.time(),
            "user_id": update.message.from_user.id,
            "cancel": False,
            "error": None,
            "progress_msg_id": None
        }

    buttons = [
        [InlineKeyboardButton("🎬 مشاهده ویدیوها", callback_data=f"chan:videos:{req_id}")],
        [InlineKeyboardButton("📁 مشاهده پلی‌لیست‌ها", callback_data=f"chan:playlists:{req_id}")]
    ]
    try:
        msg.delete()
    except:
        pass
    if thumb:
        update.message.reply_photo(photo=thumb, caption=text, reply_markup=InlineKeyboardMarkup(buttons))
    else:
        update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons))

def handle_message(update, context):
    """
    تغییرات:
      - به محض دریافت پیام حاوی لینک، یک پیام فوری 'لینک دریافت شد' ارسال می‌شود
      - انیمیشن ALI sequence نیز بلافاصله شروع می‌شود (تا زمانی که process_single_link آن را متوقف کند)
      - برای لینک‌های شبکه اجتماعی (اینستاگرام، X، فیس‌بوک و ...) دیگر مستقیم به صف اضافه نمی‌شود؛
        بلکه همانند لینک‌های معمولی وارد process_single_link می‌شود تا کیفیت‌ها شناسایی شده و سپس از کاربر پرسیده شود کجا ذخیره شود.
    """
    text = (update.message.text or "").strip()
    chat_id = update.message.chat_id
    user = update.message.from_user
    user_id = user.id
    username = user.username or str(user_id)

    with USER_MAP_LOCK:
        USER_MAP[user_id] = username

    ok, msg = user_allowed(user_id)
    if not ok:
        try:
            update.message.reply_text(msg)
        except Exception:
            pass
        return

    # پشتیبانی گروه
    try:
        chat = update.effective_chat
        if chat and chat.type in ("group", "supergroup"):
            if not ENABLE_GROUPS:
                update.message.reply_text("ربات در گروه غیرفعال است. در خصوصی پیام بده.")
                return
            if GROUP_ADMIN_ONLY and not is_admin(user_id):
                try:
                    member = context.bot.get_chat_member(chat.id, user_id)
                    status = getattr(member, "status", "")
                    if status not in ("creator", "administrator"):
                        update.message.reply_text("در این گروه فقط ادمین‌ها می‌توانند لینک بفرستند.")
                        return
                except Exception:
                    pass
    except Exception:
        pass

    # ضد سیل
    try:
        with USER_LAST_REQUEST_LOCK:
            last = USER_LAST_REQUEST.get(user_id) or 0
            now = time.time()
            if now - last < USER_FLOOD_SECONDS and not is_admin(user_id):
                update.message.reply_text("⏳ کمی صبر کن و دوباره بفرست.")
                return
            USER_LAST_REQUEST[user_id] = now
    except Exception:
        pass

    if not text:
        update.message.reply_text("لینک یا عبارت را ارسال کن.")
        return

    # تورنت / مگنت
    # لینک پیام کانال/گروه تلگرام (خصوصی یا عمومی)
    tg_ref = parse_telegram_private_link(text)
    if tg_ref:
        chat_ref, mid = tg_ref
        progress = update.message.reply_text("📥 در حال دریافت از تلگرام (Telethon)...")
        try:
            dest_dir = os.path.join(USERS_ROOT, str(user_id), "tg_private")
            path = download_telegram_private_media(chat_ref, mid, dest_dir)
            if not path or not os.path.exists(path):
                raise RuntimeError("دانلود ناموفق")
            size = os.path.getsize(path)
            caption = f"✅ از تلگرام\n📦 {human_size(size)}"
            if size < 45 * 1024 * 1024:
                with open(path, "rb") as f:
                    context.bot.send_document(chat_id=chat_id, document=f, caption=caption)
            else:
                client = ensure_telethon_client()
                if client and telethon_loop:
                    import asyncio
                    async def _up():
                        await client.send_file(chat_id, path, caption=caption)
                    asyncio.run_coroutine_threadsafe(_up(), telethon_loop).result(timeout=600)
                else:
                    with open(path, "rb") as f:
                        context.bot.send_document(chat_id=chat_id, document=f, caption=caption)
            try:
                progress.delete()
            except Exception:
                pass
        except Exception as e:
            try:
                progress.edit_text(
                    f"❗ خطا در دریافت از تلگرام:\n{str(e)[:350]}\n\n"
                    "نکته: باید با session کاربر داخل کانال/گروه باشی."
                )
            except Exception:
                pass
        return

    if is_magnet_or_torrent(text):
        progress = update.message.reply_text("🧲 تورنت — دانلود با aria2...")
        try:
            out_dir = os.path.join(USERS_ROOT, str(user_id), "torrents", uuid.uuid4().hex[:10])
            result_path = aria2_download_torrent(text.strip(), out_dir)
            with open(result_path, "rb") as f:
                context.bot.send_document(chat_id=chat_id, document=f, caption=f"✅ {os.path.basename(result_path)}")
            try:
                progress.delete()
            except Exception:
                pass
        except FileNotFoundError:
            progress.edit_text("❗ aria2c نصب نیست: apt install aria2")
        except Exception as e:
            try:
                progress.edit_text(f"❗ {str(e)[:400]}")
            except Exception:
                pass
        return

    # اسپاتیفای / ساوندکلاد
    if text.startswith("http") and is_spotify_or_soundcloud(text):
        progress = update.message.reply_text("🎵 تبدیل به جستجوی یوتیوب...")
        try:
            yurl = resolve_spotify_to_search(text)
            try:
                progress.delete()
            except Exception:
                pass
            process_single_link(update, context, yurl)
        except Exception as e:
            try:
                progress.edit_text(f"❗ {str(e)[:300]}")
            except Exception:
                pass
        return

    links = re.findall(r'https?://\S+', text)
    if len(links) > 1:
        try:
            context.bot.send_message(
                chat_id=chat_id,
                text=f"🔗 {len(links)} لینک دریافت شد. برای هر کدام کیفیت را جدا انتخاب کن.\nدر پایان گزارش کلی ارسال می‌شود.",
            )
        except Exception:
            pass
        # batch فقط برای آمار کلی؛ هر لینک هنوز UI کیفیت خودش را دارد
        batch_id = create_batch(user_id, chat_id, len(links), bot=context.bot)
        for link in links:
            try:
                context.bot.send_message(chat_id=chat_id, text=f"لینک: {link[:60]}...")
            except Exception:
                pass
            process_single_link(update, context, link)
        # ذخیره batch_id در context کاربر ساده: روی REQUESTS بعداً وصل نمی‌شود؛
        # گزارش نهایی وقتی همه جاب‌های queue شده تمام شوند از worker می‌آید اگر batch_id در task باشد.
        # برای لینک‌های UI-محور، کاربر جدا انتخاب می‌کند؛ batch گزارش اختیاری است.
        append_user_log(get_log_key_for_user(user_id), {"event": "multi_link", "count": len(links), "batch_id": batch_id})
        return

    if text.startswith("@"):
        uname = text[1:].strip()
        if not uname:
            update.message.reply_text("یوزرنیم نامعتبر است.")
            return
        return search_youtube_channel(uname, update, context)

    if text.startswith("http"):
        if is_domain_blacklisted(text):
            try:
                update.message.reply_text("🚫 این دامنه در لیست سیاه است و قابل دانلود نیست.")
            except Exception:
                pass
            return
        # immediate acknowledgement and start a short-lived animation message
        try:
            ack_msg = context.bot.send_message(chat_id=chat_id, text="لینک دریافت شد. در حال بررسی...")
        except:
            ack_msg = None
        # For social links, do NOT enqueue directly; instead show qualities and then ask mode (process_single_link)
        if is_instagram_or_x(text):
            try:
                if is_instagram_story_url(text):
                    context.bot.send_message(
                        chat_id=chat_id,
                        text="📲 استوری/هایلایت اینستاگرام شناسایی شد.\n"
                             "اگر خطا گرفتی کوکی بفرست (/cookies) یا از /story URL استفاده کن.",
                    )
                else:
                    context.bot.send_message(chat_id=chat_id, text="🔎 لینک شبکه اجتماعی شناسایی شد — در حال بررسی کیفیت و آماده‌سازی گزینه‌ها...")
            except Exception:
                pass
            process_single_link(update, context, text)
            append_user_log(get_log_key_for_user(user_id), {"event": "social_link_received", "url": text})
            return
        # call process_single_link which will manage animations and keyboard
        process_single_link(update, context, text)
        return

    update.message.reply_text("متن دریافتی لینک نیست؛ برای جستجو از /search یا /channel استفاده کن یا یوزرنیم کانال را با @ ارسال کن.")

# -------------------------
# Callback handler
# -------------------------
def send_channel_videos_page_by_req(req_id, page, bot, callback_query):
    with REQUESTS_LOCK:
        req = REQUESTS.get(req_id)
    if not req:
        try:
            safe_edit_message(bot, callback_query.message.chat_id, callback_query.message.message_id, "زمان درخواست منقضی شد.")
        except:
            pass
        return
    items = req.get("items", [])
    per = 10
    start = page*per
    end = start+per
    text = "ویدیوهای کانال:\n\n"
    buttons = []
    for idx, v in enumerate(items[start:end], start+1):
        dur = v.get("duration")
        text += f"{start+idx}. {time.strftime('%M:%S', time.gmtime(dur)) if dur else '—'}  {v['title'][:60]}\n"
        buttons.append([InlineKeyboardButton("⬇ دانلود", callback_data=f"dl_direct:{v['url']}")])
    nav = []
    if start > 0:
        nav.append(InlineKeyboardButton("⬅️ قبلی", callback_data=f"chanpage:{req_id}:{page-1}"))
    if end < len(items):
        nav.append(InlineKeyboardButton("بعدی ➡️", callback_data=f"chanpage:{req_id}:{page+1}"))
    if nav:
        buttons.append(nav)
    try:
        callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(buttons))
    except:
        try:
            callback_query.answer()
        except:
            pass

def channel_callback_handler(update, context):
    query = update.callback_query
    data = query.data
    user = query.from_user
    chat_id = query.message.chat_id
    message_id = query.message.message_id

    if data.startswith("adminpanel:"):
        handle_admin_panel_callback(query, context, data)
        return

    if data == "cancel":
        try:
            safe_edit_message(context.bot, chat_id, message_id, "لغو شد.")
        except:
            pass
        return

    if data.startswith("uf:"):
        try:
            _, action, fpath = data.split(":", 2)
        except Exception:
            query.answer()
            return
        if not os.path.exists(fpath):
            query.answer("فایل منقضی — دوباره بفرست", show_alert=True)
            return
        query.answer("در حال پردازش...")
        out_dir = os.path.dirname(fpath)
        base = os.path.splitext(os.path.basename(fpath))[0]
        try:
            if action == "audio":
                out = os.path.join(out_dir, base + "_a.mp3")
                ffmpeg_extract_audio(fpath, out)
                with open(out, "rb") as f:
                    context.bot.send_audio(chat_id=chat_id, audio=f, caption="🎧 صوت")
            elif action == "compress":
                out = os.path.join(out_dir, base + "_c.mp4")
                ffmpeg_compress(fpath, out)
                with open(out, "rb") as f:
                    context.bot.send_document(chat_id=chat_id, document=f, caption="🗜 فشرده")
            elif action == "mp4":
                out = os.path.join(out_dir, base + "_mp4.mp4")
                ffmpeg_to_mp4(fpath, out)
                with open(out, "rb") as f:
                    context.bot.send_document(chat_id=chat_id, document=f, caption="🎞 MP4")
            elif action == "gif":
                out = os.path.join(out_dir, base + ".gif")
                ffmpeg_gif(fpath, out)
                with open(out, "rb") as f:
                    context.bot.send_animation(chat_id=chat_id, animation=f, caption="GIF")
            elif action == "trim30":
                out = os.path.join(out_dir, base + "_t30.mp4")
                ffmpeg_trim(fpath, out)
                with open(out, "rb") as f:
                    context.bot.send_document(chat_id=chat_id, document=f, caption="✂ ۳۰ث")
            elif action == "frame":
                out = os.path.join(out_dir, base + "_f.jpg")
                ffmpeg_frame(fpath, out)
                with open(out, "rb") as f:
                    context.bot.send_photo(chat_id=chat_id, photo=f, caption="🖼")
            safe_edit_message(context.bot, chat_id, message_id, f"✅ {action}")
        except Exception as e:
            safe_edit_message(context.bot, chat_id, message_id, f"❗ {str(e)[:250]}")
        return

    # ضبط لایو با مدت مشخص
    if data.startswith("live:"):
        try:
            _, request_id, sec_s = data.split(":", 2)
            live_dur = int(sec_s)
        except Exception:
            query.answer()
            return
        with REQUESTS_LOCK:
            req = REQUESTS.get(request_id)
        if not req or req.get("user_id") != user.id:
            query.answer("نامعتبر")
            return
        enqueue_task({
            "user_id": user.id, "username": user.username or str(user.id),
            "url": req.get("url"), "chat_id": chat_id, "message_id": message_id,
            "action": "live", "format_id": "best", "mode": "telegram",
            "request_id": request_id, "request_info": req.get("info"),
            "live_duration": live_dur,
        })
        safe_edit_message(context.bot, chat_id, message_id, f"🔴 ضبط لایو ({live_dur // 60} دقیقه) به صف اضافه شد.")
        query.answer("به صف اضافه شد")
        return

    if data.startswith("sublangmenu:"):
        try:
            _, request_id, format_id = data.split(":", 2)
        except Exception:
            query.answer()
            return
        with REQUESTS_LOCK:
            req = REQUESTS.get(request_id)
        if not req or req.get("user_id") != user.id:
            query.answer("نامعتبر")
            return
        safe_edit_message(
            context.bot, chat_id, message_id,
            "زبان زیرنویس را انتخاب کن:",
            reply_markup=make_sublang_keyboard(request_id, format_id),
        )
        query.answer()
        return

    if data.startswith("crfmenu:"):
        try:
            _, request_id, format_id = data.split(":", 2)
        except Exception:
            query.answer()
            return
        with REQUESTS_LOCK:
            req = REQUESTS.get(request_id)
        if not req or req.get("user_id") != user.id:
            query.answer("نامعتبر")
            return
        safe_edit_message(
            context.bot, chat_id, message_id,
            "سطح فشرده‌سازی (CRF) را انتخاب کن:",
            reply_markup=make_compress_keyboard(request_id, format_id),
        )
        query.answer()
        return

    # انتخاب زبان زیرنویس
    if data.startswith("sublang:"):
        try:
            parts = data.split(":")
            request_id, format_id, lang = parts[1], parts[2], parts[3]
        except Exception:
            query.answer()
            return
        with REQUESTS_LOCK:
            req = REQUESTS.get(request_id)
        if not req or req.get("user_id") != user.id:
            query.answer("نامعتبر")
            return
        with_subs = lang != "off"
        if with_subs:
            try:
                prefs = load_user_prefs(user.id)
                prefs["subtitle_langs"] = lang
                prefs["want_subtitles"] = True
                save_user_prefs(user.id, prefs)
            except Exception:
                pass
        try:
            safe_edit_message(
                context.bot, chat_id, message_id,
                f"زبان زیرنویس: {lang if with_subs else 'خاموش'}\nمقصد را انتخاب کن:",
                reply_markup=make_output_mode_keyboard(request_id, format_id, with_subs=with_subs),
            )
        except Exception:
            pass
        query.answer()
        return

    # فشرده‌سازی با CRF دستی
    if data.startswith("crf:"):
        try:
            _, request_id, format_id, crf_s = data.split(":", 3)
            crf = int(crf_s)
        except Exception:
            query.answer()
            return
        with REQUESTS_LOCK:
            req = REQUESTS.get(request_id)
        if not req or req.get("user_id") != user.id:
            query.answer("نامعتبر")
            return
        enqueue_task({
            "user_id": user.id, "username": user.username or str(user.id),
            "url": req.get("url"), "chat_id": chat_id, "message_id": message_id,
            "action": "video", "format_id": format_id, "mode": "telegram_compress",
            "request_id": request_id, "request_info": req.get("info"),
            "compress_crf": crf,
        })
        safe_edit_message(context.bot, chat_id, message_id, f"🗜 فشرده‌سازی CRF{crf} به صف اضافه شد.")
        query.answer("به صف اضافه شد")
        return

    if data.startswith("ai:"):
        try:
            parts = data.split(":")
            ai_action = parts[1]
            request_id = parts[2] if len(parts) > 2 else None
        except Exception:
            query.answer()
            return
        with REQUESTS_LOCK:
            req = REQUESTS.get(request_id) if request_id else None
        if not req or req.get("user_id") != user.id:
            query.answer("درخواست نامعتبر یا منقضی")
            return
        info = req.get("info") or {}
        url = req.get("url") or ""
        if ai_action == "menu":
            ds = "🟢 DeepSeek فعال" if DEEPSEEK_API_KEY else "🟡 بدون کلید — حالت رایگان"
            safe_edit_message(
                context.bot, chat_id, message_id,
                f"✨ منوی AI\n{ds}\n\nکپشن • خلاصه • چندسبک • پست\nهشتگ • عنوان • تحلیل • ترجمه\nفصل • تامبنیل",
                reply_markup=make_ai_keyboard(request_id),
            )
            query.answer()
            return

        def _ai_show(label, producer):
            try:
                query.answer(f"{label}...")
                text = producer()
                with _DEEPSEEK_BILLING_LOCK:
                    bill = _DEEPSEEK_BILLING_MSG
                if bill:
                    text = bill + "\n\n────────\n" + text
                safe_edit_message(context.bot, chat_id, message_id, text[:4000], reply_markup=make_ai_keyboard(request_id))
            except DeepSeekBillingError as be:
                safe_edit_message(context.bot, chat_id, message_id, str(be), reply_markup=make_ai_keyboard(request_id))
            except Exception as e:
                safe_edit_message(context.bot, chat_id, message_id, f"❗ {str(e)[:150]}", reply_markup=make_ai_keyboard(request_id))

        if ai_action == "caption":
            _ai_show("کپشن", lambda: "✨ کپشن:\n\n" + generate_smart_caption(info, url=url))
            return
        if ai_action == "summary":
            _ai_show("خلاصه", lambda: generate_video_summary(info))
            return
        if ai_action == "styles":
            _ai_show("چند سبک", lambda: ai_generate_styles(info, url=url))
            return
        if ai_action == "post":
            _ai_show("پست", lambda: ai_generate_post(info, url=url))
            return
        if ai_action == "tags":
            _ai_show("هشتگ", lambda: ai_generate_hashtags(info, url=url))
            return
        if ai_action == "title":
            _ai_show("عنوان", lambda: ai_improve_title(info, url=url))
            return
        if ai_action == "analyze":
            _ai_show("تحلیل", lambda: ai_content_analysis(info, url=url))
            return
        if ai_action == "tr_en":
            _ai_show("ترجمه", lambda: ai_translate_caption(info, url=url))
            return
        if ai_action == "chapters":
            chapters = extract_youtube_chapters(info)
            if not chapters:
                query.answer("این ویدیو فصل ندارد", show_alert=True)
                return
            text = f"📑 {len(chapters)} فصل:\n\n"
            for i, ch in enumerate(chapters[:15], 1):
                m, s = divmod(int(ch["start"]), 60)
                h, m = divmod(m, 60)
                tstr = f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"
                text += f"{i}. {tstr} — {ch['title']}\n"
            safe_edit_message(context.bot, chat_id, message_id, text, reply_markup=make_chapters_keyboard(request_id, chapters))
            query.answer()
            return
        if ai_action == "thumb":
            thumb = info.get("thumbnail")
            if not thumb:
                query.answer("تامبنیل پیدا نشد", show_alert=True)
                return
            try:
                context.bot.send_photo(chat_id=chat_id, photo=thumb, caption="🖼 تامبنیل ویدیو")
                query.answer("ارسال شد")
            except Exception as e:
                query.answer(str(e)[:80], show_alert=True)
            return
        if ai_action == "back":
            parsed = req.get("formats") or []
            kb = make_quality_keyboard(parsed, request_id)
            try:
                safe_edit_message(context.bot, chat_id, message_id, "کیفیت مورد نظر را انتخاب کن:", reply_markup=kb)
            except Exception:
                try:
                    query.edit_message_reply_markup(reply_markup=kb)
                except Exception:
                    pass
            query.answer()
            return
        query.answer()
        return

    if data.startswith("chapter:"):
        try:
            _, request_id, idx_str = data.split(":", 2)
            idx = int(idx_str)
        except Exception:
            query.answer()
            return
        with REQUESTS_LOCK:
            req = REQUESTS.get(request_id)
        if not req or req.get("user_id") != user.id:
            query.answer("نامعتبر")
            return
        chapters = extract_youtube_chapters(req.get("info") or {})
        if idx < 0 or idx >= len(chapters):
            query.answer("فصل نامعتبر")
            return
        ch = chapters[idx]
        enqueue_task({
            "user_id": user.id, "username": user.username or str(user.id),
            "url": req.get("url"), "chat_id": chat_id, "message_id": message_id,
            "action": "video", "format_id": "best", "mode": "telegram",
            "request_id": request_id, "request_info": req.get("info"),
            "trim_start": ch["start"], "trim_end": None,
        })
        safe_edit_message(context.bot, chat_id, message_id, f"✅ فصل «{ch['title']}» به صف اضافه شد")
        query.answer("به صف اضافه شد")
        return

    if data.startswith("cancel_req:"):
        try:
            _, request_id, owner_id = data.split(":", 2)
            owner_id = int(owner_id)
        except:
            query.answer()
            return
        if user.id != owner_id:
            query.answer("فقط صاحب درخواست می‌تواند آن را لغو کند.")
            return
        with REQUESTS_LOCK:
            req = REQUESTS.get(request_id)
            if req:
                req["cancel"] = True
                req["error"] = "درخواست توسط کاربر لغو شد."
                REQUESTS[request_id] = req
        # propagate to active tasks
        with CANCEL_LOCK:
            for t_id, entry in list(CANCEL_FLAGS.items()):
                if entry.get("owner_id") == owner_id:
                    CANCEL_FLAGS[t_id]["cancel"] = True
        try:
            query.edit_message_text("❌ درخواست لغو شد.")
        except:
            pass
        append_user_log(get_log_key_for_user(owner_id), {"event": "cancel_requested", "request_id": request_id})
        query.answer("درخواست لغو شد")
        return

    if data.startswith("chan:videos:"):
        _, _, req_id = data.split(":", 2)
        with REQUESTS_LOCK:
            req = REQUESTS.get(req_id)
        if not req:
            safe_edit_message(context.bot, chat_id, message_id, "زمان درخواست منقضی شده.")
            return
        if not req.get("items"):
            try:
                channel_url = req.get("channel_url")
                videos = fetch_channel_videos(channel_url, max_results=200)
                req["items"] = videos
                with REQUESTS_LOCK:
                    REQUESTS[req_id] = req
            except Exception as e:
                safe_edit_message(context.bot, chat_id, message_id, f"خطا در دریافت ویدیوها: {str(e)}")
                append_user_log(get_log_key_for_user(user.id), {"event": "channel_videos_error", "error": str(e), "req_id": req_id})
                return
        send_channel_videos_page_by_req(req_id, 0, context.bot, query)
        return

    if data.startswith("chan:playlists:"):
        _, _, req_id = data.split(":", 2)
        with REQUESTS_LOCK:
            req = REQUESTS.get(req_id)
        if not req:
            safe_edit_message(context.bot, chat_id, message_id, "زمان درخواست منقضی شده.")
            return
        try:
            channel_url = req.get("channel_url")
            playlists = fetch_channel_playlists(channel_url)
            req["playlists"] = playlists
            with REQUESTS_LOCK:
                REQUESTS[req_id] = req
            buttons = []
            for pl in playlists[:20]:
                buttons.append([InlineKeyboardButton(pl.get("title")[:50], callback_data=f"playlist_queue:{pl.get('url')}")])
            try:
                query.edit_message_text("پلی‌لیست‌ها:", reply_markup=InlineKeyboardMarkup(buttons))
            except:
                pass
        except Exception as e:
            safe_edit_message(context.bot, chat_id, message_id, f"خطا در دریافت پلی‌لیست‌ها: {str(e)}")
            append_user_log(get_log_key_for_user(user.id), {"event": "playlists_error", "error": str(e), "req_id": req_id})
        return

    if data.startswith("playlist_queue:"):
        try:
            _, url = data.split(":", 1)
        except Exception:
            query.answer()
            return
        try:
            with yt_dlp.YoutubeDL({"quiet": True, "extract_flat": True}) as ydl:
                info = ydl.extract_info(url, download=False)
            entries = info.get("entries", []) or []
            videos = []
            for e in entries:
                vid = e.get("id")
                vurl = e.get("url") or vid
                if vurl and not vurl.startswith("http"):
                    vurl = f"https://www.youtube.com/watch?v={vurl}"
                videos.append(vurl)
            if not videos:
                safe_edit_message(context.bot, chat_id, message_id, "ویدیویی در پلی‌لیست پیدا نشد.")
                return
            req_id = uuid.uuid4().hex[:12]
            with REQUESTS_LOCK:
                REQUESTS[req_id] = {
                    "type": "playlist_pending",
                    "playlist_url": url,
                    "videos": videos,
                    "user_id": user.id,
                    "created": time.time(),
                    "cancel": False,
                }
            safe_edit_message(
                context.bot, chat_id, message_id,
                f"پلی‌لیست: {len(videos)} ویدیو\nکیفیت مورد نظر را انتخاب کن:",
            )
            try:
                query.edit_message_reply_markup(reply_markup=make_playlist_quality_keyboard(url, req_id))
            except Exception:
                context.bot.send_message(
                    chat_id=chat_id,
                    text=f"پلی‌لیست: {len(videos)} ویدیو — کیفیت را انتخاب کن:",
                    reply_markup=make_playlist_quality_keyboard(url, req_id),
                )
        except Exception as e:
            safe_edit_message(context.bot, chat_id, message_id, f"خطا در آماده‌سازی پلی‌لیست: {str(e)}")
            append_user_log(get_log_key_for_user(user.id), {"event": "playlist_queue_error", "playlist_url": url, "error": str(e)})
        return

    if data.startswith("plq:"):
        # plq:request_id:quality  →  بعد از انتخاب کیفیت، بازه را بپرس
        try:
            _, req_id, quality = data.split(":", 2)
        except Exception:
            query.answer()
            return
        with REQUESTS_LOCK:
            req = REQUESTS.get(req_id)
        if not req or req.get("user_id") != user.id:
            query.answer("درخواست نامعتبر یا منقضی")
            return
        videos = req.get("videos") or []
        total = len(videos)
        if total == 0:
            query.answer("ویدیویی نیست")
            return
        # اگر فقط ۱ ویدیو است مستقیم صف کن
        if total == 1:
            data = f"plrange:{req_id}:{quality}:all"
            # fall through handled below by re-setting — instead queue directly
            playlist_url = req.get("playlist_url") or ""
            if quality == "audio":
                action, format_id = "audio", "audio:mp3"
            elif quality in ("360", "480", "720", "1080"):
                action, format_id = "video", f"best[height<={quality}]/best"
            else:
                action, format_id = "video", "best"
            batch_id = create_batch(user.id, chat_id, 1, bot=context.bot)
            enqueue_task({
                "group_id": playlist_url,
                "batch_id": batch_id,
                "user_id": user.id,
                "username": user.username or str(user.id),
                "url": videos[0],
                "chat_id": chat_id,
                "message_id": message_id,
                "action": action,
                "format_id": format_id,
                "mode": "telegram",
                "request_id": None,
                "request_info": None,
            })
            safe_edit_message(context.bot, chat_id, message_id, "✅ ۱ ویدیو به صف اضافه شد.")
            query.answer("به صف اضافه شد")
            return
        safe_edit_message(
            context.bot, chat_id, message_id,
            f"پلی‌لیست: {total} ویدیو | کیفیت: {quality}\nبازه مورد نظر را انتخاب کن:",
            reply_markup=make_playlist_range_keyboard(req_id, quality, total),
        )
        query.answer()
        return

    if data.startswith("plrange:"):
        # plrange:request_id:quality:all  یا  plrange:request_id:quality:1-10
        try:
            parts = data.split(":")
            # plrange, req_id, quality, range
            req_id = parts[1]
            quality = parts[2]
            range_spec = parts[3] if len(parts) > 3 else "all"
        except Exception:
            query.answer()
            return
        with REQUESTS_LOCK:
            req = REQUESTS.get(req_id)
        if not req or req.get("user_id") != user.id:
            query.answer("درخواست نامعتبر یا منقضی")
            return
        videos = req.get("videos") or []
        playlist_url = req.get("playlist_url") or ""
        total = len(videos)
        if range_spec == "all":
            selected = list(videos)
            range_label = f"همه ({total})"
        else:
            try:
                a, b = range_spec.split("-", 1)
                start_i = max(1, int(a))
                end_i = min(total, int(b))
                selected = videos[start_i - 1:end_i]
                range_label = f"{start_i}–{end_i}"
            except Exception:
                selected = list(videos)
                range_label = "همه"
        if not selected:
            query.answer("بازه خالی است")
            return
        if quality == "audio":
            action, format_id = "audio", "audio:mp3"
        elif quality in ("360", "480", "720", "1080"):
            action, format_id = "video", f"best[height<={quality}]/best"
        else:
            action, format_id = "video", "best"
        batch_id = create_batch(user.id, chat_id, len(selected), bot=context.bot)
        for v in selected:
            enqueue_task({
                "group_id": playlist_url,
                "batch_id": batch_id,
                "user_id": user.id,
                "username": user.username or str(user.id),
                "url": v,
                "chat_id": chat_id,
                "message_id": message_id,
                "action": action,
                "format_id": format_id,
                "mode": "telegram",
                "request_id": None,
                "request_info": None,
            })
        append_user_log(get_log_key_for_user(user.id), {
            "event": "playlist_queued", "playlist_url": playlist_url,
            "num_videos": len(selected), "quality": quality, "range": range_label, "batch_id": batch_id,
        })
        safe_edit_message(
            context.bot, chat_id, message_id,
            f"✅ {len(selected)} ویدیو (بازه: {range_label} | کیفیت: {quality}) به صف اضافه شد.",
        )
        query.answer("به صف اضافه شد")
        return

    if data.startswith("cancel_dl:"):
        try:
            _, task_id, owner_id = data.split(":", 2)
            owner_id = int(owner_id)
        except:
            query.answer()
            return
        if user.id != owner_id:
            query.answer("فقط کاربری که دانلود را شروع کرده می‌تواند آن را لغو کند.")
            return
        with CANCEL_LOCK:
            if task_id in CANCEL_FLAGS:
                CANCEL_FLAGS[task_id]["cancel"] = True
        try:
            safe_edit_message(context.bot, chat_id, message_id, "❌ درخواست لغو ارسال شد. در حال متوقف کردن دانلود/آپلود...")
        except:
            pass
        append_user_log(get_log_key_for_user(owner_id), {"event": "cancel_requested", "task_id": task_id})
        query.answer("درخواست لغو ارسال شد")
        return

    if data.startswith("cat:"):
        try:
            _, request_id, category, page = data.split(":", 3)
            page = int(page)
        except Exception:
            query.answer()
            return
        with REQUESTS_LOCK:
            req = REQUESTS.get(request_id)
        if not req:
            query.answer("زمان درخواست منقضی شده؛ لطفاً لینک را دوباره ارسال کن.")
            return
        parsed = req.get("formats", [])
        multi_mode = bool(req.get("multi_mode"))
        selected = set(req.get("selected_formats") or [])
        kb = make_quality_keyboard(parsed, request_id, category=category, page=page, multi_mode=multi_mode, selected=selected)
        try:
            query.edit_message_reply_markup(reply_markup=kb)
        except Exception:
            try:
                query.answer()
            except Exception:
                pass
        return

    if data.startswith("multitoggle:"):
        # multitoggle:request_id:0|1
        try:
            _, request_id, flag = data.split(":", 2)
            enable = flag == "1"
        except Exception:
            query.answer()
            return
        with REQUESTS_LOCK:
            req = REQUESTS.get(request_id)
            if not req or req.get("user_id") != user.id:
                query.answer("نامعتبر")
                return
            req["multi_mode"] = enable
            if not enable:
                req["selected_formats"] = []
            REQUESTS[request_id] = req
            parsed = req.get("formats") or []
            selected = set(req.get("selected_formats") or [])
        kb = make_quality_keyboard(parsed, request_id, multi_mode=enable, selected=selected)
        try:
            query.edit_message_reply_markup(reply_markup=kb)
        except Exception:
            pass
        query.answer("حالت چندانتخابی %s" % ("فعال" if enable else "خاموش"))
        return

    if data.startswith("multisel:"):
        # multisel:request_id:format_id
        try:
            _, request_id, fid = data.split(":", 2)
        except Exception:
            query.answer()
            return
        with REQUESTS_LOCK:
            req = REQUESTS.get(request_id)
            if not req or req.get("user_id") != user.id:
                query.answer("نامعتبر")
                return
            selected = set(req.get("selected_formats") or [])
            if fid in selected:
                selected.discard(fid)
            else:
                selected.add(fid)
            req["selected_formats"] = list(selected)
            req["multi_mode"] = True
            REQUESTS[request_id] = req
            parsed = req.get("formats") or []
        kb = make_quality_keyboard(parsed, request_id, multi_mode=True, selected=selected)
        try:
            query.edit_message_reply_markup(reply_markup=kb)
        except Exception:
            pass
        query.answer("%d انتخاب شده" % len(selected))
        return

    if data.startswith("multidl:"):
        # multidl:request_id  → دانلود همه کیفیت‌های انتخاب‌شده
        try:
            _, request_id = data.split(":", 1)
        except Exception:
            query.answer()
            return
        with REQUESTS_LOCK:
            req = REQUESTS.get(request_id)
        if not req or req.get("user_id") != user.id:
            query.answer("نامعتبر")
            return
        selected = list(req.get("selected_formats") or [])
        if not selected:
            query.answer("هیچ کیفیتی انتخاب نشده")
            return
        url = req.get("url")
        prefs = load_user_prefs(user.id)
        mode = prefs.get("default_mode") or "telegram"
        batch_id = create_batch(user.id, chat_id, len(selected), bot=context.bot)
        for fid in selected:
            action = "audio" if str(fid).startswith("audio") else "video"
            enqueue_task({
                "group_id": None,
                "batch_id": batch_id,
                "user_id": user.id,
                "username": user.username or str(user.id),
                "url": url,
                "chat_id": chat_id,
                "message_id": message_id,
                "action": action,
                "format_id": fid,
                "mode": mode,
                "want_subtitles": False,
                "request_id": request_id,
                "request_info": req.get("info"),
            })
        safe_edit_message(
            context.bot, chat_id, message_id,
            f"✅ {len(selected)} کیفیت مختلف به صف اضافه شد.",
        )
        query.answer("به صف اضافه شد")
        return

    if data.startswith("popular:"):
        # محبوب: نزدیک‌ترین به 720 و 1080 + صدا
        try:
            _, request_id = data.split(":", 1)
        except Exception:
            query.answer()
            return
        with REQUESTS_LOCK:
            req = REQUESTS.get(request_id)
        if not req or req.get("user_id") != user.id:
            query.answer("نامعتبر")
            return
        parsed = req.get("formats") or []
        url = req.get("url")
        prefs = load_user_prefs(user.id)
        mode = prefs.get("default_mode") or "telegram"

        def nearest_height(target):
            cands = [p for p in parsed if p.get("height") and not p.get("is_audio_only")]
            if not cands:
                return None
            cands.sort(key=lambda p: (abs((p.get("height") or 0) - target), -(p.get("height") or 0)))
            return cands[0].get("format_id")

        fids = []
        for h in (720, 1080):
            fid = nearest_height(h)
            if fid and fid not in fids:
                fids.append(fid)
        # صدا
        _ac = prefs.get('audio_format') or 'mp3'
        audio_fid = f"audio:{_ac}:192" if _ac == "mp3" else f"audio:{_ac}:0"
        fids.append(audio_fid)
        if not fids:
            query.answer("فرمتی پیدا نشد")
            return
        batch_id = create_batch(user.id, chat_id, len(fids), bot=context.bot)
        for fid in fids:
            action = "audio" if str(fid).startswith("audio") else "video"
            enqueue_task({
                "group_id": None,
                "batch_id": batch_id,
                "user_id": user.id,
                "username": user.username or str(user.id),
                "url": url,
                "chat_id": chat_id,
                "message_id": message_id,
                "action": action,
                "format_id": fid,
                "mode": mode,
                "want_subtitles": False,
                "request_id": request_id,
                "request_info": req.get("info"),
            })
        safe_edit_message(
            context.bot, chat_id, message_id,
            f"⭐ {len(fids)} کیفیت محبوب به صف اضافه شد (۷۲۰ / ۱۰۸۰ / صدا).",
        )
        query.answer("به صف اضافه شد")
        return

    if data.startswith("dl:"):
        # پشتیبانی از audio:mp3:320 و subs_only
        parts = data.split(":")
        if len(parts) < 3:
            query.answer()
            return
        request_id = parts[1]
        fmt = parts[2]
        with_subs = False
        audio_codec = None
        audio_bitrate = None
        if len(parts) >= 4:
            if parts[3] == "subs":
                with_subs = True
            elif fmt == "audio":
                audio_codec = parts[3]
                if len(parts) >= 5 and str(parts[4]).isdigit():
                    audio_bitrate = parts[4]
        with REQUESTS_LOCK:
            req = REQUESTS.get(request_id)
        if not req:
            safe_edit_message(context.bot, chat_id, message_id, "زمان درخواست منقضی شده؛ لطفاً لینک را دوباره ارسال کن.")
            return
        if req.get("user_id") != user.id:
            query.answer("فقط کاربری که لینک را ارسال کرده می‌تواند این گزینه را انتخاب کند.")
            return
        if req.get("cancel"):
            safe_edit_message(context.bot, chat_id, message_id, "این درخواست قبلاً لغو شده است.")
            return

        prefs = load_user_prefs(user.id)
        parsed = req.get("formats") or []

        if fmt == "best":
            format_id = "best"
        elif fmt == "auto":
            format_id = pick_auto_format(
                parsed,
                preferred_height=prefs.get("preferred_height", 720),
                max_size_mb=prefs.get("max_auto_size_mb", 50),
            )
        elif fmt == "audio":
            codec = audio_codec or prefs.get("audio_format") or "mp3"
            format_id = f"audio:{codec}:{audio_bitrate}" if audio_bitrate else f"audio:{codec}"
        elif fmt == "subs_only":
            format_id = "subs_only"
        else:
            format_id = fmt

        size_text = "—"
        label = format_id
        if isinstance(format_id, str) and format_id.startswith("audio"):
            pa = format_id.split(":")
            codec = pa[1] if len(pa) > 1 else "mp3"
            br = pa[2] if len(pa) > 2 else None
            label = f"فقط صدا ({codec.upper()} {br}kbps)" if br and br != "0" else f"فقط صدا ({codec.upper()})"
        elif fmt == "subs_only":
            label = "فقط زیرنویس"
            size_text = "کوچک"
        elif fmt == "auto":
            label = f"خودکار ({format_id})"
            for p in parsed:
                if str(p.get("format_id")) == str(format_id):
                    size_text = p.get("size_text") or human_size(p.get("size"))
                    label = f"خودکار • {p.get('label') or format_id}"
                    break
        elif format_id == "best":
            label = "بهترین کیفیت"
            sizes = [p.get("size") for p in parsed if p.get("size")]
            if sizes:
                size_text = human_size(max(sizes))
        else:
            for p in parsed:
                if str(p.get("format_id")) == str(format_id):
                    size_text = p.get("size_text") or human_size(p.get("size"))
                    label = p.get("label") or format_id
                    break

        # حالت سریع
        if prefs.get("quick_mode"):
            mode = prefs.get("default_mode") or "telegram"
            if isinstance(format_id, str) and format_id.startswith("audio"):
                action = "audio"
            elif format_id == "subs_only":
                action = "subs"
            else:
                action = "video"
            enqueue_task({
                "group_id": None,
                "user_id": user.id,
                "username": user.username or str(user.id),
                "url": req["url"],
                "chat_id": chat_id,
                "message_id": message_id,
                "action": action,
                "format_id": format_id,
                "mode": mode,
                "want_subtitles": with_subs or (format_id == "subs_only"),
                "request_id": request_id,
                "request_info": req.get("info"),
            })
            try:
                qsize = download_queue.qsize()
                eta_txt = format_eta(estimate_queue_wait_seconds(qsize))
                safe_edit_message(context.bot, chat_id, message_id, f"⚡ حالت سریع — به صف اضافه شد (موقعیت: {qsize} | شروع تقریبی: {eta_txt})")
            except Exception:
                pass
            query.answer("به صف اضافه شد")
            return

        size_bytes = None
        try:
            for p in parsed:
                if str(p.get("format_id")) == str(format_id):
                    size_bytes = p.get("size")
                    break
            if format_id == "best":
                sizes = [p.get("size") for p in parsed if p.get("size")]
                if sizes:
                    size_bytes = max(sizes)
        except Exception:
            size_bytes = None
        large_warn = False
        try:
            if size_bytes and int(size_bytes) >= LARGE_FILE_WARN_MB * 1024 * 1024:
                large_warn = True
        except Exception:
            pass
        confirm_text = (
            f"📋 تأیید دانلود\n"
            f"🎞 کیفیت: {label}\n"
            f"📦 حجم تقریبی: {size_text}\n"
            f"📝 زیرنویس: {'بله' if with_subs else 'خیر'}\n"
        )
        if large_warn:
            confirm_text += f"\n⚠️ این فایل بزرگ‌تر از {LARGE_FILE_WARN_MB}MB است.\n"
        confirm_text += "\nادامه می‌دی؟"
        try:
            safe_edit_message(context.bot, chat_id, message_id, confirm_text, reply_markup=make_confirm_keyboard(request_id, format_id, with_subs=with_subs, large_warn=large_warn))
        except Exception:
            try:
                query.answer()
            except Exception:
                pass
        return

    if data.startswith("confirm:"):
        # confirm:request_id:format_id:sub_flag
        # پشتیبانی از audio:mp3 / audio:mp3:320 / subs_only
        try:
            rest = data[len("confirm:"):]
            parts = rest.split(":")
            if len(parts) >= 4 and parts[1] == "audio" and parts[2] in ("mp3", "m4a", "opus", "flac", "wav", "ogg"):
                request_id = parts[0]
                if len(parts) >= 5 and str(parts[3]).isdigit():
                    format_id = f"audio:{parts[2]}:{parts[3]}"
                    sub_flag = parts[4]
                else:
                    format_id = f"audio:{parts[2]}"
                    sub_flag = parts[3]
            elif len(parts) >= 3 and parts[1] == "subs_only":
                request_id = parts[0]
                format_id = "subs_only"
                sub_flag = parts[2]
            else:
                request_id = parts[0]
                format_id = parts[1]
                sub_flag = parts[2] if len(parts) > 2 else "0"
            with_subs = sub_flag == "1"
        except Exception:
            query.answer()
            return
        with REQUESTS_LOCK:
            req = REQUESTS.get(request_id)
        if not req or req.get("user_id") != user.id:
            query.answer("درخواست نامعتبر")
            return
        try:
            safe_edit_message(
                context.bot, chat_id, message_id,
                "می‌خوای فایل چطور تحویل داده بشه؟",
                reply_markup=make_output_mode_keyboard(request_id, format_id, with_subs=with_subs)
            )
        except:
            pass
        return

    if data.startswith("mode:"):
        # mode:request_id:fmt:mode[:sub_flag]
        # پشتیبانی از audio:mp3 / audio:mp3:320 / subs_only
        rest = data[len("mode:"):]
        parts = rest.split(":")
        if len(parts) < 3:
            query.answer()
            return
        request_id = parts[0]
        if parts[1] == "audio" and len(parts) >= 4 and parts[2] in ("mp3", "m4a", "opus", "flac", "wav", "ogg"):
            if len(parts) >= 5 and str(parts[3]).isdigit():
                fmt = f"audio:{parts[2]}:{parts[3]}"
                mode = parts[4]
                with_subs = (len(parts) >= 6 and parts[5] == "1")
            else:
                fmt = f"audio:{parts[2]}"
                mode = parts[3]
                with_subs = (len(parts) >= 5 and parts[4] == "1")
        elif parts[1] == "subs_only":
            fmt = "subs_only"
            mode = parts[2]
            with_subs = True
        else:
            fmt = parts[1]
            mode = parts[2]
            with_subs = (len(parts) >= 4 and parts[3] == "1")
        with REQUESTS_LOCK:
            req = REQUESTS.get(request_id)
        if not req:
            safe_edit_message(context.bot, chat_id, message_id, "زمان درخواست منقضی شده؛ لطفاً لینک را دوباره ارسال کن.")
            return
        if req.get("user_id") != user.id:
            query.answer("فقط کاربری که لینک را ارسال کرده می‌تواند این گزینه را انتخاب کند.")
            return
        if req.get("cancel"):
            safe_edit_message(context.bot, chat_id, message_id, "این درخواست قبلاً لغو شده است.")
            return
        url = req["url"]
        if isinstance(fmt, str) and (fmt == "audio" or fmt.startswith("audio:")):
            action = "audio"
            format_id = fmt if fmt.startswith("audio:") else "audio:mp3"
        elif fmt == "subs_only":
            action = "subs"
            format_id = "subs_only"
        else:
            action = "video"
            format_id = fmt if fmt != "best" else "best"
        # ذخیره ترجیح کاربر
        try:
            prefs = load_user_prefs(user.id)
            base_mode = mode if mode in ("local", "drive", "mirror", "link_only", "templink") else "telegram"
            prefs["default_mode"] = base_mode
            prefs["default_quality"] = format_id
            prefs["want_subtitles"] = with_subs or (fmt == "subs_only")
            if action == "audio" and ":" in format_id:
                # audio:mp3:320 → codec = mp3
                prefs["audio_format"] = format_id.split(":")[1]
            save_user_prefs(user.id, prefs)
        except Exception:
            pass
        enqueue_task({
            "group_id": None,
            "user_id": user.id,
            "username": user.username or str(user.id),
            "url": url,
            "chat_id": chat_id,
            "message_id": message_id,
            "action": action,
            "format_id": format_id,
            "mode": mode,
            "want_subtitles": with_subs or (fmt == "subs_only"),
            "request_id": request_id,
            "request_info": req.get("info")
        })
        append_user_log(get_log_key_for_user(user.id), {"event": "download_request", "url": url, "format_requested": format_id or "best", "mode": mode, "request_id": request_id})
        try:
            qsize = download_queue.qsize()
            eta_txt = format_eta(estimate_queue_wait_seconds(qsize))
            quick_kb = InlineKeyboardMarkup([[InlineKeyboardButton("⚡ همیشه با همین تنظیمات دانلود کن", callback_data=f"enablequick:{user.id}")]])
            safe_edit_message(
                context.bot, chat_id, message_id,
                f"✅ به صف اضافه شد (موقعیت تقریبی صف: {qsize} | شروع تقریبی: {eta_txt})",
                reply_markup=quick_kb,
            )
        except:
            pass
        query.answer("به صف اضافه شد")
        return

    if data.startswith("enablequick:"):
        try:
            _, uid_str = data.split(":", 1)
            uid = int(uid_str)
        except Exception:
            query.answer()
            return
        if uid != user.id:
            query.answer("این گزینه برای شما نیست.")
            return
        try:
            prefs = load_user_prefs(user.id)
            prefs["quick_mode"] = True
            save_user_prefs(user.id, prefs)
            safe_edit_message(
                context.bot, chat_id, message_id,
                "⚡ حالت سریع فعال شد؛ از این به بعد با همین تنظیمات (کیفیت و مقصد) مستقیم به صف اضافه می‌شود.\nبرای خاموش کردن: /quick"
            )
        except Exception:
            pass
        query.answer("⚡ فعال شد")
        return

    if data.startswith("dl_direct:"):
        try:
            _, vurl = data.split(":", 1)
        except:
            query.answer()
            return
        enqueue_task({
            "group_id": None,
            "user_id": user.id,
            "username": user.username or str(user.id),
            "url": vurl,
            "chat_id": chat_id,
            "message_id": message_id,
            "action": "video",
            "format_id": "best",
            "mode": "telegram",
            "request_id": None,
            "request_info": None
        })
        append_user_log(get_log_key_for_user(user.id), {"event": "download_request_direct", "url": vurl})
        try:
            safe_edit_message(context.bot, chat_id, message_id, "✅ درخواست دانلود مستقیم به صف اضافه شد.")
        except:
            pass
        query.answer("به صف اضافه شد")
        return

    if data.startswith("noop:"):
        try:
            query.answer()
        except:
            pass
        return

    query.answer()

# -------------------------
# main with resilient polling
# -------------------------
def start_workers(bot, n=4):
    for _ in range(n):
        t = threading.Thread(target=download_worker_thread, args=(bot,), daemon=True)
        t.start()

def get_log_key_for_user(user_id):
    with USER_MAP_LOCK:
        uname = USER_MAP.get(user_id)
    return uname if uname else user_id


def inline_query_handler(update, context):
    """حالت Inline: @bot <url>"""
    if not ENABLE_INLINE:
        return
    q = update.inline_query
    query = (q.query or "").strip()
    results = []
    if query.startswith("http"):
        results.append(
            InlineQueryResultArticle(
                id=uuid.uuid4().hex[:8],
                title="دانلود این لینک",
                description=query[:80],
                input_message_content=InputTextMessageContent(
                    "🔗 درخواست دانلود:\n%s\n\n(لینک را در چت خصوصی ربات هم بفرست تا کیفیت انتخاب شود)" % query[:300]
                ),
            )
        )
    else:
        results.append(
            InlineQueryResultArticle(
                id="help",
                title="لینک را بعد از @bot بفرست",
                description="مثال: @YourBot https://...",
                input_message_content=InputTextMessageContent(
                    "برای دانلود، لینک را به صورت خصوصی برای ربات بفرست یا از Inline استفاده کن:\n@Bot لینک"
                ),
            )
        )
    try:
        q.answer(results, cache_time=5, is_personal=True)
    except Exception as e:
        logger.warning("inline answer failed: %s", e)


def handle_admin_panel_callback(query, context, data):
    """دکمه‌های پنل ادمین"""
    global MAINTENANCE_MODE, WHITELIST_ENABLED
    user = query.from_user
    if not is_admin(user.id):
        query.answer("فقط ادمین", show_alert=True)
        return
    action = data.split(":", 1)[-1]
    bot = context.bot
    chat_id = query.message.chat_id
    if action == "stats":
        with GLOBAL_STATS_LOCK:
            s = dict(GLOBAL_STATS)
        with active_workers_lock:
            aw = active_workers
        text = (
            "📊 آمار\\n"
            "ورکر: %d | صف: %d\\n"
            "موفق: %s | ناموفق: %s\\n"
            "حجم آپلود: %s\\n"
            "میانگین کار: %.1fs"
            % (
                aw, download_queue.qsize(),
                s.get("downloads_ok", 0), s.get("downloads_fail", 0),
                human_size(s.get("bytes_uploaded", 0)),
                float(s.get("avg_task_seconds") or 0),
            )
        )
        safe_edit_message(bot, chat_id, query.message.message_id, text, reply_markup=make_admin_panel_keyboard())
    elif action == "queue":
        with QUEUE_META_LOCK:
            items = list(QUEUE_META.items())[:15]
        lines = ["📋 صف (%d):" % download_queue.qsize()]
        for tid, meta in items:
            lines.append("• %s | %s" % (meta.get("status"), (meta.get("url") or "")[:40]))
        if not items:
            lines.append("(خالی)")
        safe_edit_message(bot, chat_id, query.message.message_id, "\\n".join(lines), reply_markup=make_admin_panel_keyboard())
    elif action == "maint_on":
        with MAINTENANCE_LOCK:
            MAINTENANCE_MODE = True
        query.answer("تعمیرات ON")
        safe_edit_message(bot, chat_id, query.message.message_id, "🛠 تعمیرات فعال شد", reply_markup=make_admin_panel_keyboard())
    elif action == "maint_off":
        with MAINTENANCE_LOCK:
            MAINTENANCE_MODE = False
        query.answer("تعمیرات OFF")
        safe_edit_message(bot, chat_id, query.message.message_id, "✅ تعمیرات خاموش شد", reply_markup=make_admin_panel_keyboard())
    elif action == "wl_on":
        WHITELIST_ENABLED = True
        query.answer("Whitelist ON")
    elif action == "wl_off":
        WHITELIST_ENABLED = False
        query.answer("Whitelist OFF")
    elif action == "update_ytdlp":
        query.answer("در حال آپدیت...")
        try:
            import subprocess
            r = subprocess.run(
                [os.environ.get("PYTHON", "python"), "-m", "pip", "install", "-U", "yt-dlp"],
                capture_output=True, text=True, timeout=180,
            )
            tail = ((r.stdout or "") + (r.stderr or ""))[-400:]
            bot.send_message(chat_id=chat_id, text="yt-dlp:\\n" + tail)
        except Exception as e:
            bot.send_message(chat_id=chat_id, text="خطا: %s" % e)
    elif action == "backup":
        query.answer("بکاپ...")
        try:
            # reuse admin backup logic if exists
            bot.send_message(chat_id=chat_id, text="از /admin backup استفاده کن")
        except Exception:
            pass
    else:
        query.answer()


def main():
    updater = Updater(TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("help", help_cmd))
    dp.add_handler(CommandHandler("channel", handle_channel_cmd))
    dp.add_handler(CommandHandler("search", search_cmd))
    dp.add_handler(CommandHandler("status", status_cmd))
    dp.add_handler(CommandHandler("dashboard", dashboard_cmd))
    dp.add_handler(CommandHandler("queue", queue_cmd))
    dp.add_handler(CommandHandler("history", history_cmd))
    dp.add_handler(CommandHandler("quick", quick_cmd))
    dp.add_handler(CommandHandler("stats", stats_cmd))
    dp.add_handler(CommandHandler("mystats", mystats_cmd))
    dp.add_handler(CommandHandler("drivestatus", drivestatus_cmd))
    dp.add_handler(CommandHandler("set_rate", set_rate_cmd))
    dp.add_handler(CommandHandler("errors", errors_cmd))
    dp.add_handler(CommandHandler("admin", admin_cmd))
    dp.add_handler(CommandHandler("cookies", cookies_cmd))
    dp.add_handler(CommandHandler("clear_cookies", clear_cookies_cmd))
    dp.add_handler(CommandHandler("fromdrive", fromdrive_cmd))
    dp.add_handler(CommandHandler("prefs", prefs_cmd))
    dp.add_handler(CommandHandler("set_subs", set_subs_cmd))
    dp.add_handler(CommandHandler("set_quality", set_quality_cmd))
    dp.add_handler(CommandHandler("set_caption", set_caption_cmd))
    dp.add_handler(CommandHandler("set_forward", set_forward_cmd))
    dp.add_handler(CommandHandler("autoforward", autoforward_cmd))
    dp.add_handler(CommandHandler("smartcompress", smartcompress_cmd))
    dp.add_handler(CommandHandler("fetch", fetch_cmd))
    dp.add_handler(CommandHandler("clip", clip_cmd))
    dp.add_handler(CommandHandler("gif", gif_cmd))
    dp.add_handler(CommandHandler("story", story_cmd))
    dp.add_handler(CommandHandler("points", points_cmd))
    dp.add_handler(CommandHandler("translatesubs", translatesubs_cmd))
    dp.add_handler(CommandHandler("summarize", summarize_cmd))
    dp.add_handler(CommandHandler("ask", ask_cmd))
    dp.add_handler(CommandHandler("logo", logo_cmd))
    dp.add_handler(CommandHandler("merge", merge_cmd))
    dp.add_handler(CommandHandler("trim", trim_cmd))
    dp.add_handler(CommandHandler("shot", shot_cmd))
    dp.add_handler(CommandHandler("watermark", watermark_cmd))
    dp.add_handler(CommandHandler("forcemp4", forcemp4_cmd))
    dp.add_handler(CommandHandler("burn", burn_cmd))
    dp.add_handler(CommandHandler("schedule", schedule_cmd))
    dp.add_handler(CommandHandler("watch", watch_cmd))
    dp.add_handler(CommandHandler("waitqueue", waitqueue_cmd))
    dp.add_handler(MessageHandler(Filters.document, handle_document))
    dp.add_handler(MessageHandler(Filters.video, handle_document))
    dp.add_handler(MessageHandler(Filters.photo, handle_document))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))
    dp.add_handler(CallbackQueryHandler(channel_callback_handler))
    if ENABLE_INLINE:
        dp.add_handler(InlineQueryHandler(inline_query_handler))

    start_workers(updater.bot, n=MAX_CONCURRENT_DOWNLOADS)
    start_cleanup_thread()
    start_scheduler_thread(updater.bot)
    start_channel_watch_thread(updater.bot)
    start_health_check_thread(updater.bot)
    start_daily_stats_thread(updater.bot)

    if TELETHON_API_ID and TELETHON_API_HASH:
        try:
            ensure_telethon_client()
        except Exception as e:
            logger.exception("Telethon init failed: %s", e)

    attempt = 0
    while True:
        try:
            if WEBHOOK_URL:
                updater.start_webhook(
                    listen=WEBHOOK_LISTEN,
                    port=WEBHOOK_PORT,
                    url_path=TOKEN,
                    webhook_url=WEBHOOK_URL.rstrip("/") + "/" + TOKEN,
                )
                logger.info("Bot started (webhook): %s", WEBHOOK_URL)
            else:
                updater.start_polling()
                logger.info("Bot started (polling)")
            if NOTIFY_ADMIN_ON_START:
                for aid in list(ADMIN_IDS):
                    try:
                        updater.bot.send_message(chat_id=aid, text="🟢 ربات آنلاین شد\n" + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    except Exception:
                        pass
            updater.idle()
            break
        except Exception as e:
            try:
                with open(LOG_ROOT / "polling_errors.log", "a", encoding="utf-8") as f:
                    f.write(f"{datetime.now().isoformat()} - Polling error: {str(e)}\n")
                    f.write(traceback.format_exc())
                    f.write("\n" + ("-" * 60) + "\n")
            except:
                pass

            is_network = False
            if isinstance(e, NetworkError):
                is_network = True
            elif Urllib3SSLError and isinstance(e, Urllib3SSLError):
                is_network = True
            elif isinstance(e, ssl.SSLError):
                is_network = True
            elif isinstance(e, requests.exceptions.RequestException):
                is_network = True

            attempt += 1
            if not is_network:
                logger.exception("Non-network error in polling, re-raising: %s", e)
                raise

            sleep_for = min(NETWORK_RETRY_SLEEP_MAX, (NETWORK_BACKOFF_BASE ** attempt))
            sleep_for = max(NETWORK_RETRY_SLEEP_MIN, sleep_for)
            logger.warning("Network error starting polling: %s. Retrying in %.1f s (attempt %d)...", e, sleep_for, attempt)
            time.sleep(sleep_for)
            continue

if __name__ == "__main__":
    main()
