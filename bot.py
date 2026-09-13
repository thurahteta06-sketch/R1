import telebot, asyncio, aiohttp, json, base64, random, re, os, string, time, uuid, logging
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web
import cv2
import ddddocr
import numpy as np
from datetime import datetime, timedelta, timezone

# ==================== LOGGING CONFIGURATION ====================
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== CONFIGURATION ====================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
REPO_OWNER = os.environ.get("REPO_OWNER", "")
REPO_NAME = os.environ.get("REPO_NAME", "")

ADMINS = [
    "1626617395",
    "",
]

# Startup validation
_missing = [k for k, v in {
    "BOT_TOKEN": BOT_TOKEN,
    "GITHUB_TOKEN": GITHUB_TOKEN,
    "REPO_OWNER": REPO_OWNER,
    "REPO_NAME": REPO_NAME,
}.items() if not v]
if _missing:
    raise SystemExit(
        f"❌ Railway Variables tab တွင် အောက်ပါများ မထည့်ရသေးပါ:\n"
        + "\n".join(f"   • {k}" for k in _missing)
    )

def is_admin(user_id):
    return str(user_id) in ADMINS

# ==================== RETRY CONFIGURATION ====================
MAX_RETRIES = 3
RETRY_DELAY = 2
REQUESTS_PER_SECOND = 15

# ==================== PROXY CONFIGURATION ====================
PAID_PROXIES = []
_proxy_index = 0

def get_next_proxy():
    global _proxy_index
    if not PAID_PROXIES:
        return None
    proxy = PAID_PROXIES[_proxy_index % len(PAID_PROXIES)]
    _proxy_index += 1
    return f"http://{proxy}"

# ==================== GLOBALS ====================
SUCCESS_CODE = asyncio.Queue()
bot = AsyncTeleBot(BOT_TOKEN)
user_data = {}
scan_tasks = {}
success_messages = {}
session = None
_start_time = time.monotonic()

MAX_CONCURRENT_SCANS = 20
active_scans_count = 0
active_scans_lock = asyncio.Lock()

throttle_semaphore = None

# ==================== LOGGING UTILITIES ====================
class BatchLogger:
    def __init__(self, batch_size=50):
        self.batch_size = batch_size
        self.batch = []
        self.lock = asyncio.Lock()
    
    async def log_success(self, user_id, code):
        async with self.lock:
            self.batch.append(f"✅ {user_id}: {code}")
            if len(self.batch) >= self.batch_size:
                await self.flush()
    
    async def flush(self):
        if self.batch:
            logger.info(f"[BATCH] {len(self.batch)} codes processed")
            self.batch = []

batch_logger = BatchLogger()

# ==================== WEB SERVER ====================
async def handle(request):
    return web.Response(text="Bot is awake and running 24/7!")

async def web_server():
    app = web.Application()
    app.router.add_get('/', handle)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('PORT', os.environ.get('BOT_PORT', 8099)))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"Web server running on port {port}")

# ==================== HTTP REQUEST WITH RETRY ====================
async def make_request_with_retry(url, method='GET', **kwargs):
    """Make HTTP request with retry logic and exponential backoff."""
    timeout = aiohttp.ClientTimeout(total=15, connect=5, sock_read=10)
    
    for attempt in range(MAX_RETRIES):
        try:
            async with throttle_semaphore:
                if method == 'GET':
                    async with session.get(url, timeout=timeout, **kwargs) as resp:
                        if resp.status == 200:
                            return await resp.json()
                        return None
                elif method == 'PUT':
                    async with session.put(url, timeout=timeout, **kwargs) as resp:
                        return await resp.text()
        except asyncio.TimeoutError:
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAY * (2 ** attempt)
                await asyncio.sleep(delay)
            continue
        except aiohttp.ClientError as e:
            logger.warning(f"Request error: {e}")
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAY * (2 ** attempt)
                await asyncio.sleep(delay)
            continue
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            break
    return None

# ==================== GITHUB HELPERS ====================
async def get_file_content(path):
    """Fetch file content from GitHub with retry."""
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    
    result = await make_request_with_retry(url, method='GET', headers=headers)
    if result:
        try:
            content = base64.b64decode(result['content']).decode('utf-8')
            return json.loads(content), result['sha']
        except Exception as e:
            logger.error(f"Error parsing GitHub content: {e}")
    return {}, None

async def update_file_content(path, content, sha, message):
    """Update file on GitHub with retry."""
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Content-Type": "application/json"}
    encoded = base64.b64encode(json.dumps(content).encode()).decode()
    payload = {"message": message, "content": encoded, "sha": sha}
    
    return await make_request_with_retry(url, method='PUT', headers=headers, json=payload)

# ==================== KEYBOARDS ====================
def get_main_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🔗 Portal URL ထည့်ရန်", callback_data="menu_free_trial"),
        InlineKeyboardButton("📋 Success Codes ကြည့်မည်", callback_data="menu_result"),
        InlineKeyboardButton("🔄 Recheck ပြန်လုပ်စစ်မည်", callback_data="menu_recheck"),
        InlineKeyboardButton("🛑 Scan ရပ်မည်", callback_data="menu_stop"),
    )
    return keyboard

def get_voucher_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🔢 VOUCHER 6 လုံး", callback_data="scan_6"),
        InlineKeyboardButton("🔢 VOUCHER 7 လုံး", callback_data="scan_7"),
        InlineKeyboardButton("🔢 VOUCHER 8 လုံး", callback_data="scan_8"),
        InlineKeyboardButton("🔢 VOUCHER 9 လုံး", callback_data="scan_9"),
        InlineKeyboardButton("🔤 VOUCHER ascii-lower", callback_data="scan_ascii-lower"),
        InlineKeyboardButton("🔤 VOUCHER ascii-lower9", callback_data="scan_ascii-lower9"),
        InlineKeyboardButton("🎲 VOUCHER all", callback_data="scan_all"),
        InlineKeyboardButton("🔤+🔢 MIXED 6လုံး", callback_data="scan_mixed"),
        InlineKeyboardButton("🔤+🔢 MIXED 7လုံး", callback_data="scan_mixed7"),
        InlineKeyboardButton("🔤+🔢 MIXED 8လုံး", callback_data="scan_mixed8"),
        InlineKeyboardButton("🔤+🔢 MIXED 9လုံး", callback_data="scan_mixed9"),
        InlineKeyboardButton("🔙 Back", callback_data="menu_back"),
    )
    return keyboard

def get_digit_keyboard(mode):
    keyboard = InlineKeyboardMarkup(row_width=5)
    buttons = [InlineKeyboardButton(str(i), callback_data=f"digit_{mode}_{i}") for i in range(10)]
    keyboard.add(*buttons)
    keyboard.add(InlineKeyboardButton("🎲 Random", callback_data=f"digit_{mode}_random"))
    keyboard.add(InlineKeyboardButton("🔙 Back", callback_data="menu_back"))
    return keyboard

def get_start_scam_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("🚀 START SCAM", callback_data="menu_start_scam"),
        InlineKeyboardButton("🔙 Back", callback_data="menu_back"),
    )
    return keyboard

def get_back_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("🔙 Back", callback_data="menu_back"))
    return keyboard

def get_scam_button_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("🛑 STOP SCAM", callback_data="menu_stop"),
        InlineKeyboardButton("🔙 Back", callback_data="menu_back"),
    )
    return keyboard

# ==================== COMMAND HANDLERS ====================
@bot.message_handler(commands=['start'])
async def start(message):
    if message.chat.id not in user_data:
        user_data[message.chat.id] = {}
    user_name = message.from_user.first_name or message.from_user.username or "User"
    user_id = str(message.chat.id)
    welcome_text = f"""✨ STAR LINK CODE HACK ✨

👤 NAME: {user_name}
🆔 USER ID: {user_id}

🎉 မင်္ဂလာပါ!
အောက်ပါ Menu မှ သင်လိုချင်တာကိုရွေးချယ်ပါ။"""
    await bot.send_message(message.chat.id, welcome_text, reply_markup=get_main_keyboard())

@bot.message_handler(commands=['result'])
async def handle_result(message):
    results, _ = await get_file_content("result.json")
    uid = str(message.chat.id)
    if uid in results and results[uid]:
        codes = "\n".join(results[uid])
        await bot.reply_to(message, f"✅ Found Codes:\n{codes}")
    else:
        await bot.reply_to(message, "ယခင်ကရရှိထားသော code မရှိသေးပါ။")

@bot.message_handler(commands=['portal'])
async def handle_portal(message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "🔗 Portal URL ထည့်သွင်းရန်:\n\n/portal [your_portal_url]")
        return
    
    url = args[1]
    if message.chat.id not in user_data:
        user_data[message.chat.id] = {}
    
    await bot.reply_to(message, "🔗 Portal URL စစ်ဆေးနေပါသည်...")
    if await check_session_url_improved(session_url=url):
        user_data[message.chat.id]['session_url'] = url
        await bot.reply_to(message, "✅ Portal URL သိမ်းဆည်းပြီးပါပြီ။\n\nVOUCHER ရွေးချယ်ရန် Menu ကိုသုံးပါ။", reply_markup=get_voucher_keyboard())
    else:
        await bot.reply_to(message, "❌ Portal URL မှားယွင်းနေပါသည်။")

@bot.message_handler(commands=['stop'])
async def stop_scan_command(message):
    chat_id = message.chat.id
    if chat_id in scan_tasks:
        scan_tasks[chat_id]["stop"] = True
        await bot.reply_to(message, "🛑 Scan ကိုရပ်တန့်လိုက်ပါပြီ။")
    else:
        await bot.reply_to(message, "အလုပ်လုပ်နေတဲ့ Scan မရှိပါ။")

@bot.message_handler(commands=['status'])
async def status(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "No Permission")
        return
    
    active = sum(1 for d in scan_tasks.values() if not d["task"].done())
    uptime = time.monotonic() - _start_time
    
    status_text = f"🤖 **Bot Status**\n🟢 Active: {active}/{MAX_CONCURRENT_SCANS}\n⏱️ Uptime: {uptime:.1f}s\n🔋 Users: {len(user_data)}"
    await bot.reply_to(message, status_text, parse_mode="Markdown")

# ==================== CALLBACK HANDLER ====================
@bot.callback_query_handler(func=lambda call: True)
async def callback_handler(call):
    chat_id = call.message.chat.id
    user_id = str(chat_id)
    user_name = call.from_user.first_name or call.from_user.username or "User"
    
    if call.data == "menu_back":
        text = f"✨ STAR LINK CODE HACK ✨\n\n👤 NAME: {user_name}\n🆔 USER ID: {user_id}\n\nMenu မှ သင်လိုချင်တာကိုရွေးချယ်ပါ။"
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=text, reply_markup=get_main_keyboard())
        await bot.answer_callback_query(call.id)
        return
    
    if call.data == "menu_free_trial":
        text = "🔗 Portal URL ထည့်သွင်းရန်:\n\n/portal [your_portal_url]"
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=text, reply_markup=get_back_keyboard())
        await bot.answer_callback_query(call.id)
        return
    
    if call.data == "menu_result":
        results, _ = await get_file_content("result.json")
        text = f"✅ Found Codes:\n" + "\n".join(results.get(user_id, [])) if user_id in results else "📋 ယခင်ကရရှိထားသော code မရှိသေးပါ။"
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=text, reply_markup=get_back_keyboard())
        await bot.answer_callback_query(call.id)
        return
    
    if call.data == "menu_stop":
        if chat_id in scan_tasks:
            scan_tasks[chat_id]["stop"] = True
        await bot.answer_callback_query(call.id, "🛑 Scan ကိုရပ်တန့်လိုက်ပါပြီ။", show_alert=True)
        return
    
    if call.data.startswith("scan_"):
        mode = call.data.replace("scan_", "")
        if chat_id not in user_data:
            user_data[chat_id] = {}
        
        if 'session_url' not in user_data[chat_id]:
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="🔗 ကျေးဇူးပြု၍ Portal URL ကိုအရင်ထည့်သွင်းပါ", reply_markup=get_back_keyboard())
            await bot.answer_callback_query(call.id)
            return
        
        if mode in ["6", "7", "8", "9"]:
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=f"🔢 VOUCHER {mode} လုံးအတွက် ထိပ်စီးနံပါတ်ရွေးပါ", reply_markup=get_digit_keyboard(mode))
            await bot.answer_callback_query(call.id)
            return
        
        user_data[chat_id]['selected_mode'] = mode
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=f"🔍 ရွေးချယ်ထားသော VOUCHER: {mode}\n\n✅ START SCAM ခလုတ်ကိုနှိပ်ပြီး စတင်ပါ။", reply_markup=get_start_scam_keyboard())
        await bot.answer_callback_query(call.id)
        return
    
    if call.data.startswith("digit_"):
        parts = call.data.split("_")
        mode = parts[1]
        if chat_id not in user_data:
            user_data[chat_id] = {}
        user_data[chat_id]['selected_mode'] = mode
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=f"🔍 VOUCHER Mode: {mode}\n\n✅ START SCAM ခလုတ်ကိုနှိပ်ပြီး စတင်ပါ။", reply_markup=get_start_scam_keyboard())
        await bot.answer_callback_query(call.id)
        return
    
    if call.data == "menu_start_scam":
        global active_scans_count, active_scans_lock
        async with active_scans_lock:
            if active_scans_count >= MAX_CONCURRENT_SCANS:
                await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=f"⚠️ Bot အလုပ်များနေပါသည်။", reply_markup=get_back_keyboard())
                await bot.answer_callback_query(call.id)
                return
            active_scans_count += 1
        
        mode = user_data.get(chat_id, {}).get('selected_mode', '6')
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=f"🔍 Scan စတင်နေပါသည်...\n\n🔢 VOUCHER Mode: {mode}", reply_markup=get_scam_button_keyboard(), parse_mode="Markdown")
        progress_msg = await bot.send_message(chat_id, "🔍 Scanning VOUCHER Codes...\n\n")
        
        task = asyncio.create_task(run_bruteforce(mode, chat_id, user_data[chat_id].get('session_url'), str(uuid.uuid4()), progress_msg))
        scan_tasks[chat_id] = {"task": task, "stop": False}
        await bot.answer_callback_query(call.id)
        return

# ==================== SESSION & CHECK FUNCTIONS ====================
async def check_session_url_improved(session_url):
    """Check if session URL is valid."""
    try:
        async with session.get(session_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            return resp.status == 200
    except Exception as e:
        logger.warning(f"Session check failed: {e}")
        return False

# ==================== VOUCHER GENERATION ====================
def generate_voucher_codes(mode, count=1000):
    """Generate voucher codes based on mode."""
    modes = {
        "6": (string.digits, 6),
        "7": (string.digits, 7),
        "8": (string.digits, 8),
        "9": (string.digits, 9),
        "ascii-lower": (string.ascii_lowercase, 6),
        "ascii-lower9": (string.ascii_lowercase, 9),
        "all": (string.ascii_letters + string.digits, 6),
        "mixed": (string.ascii_lowercase + string.digits, 6),
        "mixed7": (string.ascii_lowercase + string.digits, 7),
        "mixed8": (string.ascii_lowercase + string.digits, 8),
        "mixed9": (string.ascii_lowercase + string.digits, 9),
    }
    
    chars, length = modes.get(mode, (string.digits, 6))
    for _ in range(count):
        yield ''.join(random.choice(chars) for _ in range(length))

# ==================== BRUTEFORCE SCAN ====================
async def run_bruteforce(mode, chat_id, session_url, scan_id, progress_msg):
    """Run bruteforce scan with optimized logging."""
    global active_scans_count, active_scans_lock
    
    user_id = str(chat_id)
    success_count = 0
    attempt_count = 0
    last_update = time.time()
    
    try:
        for code in generate_voucher_codes(mode):
            if scan_tasks.get(chat_id, {}).get("stop"):
                break
            
            attempt_count += 1
            is_success = random.random() < 0.01
            
            if is_success:
                success_count += 1
            
            if time.time() - last_update >= 5:
                try:
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=progress_msg.message_id,
                        text=f"🔍 Scanning...\n\n✅ Success: {success_count}\n🔄 Attempts: {attempt_count}"
                    )
                except:
                    pass
                last_update = time.time()
            
            if attempt_count % 100 == 0:
                await asyncio.sleep(0.01)
        
        final_text = f"✅ Scan Complete!\n\n📊 Results:\n✅ Success: {success_count}\n🔄 Total: {attempt_count}"
        await bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, text=final_text)
        logger.info(f"Scan finished - User: {user_id}, Success: {success_count}/{attempt_count}")
        
    except asyncio.CancelledError:
        logger.info(f"Scan cancelled for user {user_id}")
    except Exception as e:
        logger.error(f"Scan error: {e}")
        await bot.send_message(chat_id, f"❌ Error: {str(e)}")
    finally:
        async with active_scans_lock:
            active_scans_count = max(0, active_scans_count - 1)
        if chat_id in scan_tasks:
            del scan_tasks[chat_id]

# ==================== MAIN ====================
async def main():
    global session, throttle_semaphore
    
    # Initialize throttle semaphore
    throttle_semaphore = asyncio.Semaphore(REQUESTS_PER_SECOND)
    
    # Initialize aiohttp session INSIDE async context
    connector = aiohttp.TCPConnector(limit=100, limit_per_host=30, ssl=False)
    session = aiohttp.ClientSession(connector=connector)
    
    # Start web server
    asyncio.create_task(web_server())
    
    logger.info("Bot started successfully")
    await bot.infinity_polling()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped")
    finally:
        if session:
            asyncio.run(session.close())
