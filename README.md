# Voucher Checker Bot

## Railway Deployment

### 1. GitHub တင်ခြင်း
```bash
git init
git add .
git commit -m "initial commit"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO.git
git push -u origin main
```

### 2. Railway Setup
1. [railway.app](https://railway.app) တွင် New Project → Deploy from GitHub Repo
2. Repository ရွေးပါ
3. **Variables** tab တွင် အောက်ပါ env vars ထည့်ပါ:

| Variable | Value |
|----------|-------|
| `BOT_TOKEN` | Telegram bot token (@BotFather မှ) |
| `ADMIN_ID` | သင်၏ Telegram user ID |

4. Deploy ကို စောင့်ပါ ✅

---

## Commands

| Command | ဖော်ပြချက် |
|---------|-----------|
| `/setup <url>` | Session URL သတ်မှတ်ရန် |
| `/brute <mode> <length> [target] [plan]` | Code ရှာဖွေရန် |
| `/stop` | Scan ရပ်ရန် |
| `/resume` | Scan ပြန်စရန် |
| `/saved` | ရလဒ်ကြည့်ရန် |
| `/notify` | Notification ON/OFF |
| `/recheck` | Success codes ပြန်စစ်ရန် |
| `/status` | Bot status (Admin) |

## Brute Mode

| Mode | ဖော်ပြချက် |
|------|-----------|
| `1` | ဂဏန်း 0-9 |
| `2` | အသေး a-z |
| `3` | အကြီး A-Z |
| `4` | အကြီး+အသေး a-zA-Z |
| `5` | စာ+ဂဏန်း a-z0-9 |

## ဥပမာ

```
/brute 1 6        → ဂဏန်း ၆ လုံး
/brute 1 6 5      → ဂဏန်း ၆ လုံး, ၅ ခု
/brute 5 8 10     → ၈ လုံး, ၁၀ ခု
/brute 5 6 5 1d   → ၆ လုံး, ၁ ရက်ကျော် ၅ ခု
```
