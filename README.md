# STAR LINK CODE HACK Bot

## GitHub တင်ခြင်း

```bash
git init
git add .
git commit -m "initial commit"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO.git
git push -u origin main
```

---

## Railway Deploy

1. [railway.app](https://railway.app) → **New Project** → **Deploy from GitHub Repo**
2. Repo ရွေးပါ
3. **Variables** tab → အောက်ပါ env vars ထည့်ပါ

| Variable | Value |
|----------|-------|
| `BOT_TOKEN` | Telegram Bot Token |
| `GITHUB_TOKEN` | GitHub Personal Access Token |
| `REPO_OWNER` | GitHub username |
| `REPO_NAME` | Repository name |

4. `bot.py` ထဲ `ADMINS` list တွင် Admin Telegram ID ထည့်ပါ

---

## Commands

| Command | ဖော်ပြချက် |
|---------|-----------|
| `/start` | Bot စတင်ရန် |
| `/portal <url>` | Portal URL ထည့်ရန် |
| `/scan <mode>` | Code ရှာဖွေရန် |
| `/stop` | Scan ရပ်ရန် |
| `/recheck` | Codes ပြန်စစ်ရန် |
| `/result` | ရလဒ်ကြည့်ရန် |
| `/status` | Bot status (Admin) |

## GitHub Repo ဖြစ်ရမည့်ဖိုင်များ

```
result.json   → {}
auth_list.json → {}
```
