Here's a **complete README.md** for the final bot version (no user preferences, AI adds emojis, all deals equal).

---

```markdown
# 🤖 IndiaDeals Bot – AI‑Powered Deal Notifier

**Your personal deal hunter** – finds, validates, and analyses live deals from Indian e‑commerce, adds relevant emojis for fun reading, and automatically cleans up expired messages.

No user preferences, no filters – every user gets every deal. The AI decides what's good, what's flawed, and when a deal is dead.

---

## ✨ Features

- 🔍 **Multi‑source scraping** – DesiDime, GrabOn, Amazon, Flipkart, Reddit (hot deals)
- 🧠 **AI analysis (Gemini 1.5 Flash)** – reads live product page, detects expiry, finds flaws, suggests alternatives, and adds **relevant emojis** (🔥💰⚠️✅💡)
- ⏱️ **Real‑time expiry handling** – re‑validates deals every 4 hours; expired deals get strikethrough on the original Telegram message
- 🧹 **Auto‑cleanup** – deletes messages older than 60 days from Telegram and database (runs daily at 3 AM UTC)
- 📢 **Broadcast to all users** – every registered user receives every valid deal
- 📦 **Caching** – ChromaDB caches AI results for 2 hours to save API costs
- 🚀 **Production‑ready** – async, error‑handled, runs on Railway or any VPS

---

## 📸 Example Message

```
🔥 OnePlus Nord CE 4
💰 ₹24,999  ( MRP ₹30,999  |  19% off )
🏦 10% Instant Discount with HDFC Bank Cards
📍 Source: Amazon

🧠 AI Analysis:
✅ 50MP camera & 5000mAh battery 🔋
⚠️ No charger in box – buy separately
💡 Good deal under 25k, but check CMF Phone 1 as alternative

⚠️ Flaws Detected:
• No charger included
• Average software update policy

💡 Verdict: Good Deal
```

---

## 📋 Commands

| Command | Description |
|---------|-------------|
| `/start` | Register yourself (you’ll receive all future deals) |
| `/track <url>` | Monitor a product for price drops (coming soon) |
| `/feedback` | Send rating or comment to help improve AI |
| `/cleanup` | Admin only – manually delete old messages |

> No `/preferences` – everyone gets every deal equally.

---

## 🛠️ Setup & Installation

### Prerequisites
- Python 3.9+
- Telegram Bot Token (from [@BotFather](https://t.me/BotFather))
- Google Gemini API Key (from [Google AI Studio](https://aistudio.google.com/))

### Local Development

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/indiadeals-bot.git
   cd indiadeals-bot
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   venv\Scripts\activate     # Windows
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   playwright install
   ```

4. **Set environment variables** – create `.env` file:
   ```
   TELEGRAM_BOT_TOKEN=your_bot_token_here
   GEMINI_API_KEY=your_gemini_api_key_here
   ADMIN_USER_ID=your_telegram_user_id_here
   ```

5. **Run the bot**
   ```bash
   python main.py
   ```

---

## 🚀 Deployment on Railway

1. Push the code to a GitHub repository.
2. Log into [Railway](https://railway.app/) → **New Project** → **Deploy from GitHub repo**.
3. Select your repo.
4. Add the environment variables (same as `.env`) in the Railway dashboard.
5. Set the **Start Command**:
   ```bash
   playwright install && python main.py
   ```
6. Deploy – Railway will automatically install dependencies and run the bot.

> The bot uses local SQLite and ChromaDB; Railway’s ephemeral storage is fine. For production scale, consider attaching a persistent volume.

---

## 🧠 How It Works (High Level)

1. **Every 2 hours** – fetch deals from all sources concurrently.
2. **For each deal** – call Gemini AI with the deal metadata and live page content.
3. AI returns: analysis (with emojis), verdict, flaws, alternatives, and **expiry flag**.
4. If not expired, broadcast to **every registered user**.
5. **Every 4 hours** – re‑validate active deals (re‑scrape + AI) to detect expiry.
6. If expired – edit the original Telegram message with strikethrough (`<s>` tags).
7. **Daily at 3 AM UTC** – delete messages older than 60 days from Telegram and DB.

All scraping uses **Playwright** (for dynamic sites like Amazon/Flipkart) and **aiohttp** (for static sites). Errors are logged but never crash the bot.

---

## 📁 File Structure

```
.
├── main.py               # Bot code (single file)
├── requirements.txt      # Python dependencies
├── .env                  # Environment variables (not committed)
├── README.md             # This file
└── data/                 # Auto‑created folder for SQLite & ChromaDB
    ├── bot.db            # SQLite database
    └── chromadb/         # ChromaDB cache files
```

---

## 🧪 Testing

After starting the bot, send `/start` to your bot on Telegram. You should receive a welcome message.  
Within 2 hours (or immediately if you restart the bot), it will fetch deals and broadcast them.

Check the console logs for scraping results and AI responses.

---

## ⚠️ Troubleshooting

| Issue | Solution |
|-------|----------|
| Bot doesn’t respond | Verify `TELEGRAM_BOT_TOKEN` and `GEMINI_API_KEY` are correct. |
| Playwright errors | Run `playwright install` again. |
| No deals found | Some sites may block scraping; the bot includes a demo fallback. Increase timeout or add proxies. |
| Telegram edit fails | Bot needs “edit message” permission – it has it by default. |

---

## 🤝 Contributing

Feel free to open issues or pull requests. Ideas:
- Add more deal sources (Myntra, Ajio, Croma)
- Implement `/track` price drop alerts
- Store user feedback to fine‑tune AI prompts

---

## 📄 License

MIT – use, modify, and distribute freely.

---

## 🙏 Acknowledgements

- [python-telegram-bot](https://github.com/python-telegram-bot/python-telegram-bot)
- [Google Gemini API](https://deepmind.google/technologies/gemini/)
- [Playwright](https://playwright.dev/)
- [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/)
```

You can copy this directly into a `README.md` file in your repository. It covers everything a new user or developer needs to know.
