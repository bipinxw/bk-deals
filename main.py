import os
import re
import asyncio
import logging
import json
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from urllib.parse import urljoin

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
import aiohttp
from bs4 import BeautifulSoup
import google.generativeai as genai
import asyncpg
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from telegram.constants import ParseMode

# load_dotenv() is optional; works locally if .env exists, safe on Render
load_dotenv()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
ADMIN_ID = int(os.getenv("ADMIN_USER_ID", 0))
SUPABASE_URL = os.getenv("SUPABASE_URL")          # e.g., https://xxxxx.supabase.co
SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD")  # from Supabase DB connection string
PORT = int(os.environ.get("PORT", 8080))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------- Validate required environment ----------
missing = []
if not TOKEN: missing.append("TELEGRAM_BOT_TOKEN")
if not GEMINI_API_KEY: missing.append("GEMINI_API_KEY")
if not SUPABASE_URL: missing.append("SUPABASE_URL")
if not SUPABASE_PASSWORD: missing.append("SUPABASE_PASSWORD")
if missing:
    logger.error(f"Missing environment variables: {', '.join(missing)}")
    raise SystemExit(1)

# ---------- AI ----------
genai.configure(api_key=GEMINI_API_KEY)
gemini_model = genai.GenerativeModel('gemini-1.5-flash')

# ---------- PostgreSQL connection pool ----------
db_pool: asyncpg.Pool = None

async def init_db_pool():
    global db_pool
    # Extract host from SUPABASE_URL (remove https:// and trailing slash)
    host = SUPABASE_URL.replace("https://", "").replace("http://", "").rstrip('/')
    # Build connection string
    db_url = f"postgresql://postgres:{SUPABASE_PASSWORD}@db.{host}:5432/postgres"
    try:
        db_pool = await asyncpg.create_pool(db_url, min_size=1, max_size=5)
        logger.info("✅ Database connection pool created")
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        raise

async def init_tables():
    """Create tables if they don't exist."""
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY
            );
            CREATE TABLE IF NOT EXISTS sent_deals (
                deal_url TEXT PRIMARY KEY,
                title TEXT,
                price REAL,
                original_price REAL,
                bank_offers TEXT,
                analysis_summary TEXT,
                verdict TEXT,
                message_id INTEGER,
                chat_id BIGINT,
                sent_at TIMESTAMPTZ,
                last_validated TIMESTAMPTZ,
                is_expired BOOLEAN DEFAULT FALSE
            );
            CREATE TABLE IF NOT EXISTS tracked_products (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id),
                product_url TEXT,
                target_price REAL,
                last_price REAL,
                last_check TIMESTAMPTZ,
                UNIQUE(user_id, product_url)
            );
        """)
        logger.info("✅ Database tables ready")

# ---------- Database helpers ----------
async def register_user(user_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute("INSERT INTO users (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING", user_id)

async def get_all_users():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM users")
        return [r['user_id'] for r in rows]

async def add_sent_deal(deal_url: str, title: str, price: float, original_price: float,
                        bank_offers: str, analysis: str, verdict: str, message_id: int, chat_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO sent_deals 
                (deal_url, title, price, original_price, bank_offers, analysis_summary, verdict,
                 message_id, chat_id, sent_at, last_validated, is_expired)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (deal_url) DO UPDATE SET
                title = EXCLUDED.title,
                price = EXCLUDED.price,
                original_price = EXCLUDED.original_price,
                bank_offers = EXCLUDED.bank_offers,
                analysis_summary = EXCLUDED.analysis_summary,
                verdict = EXCLUDED.verdict,
                message_id = EXCLUDED.message_id,
                chat_id = EXCLUDED.chat_id,
                sent_at = EXCLUDED.sent_at,
                last_validated = EXCLUDED.last_validated,
                is_expired = EXCLUDED.is_expired
        """, deal_url, title, price, original_price, bank_offers, analysis, verdict,
            message_id, chat_id, datetime.utcnow().isoformat(), datetime.utcnow().isoformat(), False)

async def update_deal_expiry(deal_url: str, is_expired: bool):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE sent_deals SET is_expired=$1, last_validated=$2 WHERE deal_url=$3",
                           is_expired, datetime.utcnow().isoformat(), deal_url)

async def get_all_active_deals():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM sent_deals WHERE is_expired = FALSE")
        return [dict(r) for r in rows]

async def delete_old_deals(cutoff_iso: str):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT deal_url, chat_id, message_id FROM sent_deals WHERE sent_at < $1", cutoff_iso)
        await conn.execute("DELETE FROM sent_deals WHERE sent_at < $1", cutoff_iso)
        return [dict(r) for r in rows]

async def add_tracked_product(user_id: int, url: str):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO tracked_products (user_id, product_url, target_price, last_price, last_check)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (user_id, product_url) DO UPDATE SET last_check = EXCLUDED.last_check
        """, user_id, url, 0, 0, datetime.utcnow().isoformat())

# ---------- Scraping (aiohttp only) ----------
def extract_price(text: str) -> float:
    match = re.search(r'[\d,]+\.?\d*', text.replace(',', ''))
    return float(match.group()) if match else 0.0

async def fetch_html(url: str) -> Optional[str]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10) as resp:
                if resp.status == 200:
                    return await resp.text()
    except Exception as e:
        logger.error(f"Fetch error {url}: {e}")
    return None

async def fetch_deals_from_desidime() -> List[dict]:
    deals = []
    html = await fetch_html("https://www.desidime.com/hot-deals")
    if not html:
        return deals
    soup = BeautifulSoup(html, 'html.parser')
    for item in soup.select(".deal_fluid")[:15]:
        title_elem = item.select_one(".deal_title a")
        if not title_elem:
            continue
        title = title_elem.get_text(strip=True)
        url = urljoin("https://www.desidime.com", title_elem['href'])
        price_elem = item.select_one(".price")
        price = extract_price(price_elem.get_text(strip=True)) if price_elem else 0
        original_price = price * 1.3
        deals.append({
            "title": title, "url": url, "price": price, "original_price": original_price,
            "bank_offers": "Check site for bank offers", "rating": "4.0", "source": "DesiDime"
        })
    return deals

async def fetch_deals_from_grabon() -> List[dict]:
    deals = []
    html = await fetch_html("https://www.grabon.in/deals/")
    if not html:
        return deals
    soup = BeautifulSoup(html, 'html.parser')
    for item in soup.select(".deal-card")[:10]:
        title_elem = item.select_one(".deal-title a")
        if not title_elem:
            continue
        title = title_elem.get_text(strip=True)
        url = title_elem.get('href', '')
        if not url.startswith('http'):
            url = "https://www.grabon.in" + url
        price_elem = item.select_one(".price")
        price = extract_price(price_elem.get_text(strip=True)) if price_elem else 0
        original_price = price * 1.2
        deals.append({
            "title": title, "url": url, "price": price, "original_price": original_price,
            "bank_offers": "Check GrabOn for bank offers", "rating": "4.0", "source": "GrabOn"
        })
    return deals

async def fetch_deals_from_amazon() -> List[dict]:
    deals = []
    html = await fetch_html("https://www.amazon.in/deals")
    if not html:
        return deals
    soup = BeautifulSoup(html, 'html.parser')
    for item in soup.select("[data-testid='deal-card']")[:8]:
        title_elem = item.select_one(".dealTitle")
        if not title_elem:
            continue
        title = title_elem.get_text(strip=True)
        url_elem = item.select_one("a")
        url = url_elem.get('href') if url_elem else ""
        if url and not url.startswith('http'):
            url = "https://www.amazon.in" + url
        price_elem = item.select_one(".price")
        price = extract_price(price_elem.get_text(strip=True)) if price_elem else 0
        original_price = price * 1.25
        deals.append({
            "title": title, "url": url, "price": price, "original_price": original_price,
            "bank_offers": "Check Amazon Pay offers", "rating": "4.2", "source": "Amazon"
        })
    return deals

async def fetch_deals_from_flipkart() -> List[dict]:
    deals = []
    html = await fetch_html("https://www.flipkart.com/offers-store")
    if not html:
        return deals
    soup = BeautifulSoup(html, 'html.parser')
    for item in soup.select("._1AtVbE")[:8]:
        title_elem = item.select_one("._4rR01T")
        if not title_elem:
            continue
        title = title_elem.get_text(strip=True)
        url_elem = item.select_one("a")
        url = url_elem.get('href') if url_elem else ""
        if url and not url.startswith('http'):
            url = "https://www.flipkart.com" + url
        price_elem = item.select_one("._30jeq3")
        price = extract_price(price_elem.get_text(strip=True)) if price_elem else 0
        original_price = price * 1.2
        deals.append({
            "title": title, "url": url, "price": price, "original_price": original_price,
            "bank_offers": "Bank offers on Flipkart", "rating": "4.1", "source": "Flipkart"
        })
    return deals

async def fetch_deals_from_reddit() -> List[dict]:
    deals = []
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://www.reddit.com/r/IndiaDeals/hot.json?limit=10", headers={"User-Agent": "Mozilla/5.0"}) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for child in data['data']['children']:
                        post = child['data']
                        title = post['title']
                        url = post.get('url', '')
                        if "amazon" in url.lower() or "flipkart" in url.lower():
                            deals.append({
                                "title": title, "url": url, "price": 0, "original_price": 0,
                                "bank_offers": "Check comments", "rating": "4.0", "source": "Reddit"
                            })
    except Exception as e:
        logger.error(f"Reddit error: {e}")
    return deals

async def fetch_all_deals() -> List[dict]:
    tasks = [fetch_deals_from_desidime(), fetch_deals_from_grabon(),
             fetch_deals_from_amazon(), fetch_deals_from_flipkart(), fetch_deals_from_reddit()]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_deals = []
    for res in results:
        if isinstance(res, list):
            all_deals.extend(res)
    seen = set()
    unique = []
    for d in all_deals:
        if d['url'] not in seen:
            seen.add(d['url'])
            unique.append(d)
    return unique

# ---------- AI Analysis ----------
async def ai_validate_and_analyze_deal(deal: dict) -> dict:
    live_html = await fetch_html(deal['url'])
    live_text = BeautifulSoup(live_html, 'html.parser').get_text()[:5000] if live_html else "Page not reachable"

    prompt = f"""You are an expert Indian deal analyst. Analyse the following deal and the live page content.

DEAL METADATA:
Title: {deal['title']}
Price: ₹{deal['price']}
Original MRP: ₹{deal['original_price']}
Bank offers: {deal.get('bank_offers', 'None')}
Source: {deal.get('source', 'Unknown')}
Rating: {deal.get('rating', 'N/A')}

LIVE PAGE SAMPLE (first 5000 chars):
{live_text}

Now generate a SHORT, engaging analysis in Hinglish (mix Hindi & English). Use RELEVANT EMOJIS naturally (e.g., 🔥 for hot deal, 💰 for price, ⚠️ for flaws, ✅ for good points, 💡 for verdict). Include:
- Good points (with emojis)
- Flaws / hidden catches (with emojis)
- A final verdict: "Excellent Deal", "Good Deal", "Average", or "Avoid"
- 1-2 better alternatives (product names only)

Output **only valid JSON** in this format:
{{
  "analysis_text": "Your analysis with emojis (max 200 words)",
  "verdict": "one of four",
  "flaws": ["flaw1", "flaw2"],
  "alternatives": ["alt1", "alt2"],
  "is_expired": true/false
}}
Be honest. If the page shows 'out of stock', 'deal ended', or price > MRP, set is_expired=true."""
    try:
        response = gemini_model.generate_content(prompt)
        text = response.text.strip()
        text = re.sub(r'```json\s*|\s*```', '', text)
        ai_data = json.loads(text)
    except Exception as e:
        logger.error(f"AI analysis failed: {e}")
        ai_data = {
            "analysis_text": "⚠️ Analysis temporarily unavailable. Please check manually.",
            "verdict": "Average",
            "flaws": [],
            "alternatives": [],
            "is_expired": False
        }
    enriched = {**deal}
    enriched['analysis_text'] = ai_data.get('analysis_text', '')
    enriched['verdict'] = ai_data.get('verdict', 'Average')
    enriched['flaws'] = ai_data.get('flaws', [])
    enriched['alternatives'] = ai_data.get('alternatives', [])
    enriched['is_expired'] = ai_data.get('is_expired', False)
    return enriched

# ---------- Message Formatting ----------
def format_deal_message(deal: dict) -> str:
    if deal.get('is_expired'):
        title = f"<s>{deal['title']}</s>"
        price = f"<s>₹{deal['price']:,.0f}</s>"
        expiry_note = "\n\n❌ Deal expired • Better alternatives below"
    else:
        title = f"<b>{deal['title']}</b>"
        price = f"₹{deal['price']:,.0f}"
        expiry_note = ""
    original = f"<s>MRP ₹{deal['original_price']:,.0f}</s>" if deal['original_price'] > deal['price'] else f"MRP ₹{deal['original_price']:,.0f}"
    discount = int((1 - deal['price']/deal['original_price'])*100) if deal['original_price'] > 0 else 0
    msg = f"""
{title}
💰 {price}  ( {original}  |  {discount}% off )
🏦 {deal.get('bank_offers', 'No bank offers')}
📍 Source: {deal.get('source', 'Unknown')}

🧠 <b>AI Analysis:</b>
{deal.get('analysis_text', 'No analysis available')}

⚠️ <b>Flaws Detected:</b>
{chr(10).join([f'• {f}' for f in deal.get('flaws', [])]) if deal.get('flaws') else '• None reported'}

💡 <b>Verdict:</b> {deal.get('verdict', 'Average')}
{expiry_note}
    """
    return msg.strip()

def get_deal_keyboard(deal: dict) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton("🔗 View on Site", url=deal['url'])],
        [InlineKeyboardButton("🔔 Set Alert", callback_data=f"alert_{deal['url']}"),
         InlineKeyboardButton("🔄 Alternatives", callback_data=f"alt_{deal['url']}")]
    ]
    if not deal.get('is_expired'):
        buttons[1].append(InlineKeyboardButton("👎 Not Interested", callback_data=f"notint_{deal['url']}"))
    return InlineKeyboardMarkup(buttons)

# ---------- Broadcasting ----------
async def send_deal_to_user(bot, user_id: int, deal: dict):
    msg = format_deal_message(deal)
    keyboard = get_deal_keyboard(deal)
    try:
        sent = await bot.send_message(chat_id=user_id, text=msg, parse_mode=ParseMode.HTML, reply_markup=keyboard)
        await add_sent_deal(deal['url'], deal['title'], deal['price'], deal['original_price'],
                            deal.get('bank_offers', ''), deal.get('analysis_text', ''), deal.get('verdict', 'Average'),
                            sent.message_id, user_id)
    except Exception as e:
        logger.error(f"Failed to send to {user_id}: {e}")

async def broadcast_deal(bot, deal: dict):
    users = await get_all_users()
    await asyncio.gather(*[send_deal_to_user(bot, uid, deal) for uid in users])

# ---------- Background Jobs ----------
async def fetch_and_broadcast(app: Application):
    logger.info("Fetching deals...")
    deals = await fetch_all_deals()
    for deal in deals:
        # Duplicate check within 2 hours
        async with db_pool.acquire() as conn:
            exists = await conn.fetchval("SELECT 1 FROM sent_deals WHERE deal_url=$1 AND sent_at > NOW() - INTERVAL '2 hours'", deal['url'])
        if exists:
            continue
        enriched = await ai_validate_and_analyze_deal(deal)
        if enriched.get('is_expired'):
            continue
        await broadcast_deal(app.bot, enriched)
        await asyncio.sleep(1)

async def revalidate_deals(app: Application):
    deals = await get_all_active_deals()
    for deal in deals:
        refreshed = await ai_validate_and_analyze_deal(deal)
        if refreshed.get('is_expired'):
            await update_deal_expiry(deal['deal_url'], True)
            try:
                expired_deal = {**deal, 'is_expired': True}
                new_text = format_deal_message(expired_deal)
                keyboard = get_deal_keyboard(expired_deal)
                await app.bot.edit_message_text(chat_id=deal['chat_id'], message_id=deal['message_id'],
                                                text=new_text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
            except Exception as e:
                logger.error(f"Edit failed: {e}")
        await asyncio.sleep(0.5)

async def cleanup_messages(app: Application):
    cutoff = datetime.utcnow() - timedelta(days=60)
    old = await delete_old_deals(cutoff.isoformat())
    for row in old:
        try:
            await app.bot.delete_message(chat_id=row['chat_id'], message_id=row['message_id'])
        except:
            pass
        await asyncio.sleep(0.05)

# ---------- FastAPI Webhook ----------
telegram_app = Application.builder().token(TOKEN).build()
scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await init_db_pool()
    await init_tables()
    await telegram_app.initialize()
    webhook_url = f"https://{os.environ.get('RENDER_EXTERNAL_HOSTNAME')}/webhook"
    await telegram_app.bot.set_webhook(webhook_url)
    logger.info(f"Webhook set to {webhook_url}")
    
    scheduler.add_job(lambda: asyncio.create_task(fetch_and_broadcast(telegram_app)), IntervalTrigger(hours=2))
    scheduler.add_job(lambda: asyncio.create_task(revalidate_deals(telegram_app)), IntervalTrigger(hours=4))
    scheduler.add_job(lambda: asyncio.create_task(cleanup_messages(telegram_app)), CronTrigger(hour=3, minute=0))
    scheduler.start()
    
    yield
    
    # Shutdown
    scheduler.shutdown()
    if db_pool:
        await db_pool.close()
    await telegram_app.shutdown()

fastapi_app = FastAPI(lifespan=lifespan)

# ---------- Command Handlers ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await register_user(user_id)
    await update.message.reply_text("👋 Welcome! You'll receive all deals automatically.")

async def track(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /track <url>")
        return
    await add_tracked_product(update.effective_user.id, context.args[0])
    await update.message.reply_text("🔔 Tracking started.")

async def feedback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Thanks for your feedback!")

async def cleanup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("Admin only.")
        return
    await update.message.reply_text("🧹 Cleaning...")
    await cleanup_messages(context.application)
    await update.message.reply_text("✅ Done.")

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data.startswith("alert_"):
        await add_tracked_product(query.from_user.id, data[6:])
        await query.edit_message_text("🔔 Alert set.")
    elif data.startswith("alt_"):
        await query.edit_message_text("🔄 Alternatives: Check similar products.")
    elif data.startswith("notint_"):
        await query.edit_message_text("👎 Noted.")

telegram_app.add_handler(CommandHandler('start', start))
telegram_app.add_handler(CommandHandler('track', track))
telegram_app.add_handler(CommandHandler('feedback', feedback))
telegram_app.add_handler(CommandHandler('cleanup', cleanup_command))
telegram_app.add_handler(CallbackQueryHandler(button_callback))

@fastapi_app.post("/webhook")
async def webhook(request: Request):
    update = Update.de_json(await request.json(), telegram_app.bot)
    await telegram_app.process_update(update)
    return Response(status_code=200)

@fastapi_app.get("/health")
async def health():
    return {"status": "alive"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(fastapi_app, host="0.0.0.0", port=PORT)
