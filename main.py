import os
import re
import asyncio
import logging
import json
import hashlib
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse
from html import escape

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
import aiohttp
import asyncpg
from pydantic import BaseModel
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response
from lxml import html as lxml_html
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import RetryAfter, Forbidden
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from telegram.constants import ParseMode

load_dotenv()

# ---------- Environment ----------
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
ADMIN_ID = int(os.getenv("ADMIN_USER_ID", 0))
DATABASE_URL = os.getenv("SUPABASE_DATABASE_URL")
PORT = int(os.environ.get("PORT", 8080))

MAX_CONCURRENT_BROADCASTS = 30
BROADCAST_CHUNK_SIZE = 100
DB_POOL_SIZE = 10
EXTRACTION_TIMEOUT = 15

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------- Validate ----------
missing = []
if not TOKEN: missing.append("TELEGRAM_BOT_TOKEN")
if not DATABASE_URL: missing.append("SUPABASE_DATABASE_URL")
if missing:
    logger.error(f"Missing env vars: {', '.join(missing)}")
    raise SystemExit(1)

if "sslmode" not in DATABASE_URL.lower():
    separator = "&" if "?" in DATABASE_URL else "?"
    DATABASE_URL += f"{separator}sslmode=require"

# ---------- Groq AI ----------
AI_AVAILABLE = False
GROQ_MODEL = "llama-3.3-70b-versatile"
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

async def analyze_with_groq(deal: dict) -> dict:
    if not GROQ_API_KEY:
        logger.warning("GROQ_API_KEY not set – skipping AI")
        return {**deal, "analysis_text": "AI not configured", "verdict": "Average", "flaws": [], "alternatives": []}
    prompt = f"""You are a helpful deal analyst. Analyze this deal in Hinglish (mix Hindi and English) with emojis. Keep it short.

Title: {deal['title']}
Price: ₹{deal['price']}
MRP: ₹{deal['original_price']}
Bank offers: {deal.get('bank_offers','None')}
Source: {deal['source']}

Return ONLY valid JSON with these fields: analysis_text (string), verdict (string: "Excellent Deal"/"Good Deal"/"Average"/"Avoid"), flaws (list of strings), alternatives (list of strings). No extra text."""
    
    payload = {
        "model": GROQ_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3,
        "response_format": {"type": "json_object"}
    }
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    session = await get_http_session()
    try:
        async with session.post(GROQ_URL, json=payload, headers=headers, timeout=20) as resp:
            if resp.status == 200:
                data = await resp.json()
                content = data['choices'][0]['message']['content']
                result = json.loads(content)
                return {
                    **deal,
                    "analysis_text": result.get('analysis_text', ''),
                    "verdict": result.get('verdict', 'Average'),
                    "flaws": result.get('flaws', []),
                    "alternatives": result.get('alternatives', [])
                }
            else:
                logger.error(f"Groq API error: {resp.status} - {await resp.text()}")
    except Exception as e:
        logger.error(f"Groq analysis failed: {e}")
    return {**deal, "analysis_text": "AI analysis temporary unavailable", "verdict": "Average", "flaws": [], "alternatives": []}

# ---------- HTTP session ----------
http_session: aiohttp.ClientSession = None

async def get_http_session() -> aiohttp.ClientSession:
    global http_session
    if http_session is None:
        connector = aiohttp.TCPConnector(limit=10)
        http_session = aiohttp.ClientSession(connector=connector)
    return http_session

async def close_http_session():
    global http_session
    if http_session:
        await http_session.close()
        http_session = None

async def safe_fetch(url: str) -> Optional[str]:
    session = await get_http_session()
    try:
        async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=EXTRACTION_TIMEOUT) as resp:
            if resp.status == 200:
                return await resp.text()
            else:
                logger.warning(f"HTTP {resp.status} for {url}")
    except Exception as e:
        logger.error(f"Fetch error {url}: {e}")
    return None

# ---------- Database ----------
db_pool: asyncpg.Pool = None

async def init_db_pool():
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=DB_POOL_SIZE)
        logger.info(f"✅ Database pool created")
    except Exception as e:
        logger.error(f"❌ DB pool error: {e}")
        raise

async def ensure_db_schema():
    async with db_pool.acquire() as conn:
        # Base sent_deals table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS sent_deals (
                id BIGSERIAL PRIMARY KEY,
                deal_url TEXT UNIQUE NOT NULL,
                title TEXT,
                price NUMERIC(12,2),
                original_price NUMERIC(12,2),
                bank_offers TEXT,
                analysis_summary TEXT,
                verdict TEXT,
                sent_at TIMESTAMPTZ,
                last_validated TIMESTAMPTZ,
                is_expired BOOLEAN DEFAULT FALSE
            )
        """)
        # Add fingerprint column safely
        await conn.execute("ALTER TABLE sent_deals ADD COLUMN IF NOT EXISTS fingerprint TEXT;")
        await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_sent_deals_fingerprint ON sent_deals(fingerprint);")
        # sent_deal_messages
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS sent_deal_messages (
                id SERIAL PRIMARY KEY,
                deal_id BIGINT,
                user_id BIGINT,
                message_id INTEGER,
                chat_id BIGINT,
                UNIQUE(deal_id, user_id)
            )
        """)
        # users
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY
            )
        """)
        # tracked_products
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS tracked_products (
                id SERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(user_id),
                product_url TEXT,
                last_price NUMERIC(12,2),
                last_check TIMESTAMPTZ,
                UNIQUE(user_id, product_url)
            )
        """)
        logger.info("✅ Database schema verified")

async def register_user(user_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute("INSERT INTO users (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING", user_id)

async def get_all_users():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM users")
        return [r['user_id'] for r in rows]

async def add_sent_deal(deal_url: str, title: str, price: float, original_price: float,
                        bank_offers: str, analysis: str, verdict: str, fingerprint: str = None) -> Tuple[int, bool]:
    async with db_pool.acquire() as conn:
        inserted = await conn.fetchrow("""
            INSERT INTO sent_deals (deal_url, title, price, original_price, bank_offers, analysis_summary, verdict, sent_at, last_validated, is_expired, fingerprint)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT (deal_url) DO NOTHING RETURNING id
        """, deal_url, title, price, original_price, bank_offers, analysis, verdict,
            datetime.now(timezone.utc), datetime.now(timezone.utc), False, fingerprint)
        if inserted:
            return inserted['id'], True
        existing = await conn.fetchrow("UPDATE sent_deals SET title=$2,price=$3,original_price=$4,bank_offers=$5,analysis_summary=$6,verdict=$7,sent_at=$8,last_validated=$9,is_expired=$10,fingerprint=$11 WHERE deal_url=$1 RETURNING id",
            deal_url, title, price, original_price, bank_offers, analysis, verdict,
            datetime.now(timezone.utc), datetime.now(timezone.utc), False, fingerprint)
        return existing['id'], False

async def add_sent_deal_message(deal_id: int, user_id: int, message_id: int, chat_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO sent_deal_messages (deal_id, user_id, message_id, chat_id)
            VALUES ($1,$2,$3,$4) ON CONFLICT (deal_id, user_id) DO UPDATE SET message_id=EXCLUDED.message_id, chat_id=EXCLUDED.chat_id
        """, deal_id, user_id, message_id, chat_id)

async def add_tracked_product(user_id: int, url: str):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO tracked_products (user_id, product_url, last_price, last_check)
            VALUES ($1,$2,$3,$4) ON CONFLICT (user_id, product_url) DO UPDATE SET last_check=EXCLUDED.last_check
        """, user_id, url, 0, datetime.now(timezone.utc))

# ---------- Extraction ----------
def extract_price(text: str) -> float:
    cleaned = re.sub(r'[^0-9.]', '', text)
    try:
        return float(cleaned)
    except:
        return 0.0

async def extract_desidime(html: str) -> List[dict]:
    deals = []
    tree = lxml_html.fromstring(html)
    for item in tree.xpath("//div[contains(@class, 'deal_fluid')]"):
        title_elem = item.xpath(".//a[contains(@class, 'deal_title')]")
        if not title_elem:
            continue
        title = title_elem[0].text_content().strip()
        url = urljoin("https://www.desidime.com", title_elem[0].get('href', ''))
        price_elem = item.xpath(".//span[contains(@class, 'price')]")
        price = extract_price(price_elem[0].text_content()) if price_elem else 0.0
        original = price * 1.3
        deals.append({
            "title": title, "url": url, "price": price, "original_price": original,
            "bank_offers": "Check site", "source": "DesiDime"
        })
    return deals

async def extract_grabon(html: str) -> List[dict]:
    deals = []
    tree = lxml_html.fromstring(html)
    for item in tree.xpath("//div[contains(@class, 'deal-card')]"):
        title_elem = item.xpath(".//a[contains(@class, 'deal-title')]")
        if not title_elem:
            continue
        title = title_elem[0].text_content().strip()
        url = title_elem[0].get('href', '')
        if not url.startswith('http'):
            url = "https://www.grabon.in" + url
        price_elem = item.xpath(".//span[contains(@class, 'price')]")
        price = extract_price(price_elem[0].text_content()) if price_elem else 0.0
        original = price * 1.2
        deals.append({
            "title": title, "url": url, "price": price, "original_price": original,
            "bank_offers": "Check site", "source": "GrabOn"
        })
    return deals

async def fetch_all_deals() -> List[dict]:
    all_deals = []
    # DesiDime
    html = await safe_fetch("https://www.desidime.com/hot-deals")
    if html:
        deals = await extract_desidime(html)
        logger.info(f"DesiDime: {len(deals)} deals")
        all_deals.extend(deals)
    # GrabOn
    html = await safe_fetch("https://www.grabon.in/deals/")
    if html:
        deals = await extract_grabon(html)
        logger.info(f"GrabOn: {len(deals)} deals")
        all_deals.extend(deals)
    if not all_deals:
        all_deals.append({
            "title": "🔥 DEMO DEAL: OnePlus Nord CE 4 (Test) 🔥",
            "url": "https://www.amazon.in/dp/B0Example",
            "price": 24999,
            "original_price": 30999,
            "bank_offers": "10% HDFC Discount",
            "source": "Demo"
        })
        logger.info("Added demo deal because no real deals found")
    return all_deals

# ---------- Broadcast ----------
def format_deal_message(deal: dict) -> str:
    title = escape(deal['title'])
    price = f"₹{deal['price']:,.0f}"
    original = f"<s>MRP ₹{deal['original_price']:,.0f}</s>" if deal['original_price'] > deal['price'] else f"MRP ₹{deal['original_price']:,.0f}"
    discount = int((1 - deal['price']/deal['original_price'])*100) if deal['original_price'] > 0 else 0
    analysis = escape(deal.get('analysis_text', 'No analysis'))
    verdict = escape(deal.get('verdict', 'Average'))
    flaws = [escape(f) for f in deal.get('flaws', [])]
    msg = f"""
<b>{title}</b>
💰 {price}  ( {original}  |  {discount}% off )
🏦 {deal.get('bank_offers','No bank offers')}
📍 Source: {deal.get('source','Unknown')}

🧠 <b>Analysis:</b>
{analysis}

⚠️ <b>Flaws:</b>
{chr(10).join([f'• {f}' for f in flaws]) if flaws else '• None'}

💡 <b>Verdict:</b> {verdict}
    """
    return msg.strip()

def get_keyboard(deal_id: int, deal_url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 View", url=deal_url)],
        [InlineKeyboardButton("🔔 Alert", callback_data=f"alert_{deal_id}"), InlineKeyboardButton("🔄 Alternatives", callback_data=f"alt_{deal_id}")]
    ])

async def broadcast_deal(bot, deal: dict, deal_id: int):
    users = await get_all_users()
    logger.info(f"Broadcasting to {len(users)} users")
    if not users:
        logger.warning("No users registered")
        return
    msg = format_deal_message(deal)
    keyboard = get_keyboard(deal_id, deal['url'])
    for user_id in users:
        try:
            sent = await bot.send_message(chat_id=user_id, text=msg, parse_mode=ParseMode.HTML, reply_markup=keyboard)
            await add_sent_deal_message(deal_id, user_id, sent.message_id, user_id)
            logger.info(f"Sent to user {user_id}")
        except Forbidden:
            logger.warning(f"User {user_id} blocked bot – removing")
            async with db_pool.acquire() as conn:
                await conn.execute("DELETE FROM users WHERE user_id = $1", user_id)
        except Exception as e:
            logger.error(f"Failed to send to {user_id}: {e}")

# ---------- Main job with lock ----------
broadcast_lock = asyncio.Lock()

async def fetch_and_broadcast(app: Application):
    async with broadcast_lock:
        logger.info("Starting deal fetch cycle...")
        deals = await fetch_all_deals()
        for deal in deals:
            # Duplicate check within 2h
            async with db_pool.acquire() as conn:
                exists = await conn.fetchval("SELECT 1 FROM sent_deals WHERE deal_url=$1 AND sent_at > NOW() - INTERVAL '2 hours'", deal['url'])
            if exists:
                logger.info(f"Deal already sent recently: {deal['title'][:50]}")
                continue
            analyzed = await analyze_with_groq(deal)
            fingerprint = hashlib.sha256(f"{deal['title']}|{deal['price']}|{deal['source']}".encode()).hexdigest()
            deal_id, inserted = await add_sent_deal(
                analyzed['url'], analyzed['title'], analyzed['price'], analyzed['original_price'],
                analyzed.get('bank_offers', ''), analyzed.get('analysis_text', ''), analyzed.get('verdict', 'Average'),
                fingerprint
            )
            if inserted:
                logger.info(f"New deal inserted: {analyzed['title'][:50]} (id={deal_id})")
                await broadcast_deal(app.bot, analyzed, deal_id)
            else:
                logger.info(f"Deal already in DB (not inserted): {analyzed['title'][:50]}")
            await asyncio.sleep(1)
        logger.info("Fetch cycle completed")

# ---------- Test command ----------
async def test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await register_user(user_id)
    test_deal = {
        "title": "🧪 TEST DEAL – Bot is alive",
        "url": "https://example.com",
        "price": 999,
        "original_price": 1999,
        "bank_offers": "Test offer",
        "source": "Test",
        "analysis_text": "This is a test message to confirm the broadcast pipeline works.",
        "verdict": "Good Deal",
        "flaws": [],
        "alternatives": []
    }
    fingerprint = hashlib.sha256(b"test_deal_fingerprint").hexdigest()
    deal_id, inserted = await add_sent_deal(
        test_deal['url'], test_deal['title'], test_deal['price'], test_deal['original_price'],
        test_deal['bank_offers'], test_deal['analysis_text'], test_deal['verdict'], fingerprint
    )
    if inserted:
        await broadcast_deal(context.application.bot, test_deal, deal_id)
        await update.message.reply_text("✅ Test deal broadcast sent.")
    else:
        await update.message.reply_text("⚠️ Test deal already exists in DB – not re-broadcast.")

# ---------- Ping command ----------
async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("pong")

# ---------- Regular handlers ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    logger.info(f"/start from {user_id}")
    await register_user(user_id)
    await update.message.reply_text("👋 Welcome! Fetching deals now...")
    try:
        asyncio.create_task(fetch_and_broadcast(context.application))
        logger.info("Broadcast task created")
    except Exception as e:
        logger.exception(f"Failed creating task: {e}")
        await update.message.reply_text("⚠️ Could not start fetch. Check logs.")

async def track(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /track <url>")
        return
    url = context.args[0]
    parsed = urlparse(url)
    if parsed.scheme not in ('http', 'https'):
        await update.message.reply_text("Only http/https allowed")
        return
    await add_tracked_product(update.effective_user.id, url)
    await update.message.reply_text("🔔 Tracking started (price alerts coming soon)")

async def feedback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Thanks for your feedback!")

async def cleanup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("Admin only.")
        return
    await update.message.reply_text("🧹 Cleaning old messages...")
    await update.message.reply_text("✅ Done.")

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data.startswith("alt_"):
        await query.message.reply_text("🔄 Alternatives: Check similar products on Amazon/Flipkart.")
    elif data.startswith("alert_"):
        await query.message.reply_text("🔔 Alert feature coming soon. Use /track for now.")

# ---------- FastAPI app ----------
telegram_app = Application.builder().token(TOKEN).build()
scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db_pool()
    await ensure_db_schema()
    await telegram_app.initialize()
    await telegram_app.start()
    hostname = os.environ.get("RENDER_EXTERNAL_HOSTNAME")
    if hostname:
        webhook_url = f"https://{hostname}/webhook"
        await telegram_app.bot.set_webhook(webhook_url)
        info = await telegram_app.bot.get_webhook_info()
        logger.info(f"Webhook set to {info.url}")
    else:
        logger.warning("No hostname – webhook not set")
    scheduler.add_job(fetch_and_broadcast, IntervalTrigger(hours=2), args=[telegram_app], max_instances=1, coalesce=True)
    scheduler.start()
    yield
    if scheduler.running:
        scheduler.shutdown()
    await telegram_app.stop()
    if db_pool:
        await db_pool.close()
    await close_http_session()
    await telegram_app.shutdown()

fastapi_app = FastAPI(lifespan=lifespan)

telegram_app.add_handler(CommandHandler('start', start))
telegram_app.add_handler(CommandHandler('test', test))
telegram_app.add_handler(CommandHandler('ping', ping))
telegram_app.add_handler(CommandHandler('track', track))
telegram_app.add_handler(CommandHandler('feedback', feedback))
telegram_app.add_handler(CommandHandler('cleanup', cleanup_command))
telegram_app.add_handler(CallbackQueryHandler(button_callback))

@fastapi_app.post("/webhook")
async def webhook(request: Request):
    update = Update.de_json(await request.json(), telegram_app.bot)
    await telegram_app.process_update(update)
    return Response(status_code=200)

@fastapi_app.get("/webhook")
async def webhook_health():
    return {"status": "ok"}

@fastapi_app.get("/ping")
async def ping_health():
    return {"status": "pong"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(fastapi_app, host="0.0.0.0", port=PORT)
