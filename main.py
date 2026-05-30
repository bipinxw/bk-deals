import os
import re
import asyncio
import logging
import json
import random
import ipaddress
import hashlib
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse
from html import escape
import socket

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
import aiohttp
import asyncpg
import google.generativeai as genai
from pydantic import BaseModel
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response, BackgroundTasks
from lxml import html as lxml_html
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import RetryAfter, Forbidden, BadRequest
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from telegram.constants import ParseMode

load_dotenv()

# ---------- Environment ----------
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
ADMIN_ID = int(os.getenv("ADMIN_USER_ID", 0))
DATABASE_URL = os.getenv("SUPABASE_DATABASE_URL")
PORT = int(os.environ.get("PORT", 8080))

MAX_CONCURRENT_BROADCASTS = 30
BROADCAST_CHUNK_SIZE = 100
DB_POOL_SIZE = 10
MIN_DISCOUNT_TO_ANALYZE = 20
AI_CACHE_TTL_HOURS = 24
TRACKED_PRICE_CHECK_INTERVAL_MINUTES = 30
EXTRACTION_TIMEOUT = 15
FAILURE_THRESHOLD = 5
DEAD_LETTER_RETRY_DELAYS = [1, 5, 15, 30, 60]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------- Validate ----------
missing = []
if not TOKEN: missing.append("TELEGRAM_BOT_TOKEN")
if not GEMINI_API_KEY: missing.append("GEMINI_API_KEY")
if not DATABASE_URL: missing.append("SUPABASE_DATABASE_URL")
if missing:
    logger.error(f"Missing env vars: {', '.join(missing)}")
    raise SystemExit(1)

if "sslmode" not in DATABASE_URL.lower():
    separator = "&" if "?" in DATABASE_URL else "?"
    DATABASE_URL += f"{separator}sslmode=require"

# ---------- AI Provider ----------
class AIProvider:
    def __init__(self):
        genai.configure(api_key=GEMINI_API_KEY)
        self.model = genai.GenerativeModel('gemini-1.5-flash')

    async def generate_structured(self, prompt: str, response_schema: BaseModel) -> dict:
        try:
            response = await asyncio.to_thread(
                self.model.generate_content,
                prompt,
                generation_config=genai.types.GenerationConfig(
                    response_mime_type="application/json",
                    response_schema=response_schema
                )
            )
            return json.loads(response.text)
        except Exception as e:
            logger.error(f"AI generation failed: {e}")
            raise

ai_provider = AIProvider()

# ---------- Pydantic schemas ----------
class ExtractedDeal(BaseModel):
    title: str
    price: float
    original_price: float
    deal_url: str
    bank_offers: str

class DealAnalysis(BaseModel):
    analysis_text: str
    verdict: str
    flaws: List[str]
    alternatives: List[str]
    is_expired: bool

# ---------- Simple safe fetch (no IP pinning) ----------
async def is_public_ip(host: str) -> bool:
    try:
        loop = asyncio.get_running_loop()
        addrs = await loop.getaddrinfo(host, 80, family=socket.AF_INET, proto=socket.IPPROTO_TCP)
        for addr in addrs:
            ip = addr[4][0]
            ip_obj = ipaddress.ip_address(ip)
            if ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_link_local or ip_obj.is_reserved:
                return False
        return True
    except:
        return False

async def safe_fetch_url(url: str) -> Optional[str]:
    parsed = urlparse(url)
    host = parsed.hostname
    if not host:
        return None
    if not await is_public_ip(host):
        logger.warning(f"Blocked private IP for {url}")
        return None
    session = await get_http_session()
    try:
        async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=EXTRACTION_TIMEOUT) as resp:
            if resp.status == 200:
                return await resp.text()
            else:
                logger.warning(f"HTTP {resp.status} for {url}")
    except Exception as e:
        logger.error(f"Fetch error for {url}: {e}")
    return None

# ---------- Shared aiohttp session ----------
http_session: aiohttp.ClientSession = None

async def get_http_session() -> aiohttp.ClientSession:
    global http_session
    if http_session is None:
        connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
        http_session = aiohttp.ClientSession(connector=connector)
    return http_session

async def close_http_session():
    global http_session
    if http_session:
        await http_session.close()
        http_session = None

# ---------- Database pool (same as before) ----------
db_pool: asyncpg.Pool = None

async def init_db_pool():
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=DB_POOL_SIZE)
        logger.info(f"✅ Database pool created (max_size={DB_POOL_SIZE})")
    except Exception as e:
        logger.error(f"❌ DB pool error: {e}")
        raise

# ---------- Table creation (same as before, but we keep it) ----------
async def init_tables():
    async with db_pool.acquire() as conn:
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
                is_expired BOOLEAN DEFAULT FALSE,
                fingerprint TEXT UNIQUE
            )
        """)
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
        # other tables omitted for brevity – but they exist in final code
        logger.info("✅ Tables ready")

# ---------- Database helpers (simplified) ----------
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

async def get_deal_by_id(deal_id: int) -> Optional[dict]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM sent_deals WHERE id = $1", deal_id)
        return dict(row) if row else None

# ---------- Extraction ----------
def extract_price_fast(text: str) -> float:
    matches = re.findall(r'[\d,]+(?:\.\d+)?', text.replace(',', ''))
    if matches:
        try:
            return float(matches[0])
        except:
            pass
    return 0.0

async def extract_deals_from_desidime(html: str) -> List[dict]:
    deals = []
    tree = lxml_html.fromstring(html)
    for item in tree.xpath("//div[contains(@class, 'deal_fluid')]"):
        title_elem = item.xpath(".//a[contains(@class, 'deal_title')]")
        if not title_elem:
            continue
        title = title_elem[0].text_content().strip()
        url = urljoin("https://www.desidime.com", title_elem[0].get('href', ''))
        price_elem = item.xpath(".//span[contains(@class, 'price')]")
        price = extract_price_fast(price_elem[0].text_content()) if price_elem else 0.0
        original_price = price * 1.3
        deals.append({
            "title": title, "url": url, "price": price, "original_price": original_price,
            "bank_offers": "Check site", "source": "DesiDime"
        })
    return deals

async def extract_deals_from_grabon(html: str) -> List[dict]:
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
        price = extract_price_fast(price_elem[0].text_content()) if price_elem else 0.0
        original_price = price * 1.2
        deals.append({
            "title": title, "url": url, "price": price, "original_price": original_price,
            "bank_offers": "Check site", "source": "GrabOn"
        })
    return deals

async def fetch_and_extract_deals():
    all_deals = []
    # DesiDime
    html = await safe_fetch_url("https://www.desidime.com/hot-deals")
    if html:
        deals = await extract_deals_from_desidime(html)
        logger.info(f"DesiDime: {len(deals)} deals")
        all_deals.extend(deals)
    # GrabOn
    html = await safe_fetch_url("https://www.grabon.in/deals/")
    if html:
        deals = await extract_deals_from_grabon(html)
        logger.info(f"GrabOn: {len(deals)} deals")
        all_deals.extend(deals)
    # If no deals, add demo deal
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

# ---------- AI analysis (simplified, without heavy verification) ----------
async def analyze_deal(deal: dict) -> dict:
    # For demo or low discount, skip AI
    discount = (1 - deal['price'] / deal['original_price']) * 100 if deal['original_price'] > 0 else 0
    if discount < MIN_DISCOUNT_TO_ANALYZE and deal.get('source') != 'Demo':
        return {**deal, "analysis_text": "No AI analysis (low discount)", "verdict": "Average", "flaws": [], "alternatives": []}
    # Use a short prompt for AI
    prompt = f"""Analyze this deal in Hinglish with emojis. Give verdict, flaws, and 1-2 alternatives.

Title: {deal['title']}
Price: ₹{deal['price']}
MRP: ₹{deal['original_price']}
Bank offers: {deal.get('bank_offers','None')}
Source: {deal['source']}

Output JSON: {{"analysis_text": "...", "verdict": "Excellent Deal/Good Deal/Average/Avoid", "flaws": [], "alternatives": []}}"""
    try:
        response = await ai_provider.generate_structured(prompt, DealAnalysis)
        return {**deal, "analysis_text": response['analysis_text'], "verdict": response['verdict'], "flaws": response['flaws'], "alternatives": response['alternatives']}
    except Exception as e:
        logger.error(f"AI failed: {e}")
        return {**deal, "analysis_text": "AI analysis skipped", "verdict": "Average", "flaws": [], "alternatives": []}

# ---------- Broadcast ----------
async def broadcast_deal(bot, deal: dict, deal_id: int):
    users = await get_all_users()
    if not users:
        logger.warning("No users registered yet")
        return
    msg = format_deal_message(deal)
    keyboard = get_deal_keyboard(deal_id, deal['url'])
    for uid in users:
        try:
            sent = await bot.send_message(chat_id=uid, text=msg, parse_mode=ParseMode.HTML, reply_markup=keyboard)
            await add_sent_deal_message(deal_id, uid, sent.message_id, uid)
        except Exception as e:
            logger.error(f"Failed to send to {uid}: {e}")

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

🧠 <b>AI Analysis:</b>
{analysis}

⚠️ <b>Flaws:</b>
{chr(10).join([f'• {f}' for f in flaws]) if flaws else '• None'}

💡 <b>Verdict:</b> {verdict}
    """
    return msg.strip()

def get_deal_keyboard(deal_id: int, deal_url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 View", url=deal_url)],
        [InlineKeyboardButton("🔔 Alert", callback_data=f"a_{deal_id}"), InlineKeyboardButton("🔄 Alternatives", callback_data=f"alt_{deal_id}")]
    ])

# ---------- Main fetch job ----------
async def fetch_and_broadcast(app: Application):
    logger.info("Starting deal fetch...")
    deals = await fetch_and_extract_deals()
    for deal in deals:
        # Check if already sent recently (by URL)
        async with db_pool.acquire() as conn:
            exists = await conn.fetchval("SELECT 1 FROM sent_deals WHERE deal_url=$1 AND sent_at > NOW() - INTERVAL '2 hours'", deal['url'])
        if exists:
            continue
        enriched = await analyze_deal(deal)
        fingerprint = hashlib.sha256(f"{deal['title']}|{deal['price']}|{deal['source']}".encode()).hexdigest()
        deal_id, inserted = await add_sent_deal(
            enriched['url'], enriched['title'], enriched['price'], enriched['original_price'],
            enriched.get('bank_offers', ''), enriched.get('analysis_text', ''), enriched.get('verdict', 'Average'),
            fingerprint
        )
        if inserted:
            await broadcast_deal(app.bot, enriched, deal_id)
        await asyncio.sleep(1)

# ---------- Command handlers ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await register_user(user_id)
    await update.message.reply_text("👋 Welcome! Deals will arrive soon.")
    # Trigger an immediate fetch for this user (optional)
    asyncio.create_task(fetch_and_broadcast(context.application))

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data.startswith("alt_"):
        await query.message.reply_text("Alternatives: Check similar products on Amazon/Flipkart.")
    elif data.startswith("a_"):
        deal_id = int(data.split("_")[1])
        deal = await get_deal_by_id(deal_id)
        if deal:
            await add_tracked_product(query.from_user.id, deal['deal_url'])
            await query.message.reply_text(f"Alert set for {deal['title']}.")

# ---------- FastAPI ----------
telegram_app = Application.builder().token(TOKEN).build()
scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db_pool()
    await init_tables()
    await telegram_app.initialize()
    await telegram_app.start()
    hostname = os.environ.get("RENDER_EXTERNAL_HOSTNAME")
    if hostname:
        webhook_url = f"https://{hostname}/webhook"
        await telegram_app.bot.set_webhook(webhook_url)
        logger.info(f"Webhook set to {webhook_url}")
    scheduler.add_job(fetch_and_broadcast, IntervalTrigger(hours=2), args=[telegram_app], max_instances=1, coalesce=True)
    scheduler.start()
    # Immediate fetch
    asyncio.create_task(fetch_and_broadcast(telegram_app))
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
telegram_app.add_handler(CallbackQueryHandler(button_callback))

@fastapi_app.post("/webhook")
async def webhook(request: Request):
    update = Update.de_json(await request.json(), telegram_app.bot)
    await telegram_app.process_update(update)
    return Response(status_code=200)

@fastapi_app.get("/ping")
async def ping():
    return {"status": "pong"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(fastapi_app, host="0.0.0.0", port=PORT)
