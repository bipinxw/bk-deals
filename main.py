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
from fastapi import FastAPI, Request, Response
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
if not DATABASE_URL: missing.append("SUPABASE_DATABASE_URL")
if missing:
    logger.error(f"Missing env vars: {', '.join(missing)}")
    raise SystemExit(1)

if "sslmode" not in DATABASE_URL.lower():
    separator = "&" if "?" in DATABASE_URL else "?"
    DATABASE_URL += f"{separator}sslmode=require"

# ---------- AI Provider (Gemini 3 Flash) ----------
AI_AVAILABLE = False
try:
    if GEMINI_API_KEY:
        genai.configure(api_key=GEMINI_API_KEY)
        # Try gemini-3-flash first, fallback to 1.5-flash
        try:
            gemini_model = genai.GenerativeModel('gemini-3-flash')
            logger.info("✅ Gemini 3 Flash available")
        except:
            gemini_model = genai.GenerativeModel('gemini-1.5-flash')
            logger.info("✅ Gemini 1.5 Flash available")
        AI_AVAILABLE = True
    else:
        logger.warning("No GEMINI_API_KEY – AI analysis disabled")
except Exception as e:
    logger.warning(f"Gemini init failed: {e}")

class DealAnalysis(BaseModel):
    analysis_text: str
    verdict: str  # "Excellent Deal", "Good Deal", "Average", "Avoid"
    flaws: List[str]
    alternatives: List[str]

async def analyze_with_ai(deal: dict) -> dict:
    if not AI_AVAILABLE:
        return {**deal, "analysis_text": "AI not configured", "verdict": "Average", "flaws": [], "alternatives": []}
    prompt = f"""Analyze this deal in Hinglish with emojis.
Title: {deal['title']}
Price: ₹{deal['price']}
MRP: ₹{deal['original_price']}
Bank offers: {deal.get('bank_offers','None')}
Source: {deal['source']}
Output JSON: {{"analysis_text": "...", "verdict": "Excellent Deal/Good Deal/Average/Avoid", "flaws": [], "alternatives": []}}"""
    try:
        response = await asyncio.to_thread(gemini_model.generate_content, prompt)
        text = response.text.strip()
        # Extract JSON block
        json_match = re.search(r'\{.*\}', text, re.DOTALL)
        if json_match:
            data = json.loads(json_match.group())
            return {**deal,
                    "analysis_text": data.get('analysis_text', ''),
                    "verdict": data.get('verdict', 'Average'),
                    "flaws": data.get('flaws', []),
                    "alternatives": data.get('alternatives', [])}
    except Exception as e:
        logger.error(f"AI analysis failed: {e}")
    return {**deal, "analysis_text": "AI analysis temporarily unavailable", "verdict": "Average", "flaws": [], "alternatives": []}

# ---------- HTTP session ----------
http_session: aiohttp.ClientSession = None

async def get_http_session() -> aiohttp.ClientSession:
    global http_session
    if http_session is None:
        connector = aiohttp.TCPConnector(limit=15)
        http_session = aiohttp.ClientSession(connector=connector)
    return http_session

async def close_http_session():
    global http_session
    if http_session:
        await http_session.close()
        http_session = None

async def is_public_host(host: str) -> bool:
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

async def safe_fetch(url: str) -> Optional[str]:
    parsed = urlparse(url)
    host = parsed.hostname
    if not host or not await is_public_host(host):
        logger.warning(f"Blocked non-public host: {host}")
        return None
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
        logger.info(f"✅ Database pool created (max_size={DB_POOL_SIZE})")
    except Exception as e:
        logger.error(f"❌ DB pool error: {e}")
        raise

async def ensure_schema():
    async with db_pool.acquire() as conn:
        # sent_deals
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
        # Add fingerprint column if missing
        await conn.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='sent_deals' AND column_name='fingerprint') THEN
                    ALTER TABLE sent_deals ADD COLUMN fingerprint TEXT UNIQUE;
                END IF;
            END
            $$;
        """)
        # sent_deal_messages
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS sent_deal_messages (
                id SERIAL PRIMARY KEY,
                deal_id BIGINT REFERENCES sent_deals(id) ON DELETE CASCADE,
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
                target_price NUMERIC(12,2),
                last_price NUMERIC(12,2),
                last_check TIMESTAMPTZ,
                UNIQUE(user_id, product_url)
            )
        """)
        # price_history
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS price_history (
                id BIGSERIAL PRIMARY KEY,
                product_key TEXT,
                price NUMERIC(12,2),
                recorded_at TIMESTAMPTZ,
                source TEXT
            )
        """)
        # ai_analysis_cache (optional)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS ai_analysis_cache (
                url_hash TEXT PRIMARY KEY,
                analysis_summary TEXT,
                created_at TIMESTAMPTZ
            )
        """)
        logger.info("✅ Database schema ready")

# ---------- Database helpers ----------
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
        existing = await conn.fetchrow("""
            UPDATE sent_deals SET title=$2,price=$3,original_price=$4,bank_offers=$5,analysis_summary=$6,verdict=$7,sent_at=$8,last_validated=$9,is_expired=$10,fingerprint=$11
            WHERE deal_url=$1 RETURNING id
        """, deal_url, title, price, original_price, bank_offers, analysis, verdict,
            datetime.now(timezone.utc), datetime.now(timezone.utc), False, fingerprint)
        return existing['id'], False

async def add_sent_deal_message(deal_id: int, user_id: int, message_id: int, chat_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO sent_deal_messages (deal_id, user_id, message_id, chat_id)
            VALUES ($1,$2,$3,$4) ON CONFLICT (deal_id, user_id) DO UPDATE SET message_id=EXCLUDED.message_id, chat_id=EXCLUDED.chat_id
        """, deal_id, user_id, message_id, chat_id)

async def get_all_messages_for_deal(deal_id: int) -> List[dict]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id, message_id, chat_id FROM sent_deal_messages WHERE deal_id = $1", deal_id)
        return [dict(r) for r in rows]

async def update_deal_expiry(deal_id: int, is_expired: bool):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE sent_deals SET is_expired=$1, last_validated=$2 WHERE id=$3", is_expired, datetime.now(timezone.utc), deal_id)

async def get_deal_by_id(deal_id: int) -> Optional[dict]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM sent_deals WHERE id = $1", deal_id)
        return dict(row) if row else None

async def delete_old_messages(older_than_days: int = 60):
    cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT m.message_id, m.chat_id FROM sent_deal_messages m JOIN sent_deals d ON m.deal_id = d.id WHERE d.sent_at < $1", cutoff)
        await conn.execute("DELETE FROM sent_deals WHERE sent_at < $1", cutoff)
        return [dict(r) for r in rows]

async def add_tracked_product(user_id: int, url: str, target_price: float = 0):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO tracked_products (user_id, product_url, target_price, last_price, last_check)
            VALUES ($1,$2,$3,0,NOW())
            ON CONFLICT (user_id, product_url) DO UPDATE SET target_price=EXCLUDED.target_price
        """, user_id, url, target_price)

async def get_tracked_products() -> List[dict]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, user_id, product_url, target_price, last_price FROM tracked_products")
        return [dict(r) for r in rows]

async def update_tracked_price(track_id: int, new_price: float):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE tracked_products SET last_price=$1, last_check=NOW() WHERE id=$2", new_price, track_id)

async def record_price_history(product_key: str, price: float, source: str):
    async with db_pool.acquire() as conn:
        await conn.execute("INSERT INTO price_history (product_key, price, recorded_at, source) VALUES ($1,$2,NOW(),$3)", product_key, price, source)

# ---------- Deal extraction ----------
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

async def extract_amazon(html: str) -> List[dict]:
    # simplified – Amazon often blocks; we keep as placeholder
    return []

async def extract_flipkart(html: str) -> List[dict]:
    return []

async def extract_reddit_json(html: str) -> List[dict]:
    deals = []
    try:
        data = json.loads(html)
        for child in data.get('data', {}).get('children', []):
            post = child['data']
            title = post['title']
            url = post.get('url', '')
            if "amazon" in url.lower() or "flipkart" in url.lower():
                deals.append({
                    "title": title, "url": url, "price": 0, "original_price": 0,
                    "bank_offers": "Check comments", "source": "Reddit"
                })
    except:
        pass
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
    # Amazon (optional)
    html = await safe_fetch("https://www.amazon.in/deals")
    if html:
        deals = await extract_amazon(html)
        logger.info(f"Amazon: {len(deals)} deals")
        all_deals.extend(deals)
    # Flipkart
    html = await safe_fetch("https://www.flipkart.com/offers-store")
    if html:
        deals = await extract_flipkart(html)
        logger.info(f"Flipkart: {len(deals)} deals")
        all_deals.extend(deals)
    # Reddit
    html = await safe_fetch("https://www.reddit.com/r/IndiaDeals/hot.json?limit=10")
    if html:
        deals = await extract_reddit_json(html)
        logger.info(f"Reddit: {len(deals)} deals")
        all_deals.extend(deals)
    # if no deals, add demo
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

# ---------- Message formatting ----------
def format_deal_message(deal: dict) -> str:
    title = escape(deal['title'])
    price = f"₹{deal['price']:,.0f}"
    original = f"<s>MRP ₹{deal['original_price']:,.0f}</s>" if deal['original_price'] > deal['price'] else f"MRP ₹{deal['original_price']:,.0f}"
    discount = int((1 - deal['price']/deal['original_price'])*100) if deal['original_price'] > 0 else 0
    analysis = escape(deal.get('analysis_text', 'No analysis'))
    verdict = escape(deal.get('verdict', 'Average'))
    flaws = [escape(f) for f in deal.get('flaws', [])]
    alt_text = escape(deal.get('alternatives', []))
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

def get_keyboard(deal_id: int, deal_url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 View", url=deal_url)],
        [InlineKeyboardButton("🔔 Alert", callback_data=f"alert_{deal_id}"), InlineKeyboardButton("🔄 Alternatives", callback_data=f"alt_{deal_id}")]
    ])

# ---------- Broadcasting ----------
async def broadcast_deal(bot, deal: dict, deal_id: int):
    users = await get_all_users()
    if not users:
        logger.warning("No registered users")
        return
    msg = format_deal_message(deal)
    keyboard = get_keyboard(deal_id, deal['url'])
    for user_id in users:
        try:
            sent = await bot.send_message(chat_id=user_id, text=msg, parse_mode=ParseMode.HTML, reply_markup=keyboard)
            await add_sent_deal_message(deal_id, user_id, sent.message_id, user_id)
            logger.info(f"Sent to user {user_id}")
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after)
            try:
                sent = await bot.send_message(chat_id=user_id, text=msg, parse_mode=ParseMode.HTML, reply_markup=keyboard)
                await add_sent_deal_message(deal_id, user_id, sent.message_id, user_id)
            except Exception as e2:
                logger.error(f"Retry failed for {user_id}: {e2}")
        except Forbidden:
            # user blocked bot
            async with db_pool.acquire() as conn:
                await conn.execute("DELETE FROM users WHERE user_id = $1", user_id)
            logger.warning(f"Removed blocked user {user_id}")
        except Exception as e:
            logger.error(f"Failed to send to {user_id}: {e}")

# ---------- Main fetch job ----------
async def fetch_and_broadcast(app: Application):
    logger.info("Starting deal fetch cycle...")
    deals = await fetch_all_deals()
    for deal in deals:
        # Check duplicate within 2 hours
        async with db_pool.acquire() as conn:
            exists = await conn.fetchval("SELECT 1 FROM sent_deals WHERE deal_url=$1 AND sent_at > NOW() - INTERVAL '2 hours'", deal['url'])
        if exists:
            continue
        # AI analysis
        analyzed = await analyze_with_ai(deal)
        fingerprint = hashlib.sha256(f"{deal['title']}|{deal['price']}|{deal['source']}".encode()).hexdigest()
        deal_id, inserted = await add_sent_deal(
            analyzed['url'], analyzed['title'], analyzed['price'], analyzed['original_price'],
            analyzed.get('bank_offers', ''), analyzed.get('analysis_text', ''), analyzed.get('verdict', 'Average'),
            fingerprint
        )
        if inserted:
            await broadcast_deal(app.bot, analyzed, deal_id)
        await asyncio.sleep(1)
    logger.info("Fetch cycle completed")

# ---------- Revalidation job (expiry) ----------
async def revalidate_deals(app: Application):
    async with db_pool.acquire() as conn:
        active = await conn.fetch("SELECT * FROM sent_deals WHERE is_expired = FALSE")
    for row in active:
        deal = dict(row)
        # quick expiry check by fetching page (simplified)
        html = await safe_fetch(deal['deal_url'])
        if html:
            if "out of stock" in html.lower() or "deal ended" in html.lower():
                await update_deal_expiry(deal['id'], True)
                # Edit all sent copies
                messages = await get_all_messages_for_deal(deal['id'])
                expired_deal = {**deal, "is_expired": True, "analysis_text": deal.get('analysis_summary', '')}
                new_text = format_deal_message(expired_deal)
                for msg in messages:
                    try:
                        await app.bot.edit_message_text(
                            chat_id=msg['chat_id'],
                            message_id=msg['message_id'],
                            text=new_text,
                            parse_mode=ParseMode.HTML,
                            reply_markup=get_keyboard(deal['id'], deal['deal_url'])
                        )
                    except Exception as e:
                        if "message is not modified" not in str(e).lower():
                            logger.error(f"Edit failed for {msg['user_id']}: {e}")
        await asyncio.sleep(1)

# ---------- Price tracking job ----------
async def check_tracked_products(app: Application):
    products = await get_tracked_products()
    for prod in products:
        html = await safe_fetch(prod['product_url'])
        if not html:
            continue
        tree = lxml_html.fromstring(html)
        price_elem = tree.xpath('//span[contains(@class,"price")] | //div[contains(@class,"price")] | //meta[@property="product:price:amount"]')
        if price_elem:
            price_text = price_elem[0].text_content() if hasattr(price_elem[0], 'text_content') else price_elem[0].get('content', '')
            current_price = extract_price(price_text)
            if current_price > 0:
                product_key = hashlib.md5(prod['product_url'].encode()).hexdigest()
                await record_price_history(product_key, current_price, "tracked")
                if prod['last_price'] == 0:
                    await update_tracked_price(prod['id'], current_price)
                elif current_price < prod['last_price'] or (prod['target_price'] > 0 and current_price <= prod['target_price']):
                    msg = f"🔔 *Price Alert!*\n\n{prod['product_url']}\nPrevious: ₹{prod['last_price']:,.0f}\nNow: ₹{current_price:,.0f}"
                    try:
                        await app.bot.send_message(chat_id=prod['user_id'], text=msg, parse_mode=ParseMode.MARKDOWN)
                    except Exception as e:
                        logger.error(f"Alert failed: {e}")
                    await update_tracked_price(prod['id'], current_price)
                else:
                    await update_tracked_price(prod['id'], current_price)
        await asyncio.sleep(random.uniform(1, 3))

# ---------- Cleanup job ----------
async def cleanup_old_messages(app: Application):
    old = await delete_old_messages(60)
    for row in old:
        try:
            await app.bot.delete_message(chat_id=row['chat_id'], message_id=row['message_id'])
        except:
            pass
        await asyncio.sleep(0.05)

# ---------- Telegram handlers ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await register_user(user_id)
    await update.message.reply_text("👋 Welcome! Deals will be sent soon. Use /track <url> to monitor products.")
    # Trigger immediate fetch for first user
    asyncio.create_task(fetch_and_broadcast(context.application))

async def track(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /track <product_url>")
        return
    url = context.args[0]
    parsed = urlparse(url)
    if parsed.scheme not in ('http', 'https'):
        await update.message.reply_text("Only http/https URLs allowed")
        return
    target = float(context.args[1]) if len(context.args) > 1 else 0
    await add_tracked_product(update.effective_user.id, url, target)
    await update.message.reply_text(f"🔔 Tracking started for {url[:60]}...")

async def feedback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Thanks for your feedback!")

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data.startswith("alt_"):
        await query.message.reply_text("Alternatives: Check similar products on Amazon/Flipkart.")
    elif data.startswith("alert_"):
        deal_id = int(data.split("_")[1])
        deal = await get_deal_by_id(deal_id)
        if deal:
            await add_tracked_product(query.from_user.id, deal['deal_url'], 0)
            await query.message.reply_text(f"Alert set for {deal['title']}.")

# ---------- FastAPI webhook ----------
telegram_app = Application.builder().token(TOKEN).build()
scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db_pool()
    await ensure_schema()
    await telegram_app.initialize()
    await telegram_app.start()
    hostname = os.environ.get("RENDER_EXTERNAL_HOSTNAME")
    if hostname:
        webhook_url = f"https://{hostname}/webhook"
        await telegram_app.bot.set_webhook(webhook_url)
        logger.info(f"Webhook set to {webhook_url}")
    else:
        logger.warning("No hostname, webhook not set")
    scheduler.add_job(fetch_and_broadcast, IntervalTrigger(hours=2), args=[telegram_app], max_instances=1, coalesce=True)
    scheduler.add_job(revalidate_deals, IntervalTrigger(hours=4), args=[telegram_app], max_instances=1, coalesce=True)
    scheduler.add_job(check_tracked_products, IntervalTrigger(minutes=TRACKED_PRICE_CHECK_INTERVAL_MINUTES), args=[telegram_app], max_instances=1, coalesce=True)
    scheduler.add_job(cleanup_old_messages, CronTrigger(hour=3, minute=0), args=[telegram_app], max_instances=1, coalesce=True)
    scheduler.start()
    # Immediate fetch after startup
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
telegram_app.add_handler(CommandHandler('track', track))
telegram_app.add_handler(CommandHandler('feedback', feedback))
telegram_app.add_handler(CallbackQueryHandler(button_callback))

@fastapi_app.post("/webhook")
async def webhook(request: Request):
    update = Update.de_json(await request.json(), telegram_app.bot)
    await telegram_app.process_update(update)
    return Response(status_code=200)

@fastapi_app.get("/health")
async def health():
    return {"status": "alive"}

@fastapi_app.get("/ping")
async def ping():
    return {"status": "pong"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(fastapi_app, host="0.0.0.0", port=PORT)
