import os
import re
import asyncio
import logging
import json
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from urllib.parse import urljoin
import socket

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

load_dotenv()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
ADMIN_ID = int(os.getenv("ADMIN_USER_ID", 0))
DATABASE_URL = os.getenv("SUPABASE_DATABASE_URL")
PORT = int(os.environ.get("PORT", 8080))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------- Validate environment ----------
missing = []
if not TOKEN: missing.append("TELEGRAM_BOT_TOKEN")
if not GEMINI_API_KEY: missing.append("GEMINI_API_KEY")
if not DATABASE_URL: missing.append("SUPABASE_DATABASE_URL")
if missing:
    logger.error(f"Missing env vars: {', '.join(missing)}")
    raise SystemExit(1)

# Mask the database URL for logging (hide password)
masked_url = re.sub(r':[^@]+@', ':****@', DATABASE_URL)
logger.info(f"Using DATABASE_URL: {masked_url}")

# Ensure SSL mode is set
if "sslmode" not in DATABASE_URL.lower():
    DATABASE_URL += "&sslmode=require" if "?" in DATABASE_URL else "?sslmode=require"
    logger.info("Added sslmode=require")

# ---------- AI ----------
genai.configure(api_key=GEMINI_API_KEY)
gemini_model = genai.GenerativeModel('gemini-1.5-flash')

# ---------- PostgreSQL connection pool ----------
db_pool: asyncpg.Pool = None

async def init_db_pool():
    global db_pool
    try:
        # Test DNS resolution before connecting
        hostname = DATABASE_URL.split('@')[1].split('/')[0].split(':')[0]
        logger.info(f"Resolving hostname: {hostname}")
        try:
            addr_info = socket.getaddrinfo(hostname, 5432, socket.AF_INET)
            logger.info(f"Resolved to IPv4: {addr_info[0][4][0]}")
        except Exception as dns_err:
            logger.error(f"DNS resolution failed for {hostname}: {dns_err}")
            raise
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
        logger.info("✅ Database connection pool created")
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        raise

async def init_tables():
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (user_id BIGINT PRIMARY KEY);
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

# ---------- Database helpers (same as before) ----------
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

# ---------- Scraping (unchanged) ----------
# [Include all scraping functions from previous version exactly as they were]
# (to keep this message within length, assume they are present - you can copy from my last complete code)
# For brevity, I'll note that all fetch_deals_from_* functions remain identical.

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

# ... (all other fetch functions remain identical – copy from previous final code)

# ---------- AI Analysis (unchanged) ----------
# ... (copy from previous final code)

# ---------- Message Formatting (unchanged) ----------
# ... (copy from previous final code)

# ---------- Broadcasting (unchanged) ----------
# ... (copy from previous final code)

# ---------- Background Jobs (unchanged) ----------
# ... (copy from previous final code)

# ---------- FastAPI Webhook (unchanged) ----------
telegram_app = Application.builder().token(TOKEN).build()
scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
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
    scheduler.shutdown()
    if db_pool:
        await db_pool.close()
    await telegram_app.shutdown()

fastapi_app = FastAPI(lifespan=lifespan)

# ---------- Handlers ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await register_user(update.effective_user.id)
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
