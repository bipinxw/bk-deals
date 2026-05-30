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
from typing import List, Dict, Optional, Tuple, Any
from urllib.parse import urljoin, urlparse
from html import escape
import socket

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
import aiohttp
import asyncpg
import google.generativeai as genai
from pydantic import BaseModel, ValidationError
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response, BackgroundTasks
from lxml import html as lxml_html
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import RetryAfter, Forbidden, BadRequest
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from telegram.constants import ParseMode

load_dotenv()

# ---------- Environment variables ----------
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
ADMIN_ID = int(os.getenv("ADMIN_USER_ID", 0))
DATABASE_URL = os.getenv("SUPABASE_DATABASE_URL")
PORT = int(os.environ.get("PORT", 8080))

# Configuration
MAX_CONCURRENT_BROADCASTS = 30
BROADCAST_CHUNK_SIZE = 100
DB_POOL_SIZE = 10
HTTP_CONNECTOR_LIMIT = 50
MIN_DISCOUNT_TO_ANALYZE = 20
AI_CACHE_TTL_HOURS = 24
TRACKED_PRICE_CHECK_INTERVAL_MINUTES = 30
EXTRACTION_TIMEOUT = 15
FAILURE_THRESHOLD = 5
DEAD_LETTER_RETRY_DELAYS = [1, 5, 15, 30, 60]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------- Validation ----------
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

# ---------- AI Provider abstraction ----------
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

# ---------- Shared aiohttp session ----------
http_session: aiohttp.ClientSession = None

async def get_http_session() -> aiohttp.ClientSession:
    global http_session
    if http_session is None:
        connector = aiohttp.TCPConnector(limit=HTTP_CONNECTOR_LIMIT, ttl_dns_cache=300)
        http_session = aiohttp.ClientSession(connector=connector)
    return http_session

async def close_http_session():
    global http_session
    if http_session:
        await http_session.close()
        http_session = None

# ---------- SSRF protection (DNS rebinding resistant) ----------
async def resolve_and_pin_ip(url: str) -> Optional[str]:
    parsed = urlparse(url)
    host = parsed.hostname
    if not host:
        return None
    try:
        loop = asyncio.get_running_loop()
        addrs = await loop.getaddrinfo(host, 80, family=socket.AF_INET, proto=socket.IPPROTO_TCP)
        for addr in addrs:
            ip = addr[4][0]
            ip_obj = ipaddress.ip_address(ip)
            if not (ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_link_local or ip_obj.is_reserved):
                return ip
        return None
    except socket.gaierror:
        return None

async def safe_fetch_url(url: str) -> Optional[str]:
    pinned_ip = await resolve_and_pin_ip(url)
    if not pinned_ip:
        logger.warning(f"Could not resolve safe IP for {url}")
        return None
    parsed = urlparse(url)
    ip_url = f"{parsed.scheme}://{pinned_ip}{parsed.path or ''}"
    if parsed.query:
        ip_url += f"?{parsed.query}"
    headers = {"Host": parsed.hostname, "User-Agent": "Mozilla/5.0"}
    session = await get_http_session()
    try:
        async with session.get(ip_url, headers=headers, timeout=EXTRACTION_TIMEOUT) as resp:
            if resp.status == 200:
                return await resp.text()
            else:
                logger.warning(f"IP fetch failed for {url}: HTTP {resp.status}")
    except Exception as e:
        logger.error(f"IP fetch error: {e}")
    return None

# ---------- Database connection and tables ----------
db_pool: asyncpg.Pool = None

async def init_db_pool():
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=DB_POOL_SIZE)
        logger.info(f"✅ Database pool created (max_size={DB_POOL_SIZE})")
    except Exception as e:
        logger.error(f"❌ DB pool error: {e}")
        raise

async def init_tables():
    async with db_pool.acquire() as conn:
        # 1. Create sent_deals (no foreign keys yet)
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
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_sent_deals_url ON sent_deals(deal_url);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_sent_deals_sent_at ON sent_deals(sent_at);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_sent_deals_expired ON sent_deals(is_expired);")

        # 2. Create sent_deal_messages (deal_id column without foreign key initially)
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
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_sent_messages_deal ON sent_deal_messages(deal_id);")

        # 3. Now add the foreign key constraint (sent_deals.id already exists)
        await conn.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.table_constraints
                    WHERE constraint_name = 'fk_sent_deal_messages_deal_id'
                ) THEN
                    ALTER TABLE sent_deal_messages
                    ADD CONSTRAINT fk_sent_deal_messages_deal_id
                    FOREIGN KEY (deal_id) REFERENCES sent_deals(id) ON DELETE CASCADE;
                END IF;
            END
            $$;
        """)

        # 4. Create other tables
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS deal_snapshots (
                id BIGSERIAL PRIMARY KEY,
                source TEXT,
                url TEXT,
                raw_html TEXT,
                raw_text TEXT,
                captured_at TIMESTAMPTZ
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS source_health (
                source TEXT PRIMARY KEY,
                successes INTEGER DEFAULT 0,
                failures INTEGER DEFAULT 0,
                last_success TIMESTAMPTZ,
                last_failure TIMESTAMPTZ,
                consecutive_failures INTEGER DEFAULT 0,
                downgraded BOOLEAN DEFAULT FALSE
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS failed_jobs (
                id BIGSERIAL PRIMARY KEY,
                job_type TEXT,
                payload JSONB,
                error TEXT,
                retry_count INTEGER DEFAULT 0,
                next_retry_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS price_history (
                id BIGSERIAL PRIMARY KEY,
                product_key TEXT,
                price NUMERIC(12,2),
                recorded_at TIMESTAMPTZ,
                source TEXT
            )
        """)
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_price_history_product_key ON price_history(product_key);")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS ai_analysis_cache (
                url_hash TEXT PRIMARY KEY,
                deal_url TEXT,
                analysis_summary TEXT,
                verdict TEXT,
                flaws TEXT,
                alternatives TEXT,
                is_expired BOOLEAN,
                created_at TIMESTAMPTZ
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY
            )
        """)
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
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_tracked_products_user ON tracked_products(user_id);")

        logger.info("✅ All tables ready")

# ---------- Database helpers ----------
async def register_user(user_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute("INSERT INTO users (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING", user_id)

async def remove_user(user_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM users WHERE user_id = $1", user_id)

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

async def get_all_sent_messages_for_deal(deal_id: int) -> List[dict]:
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

async def get_active_deals_to_revalidate(cutoff_days: int = 7):
    cutoff = datetime.now(timezone.utc) - timedelta(days=cutoff_days)
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM sent_deals WHERE is_expired = FALSE AND sent_at > $1", cutoff)
        return [dict(r) for r in rows]

async def delete_old_deals(cutoff_days: int = 60):
    cutoff = datetime.now(timezone.utc) - timedelta(days=cutoff_days)
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT m.message_id, m.chat_id FROM sent_deal_messages m JOIN sent_deals d ON m.deal_id = d.id WHERE d.sent_at < $1", cutoff)
        await conn.execute("DELETE FROM sent_deals WHERE sent_at < $1", cutoff)
        return [dict(r) for r in rows]

async def add_tracked_product(user_id: int, url: str):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO tracked_products (user_id, product_url, target_price, last_price, last_check)
            VALUES ($1,$2,$3,$4,$5) ON CONFLICT (user_id, product_url) DO UPDATE SET last_check=EXCLUDED.last_check
        """, user_id, url, 0, 0, datetime.now(timezone.utc))

async def get_tracked_products() -> List[dict]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, user_id, product_url, target_price, last_price FROM tracked_products")
        return [dict(r) for r in rows]

async def update_tracked_product_price(track_id: int, new_price: float):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE tracked_products SET last_price=$1, last_check=$2 WHERE id=$3", new_price, datetime.now(timezone.utc), track_id)

# ---------- Source health ----------
async def record_source_success(source: str):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO source_health (source, successes, last_success, failures, consecutive_failures, downgraded)
            VALUES ($1,1,$2,0,0,FALSE)
            ON CONFLICT (source) DO UPDATE SET
                successes = source_health.successes + 1,
                last_success = $2,
                consecutive_failures = 0,
                downgraded = FALSE
        """, source, datetime.now(timezone.utc))

async def record_source_failure(source: str):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO source_health (source, failures, last_failure, consecutive_failures, downgraded)
            VALUES ($1,1,$2,1,FALSE)
            ON CONFLICT (source) DO UPDATE SET
                failures = source_health.failures + 1,
                last_failure = $2,
                consecutive_failures = source_health.consecutive_failures + 1,
                downgraded = (source_health.consecutive_failures + 1) >= $3
        """, source, datetime.now(timezone.utc), FAILURE_THRESHOLD)

async def is_source_downgraded(source: str) -> bool:
    async with db_pool.acquire() as conn:
        row = await conn.fetchval("SELECT downgraded FROM source_health WHERE source = $1", source)
        return row or False

# ---------- Dead letter queue ----------
async def add_failed_job(job_type: str, payload: dict, error: str):
    next_retry = datetime.now(timezone.utc) + timedelta(minutes=DEAD_LETTER_RETRY_DELAYS[0])
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO failed_jobs (job_type, payload, error, retry_count, next_retry_at, created_at)
            VALUES ($1,$2,$3,0,$4,$5)
        """, job_type, json.dumps(payload), error, next_retry, datetime.now(timezone.utc))

async def retry_failed_jobs():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, job_type, payload, retry_count FROM failed_jobs WHERE next_retry_at <= NOW()")
        for row in rows:
            job_type = row['job_type']
            payload = json.loads(row['payload'])
            retry_count = row['retry_count'] + 1
            if retry_count >= len(DEAD_LETTER_RETRY_DELAYS):
                await conn.execute("DELETE FROM failed_jobs WHERE id = $1", row['id'])
                logger.error(f"Job {job_type} permanently failed after {retry_count} retries")
                continue
            next_retry = datetime.now(timezone.utc) + timedelta(minutes=DEAD_LETTER_RETRY_DELAYS[retry_count])
            await conn.execute("UPDATE failed_jobs SET retry_count=$1, next_retry_at=$2 WHERE id=$3", retry_count, next_retry, row['id'])
            if job_type == "extract":
                await extraction_queue.put(payload)
            elif job_type == "analyze":
                await analysis_queue.put(payload)
            elif job_type == "broadcast":
                await broadcast_queue.put(payload)

# ---------- Fingerprint deduplication ----------
def generate_deal_fingerprint(title: str, price: float, source: str) -> str:
    normalized = re.sub(r'[^a-z0-9]', '', title.lower())
    return hashlib.sha256(f"{normalized}|{price:.2f}|{source}".encode()).hexdigest()

async def is_deal_seen(fingerprint: str) -> bool:
    async with db_pool.acquire() as conn:
        return bool(await conn.fetchval("SELECT 1 FROM sent_deals WHERE fingerprint = $1", fingerprint))

# ---------- Price history ----------
async def record_price_history(product_key: str, price: float, source: str):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO price_history (product_key, price, recorded_at, source)
            VALUES ($1,$2,$3,$4)
        """, product_key, price, datetime.now(timezone.utc), source)

# ---------- Fast extraction (no confidence logic) ----------
def extract_price_fast(text: str) -> float:
    matches = re.findall(r'[\d,]+(?:\.\d+)?', text.replace(',', ''))
    if matches:
        try:
            return float(matches[0])
        except:
            pass
    return 0.0

async def fast_extract_deals_from_html(html_content: str, source: str) -> List[dict]:
    deals = []
    try:
        tree = lxml_html.fromstring(html_content)
        if source == "DesiDime":
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
                    "bank_offers": "Check site", "rating": "4.0", "source": source
                })
        # For other sources, implement similar logic – placeholder
    except Exception as e:
        logger.warning(f"Fast extraction failed for {source}: {e}")
    return deals

async def ai_extract_deals_from_html(html_content: str, source: str, url: str) -> List[dict]:
    tree = lxml_html.fromstring(html_content)
    for elem in tree.xpath('//script|//style'):
        if elem.getparent() is not None:
            elem.getparent().remove(elem)
    body_text = tree.xpath('//body')[0].text_content() if tree.xpath('//body') else tree.text_content()
    sample = body_text[:4000]
    prompt = f"""Extract all product deals from this page. Return JSON array of objects with fields: title, price (numeric), original_price (numeric), deal_url (absolute), bank_offers (string). If a field is missing, use null or 0.

Source: {source}
URL: {url}
HTML sample:
{sample}
"""
    try:
        extracted = await ai_provider.generate_structured(prompt, List[ExtractedDeal])
        for d in extracted:
            d['source'] = source
            if 'deal_url' not in d or not d['deal_url']:
                d['deal_url'] = url
        return extracted
    except Exception as e:
        logger.error(f"AI extraction failed for {url}: {e}")
        return []

async def extract_deals_with_fallback(source: str, url: str, html: str) -> List[dict]:
    deals = await fast_extract_deals_from_html(html, source)
    if deals and all(d.get('price', 0) > 0 for d in deals):
        await record_source_success(source)
        return deals
    if await is_source_downgraded(source):
        logger.info(f"Source {source} is downgraded – using AI extraction")
        deals = await ai_extract_deals_from_html(html, source, url)
        if deals:
            await record_source_success(source)
        else:
            await record_source_failure(source)
            await add_failed_job("extract", {"source": source, "url": url, "html_snippet": html[:200]}, "AI extraction returned empty")
        return deals
    else:
        await record_source_failure(source)
        logger.info(f"Fast extraction failed for {source}, trying AI fallback")
        deals = await ai_extract_deals_from_html(html, source, url)
        if deals:
            await record_source_success(source)
        else:
            await add_failed_job("extract", {"source": source, "url": url, "html_snippet": html[:200]}, "Both fast and AI extraction failed")
        return deals

async def store_snapshot(source: str, url: str, html: str, text: str):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO deal_snapshots (source, url, raw_html, raw_text, captured_at)
            VALUES ($1,$2,$3,$4,$5)
        """, source, url, html, text, datetime.now(timezone.utc))

async def analyze_deal_with_ai(deal: dict) -> dict:
    url_hash = hashlib.md5(deal['url'].encode()).hexdigest()
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT analysis_summary, verdict, flaws, alternatives, is_expired FROM ai_analysis_cache WHERE url_hash = $1 AND created_at > NOW() - INTERVAL '24 hours'", url_hash)
        if row:
            return {**deal, "analysis_text": row['analysis_summary'], "verdict": row['verdict'],
                    "flaws": json.loads(row['flaws']), "alternatives": json.loads(row['alternatives']), "is_expired": row['is_expired']}
    discount = (1 - deal['price'] / deal['original_price']) * 100 if deal['original_price'] > 0 else 0
    if discount < MIN_DISCOUNT_TO_ANALYZE and deal.get('source') not in ('Reddit', 'Demo'):
        return {**deal, "analysis_text": "No AI analysis (low discount)", "verdict": "Average", "flaws": [], "alternatives": [], "is_expired": False}
    html = await safe_fetch_url(deal['url'])
    if not html:
        return {**deal, "analysis_text": "Analysis skipped – page unreachable", "verdict": "Average", "flaws": [], "alternatives": [], "is_expired": False}
    tree = lxml_html.fromstring(html)
    for elem in tree.xpath('//script|//style'):
        if elem.getparent() is not None:
            elem.getparent().remove(elem)
    body_text = tree.xpath('//body')[0].text_content() if tree.xpath('//body') else tree.text_content()
    sample = body_text[:4000]
    prompt = f"""Analyze the following deal and the live page content.

DEAL METADATA:
Title: {deal['title']}
Price: ₹{deal['price']}
Original MRP: ₹{deal['original_price']}
Bank offers: {deal.get('bank_offers', 'None')}
Source: {deal.get('source', 'Unknown')}
Rating: {deal.get('rating', 'N/A')}

LIVE PAGE SAMPLE (first 4000 chars):
{sample}

Generate a SHORT, engaging analysis in Hinglish. Use emojis naturally. Include good points, flaws, verdict, and 1-2 better alternatives. Output JSON strictly matching schema: {DealAnalysis.schema_json()}
"""
    try:
        ai_response = await ai_provider.generate_structured(prompt, DealAnalysis)
        price_str = f"₹{deal['price']:,.0f}"
        if price_str not in html and str(int(deal['price'])) not in html:
            logger.warning(f"AI generated price {deal['price']} not found in HTML for {deal['url']}")
            ai_response.is_expired = True
        async with db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO ai_analysis_cache (url_hash, deal_url, analysis_summary, verdict, flaws, alternatives, is_expired, created_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                ON CONFLICT (url_hash) DO UPDATE SET
                    analysis_summary=EXCLUDED.analysis_summary,
                    verdict=EXCLUDED.verdict,
                    flaws=EXCLUDED.flaws,
                    alternatives=EXCLUDED.alternatives,
                    is_expired=EXCLUDED.is_expired,
                    created_at=EXCLUDED.created_at
            """, url_hash, deal['url'], ai_response.analysis_text, ai_response.verdict,
                json.dumps(ai_response.flaws), json.dumps(ai_response.alternatives),
                ai_response.is_expired, datetime.now(timezone.utc))
        return {**deal, "analysis_text": ai_response.analysis_text, "verdict": ai_response.verdict,
                "flaws": ai_response.flaws, "alternatives": ai_response.alternatives, "is_expired": ai_response.is_expired}
    except Exception as e:
        logger.error(f"AI analysis failed for {deal['url']}: {e}")
        await add_failed_job("analyze", {"deal": deal}, str(e))
        return {**deal, "analysis_text": "⚠️ Analysis temporarily unavailable", "verdict": "Average", "flaws": [], "alternatives": [], "is_expired": False}

async def check_tracked_products(app: Application):
    products = await get_tracked_products()
    for prod in products:
        html = await safe_fetch_url(prod['product_url'])
        if not html:
            continue
        tree = lxml_html.fromstring(html)
        price_elem = tree.xpath('//span[contains(@class,"price")] | //div[contains(@class,"price")] | //meta[@property="product:price:amount"]')
        price = 0.0
        if price_elem:
            price_text = price_elem[0].text_content() if hasattr(price_elem[0], 'text_content') else price_elem[0].get('content', '')
            price = extract_price_fast(price_text)
        if price <= 0:
            continue
        product_key = generate_deal_fingerprint(prod['product_url'], price, "tracked")
        await record_price_history(product_key, price, "tracked")
        if prod['last_price'] == 0:
            await update_tracked_product_price(prod['id'], price)
            continue
        if price < prod['last_price'] or (prod['target_price'] > 0 and price <= prod['target_price']):
            msg = f"🔔 *Price Alert!*\n\n{prod['product_url']}\nPrevious: ₹{prod['last_price']:,.0f}\nNow: ₹{price:,.0f}"
            try:
                await app.bot.send_message(chat_id=prod['user_id'], text=msg, parse_mode=ParseMode.MARKDOWN)
            except Exception as e:
                logger.error(f"Alert failed: {e}")
            await update_tracked_product_price(prod['id'], price)
        else:
            await update_tracked_product_price(prod['id'], price)
        await asyncio.sleep(random.uniform(1, 3))

# ---------- Queues ----------
extraction_queue = asyncio.Queue()
analysis_queue = asyncio.Queue()
broadcast_queue = asyncio.Queue()

async def extraction_worker(app: Application):
    while True:
        try:
            task = await extraction_queue.get()
            source = task['source']
            url = task['url']
            html = task['html']
            deals = await extract_deals_with_fallback(source, url, html)
            for deal in deals:
                fingerprint = generate_deal_fingerprint(deal['title'], deal['price'], deal['source'])
                if await is_deal_seen(fingerprint):
                    continue
                await analysis_queue.put(deal)
            extraction_queue.task_done()
        except Exception as e:
            logger.error(f"Extraction worker error: {e}")
            await asyncio.sleep(1)

async def analysis_worker(app: Application):
    while True:
        try:
            deal = await analysis_queue.get()
            enriched = await analyze_deal_with_ai(deal)
            if enriched.get('is_expired'):
                continue
            fingerprint = generate_deal_fingerprint(enriched['title'], enriched['price'], enriched['source'])
            if await is_deal_seen(fingerprint):
                continue
            deal_id, inserted = await add_sent_deal(
                enriched['url'], enriched['title'], enriched['price'], enriched['original_price'],
                enriched.get('bank_offers', ''), enriched.get('analysis_text', ''), enriched.get('verdict', 'Average'),
                fingerprint
            )
            if inserted:
                await broadcast_queue.put({'deal': enriched, 'deal_id': deal_id})
            analysis_queue.task_done()
        except Exception as e:
            logger.error(f"Analysis worker error: {e}")
            await asyncio.sleep(1)

async def broadcast_worker(app: Application):
    while True:
        try:
            item = await broadcast_queue.get()
            deal = item['deal']
            deal_id = item['deal_id']
            users = await get_all_users()
            for i in range(0, len(users), BROADCAST_CHUNK_SIZE):
                chunk = users[i:i + BROADCAST_CHUNK_SIZE]
                async def send_one(user_id):
                    msg = format_deal_message(deal)
                    keyboard = get_deal_keyboard(deal_id, deal['url'])
                    try:
                        sent = await app.bot.send_message(chat_id=user_id, text=msg, parse_mode=ParseMode.HTML, reply_markup=keyboard)
                        await add_sent_deal_message(deal_id, user_id, sent.message_id, user_id)
                    except RetryAfter as e:
                        await asyncio.sleep(e.retry_after)
                        try:
                            sent = await app.bot.send_message(chat_id=user_id, text=msg, parse_mode=ParseMode.HTML, reply_markup=keyboard)
                            await add_sent_deal_message(deal_id, user_id, sent.message_id, user_id)
                        except Exception as e2:
                            logger.error(f"Retry failed for {user_id}: {e2}")
                    except Forbidden:
                        await remove_user(user_id)
                    except Exception as e:
                        logger.error(f"Failed to send to {user_id}: {e}")
                await asyncio.gather(*[send_one(uid) for uid in chunk])
                await asyncio.sleep(2)
            broadcast_queue.task_done()
        except Exception as e:
            logger.error(f"Broadcast worker error: {e}")
            await asyncio.sleep(1)

async def fetch_and_enqueue():
    sources = [
        ("DesiDime", "https://www.desidime.com/hot-deals"),
        ("GrabOn", "https://www.grabon.in/deals/"),
        ("Amazon", "https://www.amazon.in/deals"),
        ("Flipkart", "https://www.flipkart.com/offers-store"),
        ("Reddit", "https://www.reddit.com/r/IndiaDeals/hot.json?limit=10")
    ]
    for source, url in sources:
        html = await safe_fetch_url(url)
        if not html:
            await record_source_failure(source)
            continue
        await record_source_success(source)
        text = lxml_html.fromstring(html).text_content() if html else ""
        await store_snapshot(source, url, html, text)
        await extraction_queue.put({'source': source, 'url': url, 'html': html})
        await asyncio.sleep(2)

async def revalidate_deals(app: Application):
    deals = await get_active_deals_to_revalidate()
    for deal in deals:
        await asyncio.sleep(random.uniform(0, 30))
        quick = await quick_check_page(deal['deal_url'])
        if quick.get('expired'):
            await update_deal_expiry(deal['id'], True)
            expired_deal = {**deal, "analysis_text": deal.get('analysis_summary', ''), "is_expired": True}
            messages = await get_all_sent_messages_for_deal(deal['id'])
            for msg in messages:
                try:
                    new_text = format_deal_message(expired_deal)
                    keyboard = get_deal_keyboard(deal['id'], deal['deal_url'])
                    await app.bot.edit_message_text(
                        chat_id=msg['chat_id'],
                        message_id=msg['message_id'],
                        text=new_text,
                        parse_mode=ParseMode.HTML,
                        reply_markup=keyboard
                    )
                except Exception as e:
                    if "message is not modified" not in str(e).lower():
                        logger.error(f"Edit failed: {e}")
        await asyncio.sleep(0.5)

async def cleanup_messages(app: Application):
    old = await delete_old_deals()
    for row in old:
        try:
            if row.get('message_id') and row.get('chat_id'):
                await app.bot.delete_message(chat_id=row['chat_id'], message_id=row['message_id'])
        except Exception as e:
            logger.warning(f"Delete failed: {e}")
        await asyncio.sleep(0.05)

def format_deal_message(deal: dict) -> str:
    analysis = deal.get('analysis_text') or deal.get('analysis_summary', 'No analysis')
    title = escape(deal['title'])
    analysis_escaped = escape(analysis)
    flaws = [escape(f) for f in deal.get('flaws', [])]
    verdict = escape(deal.get('verdict', 'Average'))
    bank = escape(deal.get('bank_offers', 'No bank offers'))
    source = escape(deal.get('source', 'Unknown'))

    if deal.get('is_expired'):
        title_display = f"<s>{title}</s>"
        price_display = f"<s>₹{float(deal['price']):,.0f}</s>"
        expiry_note = "\n\n❌ Deal expired • Better alternatives below"
    else:
        title_display = f"<b>{title}</b>"
        price_display = f"₹{float(deal['price']):,.0f}"
        expiry_note = ""

    original = f"<s>MRP ₹{float(deal['original_price']):,.0f}</s>" if float(deal['original_price']) > float(deal['price']) else f"MRP ₹{float(deal['original_price']):,.0f}"
    discount = int((1 - float(deal['price'])/float(deal['original_price']))*100) if float(deal['original_price']) > 0 else 0

    msg = f"""
{title_display}
💰 {price_display}  ( {original}  |  {discount}% off )
🏦 {bank}
📍 Source: {source}

🧠 <b>AI Analysis:</b>
{analysis_escaped}

⚠️ <b>Flaws Detected:</b>
{chr(10).join([f'• {f}' for f in flaws]) if flaws else '• None reported'}

💡 <b>Verdict:</b> {verdict}
{expiry_note}
    """
    msg = msg.strip()
    if len(msg) > 4000:
        msg = msg[:4000] + "\n\n⚠️ Truncated"
    return msg

def get_deal_keyboard(deal_id: int, deal_url: str) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton("🔗 View on Site", url=deal_url)],
        [InlineKeyboardButton("🔔 Set Alert", callback_data=f"a_{deal_id}"),
         InlineKeyboardButton("🔄 Alternatives", callback_data=f"alt_{deal_id}")]
    ]
    return InlineKeyboardMarkup(buttons)

async def quick_check_page(url: str) -> dict:
    html = await safe_fetch_url(url)
    if not html:
        return {"price": 0, "in_stock": True, "expired": False}
    tree = lxml_html.fromstring(html)
    price_elem = tree.xpath('//span[contains(@class,"price")] | //div[contains(@class,"price")]')
    price = 0.0
    if price_elem:
        price = extract_price_fast(price_elem[0].text_content())
    oos_text = tree.xpath('//*[contains(text(),"out of stock") or contains(text(),"Out of Stock")]')
    expired = bool(oos_text)
    return {"price": price, "in_stock": not expired, "expired": expired}

# ---------- Telegram handlers ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await register_user(user_id)
    await update.message.reply_text("👋 Welcome! You'll receive all deals automatically.")

async def track(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /track <url>")
        return
    url = context.args[0]
    parsed = urlparse(url)
    if parsed.scheme not in ('http', 'https'):
        await update.message.reply_text("❌ Only http/https URLs allowed")
        return
    await add_tracked_product(update.effective_user.id, url)
    await update.message.reply_text("🔔 Tracking started. You'll be notified on price drops.")

async def feedback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Thanks for your feedback!")

async def cleanup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("Admin only.")
        return
    await update.message.reply_text("🧹 Cleaning old messages...")
    await cleanup_messages(context.application)
    await update.message.reply_text("✅ Done.")

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data.startswith("alt_"):
        deal_id = int(data.split("_")[1])
        deal = await get_deal_by_id(deal_id)
        if deal:
            await query.message.reply_text(f"🔄 Alternatives for {deal['title']}: Check similar products.")
        else:
            await query.message.reply_text("Deal not found.")
    elif data.startswith("a_"):
        deal_id = int(data.split("_")[1])
        deal = await get_deal_by_id(deal_id)
        if deal:
            await add_tracked_product(query.from_user.id, deal['deal_url'])
            await query.message.reply_text(f"🔔 Alert set for {deal['title']}.")
        else:
            await query.message.reply_text("Could not set alert.")

# ---------- FastAPI app ----------
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
    else:
        logger.warning("No RENDER_EXTERNAL_HOSTNAME, skipping webhook")
    # Start workers
    for _ in range(3):
        asyncio.create_task(extraction_worker(telegram_app))
        asyncio.create_task(analysis_worker(telegram_app))
        asyncio.create_task(broadcast_worker(telegram_app))
    scheduler.add_job(fetch_and_enqueue, IntervalTrigger(hours=2), max_instances=1, coalesce=True)
    scheduler.add_job(revalidate_deals, IntervalTrigger(hours=4), args=[telegram_app], max_instances=1, coalesce=True)
    scheduler.add_job(cleanup_messages, CronTrigger(hour=3, minute=0), args=[telegram_app], max_instances=1, coalesce=True)
    scheduler.add_job(check_tracked_products, IntervalTrigger(minutes=TRACKED_PRICE_CHECK_INTERVAL_MINUTES), args=[telegram_app], max_instances=1, coalesce=True)
    scheduler.add_job(retry_failed_jobs, IntervalTrigger(minutes=5), max_instances=1, coalesce=True)
    scheduler.start()
    asyncio.create_task(asyncio.sleep(10))
    asyncio.create_task(fetch_and_enqueue())
    yield
    if scheduler.running:
        scheduler.shutdown()
    await telegram_app.stop()
    if db_pool:
        await db_pool.close()
    await close_http_session()
    await telegram_app.shutdown()

fastapi_app = FastAPI(lifespan=lifespan)

# Register handlers
telegram_app.add_handler(CommandHandler('start', start))
telegram_app.add_handler(CommandHandler('track', track))
telegram_app.add_handler(CommandHandler('feedback', feedback))
telegram_app.add_handler(CommandHandler('cleanup', cleanup_command))
telegram_app.add_handler(CallbackQueryHandler(button_callback))

@fastapi_app.post("/webhook")
async def webhook(request: Request, background_tasks: BackgroundTasks):
    body = await request.body()
    background_tasks.add_task(process_update, body)
    return Response(status_code=200)

async def process_update(body: bytes):
    try:
        update = Update.de_json(json.loads(body), telegram_app.bot)
        await telegram_app.process_update(update)
    except Exception as e:
        logger.error(f"Update error: {e}")

@fastapi_app.get("/webhook")
async def webhook_get():
    return {"status": "ok - use POST"}

@fastapi_app.get("/health")
async def health():
    return {"status": "alive"}

@fastapi_app.get("/ping")
async def ping():
    return {"status": "pong"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(fastapi_app, host="0.0.0.0", port=PORT)
