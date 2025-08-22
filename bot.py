import os
import json
import logging
import asyncio
import aiohttp
import psycopg2
import signal
from functools import wraps
from aiohttp import ClientTimeout, web
from urllib.parse import urlparse
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
from telegram.error import BadRequest, TelegramError
import asyncpg
from datetime import datetime, timedelta, timezone
import pytz
import random
import string
from logging.handlers import RotatingFileHandler
from prometheus_client import Counter, Histogram, start_http_server
from cachetools import TTLCache
import hmac
import hashlib
import requests
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from io import BytesIO
from flask import Flask, render_template, request, redirect, url_for, flash, session
import bcrypt
from aiohttp_wsgi import WSGIHandler

# Flask application setup
app_flask = Flask(__name__)
app_flask.secret_key = os.getenv("FLASK_SECRET_KEY", "your-secret-key")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")

# Logging setup with rotation
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.DEBUG,
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler("bot.log", maxBytes=10_000_000, backupCount=5, encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)

# Prometheus metrics
REQUESTS = Counter("bot_requests_total", "Total number of requests", ["endpoint"])
ERRORS = Counter("bot_errors_total", "Total number of errors", ["type", "endpoint"])
RESPONSE_TIME = Histogram("bot_response_time_seconds", "Response time of handlers", ["endpoint"])

# Load environment variables
load_dotenv()

# Constants
BOT_TOKEN = os.getenv("BOT_TOKEN")
POSTGRES_URL = os.getenv("POSTGRES_URL") or os.getenv("DATABASE_URL")
SPLIT_API_TOKEN = os.getenv("SPLIT_API_TOKEN")
CRYPTOBOT_API_TOKEN = os.getenv("CRYPTOBOT_API_TOKEN")
TON_SPACE_API_TOKEN = os.getenv("TON_SPACE_API_TOKEN")
TON_API_KEY = os.getenv("TON_API_KEY")
OWNER_WALLET = os.getenv("OWNER_WALLET")
PROVIDER_TOKEN = os.getenv("PROVIDER_TOKEN")
SPLIT_API_URL = "https://api.split.tg/buy/stars"
CRYPTOBOT_API_URL = "https://pay.crypt.bot/api"
TON_SPACE_API_URL = "https://api.ton.space/v1"
SUPPORT_CHANNEL = "https://t.me/CheapStarsShop_support"
REVIEWS_CHANNEL = "https://t.me/CheapStarsShop_support"
NEWS_CHANNEL = "https://t.me/cheapstarshop_news"
TWIN_ACCOUNT_ID = int(os.getenv("TWIN_ACCOUNT_ID", 6956377285))
ADMIN_BACKUP_ID = 6956377285
PRICE_USD_PER_50 = float(os.getenv("PRICE_USD_PER_50", 0.81))
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
PORT = int(os.getenv("PORT", 8080))
MARKUP_PERCENTAGE = float(os.getenv("MARKUP_PERCENTAGE", 10))
REFERRAL_BONUS_PERCENTAGE = float(os.getenv("REFERRAL_BONUS_PERCENTAGE", 30))

# State constants
STATES = {
    "main_menu": 0,
    "profile": 1,
    "referrals": 2,
    "buy_stars_recipient": 3,
    "buy_stars_amount": 4,
    "buy_stars_payment_method": 5,
    "buy_stars_crypto_type": 6,
    "buy_stars_confirm": 7,
    "admin_panel": 8,
    "admin_stats": 9,
    "admin_broadcast": 10,
    "admin_edit_profile": 11,
    "top_referrals": 12,
    "top_purchases": 13,
    "set_db_reminder": 14,
    "all_users": 15,
    "tech_break": 16,
    "bot_settings": 17,
    "feedback": 18,
    "support_ticket": 19,
    "ban_user": 20,
    "unban_user": 21,
    "select_stars_menu": 22,
    "buy_stars_custom": 23,
    "transaction_history": 24,
    "buy_stars_payment": 25,
    "profile_transactions": 26
}

# Global variables
db_pool = None
_db_pool_lock = asyncio.Lock()
telegram_app = None
transaction_cache = TTLCache(maxsize=1000, ttl=3600)
tech_break_info = None

# Flask login decorator
def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'logged_in' not in session:
            logger.warning("Unauthorized access attempt to protected route")
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated

# Database connection (synchronous for Flask)
def get_db_connection():
    try:
        conn = psycopg2.connect(os.getenv("POSTGRES_URL"))
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise

# Flask routes
@app_flask.route('/login', methods=['GET', 'POST'])
def login():
    if session.get('logged_in'):
        logger.info("User already logged in, redirecting to transactions")
        return redirect(url_for('transactions'))
    
    if request.method == 'POST':
        password = request.form.get('password')
        stored_password = os.getenv('ADMIN_PASSWORD')
        if not stored_password:
            logger.error("ADMIN_PASSWORD not set")
            flash("Server configuration error.", "error")
            return render_template('login.html')
        
        hashed = stored_password.encode('utf-8') if stored_password.startswith('$2b$') else None
        if hashed and bcrypt.checkpw(password.encode('utf-8'), hashed):
            session['logged_in'] = True
            logger.info("Successful login")
            flash("Login successful!", "success")
            return redirect(url_for('transactions'))
        elif password == stored_password:
            session['logged_in'] = True
            logger.info("Successful login (plain password)")
            flash("Login successful!", "success")
            return redirect(url_for('transactions'))
        else:
            logger.warning("Failed login attempt")
            flash("Invalid password.", "error")
    
    return render_template('login.html')

@app_flask.route('/logout')
def logout():
    session.pop('logged_in', None)
    logger.info("User logged out")
    flash("You have logged out.", "success")
    return redirect(url_for('login'))

@app_flask.route('/')
@login_required
def index():
    return redirect(url_for('transactions'))

@app_flask.route('/transactions')
@login_required
def transactions():
    page = int(request.args.get('page', 1))
    per_page = 10
    user_id = request.args.get('user_id', '')
    recipient = request.args.get('recipient', '')
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    min_stars = request.args.get('min_stars', '')
    max_stars = request.args.get('max_stars', '')

    query = """
        SELECT id, user_id, recipient_username, stars_amount, price_ton, purchase_time, checked_status
        FROM transactions WHERE 1=1
    """
    count_query = """
        SELECT COUNT(*) FROM transactions WHERE 1=1
    """
    params = []

    if user_id:
        try:
            query += " AND user_id = %s"
            count_query += " AND user_id = %s"
            params.append(int(user_id))
        except ValueError:
            flash("User ID must be a number.", "error")
            logger.error(f"Invalid user_id format: {user_id}")

    if recipient:
        query += " AND recipient_username ILIKE %s"
        count_query += " AND recipient_username ILIKE %s"
        params.append(f'%{recipient}%')

    if start_date:
        try:
            query += " AND purchase_time >= %s"
            count_query += " AND purchase_time >= %s"
            params.append(datetime.strptime(start_date, "%Y-%m-%d"))
        except ValueError:
            flash("Invalid start date format (yyyy-mm-dd).", "error")
            logger.error(f"Invalid start_date format: {start_date}")

    if end_date:
        try:
            query += " AND purchase_time <= %s"
            count_query += " AND purchase_time <= %s"
            params.append(datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1))
        except ValueError:
            flash("Invalid end date format (yyyy-mm-dd).", "error")
            logger.error(f"Invalid end_date format: {end_date}")

    if min_stars:
        try:
            query += " AND stars_amount >= %s"
            count_query += " AND stars_amount >= %s"
            params.append(int(min_stars))
        except ValueError:
            flash("Minimum stars must be a number.", "error")
            logger.error(f"Invalid min_stars format: {min_stars}")

    if max_stars:
        try:
            query += " AND stars_amount <= %s"
            count_query += " AND stars_amount <= %s"
            params.append(int(max_stars))
        except ValueError:
            flash("Maximum stars must be a number.", "error")
            logger.error(f"Invalid max_stars format: {max_stars}")

    query += " ORDER BY purchase_time DESC LIMIT %s OFFSET %s"
    params.extend([per_page, (page - 1) * per_page])

    for attempt in range(3):
        try:
            conn = psycopg2.connect(POSTGRES_URL)
            cur = conn.cursor()
            cur.execute(query, params)
            transactions = cur.fetchall()
            cur.execute(count_query, params[:-2])
            total = cur.fetchone()[0]
            cur.close()
            conn.close()

            total_pages = (total + per_page - 1) // per_page
            eest = pytz.timezone("Europe/Tallinn")
            transactions = [
                {
                    "id": t[0],
                    "user_id": t[1],
                    "recipient_username": t[2],
                    "stars_amount": t[3],
                    "price_ton": t[4],
                    "purchase_time": t[5].astimezone(eest).strftime("%Y-%m-%d %H:%M:%S EEST"),
                    "checked_status": t[6]
                }
                for t in transactions
            ]

            logger.info(f"Displayed {len(transactions)} transactions, page {page} of {total_pages}")
            return render_template(
                'transactions.html',
                transactions=transactions,
                page=page,
                total_pages=total_pages,
                user_id=user_id,
                start_date=start_date,
                end_date=end_date,
                min_stars=min_stars,
                max_stars=max_stars,
                recipient=recipient
            )
        except Exception as e:
            logger.error(f"Error loading transactions (attempt {attempt+1}): {e}", exc_info=True)
            if attempt < 2:
                continue
            flash(f"Error loading transactions: {str(e)}", "error")
            return render_template(
                'transactions.html',
                transactions=[],
                page=1,
                total_pages=1,
                user_id=user_id,
                start_date=start_date,
                end_date=end_date,
                min_stars=min_stars,
                max_stars=max_stars,
                recipient=recipient
            )

@app_flask.route('/users')
@login_required
def users():
    page = int(request.args.get('page', 1))
    per_page = 10
    user_id = request.args.get('user_id', '')
    username = request.args.get('username', '')
    is_admin = request.args.get('is_admin', '')

    query = """
        SELECT user_id, username, stars_bought, ref_bonus_ton, referrals, created_at, is_new, is_admin, prefix
        FROM users WHERE 1=1
    """
    count_query = """
        SELECT COUNT(*) FROM users WHERE 1=1
    """
    params = []

    if user_id:
        try:
            query += " AND user_id = %s"
            count_query += " AND user_id = %s"
            params.append(int(user_id))
        except ValueError:
            flash("User ID must be a number.", "error")
            logger.error(f"Invalid user_id format: {user_id}")

    if username:
        query += " AND username ILIKE %s"
        count_query += " AND username ILIKE %s"
        params.append(f'%{username}%')

    if is_admin:
        query += " AND is_admin = %s"
        count_query += " AND is_admin = %s"
        params.append(is_admin == 'true')

    query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
    params.extend([per_page, (page - 1) * per_page])

    for attempt in range(3):
        try:
            conn = psycopg2.connect(POSTGRES_URL)
            cur = conn.cursor()
            cur.execute(query, params)
            users = cur.fetchall()
            cur.execute(count_query, params[:-2])
            total = cur.fetchone()[0]
            cur.close()
            conn.close()

            total_pages = (total + per_page - 1) // per_page
            users = [
                {
                    "user_id": u[0],
                    "username": u[1],
                    "stars_bought": u[2],
                    "ref_bonus_ton": u[3],
                    "referrals": u[4] if u[4] is not None else [],  # Use jsonb field directly
                    "created_at": u[5].strftime("%Y-%m-%d %H:%M:%S"),
                    "is_new": u[6],
                    "is_admin": u[7],
                    "prefix": u[8]
                }
                for u in users
            ]

            logger.info(f"Displayed {len(users)} users, page {page} of {total_pages}")
            return render_template(
                'users.html',
                users=users,
                page=page,
                total_pages=total_pages,
                user_id=user_id,
                username=username,
                is_admin=is_admin
            )
        except Exception as e:
            logger.error(f"Error loading users (attempt {attempt+1}): {e}", exc_info=True)
            if attempt < 2:
                continue
            flash(f"Error loading users: {str(e)}", "error")
            return render_template(
                'users.html',
                users=[],
                page=1,
                total_pages=1,
                user_id=user_id,
                username=username,
                is_admin=is_admin
            )

from flask import jsonify

@app_flask.route('/update_status', methods=['POST'])
@login_required
def update_status():
    data = request.get_json()
    type_ = data.get('type')
    field = data.get('field')
    user_id = data.get('user_id')
    value = data.get('value')

    try:
        conn = psycopg2.connect(POSTGRES_URL)
        cur = conn.cursor()
        if type_ == 'user' and field in ['is_admin', 'prefix']:
            if field == 'prefix' and value not in ['Beginner', 'Newbie', 'Buyer', 'Regular Buyer', 'Verified']:
                logger.error(f"Invalid prefix value: {value}")
                cur.close()
                conn.close()
                return jsonify({'message': 'Invalid prefix value'}), 400
            cur.execute(f"UPDATE users SET {field} = %s WHERE user_id = %s",
                        (value if field == 'prefix' else value == 'true', int(user_id)))
            conn.commit()
            logger.info(f"Updated {field} for user_id={user_id} to {value}")
            cur.close()
            conn.close()
            return jsonify({'message': f'{field} updated for user {user_id}'})
        elif type_ == 'transaction' and field == 'checked_status':
            cur.execute("UPDATE transactions SET checked_status = %s WHERE id = %s", (value, int(user_id)))
            conn.commit()
            logger.info(f"Updated checked_status for transaction_id={user_id} to {value}")
            cur.close()
            conn.close()
            return jsonify({'message': f'Status updated for transaction {user_id}'})
        else:
            logger.error(f"Invalid type or field: type={type_}, field={field}")
            cur.close()
            conn.close()
            return jsonify({'message': 'Invalid type or field'}), 400
    except Exception as e:
        logger.error(f"Error updating status: {e}", exc_info=True)
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
        return jsonify({'message': f'Error updating status: {str(e)}'}), 500
# Database initialization
async def ensure_db_pool():
    global db_pool
    async with _db_pool_lock:
        if db_pool is None or db_pool._closed:
            try:
                loop = asyncio.get_event_loop()
                db_pool = await asyncpg.create_pool(
                    POSTGRES_URL,
                    min_size=10,
                    max_size=50,
                    max_inactive_connection_lifetime=300,
                    loop=loop
                )
                logger.info("Database pool initialized")
            except Exception as e:
                logger.error(f"Failed to initialize database pool: {e}", exc_info=True)
                raise
    return db_pool

async def init_db():
    try:
        async with (await ensure_db_pool()) as conn:
            # Create users table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    stars_bought INTEGER DEFAULT 0,
                    ref_bonus_ton FLOAT DEFAULT 0.0,
                    referrals JSONB DEFAULT '[]',
                    is_new BOOLEAN DEFAULT TRUE,
                    is_admin BOOLEAN DEFAULT FALSE,
                    prefix TEXT DEFAULT 'Beginner',
                    referrer_id BIGINT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    is_banned BOOLEAN DEFAULT FALSE
                )
            """)
            logger.info("Users table created or verified")

            # Create transactions table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT REFERENCES users(user_id),
                    recipient_username TEXT,
                    stars_amount INTEGER,
                    price_ton FLOAT,
                    purchase_time TIMESTAMP WITH TIME ZONE,
                    checked_status TEXT DEFAULT 'pending',
                    invoice_id TEXT
                )
            """)
            logger.info("Transactions table created or verified")

            # Create settings table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value FLOAT
                )
            """)
            logger.info("Settings table created or verified")

            # Create analytics table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS analytics (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    action TEXT,
                    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    data JSONB
                )
            """)
            logger.info("Analytics table created or verified")

            # Create reminders table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS reminders (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    reminder_date DATE,
                    reminder_type TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """)
            logger.info("Reminders table created or verified")

            # Insert default settings
            default_settings = [
                ("price_usd", 0.81),
                ("markup", 10.0),
                ("ref_bonus", 30.0)
            ]
            for key, value in default_settings:
                await conn.execute(
                    """
                    INSERT INTO settings (key, value)
                    VALUES ($1, $2)
                    ON CONFLICT (key) DO NOTHING
                    """,
                    key, value
                )
            logger.info("Default settings inserted or verified")

            # Update prefixes based on stars_bought
            await conn.execute("""
                UPDATE users SET prefix = CASE
                    WHEN is_admin THEN 'Verified'
                    WHEN stars_bought >= 50000 THEN 'Verified'
                    WHEN stars_bought >= 10000 THEN 'Regular Buyer'
                    WHEN stars_bought >= 5000 THEN 'Buyer'
                    WHEN stars_bought >= 1000 THEN 'Newbie'
                    ELSE 'Beginner'
                END
            """)
            logger.info("User prefixes updated based on stars_bought")

            # Ensure admin user
            admin_user_id = 6956377285
            await conn.execute(
                """
                INSERT INTO users (user_id, username, is_admin, prefix, created_at)
                VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
                ON CONFLICT (user_id) DO UPDATE
                SET is_admin = EXCLUDED.is_admin, prefix = EXCLUDED.prefix
                """,
                admin_user_id, "Admin", True, "Verified"
            )
            logger.info(f"Admin user {admin_user_id} ensured")

    except Exception as e:
        logger.error(f"Error initializing database: {e}", exc_info=True)
        raise

async def close_db_pool():
    global db_pool
    async with _db_pool_lock:
        if db_pool and not db_pool._closed:
            await db_pool.close()
            logger.info("Database pool closed")
            db_pool = None

# Utility functions
async def get_text(key: str, **kwargs) -> str:
    texts = {
        "welcome": "Добро пожаловать! 🌟\nВсего продано звезд: {total_stars}\nВаши звезды: {stars_bought}",
        "profile": "Ваш профиль:\nЗвезд куплено: {stars_bought}\nРефералов: {ref_count}\nРеферальный бонус: {ref_bonus_ton} TON",
        "referrals": "Ваша реферальная ссылка: {ref_link}\nРефералов: {ref_count}\nБонус: {ref_bonus_ton} TON",
        "referral_leaderboard": "Топ рефералов:\n{users_list}",
        "top_purchases": "Топ покупок:\n{users_list}",
        "admin_panel": "Админ-панель:\nНапоминание о БД: {reminder_date}",
        "stats": "Статистика:\nПользователей: {total_users}\nЗвезд продано: {total_stars}\nРефералов: {total_referrals}",
        "all_users": "Список пользователей:\n{users_list}",
        "reminder_set": "Напоминание установлено на {reminder_date}",
        "db_reminder": "Напоминание: обновите базу данных ({reminder_date})!",
        "db_reminder_exists": "Напоминание о БД уже установлено на {reminder_date}. Очистите текущее напоминание, чтобы установить новое.",
        "mention_set": "Упоминание установлено на {mention_date}",
        "tech_break_active": "Технический перерыв до {end_time} ({minutes_left} мин).\nПричина: {reason}",
        "user_banned": "Вы забанены. Обратитесь в поддержку: {support_channel}",
        "bot_settings": "Настройки бота:\nЦена за 50 звезд: ${price_usd}\nНакрутка: {markup}%\nРеферальный бонус: {ref_bonus}%",
        "tech_support": "📞 Поддержка: {support_channel}",
        "reviews": "📝 Отзывы: {support_channel}"
    }
    return texts.get(key, "Неизвестный текст").format(**kwargs)

async def log_analytics(user_id: int, action: str, data: dict = None):
    try:
        async with (await ensure_db_pool()) as conn:
            await conn.execute(
                "INSERT INTO analytics (user_id, action, timestamp, data) VALUES ($1, $2, $3, $4)",
                user_id, action, datetime.now(pytz.UTC), json.dumps(data) if data else None
            )
    except Exception as e:
        logger.error(f"Ошибка логирования аналитики: {e}", exc_info=True)

async def update_ton_price():
    if not TON_API_KEY:
        logger.error("TON_API_KEY не задан, пропуск обновления цены TON")
        telegram_app.bot_data["ton_price_info"] = {"price": 0.0, "diff_24h": 0.0}
        return
    try:
        headers = {"Authorization": f"Bearer {TON_API_KEY}"}
        url = "https://tonapi.io/v2/rates?tokens=ton&currencies=usd"
        logger.debug(f"Запрос к TonAPI: {url}")
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=10) as response:
                response.raise_for_status()
                data = await response.json()
                ton_price = data["rates"]["TON"]["prices"]["USD"]
                diff_24h = data["rates"]["TON"].get("diff_24h", {}).get("USD", "0.0")
                try:
                    diff_24h = diff_24h.replace("−", "-")
                    diff_24h = float(diff_24h.replace("%", "")) if isinstance(diff_24h, str) else float(diff_24h)
                except (ValueError, TypeError) as e:
                    logger.error(f"Некорректный формат diff_24h: {diff_24h}, установка 0.0, ошибка: {e}")
                    diff_24h = 0.0
                telegram_app.bot_data["ton_price_info"] = {
                    "price": ton_price,
                    "diff_24h": diff_24h
                }
                logger.info(f"Цена TON обновлена: ${ton_price:.2f}, изменение за 24ч: {diff_24h:.2f}%")
    except Exception as e:
        logger.error(f"Ошибка получения цены TON: {e}", exc_info=True)
        ERRORS.labels(type="api", endpoint="update_ton_price").inc()
        telegram_app.bot_data["ton_price_info"] = {"price": 0.0, "diff_24h": 0.0}

async def load_settings():
    global PRICE_USD_PER_50, MARKUP_PERCENTAGE, REFERRAL_BONUS_PERCENTAGE
    async with (await ensure_db_pool()) as conn:
        settings = await conn.fetch("SELECT key, value FROM settings")
        for setting in settings:
            if setting["key"] == "price_usd":
                PRICE_USD_PER_50 = float(setting["value"])
            elif setting["key"] == "markup":
                MARKUP_PERCENTAGE = float(setting["value"])
            elif setting["key"] == "ref_bonus":
                REFERRAL_BONUS_PERCENTAGE = float(setting["value"])
        logger.info("Settings loaded from database")

async def generate_payload(user_id):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    random_str = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    payload = f"{user_id}_{timestamp}_{random_str}"
    secret = os.getenv("BOT_TOKEN").encode()
    signature = hmac.new(secret, payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}:{signature}"

async def verify_payload(payload, signature):
    secret = os.getenv("BOT_TOKEN").encode()
    expected_signature = hmac.new(secret, payload.encode(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(signature, expected_signature)

async def create_cryptobot_invoice(amount_usd, currency, user_id, stars, recipient, payload):
    if not CRYPTOBOT_API_TOKEN:
        logger.error("CRYPTOBOT_API_TOKEN не задан")
        return None, None
    async with aiohttp.ClientSession(timeout=ClientTimeout(total=30)) as session:
        for attempt in range(3):
            try:
                headers = {"Crypto-Pay-API-Token": CRYPTOBOT_API_TOKEN}
                data = {
                    "amount": str(amount_usd),
                    "currency": currency,
                    "description": f"Покупка {stars} звезд для @{recipient}",
                    "payload": payload
                }
                async with session.post(CRYPTOBOT_API_URL + "/createInvoice", headers=headers, json=data) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result["result"]["invoice_id"], result["result"]["pay_url"]
                    else:
                        logger.error(f"Cryptobot API error: {response.status} - {await response.text()}")
                        ERRORS.labels(type="api", endpoint="create_cryptobot_invoice").inc()
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Cryptobot invoice creation failed (attempt {attempt+1}): {e}", exc_info=True)
                ERRORS.labels(type="api", endpoint="create_cryptobot_invoice").inc()
                await asyncio.sleep(2)
        return None, None

async def check_environment():
    required_vars = [
        "BOT_TOKEN",
        "POSTGRES_URL",
        "SPLIT_API_TOKEN",
        "PROVIDER_TOKEN",
        "OWNER_WALLET",
        "WEBHOOK_URL",
        "CRYPTOBOT_API_TOKEN",
        "TON_API_KEY"
    ]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Отсутствуют обязательные переменные окружения: {', '.join(missing_vars)}")
        raise ValueError(f"Отсутствуют обязательные переменные окружения: {', '.join(missing_vars)}")

def setup_handlers(app):
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("tonprice", ton_price_command))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(PreCheckoutQueryHandler(pre_checkout_callback))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment_callback))
    logger.info("Registered Telegram handlers: start, tonprice, button_callback, handle_message, pre_checkout_callback, successful_payment_callback")

async def format_time_remaining(end_time):
    now = datetime.now(pytz.UTC)
    if end_time <= now:
        return "Технический перерыв завершён."
    delta = end_time - now
    days = delta.days
    hours = delta.seconds // 3600
    minutes = (delta.seconds % 3600) // 60
    parts = []
    if days > 0:
        parts.append(f"{days} дн.")
    if hours > 0:
        parts.append(f"{hours} ч.")
    if minutes > 0 or (days == 0 and hours == 0):
        parts.append(f"{minutes} мин.")
    return " ".join(parts) if parts else "менее минуты"

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    username = update.effective_user.username or str(user_id)
    referrer_id = None
    args = context.args

    logger.info(f"Start command received: user_id={user_id}, username={username}, args={args}")
    async with (await ensure_db_pool()) as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
        if not user:
            if args and args[0].isdigit():
                referrer_id = int(args[0])
                referrer = await conn.fetchrow("SELECT referrals FROM users WHERE user_id = $1", referrer_id)
                if referrer:
                    referrals = json.loads(referrer["referrals"]) if referrer["referrals"] else []
                    if user_id not in referrals:
                        referrals.append(user_id)
                        await conn.execute(
                            "UPDATE users SET referrals = $1 WHERE user_id = $2",
                            json.dumps(referrals), referrer_id
                        )
                        logger.info(f"Added referral: user_id={user_id} to referrer_id={referrer_id}")
            await conn.execute(
                "INSERT INTO users (user_id, username, stars_bought, referrals, ref_bonus_ton, is_admin, is_banned) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7)",
                user_id, username, 0, json.dumps([]), 0.0, False, False
            )
            logger.info(f"New user registered: user_id={user_id}, username={username}")
        else:
            await conn.execute(
                "UPDATE users SET username = $1 WHERE user_id = $2",
                username, user_id
            )
        is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id) or False
        is_banned = await conn.fetchval("SELECT is_banned FROM users WHERE user_id = $1", user_id) or False
        if is_banned:
            text = await get_text("user_banned", support_channel="https://t.me/CheapStarsShop_support")
            await update.message.reply_text(text)
            context.user_data["state"] = 0  # STATES["main_menu"]
            await log_analytics(user_id, "start_banned", {})
            return 0
        if context.bot_data.get("tech_break_info", {}).get("end_time", datetime.min.replace(tzinfo=pytz.UTC)) > datetime.now(pytz.UTC) and not is_admin:
            time_remaining = await format_time_remaining(context.bot_data["tech_break_info"]["end_time"])
            text = await get_text(
                "tech_break_active",
                end_time=context.bot_data["tech_break_info"]["end_time"].strftime("%Y-%m-%d %H:%M:%S UTC"),
                minutes_left=time_remaining,
                reason=context.bot_data["tech_break_info"]["reason"]
            )
            await update.message.reply_text(text)
            context.user_data["state"] = 0
            await log_analytics(user_id, "start_tech_break", {})
            return 0
        total_stars = await conn.fetchval("SELECT SUM(stars_bought) FROM users") or 0
        user_stars = await conn.fetchval("SELECT stars_bought FROM users WHERE user_id = $1", user_id) or 0
        text = await get_text("welcome", total_stars=total_stars, stars_bought=user_stars)
        keyboard = [
            [
                InlineKeyboardButton("📰 Новости", url="https://t.me/cheapstarshop_news"),
                InlineKeyboardButton("📞 Поддержка и Отзывы", url="https://t.me/CheapStarsShop_support")
            ],
            [
                InlineKeyboardButton("👤 Профиль", callback_data="profile"),
                InlineKeyboardButton("🤝 Рефералы", callback_data="referrals")
            ],
            [InlineKeyboardButton("🛒 Купить звезды", callback_data="buy_stars")]
        ]
        if is_admin:
            keyboard.append([InlineKeyboardButton("🔧 Админ-панель", callback_data="admin_panel")])
        await update.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML"
        )
        context.user_data["state"] = 0
        await log_analytics(user_id, "start", {"referrer_id": referrer_id})
        return 0

async def ton_price_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    async with (await ensure_db_pool()) as conn:
        is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id) or False
        is_banned = await conn.fetchval("SELECT is_banned FROM users WHERE user_id = $1", user_id) or False
        if is_banned:
            text = await get_text("user_banned", support_channel="https://t.me/CheapStarsShop_support")
            await update.message.reply_text(text)
            context.user_data["state"] = 0  # STATES["main_menu"]
            await log_analytics(user_id, "ton_price_banned", {})
            return 0
        if context.bot_data.get("tech_break_info", {}).get("end_time", datetime.min.replace(tzinfo=pytz.UTC)) > datetime.now(pytz.UTC) and not is_admin:
            time_remaining = await format_time_remaining(context.bot_data["tech_break_info"]["end_time"])
            text = await get_text(
                "tech_break_active",
                end_time=context.bot_data["tech_break_info"]["end_time"].strftime("%Y-%m-%d %H:%M:%S UTC"),
                minutes_left=time_remaining,
                reason=context.bot_data["tech_break_info"]["reason"]
            )
            await update.message.reply_text(text)
            context.user_data["state"] = 0
            await log_analytics(user_id, "ton_price_tech_break", {})
            return 0

        try:
            if "ton_price_info" not in context.bot_data or context.bot_data["ton_price_info"].get("price", 0.0) == 0.0:
                await update_ton_price(context)
            price = context.bot_data["ton_price_info"]["price"]
            diff_24h = context.bot_data["ton_price_info"]["diff_24h"]
            if price == 0.0:
                await update.message.reply_text("Ошибка получения цены TON. Попробуйте позже.")
                context.user_data["state"] = 0
                await log_analytics(user_id, "ton_price_error", {})
                return 0
            change_text = f"📈 +{diff_24h:.2f}%" if diff_24h >= 0 else f"📉 {diff_24h:.2f}%"
            text = f"💰 Цена TON: ${price:.2f}\nИзменение за 24ч: {change_text}"
            await update.message.reply_text(text)
            context.user_data["state"] = 0
            await log_analytics(user_id, "ton_price", {})
            logger.info(f"/tonprice executed for user_id={user_id}")
            return 0
        except Exception as e:
            logger.error(f"Error in /tonprice for user_id={user_id}: {e}", exc_info=True)
            await update.message.reply_text("Ошибка при получении цены TON. Попробуйте позже.")
            context.user_data["state"] = 0
            await log_analytics(user_id, "ton_price_error", {"error": str(e)})
            return 0

async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data
    REQUESTS.labels(endpoint="callback_query").inc()
    with RESPONSE_TIME.labels(endpoint="callback_query").time():
        logger.info(f"Callback query received: user_id={user_id}, data={data}")
        try:
            async with (await ensure_db_pool()) as conn:
                is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id) or False
                is_banned = await conn.fetchval("SELECT is_banned FROM users WHERE user_id = $1", user_id) or False
                if is_banned:
                    text = await get_text("user_banned", support_channel=SUPPORT_CHANNEL)
                    await query.message.edit_text(
                        text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]),
                        parse_mode="HTML"
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["main_menu"]
                    await log_analytics(user_id, "callback_banned", {})
                    return STATES["main_menu"]
                if tech_break_info and tech_break_info["end_time"] > datetime.now(pytz.UTC) and not is_admin:
                    time_remaining = await format_time_remaining(tech_break_info["end_time"])
                    text = await get_text(
                        "tech_break_active",
                        end_time=tech_break_info["end_time"].strftime("%Y-%m-%d %H:%M:%S UTC"),
                        minutes_left=time_remaining,
                        reason=tech_break_info["reason"]
                    )
                    await query.message.edit_text(
                        text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]),
                        parse_mode="HTML"
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["main_menu"]
                    await log_analytics(user_id, "callback_tech_break", {})
                    return STATES["main_menu"]

                if data == "profile":
                    user = await conn.fetchrow(
                        "SELECT stars_bought, referrals, ref_bonus_ton FROM users WHERE user_id = $1", user_id
                    )
                    stars_bought = user["stars_bought"] if user else 0
                    referrals = json.loads(user["referrals"]) if user and user["referrals"] else []
                    ref_bonus_ton = user["ref_bonus_ton"] if user else 0.0
                    text = await get_text(
                        "profile",
                        stars_bought=stars_bought,
                        ref_count=len(referrals),
                        ref_bonus_ton=ref_bonus_ton
                    )
                    keyboard = [
                        [InlineKeyboardButton("📜 Мои транзакции", callback_data="profile_transactions_0")],
                        [InlineKeyboardButton("🏆 Топ рефералов", callback_data="referral_leaderboard")],
                        [InlineKeyboardButton("🏅 Топ покупок", callback_data="top_purchases")],
                        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]
                    ]
                    await query.message.edit_text(
                        text,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode="HTML"
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["profile"]
                    await log_analytics(user_id, "view_profile", {})
                    return STATES["profile"]

                elif data.startswith("profile_transactions"):
                    page = int(data.split("_")[-1]) if "_" in data else 0
                    transactions_per_page = 10
                    offset = page * transactions_per_page
                    transactions = await conn.fetch(
                        "SELECT recipient_username, stars_amount, price_ton, purchase_time "
                        "FROM transactions WHERE user_id = $1 ORDER BY purchase_time DESC LIMIT $2 OFFSET $3",
                        user_id, transactions_per_page, offset
                    )
                    total_transactions = await conn.fetchval("SELECT COUNT(*) FROM transactions WHERE user_id = $1", user_id)
                    if not transactions:
                        text = "Транзакции отсутствуют."
                    else:
                        text = f"Ваши транзакции (страница {page + 1}):\n\n"
                        for idx, t in enumerate(transactions, start=1 + offset):
                            utc_time = t['purchase_time']
                            eest_time = utc_time.astimezone(pytz.timezone('Europe/Tallinn')).strftime('%Y-%m-%d %H:%M:%S EEST')
                            text += (
                                f"{idx}. Куплено {t['stars_amount']} звезд для {t['recipient_username']} "
                                f"за {t['price_ton']:.2f} TON в {eest_time}\n\n"
                            )
                    keyboard = []
                    if total_transactions > (page + 1) * transactions_per_page:
                        keyboard.append([InlineKeyboardButton("➡️ Далее", callback_data=f"profile_transactions_{page + 1}")])
                    if page > 0:
                        keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"profile_transactions_{page - 1}")])
                    keyboard.append([InlineKeyboardButton("🔙 В профиль", callback_data="profile")])
                    await query.message.edit_text(
                        text,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode="HTML"
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["profile_transactions"]
                    await log_analytics(user_id, "view_profile_transactions", {"page": page})
                    return STATES["profile_transactions"]

                elif data == "referrals":
                    user = await conn.fetchrow(
                        "SELECT referrals, ref_bonus_ton FROM users WHERE user_id = $1", user_id
                    )
                    referrals = json.loads(user["referrals"]) if user and user["referrals"] else []
                    ref_bonus_ton = user["ref_bonus_ton"] if user else 0.0
                    ref_link = f"https://t.me/{telegram_app.bot.username}?start={user_id}"
                    text = await get_text(
                        "referrals",
                        ref_link=ref_link,
                        ref_count=len(referrals),
                        ref_bonus_ton=ref_bonus_ton
                    )
                    await query.message.edit_text(
                        text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]),
                        parse_mode="HTML"
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["referrals"]
                    await log_analytics(user_id, "view_referrals", {})
                    return STATES["referrals"]

                elif data == "referral_leaderboard":
                    users = await conn.fetch(
                        "SELECT user_id, username, jsonb_array_length(referrals) as ref_count "
                        "FROM users WHERE jsonb_array_length(referrals) > 0 "
                        "ORDER BY ref_count DESC LIMIT 10"
                    )
                    text_lines = []
                    for user in users:
                        username = f"@{user['username']}" if user['username'] else f"ID <code>{user['user_id']}</code>"
                        text_lines.append(f"{username}, Рефералов: {user['ref_count']}")
                    text = await get_text(
                        "referral_leaderboard",
                        users_list="\n".join(text_lines) if text_lines else "Рефералов пока нет."
                    )
                    await query.message.edit_text(
                        text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]),
                        parse_mode="HTML"
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["top_referrals"]
                    await log_analytics(user_id, "view_referral_leaderboard", {})
                    return STATES["top_referrals"]

                elif data == "top_purchases":
                    users = await conn.fetch(
                        "SELECT user_id, username, stars_bought FROM users "
                        "WHERE stars_bought > 0 ORDER BY stars_bought DESC LIMIT 10"
                    )
                    text_lines = []
                    for user in users:
                        username = f"@{user['username']}" if user['username'] else f"ID <code>{user['user_id']}</code>"
                        text_lines.append(f"{username}, Звезды: {user['stars_bought']}")
                    text = await get_text(
                        "top_purchases",
                        users_list="\n".join(text_lines) if text_lines else "Покупок пока нет."
                    )
                    await query.message.edit_text(
                        text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]),
                        parse_mode="HTML"
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["top_purchases"]
                    await log_analytics(user_id, "view_top_purchases", {})
                    return STATES["top_purchases"]

                elif data == "buy_stars":
                    recipient = context.user_data.get("recipient", "Не выбран")
                    stars = context.user_data.get("stars_amount", "Не выбрано")
                    price_ton = await calculate_price_ton(int(stars)) if stars and isinstance(stars, str) and stars.isdigit() else None
                    price_text = f"~{price_ton:.2f} TON" if price_ton is not None else "Цена"
                    text = (
                        f"Пользователь: {recipient}\n"
                        f"Количество звезд: {stars}\n"
                        f"Способ оплаты: TON Wallet"
                    )
                    keyboard = [
                        [InlineKeyboardButton(f"Пользователь: {recipient}", callback_data="select_recipient")],
                        [InlineKeyboardButton(f"Количество: {stars}", callback_data="select_stars_menu")],
                        [
                            InlineKeyboardButton(price_text, callback_data="show_price"),
                            InlineKeyboardButton("Оплатить", callback_data="proceed_to_payment")
                        ],
                        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]
                    ]
                    try:
                        await query.message.edit_text(
                            text,
                            reply_markup=InlineKeyboardMarkup(keyboard),
                            parse_mode="HTML"
                        )
                    except BadRequest as e:
                        if "Message is not modified" in str(e):
                            pass
                        else:
                            raise
                    await query.answer()
                    context.user_data["state"] = STATES["buy_stars_payment_method"]
                    await log_analytics(user_id, "open_buy_stars_payment_method", {})
                    return STATES["buy_stars_payment_method"]

                elif data == "show_price":
                    stars = context.user_data.get("stars_amount", "Не выбрано")
                    price_ton = await calculate_price_ton(int(stars)) if stars and isinstance(stars, str) and stars.isdigit() else None
                    price_text = f"~{price_ton:.2f} TON" if price_ton is not None else "Цена не определена"
                    await query.answer(text=price_text, show_alert=True)
                    return context.user_data["state"]

                elif data == "select_recipient":
                    await query.message.edit_text(
                        "Введите имя пользователя (например, @username):",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="buy_stars")]])
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["buy_stars_recipient"]
                    await log_analytics(user_id, "start_select_recipient", {})
                    return STATES["buy_stars_recipient"]

                elif data == "select_stars_menu":
                    recipient = context.user_data.get("recipient", "Не выбран")
                    text = f"Пользователь: {recipient}\nВыберите количество звезд:"
                    keyboard = [
                        [
                            InlineKeyboardButton("100", callback_data="select_stars_100"),
                            InlineKeyboardButton("250", callback_data="select_stars_250"),
                            InlineKeyboardButton("500", callback_data="select_stars_500"),
                            InlineKeyboardButton("1000", callback_data="select_stars_1000")
                        ],
                        [InlineKeyboardButton("Другое", callback_data="select_stars_custom")],
                        [InlineKeyboardButton("🔙 Назад", callback_data="buy_stars")]
                    ]
                    try:
                        await query.message.edit_text(
                            text,
                            reply_markup=InlineKeyboardMarkup(keyboard),
                            parse_mode="HTML"
                        )
                    except BadRequest as e:
                        if "Message is not modified" in str(e):
                            pass
                        else:
                            raise
                    await query.answer()
                    context.user_data["state"] = STATES["select_stars_menu"]
                    await log_analytics(user_id, "open_select_stars_menu", {})
                    return STATES["select_stars_menu"]

                elif data in ["select_stars_100", "select_stars_250", "select_stars_500", "select_stars_1000"]:
                    stars = data.split("_")[-1]
                    context.user_data["stars_amount"] = stars
                    recipient = context.user_data.get("recipient", "Не выбран")
                    price_ton = await calculate_price_ton(int(stars))
                    text = (
                        f"Пользователь: {recipient}\n"
                        f"Количество звезд: {stars}\n"
                        f"Способ оплаты: TON Wallet"
                    )
                    keyboard = [
                        [InlineKeyboardButton(f"Пользователь: {recipient}", callback_data="select_recipient")],
                        [InlineKeyboardButton(f"Количество: {stars}", callback_data="select_stars_menu")],
                        [
                            InlineKeyboardButton(f"~{price_ton:.2f} TON", callback_data="show_price"),
                            InlineKeyboardButton("Оплатить", callback_data="proceed_to_payment")
                        ],
                        [InlineKeyboardButton("🔙 Назад", callback_data="buy_stars")]
                    ]
                    try:
                        await query.message.edit_text(
                            text,
                            reply_markup=InlineKeyboardMarkup(keyboard),
                            parse_mode="HTML"
                        )
                    except BadRequest as e:
                        if "Message is not modified" in str(e):
                            pass
                        else:
                            raise
                    await query.answer()
                    context.user_data["state"] = STATES["buy_stars_payment_method"]
                    await log_analytics(user_id, f"select_stars_{stars}", {"stars": stars})
                    return STATES["buy_stars_payment_method"]

                elif data == "select_stars_custom":
                    await query.message.edit_text(
                        "Введите количество звезд (положительное число):",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="select_stars_menu")]])
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["buy_stars_custom"]
                    await log_analytics(user_id, "start_select_stars_custom", {})
                    return STATES["buy_stars_custom"]

                elif data == "proceed_to_payment":
                    stars = context.user_data.get("stars_amount")
                    recipient = context.user_data.get("recipient")
                    if not stars or not recipient or not isinstance(stars, str) or not stars.isdigit():
                        text = "Ошибка: выберите пользователя и количество звезд."
                        price_ton = None
                        price_text = "Цена"
                        if stars and isinstance(stars, str) and stars.isdigit():
                            price_ton = await calculate_price_ton(int(stars))
                            price_text = f"~{price_ton:.2f} TON"
                        keyboard = [
                            [InlineKeyboardButton(f"Пользователь: {context.user_data.get('recipient', 'Не выбран')}", callback_data="select_recipient")],
                            [InlineKeyboardButton(f"Количество: {context.user_data.get('stars_amount', 'Не выбрано')}", callback_data="select_stars_menu")],
                            [
                                InlineKeyboardButton(price_text, callback_data="show_price"),
                                InlineKeyboardButton("Оплатить", callback_data="proceed_to_payment")
                            ],
                            [InlineKeyboardButton("🔙 Назад", callback_data="buy_stars")]
                        ]
                        await query.message.edit_text(
                            text,
                            reply_markup=InlineKeyboardMarkup(keyboard),
                            parse_mode="HTML"
                        )
                        await query.answer()
                        context.user_data["state"] = STATES["buy_stars_payment_method"]
                        return STATES["buy_stars_payment_method"]
                    stars = int(stars)
                    price_usd = await conn.fetchval("SELECT value FROM settings WHERE key = 'price_usd'") or PRICE_USD_PER_50
                    markup = await conn.fetchval("SELECT value FROM settings WHERE key = 'markup'") or MARKUP_PERCENTAGE
                    price_usd = (stars / 50) * price_usd * (1 + markup / 100)
                    price_ton = await calculate_price_ton(stars)
                    payload = await generate_payload(user_id)
                    invoice_id, pay_url = await create_cryptobot_invoice(price_usd, "TON", user_id, stars, recipient, payload)
                    if not pay_url:
                        await query.message.edit_text(
                            "Ошибка создания платежа. Попробуйте позже.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="buy_stars")]])
                        )
                        await query.answer()
                        context.user_data["state"] = STATES["buy_stars_payment_method"]
                        return STATES["buy_stars_payment_method"]
                    await conn.execute(
                        "INSERT INTO transactions (user_id, recipient_username, stars_amount, price_ton, invoice_id, purchase_time) "
                        "VALUES ($1, $2, $3, $4, $5, $6)",
                        user_id, recipient, stars, price_ton, invoice_id, datetime.now(pytz.UTC)
                    )
                    text = (
                        f"Подтвердите покупку:\n"
                        f"Звезды: {stars}\n"
                        f"Получатель: {recipient}\n"
                        f"Сумма: ~{price_ton:.2f} TON\n"
                        f"Оплатите по ссылке:"
                    )
                    keyboard = [
                        [InlineKeyboardButton("Оплатить", url=pay_url)],
                        [InlineKeyboardButton("Проверить оплату", callback_data=f"check_payment_{invoice_id}")],
                        [InlineKeyboardButton("🔙 Назад", callback_data="buy_stars")]
                    ]
                    await query.message.edit_text(
                        text,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode="HTML"
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["buy_stars_payment"]
                    context.user_data["invoice_id"] = invoice_id
                    context.user_data["price_ton"] = price_ton
                    await log_analytics(user_id, "proceed_to_payment", {"stars": stars, "recipient": recipient, "invoice_id": invoice_id})
                    return STATES["buy_stars_payment"]

                elif data.startswith("check_payment_"):
                    invoice_id = data.split("_")[-1]
                    stars = context.user_data.get("stars_amount")
                    recipient = context.user_data.get("recipient")
                    price_ton = context.user_data.get("price_ton")
                    if not stars or not recipient or not price_ton:
                        await query.message.edit_text(
                            "Ошибка: данные о покупке отсутствуют. Начните заново.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="buy_stars")]])
                        )
                        await query.answer()
                        context.user_data["state"] = STATES["buy_stars_payment_method"]
                        return STATES["buy_stars_payment_method"]
                    price_usd = await conn.fetchval("SELECT value FROM settings WHERE key = 'price_usd'") or PRICE_USD_PER_50
                    markup = await conn.fetchval("SELECT value FROM settings WHERE key = 'markup'") or MARKUP_PERCENTAGE
                    ref_bonus_percentage = await conn.fetchval("SELECT value FROM settings WHERE key = 'ref_bonus'") or REFERRAL_BONUS_PERCENTAGE
                    price_usd = (int(stars) / 50) * price_usd * (1 + markup / 100)
                    await conn.execute(
                        "UPDATE users SET stars_bought = stars_bought + $1 WHERE user_id = $2",
                        int(stars), user_id
                    )
                    await conn.execute(
                        "UPDATE transactions SET purchase_time = $1 WHERE invoice_id = $2",
                        datetime.now(pytz.UTC), invoice_id
                    )
                    referrer_id = await conn.fetchval("SELECT referrer_id FROM users WHERE user_id = $1", user_id)
                    if referrer_id:
                        ref_bonus_ton = price_ton * (ref_bonus_percentage / 100)
                        await conn.execute(
                            "UPDATE users SET ref_bonus_ton = ref_bonus_ton + $1 WHERE user_id = $2",
                            ref_bonus_ton, referrer_id
                        )
                        await log_analytics(user_id, "referral_bonus_added", {"referrer_id": referrer_id, "bonus_ton": ref_bonus_ton})
                    await query.message.edit_text(
                        f"Платеж подтвержден!\n{stars} звезд добавлены для {recipient}.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["main_menu"]
                    context.user_data.pop("stars_amount", None)
                    context.user_data.pop("recipient", None)
                    context.user_data.pop("price_ton", None)
                    context.user_data.pop("invoice_id", None)
                    await log_analytics(user_id, "payment_confirmed_test", {"stars": stars, "recipient": recipient, "currency": "TON"})
                    return STATES["main_menu"]

                elif data.startswith("transaction_history") and is_admin:
                    page = int(data.split("_")[-1]) if "_" in data else 0
                    transactions_per_page = 10
                    offset = page * transactions_per_page
                    transactions = await conn.fetch(
                        "SELECT user_id, recipient_username, stars_amount, price_ton, purchase_time "
                        "FROM transactions ORDER BY purchase_time DESC LIMIT $1 OFFSET $2",
                        transactions_per_page, offset
                    )
                    total_transactions = await conn.fetchval("SELECT COUNT(*) FROM transactions")
                    if not transactions:
                        text = "История транзакций пуста."
                    else:
                        text = f"Последние транзакции (страница {page + 1}):\n\n"
                        for idx, t in enumerate(transactions, start=1 + offset):
                            utc_time = t['purchase_time']
                            eest_time = utc_time.astimezone(pytz.timezone('Europe/Tallinn')).strftime('%Y-%m-%d %H:%M:%S EEST')
                            text += (
                                f"{idx}. Пользователь ID {t['user_id']} купил {t['stars_amount']} звезд для {t['recipient_username']} "
                                f"за {t['price_ton']:.2f} TON в {eest_time}\n\n"
                            )
                    keyboard = []
                    if total_transactions > (page + 1) * transactions_per_page:
                        keyboard.append([InlineKeyboardButton("➡️ Далее", callback_data=f"transaction_history_{page + 1}")])
                    if page > 0:
                        keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"transaction_history_{page - 1}")])
                    keyboard.append([InlineKeyboardButton("🔙 В админ-панель", callback_data="back_to_admin")])
                    await query.message.edit_text(
                        text,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode="HTML"
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["transaction_history"]
                    await log_analytics(user_id, "view_transaction_history", {"page": page})
                    return STATES["transaction_history"]

                elif data == "admin_panel" and is_admin:
                    return await show_admin_panel(update, context)

                elif data == "admin_stats" and is_admin:
                    total_users = await conn.fetchval("SELECT COUNT(*) FROM users")
                    total_stars = await conn.fetchval("SELECT SUM(stars_bought) FROM users") or 0
                    total_referrals = await conn.fetchval("SELECT SUM(jsonb_array_length(referrals)) FROM users") or 0
                    text = await get_text(
                        "stats",
                        total_users=total_users,
                        total_stars=total_stars,
                        total_referrals=total_referrals
                    )
                    await query.message.edit_text(
                        text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]]),
                        parse_mode="HTML"
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["admin_stats"]
                    await log_analytics(user_id, "view_admin_stats", {})
                    return STATES["admin_stats"]

                elif data == "broadcast_message" and is_admin:
                    await query.message.edit_text(
                        "Введите текст для рассылки:",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["admin_broadcast"]
                    await log_analytics(user_id, "start_broadcast", {})
                    return STATES["admin_broadcast"]

                elif data == "confirm_broadcast" and is_admin:
                    broadcast_text = context.user_data.get("broadcast_text", "")
                    if not broadcast_text:
                        await query.message.edit_text(
                            "Текст рассылки пуст. Введите текст заново.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                        )
                        await query.answer()
                        context.user_data["state"] = STATES["admin_broadcast"]
                        return STATES["admin_broadcast"]
                    users = await conn.fetch("SELECT user_id FROM users WHERE is_banned = FALSE")
                    success_count = 0
                    for user in users:
                        try:
                            await telegram_app.bot.send_message(
                                chat_id=user["user_id"],
                                text=broadcast_text,
                                parse_mode="HTML"
                            )
                            success_count += 1
                        except TelegramError as e:
                            logger.error(f"Failed to send broadcast to {user['user_id']}: {e}")
                    await query.message.edit_text(
                        f"Рассылка завершена. Отправлено {success_count} из {len(users)} пользователям.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    await query.answer()
                    context.user_data.pop("broadcast_text", None)
                    context.user_data["state"] = STATES["admin_panel"]
                    await log_analytics(user_id, "complete_broadcast", {"success_count": success_count, "total_users": len(users)})
                    return STATES["admin_panel"]

                elif data == "cancel_broadcast" and is_admin:
                    await query.message.edit_text(
                        "Рассылка отменена.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    await query.answer()
                    context.user_data.pop("broadcast_text", None)
                    context.user_data["state"] = STATES["admin_panel"]
                    await log_analytics(user_id, "cancel_broadcast", {})
                    return await show_admin_panel(update, context)

                elif data == "admin_edit_profile" and is_admin:
                    await query.message.edit_text(
                        "Введите ID пользователя для редактирования:",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("📋 Все пользователи", callback_data="all_users")],
                            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]
                        ])
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["admin_edit_profile"]
                    await log_analytics(user_id, "start_edit_profile", {})
                    return STATES["admin_edit_profile"]

                elif data == "all_users" and is_admin:
                    users = await conn.fetch(
                        "SELECT user_id, username, stars_bought FROM users ORDER BY stars_bought DESC LIMIT 10"
                    )
                    text_lines = []
                    for user in users:
                        username = f"@{user['username']}" if user['username'] else f"ID <code>{user['user_id']}</code>"
                        text_lines.append(f"{username}, ID <code>{user['user_id']}</code> Звезды: {user['stars_bought']}")
                    text = await get_text(
                        "all_users",
                        users_list="\n".join(text_lines) if text_lines else "Пользователи не найдены."
                    )
                    await query.message.edit_text(
                        text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад в админ-панель", callback_data="back_to_admin")]]),
                        parse_mode="HTML"
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["all_users"]
                    await log_analytics(user_id, "view_all_users", {"users_count": len(users)})
                    return STATES["all_users"]

                elif data == "edit_profile_stars" and is_admin:
                    context.user_data["edit_profile_field"] = "stars_bought"
                    await query.message.edit_text(
                        "Введите новое количество звезд:",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["admin_edit_profile"]
                    await log_analytics(user_id, "start_edit_stars", {})
                    return STATES["admin_edit_profile"]

                elif data == "edit_profile_referrals" and is_admin:
                    context.user_data["edit_profile_field"] = "referrals"
                    await query.message.edit_text(
                        "Введите ID рефералов через запятую:",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["admin_edit_profile"]
                    await log_analytics(user_id, "start_edit_referrals", {})
                    return STATES["admin_edit_profile"]

                elif data == "edit_profile_ref_bonus" and is_admin:
                    context.user_data["edit_profile_field"] = "ref_bonus_ton"
                    await query.message.edit_text(
                        "Введите новый реферальный бонус (TON):",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["admin_edit_profile"]
                    await log_analytics(user_id, "start_edit_ref_bonus", {})
                    return STATES["admin_edit_profile"]

                elif data == "set_db_reminder" and is_admin:
                    await query.message.edit_text(
                        "Введите дату напоминания в формате гггг-мм-дд:",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["set_db_reminder"]
                    await log_analytics(user_id, "start_set_db_reminder", {})
                    return STATES["set_db_reminder"]

                elif data == "view_db_reminder" and is_admin:
                    reminder = await conn.fetchrow("SELECT reminder_date FROM reminders WHERE reminder_type = 'db_update'")
                    reminder_text = reminder["reminder_date"].strftime("%Y-%m-%d") if reminder else "Не установлено"
                    text = await get_text("db_reminder_exists", reminder_date=reminder_text)
                    await query.message.edit_text(
                        text,
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🗑️ Очистить", callback_data="clear_db_reminder")],
                            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]
                        ]),
                        parse_mode="HTML"
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["admin_panel"]
                    await log_analytics(user_id, "view_db_reminder", {"reminder_date": reminder_text})
                    return STATES["admin_panel"]

                elif data == "clear_db_reminder" and is_admin:
                    await conn.execute("DELETE FROM reminders WHERE reminder_type = 'db_update'")
                    await query.message.edit_text(
                        "Напоминания об обновлении базы данных удалены.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["admin_panel"]
                    await log_analytics(user_id, "clear_db_reminder", {})
                    return await show_admin_panel(update, context)

                elif data == "bot_settings" and is_admin:
                    price_usd = await conn.fetchval("SELECT value FROM settings WHERE key = 'price_usd'") or PRICE_USD_PER_50
                    markup = await conn.fetchval("SELECT value FROM settings WHERE key = 'markup'") or MARKUP_PERCENTAGE
                    ref_bonus = await conn.fetchval("SELECT value FROM settings WHERE key = 'ref_bonus'") or REFERRAL_BONUS_PERCENTAGE
                    text = await get_text(
                        "bot_settings",
                        price_usd=price_usd,
                        markup=markup,
                        ref_bonus=ref_bonus
                    )
                    await query.message.edit_text(
                        text,
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("Цена за 50 звезд ($)", callback_data="set_price_usd")],
                            [InlineKeyboardButton("Процент накрутки (%)", callback_data="set_markup")],
                            [InlineKeyboardButton("Реферальный бонус (%)", callback_data="set_ref_bonus")],
                            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]
                        ]),
                        parse_mode="HTML"
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["bot_settings"]
                    await log_analytics(user_id, "start_bot_settings", {})
                    return STATES["bot_settings"]

                elif data == "set_price_usd" and is_admin:
                    context.user_data["setting_field"] = "price_usd"
                    await query.message.edit_text(
                        "Введите новую цену за 50 звезд в USD:",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["bot_settings"]
                    await log_analytics(user_id, "start_set_price_usd", {})
                    return STATES["bot_settings"]

                elif data == "set_markup" and is_admin:
                    context.user_data["setting_field"] = "markup"
                    await query.message.edit_text(
                        "Введите новый процент накрутки:",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["bot_settings"]
                    await log_analytics(user_id, "start_set_markup", {})
                    return STATES["bot_settings"]

                elif data == "set_ref_bonus" and is_admin:
                    context.user_data["setting_field"] = "ref_bonus"
                    await query.message.edit_text(
                        "Введите новый процент реферального бонуса:",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["bot_settings"]
                    await log_analytics(user_id, "start_set_ref_bonus", {})
                    return STATES["bot_settings"]

                elif data == "back_to_admin" and is_admin:
                    return await show_admin_panel(update, context)

                elif data == "back_to_menu":
                    total_stars = await conn.fetchval("SELECT SUM(stars_bought) FROM users") or 0
                    user_stars = await conn.fetchval("SELECT stars_bought FROM users WHERE user_id = $1", user_id) or 0
                    text = await get_text("welcome", total_stars=total_stars, stars_bought=user_stars)
                    keyboard = [
                        [
                            InlineKeyboardButton("📰 Новости", url=NEWS_CHANNEL),
                            InlineKeyboardButton("📞 Поддержка и Отзывы", url=SUPPORT_CHANNEL)
                        ],
                        [
                            InlineKeyboardButton("👤 Профиль", callback_data="profile"),
                            InlineKeyboardButton("🤝 Рефералы", callback_data="referrals")
                        ],
                        [InlineKeyboardButton("🛒 Купить звезды", callback_data="buy_stars")]
                    ]
                    if is_admin:
                        keyboard.append([InlineKeyboardButton("🔧 Админ-панель", callback_data="admin_panel")])
                    await query.message.edit_text(
                        text,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode="HTML"
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["main_menu"]
                    await log_analytics(user_id, "back_to_menu", {})
                    return STATES["main_menu"]

                elif data == "ban_user" and is_admin:
                    await query.message.edit_text(
                        "Введите ID пользователя для бана:",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("📋 Все пользователи", callback_data="all_users")],
                            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]
                        ])
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["ban_user"]
                    await log_analytics(user_id, "start_ban_user", {})
                    return STATES["ban_user"]

                elif data == "unban_user" and is_admin:
                    await query.message.edit_text(
                        "Введите ID пользователя для разбана:",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("📋 Все пользователи", callback_data="all_users")],
                            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]
                        ])
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["unban_user"]
                    await log_analytics(user_id, "start_unban_user", {})
                    return STATES["unban_user"]

                elif data == "tech_break" and is_admin:
                    await query.message.edit_text(
                        "Введите длительность технического перерыва (в минутах) и причину (формат: <минуты> <причина>):",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["tech_break"]
                    await log_analytics(user_id, "start_tech_break", {})
                    return STATES["tech_break"]

                else:
                    await query.message.edit_text(
                        "Неизвестная команда. Вернитесь в главное меню.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["main_menu"]
                    await log_analytics(user_id, "unknown_callback", {"data": data})
                    return STATES["main_menu"]

        except asyncpg.exceptions.InterfaceError as e:
            logger.error(f"Database pool error in callback_query_handler: {e}", exc_info=True)
            await query.message.edit_text(
                "Ошибка базы данных. Попробуйте позже.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
            )
            await query.answer()
            context.user_data["state"] = STATES["main_menu"]
            await log_analytics(user_id, "db_error_callback", {"error": str(e)})
            return STATES["main_menu"]
        except TelegramError as e:
            logger.error(f"Telegram API error in callback_query_handler for user_id={user_id}: {e}", exc_info=True)
            ERRORS.labels(type="telegram", endpoint="callback_query").inc()
            await query.message.edit_text(
                "Ошибка Telegram API. Попробуйте позже.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
            )
            await query.answer()
            context.user_data["state"] = STATES["main_menu"]
            await log_analytics(user_id, "telegram_error_callback", {"error": str(e)})
            return STATES["main_menu"]
        except Exception as e:
            logger.error(f"Unexpected error in callback_query_handler for user_id={user_id}: {e}", exc_info=True)
            ERRORS.labels(type="unexpected", endpoint="callback_query").inc()
            await query.message.edit_text(
                "Произошла ошибка. Попробуйте позже.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
            )
            await query.answer()
            context.user_data["state"] = STATES["main_menu"]
            await log_analytics(user_id, "unexpected_error_callback", {"error": str(e)})
            return STATES["main_menu"]

async def show_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    async with (await ensure_db_pool()) as conn:
        reminder = await conn.fetchrow("SELECT reminder_date FROM reminders WHERE reminder_type = 'db_update'")
        reminder_date = reminder["reminder_date"].strftime("%Y-%m-%d") if reminder else "Не установлено"
        text = await get_text("admin_panel", reminder_date=reminder_date)
        keyboard = [
            [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
            [InlineKeyboardButton("📢 Рассылка", callback_data="broadcast_message")],
            [InlineKeyboardButton("👤 Редактировать профиль", callback_data="admin_edit_profile")],
            [InlineKeyboardButton("📜 История транзакций", callback_data="transaction_history_0")],
            [InlineKeyboardButton("🔔 Установить напоминание БД", callback_data="set_db_reminder")],
            [InlineKeyboardButton("📋 Просмотреть напоминание БД", callback_data="view_db_reminder")],
            [InlineKeyboardButton("⚙️ Настройки бота", callback_data="bot_settings")],
            [InlineKeyboardButton("🚫 Забанить пользователя", callback_data="ban_user")],
            [InlineKeyboardButton("✅ Разбанить пользователя", callback_data="unban_user")],
            [InlineKeyboardButton("🛠 Технический перерыв", callback_data="tech_break")],
            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]
        ]
        try:
            await update.callback_query.message.edit_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML"
            )
            await update.callback_query.answer()
            context.user_data["state"] = STATES["admin_panel"]
            await log_analytics(user_id, "open_admin_panel", {})
            return STATES["admin_panel"]
        except BadRequest as e:
            if "Message is not modified" in str(e):
                pass
            else:
                raise

async def calculate_price_ton(stars: int) -> float:
    async with (await ensure_db_pool()) as conn:
        price_usd = await conn.fetchval("SELECT value FROM settings WHERE key = 'price_usd'") or PRICE_USD_PER_50
        markup = await conn.fetchval("SELECT value FROM settings WHERE key = 'markup'") or MARKUP_PERCENTAGE
        price_usd = (stars / 50) * price_usd * (1 + markup / 100)
        ton_price = telegram_app.bot_data.get("ton_price_info", {}).get("price", 0.0)
        if ton_price == 0.0:
            await update_ton_price()
            ton_price = telegram_app.bot_data.get("ton_price_info", {}).get("price", 0.0)
        if ton_price == 0.0:
            logger.error("Не удалось получить цену TON для расчета")
            return 0.0
        return price_usd / ton_price

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()
    state = context.user_data.get("state", STATES["main_menu"])
    REQUESTS.labels(endpoint="message").inc()
    with RESPONSE_TIME.labels(endpoint="message").time():
        logger.info(f"Message received: user_id={user_id}, text={text}, state={state}")
        try:
            async with (await ensure_db_pool()) as conn:
                is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id) or False
                is_banned = await conn.fetchval("SELECT is_banned FROM users WHERE user_id = $1", user_id) or False
                if is_banned:
                    text = await get_text("user_banned", support_channel=SUPPORT_CHANNEL)
                    await update.message.reply_text(
                        text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
                    )
                    context.user_data["state"] = STATES["main_menu"]
                    await log_analytics(user_id, "message_banned", {})
                    return STATES["main_menu"]
                if tech_break_info and tech_break_info["end_time"] > datetime.now(pytz.UTC) and not is_admin:
                    time_remaining = await format_time_remaining(tech_break_info["end_time"])
                    text = await get_text(
                        "tech_break_active",
                        end_time=tech_break_info["end_time"].strftime("%Y-%m-%d %H:%M:%S UTC"),
                        minutes_left=time_remaining,
                        reason=tech_break_info["reason"]
                    )
                    await update.message.reply_text(
                        text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
                    )
                    context.user_data["state"] = STATES["main_menu"]
                    await log_analytics(user_id, "message_tech_break", {})
                    return STATES["main_menu"]

                if state == STATES["buy_stars_recipient"]:
                    if not text.startswith("@"):
                        await update.message.reply_text(
                            "Пожалуйста, введите имя пользователя в формате @username.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="buy_stars")]])
                        )
                        return STATES["buy_stars_recipient"]
                    context.user_data["recipient"] = text
                    stars = context.user_data.get("stars_amount", "Не выбрано")
                    price_ton = await calculate_price_ton(int(stars)) if stars and isinstance(stars, str) and stars.isdigit() else None
                    price_text = f"~{price_ton:.2f} TON" if price_ton is not None else "Цена"
                    reply_text = (
                        f"Пользователь: {text}\n"
                        f"Количество звезд: {stars}\n"
                        f"Способ оплаты: TON Wallet"
                    )
                    keyboard = [
                        [InlineKeyboardButton(f"Пользователь: {text}", callback_data="select_recipient")],
                        [InlineKeyboardButton(f"Количество: {stars}", callback_data="select_stars_menu")],
                        [
                            InlineKeyboardButton(price_text, callback_data="show_price"),
                            InlineKeyboardButton("Оплатить", callback_data="proceed_to_payment")
                        ],
                        [InlineKeyboardButton("🔙 Назад", callback_data="buy_stars")]
                    ]
                    await update.message.reply_text(
                        reply_text,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode="HTML"
                    )
                    context.user_data["state"] = STATES["buy_stars_payment_method"]
                    await log_analytics(user_id, "select_recipient", {"recipient": text})
                    return STATES["buy_stars_payment_method"]

                elif state == STATES["buy_stars_custom"]:
                    if not text.isdigit() or int(text) <= 0:
                        await update.message.reply_text(
                            "Пожалуйста, введите положительное число звезд.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="select_stars_menu")]])
                        )
                        return STATES["buy_stars_custom"]
                    stars = text
                    context.user_data["stars_amount"] = stars
                    recipient = context.user_data.get("recipient", "Не выбран")
                    price_ton = await calculate_price_ton(int(stars))
                    reply_text = (
                        f"Пользователь: {recipient}\n"
                        f"Количество звезд: {stars}\n"
                        f"Способ оплаты: TON Wallet"
                    )
                    keyboard = [
                        [InlineKeyboardButton(f"Пользователь: {recipient}", callback_data="select_recipient")],
                        [InlineKeyboardButton(f"Количество: {stars}", callback_data="select_stars_menu")],
                        [
                            InlineKeyboardButton(f"~{price_ton:.2f} TON", callback_data="show_price"),
                            InlineKeyboardButton("Оплатить", callback_data="proceed_to_payment")
                        ],
                        [InlineKeyboardButton("🔙 Назад", callback_data="buy_stars")]
                    ]
                    await update.message.reply_text(
                        reply_text,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode="HTML"
                    )
                    context.user_data["state"] = STATES["buy_stars_payment_method"]
                    await log_analytics(user_id, "select_stars_custom", {"stars": stars})
                    return STATES["buy_stars_payment_method"]

                elif state == STATES["admin_broadcast"] and is_admin:
                    context.user_data["broadcast_text"] = text
                    await update.message.reply_text(
                        f"Подтвердите рассылку:\n\n{text}",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("✅ Подтвердить", callback_data="confirm_broadcast")],
                            [InlineKeyboardButton("❌ Отменить", callback_data="cancel_broadcast")]
                        ]),
                        parse_mode="HTML"
                    )
                    context.user_data["state"] = STATES["admin_broadcast"]
                    await log_analytics(user_id, "set_broadcast_text", {})
                    return STATES["admin_broadcast"]

                elif state == STATES["admin_edit_profile"] and is_admin:
                    edit_user_id = context.user_data.get("edit_user_id")
                    if not edit_user_id:
                        if not text.isdigit():
                            await update.message.reply_text(
                                "Пожалуйста, введите корректный ID пользователя.",
                                reply_markup=InlineKeyboardMarkup([
                                    [InlineKeyboardButton("📋 Все пользователи", callback_data="all_users")],
                                    [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]
                                ])
                            )
                            return STATES["admin_edit_profile"]
                        edit_user_id = int(text)
                        user_exists = await conn.fetchval("SELECT 1 FROM users WHERE user_id = $1", edit_user_id)
                        if not user_exists:
                            await update.message.reply_text(
                                "Пользователь не найден.",
                                reply_markup=InlineKeyboardMarkup([
                                    [InlineKeyboardButton("📋 Все пользователи", callback_data="all_users")],
                                    [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]
                                ])
                            )
                            return STATES["admin_edit_profile"]
                        context.user_data["edit_user_id"] = edit_user_id
                        await update.message.reply_text(
                            f"Редактирование пользователя ID {edit_user_id}. Выберите действие:",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("Изменить звезды", callback_data="edit_profile_stars")],
                                [InlineKeyboardButton("Изменить рефералов", callback_data="edit_profile_referrals")],
                                [InlineKeyboardButton("Изменить реф. бонус", callback_data="edit_profile_ref_bonus")],
                                [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]
                            ])
                        )
                        await log_analytics(user_id, "select_edit_user", {"edit_user_id": edit_user_id})
                        return STATES["admin_edit_profile"]
                    else:
                        field = context.user_data.get("edit_profile_field")
                        if field == "stars_bought":
                            if not text.isdigit() or int(text) < 0:
                                await update.message.reply_text(
                                    "Пожалуйста, введите неотрицательное число звезд.",
                                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                                )
                                return STATES["admin_edit_profile"]
                            await conn.execute(
                                "UPDATE users SET stars_bought = $1 WHERE user_id = $2",
                                int(text), edit_user_id
                            )
                            await update.message.reply_text(
                                f"Количество звезд для пользователя ID {edit_user_id} обновлено.",
                                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                            )
                            await log_analytics(user_id, "edit_stars", {"edit_user_id": edit_user_id, "stars": text})
                        elif field == "referrals":
                            try:
                                referrals = [int(x) for x in text.split(",") if x.strip().isdigit()]
                                await conn.execute(
                                    "UPDATE users SET referrals = $1 WHERE user_id = $2",
                                    json.dumps(referrals), edit_user_id
                                )
                                await update.message.reply_text(
                                    f"Рефералы для пользователя ID {edit_user_id} обновлены.",
                                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                                )
                                await log_analytics(user_id, "edit_referrals", {"edit_user_id": edit_user_id, "referrals": referrals})
                            except ValueError:
                                await update.message.reply_text(
                                    "Пожалуйста, введите ID рефералов через запятую.",
                                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                                )
                                return STATES["admin_edit_profile"]
                        elif field == "ref_bonus_ton":
                            try:
                                ref_bonus = float(text)
                                if ref_bonus < 0:
                                    raise ValueError("Бонус не может быть отрицательным")
                                await conn.execute(
                                    "UPDATE users SET ref_bonus_ton = $1 WHERE user_id = $2",
                                    ref_bonus, edit_user_id
                                )
                                await update.message.reply_text(
                                    f"Реферальный бонус для пользователя ID {edit_user_id} обновлен.",
                                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                                )
                                await log_analytics(user_id, "edit_ref_bonus", {"edit_user_id": edit_user_id, "ref_bonus": ref_bonus})
                            except ValueError:
                                await update.message.reply_text(
                                    "Пожалуйста, введите корректное число для реферального бонуса.",
                                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                                )
                                return STATES["admin_edit_profile"]
                        context.user_data.pop("edit_user_id", None)
                        context.user_data.pop("edit_profile_field", None)
                        context.user_data["state"] = STATES["admin_panel"]
                        return await show_admin_panel(update, context)

                elif state == STATES["set_db_reminder"] and is_admin:
                    try:
                        reminder_date = datetime.strptime(text, "%Y-%m-%d").date()
                        existing_reminder = await conn.fetchval(
                            "SELECT 1 FROM reminders WHERE reminder_type = 'db_update'"
                        )
                        if existing_reminder:
                            existing_date = await conn.fetchval(
                                "SELECT reminder_date FROM reminders WHERE reminder_type = 'db_update'"
                            )
                            await update.message.reply_text(
                                await get_text("db_reminder_exists", reminder_date=existing_date.strftime("%Y-%m-%d")),
                                reply_markup=InlineKeyboardMarkup([
                                    [InlineKeyboardButton("🗑️ Очистить", callback_data="clear_db_reminder")],
                                    [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]
                                ])
                            )
                            return STATES["set_db_reminder"]
                        await conn.execute(
                            "INSERT INTO reminders (user_id, reminder_date, reminder_type, created_at) "
                            "VALUES ($1, $2, $3, $4)",
                            user_id, reminder_date, "db_update", datetime.now(pytz.UTC)
                        )
                        await update.message.reply_text(
                            await get_text("reminder_set", reminder_date=reminder_date.strftime("%Y-%m-%d")),
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                        )
                        context.user_data["state"] = STATES["admin_panel"]
                        await log_analytics(user_id, "set_db_reminder", {"reminder_date": text})
                        return await show_admin_panel(update, context)
                    except ValueError:
                        await update.message.reply_text(
                            "Пожалуйста, введите дату в формате гггг-мм-дд.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                        )
                        return STATES["set_db_reminder"]

                elif state == STATES["bot_settings"] and is_admin:
                    setting_field = context.user_data.get("setting_field")
                    try:
                        value = float(text)
                        if value < 0:
                            raise ValueError("Значение не может быть отрицательным")
                        await conn.execute(
                            "INSERT INTO settings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2",
                            setting_field, value
                        )
                        await update.message.reply_text(
                            f"{setting_field} обновлено до {value}.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                        )
                        context.user_data.pop("setting_field", None)
                        context.user_data["state"] = STATES["admin_panel"]
                        await log_analytics(user_id, f"update_{setting_field}", {"value": value})
                        return await show_admin_panel(update, context)
                    except ValueError:
                        await update.message.reply_text(
                            f"Пожалуйста, введите корректное число для {setting_field}.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                        )
                        return STATES["bot_settings"]

                elif state == STATES["ban_user"] and is_admin:
                    if not text.isdigit():
                        await update.message.reply_text(
                            "Пожалуйста, введите корректный ID пользователя.",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("📋 Все пользователи", callback_data="all_users")],
                                [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]
                            ])
                        )
                        return STATES["ban_user"]
                    ban_user_id = int(text)
                    user_exists = await conn.fetchval("SELECT 1 FROM users WHERE user_id = $1", ban_user_id)
                    if not user_exists:
                        await update.message.reply_text(
                            "Пользователь не найден.",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("📋 Все пользователи", callback_data="all_users")],
                                [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]
                            ])
                        )
                        return STATES["ban_user"]
                    await conn.execute("UPDATE users SET is_banned = TRUE WHERE user_id = $1", ban_user_id)
                    await update.message.reply_text(
                        f"Пользователь ID {ban_user_id} забанен.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    context.user_data["state"] = STATES["admin_panel"]
                    await log_analytics(user_id, "ban_user", {"ban_user_id": ban_user_id})
                    return await show_admin_panel(update, context)

                elif state == STATES["unban_user"] and is_admin:
                    if not text.isdigit():
                        await update.message.reply_text(
                            "Пожалуйста, введите корректный ID пользователя.",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("📋 Все пользователи", callback_data="all_users")],
                                [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]
                            ])
                        )
                        return STATES["unban_user"]
                    unban_user_id = int(text)
                    user_exists = await conn.fetchval("SELECT 1 FROM users WHERE user_id = $1", unban_user_id)
                    if not user_exists:
                        await update.message.reply_text(
                            "Пользователь не найден.",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("📋 Все пользователи", callback_data="all_users")],
                                [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]
                            ])
                        )
                        return STATES["unban_user"]
                    await conn.execute("UPDATE users SET is_banned = FALSE WHERE user_id = $1", unban_user_id)
                    await update.message.reply_text(
                        f"Пользователь ID {unban_user_id} разбанен.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    context.user_data["state"] = STATES["admin_panel"]
                    await log_analytics(user_id, "unban_user", {"unban_user_id": unban_user_id})
                    return await show_admin_panel(update, context)

                elif state == STATES["tech_break"] and is_admin:
                    try:
                        minutes, reason = text.split(" ", 1)
                        minutes = int(minutes)
                        if minutes <= 0:
                            raise ValueError("Длительность должна быть положительным числом")
                        tech_break_info = {
                            "end_time": datetime.now(pytz.UTC) + timedelta(minutes=minutes),
                            "reason": reason
                        }
                        await update.message.reply_text(
                            f"Технический перерыв установлен до {tech_break_info['end_time'].strftime('%Y-%m-%d %H:%M:%S UTC')}.\nПричина: {reason}",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                        )
                        context.user_data["state"] = STATES["admin_panel"]
                        await log_analytics(user_id, "set_tech_break", {"minutes": minutes, "reason": reason})
                        return await show_admin_panel(update, context)
                    except ValueError:
                        await update.message.reply_text(
                            "Пожалуйста, введите длительность в минутах и причину (формат: <минуты> <причина>).",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                        )
                        return STATES["tech_break"]

                else:
                    total_stars = await conn.fetchval("SELECT SUM(stars_bought) FROM users") or 0
                    user_stars = await conn.fetchval("SELECT stars_bought FROM users WHERE user_id = $1", user_id) or 0
                    text = await get_text("welcome", total_stars=total_stars, stars_bought=user_stars)
                    keyboard = [
                        [
                            InlineKeyboardButton("📰 Новости", url=NEWS_CHANNEL),
                            InlineKeyboardButton("📞 Поддержка и Отзывы", url=SUPPORT_CHANNEL)
                        ],
                        [
                            InlineKeyboardButton("👤 Профиль", callback_data="profile"),
                            InlineKeyboardButton("🤝 Рефералы", callback_data="referrals")
                        ],
                        [InlineKeyboardButton("🛒 Купить звезды", callback_data="buy_stars")]
                    ]
                    if is_admin:
                        keyboard.append([InlineKeyboardButton("🔧 Админ-панель", callback_data="admin_panel")])
                    await update.message.reply_text(
                        text,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode="HTML"
                    )
                    context.user_data["state"] = STATES["main_menu"]
                    await log_analytics(user_id, "default_message", {})
                    return STATES["main_menu"]

        except asyncpg.exceptions.InterfaceError as e:
            logger.error(f"Database pool error in message_handler: {e}", exc_info=True)
            await update.message.reply_text(
                "Ошибка базы данных. Попробуйте позже.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
            )
            context.user_data["state"] = STATES["main_menu"]
            await log_analytics(user_id, "db_error_message", {"error": str(e)})
            return STATES["main_menu"]
        except TelegramError as e:
            logger.error(f"Telegram API error in message_handler for user_id={user_id}: {e}", exc_info=True)
            ERRORS.labels(type="telegram", endpoint="message").inc()
            await update.message.reply_text(
                "Ошибка Telegram API. Попробуйте позже.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
            )
            context.user_data["state"] = STATES["main_menu"]
            await log_analytics(user_id, "telegram_error_message", {"error": str(e)})
            return STATES["main_menu"]
        except Exception as e:
            logger.error(f"Unexpected error in message_handler for user_id={user_id}: {e}", exc_info=True)
            ERRORS.labels(type="unexpected", endpoint="message").inc()
            await update.message.reply_text(
                "Произошла ошибка. Попробуйте позже.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
            )
            context.user_data["state"] = STATES["main_menu"]
            await log_analytics(user_id, "unexpected_error_message", {"error": str(e)})
            return STATES["main_menu"]

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Update {update} caused error {context.error}", exc_info=True)
    ERRORS.labels(type="handler", endpoint="error").inc()
    if update and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "Произошла ошибка. Попробуйте позже.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
            )
        except Exception as e:
            logger.error(f"Failed to send error message: {e}", exc_info=True)

# Aiohttp application setup for webhook and Flask integration
async def lifespan(app: web.Application):
    global telegram_app
    try:
        await check_environment()
        await ensure_db_pool()
        await init_db()
        await load_settings()
        telegram_app = (
            ApplicationBuilder()
            .token(BOT_TOKEN)
            .http_version("1.1")
            .concurrent_updates(True)
            .build()
        )
        setup_handlers(telegram_app)
        await telegram_app.initialize()
        await telegram_app.start()
        logger.info("Telegram application initialized and started")
        webhook_url = f"{WEBHOOK_URL}/webhook"
        await telegram_app.bot.set_webhook(webhook_url, drop_pending_updates=True)
        logger.info(f"Telegram webhook set to {webhook_url}")
        # Fallback to polling for debugging
        asyncio.create_task(telegram_app.run_polling(drop_pending_updates=True))
        logger.info("Telegram application running in polling mode for debugging")
        scheduler = AsyncIOScheduler(timezone=pytz.UTC)
        scheduler.add_job(update_ton_price, "interval", minutes=5, id="update_ton_price", replace_existing=True)
        scheduler.start()
        logger.info("Scheduler started for TON price updates")
        yield
        if telegram_app is not None:
            await telegram_app.shutdown()
            logger.info("Telegram application shut down")
        await close_db_pool()
        scheduler.shutdown()
        logger.info("Scheduler shut down")
    except Exception as e:
        logger.error(f"Error during application lifespan: {e}", exc_info=True)
        raise

async def webhook_handler(request: web.Request):
    try:
        update = await request.json()
        logger.info(f"Received webhook update: {json.dumps(update, indent=2)}")
        update_obj = Update.de_json(update, telegram_app.bot)
        if update_obj is None:
            logger.error("Failed to parse update into Update object")
            return web.json_response({"status": "error", "message": "Invalid update format"}, status=400)
        await telegram_app.update_queue.put(update_obj)
        logger.info("Update added to telegram_app.update_queue")
        return web.json_response({"status": "ok"})
    except Exception as e:
        logger.error(f"Webhook error: {e}", exc_info=True)
        ERRORS.labels(type="webhook", endpoint="webhook").inc()
        return web.json_response({"status": "error", "message": str(e)}, status=500)

async def health_check(request: web.Request):
    return web.json_response({"status": "healthy"})

def create_aiohttp_app():
    app = web.Application()
    app.cleanup_ctx.append(lifespan)
    app.router.add_post("/webhook", webhook_handler)
    app.router.add_get("/health", health_check)
    wsgi_handler = WSGIHandler(app_flask)
    app.router.add_route("*", "/{path_info:.*}", wsgi_handler.handle_request)
    return app

if __name__ == "__main__":
    try:
        start_http_server(8000)  # Prometheus metrics
        logger.info("Prometheus metrics server started on port 8000")
        webhook_path = urlparse(WEBHOOK_URL).path if WEBHOOK_URL else "/webhook"
        app = create_aiohttp_app()
        web.run_app(app, host="0.0.0.0", port=PORT)
    except KeyboardInterrupt:
        logger.info("Application shutdown initiated")
    except Exception as e:
        logger.error(f"Application startup failed: {e}", exc_info=True)
        raise
