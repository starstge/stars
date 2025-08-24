import os
import pytz
import json
import logging
import asyncio
import aiohttp
import psycopg2
from asyncpg.pool import Pool
import signal
from functools import wraps
from aiohttp import ClientTimeout, web
from urllib.parse import urlparse
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from typing import Optional
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    PreCheckoutQueryHandler,
    filters,
    ContextTypes,
)
from telegram.error import BadRequest, TelegramError
import asyncpg
from datetime import datetime, timedelta, timezone
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
from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
import bcrypt
from aiohttp_wsgi import WSGIHandler
from asyncio import Lock

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
DB_URL = "postgresql://db_bkoq_user@dpg-d2do47juibrs738n229g-a.oregon-postgres.render.com/db_bkoq?sslmode=require"
DB_PASSWORD = "K3OwgY5JWTrdQprZsc11SkRcDy3d0VV5"
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
tech_break_info = {}  # Initialize as empty dict to avoid NoneType errors

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

async def pre_checkout_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.pre_checkout_query
    user_id = query.from_user.id
    logger.info(f"Pre-checkout query received: user_id={user_id}, invoice_payload={query.invoice_payload}")
    try:
        payload = query.invoice_payload
        await query.answer(ok=True)
    except Exception as e:
        logger.error(f"Ошибка в pre_checkout_callback: {e}", exc_info=True)
        await query.answer(ok=False, error_message="Ошибка обработки платежа")
    try:
        await log_analytics(user_id, "pre_checkout", {"invoice_payload": query.invoice_payload})
    except NameError:
        logger.warning("Функция log_analytics не определена, пропускаем логирование аналитики")

async def successful_payment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    payment = update.message.successful_payment
    logger.info(f"Успешный платеж: user_id={user_id}, invoice_payload={payment.invoice_payload}")
    try:
        async with (await ensure_db_pool()) as conn:
            await conn.execute(
                "UPDATE transactions SET checked_status = $1 WHERE invoice_id = $2",
                "completed", payment.invoice_payload
            )
            await update.message.reply_text(
                "Платеж успешно обработан! Спасибо за покупку.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
            )
            context.user_data["state"] = STATES["main_menu"]
            try:
                await log_analytics(user_id, "successful_payment", {"invoice_payload": payment.invoice_payload})
            except NameError:
                logger.warning("Функция log_analytics не определена, пропускаем логирование аналитики")
    except Exception as e:
        logger.error(f"Ошибка в successful_payment_callback: {e}", exc_info=True)
        await update.message.reply_text(
            "Ошибка при обработке платежа. Обратитесь в поддержку.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
        )

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
                    "referrals": u[4] if u[4] is not None else [],
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


_db_pool: Pool | None = None
_db_pool_lock = asyncio.Lock()

_db_pool_lock = Lock()

async def ensure_db_pool():
    global _db_pool
    max_retries = 5
    retry_delay = 2
    async with _db_pool_lock:
        for attempt in range(max_retries):
            try:
                if _db_pool is None or _db_pool._closed or _db_pool._closing:
                    logger.info(f"Creating new database pool (attempt {attempt + 1}/{max_retries})")
                    if _db_pool:
                        try:
                            await _db_pool.close()
                            logger.debug("Closed existing pool")
                        except Exception as e:
                            logger.warning(f"Error closing existing pool: {e}")
                    _db_pool = await asyncpg.create_pool(
                        dsn=os.getenv("DATABASE_URL"),
                        min_size=1,
                        max_size=10,
                        max_inactive_connection_lifetime=300,
                        timeout=30
                    )
                    logger.info("Database pool created successfully")
                async with _db_pool.acquire() as conn:
                    await conn.execute("SELECT 1")
                    logger.debug("Database pool validated successfully")
                return _db_pool
            except Exception as e:
                logger.warning(f"Failed to create/validate pool (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt + 1 < max_retries:
                    await asyncio.sleep(retry_delay)
                continue
        logger.error(f"Failed to create pool after {max_retries} attempts")
        raise asyncpg.exceptions.InterfaceError("Failed to establish database connection pool")
        
async def init_db():
    try:
        async with (await ensure_db_pool()) as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    stars_bought INTEGER DEFAULT 0,
                    ref_bonus_ton FLOAT DEFAULT 0.0,
                    referrals JSONB DEFAULT '[]',
                    is_new BOOLEAN DEFAULT TRUE,
                    is_admin BOOLEAN DEFAULT FALSE,
                    is_banned BOOLEAN DEFAULT FALSE,
                    prefix TEXT DEFAULT 'Beginner',
                    referrer_id BIGINT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """)
            logger.info("Users table created or verified")

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

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value FLOAT
                )
            """)
            logger.info("Settings table created or verified")

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

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS ton_price (
                    price FLOAT,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """)
            logger.info("Ton_price table created or verified")

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

logger = logging.getLogger(__name__)
_db_pool = None
_db_pool_lock = Lock()

async def ensure_db_pool():
    global _db_pool
    max_retries = 5
    retry_delay = 2
    async with _db_pool_lock:
        for attempt in range(max_retries):
            try:
                if _db_pool is None or _db_pool._closed or _db_pool._closing:
                    logger.info(f"Creating new database pool (attempt {attempt + 1}/{max_retries})")
                    if _db_pool:
                        try:
                            await _db_pool.close()
                            logger.debug("Closed existing pool")
                        except Exception as e:
                            logger.warning(f"Error closing existing pool: {e}")
                    _db_pool = await asyncpg.create_pool(
                        dsn=os.getenv("DATABASE_URL"),
                        min_size=1,
                        max_size=10,
                        max_inactive_connection_lifetime=300,
                        timeout=30
                    )
                    logger.info("Database pool created successfully")
                async with _db_pool.acquire() as conn:
                    await conn.execute("SELECT 1")
                    logger.debug("Database pool validated successfully")
                return _db_pool
            except Exception as e:
                logger.warning(f"Failed to create/validate pool (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt + 1 < max_retries:
                    await asyncio.sleep(retry_delay)
                continue
        logger.error(f"Failed to create pool after {max_retries} attempts")
        raise asyncpg.exceptions.InterfaceError("Failed to establish database connection pool")

async def health_check(request: web.Request) -> web.Response:
    logger.info("Health check called: %s %s", request.method, request.path)
    try:
        pool = await ensure_db_pool()
        async with pool.acquire() as conn:
            await conn.execute("SELECT 1")
        return web.json_response({"status": "healthy", "database": "connected"})
    except Exception as e:
        logger.error(f"Health check failed: {e}", exc_info=True)
        return web.json_response(
            {"status": "running", "database": "unavailable", "error": str(e)},
            status=200
        )
Changes:

async def update_ton_price(context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.debug("Updating TON price...")
    max_retries = 3
    base_delay = 120
    for attempt in range(max_retries):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "https://api.coingecko.com/api/v3/coins/the-open-network",
                    headers={"User-Agent": "TelegramBot/1.0"}
                ) as response:
                    if response.status == 429:
                        delay = base_delay * (2 ** attempt)
                        logger.warning(f"Rate limit hit on attempt {attempt + 1}/{max_retries}, waiting {delay}s")
                        await asyncio.sleep(delay)
                        continue
                    if response.status != 200:
                        logger.error(f"Failed to fetch TON price: HTTP {response.status}")
                        break
                    data = await response.json()
                    price = float(data["market_data"]["current_price"]["usd"])
                    diff_24h = data["market_data"].get("price_change_percentage_24h", 0.0)
                    context.bot_data["ton_price_info"] = {
                        "price": price,
                        "diff_24h": float(str(diff_24h).replace("−", "-").rstrip("%") or 0.0),
                        "updated_at": datetime.now(pytz.UTC)
                    }
                    logger.debug(f"TON price updated: price={price}, diff_24h={diff_24h}")
                    try:
                        async with (await ensure_db_pool()) as conn:
                            await conn.execute(
                                "INSERT INTO ton_price (price, updated_at) VALUES ($1, $2)",
                                price, datetime.now(pytz.UTC)
                            )
                            logger.debug("Stored TON price in database")
                    except Exception as e:
                        logger.warning(f"Failed to store TON price in database: {e}")
                    return
        except Exception as e:
            logger.error(f"Error in update_ton_price (attempt {attempt + 1}/{max_retries}): {e}", exc_info=True)
            if attempt + 1 < max_retries:
                await asyncio.sleep(base_delay * (2 ** attempt))
            continue
    logger.error("Failed to update TON price after all retries")
    try:
        async with (await ensure_db_pool()) as conn:
            price_record = await conn.fetchrow(
                "SELECT price, updated_at FROM ton_price ORDER BY updated_at DESC LIMIT 1"
            )
            if price_record and (datetime.now(pytz.UTC) - price_record["updated_at"]).total_seconds() < 3600:
                context.bot_data["ton_price_info"] = {
                    "price": price_record["price"],
                    "diff_24h": 0.0,
                    "updated_at": price_record["updated_at"]
                }
                logger.info(f"Using database TON price: {price_record['price']}")
                return
    except Exception as e:
        logger.error(f"Failed to fetch TON price from database: {e}")
    context.bot_data["ton_price_info"] = {
        "price": 3.32,
        "diff_24h": 0.0,
        "updated_at": datetime.now(pytz.UTC)
    }
    logger.warning("Using fallback TON price: 3.32")
async def safe_reply_text(update: Update, text: str, reply_markup=None, parse_mode=None, retry_count=3):
    for attempt in range(retry_count):
        try:
            await update.message.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
            logger.debug(f"Successfully sent message to user_id={update.effective_user.id}")
            return
        except RetryAfter as e:
            logger.warning(f"Rate limit hit, retrying after {e.retry_after} seconds (attempt {attempt + 1}/{retry_count})")
            await asyncio.sleep(e.retry_after)
        except TelegramError as e:
            logger.error(f"Telegram API error: {e}", exc_info=True)
            if attempt + 1 == retry_count:
                logger.error(f"Failed to send message after {retry_count} attempts")
                raise
async def ton_price_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    username = update.effective_user.username or str(user_id)
    logger.debug(f"Entering ton_price_command handler: user_id={user_id}, username={username}, state={context.user_data.get('state', 0)}")

    try:
        # Reset state to ensure commands are processed correctly
        context.user_data["state"] = 0
        logger.debug(f"State reset to main_menu for user_id={user_id}")

        # Ensure database pool
        pool = await ensure_db_pool()
        async with pool.acquire() as conn:
            # Check admin status
            is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id) or False
            logger.debug(f"User admin status: is_admin={is_admin}")

            # Initialize tech_break_info if not present
            if "tech_break_info" not in context.bot_data:
                context.bot_data["tech_break_info"] = {"end_time": datetime.min.replace(tzinfo=pytz.UTC), "reason": ""}
                logger.debug("Initialized tech_break_info")

            # Check tech break
            if (
                context.bot_data["tech_break_info"].get("end_time", datetime.min.replace(tzinfo=pytz.UTC))
                > datetime.now(pytz.UTC)
                and not is_admin
            ):
                time_remaining = await format_time_remaining(context.bot_data["tech_break_info"]["end_time"])
                text = await get_text(
                    "tech_break_active",
                    end_time=context.bot_data["tech_break_info"]["end_time"].strftime("%Y-%m-%d %H:%M:%S UTC"),
                    minutes_left=time_remaining,
                    reason=context.bot_data["tech_break_info"].get("reason", "Не указана")
                )
                await update.message.reply_text(
                    text,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]),
                    parse_mode="HTML"
                )
                await log_analytics(user_id, "ton_price_tech_break", {})
                return 0

            # Check TON price in bot_data
            if "ton_price_info" not in context.bot_data or context.bot_data["ton_price_info"].get("price", 0.0) == 0.0:
                logger.debug("TON price not in bot_data, fetching...")
                await update_ton_price(context)

            price = context.bot_data.get("ton_price_info", {}).get("price", 0.0)
            diff_24h = context.bot_data.get("ton_price_info", {}).get("diff_24h", 0.0)

            # Fallback to database if price is unavailable
            if price == 0.0:
                logger.warning("TON price is zero, attempting to fetch from database")
                price_record = await conn.fetchrow(
                    "SELECT price, updated_at FROM ton_price ORDER BY updated_at DESC LIMIT 1"
                )
                if price_record and (datetime.now(pytz.UTC) - price_record["updated_at"]).total_seconds() < 3600:
                    price = price_record["price"]
                    diff_24h = 0.0  # No diff_24h available from DB
                else:
                    logger.error("No valid TON price available")
                    await update.message.reply_text(
                        "Ошибка получения цены TON. Попробуйте позже.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]),
                        parse_mode="HTML"
                    )
                    await log_analytics(user_id, "ton_price_error", {"error": "No valid price"})
                    return 0

            # Format response
            change_text = f"📈 +{diff_24h:.2f}%" if diff_24h >= 0 else f"📉 {diff_24h:.2f}%"
            text = f"💰 Цена TON: ${price:.2f}\nИзменение за 24ч: {change_text}"

            # Send response
            await update.message.reply_text(
                text,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]),
                parse_mode="HTML"
            )
            await log_analytics(user_id, "ton_price", {"price": price, "diff_24h": diff_24h})
            logger.info(f"Successfully executed /tonprice for user_id={user_id}")
            return 0

    except Exception as e:
        logger.error(f"Error in ton_price_command: {e}", exc_info=True)
        await update.message.reply_text(
            "Ошибка при получении цены TON. Попробуйте позже.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]),
            parse_mode="HTML"
        )
        await log_analytics(user_id, "ton_price_error", {"error": str(e)})
        return 0
        
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
    logger.debug(f"Entering start handler: user_id={user_id}, username={username}, args={context.args}, state={context.user_data.get('state', 0)}")

    try:
        # Reset state to ensure commands are processed correctly
        context.user_data["state"] = 0
        logger.debug(f"State reset to main_menu for user_id={user_id}")

        # Initialize referrer_id to None
        referrer_id = None
        if context.args and context.args[0].isdigit():
            referrer_id = int(context.args[0])
            logger.debug(f"Referrer ID provided: {referrer_id}")

        # Ensure database pool
        pool = await ensure_db_pool()
        async with pool.acquire() as conn:
            # Check if user exists
            user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
            if not user:
                if referrer_id:
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
                    "INSERT INTO users (user_id, username, stars_bought, referrals, ref_bonus_ton, is_admin, created_at) "
                    "VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    user_id, username, 0, json.dumps([]), 0.0, False, datetime.now(pytz.UTC)
                )
                logger.info(f"New user registered: user_id={user_id}, username={username}")
            else:
                await conn.execute(
                    "UPDATE users SET username = $1 WHERE user_id = $2",
                    username, user_id
                )
                logger.debug(f"Updated username for user_id={user_id}")

            # Check admin status
            is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id) or False
            logger.debug(f"User admin status: is_admin={is_admin}")

            # Initialize tech_break_info if not present
            if "tech_break_info" not in context.bot_data:
                context.bot_data["tech_break_info"] = {"end_time": datetime.min.replace(tzinfo=pytz.UTC), "reason": ""}
                logger.debug("Initialized tech_break_info")

            # Check tech break
            if (
                context.bot_data["tech_break_info"].get("end_time", datetime.min.replace(tzinfo=pytz.UTC))
                > datetime.now(pytz.UTC)
                and not is_admin
            ):
                time_remaining = await format_time_remaining(context.bot_data["tech_break_info"]["end_time"])
                text = await get_text(
                    "tech_break_active",
                    end_time=context.bot_data["tech_break_info"]["end_time"].strftime("%Y-%m-%d %H:%M:%S UTC"),
                    minutes_left=time_remaining,
                    reason=context.bot_data["tech_break_info"].get("reason", "Не указана")
                )
                await update.message.reply_text(
                    text,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]),
                    parse_mode="HTML"
                )
                await log_analytics(user_id, "start_tech_break", {"referrer_id": referrer_id if referrer_id is not None else "none"})
                return 0

            # Fetch total and user stars
            total_stars = await conn.fetchval("SELECT SUM(stars_bought) FROM users") or 0
            user_stars = await conn.fetchval("SELECT stars_bought FROM users WHERE user_id = $1", user_id) or 0
            text = await get_text("welcome", total_stars=total_stars, stars_bought=user_stars)

            # Build keyboard
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

            # Send response
            await update.message.reply_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML"
            )
            await log_analytics(user_id, "start", {"referrer_id": referrer_id if referrer_id is not None else "none"})
            logger.info(f"Successfully executed /start for user_id={user_id}")
            return 0

    except Exception as e:
        logger.error(f"Error in start handler: {e}", exc_info=True)
        await update.message.reply_text(
            "Произошла ошибка. Попробуйте снова или обратитесь в поддержку.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]),
            parse_mode="HTML"
        )
        await log_analytics(user_id, "start_error", {"error": str(e), "referrer_id": referrer_id if referrer_id is not None else "none"})
        return 0
async def show_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    user_id = query.from_user.id
    logger.info(f"Showing admin panel for user_id={user_id}")
    try:
        async with (await ensure_db_pool()) as conn:
            reminder = await conn.fetchrow("SELECT reminder_date FROM reminders WHERE reminder_type = 'db' ORDER BY created_at DESC LIMIT 1")
            reminder_date = reminder["reminder_date"].strftime("%Y-%m-%d") if reminder else "Не установлено"
            text = await get_text("admin_panel", reminder_date=reminder_date)
            keyboard = [
                [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
                [InlineKeyboardButton("📢 Рассылка", callback_data="broadcast_message")],
                [InlineKeyboardButton("👤 Редактировать профиль", callback_data="admin_edit_profile")],
                [InlineKeyboardButton("📅 Установить напоминание БД", callback_data="set_db_reminder")],
                [InlineKeyboardButton("🛠 Тех. перерыв", callback_data="tech_break")],
                [InlineKeyboardButton("⚙️ Настройки бота", callback_data="bot_settings")],
                [InlineKeyboardButton("🚫 Забанить", callback_data="ban_user")],
                [InlineKeyboardButton("✅ Разбанить", callback_data="unban_user")],
                [InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]
            ]
            current_text = query.message.text
            current_reply_markup = query.message.reply_markup
            new_reply_markup = InlineKeyboardMarkup(keyboard)
            if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                await query.message.edit_text(
                    text,
                    reply_markup=new_reply_markup,
                    parse_mode="HTML"
                )
            await query.answer()
            context.user_data["state"] = 8
            await log_analytics(user_id, "view_admin_panel", {})
            return 8
    except Exception as e:
        logger.error(f"Ошибка в show_admin_panel: {e}", exc_info=True)
        await query.message.edit_text(
            "Ошибка при открытии админ-панели. Попробуйте снова.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]),
            parse_mode="HTML"
        )
        await query.answer()
        context.user_data["state"] = 0
        await log_analytics(user_id, "admin_panel_error", {"error": str(e)})
        return 0

async def calculate_price_ton(context: ContextTypes.DEFAULT_TYPE, stars: int) -> float:
    try:
        async with (await ensure_db_pool()) as conn:
            price_usd = await conn.fetchval("SELECT value FROM settings WHERE key = 'price_usd'") or 0.81
            markup = await conn.fetchval("SELECT value FROM settings WHERE key = 'markup'") or 10.0
            price_usd = (stars / 50) * price_usd * (1 + markup / 100)
            if "ton_price_info" not in context.bot_data or context.bot_data["ton_price_info"].get("price", 0.0) == 0.0:
                await update_ton_price(context)
            ton_price = context.bot_data["ton_price_info"].get("price", 3.32)  # Fallback from logs
            if ton_price == 0.0:
                logger.warning("TON price is zero, fetching from database")
                price_record = await conn.fetchrow(
                    "SELECT price FROM ton_price ORDER BY updated_at DESC LIMIT 1"
                )
                ton_price = price_record["price"] if price_record else 3.32
            price_ton = price_usd / ton_price
            logger.debug(f"Calculated price: {stars} stars = {price_ton:.4f} TON")
            return round(price_ton, 4)
    except Exception as e:
        logger.error(f"Error in calculate_price_ton: {e}", exc_info=True)
        price_usd = (stars / 50) * 0.81 * (1 + 10 / 100)  # Fallback to defaults
        ton_price = context.bot_data.get("ton_price_info", {}).get("price", 3.32)
        price_ton = price_usd / ton_price
        logger.warning(f"Using fallback price: {stars} stars = {price_ton:.4f} TON")
        return round(price_ton, 4)

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    username = update.effective_user.username or str(user_id)
    text = update.message.text
    state = context.user_data.get("state", 0)
    logger.info(f"Message received: user_id={user_id}, state={state}, text={text}")

    try:
        async with (await ensure_db_pool()) as conn:
            is_banned = await conn.fetchval("SELECT is_banned FROM users WHERE user_id = $1", user_id) or False
            is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id) or False
            if is_banned:
                text = await get_text("user_banned", support_channel=SUPPORT_CHANNEL)
                await update.message.reply_text(
                    text,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📞 Поддержка", url=SUPPORT_CHANNEL)]]),
                    parse_mode="HTML"
                )
                await log_analytics(user_id, "banned_user_message", {})
                return 0

            if tech_break_info.get("end_time", datetime.min.replace(tzinfo=pytz.UTC)) > datetime.now(pytz.UTC) and not is_admin:
                time_remaining = await format_time_remaining(tech_break_info["end_time"])
                text = await get_text(
                    "tech_break_active",
                    end_time=tech_break_info["end_time"].strftime("%Y-%m-%d %H:%M:%S UTC"),
                    minutes_left=time_remaining,
                    reason=tech_break_info.get("reason", "Не указана")
                )
                await update.message.reply_text(
                    text,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]),
                    parse_mode="HTML"
                )
                context.user_data["state"] = 0
                await log_analytics(user_id, "message_tech_break", {})
                return 0

            if state == STATES["buy_stars_recipient"]:
                if not text.startswith("@") or len(text) < 2:
                    await update.message.reply_text(
                        "Пожалуйста, введите корректное имя пользователя, начиная с @.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="buy_stars")]])
                    )
                    await log_analytics(user_id, "invalid_recipient", {"input": text})
                    return 3
                context.user_data["recipient"] = text
                price_ton = await calculate_price_ton(context, int(context.user_data.get("stars_amount", 0))) if context.user_data.get("stars_amount") else None
                price_text = f"~{price_ton:.2f} TON" if price_ton is not None else "Цена"
                reply_text = (
                    f"Пользователь: {text}\n"
                    f"Количество звезд: {context.user_data.get('stars_amount', 'Не выбрано')}\n"
                    f"Способ оплаты: TON Wallet"
                )
                keyboard = [
                    [InlineKeyboardButton(f"Пользователь: {text}", callback_data="select_recipient")],
                    [InlineKeyboardButton(f"Количество: {context.user_data.get('stars_amount', 'Не выбрано')}", callback_data="select_stars_menu")],
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
                context.user_data["state"] = 5
                await log_analytics(user_id, "set_recipient", {"recipient": text})
                return 5

            elif state == STATES["buy_stars_custom"]:
                if not text.isdigit() or int(text) <= 0:
                    await update.message.reply_text(
                        "Пожалуйста, введите положительное число звезд.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="select_stars_menu")]])
                    )
                    await log_analytics(user_id, "invalid_stars_amount", {"input": text})
                    return 23
                context.user_data["stars_amount"] = text
                recipient = context.user_data.get("recipient", "Не выбран")
                price_ton = await calculate_price_ton(context, int(text))
                reply_text = (
                    f"Пользователь: {recipient}\n"
                    f"Количество звезд: {text}\n"
                    f"Способ оплаты: TON Wallet"
                )
                keyboard = [
                    [InlineKeyboardButton(f"Пользователь: {recipient}", callback_data="select_recipient")],
                    [InlineKeyboardButton(f"Количество: {text}", callback_data="select_stars_menu")],
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
                context.user_data["state"] = 5
                await log_analytics(user_id, "set_custom_stars", {"stars": text})
                return 5

            elif state == STATES["admin_broadcast"] and is_admin:
                context.user_data["broadcast_text"] = text
                reply_text = f"Подтвердите рассылку:\n\n{text}"
                keyboard = [
                    [InlineKeyboardButton("✅ Подтвердить", callback_data="confirm_broadcast")],
                    [InlineKeyboardButton("❌ Отменить", callback_data="cancel_broadcast")]
                ]
                await update.message.reply_text(
                    reply_text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
                context.user_data["state"] = 10
                await log_analytics(user_id, "set_broadcast_text", {"text_length": len(text)})
                return 10

            elif state == STATES["admin_edit_profile"] and is_admin:
                if not text.isdigit():
                    await update.message.reply_text(
                        "Пожалуйста, введите корректный ID пользователя.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="admin_edit_profile")]])
                    )
                    await log_analytics(user_id, "invalid_edit_user_id", {"input": text})
                    return 11
                edit_user_id = int(text)
                user = await conn.fetchrow("SELECT username, stars_bought, ref_bonus_ton, referrals FROM users WHERE user_id = $1", edit_user_id)
                if not user:
                    await update.message.reply_text(
                        "Пользователь не найден.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="admin_edit_profile")]])
                    )
                    await log_analytics(user_id, "edit_user_not_found", {"edit_user_id": edit_user_id})
                    return 11
                context.user_data["edit_user_id"] = edit_user_id
                username = f"@{user['username']}" if user['username'] else f"ID {edit_user_id}"
                reply_text = (
                    f"Редактирование пользователя {username}:\n"
                    f"Звезды: {user['stars_bought']}\n"
                    f"Реф. бонус: {user['ref_bonus_ton']} TON\n"
                    f"Рефералы: {len(json.loads(user['referrals']) if user['referrals'] else [])}"
                )
                keyboard = [
                    [InlineKeyboardButton("Изменить звезды", callback_data="edit_profile_stars")],
                    [InlineKeyboardButton("Изменить рефералов", callback_data="edit_profile_referrals")],
                    [InlineKeyboardButton("Изменить реф. бонус", callback_data="edit_profile_ref_bonus")],
                    [InlineKeyboardButton("🔙 Назад", callback_data="admin_edit_profile")]
                ]
                await update.message.reply_text(
                    reply_text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
                context.user_data["state"] = 11
                await log_analytics(user_id, "view_edit_profile", {"edit_user_id": edit_user_id})
                return 11

            elif state == STATES["admin_edit_profile"] and is_admin and "edit_profile_field" in context.user_data:
                edit_user_id = context.user_data.get("edit_user_id")
                field = context.user_data["edit_profile_field"]
                if field == "stars_bought":
                    if not text.isdigit() or int(text) < 0:
                        await update.message.reply_text(
                            "Пожалуйста, введите корректное количество звезд.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="admin_edit_profile")]])
                        )
                        await log_analytics(user_id, "invalid_stars_input", {"input": text})
                        return 11
                    await conn.execute(
                        "UPDATE users SET stars_bought = $1 WHERE user_id = $2",
                        int(text), edit_user_id
                    )
                    reply_text = f"Звезды для пользователя ID {edit_user_id} обновлены: {text}"
                elif field == "referrals":
                    try:
                        referral_ids = [int(r) for r in text.split(",") if r.strip().isdigit()]
                        await conn.execute(
                            "UPDATE users SET referrals = $1 WHERE user_id = $2",
                            json.dumps(referral_ids), edit_user_id
                        )
                        reply_text = f"Рефералы для пользователя ID {edit_user_id} обновлены."
                    except ValueError:
                        await update.message.reply_text(
                            "Пожалуйста, введите ID рефералов через запятую.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="admin_edit_profile")]])
                        )
                        await log_analytics(user_id, "invalid_referrals_input", {"input": text})
                        return 11
                elif field == "ref_bonus_ton":
                    try:
                        bonus = float(text)
                        if bonus < 0:
                            raise ValueError("Negative bonus")
                        await conn.execute(
                            "UPDATE users SET ref_bonus_ton = $1 WHERE user_id = $2",
                            bonus, edit_user_id
                        )
                        reply_text = f"Реферальный бонус для пользователя ID {edit_user_id} обновлен: {bonus} TON"
                    except ValueError:
                        await update.message.reply_text(
                            "Пожалуйста, введите корректное число для реф. бонуса.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="admin_edit_profile")]])
                        )
                        await log_analytics(user_id, "invalid_ref_bonus_input", {"input": text})
                        return 11
                await conn.execute(
                    "UPDATE users SET prefix = CASE "
                    "WHEN is_admin THEN 'Verified' "
                    "WHEN stars_bought >= 50000 THEN 'Verified' "
                    "WHEN stars_bought >= 10000 THEN 'Regular Buyer' "
                    "WHEN stars_bought >= 5000 THEN 'Buyer' "
                    "WHEN stars_bought >= 1000 THEN 'Newbie' "
                    "ELSE 'Beginner' END WHERE user_id = $1",
                    edit_user_id
                )
                await update.message.reply_text(
                    reply_text,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="admin_edit_profile")]])
                )
                context.user_data.pop("edit_profile_field", None)
                context.user_data["state"] = 11
                await log_analytics(user_id, f"edit_{field}", {"edit_user_id": edit_user_id, "value": text})
                return 11

            elif state == STATES["set_db_reminder"] and is_admin:
                try:
                    reminder_date = datetime.strptime(text, "%Y-%m-%d").date()
                    existing_reminder = await conn.fetchval(
                        "SELECT id FROM reminders WHERE reminder_type = 'db' AND reminder_date >= $1",
                        datetime.now(pytz.UTC).date()
                    )
                    if existing_reminder:
                        reply_text = await get_text(
                            "db_reminder_exists",
                            reminder_date=reminder_date.strftime("%Y-%m-%d")
                        )
                        await update.message.reply_text(
                            reply_text,
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                        )
                        await log_analytics(user_id, "db_reminder_exists", {"reminder_date": text})
                        return 14
                    await conn.execute(
                        "INSERT INTO reminders (user_id, reminder_date, reminder_type, created_at) "
                        "VALUES ($1, $2, $3, $4)",
                        user_id, reminder_date, "db", datetime.now(pytz.UTC)
                    )
                    reply_text = await get_text("reminder_set", reminder_date=reminder_date.strftime("%Y-%m-%d"))
                    await update.message.reply_text(
                        reply_text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    context.user_data["state"] = 8
                    await log_analytics(user_id, "set_db_reminder", {"reminder_date": text})
                    return 8
                except ValueError:
                    await update.message.reply_text(
                        "Неверный формат даты. Используйте гггг-мм-дд.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    await log_analytics(user_id, "invalid_reminder_date", {"input": text})
                    return 14

            elif state == STATES["tech_break"] and is_admin:
                try:
                    minutes, reason = text.split(" ", 1)
                    minutes = int(minutes)
                    if minutes <= 0:
                        raise ValueError("Minutes must be positive")
                    tech_break_info = {
                        "end_time": datetime.now(pytz.UTC) + timedelta(minutes=minutes),
                        "reason": reason
                    }
                    reply_text = await get_text(
                        "tech_break_active",
                        end_time=tech_break_info["end_time"].strftime("%Y-%m-%d %H:%M:%S UTC"),
                        minutes_left=minutes,
                        reason=reason
                    )
                    await update.message.reply_text(
                        reply_text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    context.user_data["state"] = 8
                    await log_analytics(user_id, "set_tech_break", {"minutes": minutes, "reason": reason})
                    return 8
                except ValueError:
                    await update.message.reply_text(
                        "Неверный формат. Введите: <минуты> <причина>",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    await log_analytics(user_id, "invalid_tech_break_input", {"input": text})
                    return 16

            elif state == STATES["bot_settings"] and is_admin and "edit_setting" in context.user_data:
                setting = context.user_data["edit_setting"]
                try:
                    value = float(text)
                    if value < 0:
                        raise ValueError("Value must be non-negative")
                    await conn.execute(
                        "INSERT INTO settings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                        setting, value
                    )
                    await load_settings()
                    reply_text = f"Настройка '{setting}' обновлена: {value}"
                    await update.message.reply_text(
                        reply_text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="bot_settings")]])
                    )
                    context.user_data.pop("edit_setting", None)
                    context.user_data["state"] = 17
                    await log_analytics(user_id, f"edit_{setting}", {"value": value})
                    return 17
                except ValueError:
                    await update.message.reply_text(
                        "Пожалуйста, введите корректное число.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="bot_settings")]])
                    )
                    await log_analytics(user_id, f"invalid_{setting}_input", {"input": text})
                    return 17

            elif state == STATES["ban_user"] and is_admin:
                if not text.isdigit():
                    await update.message.reply_text(
                        "Пожалуйста, введите корректный ID пользователя.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    await log_analytics(user_id, "invalid_ban_user_id", {"input": text})
                    return 20
                ban_user_id = int(text)
                user = await conn.fetchrow("SELECT username FROM users WHERE user_id = $1", ban_user_id)
                if not user:
                    await update.message.reply_text(
                        "Пользователь не найден.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    await log_analytics(user_id, "ban_user_not_found", {"ban_user_id": ban_user_id})
                    return 20
                await conn.execute("UPDATE users SET is_banned = TRUE WHERE user_id = $1", ban_user_id)
                reply_text = f"Пользователь ID {ban_user_id} забанен."
                await update.message.reply_text(
                    reply_text,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                )
                context.user_data["state"] = 8
                await log_analytics(user_id, "ban_user", {"ban_user_id": ban_user_id})
                return 8

            elif state == STATES["unban_user"] and is_admin:
                if not text.isdigit():
                    await update.message.reply_text(
                        "Пожалуйста, введите корректный ID пользователя.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    await log_analytics(user_id, "invalid_unban_user_id", {"input": text})
                    return 21
                unban_user_id = int(text)
                user = await conn.fetchrow("SELECT username FROM users WHERE user_id = $1", unban_user_id)
                if not user:
                    await update.message.reply_text(
                        "Пользователь не найден.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    await log_analytics(user_id, "unban_user_not_found", {"unban_user_id": unban_user_id})
                    return 21
                await conn.execute("UPDATE users SET is_banned = FALSE WHERE user_id = $1", unban_user_id)
                reply_text = f"Пользователь ID {unban_user_id} разбанен."
                await update.message.reply_text(
                    reply_text,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                )
                context.user_data["state"] = 8
                await log_analytics(user_id, "unban_user", {"unban_user_id": unban_user_id})
                return 8

            else:
                await update.message.reply_text(
                    "Неизвестная команда или состояние. Вернитесь в главное меню.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
                )
                context.user_data["state"] = 0
                await log_analytics(user_id, "unknown_message_state", {"state": state, "text": text})
                return 0

    except Exception as e:
        logger.error(f"Ошибка в message_handler: {e}", exc_info=True)
        await update.message.reply_text(
            "Произошла ошибка. Попробуйте снова или обратитесь в поддержку.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
        )
        context.user_data["state"] = 0
        await log_analytics(user_id, "message_error", {"error": str(e)})
        return 0


async def show_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    user_id = query.from_user.id
    logger.info(f"Showing admin panel for user_id={user_id}")
    try:
        async with (await ensure_db_pool()) as conn:
            reminder = await conn.fetchrow("SELECT reminder_date FROM reminders WHERE reminder_type = 'db' ORDER BY created_at DESC LIMIT 1")
            reminder_date = reminder["reminder_date"].strftime("%Y-%m-%d") if reminder else "Не установлено"
            text = await get_text("admin_panel", reminder_date=reminder_date)
            keyboard = [
                [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
                [InlineKeyboardButton("📜 Транзакции", callback_data="admin_transactions")],
                [InlineKeyboardButton("📢 Рассылка", callback_data="broadcast_message")],
                [InlineKeyboardButton("👤 Редактировать профиль", callback_data="admin_edit_profile")],
                [InlineKeyboardButton("📅 Установить напоминание БД", callback_data="set_db_reminder")],
                [InlineKeyboardButton("🛠 Тех. перерыв", callback_data="tech_break")],
                [InlineKeyboardButton("⚙️ Настройки бота", callback_data="bot_settings")],
                [InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]
            ]
            current_text = query.message.text
            current_reply_markup = query.message.reply_markup
            new_reply_markup = InlineKeyboardMarkup(keyboard)
            if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                await query.message.edit_text(
                    text,
                    reply_markup=new_reply_markup,
                    parse_mode="HTML"
                )
            await query.answer()
            context.user_data["state"] = 8
            await log_analytics(user_id, "view_admin_panel", {})
            return 8
    except Exception as e:
        logger.error(f"Ошибка в show_admin_panel: {e}", exc_info=True)
        await query.message.edit_text(
            "Ошибка при открытии админ-панели. Попробуйте снова.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]),
            parse_mode="HTML"
        )
        await query.answer()
        context.user_data["state"] = 0
        await log_analytics(user_id, "admin_panel_error", {"error": str(e)})
        return 0

async def calculate_price_ton(context: ContextTypes.DEFAULT_TYPE, stars: int) -> float:
    try:
        async with (await ensure_db_pool()) as conn:
            price_usd = await conn.fetchval("SELECT value FROM settings WHERE key = 'price_usd'") or 0.81
            markup = await conn.fetchval("SELECT value FROM settings WHERE key = 'markup'") or 10.0
            price_usd = (stars / 50) * price_usd * (1 + markup / 100)
            if "ton_price_info" not in context.bot_data or context.bot_data["ton_price_info"].get("price", 0.0) == 0.0:
                await update_ton_price(context)
            ton_price = context.bot_data["ton_price_info"].get("price", 1.0)
            if ton_price == 0.0:
                logger.error("TON price is zero, cannot calculate price")
                return 0.0
            price_ton = price_usd / ton_price
            return round(price_ton, 2)
    except Exception as e:
        logger.error(f"Ошибка в calculate_price_ton: {e}", exc_info=True)
        return 0.0

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    username = update.effective_user.username or str(user_id)
    text = update.message.text
    state = context.user_data.get("state", 0)
    logger.info(f"Message received: user_id={user_id}, state={state}, text={text}")

    try:
        async with (await ensure_db_pool()) as conn:
            is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id) or False
            if context.bot_data["tech_break_info"].get("end_time", datetime.min.replace(tzinfo=pytz.UTC)) > datetime.now(pytz.UTC) and not is_admin:
                time_remaining = await format_time_remaining(context.bot_data["tech_break_info"]["end_time"])
                text = await get_text(
                    "tech_break_active",
                    end_time=context.bot_data["tech_break_info"]["end_time"].strftime("%Y-%m-%d %H:%M:%S UTC"),
                    minutes_left=time_remaining,
                    reason=context.bot_data["tech_break_info"].get("reason", "Не указана")
                )
                await update.message.reply_text(
                    text,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]),
                    parse_mode="HTML"
                )
                context.user_data["state"] = 0
                await log_analytics(user_id, "message_tech_break", {})
                return 0

            if state == STATES["buy_stars_recipient"]:
                if not text.startswith("@") or len(text) < 2:
                    await update.message.reply_text(
                        "Пожалуйста, введите корректное имя пользователя, начиная с @.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="buy_stars")]])
                    )
                    await log_analytics(user_id, "invalid_recipient", {"input": text})
                    return 3
                context.user_data["recipient"] = text
                price_ton = await calculate_price_ton(context, int(context.user_data.get("stars_amount", 0))) if context.user_data.get("stars_amount") else None
                price_text = f"~{price_ton:.2f} TON" if price_ton is not None else "Цена"
                reply_text = (
                    f"Пользователь: {text}\n"
                    f"Количество звезд: {context.user_data.get('stars_amount', 'Не выбрано')}"
                )
                keyboard = [
                    [InlineKeyboardButton(f"Пользователь: {text}", callback_data="select_recipient")],
                    [InlineKeyboardButton(f"Количество: {context.user_data.get('stars_amount', 'Не выбрано')}", callback_data="select_stars_menu")],
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
                context.user_data["state"] = 5
                await log_analytics(user_id, "set_recipient", {"recipient": text})
                return 5

            elif state == STATES["buy_stars_custom"]:
                if not text.isdigit() or int(text) <= 0:
                    await update.message.reply_text(
                        "Пожалуйста, введите положительное число звезд.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="select_stars_menu")]])
                    )
                    await log_analytics(user_id, "invalid_stars_amount", {"input": text})
                    return 23
                context.user_data["stars_amount"] = text
                recipient = context.user_data.get("recipient", "Не выбран")
                price_ton = await calculate_price_ton(context, int(text))
                reply_text = (
                    f"Пользователь: {recipient}\n"
                    f"Количество звезд: {text}"
                )
                keyboard = [
                    [InlineKeyboardButton(f"Пользователь: {recipient}", callback_data="select_recipient")],
                    [InlineKeyboardButton(f"Количество: {text}", callback_data="select_stars_menu")],
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
                context.user_data["state"] = 5
                await log_analytics(user_id, "set_custom_stars", {"stars": text})
                return 5

            elif state == STATES["admin_broadcast"] and is_admin:
                context.user_data["broadcast_text"] = text
                reply_text = f"Подтвердите рассылку:\n\n{text}"
                keyboard = [
                    [InlineKeyboardButton("✅ Подтвердить", callback_data="confirm_broadcast")],
                    [InlineKeyboardButton("❌ Отменить", callback_data="cancel_broadcast")]
                ]
                await update.message.reply_text(
                    reply_text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
                context.user_data["state"] = 10
                await log_analytics(user_id, "set_broadcast_text", {"text_length": len(text)})
                return 10

            elif state == STATES["admin_edit_profile"] and is_admin:
                if not text.isdigit():
                    await update.message.reply_text(
                        "Пожалуйста, введите корректный ID пользователя.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="admin_edit_profile")]])
                    )
                    await log_analytics(user_id, "invalid_edit_user_id", {"input": text})
                    return 11
                edit_user_id = int(text)
                user = await conn.fetchrow("SELECT username, stars_bought, ref_bonus_ton, referrals FROM users WHERE user_id = $1", edit_user_id)
                if not user:
                    await update.message.reply_text(
                        "Пользователь не найден.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="admin_edit_profile")]])
                    )
                    await log_analytics(user_id, "edit_user_not_found", {"edit_user_id": edit_user_id})
                    return 11
                context.user_data["edit_user_id"] = edit_user_id
                username = f"@{user['username']}" if user['username'] else f"ID {edit_user_id}"
                reply_text = (
                    f"Редактирование пользователя {username}:\n"
                    f"Звезды: {user['stars_bought']}\n"
                    f"Реф. бонус: {user['ref_bonus_ton']} TON\n"
                    f"Рефералы: {len(json.loads(user['referrals']) if user['referrals'] else [])}"
                )
                keyboard = [
                    [InlineKeyboardButton("Изменить звезды", callback_data="edit_profile_stars")],
                    [InlineKeyboardButton("Изменить рефералов", callback_data="edit_profile_referrals")],
                    [InlineKeyboardButton("Изменить реф. бонус", callback_data="edit_profile_ref_bonus")],
                    [InlineKeyboardButton("🔙 Назад", callback_data="admin_edit_profile")]
                ]
                await update.message.reply_text(
                    reply_text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
                context.user_data["state"] = 11
                await log_analytics(user_id, "view_edit_profile", {"edit_user_id": edit_user_id})
                return 11

            elif state == STATES["admin_edit_profile"] and is_admin and "edit_profile_field" in context.user_data:
                edit_user_id = context.user_data.get("edit_user_id")
                field = context.user_data["edit_profile_field"]
                if field == "stars_bought":
                    if not text.isdigit() or int(text) < 0:
                        await update.message.reply_text(
                            "Пожалуйста, введите корректное количество звезд.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="admin_edit_profile")]])
                        )
                        await log_analytics(user_id, "invalid_stars_input", {"input": text})
                        return 11
                    await conn.execute(
                        "UPDATE users SET stars_bought = $1 WHERE user_id = $2",
                        int(text), edit_user_id
                    )
                    reply_text = f"Звезды для пользователя ID {edit_user_id} обновлены: {text}"
                elif field == "referrals":
                    try:
                        referral_ids = [int(r) for r in text.split(",") if r.strip().isdigit()]
                        await conn.execute(
                            "UPDATE users SET referrals = $1 WHERE user_id = $2",
                            json.dumps(referral_ids), edit_user_id
                        )
                        reply_text = f"Рефералы для пользователя ID {edit_user_id} обновлены."
                    except ValueError:
                        await update.message.reply_text(
                            "Пожалуйста, введите ID рефералов через запятую.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="admin_edit_profile")]])
                        )
                        await log_analytics(user_id, "invalid_referrals_input", {"input": text})
                        return 11
                elif field == "ref_bonus_ton":
                    try:
                        bonus = float(text)
                        if bonus < 0:
                            raise ValueError("Negative bonus")
                        await conn.execute(
                            "UPDATE users SET ref_bonus_ton = $1 WHERE user_id = $2",
                            bonus, edit_user_id
                        )
                        reply_text = f"Реферальный бонус для пользователя ID {edit_user_id} обновлен: {bonus} TON"
                    except ValueError:
                        await update.message.reply_text(
                            "Пожалуйста, введите корректное число для реф. бонуса.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="admin_edit_profile")]])
                        )
                        await log_analytics(user_id, "invalid_ref_bonus_input", {"input": text})
                        return 11
                await conn.execute(
                    "UPDATE users SET prefix = CASE "
                    "WHEN is_admin THEN 'Verified' "
                    "WHEN stars_bought >= 50000 THEN 'Verified' "
                    "WHEN stars_bought >= 10000 THEN 'Regular Buyer' "
                    "WHEN stars_bought >= 5000 THEN 'Buyer' "
                    "WHEN stars_bought >= 1000 THEN 'Newbie' "
                    "ELSE 'Beginner' END WHERE user_id = $1",
                    edit_user_id
                )
                await update.message.reply_text(
                    reply_text,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="admin_edit_profile")]])
                )
                context.user_data.pop("edit_profile_field", None)
                context.user_data["state"] = 11
                await log_analytics(user_id, f"edit_{field}", {"edit_user_id": edit_user_id, "value": text})
                return 11

            elif state == STATES["set_db_reminder"] and is_admin:
                try:
                    reminder_date = datetime.strptime(text, "%Y-%m-%d").date()
                    existing_reminder = await conn.fetchval(
                        "SELECT id FROM reminders WHERE reminder_type = 'db' AND reminder_date >= $1",
                        datetime.now(pytz.UTC).date()
                    )
                    if existing_reminder:
                        reply_text = await get_text(
                            "db_reminder_exists",
                            reminder_date=reminder_date.strftime("%Y-%m-%d")
                        )
                        await update.message.reply_text(
                            reply_text,
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                        )
                        await log_analytics(user_id, "db_reminder_exists", {"reminder_date": text})
                        return 14
                    await conn.execute(
                        "INSERT INTO reminders (user_id, reminder_date, reminder_type, created_at) "
                        "VALUES ($1, $2, $3, $4)",
                        user_id, reminder_date, "db", datetime.now(pytz.UTC)
                    )
                    reply_text = await get_text("reminder_set", reminder_date=reminder_date.strftime("%Y-%m-%d"))
                    await update.message.reply_text(
                        reply_text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    context.user_data["state"] = 8
                    await log_analytics(user_id, "set_db_reminder", {"reminder_date": text})
                    return 8
                except ValueError:
                    await update.message.reply_text(
                        "Неверный формат даты. Используйте гггг-мм-дд.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    await log_analytics(user_id, "invalid_reminder_date", {"input": text})
                    return 14

            elif state == STATES["tech_break"] and is_admin:
                try:
                    minutes, reason = text.split(" ", 1)
                    minutes = int(minutes)
                    if minutes <= 0:
                        raise ValueError("Minutes must be positive")
                    context.bot_data["tech_break_info"] = {
                        "end_time": datetime.now(pytz.UTC) + timedelta(minutes=minutes),
                        "reason": reason
                    }
                    reply_text = await get_text(
                        "tech_break_active",
                        end_time=context.bot_data["tech_break_info"]["end_time"].strftime("%Y-%m-%d %H:%M:%S UTC"),
                        minutes_left=minutes,
                        reason=reason
                    )
                    await update.message.reply_text(
                        reply_text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    context.user_data["state"] = 8
                    await log_analytics(user_id, "set_tech_break", {"minutes": minutes, "reason": reason})
                    return 8
                except ValueError:
                    await update.message.reply_text(
                        "Неверный формат. Введите: <минуты> <причина>",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    await log_analytics(user_id, "invalid_tech_break_input", {"input": text})
                    return 16

            elif state == STATES["bot_settings"] and is_admin and "edit_setting" in context.user_data:
                setting = context.user_data["edit_setting"]
                try:
                    value = float(text)
                    if value < 0:
                        raise ValueError("Value must be non-negative")
                    await conn.execute(
                        "INSERT INTO settings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                        setting, value
                    )
                    await load_settings()
                    reply_text = f"Настройка '{setting}' обновлена: {value}"
                    await update.message.reply_text(
                        reply_text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="bot_settings")]])
                    )
                    context.user_data.pop("edit_setting", None)
                    context.user_data["state"] = 17
                    await log_analytics(user_id, f"edit_{setting}", {"value": value})
                    return 17
                except ValueError:
                    await update.message.reply_text(
                        "Пожалуйста, введите корректное число.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="bot_settings")]])
                    )
                    await log_analytics(user_id, f"invalid_{setting}_input", {"input": text})
                    return 17

            else:
                await update.message.reply_text(
                    "Неизвестная команда или состояние. Вернитесь в главное меню.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
                )
                context.user_data["state"] = 0
                await log_analytics(user_id, "unknown_message_state", {"state": state, "text": text})
                return 0

    except Exception as e:
        logger.error(f"Ошибка в message_handler: {e}", exc_info=True)
        await update.message.reply_text(
            "Произошла ошибка. Попробуйте снова или обратитесь в поддержку.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
        )
        context.user_data["state"] = 0
        await log_analytics(user_id, "message_error", {"error": str(e)})
        return 0

import uuid
import json
import pytz
from datetime import datetime
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError
from telegram import Update
from telegram.ext import ContextTypes
import asyncpg
import asyncio
import logging
import os

logger = logging.getLogger(__main__)

async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data
    logger.info(f"Callback query received: user_id={user_id}, data={data}")

    # Answer query immediately to avoid timeout
    try:
        await query.answer()
    except TelegramError as e:
        logger.warning(f"Failed to answer callback query: {e}")

    # Initialize tech_break_info
    if "tech_break_info" not in context.bot_data:
        context.bot_data["tech_break_info"] = {"end_time": datetime.min.replace(tzinfo=pytz.UTC), "reason": ""}

    max_retries = 3
    retry_delay = 1
    try:
        async with (await ensure_db_pool()) as conn:
            is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id) or False

            # Tech break check
            if context.bot_data["tech_break_info"].get("end_time", datetime.min.replace(tzinfo=pytz.UTC)) > datetime.now(pytz.UTC) and not is_admin:
                time_remaining = await format_time_remaining(context.bot_data["tech_break_info"]["end_time"])
                text = await get_text(
                    "tech_break_active",
                    end_time=context.bot_data["tech_break_info"]["end_time"].strftime("%Y-%m-%d %H:%M:%S UTC"),
                    minutes_left=time_remaining,
                    reason=context.bot_data["tech_break_info"].get("reason", "Не указана")
                )
                new_keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]
                new_reply_markup = InlineKeyboardMarkup(new_keyboard)
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                context.user_data["state"] = 0
                await log_analytics(user_id, "callback_tech_break", {})
                return 0

            # Profile
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
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup(keyboard)
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                context.user_data["state"] = 1
                await log_analytics(user_id, "view_profile", {})
                return 1

            # Profile Transactions
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
                            f"за {t['price_ton']:.4f} TON в {eest_time}\n\n"
                        )
                keyboard = []
                if total_transactions > (page + 1) * transactions_per_page:
                    keyboard.append([InlineKeyboardButton("➡️ Далее", callback_data=f"profile_transactions_{page + 1}")])
                if page > 0:
                    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"profile_transactions_{page - 1}")])
                keyboard.append([InlineKeyboardButton("🔙 В профиль", callback_data="profile")])
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup(keyboard)
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                context.user_data["state"] = 26
                await log_analytics(user_id, "view_profile_transactions", {"page": page})
                return 26

            # Referrals
            elif data == "referrals":
                user = await conn.fetchrow(
                    "SELECT referrals, ref_bonus_ton FROM users WHERE user_id = $1", user_id
                )
                referrals = json.loads(user["referrals"]) if user and user["referrals"] else []
                ref_bonus_ton = user["ref_bonus_ton"] if user else 0.0
                ref_link = f"https://t.me/{context.bot.username}?start={user_id}"
                text = await get_text(
                    "referrals",
                    ref_link=ref_link,
                    ref_count=len(referrals),
                    ref_bonus_ton=ref_bonus_ton
                )
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                context.user_data["state"] = 2
                await log_analytics(user_id, "view_referrals", {})
                return 2

            # Referral Leaderboard
            elif data == "referral_leaderboard":
                users = await conn.fetch(
                    "SELECT user_id, username, jsonb_array_length(referrals) as ref_count "
                    "FROM users WHERE jsonb_array_length(referrals) > 0 "
                    "ORDER BY ref_count DESC LIMIT 10"
                )
                text_lines = []
                for user in users:
                    try:
                        tg_user = await context.bot.get_chat(user['user_id'])
                        username = f"@{tg_user.username}" if tg_user.username else f"ID <code>{user['user_id']}</code>"
                    except TelegramError:
                        username = f"@{user['username']}" if user['username'] else f"ID <code>{user['user_id']}</code>"
                    text_lines.append(f"{username}, Рефералов: {user['ref_count']}")
                text = await get_text(
                    "referral_leaderboard",
                    users_list="\n".join(text_lines) if text_lines else "Рефералов пока нет."
                )
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                context.user_data["state"] = 12
                await log_analytics(user_id, "view_referral_leaderboard", {})
                return 12

            # Top Purchases
            elif data == "top_purchases":
                users = await conn.fetch(
                    "SELECT user_id, username, stars_bought FROM users "
                    "WHERE stars_bought > 0 ORDER BY stars_bought DESC LIMIT 10"
                )
                text_lines = []
                for user in users:
                    try:
                        tg_user = await context.bot.get_chat(user['user_id'])
                        username = f"@{tg_user.username}" if tg_user.username else f"ID <code>{user['user_id']}</code>"
                    except TelegramError:
                        username = f"@{user['username']}" if user['username'] else f"ID <code>{user['user_id']}</code>"
                    text_lines.append(f"{username}, Звезды: {user['stars_bought']}")
                text = await get_text(
                    "top_purchases",
                    users_list="\n".join(text_lines) if text_lines else "Покупок пока нет."
                )
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                context.user_data["state"] = 13
                await log_analytics(user_id, "view_top_purchases", {})
                return 13

            # Buy Stars
            elif data == "buy_stars":
                recipient = context.user_data.get("recipient", "Не выбран")
                stars = context.user_data.get("stars_amount", "Не выбрано")
                price_ton = await calculate_price_ton(context, int(stars)) if stars and isinstance(stars, str) and stars.isdigit() else None
                price_text = f"~{price_ton:.4f} TON" if price_ton is not None else "Цена"
                text = (
                    f"Пользователь: {recipient}\n"
                    f"Количество звезд: {stars}"
                )
                keyboard = [
                    [InlineKeyboardButton(f"Пользователь: {recipient}", callback_data="select_recipient")],
                    [InlineKeyboardButton(f"Количество: {stars}", callback_data="select_stars_menu")],
                    [
                        InlineKeyboardButton(price_text, callback_data="show_price"),
                        InlineKeyboardButton("Далее", callback_data="pay_stars_menu")
                    ],
                    [InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]
                ]
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup(keyboard)
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                context.user_data["state"] = 5
                await log_analytics(user_id, "open_buy_stars_payment_method", {})
                return 5

            # Show Price
            elif data == "show_price":
                stars = context.user_data.get("stars_amount", "Не выбрано")
                price_ton = await calculate_price_ton(context, int(stars)) if stars and isinstance(stars, str) and stars.isdigit() else None
                price_text = f"~{price_ton:.4f} TON" if price_ton is not None else "Цена не определена"
                await query.answer(text=price_text, show_alert=True)
                return context.user_data["state"]

            # Select Recipient
            elif data == "select_recipient":
                text = "Введите имя пользователя (например, @username):"
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="buy_stars")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                context.user_data["state"] = 3
                await log_analytics(user_id, "start_select_recipient", {})
                return 3

            # Select Stars Menu
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
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup(keyboard)
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                context.user_data["state"] = 22
                await log_analytics(user_id, "open_select_stars_menu", {})
                return 22

            # Select Stars
            elif data in ["select_stars_100", "select_stars_250", "select_stars_500", "select_stars_1000"]:
                stars = data.split("_")[-1]
                context.user_data["stars_amount"] = stars
                recipient = context.user_data.get("recipient", "Не выбран")
                price_ton = await calculate_price_ton(context, int(stars))
                text = (
                    f"Пользователь: {recipient}\n"
                    f"Количество звезд: {stars}"
                )
                keyboard = [
                    [InlineKeyboardButton(f"Пользователь: {recipient}", callback_data="select_recipient")],
                    [InlineKeyboardButton(f"Количество: {stars}", callback_data="select_stars_menu")],
                    [
                        InlineKeyboardButton(f"~{price_ton:.4f} TON", callback_data="show_price"),
                        InlineKeyboardButton("Далее", callback_data="pay_stars_menu")
                    ],
                    [InlineKeyboardButton("🔙 Назад", callback_data="buy_stars")]
                ]
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup(keyboard)
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                context.user_data["state"] = 5
                await log_analytics(user_id, f"select_stars_{stars}", {"stars": stars})
                return 5

            # Select Custom Stars
            elif data == "select_stars_custom":
                text = "Введите количество звезд (положительное число):"
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="select_stars_menu")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                context.user_data["state"] = 23
                await log_analytics(user_id, "start_select_stars_custom", {})
                return 23

            # Pay Stars Menu
            elif data == "pay_stars_menu":
                stars = context.user_data.get("stars_amount")
                recipient = context.user_data.get("recipient")
                if not stars or not recipient or not isinstance(stars, str) or not stars.isdigit():
                    text = "Ошибка: выберите пользователя и количество звезд."
                    keyboard = [
                        [InlineKeyboardButton(f"Пользователь: {context.user_data.get('recipient', 'Не выбран')}", callback_data="select_recipient")],
                        [InlineKeyboardButton(f"Количество: {context.user_data.get('stars_amount', 'Не выбрано')}", callback_data="select_stars_menu")],
                        [InlineKeyboardButton("Цена", callback_data="show_price"), InlineKeyboardButton("Далее", callback_data="pay_stars_menu")],
                        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]
                    ]
                    current_text = query.message.text
                    current_reply_markup = query.message.reply_markup
                    new_reply_markup = InlineKeyboardMarkup(keyboard)
                    if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                        await query.message.edit_text(
                            text,
                            reply_markup=new_reply_markup,
                            parse_mode="HTML"
                        )
                    context.user_data["state"] = 5
                    await log_analytics(user_id, "pay_stars_menu_error", {"stars": stars, "recipient": recipient})
                    return 5

                stars = int(stars)
                price_ton = await calculate_price_ton(context, stars)
                if price_ton == 0.0:
                    text = "Ошибка: не удалось рассчитать цену. Попробуйте позже."
                    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="buy_stars")]]
                    await query.message.edit_text(
                        text,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode="HTML"
                    )
                    context.user_data["state"] = 5
                    await log_analytics(user_id, "pay_stars_menu_price_error", {"stars": stars})
                    return 5

                invoice_id = str(uuid.uuid4())[:8]
                owner_wallet = os.getenv("OWNER_WALLET", "YOUR_DEFAULT_WALLET_ADDRESS")
                amount_nano = int(price_ton * 1_000_000_000)
                payment_link = f"ton://transfer/{owner_wallet}?amount={amount_nano}&text={invoice_id}"

                await conn.execute(
                    "INSERT INTO transactions (user_id, recipient_username, stars_amount, price_ton, invoice_id, purchase_time, checked_status) "
                    "VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    user_id, recipient, stars, price_ton, invoice_id, datetime.now(pytz.UTC), "pending"
                )

                text = (
                    f"Оплата за {stars} звезд:\n"
                    f"Тип кошелька: TON\n"
                    f"Цена: {price_ton:.4f} TON\n"
                    f"Комментарий: {invoice_id}\n"
                    f"Ссылка для оплаты: <a href='{payment_link}'>Оплатить через Tonkeeper</a>"
                )
                keyboard = [
                    [InlineKeyboardButton("✅ Проверить оплату", callback_data=f"check_payment_{invoice_id}")],
                    [InlineKeyboardButton("🔙 Назад", callback_data="buy_stars")]
                ]
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup(keyboard)
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML",
                        disable_web_page_preview=True
                    )
                context.user_data["state"] = 25
                context.user_data["invoice_id"] = invoice_id
                context.user_data["price_ton"] = price_ton
                await log_analytics(user_id, "pay_stars_menu", {"stars": stars, "recipient": recipient, "invoice_id": invoice_id})
                return 25

            # Check Payment
            elif data.startswith("check_payment_"):
                invoice_id = data.split("_")[-1]
                stars = context.user_data.get("stars_amount")
                recipient = context.user_data.get("recipient")
                price_ton = context.user_data.get("price_ton")
                if not stars or not recipient or not price_ton or not invoice_id:
                    text = "Ошибка: данные о покупке отсутствуют. Начните заново."
                    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="buy_stars")]]
                    await query.message.edit_text(
                        text,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode="HTML"
                    )
                    context.user_data["state"] = 5
                    await log_analytics(user_id, "check_payment_error", {"invoice_id": invoice_id})
                    return 5

                transaction = await conn.fetchrow(
                    "SELECT checked_status FROM transactions WHERE invoice_id = $1", invoice_id
                )
                if transaction and transaction["checked_status"] == "completed":
                    text = f"Платеж подтвержден!\n{stars} звезд добавлены для {recipient}."
                    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]
                    await query.message.edit_text(
                        text,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode="HTML"
                    )
                    context.user_data["state"] = 0
                    context.user_data.pop("stars_amount", None)
                    context.user_data.pop("recipient", None)
                    context.user_data.pop("price_ton", None)
                    context.user_data.pop("invoice_id", None)
                    await log_analytics(user_id, "payment_confirmed", {"stars": stars, "recipient": recipient, "invoice_id": invoice_id})
                    return 0

                # Placeholder for TON blockchain verification
                owner_wallet = os.getenv("OWNER_WALLET", "YOUR_DEFAULT_WALLET_ADDRESS")
                payment_verified = False
                # Example TON API call (uncomment and configure with TON API)
                # async with aiohttp.ClientSession() as session:
                #     async with session.get(
                #         f"https://tonapi.io/v2/transactions?to={owner_wallet}&comment={invoice_id}",
                #         headers={"Authorization": f"Bearer {os.getenv('TON_API_KEY')}"}
                #     ) as response:
                #         if response.status == 200:
                #             data = await response.json()
                #             payment_verified = any(tx["amount"] == int(price_ton * 1_000_000_000) for tx in data["transactions"])

                if payment_verified:
                    await conn.execute(
                        "UPDATE transactions SET checked_status = 'completed', purchase_time = $1 WHERE invoice_id = $2",
                        datetime.now(pytz.UTC), invoice_id
                    )
                    await conn.execute(
                        "UPDATE users SET stars_bought = stars_bought + $1 WHERE user_id = $2",
                        int(stars), user_id
                    )
                    ref_bonus_percentage = await conn.fetchval("SELECT value FROM settings WHERE key = 'ref_bonus'") or 30.0
                    referrer_id = await conn.fetchval("SELECT referrer_id FROM users WHERE user_id = $1", user_id)
                    if referrer_id:
                        ref_bonus_ton = price_ton * (ref_bonus_percentage / 100)
                        await conn.execute(
                            "UPDATE users SET ref_bonus_ton = ref_bonus_ton + $1 WHERE user_id = $2",
                            ref_bonus_ton, referrer_id
                        )
                        await log_analytics(user_id, "referral_bonus_added", {"referrer_id": referrer_id, "bonus_ton": ref_bonus_ton})
                    text = f"Платеж подтвержден!\n{stars} звезд добавлены для {recipient}."
                    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]
                    await query.message.edit_text(
                        text,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode="HTML"
                    )
                    context.user_data["state"] = 0
                    context.user_data.pop("stars_amount", None)
                    context.user_data.pop("recipient", None)
                    context.user_data.pop("price_ton", None)
                    context.user_data.pop("invoice_id", None)
                    await log_analytics(user_id, "payment_confirmed", {"stars": stars, "recipient": recipient, "invoice_id": invoice_id})
                    return 0
                else:
                    text = "Оплата еще не подтверждена. Попробуйте снова через несколько минут."
                    keyboard = [
                        [InlineKeyboardButton("✅ Проверить снова", callback_data=f"check_payment_{invoice_id}")],
                        [InlineKeyboardButton("🔙 Назад", callback_data="buy_stars")]
                    ]
                    await query.message.edit_text(
                        text,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode="HTML"
                    )
                    context.user_data["state"] = 25
                    await log_analytics(user_id, "payment_check_failed", {"invoice_id": invoice_id})
                    return 25

            # Admin Panel
            elif data == "admin_panel" and is_admin:
                return await show_admin_panel(update, context)

            # Admin Stats
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
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                context.user_data["state"] = 9
                await log_analytics(user_id, "view_admin_stats", {})
                return 9

            # Broadcast Message
            elif data == "broadcast_message" and is_admin:
                text = "Введите текст для рассылки:"
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                context.user_data["state"] = 10
                await log_analytics(user_id, "start_broadcast", {})
                return 10

            # Confirm Broadcast
            elif data == "confirm_broadcast" and is_admin:
                broadcast_text = context.user_data.get("broadcast_text", "")
                if not broadcast_text:
                    text = "Текст рассылки пуст. Введите текст заново."
                    current_text = query.message.text
                    current_reply_markup = query.message.reply_markup
                    new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                        await query.message.edit_text(
                            text,
                            reply_markup=new_reply_markup,
                            parse_mode="HTML"
                        )
                    context.user_data["state"] = 10
                    return 10
                users = await conn.fetch("SELECT user_id FROM users")
                success_count = 0
                for user in users:
                    try:
                        await context.bot.send_message(
                            chat_id=user["user_id"],
                            text=broadcast_text,
                            parse_mode="HTML"
                        )
                        success_count += 1
                    except TelegramError as e:
                        logger.error(f"Failed to send broadcast to {user['user_id']}: {e}")
                text = f"Рассылка завершена. Отправлено {success_count} из {len(users)} пользователям."
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                context.user_data.pop("broadcast_text", None)
                context.user_data["state"] = 8
                await log_analytics(user_id, "complete_broadcast", {"success_count": success_count, "total_users": len(users)})
                return 8

            # Cancel Broadcast
            elif data == "cancel_broadcast" and is_admin:
                text = "Рассылка отменена."
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                context.user_data.pop("broadcast_text", None)
                context.user_data["state"] = 8
                await log_analytics(user_id, "cancel_broadcast", {})
                return await show_admin_panel(update, context)

            # Admin Edit Profile
            elif data == "admin_edit_profile" and is_admin:
                text = "Введите ID пользователя для редактирования:"
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📋 Все пользователи", callback_data="all_users")],
                    [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]
                ])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                context.user_data["state"] = 11
                await log_analytics(user_id, "start_edit_profile", {})
                return 11

            # All Users
            elif data == "all_users" and is_admin:
                users = await conn.fetch(
                    "SELECT user_id, username, stars_bought FROM users ORDER BY stars_bought DESC LIMIT 10"
                )
                text_lines = []
                for user in users:
                    try:
                        tg_user = await context.bot.get_chat(user['user_id'])
                        username = f"@{tg_user.username}" if tg_user.username else f"ID <code>{user['user_id']}</code>"
                        if tg_user.username and tg_user.username != user['username']:
                            await conn.execute(
                                "UPDATE users SET username = $1 WHERE user_id = $2",
                                tg_user.username, user['user_id']
                            )
                            logger.info(f"Updated username for user_id={user['user_id']} to {tg_user.username}")
                    except TelegramError:
                        username = f"@{user['username']}" if user['username'] else f"ID <code>{user['user_id']}</code>"
                    text_lines.append(f"{username}, ID <code>{user['user_id']}</code> Звезды: {user['stars_bought']}")
                text = await get_text(
                    "all_users",
                    users_list="\n".join(text_lines) if text_lines else "Пользователи не найдены."
                )
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад в админ-панель", callback_data="back_to_admin")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                context.user_data["state"] = 15
                await log_analytics(user_id, "view_all_users", {"users_count": len(users)})
                return 15

            # Edit Profile Stars
            elif data == "edit_profile_stars" and is_admin:
                context.user_data["edit_profile_field"] = "stars_bought"
                text = "Введите новое количество звезд:"
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                context.user_data["state"] = 11
                await log_analytics(user_id, "start_edit_stars", {})
                return 11

            # Edit Profile Referrals
            elif data == "edit_profile_referrals" and is_admin:
                context.user_data["edit_profile_field"] = "referrals"
                text = "Введите ID рефералов через запятую:"
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                context.user_data["state"] = 11
                await log_analytics(user_id, "start_edit_referrals", {})
                return 11

            # Edit Profile Referral Bonus
            elif data == "edit_profile_ref_bonus" and is_admin:
                context.user_data["edit_profile_field"] = "ref_bonus_ton"
                text = "Введите новый реферальный бонус (TON):"
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                context.user_data["state"] = 11
                await log_analytics(user_id, "start_edit_ref_bonus", {})
                return 11

            # Set DB Reminder
            elif data == "set_db_reminder" and is_admin:
                text = "Введите дату напоминания в формате гггг-мм-дд:"
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                context.user_data["state"] = 14
                await log_analytics(user_id, "start_set_db_reminder", {})
                return 14

            # Tech Break
            elif data == "tech_break" and is_admin:
                text = "Введите длительность тех. перерыва (в минутах) и причину (формат: <минуты> <причина>):"
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                context.user_data["state"] = 16
                await log_analytics(user_id, "start_tech_break", {})
                return 16

            # Bot Settings
            elif data == "bot_settings" and is_admin:
                price_usd = await conn.fetchval("SELECT value FROM settings WHERE key = 'price_usd'") or 0.81
                markup = await conn.fetchval("SELECT value FROM settings WHERE key = 'markup'") or 10.0
                ref_bonus = await conn.fetchval("SELECT value FROM settings WHERE key = 'ref_bonus'") or 30.0
                text = await get_text(
                    "bot_settings",
                    price_usd=price_usd,
                    markup=markup,
                    ref_bonus=ref_bonus
                )
                keyboard = [
                    [InlineKeyboardButton("Изменить цену за 50 звезд", callback_data="edit_price_usd")],
                    [InlineKeyboardButton("Изменить накрутку", callback_data="edit_markup")],
                    [InlineKeyboardButton("Изменить реф. бонус", callback_data="edit_ref_bonus")],
                    [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]
                ]
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup(keyboard)
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                context.user_data["state"] = 17
                await log_analytics(user_id, "view_bot_settings", {})
                return 17

            # Edit Price USD
            elif data == "edit_price_usd" and is_admin:
                text = "Введите новую цену за 50 звезд (USD):"
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="bot_settings")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                context.user_data["state"] = 17
                context.user_data["edit_setting"] = "price_usd"
                await log_analytics(user_id, "start_edit_price_usd", {})
                return 17

            # Edit Markup
            elif data == "edit_markup" and is_admin:
                text = "Введите новую накрутку (%):"
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="bot_settings")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                context.user_data["state"] = 17
                context.user_data["edit_setting"] = "markup"
                await log_analytics(user_id, "start_edit_markup", {})
                return 17

            # Edit Referral Bonus
            elif data == "edit_ref_bonus" and is_admin:
                text = "Введите новый реферальный бонус (%):"
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="bot_settings")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                context.user_data["state"] = 17
                context.user_data["edit_setting"] = "ref_bonus"
                await log_analytics(user_id, "start_edit_ref_bonus", {})
                return 17

            # Back to Admin
            elif data == "back_to_admin" and is_admin:
                return await show_admin_panel(update, context)

            # Back to Menu
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
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup(keyboard)
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                context.user_data["state"] = 0
                await log_analytics(user_id, "back_to_menu", {})
                return 0

            else:
                await query.answer("Неизвестная команда.")
                return context.user_data.get("state", 0)

    except asyncpg.exceptions.InterfaceError as e:
        logger.error(f"Database pool error: {e}", exc_info=True)
        text = "Ошибка: не удалось подключиться к базе данных. Попробуйте снова позже."
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]
        await query.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML"
        )
        context.user_data["state"] = 0
        await log_analytics(user_id, "database_error", {"error": str(e)})
        return 0
    except Exception as e:
        logger.error(f"Ошибка в callback_query_handler: {e}", exc_info=True)
        text = "Произошла ошибка. Попробуйте снова или обратитесь в поддержку."
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]
        await query.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML"
        )
        context.user_data["state"] = 0
        await log_analytics(user_id, "callback_error", {"error": str(e)})
        return 0


async def lifespan(app):
    global telegram_app
    if telegram_app is None:
        logger.error("telegram_app is None in lifespan")
        raise RuntimeError("telegram_app not initialized")
    logger.info("Initializing telegram_app...")
    await telegram_app.initialize()
    yield
    logger.info("Shutting down telegram_app...")
    await telegram_app.shutdown()

async def webhook(request: web.Request) -> web.Response:
    try:
        if telegram_app is None:
            logger.error("telegram_app is None")
            return web.json_response({"status": "error", "message": "Application not initialized"}, status=500)
        data = await request.json()
        logger.debug(f"Webhook received data: {data}")
        update = Update.de_json(data, telegram_app.bot)
        if update:
            logger.debug(f"Processing update: update_id={update.update_id}, message={update.message}, command={update.message.text if update.message else None}")
            await telegram_app.process_update(update)
            logger.info("Webhook processed successfully")
            return web.json_response({"status": "ok"})
        else:
            logger.warning("Invalid webhook update received")
            return web.json_response({"status": "invalid update"}, status=400)
    except Exception as e:
        logger.error(f"Error in webhook: {e}", exc_info=True)
        return web.json_response({"status": "error", "message": str(e)}, status=500)

async def health_check(request: web.Request) -> web.Response:
    logger.info("Health check called: %s %s", request.method, request.path)
    try:
        pool = await ensure_db_pool()
        async with pool.acquire() as conn:
            await conn.execute("SELECT 1")
        return web.json_response({"status": "healthy", "database": "connected"})
    except Exception as e:
        logger.error(f"Health check failed: {e}", exc_info=True)
        return web.json_response(
            {"status": "running", "database": "unavailable", "error": str(e)},
            status=200  # Return 200 to avoid UptimeRobot alerts
        )
async def setup_handlers(app: Application):
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("tonprice", ton_price_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
    app.add_handler(CallbackQueryHandler(callback_query_handler))
    app.add_handler(CommandHandler("test", test))
    logger.info("Handlers registered: start, tonprice, message, callback_query, test")

async def test(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    logger.debug(f"Test command received: user_id={user_id}")
    await update.message.reply_text("Bot is responding!", parse_mode="HTML")
    await log_analytics(user_id, "test_command", {})

async def main():
    global telegram_app
    logger.info("Creating telegram_app...")
    bot_token = os.getenv("BOT_TOKEN")
    if not bot_token:
        logger.error("BOT_TOKEN environment variable not set")
        raise RuntimeError("BOT_TOKEN not set")

    telegram_app = Application.builder().token(bot_token).build()

    # Initialize database pool
    try:
        await ensure_db_pool()
        logger.info("Database pool initialized in main")
    except Exception as e:
        logger.error(f"Failed to initialize database pool: {e}", exc_info=True)
        logger.warning("Starting bot without database connection, using fallbacks")

    # Register handlers
    await setup_handlers(telegram_app)
    logger.info("telegram_app created successfully")

    # Initialize tech_break_info
    telegram_app.bot_data["tech_break_info"] = {
        "end_time": datetime.min.replace(tzinfo=pytz.UTC),
        "reason": ""
    }
    logger.debug("Initialized tech_break_info in bot_data")

    # Initialize telegram_app
    logger.info("Initializing telegram_app...")
    try:
        await telegram_app.initialize()
        logger.info("telegram_app initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize telegram_app: {e}", exc_info=True)
        raise

    # Set up aiohttp server
    app = web.Application()
    app.router.add_post('/webhook', webhook)
    app.router.add_get('/', health_check)
    wsgi_handler = WSGIHandler(app_flask)
    app.router.add_route('*', '/{path_info:.*}', wsgi_handler.handle_request)

    try:
        runner = web.AppRunner(app)
        await runner.setup()
        port = int(os.getenv("PORT", 8080))
        site = web.TCPSite(runner, '0.0.0.0', port)
        await site.start()
        logger.info(f"aiohttp server started on port {port}")
        webhook_url = f"https://stars-ejwz.onrender.com/webhook"
        logger.info("Deleting existing webhook...")
        await telegram_app.bot.delete_webhook(drop_pending_updates=True)
        logger.info(f"Setting webhook to {webhook_url}...")
        await telegram_app.bot.set_webhook(webhook_url)
        logger.info("Webhook set successfully")

        # Schedule TON price updates
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        scheduler = AsyncIOScheduler(timezone=pytz.UTC)
        scheduler.add_job(update_ton_price, 'interval', minutes=5, args=[telegram_app])
        scheduler.start()
        logger.info("TON price update scheduler started")

        await asyncio.Event().wait()
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        raise
    finally:
        logger.info("Shutting down telegram_app...")
        try:
            await telegram_app.shutdown()
            logger.info("telegram_app shut down successfully")
        except Exception as e:
            logger.error(f"Failed to shut down telegram_app: {e}", exc_info=True)
        if _db_pool is not None:
            await _db_pool.close()
            logger.info("Database pool closed")
        await runner.cleanup()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
