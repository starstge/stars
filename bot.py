import os
import json
import logging
import asyncio
import aiohttp
import psycopg2
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
from datetime import datetime, timedelta
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
import telegram
from flask import Flask, render_template, request, redirect, url_for, flash, session
import bcrypt
from aiohttp_wsgi import WSGIHandler


app_flask = Flask(__name__)
app_flask.secret_key = os.getenv("FLASK_SECRET_KEY", "your-secret-key")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")

# Настройка логирования с ротацией
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.DEBUG,
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler("bot.log", maxBytes=10_000_000, backupCount=5, encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)

# Метрики Prometheus
REQUESTS = Counter("bot_requests_total", "Total number of requests", ["endpoint"])
ERRORS = Counter("bot_errors_total", "Total number of errors", ["type", "endpoint"])
RESPONSE_TIME = Histogram("bot_response_time_seconds", "Response time of handlers", ["endpoint"])

# Загрузка .env
load_dotenv()

# Константы
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

# Новые константы для дополнительных функций
FEEDBACK = "feedback"
TRANSACTION_HISTORY = "transaction_history"
REFERRAL_LEADERBOARD = "referral_leaderboard"
BAN_USER = "ban_user"
UNBAN_USER = "unban_user"
SUPPORT_TICKET = "support_ticket"
STATE_FEEDBACK = "feedback"
STATE_SUPPORT_TICKET = "support_ticket"
STATE_BAN_USER = "ban_user"
STATE_UNBAN_USER = "unban_user"

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
    "STATE_FEEDBACK": 18,
    "STATE_SUPPORT_TICKET": 19,
    "STATE_BAN_USER": 20,
    "STATE_UNBAN_USER": 21,
    "select_stars_menu": 22,
    "buy_stars_custom": 23,
    "transaction_history": 24,
    "buy_stars_payment": 25,
    "profile_transactions": 26  # Added for user transaction history
}

# Глобальные переменные
db_pool = None
_db_pool_lock = asyncio.Lock()
telegram_app = None
transaction_cache = TTLCache(maxsize=1000, ttl=3600)
tech_break_info = {}  # Хранит информацию о техническом перерыве: {"end_time": datetime, "reason": str}

def login_required(f):
    @wraps(f)
    async def decorated(*args, **kwargs):
        if 'logged_in' not in session:
            logger.warning("Unauthorized access attempt to protected route")
            return redirect(url_for('login'))
        return await f(*args, **kwargs)
    return decorated


async def debug_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Debug handler to log all incoming updates."""
    logger.info(f"Received update: {update.to_dict()}")
    await log_analytics(
        update.effective_user.id if update.effective_user else 0,
        "debug_update",
        {"update": update.to_dict()}
    )

app_flask = Flask(__name__)
app_flask.secret_key = os.getenv("FLASK_SECRET_KEY", "your-secret-key")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")

# Настройка логирования
logger = logging.getLogger(__name__)

# Synchronous database connection
def get_db_connection():
    try:
        conn = psycopg2.connect(os.getenv("POSTGRES_URL"))
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise


@app_flask.route('/login', methods=['GET', 'POST'])
async def login():
    """Handle admin login."""
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
async def logout():
    """Handle admin logout."""
    session.pop('logged_in', None)
    logger.info("User logged out")
    flash("You have logged out.", "success")
    return redirect(url_for('login'))

@app_flask.route('/')
@login_required
async def index():
    """Redirect to transactions page."""
    return redirect(url_for('transactions'))

@app_flask.route('/transactions')
@login_required
async def transactions():
    """Handle transactions page."""
    await ensure_db_pool()  # Ensure pool is open
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
    param_index = 1

    if user_id:
        try:
            query += f" AND user_id = ${param_index}"
            count_query += f" AND user_id = ${param_index}"
            params.append(int(user_id))
            param_index += 1
        except ValueError:
            flash("User ID must be a number.", "error")
            logger.error(f"Invalid user_id format: {user_id}")

    if recipient:
        query += f" AND recipient_username ILIKE ${param_index}"
        count_query += f" AND recipient_username ILIKE ${param_index}"
        params.append(f'%{recipient}%')
        param_index += 1

    if start_date:
        try:
            query += f" AND purchase_time >= ${param_index}"
            count_query += f" AND purchase_time >= ${param_index}"
            params.append(datetime.strptime(start_date, "%Y-%m-%d"))
            param_index += 1
        except ValueError:
            flash("Invalid start date format (yyyy-mm-dd).", "error")
            logger.error(f"Invalid start_date format: {start_date}")

    if end_date:
        try:
            query += f" AND purchase_time <= ${param_index}"
            count_query += f" AND purchase_time <= ${param_index}"
            params.append(datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1))
            param_index += 1
        except ValueError:
            flash("Invalid end date format (yyyy-mm-dd).", "error")
            logger.error(f"Invalid end_date format: {end_date}")

    if min_stars:
        try:
            query += f" AND stars_amount >= ${param_index}"
            count_query += f" AND stars_amount >= ${param_index}"
            params.append(int(min_stars))
            param_index += 1
        except ValueError:
            flash("Minimum stars must be a number.", "error")
            logger.error(f"Invalid min_stars format: {min_stars}")

    if max_stars:
        try:
            query += f" AND stars_amount <= ${param_index}"
            count_query += f" AND stars_amount <= ${param_index}"
            params.append(int(max_stars))
            param_index += 1
        except ValueError:
            flash("Maximum stars must be a number.", "error")
            logger.error(f"Invalid max_stars format: {max_stars}")

    query += f" ORDER BY purchase_time DESC LIMIT ${param_index} OFFSET ${param_index + 1}"
    params.extend([per_page, (page - 1) * per_page])

    for attempt in range(3):  # Retry logic
        try:
            async with db_pool.acquire() as conn:
                transactions = await conn.fetch(query, *params)
                total = await conn.fetchval(count_query, *params[:-2])
                total_pages = (total + per_page - 1) // per_page

                eest = pytz.timezone("Europe/Tallinn")
                transactions = [
                    {
                        "id": t["id"],
                        "user_id": t["user_id"],
                        "recipient_username": t["recipient_username"],
                        "stars_amount": t["stars_amount"],
                        "price_ton": t["price_ton"],
                        "purchase_time": t["purchase_time"].astimezone(eest).strftime("%Y-%m-%d %H:%M:%S EEST"),
                        "checked_status": t["checked_status"]
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
        except (asyncpg.InterfaceError, RuntimeError) as e:
            logger.error(f"Error loading transactions (attempt {attempt+1}): {e}", exc_info=True)
            if attempt < 2:
                await asyncio.sleep(1)  # Wait before retry
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
async def users():
    """Handle users page."""
    await ensure_db_pool()  # Ensure pool is open
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
    param_index = 1

    if user_id:
        try:
            query += f" AND user_id = ${param_index}"
            count_query += f" AND user_id = ${param_index}"
            params.append(int(user_id))
            param_index += 1
        except ValueError:
            flash("User ID must be a number.", "error")
            logger.error(f"Invalid user_id format: {user_id}")

    if username:
        query += f" AND username ILIKE ${param_index}"
        count_query += f" AND username ILIKE ${param_index}"
        params.append(f'%{username}%')
        param_index += 1

    if is_admin:
        query += f" AND is_admin = ${param_index}"
        count_query += f" AND is_admin = ${param_index}"
        params.append(is_admin == 'true')
        param_index += 1

    query += f" ORDER BY created_at DESC LIMIT ${param_index} OFFSET ${param_index + 1}"
    params.extend([per_page, (page - 1) * per_page])

    for attempt in range(3):  # Retry logic
        try:
            async with db_pool.acquire() as conn:
                users = await conn.fetch(query, *params)
                total = await conn.fetchval(count_query, *params[:-2])
                total_pages = (total + per_page - 1) // per_page

                # Convert referrals JSON to length for display
                users = [
                    {
                        "user_id": u["user_id"],
                        "username": u["username"],
                        "stars_bought": u["stars_bought"],
                        "ref_bonus_ton": u["ref_bonus_ton"],
                        "referrals": json.loads(u["referrals"]) if u["referrals"] else [],
                        "created_at": u["created_at"].strftime("%Y-%m-%d %H:%M:%S"),
                        "is_new": u["is_new"],
                        "is_admin": u["is_admin"],
                        "prefix": u["prefix"]
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
        except (asyncpg.InterfaceError, RuntimeError) as e:
            logger.error(f"Error loading users (attempt {attempt+1}): {e}", exc_info=True)
            if attempt < 2:
                await asyncio.sleep(1)  # Wait before retry
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
async def update_status():
    """Handle status updates for users and transactions."""
    data = request.get_json()
    type_ = data.get('type')
    field = data.get('field')
    user_id = data.get('user_id')
    value = data.get('value')

    try:
        async with db_pool.acquire() as conn:
            if type_ == 'user' and field in ['is_admin', 'prefix']:
                if field == 'prefix' and value not in ['Новичок', 'Новенький', 'Покупатель', 'Постоянный Покупатель', 'Проверенный']:
                    logger.error(f"Invalid prefix value: {value}")
                    return jsonify({'message': 'Invalid prefix value'}), 400
                await conn.execute(f"UPDATE users SET {field} = $1 WHERE user_id = $2", value if field == 'prefix' else value == 'true', int(user_id))
                logger.info(f"Updated {field} for user_id={user_id} to {value}")
                return jsonify({'message': f'{field} updated for user {user_id}'})
            elif type_ == 'transaction' and field == 'checked_status':
                await conn.execute("UPDATE transactions SET checked_status = $1 WHERE id = $2", value, int(user_id))
                logger.info(f"Updated checked_status for transaction_id={user_id} to {value}")
                return jsonify({'message': f'Status updated for transaction {user_id}'})
            else:
                logger.error(f"Invalid type or field: type={type_}, field={field}")
                return jsonify({'message': 'Invalid type or field'}), 400
    except Exception as e:
        logger.error(f"Error updating status: {e}", exc_info=True)
        return jsonify({'message': f'Error updating status: {str(e)}'}), 500
        
async def ensure_db_pool():
    """Initialize or reinitialize the database connection pool."""
    global db_pool
    async with _db_pool_lock:
        if db_pool is None or db_pool._closed:
            try:
                loop = asyncio.get_event_loop()
                db_pool = await asyncpg.create_pool(
                    POSTGRES_URL,
                    min_size=10,  # Adjusted for concurrency
                    max_size=50,
                    max_inactive_connection_lifetime=300,
                    loop=loop
                )
                logger.info("Database pool initialized")
            except Exception as e:
                logger.error(f"Failed to initialize database pool: {e}", exc_info=True)
                raise
    return db_pool
        
import asyncpg
import logging
from datetime import datetime
import pytz
import json

# Configure logging
logger = logging.getLogger(__name__)

async def init_db():
    """Initialize the database schema and set up default values."""
    try:
        async with (await ensure_db_pool()) as conn:
            # Drop is_banned column if it exists
            await conn.execute("ALTER TABLE users DROP COLUMN IF EXISTS is_banned")
            logger.info("Dropped is_banned column if it existed")

            # Create users table with English prefixes
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
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
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

            # Insert default settings if they don't exist
            default_settings = [
                ("price_usd", 2.5),  # Default price per 50 stars in USD
                ("markup", 10.0),    # Default markup percentage
                ("ref_bonus", 5.0)   # Default referral bonus percentage
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

            # Update prefixes to English based on stars_bought
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
            logger.info("User prefixes updated to English based on stars_bought")

            # Ensure admin user exists
            admin_user_id = 6956377285  # Replace with your admin user ID
            await conn.execute(
                """
                INSERT INTO users (user_id, username, is_admin, prefix, created_at)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (user_id) DO UPDATE
                SET is_admin = EXCLUDED.is_admin, prefix = EXCLUDED.prefix
                """,
                admin_user_id, "Admin", True, "Verified", datetime.now(pytz.UTC)
            )
            logger.info(f"Admin user {admin_user_id} ensured")

    except asyncpg.InterfaceError as e:
        logger.error(f"Database connection error during initialization: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Error initializing database: {e}", exc_info=True)
        raise
    
async def close_db_pool():
    """Close the database pool."""
    global db_pool
    async with _db_pool_lock:
        if db_pool and not db_pool._closed:
            await db_pool.close()
            logger.info("Database pool closed")
            db_pool = None

async def get_text(key: str, **kwargs) -> str:
    """Получение текста сообщения с подстановкой параметров."""
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
        "user_banned": "Вы забанены. Обратитесь в поддержку: https://t.me/CheapStarsShop_support",
        "bot_settings": "Настройки бота:\nЦена за 50 звезд: ${price_usd}\nНакрутка: {markup}%\nРеферальный бонус: {ref_bonus}%",
        "tech_support": "📞 Поддержка: https://t.me/CheapStarsShop_support",
        "reviews": "📝 Отзывы: https://t.me/CheapStarsShop_support"
    }
    return texts.get(key, "Неизвестный текст").format(**kwargs)
    
async def log_analytics(user_id: int, action: str, data: dict = None):
    """Логирование аналитики."""
    try:
        async with (await ensure_db_pool()) as conn:
            await conn.execute(
                "INSERT INTO analytics (user_id, action, timestamp, data) VALUES ($1, $2, $3, $4)",
                user_id, action, datetime.now(pytz.UTC), json.dumps(data) if data else None
            )
    except Exception as e:
        logger.error(f"Ошибка логирования аналитики: {e}", exc_info=True)

async def update_ton_price():
    """Обновление цены TON с использованием TonAPI."""
    if not TON_API_KEY:
        logger.error("TON_API_KEY не задан, пропуск обновления цены TON")
        telegram_app.bot_data["ton_price_info"] = {"price": 0.0, "diff_24h": 0.0}
        return
    try:
        headers = {"Authorization": f"Bearer {TON_API_KEY}"}
        url = "https://tonapi.io/v2/rates?tokens=ton&currencies=usd"
        logger.debug(f"Запрос к TonAPI: {url}")
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
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
    """Load bot settings from the database."""
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


async def ton_price_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /tonprice."""
    global tech_break_info
    user_id = update.effective_user.id
    async with (await ensure_db_pool()) as conn:
        is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id)
        is_banned = await conn.fetchval("SELECT is_banned FROM users WHERE user_id = $1", user_id)
        if is_banned:
            text = await get_text("user_banned", support_channel=SUPPORT_CHANNEL)
            await update.message.reply_text(text)
            context.user_data["state"] = STATES["main_menu"]
            await log_analytics(user_id, "ton_price_banned", {})
            return STATES["main_menu"]
        if not is_admin and tech_break_info and tech_break_info["end_time"] > datetime.now(pytz.UTC):
            time_remaining = format_time_remaining(tech_break_info["end_time"])
            text = await get_text(
                "tech_break_active",
                end_time=tech_break_info["end_time"].strftime("%Y-%m-%d %H:%M:%S UTC"),
                minutes_left=time_remaining,
                reason=tech_break_info["reason"]
            )
            await update.message.reply_text(text)
            context.user_data["state"] = STATES["main_menu"]
            await log_analytics(user_id, "ton_price_tech_break", {})
            return STATES["main_menu"]
    
    REQUESTS.labels(endpoint="tonprice").inc()
    with RESPONSE_TIME.labels(endpoint="tonprice").time():
        try:
            if "ton_price_info" not in telegram_app.bot_data or telegram_app.bot_data["ton_price_info"].get("price", 0.0) == 0.0:
                await update_ton_price()
            if "ton_price_info" not in telegram_app.bot_data or telegram_app.bot_data["ton_price_info"].get("price", 0.0) == 0.0:
                await update.message.reply_text("Ошибка получения цены TON. Попробуйте позже.")
                context.user_data["state"] = STATES["main_menu"]
                await log_analytics(user_id, "ton_price_error", {})
                return STATES["main_menu"]
            price = telegram_app.bot_data["ton_price_info"]["price"]
            diff_24h = telegram_app.bot_data["ton_price_info"]["diff_24h"]
            change_text = f"📈 +{diff_24h:.2f}%" if diff_24h >= 0 else f"📉 {diff_24h:.2f}%"
            text = f"💰 Цена TON: ${price:.2f}\nИзменение за 24ч: {change_text}"
            await update.message.reply_text(text)
            context.user_data["state"] = STATES["main_menu"]
            await log_analytics(user_id, "ton_price", {})
            logger.info(f"/tonprice выполнен для user_id={user_id}")
        except Exception as e:
            logger.error(f"Ошибка в /tonprice для user_id={user_id}: {e}", exc_info=True)
            ERRORS.labels(type="tonprice", endpoint="tonprice").inc()
            await update.message.reply_text("Ошибка при получении цены TON. Попробуйте позже.")
            context.user_data["state"] = STATES["main_menu"]
            await log_analytics(user_id, "ton_price_error", {"error": str(e)})
        return STATES["main_menu"]
async def generate_payload(user_id):
    """Генерация уникального payload для платежа."""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    random_str = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    payload = f"{user_id}_{timestamp}_{random_str}"
    secret = os.getenv("BOT_TOKEN").encode()
    signature = hmac.new(secret, payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}:{signature}"

def format_time_remaining(end_time):
    """Format the time remaining until end_time in days, hours, and minutes."""
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

async def verify_payload(payload, signature):
    """Проверка подписи payload."""
    secret = os.getenv("BOT_TOKEN").encode()
    expected_signature = hmac.new(secret, payload.encode(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(signature, expected_signature)

async def create_cryptobot_invoice(amount_usd, currency, user_id, stars, recipient, payload):
    """Создание инвойса в Cryptobot."""
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
    """Проверка переменных окружения."""
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
    missing_vars = []
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            missing_vars.append(var)
            logger.error(f"Отсутствует обязательная переменная окружения: {var}")
        else:
            logger.debug(f"Переменная окружения {var} установлена")
    if missing_vars:
        raise ValueError(f"Отсутствуют обязательные переменные окружения: {', '.join(missing_vars)}")

@asynccontextmanager
async def lifespan(app: web.Application):
    """Manage application startup and shutdown."""
    global telegram_app
    try:
        # Startup
        await check_environment()
        await ensure_db_pool()
        await init_db()  # Initialize database schema
        await load_settings()

        # Initialize Telegram bot
        telegram_app = (
            ApplicationBuilder()
            .token(BOT_TOKEN)
            .concurrent_updates(True)
            .http_version("1.1")
            .build()
        )
        setup_handlers(telegram_app)  # Register handlers
        await telegram_app.initialize()
        webhook_url = f"{WEBHOOK_URL}/webhook"
        await telegram_app.bot.set_webhook(webhook_url)
        await telegram_app.updater.start_webhook(
            listen="0.0.0.0",
            port=8443,
            url_path="/webhook",
            webhook_url=webhook_url,
            cert=SSL_CERT_PATH,
            key=SSL_KEY_PATH
        )
        logger.info(f"Telegram webhook started at {webhook_url}")

        # Start scheduler
        scheduler = AsyncIOScheduler(timezone="UTC")
        scheduler.add_job(heartbeat_check, "interval", seconds=300, args=[telegram_app])
        scheduler.add_job(check_reminders, "interval", seconds=60)
        scheduler.add_job(backup_db, "interval", hours=24)
        scheduler.add_job(update_ton_price, "interval", minutes=30)
        scheduler.add_job(keep_alive, "interval", minutes=10, args=[telegram_app])
        scheduler.start()
        logger.info("Scheduler started")

        yield  # Application runs here

    finally:
        # Shutdown
        if 'scheduler' in locals():
            scheduler.shutdown()
            logger.info("Scheduler shut down")
        if telegram_app:
            await telegram_app.updater.stop()
            await telegram_app.shutdown()
            logger.info("Telegram bot shut down")
        await close_db_pool()
        logger.info("Application shutdown complete")


async def test_db_connection():
    """Тестирование подключения к базе данных."""
    try:
        async with (await ensure_db_pool()) as conn:
            version = await conn.fetchval("SELECT version();")
            logger.info(f"DB connected: {version}")
    except Exception as e:
        logger.error(f"Ошибка подключения к базе данных: {e}", exc_info=True)
        raise

async def check_webhook():
    webhook_info = await telegram_app.bot.get_webhook_info()
    logger.info(f"Webhook info: {webhook_info}")
    expected_url = f"{WEBHOOK_URL}/webhook"
    if webhook_info.url != expected_url:
        logger.warning(f"Webhook URL mismatch: expected {expected_url}, got {webhook_info.url}")
        await telegram_app.bot.set_webhook(expected_url)
        logger.info(f"Webhook reset to {expected_url}")
        
async def heartbeat_check(app):
    """Проверка работоспособности DB и API."""
    try:
        # Ensure pool is initialized before proceeding
        async with (await ensure_db_pool()) as conn:
            pass  # Just to ensure pool is ready
        await test_db_connection()
        await check_webhook()
        async with aiohttp.ClientSession(timeout=ClientTimeout(total=30)) as session:
            headers = {"Authorization": f"Bearer {TON_API_KEY}"}
            try:
                async with session.get("https://tonapi.io/v2/status", headers=headers) as resp:
                    if resp.status != 200:
                        logger.warning(f"TON API health check failed: {resp.status}")
                        ERRORS.labels(type="api", endpoint="ton_health").inc()
            except asyncio.exceptions.CancelledError as e:
                logger.error(f"CancelledError in TON API health check: {e}", exc_info=True)
                ERRORS.labels(type="cancelled", endpoint="ton_health").inc()
            except Exception as e:
                logger.error(f"Ошибка проверки TON API: {e}", exc_info=True)
                ERRORS.labels(type="api", endpoint="ton_health").inc()
            if CRYPTOBOT_API_TOKEN:
                try:
                    async with session.get(CRYPTOBOT_API_URL + "/getMe", headers={"Crypto-Pay-API-Token": CRYPTOBOT_API_TOKEN}) as resp:
                        if resp.status != 200:
                            logger.warning(f"Cryptobot API health check failed: {resp.status}")
                            ERRORS.labels(type="api", endpoint="cryptobot_health").inc()
                except asyncio.exceptions.CancelledError as e:
                    logger.error(f"CancelledError in Cryptobot API health check: {e}", exc_info=True)
                    ERRORS.labels(type="cancelled", endpoint="cryptobot_health").inc()
                except Exception as e:
                    logger.error(f"Ошибка проверки Cryptobot API: {e}", exc_info=True)
                    ERRORS.labels(type="api", endpoint="cryptobot_health").inc()
        logger.info("Heartbeat check passed")
    except Exception as e:
        logger.error(f"Heartbeat check failed: {e}", exc_info=True)
        ERRORS.labels(type="heartbeat", endpoint="heartbeat").inc()
        try:
            await app.bot.send_message(
                chat_id=ADMIN_BACKUP_ID,
                text=f"⚠️ Бот: Проблема с подключением: {str(e)}"
            )
        except Exception as notify_error:
            logger.error(f"Не удалось отправить уведомление об ошибке: {notify_error}")

async def keep_alive(app):
    """Отправка команды /start для поддержания активности бота."""
    chat_id = str(TWIN_ACCOUNT_ID)
    try:
        await app.bot.send_message(chat_id=chat_id, text="/start")
        logger.info(f"Sent /start to chat_id={chat_id} to keep bot active")
    except Exception as e:
        logger.error(f"Failed to send keep-alive /start to chat_id={chat_id}: {e}")
        ERRORS.labels(type="telegram_api", endpoint="keep_alive").inc()

async def check_reminders():
    """Проверка и отправка напоминаний."""
    try:
        async with (await ensure_db_pool()) as conn:
            today = datetime.now(pytz.UTC).date()
            reminders = await conn.fetch(
                "SELECT user_id, reminder_type FROM reminders WHERE reminder_date = $1",
                today
            )
            for reminder in reminders:
                user_id = reminder["user_id"]
                reminder_type = reminder["reminder_type"]
                try:
                    await telegram_app.bot.send_message(
                        chat_id=user_id,
                        text=f"📅 Напоминание: Пора обновить базу данных ({reminder_type})!"
                    )
                    await conn.execute(
                        "DELETE FROM reminders WHERE user_id = $1 AND reminder_date = $2 AND reminder_type = $3",
                        user_id, today, reminder_type
                    )
                    await log_analytics(user_id, "send_reminder", {"type": reminder_type})
                except Exception as e:
                    logger.error(f"Ошибка отправки напоминания пользователю {user_id}: {e}")
                    ERRORS.labels(type="telegram_api", endpoint="check_reminders").inc()
    except Exception as e:
        logger.error(f"Ошибка проверки напоминаний: {e}", exc_info=True)
        ERRORS.labels(type="reminder", endpoint="check_reminders").inc()

async def backup_db():
    """Создание бэкапа базы данных и отправка администратору."""
    try:
        async with (await ensure_db_pool()) as conn:
            users = await conn.fetch("SELECT * FROM users")
            analytics = await conn.fetch("SELECT * FROM analytics")
            reminders = await conn.fetch("SELECT * FROM reminders")
            transactions = await conn.fetch("SELECT * FROM transactions")
            feedback = await conn.fetch("SELECT * FROM feedback")
            support_tickets = await conn.fetch("SELECT * FROM support_tickets")
            backup_data = {
                "users": [
                    {
                        **dict(row),
                        "referrals": json.loads(row["referrals"]) if row["referrals"] else []
                    } for row in users
                ],
                "analytics": [
                    {
                        **dict(row),
                        "timestamp": row["timestamp"].isoformat() if row["timestamp"] else None,
                        "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                        "data": json.loads(row["data"]) if row["data"] else None
                    } for row in analytics
                ],
                "reminders": [
                    {
                        **dict(row),
                        "reminder_date": row["reminder_date"].isoformat() if row["reminder_date"] else None,
                        "created_at": row["created_at"].isoformat() if row["created_at"] else None
                    } for row in reminders
                ],
                "transactions": [
                    {
                        **dict(row),
                        "timestamp": row["timestamp"].isoformat() if row["timestamp"] else None
                    } for row in transactions
                ],
                "feedback": [
                    {
                        **dict(row),
                        "timestamp": row["timestamp"].isoformat() if row["timestamp"] else None
                    } for row in feedback
                ],
                "support_tickets": [
                    {
                        **dict(row),
                        "timestamp": row["timestamp"].isoformat() if row["timestamp"] else None
                    } for row in support_tickets
                ]
            }
            backup_file = f"db_backup_{datetime.now(pytz.UTC).strftime('%Y-%m-%d_%H-%M-%S')}.json"
            backup_json = json.dumps(backup_data, ensure_ascii=False, indent=2)
            bio = BytesIO(backup_json.encode('utf-8'))
            bio.name = backup_file
            await telegram_app.bot.send_document(
                chat_id=ADMIN_BACKUP_ID,
                document=bio,
                filename=backup_file
            )
            logger.info(f"Бэкап отправлен администратору: {backup_file}")
            return backup_file, backup_data
    except Exception as e:
        logger.error(f"Ошибка создания/отправки бэкапа: {e}", exc_info=True)
        ERRORS.labels(type="backup", endpoint="backup_db").inc()
        raise

async def broadcast_new_menu():
    """Рассылка нового меню всем пользователям."""
    try:
        async with (await ensure_db_pool()) as conn:
            users = await conn.fetch("SELECT user_id, username, stars_bought FROM users WHERE is_banned = FALSE")
            total_stars = await conn.fetchval("SELECT SUM(stars_bought) FROM users") or 0
            for user in users:
                user_id = user["user_id"]
                try:
                    user_stars = user["stars_bought"] or 0
                    text = await get_text(
                        "welcome",
                        username=user["username"] or "User",
                        stars_bought=user_stars,
                        total_stars=total_stars
                    )
                    text += "\n\n⚠️ Используйте новое меню ниже для корректной работы бота."
                    keyboard = [
                        [
                            InlineKeyboardButton("📰 Новости", url=NEWS_CHANNEL),
                            InlineKeyboardButton("📞 Поддержка и Отзывы", callback_data="support_reviews")
                        ],
                        [InlineKeyboardButton("👤 Профиль", callback_data="profile"), InlineKeyboardButton("🤝 Рефералы", callback_data="referrals")],
                        [InlineKeyboardButton("🛒 Купить звезды", callback_data="buy_stars")],
                        [InlineKeyboardButton("📜 История транзакций", callback_data=TRANSACTION_HISTORY)],
                        [InlineKeyboardButton("🏆 Лидеры рефералов", callback_data=REFERRAL_LEADERBOARD)],
                        [InlineKeyboardButton("📢 Отправить отзыв", callback_data=FEEDBACK)],
                        [InlineKeyboardButton("🆘 Создать тикет поддержки", callback_data=SUPPORT_TICKET)]
                    ]
                    if user_id == 6956377285:
                        keyboard.append([InlineKeyboardButton("🔧 Админ-панель", callback_data="admin_panel")])
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    last_message = telegram_app.bot_data.get(f"last_message_{user_id}", {})
                    if last_message.get("message_id") and last_message.get("chat_id"):
                        try:
                            await telegram_app.bot.edit_message_text(
                                chat_id=last_message["chat_id"],
                                message_id=last_message["message_id"],
                                text=text,
                                reply_markup=reply_markup
                            )
                            logger.info(f"Сообщение обновлено для user_id={user_id}, message_id={last_message['message_id']}")
                        except BadRequest as e:
                            if "Message to edit not found" in str(e) or "Message is not modified" in str(e):
                                sent_message = await telegram_app.bot.send_message(
                                    chat_id=user_id,
                                    text=text,
                                    reply_markup=reply_markup
                                )
                                telegram_app.bot_data[f"last_message_{user_id}"] = {
                                    "chat_id": sent_message.chat_id,
                                    "message_id": sent_message.message_id
                                }
                                logger.info(f"Новое сообщение отправлено для user_id={user_id}, message_id={sent_message.message_id}")
                            else:
                                logger.error(f"Ошибка редактирования сообщения для user_id={user_id}: {e}")
                                ERRORS.labels(type="telegram_api", endpoint="broadcast_new_menu").inc()
                    else:
                        sent_message = await telegram_app.bot.send_message(
                            chat_id=user_id,
                            text=text,
                            reply_markup=reply_markup
                        )
                        telegram_app.bot_data[f"last_message_{user_id}"] = {
                            "chat_id": sent_message.chat_id,
                            "message_id": sent_message.message_id
                        }
                        logger.info(f"Новое сообщение отправлено для user_id={user_id}, message_id={sent_message.message_id}")
                    await log_analytics(user_id, "broadcast_new_menu")
                    await asyncio.sleep(0.05)
                except TelegramError as e:
                    logger.error(f"Ошибка отправки нового меню пользователю {user_id}: {e}")
                    ERRORS.labels(type="telegram_api", endpoint="broadcast_new_menu").inc()
        logger.info("Рассылка нового меню завершена")
    except Exception as e:
        logger.error(f"Ошибка при рассылке нового меню: {e}", exc_info=True)
        ERRORS.labels(type="broadcast", endpoint="broadcast_new_menu").inc()

async def broadcast_message_to_users(message: str):
    """Отправка сообщения всем пользователям."""
    async with (await ensure_db_pool()) as conn:
        users = await conn.fetch("SELECT user_id FROM users WHERE is_banned = FALSE")
        success_count = 0
        failed_count = 0
        for user in users:
            try:
                await telegram_app.bot.send_message(chat_id=user["user_id"], text=message)
                success_count += 1
                await asyncio.sleep(0.05)
            except TelegramError as e:
                logger.error(f"Ошибка отправки сообщения пользователю {user['user_id']}: {e}")
                failed_count += 1
        return success_count, failed_count

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the /start command."""
    user_id = update.effective_user.id
    username = update.effective_user.username or str(user_id)
    referrer_id = None
    args = context.args
    REQUESTS.labels(endpoint="start").inc()
    with RESPONSE_TIME.labels(endpoint="start").time():
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
                    "INSERT INTO users (user_id, username, stars_bought, referrals, ref_bonus_ton, is_admin) "
                    "VALUES ($1, $2, $3, $4, $5, $6)",
                    user_id, username, 0, json.dumps([]), 0.0, False
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
                text = await get_text("user_banned", support_channel=SUPPORT_CHANNEL)
                await update.message.reply_text(text)
                context.user_data["state"] = STATES["main_menu"]
                await log_analytics(user_id, "start_banned", {})
                return STATES["main_menu"]
            if tech_break_info and tech_break_info["end_time"] > datetime.now(pytz.UTC) and not is_admin:
                time_remaining = format_time_remaining(tech_break_info["end_time"])
                text = await get_text(
                    "tech_break_active",
                    end_time=tech_break_info["end_time"].strftime("%Y-%m-%d %H:%M:%S UTC"),
                    minutes_left=time_remaining,
                    reason=tech_break_info["reason"]
                )
                await update.message.reply_text(text)
                context.user_data["state"] = STATES["main_menu"]
                await log_analytics(user_id, "start_tech_break", {})
                return STATES["main_menu"]
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
            context.user_data["state"] = STATES["main_menu"]
            await log_analytics(user_id, "start", {"referrer_id": referrer_id})
            return STATES["main_menu"]

async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик callback-запросов."""
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
                    time_remaining = format_time_remaining(tech_break_info["end_time"])
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
                    # Test mode: simulate successful payment
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
                "Ошибка подключения к базе данных. Попробуйте позже.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
            )
            await query.answer()
            context.user_data["state"] = STATES["main_menu"]
            await log_analytics(user_id, "callback_error", {"error": str(e)})
            return STATES["main_menu"]

        except asyncpg.exceptions.InterfaceError as e:
            logger.error(f"Database pool error in callback_query_handler: {e}", exc_info=True)
            await query.message.edit_text(
                "Ошибка подключения к базе данных. Попробуйте позже.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
            )
            await query.answer()
            context.user_data["state"] = STATES["main_menu"]
            await log_analytics(user_id, "callback_error", {"error": str(e)})
            return STATES["main_menu"]

async def show_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отображение админ-панели."""
    user_id = update.effective_user.id
    logger.debug(f"Entering show_admin_panel for user_id={user_id}")
    try:
        async with (await ensure_db_pool()) as conn:
            logger.debug(f"Checking admin status for user_id={user_id}")
            is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id) or False
            if not is_admin:
                logger.warning(f"User {user_id} is not an admin, redirecting to main menu")
                text = "У вас нет доступа к админ-панели."
                if update.callback_query:
                    await update.callback_query.message.edit_text(
                        text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]),
                        parse_mode="HTML"
                    )
                else:
                    await update.message.reply_text(
                        text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]),
                        parse_mode="HTML"
                    )
                context.user_data["state"] = STATES["main_menu"]
                await log_analytics(user_id, "admin_panel_access_denied", {})
                return STATES["main_menu"]

            logger.debug("Fetching reminder date")
            reminder = await conn.fetchrow("SELECT reminder_date FROM reminders WHERE reminder_type = 'db_update'")
            reminder_text = reminder["reminder_date"].strftime("%Y-%m-%d") if reminder else "Не установлено"
            logger.debug(f"Reminder text: {reminder_text}")
            text = await get_text(
                "admin_panel",
                reminder_date=reminder_text
            )
            keyboard = [
                [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
                [InlineKeyboardButton("📝 Редактировать профиль", callback_data="admin_edit_profile")],
                [InlineKeyboardButton("📋 Список пользователей", callback_data="all_users")],
                [InlineKeyboardButton("📬 Рассылка", callback_data="broadcast_message")],
                [InlineKeyboardButton("⚙️ Настройки бота", callback_data="bot_settings")],
                [InlineKeyboardButton("📜 История транзакций", callback_data="transaction_history_0")],
                [
                    InlineKeyboardButton(f"🔔 Напоминание о БД: {reminder_text}", callback_data="set_db_reminder" if not reminder else "view_db_reminder"),
                    InlineKeyboardButton("🗑️ Очистить", callback_data="clear_db_reminder")
                ],
                [InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]
            ]
            logger.debug("Attempting to send/edit message with admin panel")
            try:
                if update.callback_query:
                    await update.callback_query.message.edit_text(
                        text,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode="HTML"
                    )
                    await update.callback_query.answer()
                else:
                    await update.message.reply_text(
                        text,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode="HTML"
                    )
            except BadRequest as e:
                logger.error(f"Failed to edit message: {e}", exc_info=True)
                if update.callback_query:
                    await update.callback_query.message.edit_text(
                        "Ошибка отображения админ-панели. Попробуйте позже.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]),
                        parse_mode="HTML"
                    )
                    await update.callback_query.answer()
                else:
                    await update.message.reply_text(
                        "Ошибка отображения админ-панели. Попробуйте позже.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]),
                        parse_mode="HTML"
                    )
                context.user_data["state"] = STATES["main_menu"]
                await log_analytics(user_id, "admin_panel_message_error", {"error": str(e)})
                return STATES["main_menu"]

            context.user_data["state"] = STATES["admin_panel"]
            await log_analytics(user_id, "view_admin_panel", {})
            logger.debug("Admin panel displayed successfully")
            return STATES["admin_panel"]

    except asyncpg.exceptions.InterfaceError as e:
        logger.error(f"Database pool error in show_admin_panel: {e}", exc_info=True)
        text = "Ошибка подключения к базе данных. Попробуйте позже."
        if update.callback_query:
            await update.callback_query.message.edit_text(
                text,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]),
                parse_mode="HTML"
            )
            await update.callback_query.answer()
        else:
            await update.message.reply_text(
                text,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]),
                parse_mode="HTML"
            )
        context.user_data["state"] = STATES["main_menu"]
        await log_analytics(user_id, "admin_panel_db_error", {"error": str(e)})
        return STATES["main_menu"]
    except Exception as e:
        logger.error(f"Unexpected error in show_admin_panel: {e}", exc_info=True)
        text = f"Произошла ошибка: {str(e)}. Попробуйте позже."
        if update.callback_query:
            await update.callback_query.message.edit_text(
                text,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]),
                parse_mode="HTML"
            )
            await update.callback_query.answer()
        else:
            await update.message.reply_text(
                text,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]),
                parse_mode="HTML"
            )
        context.user_data["state"] = STATES["main_menu"]
        await log_analytics(user_id, "admin_panel_unexpected_error", {"error": str(e)})
        return STATES["main_menu"]

async def webhook_handler(request: web.Request) -> web.Response:
    """Handle incoming Telegram webhook updates."""
    global telegram_app
    try:
        if not telegram_app:
            logger.error("Telegram application not initialized")
            return web.Response(status=500, text="Internal Server Error: Telegram app not initialized")

        # Read and parse the webhook request
        update = await request.json()
        if not update:
            logger.warning("Received empty webhook update")
            return web.Response(status=400, text="Bad Request: Empty update")

        # Process the update using python-telegram-bot
        await telegram_app.update_queue.put(update)
        return web.Response(status=200, text="OK")

    except Exception as e:
        logger.error(f"Error processing webhook update: {e}", exc_info=True)
        return web.Response(status=500, text=f"Internal Server Error: {str(e)}")
        
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text
    logger.info(f"Received message from user {user_id}: {text}")

    # Check if user is admin
    async with (await ensure_db_pool()) as conn:
        is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id) or False

    # Handle admin-specific commands
    if is_admin and text.lower() == "/admin":
        return await show_admin_panel(update, context)
    else:
        # Handle other text messages
        await update.message.reply_text(
            "Пожалуйста, используйте команды бота или свяжитесь с поддержкой: @CheapStarsShop_support",
            parse_mode="HTML"
        )
                
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений."""
    global tech_break_info
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
                    await update.message.reply_text(text)
                    context.user_data["state"] = STATES["main_menu"]
                    await log_analytics(user_id, "message_banned", {})
                    return STATES["main_menu"]
                if not is_admin and tech_break_info and tech_break_info["end_time"] > datetime.now(pytz.UTC):
                    time_remaining = format_time_remaining(tech_break_info["end_time"])
                    text = await get_text(
                        "tech_break_active",
                        end_time=tech_break_info["end_time"].strftime("%Y-%m-%d %H:%M:%S UTC"),
                        minutes_left=time_remaining,
                        reason=tech_break_info["reason"]
                    )
                    await update.message.reply_text(text)
                    context.user_data["state"] = STATES["main_menu"]
                    await log_analytics(user_id, "message_tech_break", {})
                    return STATES["main_menu"]

                if state == STATES["buy_stars_recipient"]:
                    recipient = text.strip()
                    if not recipient.startswith("@"):
                        recipient = f"@{recipient}"
                    try:
                        chat = await telegram_app.bot.get_chat(recipient.lower())
                        context.user_data["recipient"] = recipient
                        stars = context.user_data.get("stars_amount", "Не выбрано")
                        price_ton = await calculate_price_ton(int(stars)) if stars and isinstance(stars, str) and stars.isdigit() else None
                        price_text = f"~{price_ton:.2f} TON" if price_ton is not None else "Цена"
                        if not stars or not isinstance(stars, str) or not stars.isdigit():
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
                            await update.message.reply_text(
                                text,
                                reply_markup=InlineKeyboardMarkup(keyboard),
                                parse_mode="HTML"
                            )
                            context.user_data["state"] = STATES["select_stars_menu"]
                            await log_analytics(user_id, "set_recipient_no_stars", {"recipient": recipient})
                            return STATES["select_stars_menu"]
                        reply_text = (
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
                            [InlineKeyboardButton("🔙 Назад", callback_data="buy_stars")]
                        ]
                        await update.message.reply_text(
                            reply_text,
                            reply_markup=InlineKeyboardMarkup(keyboard),
                            parse_mode="HTML"
                        )
                        context.user_data["state"] = STATES["buy_stars_payment_method"]
                        await log_analytics(user_id, "set_recipient", {"recipient": recipient})
                        return STATES["buy_stars_payment_method"]
                    except TelegramError as e:
                        logger.error(f"Failed to get chat for {recipient}: {e}")
                        username = recipient[1:].lower()
                        user_exists = await conn.fetchval(
                            "SELECT 1 FROM users WHERE lower(username) = $1", username
                        )
                        if user_exists:
                            context.user_data["recipient"] = recipient
                            stars = context.user_data.get("stars_amount", "Не выбрано")
                            price_ton = await calculate_price_ton(int(stars)) if stars and isinstance(stars, str) and stars.isdigit() else None
                            price_text = f"~{price_ton:.2f} TON" if price_ton is not None else "Цена"
                            if not stars or not isinstance(stars, str) or not stars.isdigit():
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
                                await update.message.reply_text(
                                    text,
                                    reply_markup=InlineKeyboardMarkup(keyboard),
                                    parse_mode="HTML"
                                )
                                context.user_data["state"] = STATES["select_stars_menu"]
                                await log_analytics(user_id, "set_recipient_no_stars_db", {"recipient": recipient})
                                return STATES["select_stars_menu"]
                            reply_text = (
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
                                [InlineKeyboardButton("🔙 Назад", callback_data="buy_stars")]
                            ]
                            await update.message.reply_text(
                                reply_text,
                                reply_markup=InlineKeyboardMarkup(keyboard),
                                parse_mode="HTML"
                            )
                            context.user_data["state"] = STATES["buy_stars_payment_method"]
                            await log_analytics(user_id, "set_recipient_db", {"recipient": recipient})
                            return STATES["buy_stars_payment_method"]
                        await update.message.reply_text(
                            f"Неверное имя пользователя: {recipient}. Введите корректное имя (например, @username).",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="buy_stars")]])
                        )
                        return STATES["buy_stars_recipient"]

                elif state == STATES["buy_stars_custom"]:
                    try:
                        stars = int(text)
                        if stars <= 0:
                            raise ValueError("Количество звезд должно быть положительным")
                        context.user_data["stars_amount"] = str(stars)
                        recipient = context.user_data.get("recipient", "Не выбран")
                        price_ton = await calculate_price_ton(stars)
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
                        await log_analytics(user_id, "set_custom_stars", {"stars": stars})
                        return STATES["buy_stars_payment_method"]
                    except ValueError:
                        await update.message.reply_text(
                            "Пожалуйста, введите корректное число звезд (положительное).",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="select_stars_menu")]])
                        )
                        context.user_data["state"] = STATES["buy_stars_custom"]
                        return STATES["buy_stars_custom"]

                elif state == STATES["admin_broadcast"] and is_admin:
                    context.user_data["broadcast_text"] = text
                    keyboard = [
                        [InlineKeyboardButton("✅ Отправить", callback_data="confirm_broadcast")],
                        [InlineKeyboardButton("❌ Отменить", callback_data="cancel_broadcast")]
                    ]
                    await update.message.reply_text(
                        f"Подтвердите текст рассылки:\n\n{text}",
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode="HTML"
                    )
                    context.user_data["state"] = STATES["admin_broadcast"]
                    await log_analytics(user_id, "set_broadcast_text", {"text": text[:50]})
                    return STATES["admin_broadcast"]

                elif state == STATES["admin_edit_profile"] and is_admin:
                    edit_user_id = context.user_data.get("edit_user_id")
                    edit_field = context.user_data.get("edit_profile_field")
                    if not edit_user_id:
                        try:
                            edit_user_id = int(text)
                            user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", edit_user_id)
                            if not user:
                                await update.message.reply_text(
                                    "Пользователь не найден. Введите другой ID.",
                                    reply_markup=InlineKeyboardMarkup([
                                        [InlineKeyboardButton("📋 Все пользователи", callback_data="all_users")],
                                        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]
                                    ])
                                )
                                return STATES["admin_edit_profile"]
                            context.user_data["edit_user_id"] = edit_user_id
                            keyboard = [
                                [InlineKeyboardButton("Звезды", callback_data="edit_profile_stars")],
                                [InlineKeyboardButton("Рефералы", callback_data="edit_profile_referrals")],
                                [InlineKeyboardButton("Реферальный бонус", callback_data="edit_profile_ref_bonus")],
                                [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]
                            ]
                            await update.message.reply_text(
                                f"Редактирование профиля ID {edit_user_id}. Выберите поле:",
                                reply_markup=InlineKeyboardMarkup(keyboard)
                            )
                            context.user_data["state"] = STATES["admin_edit_profile"]
                            await log_analytics(user_id, "select_edit_user", {"edit_user_id": edit_user_id})
                            return STATES["admin_edit_profile"]
                        except ValueError:
                            await update.message.reply_text(
                                "Пожалуйста, введите корректный ID пользователя.",
                                reply_markup=InlineKeyboardMarkup([
                                    [InlineKeyboardButton("📋 Все пользователи", callback_data="all_users")],
                                    [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]
                                ])
                            )
                            return STATES["admin_edit_profile"]
                    elif edit_field:
                        try:
                            if edit_field == "stars_bought":
                                stars = int(text)
                                if stars < 0:
                                    raise ValueError("Количество звезд не может быть отрицательным.")
                                await conn.execute(
                                    "UPDATE users SET stars_bought = $1 WHERE user_id = $2",
                                    stars, edit_user_id
                                )
                                await update.message.reply_text(
                                    f"Количество звезд для ID {edit_user_id} обновлено: {stars}.",
                                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                                )
                                await log_analytics(user_id, "edit_profile_stars", {"edit_user_id": edit_user_id, "stars": stars})
                            elif edit_field == "referrals":
                                referrals = [int(r) for r in text.split(",") if r.strip().isdigit()]
                                await conn.execute(
                                    "UPDATE users SET referrals = $1 WHERE user_id = $2",
                                    json.dumps(referrals), edit_user_id
                                )
                                await update.message.reply_text(
                                    f"Рефералы для ID {edit_user_id} обновлены.",
                                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                                )
                                await log_analytics(user_id, "edit_profile_referrals", {"edit_user_id": edit_user_id, "referrals": len(referrals)})
                            elif edit_field == "ref_bonus_ton":
                                bonus = float(text)
                                if bonus < 0:
                                    raise ValueError("Бонус не может быть отрицательным.")
                                await conn.execute(
                                    "UPDATE users SET ref_bonus_ton = $1 WHERE user_id = $2",
                                    bonus, edit_user_id
                                )
                                await update.message.reply_text(
                                    f"Реферальный бонус для ID {edit_user_id} обновлен: {bonus} TON.",
                                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                                )
                                await log_analytics(user_id, "edit_profile_ref_bonus", {"edit_user_id": edit_user_id, "bonus": bonus})
                            context.user_data.pop("edit_user_id", None)
                            context.user_data.pop("edit_profile_field", None)
                            context.user_data["state"] = STATES["admin_panel"]
                            return await show_admin_panel(update, context)
                        except ValueError as e:
                            await update.message.reply_text(
                                f"Ошибка: {str(e)}. Пожалуйста, введите корректное значение.",
                                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                            )
                            return STATES["admin_edit_profile"]

                elif state == STATES["bot_settings"] and is_admin:
                    setting_field = context.user_data.get("setting_field")
                    if setting_field:
                        try:
                            value = float(text)
                            if value < 0:
                                raise ValueError("Значение не может быть отрицательным.")
                            await conn.execute(
                                "INSERT INTO settings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2",
                                setting_field, value
                            )
                            await update.message.reply_text(
                                f"Настройка '{setting_field}' обновлена: {value}.",
                                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                            )
                            context.user_data.pop("setting_field", None)
                            context.user_data["state"] = STATES["admin_panel"]
                            await log_analytics(user_id, f"set_{setting_field}", {"value": value})
                            return await show_admin_panel(update, context)
                        except ValueError as e:
                            await update.message.reply_text(
                                f"Ошибка: {str(e)}. Введите корректное числовое значение.",
                                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                            )
                            return STATES["bot_settings"]

                elif state == STATES["set_db_reminder"] and is_admin:
                    try:
                        reminder_date = datetime.strptime(text, "%Y-%m-%d").date()
                        if reminder_date < datetime.now(pytz.UTC).date():
                            raise ValueError("Дата напоминания не может быть в прошлом.")
                        await conn.execute("DELETE FROM reminders WHERE reminder_type = 'db_update'")
                        await conn.execute(
                            "INSERT INTO reminders (user_id, reminder_date, reminder_type) VALUES ($1, $2, $3)",
                            user_id, reminder_date, "db_update"
                        )
                        text = await get_text("reminder_set", reminder_date=reminder_date)
                        await update.message.reply_text(
                            text,
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")],
                                [InlineKeyboardButton("Изменить дату", callback_data="set_db_reminder")]
                            ])
                        )
                        context.user_data["state"] = STATES["admin_panel"]
                        await log_analytics(user_id, "set_db_reminder", {"reminder_date": str(reminder_date)})
                        return await show_admin_panel(update, context)
                    except ValueError as e:
                        await update.message.reply_text(
                            f"Ошибка: {str(e)}. Введите дату в формате гггг-мм-дд.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                        )
                        return STATES["set_db_reminder"]

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
                    await log_analytics(user_id, "unknown_message", {"text": text[:50], "state": state})
                    return STATES["main_menu"]

        except asyncpg.exceptions.InterfaceError as e:
            logger.error(f"Database pool error in message_handler: {e}", exc_info=True)
            await update.message.reply_text(
                "Ошибка подключения к базе данных. Попробуйте позже.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
            )
            context.user_data["state"] = STATES["main_menu"]
            await log_analytics(user_id, "message_error", {"error": str(e)})
            return STATES["main_menu"]
                
async def calculate_price_ton(stars: int) -> float:
    """Calculate the price in TON for a given number of stars."""
    if not isinstance(stars, int) or stars <= 0:
        return 0.0
    ton_price = telegram_app.bot_data.get("ton_price_info", {}).get("price", 0.0)
    if ton_price == 0.0:
        await update_ton_price()
        ton_price = telegram_app.bot_data.get("ton_price_info", {}).get("price", 1.0)  # Fallback to 1.0 if still 0
    async with (await ensure_db_pool()) as conn:
        price_usd = await conn.fetchval("SELECT value FROM settings WHERE key = 'price_usd'") or PRICE_USD_PER_50
        markup = await conn.fetchval("SELECT value FROM settings WHERE key = 'markup'") or MARKUP_PERCENTAGE
    usd_price = (stars / 50) * price_usd * (1 + markup / 100)
    ton_price = usd_price / ton_price if ton_price > 0 else usd_price
    return round(ton_price, 2)

async def ton_price_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /tonprice."""
    global tech_break_info
    user_id = update.effective_user.id
    async with (await ensure_db_pool()) as conn:
        is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id)
        is_banned = await conn.fetchval("SELECT is_banned FROM users WHERE user_id = $1", user_id)
        if is_banned:
            text = await get_text("user_banned", support_channel=SUPPORT_CHANNEL)
            await update.message.reply_text(text)
            context.user_data["state"] = STATES["main_menu"]
            await log_analytics(user_id, "ton_price_banned", {})
            return STATES["main_menu"]
        if not is_admin and tech_break_info and tech_break_info["end_time"] > datetime.now(pytz.UTC):
            time_remaining = format_time_remaining(tech_break_info["end_time"])
            text = await get_text(
                "tech_break_active",
                end_time=tech_break_info["end_time"].strftime("%Y-%m-%d %H:%M:%S UTC"),
                minutes_left=time_remaining,
                reason=tech_break_info["reason"]
            )
            await update.message.reply_text(text)
            context.user_data["state"] = STATES["main_menu"]
            await log_analytics(user_id, "ton_price_tech_break", {})
            return STATES["main_menu"]
    
    REQUESTS.labels(endpoint="tonprice").inc()
    with RESPONSE_TIME.labels(endpoint="tonprice").time():
        try:
            if "ton_price_info" not in telegram_app.bot_data or telegram_app.bot_data["ton_price_info"].get("price", 0.0) == 0.0:
                await update_ton_price()
            if "ton_price_info" not in telegram_app.bot_data or telegram_app.bot_data["ton_price_info"].get("price", 0.0) == 0.0:
                await update.message.reply_text("Ошибка получения цены TON. Попробуйте позже.")
                context.user_data["state"] = STATES["main_menu"]
                await log_analytics(user_id, "ton_price_error", {})
                return STATES["main_menu"]
            price = telegram_app.bot_data["ton_price_info"]["price"]
            diff_24h = telegram_app.bot_data["ton_price_info"]["diff_24h"]
            change_text = f"📈 +{diff_24h:.2f}%" if diff_24h >= 0 else f"📉 {diff_24h:.2f}%"
            text = f"💰 Цена TON: ${price:.2f}\nИзменение за 24ч: {change_text}"
            await update.message.reply_text(text)
            context.user_data["state"] = STATES["main_menu"]
            await log_analytics(user_id, "ton_price", {})
            logger.info(f"/tonprice выполнен для user_id={user_id}")
        except Exception as e:
            logger.error(f"Ошибка в /tonprice для user_id={user_id}: {e}", exc_info=True)
            ERRORS.labels(type="tonprice", endpoint="tonprice").inc()
            await update.message.reply_text("Ошибка при получении цены TON. Попробуйте позже.")
            context.user_data["state"] = STATES["main_menu"]
            await log_analytics(user_id, "ton_price_error", {"error": str(e)})
        return STATES["main_menu"]

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors."""
    logger.error(f"Update {update} caused error {context.error}", exc_info=True)
    ERRORS.labels(type="general", endpoint="error_handler").inc()
    if update:
        try:
            user_id = update.effective_user.id if update.effective_user else 0
            await log_analytics(user_id, "error", {"error": str(context.error)})
            if update.message or update.callback_query:
                await (update.message or update.callback_query.message).reply_text(
                    "Произошла ошибка. Пожалуйста, попробуйте снова или свяжитесь с поддержкой: @CheapStarsShop_support"
                )
        except Exception as e:
            logger.error(f"Failed to send error message: {e}")

async def shutdown(app: web.Application):
    """Handle application shutdown."""
    global telegram_app, scheduler
    try:
        if 'scheduler' in globals():
            scheduler.shutdown()
            logger.info("Scheduler shut down")
        if telegram_app:
            await telegram_app.updater.stop()
            await telegram_app.shutdown()
            logger.info("Telegram bot shut down")
        await close_db_pool()
        logger.info("Application shutdown complete")
    except Exception as e:
        logger.error(f"Shutdown failed: {e}", exc_info=True)

async def main():
    """Main entry point for the application."""
    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(loop)  # Ensure consistent event loop

    # Create aiohttp application
    app = web.Application()
    wsgi_handler = WSGIHandler(app_flask)
    app.router.add_route('*', '/{path:.*}', wsgi_handler.handle_request)
    app.router.add_post("/webhook", webhook_handler)
    app.router.add_get("/webhook", lambda request: web.Response(status=405, text="Method Not Allowed"))

    # Configure SSL
    ssl_context = None
    if SSL_CERT_PATH and SSL_KEY_PATH:
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_context.load_cert_chain(SSL_CERT_PATH, SSL_KEY_PATH)
        logger.info("SSL context configured")
    else:
        logger.warning("SSL certificate or key not provided; running without HTTPS")

    # Set up signal handlers
    def handle_shutdown():
        logger.info("Received shutdown signal")
        asyncio.create_task(shutdown(app))

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_shutdown)

    # Start application
    try:
        await startup(app)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(
            runner,
            host='0.0.0.0',
            port=int(os.getenv("PORT", 8443)),
            ssl_context=ssl_context
        )
        logger.info(f"Starting aiohttp server on port {int(os.getenv('PORT', 8443))}")
        await site.start()

        # Keep the application running
        await asyncio.Event().wait()
    except Exception as e:
        logger.error(f"Fatal error in main: {e}", exc_info=True)
        raise
    finally:
        await shutdown(app)
        await runner.cleanup()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Application terminated by user")
    except Exception as e:
        logger.error(f"Fatal error in main: {e}", exc_info=True)
        try:
            if telegram_app and telegram_app.bot:
                asyncio.run(telegram_app.bot.send_message(
                    chat_id=ADMIN_BACKUP_ID,
                    text=f"⚠️ Bot: Fatal error in main: {str(e)}"
                ))
        except Exception as notify_error:
            logger.error(f"Failed to send fatal error notification: {notify_error}", exc_info=True)
        raise
