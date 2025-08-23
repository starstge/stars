import os
import pytz
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

async def ensure_db_pool():
    global db_pool
    async with _db_pool_lock:
        if db_pool is None or db_pool._closed:
            try:
                database_url = os.getenv("DATABASE_URL")
                if not database_url:
                    raise ValueError("DATABASE_URL не установлен")
                db_pool = await asyncpg.create_pool(database_url, min_size=1, max_size=10)
                logger.info("Database pool initialized")
            except Exception as e:
                logger.error(f"Ошибка при создании пула базы данных: {e}", exc_info=True)
                raise
    return db_pool

async def close_db_pool():
    global db_pool
    async with _db_pool_lock:
        if db_pool is not None and not db_pool._closed:
            await db_pool.close()
            logger.info("Database pool closed")
            db_pool = None

async def lifespan(app: web.Application):
    global telegram_app
    scheduler = None
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
        scheduler = AsyncIOScheduler(timezone=pytz.UTC)
        scheduler.add_job(update_ton_price, "interval", minutes=5, id="update_ton_price", replace_existing=True, args=[telegram_app])
        scheduler.start()
        logger.info("Scheduler started for TON price updates")
        yield
    except Exception as e:
        logger.error(f"Ошибка в lifespan: {e}", exc_info=True)
        raise
    finally:
        if scheduler is not None:
            scheduler.shutdown()
            logger.info("Scheduler shut down")
        if telegram_app is not None:
            await telegram_app.stop()
            await telegram_app.update_queue.join()
            await telegram_app.shutdown()
            logger.info("Telegram application shut down")
        await close_db_pool()

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

async def update_ton_price(context: ContextTypes.DEFAULT_TYPE):
    try:
        async with aiohttp.ClientSession() as session:
            headers = {"Authorization": f"Bearer {TON_API_KEY}"} if TON_API_KEY else {}
            async with session.get(
                "https://tonapi.io/v2/rates?tokens=ton&currencies=usd",
                headers=headers
            ) as response:
                response.raise_for_status()
                data = await response.json()
                price = data.get("rates", {}).get("TON", {}).get("prices", {}).get("USD")
                diff_24h = data.get("rates", {}).get("TON", {}).get("diff_24h", {}).get("USD", 0.0)
                if price is None:
                    raise ValueError("USD price not found in API response")
        
        context.bot_data["ton_price_info"] = {"price": price, "diff_24h": float(diff_24h)}
        
        async with (await ensure_db_pool()) as conn:
            await conn.execute(
                "INSERT INTO ton_price (price, updated_at) VALUES ($1, $2)",
                price, datetime.now(pytz.UTC)
            )
        logger.info(f"Updated TON price: ${price:.2f}, diff_24h: {diff_24h:.2f}%")
    except Exception as e:
        logger.error(f"Ошибка в update_ton_price: {e}", exc_info=True)

async def ton_price_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    username = update.effective_user.username or str(user_id)
    logger.info(f"TON price command received: user_id={user_id}, username={username}")
    try:
        async with (await ensure_db_pool()) as conn:
            is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id) or False
            if tech_break_info.get("end_time", datetime.min.replace(tzinfo=pytz.UTC)) > datetime.now(pytz.UTC) and not is_admin:
                time_remaining = await format_time_remaining(tech_break_info["end_time"])
                text = await get_text(
                    "tech_break_active",
                    end_time=tech_break_info["end_time"].strftime("%Y-%m-%d %H:%M:%S UTC"),
                    minutes_left=time_remaining,
                    reason=tech_break_info["reason"]
                )
                await update.message.reply_text(text)
                context.user_data["state"] = 0
                await log_analytics(user_id, "ton_price_tech_break", {})
                return 0
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
        logger.error(f"Ошибка в ton_price_command: {e}", exc_info=True)
        await update.message.reply_text(
            "Ошибка при получении цены TON. Попробуйте позже.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
        )
        context.user_data["state"] = 0
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

def setup_handlers(app: Application):
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("tonprice", ton_price_command))
    app.add_handler(CallbackQueryHandler(callback_query_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
    app.add_handler(PreCheckoutQueryHandler(pre_checkout_callback))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment_callback))
    logger.info("Registered Telegram handlers: start, tonprice, callback_query_handler, message_handler, pre_checkout_callback, successful_payment_callback")

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
    try:
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
            if tech_break_info.get("end_time", datetime.min.replace(tzinfo=pytz.UTC)) > datetime.now(pytz.UTC) and not is_admin:
                time_remaining = await format_time_remaining(tech_break_info["end_time"])
                text = await get_text(
                    "tech_break_active",
                    end_time=tech_break_info["end_time"].strftime("%Y-%m-%d %H:%M:%S UTC"),
                    minutes_left=time_remaining,
                    reason=tech_break_info["reason"]
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
    except Exception as e:
        logger.error(f"Ошибка в start: {e}", exc_info=True)
        await update.message.reply_text(
            "Произошла ошибка. Попробуйте снова или обратитесь в поддержку.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
        )
        context.user_data["state"] = 0
        await log_analytics(user_id, "start_error", {"error": str(e)})
        return 0

async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data
    logger.info(f"Callback query received: user_id={user_id}, data={data}")
    
    try:
        async with (await ensure_db_pool()) as conn:
            is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id) or False
            
            if tech_break_info.get("end_time", datetime.min.replace(tzinfo=pytz.UTC)) > datetime.now(pytz.UTC) and not is_admin:
                time_remaining = await format_time_remaining(tech_break_info["end_time"])
                text = await get_text(
                    "tech_break_active",
                    end_time=tech_break_info["end_time"].strftime("%Y-%m-%d %H:%M:%S UTC"),
                    minutes_left=time_remaining,
                    reason=tech_break_info.get("reason", "Не указана")
                )
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]
                new_reply_markup = InlineKeyboardMarkup(new_keyboard)
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                await query.answer()
                context.user_data["state"] = 0
                await log_analytics(user_id, "callback_tech_break", {})
                return 0

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
                await query.answer()
                context.user_data["state"] = 1
                await log_analytics(user_id, "view_profile", {})
                return 1

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
                context.user_data["state"] = 26
                await log_analytics(user_id, "view_profile_transactions", {"page": page})
                return 26

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
                await query.answer()
                context.user_data["state"] = 2
                await log_analytics(user_id, "view_referrals", {})
                return 2

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
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                await query.answer()
                context.user_data["state"] = 12
                await log_analytics(user_id, "view_referral_leaderboard", {})
                return 12

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
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                await query.answer()
                context.user_data["state"] = 13
                await log_analytics(user_id, "view_top_purchases", {})
                return 13

            elif data == "buy_stars":
                recipient = context.user_data.get("recipient", "Не выбран")
                stars = context.user_data.get("stars_amount", "Не выбрано")
                price_ton = await calculate_price_ton(context, int(stars)) if stars and isinstance(stars, str) and stars.isdigit() else None
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
                context.user_data["state"] = 5
                await log_analytics(user_id, "open_buy_stars_payment_method", {})
                return 5

            elif data == "show_price":
                stars = context.user_data.get("stars_amount", "Не выбрано")
                price_ton = await calculate_price_ton(context, int(stars)) if stars and isinstance(stars, str) and stars.isdigit() else None
                price_text = f"~{price_ton:.2f} TON" if price_ton is not None else "Цена не определена"
                await query.answer(text=price_text, show_alert=True)
                return context.user_data["state"]

            elif data == "select_recipient":
                current_text = query.message.text
                text = "Введите имя пользователя (например, @username):"
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="buy_stars")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                await query.answer()
                context.user_data["state"] = 3
                await log_analytics(user_id, "start_select_recipient", {})
                return 3

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
                await query.answer()
                context.user_data["state"] = 22
                await log_analytics(user_id, "open_select_stars_menu", {})
                return 22

            elif data in ["select_stars_100", "select_stars_250", "select_stars_500", "select_stars_1000"]:
                stars = data.split("_")[-1]
                context.user_data["stars_amount"] = stars
                recipient = context.user_data.get("recipient", "Не выбран")
                price_ton = await calculate_price_ton(context, int(stars))
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
                context.user_data["state"] = 5
                await log_analytics(user_id, f"select_stars_{stars}", {"stars": stars})
                return 5

            elif data == "select_stars_custom":
                current_text = query.message.text
                text = "Введите количество звезд (положительное число):"
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="select_stars_menu")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                await query.answer()
                context.user_data["state"] = 23
                await log_analytics(user_id, "start_select_stars_custom", {})
                return 23

            elif data == "proceed_to_payment":
                stars = context.user_data.get("stars_amount")
                recipient = context.user_data.get("recipient")
                if not stars or not recipient or not isinstance(stars, str) or not stars.isdigit():
                    text = "Ошибка: выберите пользователя и количество звезд."
                    price_ton = None
                    price_text = "Цена"
                    if stars and isinstance(stars, str) and stars.isdigit():
                        price_ton = await calculate_price_ton(context, int(stars))
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
                    context.user_data["state"] = 5
                    return 5
                stars = int(stars)
                price_usd = await conn.fetchval("SELECT value FROM settings WHERE key = 'price_usd'") or 0.81
                markup = await conn.fetchval("SELECT value FROM settings WHERE key = 'markup'") or 10.0
                price_usd = (stars / 50) * price_usd * (1 + markup / 100)
                price_ton = await calculate_price_ton(context, stars)
                payload = await generate_payload(user_id)
                invoice_id, pay_url = await create_cryptobot_invoice(price_usd, "TON", user_id, stars, recipient, payload)
                if not pay_url:
                    current_text = query.message.text
                    text = "Ошибка создания платежа. Попробуйте позже."
                    current_reply_markup = query.message.reply_markup
                    new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="buy_stars")]])
                    if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                        await query.message.edit_text(
                            text,
                            reply_markup=new_reply_markup,
                            parse_mode="HTML"
                        )
                    await query.answer()
                    context.user_data["state"] = 5
                    return 5
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
                context.user_data["state"] = 25
                context.user_data["invoice_id"] = invoice_id
                context.user_data["price_ton"] = price_ton
                await log_analytics(user_id, "proceed_to_payment", {"stars": stars, "recipient": recipient, "invoice_id": invoice_id})
                return 25

            elif data.startswith("check_payment_"):
                invoice_id = data.split("_")[-1]
                stars = context.user_data.get("stars_amount")
                recipient = context.user_data.get("recipient")
                price_ton = context.user_data.get("price_ton")
                if not stars or not recipient or not price_ton:
                    current_text = query.message.text
                    text = "Ошибка: данные о покупке отсутствуют. Начните заново."
                    current_reply_markup = query.message.reply_markup
                    new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="buy_stars")]])
                    if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                        await query.message.edit_text(
                            text,
                            reply_markup=new_reply_markup,
                            parse_mode="HTML"
                        )
                    await query.answer()
                    context.user_data["state"] = 5
                    return 5
                price_usd = await conn.fetchval("SELECT value FROM settings WHERE key = 'price_usd'") or 0.81
                markup = await conn.fetchval("SELECT value FROM settings WHERE key = 'markup'") or 10.0
                ref_bonus_percentage = await conn.fetchval("SELECT value FROM settings WHERE key = 'ref_bonus'") or 30.0
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
                current_text = query.message.text
                text = f"Платеж подтвержден!\n{stars} звезд добавлены для {recipient}."
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                await query.answer()
                context.user_data["state"] = 0
                context.user_data.pop("stars_amount", None)
                context.user_data.pop("recipient", None)
                context.user_data.pop("price_ton", None)
                context.user_data.pop("invoice_id", None)
                await log_analytics(user_id, "payment_confirmed_test", {"stars": stars, "recipient": recipient, "currency": "TON"})
                return 0

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
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                await query.answer()
                context.user_data["state"] = 9
                await log_analytics(user_id, "view_admin_stats", {})
                return 9

            elif data == "broadcast_message" and is_admin:
                current_text = query.message.text
                text = "Введите текст для рассылки:"
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                await query.answer()
                context.user_data["state"] = 10
                await log_analytics(user_id, "start_broadcast", {})
                return 10

            elif data == "confirm_broadcast" and is_admin:
                broadcast_text = context.user_data.get("broadcast_text", "")
                if not broadcast_text:
                    current_text = query.message.text
                    text = "Текст рассылки пуст. Введите текст заново."
                    current_reply_markup = query.message.reply_markup
                    new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                        await query.message.edit_text(
                            text,
                            reply_markup=new_reply_markup,
                            parse_mode="HTML"
                        )
                    await query.answer()
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
                current_text = query.message.text
                text = f"Рассылка завершена. Отправлено {success_count} из {len(users)} пользователям."
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                await query.answer()
                context.user_data.pop("broadcast_text", None)
                context.user_data["state"] = 8
                await log_analytics(user_id, "complete_broadcast", {"success_count": success_count, "total_users": len(users)})
                return 8

            elif data == "cancel_broadcast" and is_admin:
                current_text = query.message.text
                text = "Рассылка отменена."
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                await query.answer()
                context.user_data.pop("broadcast_text", None)
                context.user_data["state"] = 8
                await log_analytics(user_id, "cancel_broadcast", {})
                return await show_admin_panel(update, context)

            elif data == "admin_edit_profile" and is_admin:
                current_text = query.message.text
                text = "Введите ID пользователя для редактирования:"
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
                await query.answer()
                context.user_data["state"] = 11
                await log_analytics(user_id, "start_edit_profile", {})
                return 11

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
                current_text = query.message.text
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад в админ-панель", callback_data="back_to_admin")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                await query.answer()
                context.user_data["state"] = 15
                await log_analytics(user_id, "view_all_users", {"users_count": len(users)})
                return 15

            elif data == "edit_profile_stars" and is_admin:
                context.user_data["edit_profile_field"] = "stars_bought"
                current_text = query.message.text
                text = "Введите новое количество звезд:"
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                await query.answer()
                context.user_data["state"] = 11
                await log_analytics(user_id, "start_edit_stars", {})
                return 11

            elif data == "edit_profile_referrals" and is_admin:
                context.user_data["edit_profile_field"] = "referrals"
                current_text = query.message.text
                text = "Введите ID рефералов через запятую:"
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                await query.answer()
                context.user_data["state"] = 11
                await log_analytics(user_id, "start_edit_referrals", {})
                return 11

            elif data == "edit_profile_ref_bonus" and is_admin:
                context.user_data["edit_profile_field"] = "ref_bonus_ton"
                current_text = query.message.text
                text = "Введите новый реферальный бонус (TON):"
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                await query.answer()
                context.user_data["state"] = 11
                await log_analytics(user_id, "start_edit_ref_bonus", {})
                return 11

            elif data == "set_db_reminder" and is_admin:
                current_text = query.message.text
                text = "Введите дату напоминания в формате гггг-мм-дд:"
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                await query.answer()
                context.user_data["state"] = 14
                await log_analytics(user_id, "start_set_db_reminder", {})
                return 14

            elif data == "tech_break" and is_admin:
                current_text = query.message.text
                text = "Введите длительность тех. перерыва (в минутах) и причину (формат: <минуты> <причина>):"
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                await query.answer()
                context.user_data["state"] = 16
                await log_analytics(user_id, "start_tech_break", {})
                return 16

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
                await query.answer()
                context.user_data["state"] = 17
                await log_analytics(user_id, "view_bot_settings", {})
                return 17

            elif data == "edit_price_usd" and is_admin:
                current_text = query.message.text
                text = "Введите новую цену за 50 звезд (USD):"
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="bot_settings")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                await query.answer()
                context.user_data["state"] = 17
                context.user_data["edit_setting"] = "price_usd"
                await log_analytics(user_id, "start_edit_price_usd", {})
                return 17

            elif data == "edit_markup" and is_admin:
                current_text = query.message.text
                text = "Введите новую накрутку (%):"
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="bot_settings")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                await query.answer()
                context.user_data["state"] = 17
                context.user_data["edit_setting"] = "markup"
                await log_analytics(user_id, "start_edit_markup", {})
                return 17

            elif data == "edit_ref_bonus" and is_admin:
                current_text = query.message.text
                text = "Введите новый реферальный бонус (%):"
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="bot_settings")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                await query.answer()
                context.user_data["state"] = 17
                context.user_data["edit_setting"] = "ref_bonus"
                await log_analytics(user_id, "start_edit_ref_bonus", {})
                return 17

            elif data == "ban_user" and is_admin:
                current_text = query.message.text
                text = "Введите ID пользователя для бана:"
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                await query.answer()
                context.user_data["state"] = 20
                await log_analytics(user_id, "start_ban_user", {})
                return 20

            elif data == "unban_user" and is_admin:
                current_text = query.message.text
                text = "Введите ID пользователя для разбана:"
                current_reply_markup = query.message.reply_markup
                new_reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                if current_text != text or str(current_reply_markup) != str(new_reply_markup):
                    await query.message.edit_text(
                        text,
                        reply_markup=new_reply_markup,
                        parse_mode="HTML"
                    )
                await query.answer()
                context.user_data["state"] = 21
                await log_analytics(user_id, "start_unban_user", {})
                return 21

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
                context.user_data["state"] = 0
                await log_analytics(user_id, "back_to_menu", {})
                return 0

            else:
                await query.answer("Неизвестная команда.")
                return context.user_data.get("state", 0)

    except Exception as e:
        logger.error(f"Ошибка в callback_query_handler: {e}", exc_info=True)
        await query.message.edit_text(
            "Произошла ошибка. Попробуйте снова или обратитесь в поддержку.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]),
            parse_mode="HTML"
        )
        await query.answer()
        context.user_data["state"] = 0
        await log_analytics(user_id, "callback_error", {"error": str(e)})
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
                    global tech_break_info
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

async def webhook(request: web.Request) -> web.Response:
    try:
        update = Update.de_json(await request.json(), telegram_app.bot)
        if update:
            await telegram_app.process_update(update)
            REQUESTS.labels(endpoint="webhook").inc()
            logger.info("Webhook processed successfully")
            return web.json_response({"status": "ok"})
        else:
            logger.warning("Invalid webhook update received")
            return web.json_response({"status": "invalid update"}, status=400)
    except Exception as e:
        logger.error(f"Ошибка в webhook: {e}", exc_info=True)
        ERRORS.labels(type="webhook", endpoint="webhook").inc()
        return web.json_response({"status": "error", "message": str(e)}, status=500)

async def health_check(request: web.Request) -> web.Response:
    try:
        async with (await ensure_db_pool()) as conn:
            await conn.fetchval("SELECT 1")
        return web.json_response({"status": "healthy"})
    except Exception as e:
        logger.error(f"Health check failed: {e}", exc_info=True)
        return web.json_response({"status": "unhealthy", "message": str(e)}, status=500)

def main():
    app = web.Application()
    app.cleanup_ctx.append(lifespan)
    app.router.add_post("/webhook", webhook)
    app.router.add_get("/health", health_check)
    wsgi_handler = WSGIHandler(app_flask)
    app.router.add_route("*", "/{path:.*}", wsgi_handler.handle_request)
    start_http_server(8000)
    logger.info("Starting web application")
    web.run_app(app, host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    main()
