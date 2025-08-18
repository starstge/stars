import os
import json
import logging
import asyncio
import aiohttp
from aiohttp import ClientTimeout, web
from urllib.parse import urlparse
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
SUPPORT_CHANNEL = "https://t.me/CheapStarsSupport"
REVIEWS_CHANNEL = "https://t.me/CheapStarsReviews"
NEWS_CHANNEL = "https://t.me/CheapStarsShopNews"
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

# Обновление словаря состояний
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
    STATE_FEEDBACK: 18,
    STATE_SUPPORT_TICKET: 19,
    STATE_BAN_USER: 20,
    STATE_UNBAN_USER: 21
}

# Глобальные переменные
_db_pool = None
_db_pool_lock = asyncio.Lock()
telegram_app = None
transaction_cache = TTLCache(maxsize=1000, ttl=3600)
tech_break_info = {}  # Хранит информацию о техническом перерыве: {"end_time": datetime, "reason": str}

async def debug_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Debug handler to log all incoming updates."""
    logger.info(f"Received update: {update.to_dict()}")
    await log_analytics(
        update.effective_user.id if update.effective_user else 0,
        "debug_update",
        {"update": update.to_dict()}
    )

async def ensure_db_pool():
    """Получение пула соединений с базой данных."""
    global _db_pool
    async with _db_pool_lock:
        if _db_pool is None or _db_pool._closed:
            logger.info("Создание нового пула базы данных")
            if not POSTGRES_URL:
                logger.error("POSTGRES_URL or DATABASE_URL not set")
                raise ValueError("POSTGRES_URL or DATABASE_URL not set")
            try:
                _db_pool = await asyncpg.create_pool(
                    POSTGRES_URL,
                    min_size=1,
                    max_size=10,
                    timeout=30,
                    command_timeout=60,
                    max_inactive_connection_lifetime=300
                )
                logger.info("Пул DB создан успешно")
            except Exception as e:
                logger.error(f"Ошибка создания пула DB: {e}")
                raise
        return _db_pool

async def init_db():
    """Инициализация базы данных."""
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
                    is_banned BOOLEAN DEFAULT FALSE
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS analytics (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    action TEXT,
                    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    data JSONB,
                    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS reminders (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    reminder_date DATE,
                    reminder_type TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    recipient TEXT,
                    amount INTEGER,
                    price_ton FLOAT,
                    payment_method TEXT,
                    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS feedback (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    message TEXT,
                    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS support_tickets (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    issue TEXT,
                    status TEXT DEFAULT 'open',
                    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """)
            columns = await conn.fetch(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'analytics'"
            )
            column_names = [col['column_name'] for col in columns]
            if 'details' in column_names and 'data' not in column_names:
                await conn.execute("""
                    ALTER TABLE analytics RENAME COLUMN details TO data;
                """)
            await conn.execute(
                "INSERT INTO users (user_id, is_admin) VALUES ($1, $2) ON CONFLICT (user_id) DO UPDATE SET is_admin = $2",
                6956377285, True
            )
        logger.info("База данных инициализирована")
    except Exception as e:
        logger.error(f"Ошибка инициализации базы данных: {e}", exc_info=True)
        raise

async def close_db_pool():
    """Закрытие пула соединений."""
    global _db_pool
    async with _db_pool_lock:
        if _db_pool is not None and not _db_pool._closed:
            await _db_pool.close()
            logger.info("Пул DB закрыт")
            _db_pool = None

async def get_text(key: str, **kwargs) -> str:
    """Получение текста из статических шаблонов и форматирование с параметрами."""
    templates = {
        "welcome": "Добро пожаловать в Stars Market! 🎉\nВ нашем боте куплено {total_stars} звезд.\nВы купили {stars_bought} звезд.",
        "referrals": "🤝 Твоя реферальная ссылка: {ref_link}\nРефералов: {ref_count}\nБонус: {ref_bonus_ton} TON",
        "profile": "👤 Профиль:\nЗвезд куплено: {stars_bought}\nРефералов: {ref_count}\nРеферальный бонус: {ref_bonus_ton} TON",
        "buy_success": "✅ Успешная покупка! {stars} звезд отправлено на {recipient}.",
        "buy_prompt": "Введите имя пользователя получателя (с @ или без):",
        "tech_support": "Свяжитесь с поддержкой: https://t.me/CheapStarsSupport\nОтзывы: https://t.me/CheapStarsReviews",
        "news": "Новости канала: https://t.me/CheapStarsShopNews",
        "all_users": "Список всех пользователей:\n{users_list}",
        "top_referrals": "🏆 Топ-10 рефералов:\n{text}",
        "top_purchases": "🏆 Топ-10 покупок:\n{text}",
        "stats": "📊 Статистика:\nВсего пользователей: {total_users}\nВсего куплено звезд: {total_stars}\nВсего рефералов: {total_referrals}",
        "admin_panel": "🔧 Админ-панель:\nВыберите действие:",
        "tech_break_active": "⚠️ Технический перерыв до {end_time} (осталось {minutes_left} минут).\nПричина: {reason}",
        "tech_break_set": "Технический перерыв установлен до {end_time}.\nПричина: {reason}",
        "bot_settings": "⚙️ Настройки бота:\nТекущая цена за 50 звезд: ${price_usd:.2f}\nНакрутка: {markup}%\nРеферальный бонус: {ref_bonus}%",
        "feedback_prompt": "Пожалуйста, напишите ваш отзыв о боте:",
        "feedback_success": "Спасибо за ваш отзыв! Он отправлен на рассмотрение.",
        "transaction_history": "📜 История транзакций:\n{text}",
        "referral_leaderboard": "🏆 Лидеры рефералов:\n{text}",
        "support_ticket_prompt": "Опишите вашу проблему:",
        "support_ticket_success": "Ваш запрос в поддержку отправлен. Номер тикета: {ticket_id}",
        "ban_success": "Пользователь {user_id} заблокирован.",
        "unban_success": "Пользователь {user_id} разблокирован.",
        "user_banned": "Вы заблокированы. Свяжитесь с поддержкой: {support_channel}"
    }
    text = templates.get(key, f"Текст для {key} не задан.")
    try:
        return text.format(**kwargs)
    except KeyError as e:
        logger.error(f"Ошибка форматирования текста для ключа {key}: отсутствует параметр {e}")
        default_kwargs = {k: v for k, v in kwargs.items() if k in text}
        try:
            return text.format(**default_kwargs)
        except KeyError:
            return text

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

async def ton_price_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /tonprice."""
    global tech_break_info
    user_id = update.effective_user.id
    async with (await ensure_db_pool()) as conn:
        # Check if is_banned column exists
        column_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'users' AND column_name = 'is_banned'
            )
        """)
        if not column_exists:
            logger.error("is_banned column does not exist in users table")
            await update.message.reply_text(
                "Произошла ошибка базы данных. Пожалуйста, свяжитесь с поддержкой: @sacoectasy"
            )
            return STATES["main_menu"]
        
        is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id)
        is_banned = await conn.fetchval("SELECT is_banned FROM users WHERE user_id = $1", user_id)
        if is_banned:
            text = await get_text("user_banned", support_channel=SUPPORT_CHANNEL)
            await update.message.reply_text(text)
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
            return STATES["main_menu"]
    
    REQUESTS.labels(endpoint="tonprice").inc()
    with RESPONSE_TIME.labels(endpoint="tonprice").time():
        try:
            if "ton_price_info" not in telegram_app.bot_data or telegram_app.bot_data["ton_price_info"].get("price", 0.0) == 0.0:
                await update_ton_price()
            if "ton_price_info" not in telegram_app.bot_data or telegram_app.bot_data["ton_price_info"].get("price", 0.0) == 0.0:
                await update.message.reply_text("Ошибка получения цены TON. Попробуйте позже.")
                return STATES["main_menu"]
            price = telegram_app.bot_data["ton_price_info"]["price"]
            diff_24h = telegram_app.bot_data["ton_price_info"]["diff_24h"]
            change_text = f"📈 +{diff_24h:.2f}%" if diff_24h >= 0 else f"📉 {diff_24h:.2f}%"
            text = f"💰 Цена TON: ${price:.2f}\nИзменение за 24ч: {change_text}"
            await update.message.reply_text(text)
            await log_analytics(user_id, "ton_price")
            logger.info(f"/tonprice выполнен для user_id={user_id}")
        except Exception as e:
            logger.error(f"Ошибка в /tonprice для user_id={user_id}: {e}", exc_info=True)
            ERRORS.labels(type="tonprice", endpoint="tonprice").inc()
            await update.message.reply_text("Ошибка при получении цены TON. Попробуйте позже.")
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
    """Проверка доступности вебхука."""
    try:
        webhook_info = await telegram_app.bot.get_webhook_info()
        logger.info(f"Webhook info: {webhook_info}")
        if webhook_info.url != f"{WEBHOOK_URL}/webhook":
            logger.warning(f"Webhook URL mismatch: expected {WEBHOOK_URL}/webhook, got {webhook_info.url}")
            await telegram_app.bot.set_webhook(f"{WEBHOOK_URL}/webhook")
            logger.info(f"Webhook reset to {WEBHOOK_URL}/webhook")
        if webhook_info.pending_update_count > 0:
            logger.warning(f"Pending updates: {webhook_info.pending_update_count}")
            await telegram_app.bot.delete_webhook(drop_pending_updates=True)
            await telegram_app.bot.set_webhook(f"{WEBHOOK_URL}/webhook")
            logger.info("Pending updates cleared and webhook reset")
    except Exception as e:
        logger.error(f"Ошибка проверки вебхука: {e}", exc_info=True)
        ERRORS.labels(type="webhook", endpoint="check_webhook").inc()

async def heartbeat_check(app):
    """Проверка работоспособности DB и API."""
    try:
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
    """Обработчик команды /start."""
    global tech_break_info
    user_id = update.effective_user.id
    username = update.effective_user.username or "Unknown"
    chat_id = update.effective_chat.id
    message_text = update.message.text if update.message else "CallbackQuery: back_to_menu"
    logger.info(f"Вызов /start для user_id={user_id}, message={message_text}")
    
    async with (await ensure_db_pool()) as conn:
        # Check if is_banned column exists
        column_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'users' AND column_name = 'is_banned'
            )
        """)
        if not column_exists:
            logger.error("is_banned column does not exist in users table")
            await update.message.reply_text(
                "Произошла ошибка базы данных. Пожалуйста, свяжитесь с поддержкой: @sacoectasy"
            )
            return STATES["main_menu"]
        
        is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id)
        is_banned = await conn.fetchval("SELECT is_banned FROM users WHERE user_id = $1", user_id)
        if is_banned:
            text = await get_text("user_banned", support_channel=SUPPORT_CHANNEL)
            await update.message.reply_text(text)
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
            return STATES["main_menu"]
        
        user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
        if not user:
            await conn.execute(
                """
                INSERT INTO users (user_id, username, stars_bought, ref_bonus_ton, referrals)
                VALUES ($1, $2, 0, 0.0, '[]')
                """,
                user_id, username
            )
            logger.info(f"Создан новый пользователь: user_id={user_id}, username={username}")
            stars_bought = 0
        else:
            stars_bought = user["stars_bought"]
        total_stars = await conn.fetchval("SELECT SUM(stars_bought) FROM users") or 0
        
        args = context.args
        referrer_id = None
        if args and args[0].startswith("ref_"):
            try:
                referrer_id = int(args[0].split("_")[1])
                if referrer_id == user_id:
                    referrer_id = None
            except (IndexError, ValueError):
                logger.warning(f"Некорректный реферальный параметр: {args[0]}")
        
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
                    logger.info(f"Добавлен реферал user_id={user_id} для referrer_id={referrer_id}")
    
    try:
        text = await get_text(
            "welcome",
            username=username,
            stars_bought=stars_bought,
            total_stars=total_stars
        )
    except Exception as e:
        logger.error(f"Ошибка в get_text для welcome: {e}")
        text = f"Добро пожаловать в Stars Market! 🎉\nВы купили {stars_bought} звезд."
    
    keyboard = [
        [
            InlineKeyboardButton("📰 Новости", url=NEWS_CHANNEL),
            InlineKeyboardButton("📞 Поддержка и Отзывы", url="https://t.me/CheapStarsShop_support")
        ],
        [
            InlineKeyboardButton("👤 Профиль", callback_data="profile"),
            InlineKeyboardButton("🤝 Рефералы", callback_data="referrals")
        ],
        [InlineKeyboardButton("🛒 Купить звезды", callback_data="buy_stars")],
        [InlineKeyboardButton("📜 История транзакций", callback_data=TRANSACTION_HISTORY)],
        [InlineKeyboardButton("🏆 Лидеры рефералов", callback_data=REFERRAL_LEADERBOARD)],
        [InlineKeyboardButton("📢 Отправить отзыв", callback_data=FEEDBACK)],
        [InlineKeyboardButton("🆘 Создать тикет поддержки", callback_data=SUPPORT_TICKET)]
    ]
    if is_admin:
        keyboard.append([InlineKeyboardButton("🔧 Админ-панель", callback_data="admin_panel")])
    
    try:
        last_message = telegram_app.bot_data.get(f"last_message_{user_id}")
        if last_message and last_message["chat_id"] and last_message["message_id"]:
            logger.debug(f"Attempting to edit message: chat_id={last_message['chat_id']}, message_id={last_message['message_id']}")
            await telegram_app.bot.edit_message_text(
                text=text,
                chat_id=last_message["chat_id"],
                message_id=last_message["message_id"],
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML"
            )
        else:
            if update.message:
                sent_message = await update.message.reply_text(
                    text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
                telegram_app.bot_data[f"last_message_{user_id}"] = {
                    "chat_id": sent_message.chat_id,
                    "message_id": sent_message.message_id
                }
            else:
                sent_message = await telegram_app.bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
                telegram_app.bot_data[f"last_message_{user_id}"] = {
                    "chat_id": sent_message.chat_id,
                    "message_id": sent_message.message_id
                }
    except BadRequest as e:
        logger.warning(f"Не удалось отредактировать сообщение: {e}")
        if update.message:
            sent_message = await update.message.reply_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML"
            )
            telegram_app.bot_data[f"last_message_{user_id}"] = {
                "chat_id": sent_message.chat_id,
                "message_id": sent_message.message_id
            }
        else:
            sent_message = await telegram_app.bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML"
            )
            telegram_app.bot_data[f"last_message_{user_id}"] = {
                "chat_id": sent_message.chat_id,
                "message_id": sent_message.message_id
            }
    
    await log_analytics(user_id, "start", {"referrer_id": referrer_id})
    context.user_data["state"] = STATE_MAIN_MENU
    logger.info(f"/start успешно обработан для user_id={user_id}")
    return STATES["main_menu"]

async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик callback-запросов."""
    global tech_break_info, PRICE_USD_PER_50, MARKUP_PERCENTAGE, REFERRAL_BONUS_PERCENTAGE
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data
    logger.info(f"Callback query received: user_id={user_id}, data={data}")
    
    try:
        async with (await ensure_db_pool()) as conn:
            # Check if is_banned column exists
            column_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_name = 'users' AND column_name = 'is_banned'
                )
            """)
            if not column_exists:
                logger.error("is_banned column does not exist in users table")
                await query.message.reply_text(
                    "Произошла ошибка базы данных. Пожалуйста, свяжитесь с поддержкой: @sacoectasy"
                )
                await query.answer()
                return STATES["main_menu"]
            
            is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id)
            is_banned = await conn.fetchval("SELECT is_banned FROM users WHERE user_id = $1", user_id)
            if is_banned:
                text = await get_text("user_banned", support_channel=SUPPORT_CHANNEL)
                await query.message.reply_text(text)
                await query.answer()
                return STATES["main_menu"]
            if not is_admin and tech_break_info and tech_break_info["end_time"] > datetime.now(pytz.UTC):
                time_remaining = format_time_remaining(tech_break_info["end_time"])
                text = await get_text(
                    "tech_break_active",
                    end_time=tech_break_info["end_time"].strftime("%Y-%m-%d %H:%M:%S UTC"),
                    minutes_left=time_remaining,
                    reason=tech_break_info["reason"]
                )
                await query.message.reply_text(text)
                await query.answer()
                return STATES["main_menu"]
            
            if data == "buy_stars":
                buy_data = context.user_data.get("buy_data", {})
                recipient = buy_data.get("recipient", "####")
                amount = buy_data.get("amount", "####")
                payment_method = buy_data.get("payment_method", "Криптовалютой")
                price_ton = await calculate_price_ton(amount) if amount != "####" else "0.0"
                keyboard = [
                    [InlineKeyboardButton(f"Пользователь {recipient}", callback_data="select_user")],
                    [InlineKeyboardButton(f"Способ оплаты: {payment_method}", callback_data="select_payment")],
                    [InlineKeyboardButton(f"Количество звезд: {amount}", callback_data="select_amount")],
                    [
                        InlineKeyboardButton(f"Цена: {price_ton} TON", callback_data="price_info"),
                        InlineKeyboardButton("Оплатить", callback_data="confirm_payment")
                    ],
                    [InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]
                ]
                await query.message.edit_text(
                    "Выберите параметры покупки:",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
                context.user_data["state"] = STATE_BUY_STARS_RECIPIENT
                await log_analytics(user_id, "buy_stars_menu", {})
                await query.answer()
                return STATES["buy_stars_recipient"]
            
            elif data == "select_user":
                await query.message.reply_text("Введите имя пользователя (например, @username):")
                context.user_data["state"] = STATE_BUY_STARS_RECIPIENT
                await query.answer()
                return STATES["buy_stars_recipient"]
            
            elif data == "select_payment":
                keyboard = [
                    [InlineKeyboardButton("Криптовалютой", callback_data="set_payment_crypto")],
                    [InlineKeyboardButton("🔙 Назад", callback_data="buy_stars")]
                ]
                await query.message.edit_text(
                    "Выберите способ оплаты:",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
                context.user_data["state"] = STATE_BUY_STARS_PAYMENT_METHOD
                await query.answer()
                return STATES["buy_stars_payment_method"]
            
            elif data == "set_payment_crypto":
                buy_data = context.user_data.get("buy_data", {})
                buy_data["payment_method"] = "Криптовалютой"
                context.user_data["buy_data"] = buy_data
                recipient = buy_data.get("recipient", "####")
                amount = buy_data.get("amount", "####")
                price_ton = await calculate_price_ton(amount) if amount != "####" else "0.0"
                keyboard = [
                    [InlineKeyboardButton(f"Пользователь {recipient}", callback_data="select_user")],
                    [InlineKeyboardButton(f"Способ оплаты: Криптовалютой", callback_data="select_payment")],
                    [InlineKeyboardButton(f"Количество звезд: {amount}", callback_data="select_amount")],
                    [
                        InlineKeyboardButton(f"Цена: {price_ton} TON", callback_data="price_info"),
                        InlineKeyboardButton("Оплатить", callback_data="confirm_payment")
                    ],
                    [InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]
                ]
                await query.message.edit_text(
                    "Выберите параметры покупки:",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
                context.user_data["state"] = STATE_BUY_STARS_RECIPIENT
                await log_analytics(user_id, "set_payment_crypto", {})
                await query.answer()
                return STATES["buy_stars_recipient"]
            
            elif data == "select_amount":
                await query.message.reply_text(
                    "Выберите количество звезд или введите свое значение:",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("50", callback_data="set_amount_50"), InlineKeyboardButton("100", callback_data="set_amount_100")],
                        [InlineKeyboardButton("500", callback_data="set_amount_500"), InlineKeyboardButton("1000", callback_data="set_amount_1000")],
                        [InlineKeyboardButton("🔢 Ввести свое", callback_data="set_amount_custom")],
                        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]
                    ])
                )
                context.user_data["state"] = STATE_BUY_STARS_AMOUNT
                await query.answer()
                return STATES["buy_stars_amount"]
            
            elif data.startswith("set_amount_"):
                try:
                    amount = int(data.split("_")[-1])
                    buy_data = context.user_data.get("buy_data", {})
                    buy_data["amount"] = amount
                    context.user_data["buy_data"] = buy_data
                    recipient = buy_data.get("recipient", "####")
                    payment_method = buy_data.get("payment_method", "Криптовалютой")
                    price_ton = await calculate_price_ton(amount)
                    keyboard = [
                        [InlineKeyboardButton(f"Пользователь {recipient}", callback_data="select_user")],
                        [InlineKeyboardButton(f"Способ оплаты: {payment_method}", callback_data="select_payment")],
                        [InlineKeyboardButton(f"Количество звезд: {amount}", callback_data="select_amount")],
                        [
                            InlineKeyboardButton(f"Цена: {price_ton} TON", callback_data="price_info"),
                            InlineKeyboardButton("Оплатить", callback_data="confirm_payment")
                        ],
                        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]
                    ]
                    await query.message.edit_text(
                        "Выберите параметры покупки:",
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode="HTML"
                    )
                    context.user_data["state"] = STATE_BUY_STARS_RECIPIENT
                    await log_analytics(user_id, "set_amount", {"amount": amount})
                    await query.answer()
                    return STATES["buy_stars_recipient"]
                except ValueError:
                    await query.message.reply_text("Ошибка при выборе количества звезд.")
                    await query.answer()
                    return STATES["buy_stars_amount"]
            
            elif data == "set_amount_custom":
                await query.message.reply_text("Введите количество звезд (например, 250):")
                context.user_data["state"] = STATE_BUY_STARS_AMOUNT
                await query.answer()
                return STATES["buy_stars_amount"]
            
            elif data == "confirm_payment":
                buy_data = context.user_data.get("buy_data", {})
                recipient = buy_data.get("recipient", "####")
                amount = buy_data.get("amount", "####")
                if recipient == "####" or amount == "####":
                    await query.message.reply_text("Пожалуйста, выберите пользователя и количество звезд.")
                    await query.answer()
                    return STATES["buy_stars_recipient"]
                price_ton = await calculate_price_ton(amount)
                keyboard = [
                    [InlineKeyboardButton("Оплатить", callback_data="process_payment")],
                    [InlineKeyboardButton("Проверить оплату", callback_data="check_payment")],
                    [InlineKeyboardButton("🔙 Назад", callback_data="buy_stars")]
                ]
                await query.message.edit_text(
                    f"Подтвердите покупку:\nПользователь: {recipient}\nКоличество звезд: {amount}\nЦена: {price_ton} TON",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
                context.user_data["state"] = STATE_BUY_STARS_CONFIRM
                await log_analytics(user_id, "confirm_payment", {"recipient": recipient, "amount": amount, "price_ton": price_ton})
                await query.answer()
                return STATES["buy_stars_confirm"]
            
            elif data == "process_payment":
                buy_data = context.user_data.get("buy_data", {})
                recipient = buy_data.get("recipient", "####")
                amount = buy_data.get("amount", "####")
                price_ton = float(await calculate_price_ton(amount))
                payment_method = buy_data.get("payment_method", "Криптовалютой")
                logger.info(f"Processing payment for user_id={user_id}, recipient={recipient}, amount={amount}, price={price_ton} TON")
                await conn.execute(
                    """
                    INSERT INTO transactions (user_id, recipient, amount, price_ton, payment_method, timestamp)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    """,
                    user_id, recipient, amount, price_ton, payment_method, datetime.now(pytz.UTC)
                )
                await conn.execute(
                    "UPDATE users SET stars_bought = stars_bought + $1 WHERE user_id = $2",
                    amount, user_id
                )
                await query.message.edit_text(
                    f"Оплата успешно обработана (тестовый режим):\nПользователь: {recipient}\nКоличество звезд: {amount}\nЦена: {price_ton} TON",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
                )
                await log_analytics(user_id, "mock_payment", {"recipient": recipient, "amount": amount, "price_ton": price_ton})
                context.user_data.pop("buy_data", None)
                context.user_data["state"] = STATE_MAIN_MENU
                await query.answer()
                return await start(update, context)
            
            elif data == "check_payment":
                await query.message.reply_text("Проверка оплаты не требуется в тестовом режиме.")
                await query.answer()
                return STATES["buy_stars_confirm"]
            
            elif data == "back_to_menu":
                return await start(update, context)
            
            elif data == "profile":
                user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
                ref_count = len(json.loads(user["referrals"])) if user["referrals"] else 0
                text = await get_text(
                    "profile",
                    user_id=user_id,
                    stars_bought=user["stars_bought"],
                    ref_count=ref_count,
                    ref_bonus_ton=user["ref_bonus_ton"]
                )
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]
                await query.message.edit_text(
                    text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
                context.user_data["state"] = STATE_MAIN_MENU
                await query.answer()
                return STATES["main_menu"]
            
            elif data == "referrals":
                user = await conn.fetchrow("SELECT referrals, ref_bonus_ton FROM users WHERE user_id = $1", user_id)
                referrals = json.loads(user["referrals"]) if user["referrals"] else []
                ref_count = len(referrals)
                ref_bonus_ton = user["ref_bonus_ton"] or 0.0
                referral_link = f"https://t.me/CheapStarsShop_bot?start=ref_{user_id}"
                referred_users = []
                if referrals:
                    for ref_id in referrals[:10]:
                        try:
                            chat = await telegram_app.bot.get_chat(ref_id)
                            username = f"@{chat.username}" if chat.username else f"ID {ref_id}"
                            referred_users.append(username)
                        except Exception as e:
                            logger.error(f"Failed to fetch username for user_id {ref_id}: {e}")
                            referred_users.append(f"ID {ref_id}")
                text = (
                    f"🤝 Ваши рефералы:\n"
                    f"Количество рефералов: {ref_count}\n"
                    f"Реферальная ссылка: <a href='{referral_link}'>Пригласить друга</a>\n"
                    f"Бонус TON: {ref_bonus_ton:.2f}\n"
                    f"Приглашенные пользователи: {', '.join(referred_users) if referred_users else 'Нет рефералов'}"
                )
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]
                await query.message.edit_text(
                    text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
                context.user_data["state"] = STATE_MAIN_MENU
                await log_analytics(user_id, "view_referrals", {"ref_count": ref_count, "ref_bonus_ton": ref_bonus_ton})
                await query.answer()
                return STATES["main_menu"]
            
            elif data == FEEDBACK:
                await query.message.reply_text(
                    await get_text("feedback_prompt"),
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
                )
                context.user_data["state"] = STATE_FEEDBACK
                await query.answer()
                return STATES["feedback"]
            
            elif data == TRANSACTION_HISTORY:
                transactions = await conn.fetch(
                    "SELECT * FROM transactions WHERE user_id = $1 ORDER BY timestamp DESC LIMIT 10",
                    user_id
                )
                text_lines = []
                for tx in transactions:
                    text_lines.append(
                        f"Дата: {tx['timestamp'].strftime('%Y-%m-%d %H:%M:%S UTC')}, "
                        f"Получатель: {tx['recipient']}, "
                        f"Звезды: {tx['amount']}, "
                        f"Цена: {tx['price_ton']:.2f} TON, "
                        f"Метод: {tx['payment_method']}"
                    )
                text = await get_text(
                    "transaction_history",
                    text="\n".join(text_lines) if text_lines else "Нет транзакций."
                )
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]
                await query.message.edit_text(
                    text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
                context.user_data["state"] = STATE_MAIN_MENU
                await log_analytics(user_id, "view_transaction_history", {"transaction_count": len(transactions)})
                await query.answer()
                return STATES["main_menu"]
            
            elif data == REFERRAL_LEADERBOARD:
                users = await conn.fetch(
                    "SELECT user_id, username, referrals FROM users WHERE is_banned = FALSE ORDER BY jsonb_array_length(referrals) DESC LIMIT 10"
                )
                text_lines = []
                for i, user in enumerate(users, 1):
                    ref_count = len(json.loads(user["referrals"])) if user["referrals"] else 0
                    username = user["username"] or f"ID {user['user_id']}"
                    text_lines.append(f"{i}. {username} - {ref_count} рефералов")
                text = await get_text(
                    "referral_leaderboard",
                    text="\n".join(text_lines) if text_lines else "Нет данных о рефералах."
                )
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]
                await query.message.edit_text(
                    text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
                context.user_data["state"] = STATE_MAIN_MENU
                await log_analytics(user_id, "view_referral_leaderboard", {})
                await query.answer()
                return STATES["main_menu"]
            
            elif data == SUPPORT_TICKET:
                await query.message.reply_text(
                    await get_text("support_ticket_prompt"),
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
                )
                context.user_data["state"] = STATE_SUPPORT_TICKET
                await query.answer()
                return STATES["support_ticket"]
            
            elif data == "admin_panel" and is_admin:
                return await show_admin_panel(update, context)
            
            elif data == "admin_stats" and is_admin:
                total_users = await conn.fetchval("SELECT COUNT(*) FROM users WHERE is_banned = FALSE")
                total_stars = await conn.fetchval("SELECT SUM(stars_bought) FROM users WHERE is_banned = FALSE") or 0
                total_referrals = await conn.fetchval("SELECT SUM(jsonb_array_length(referrals)) FROM users WHERE is_banned = FALSE") or 0
                text = await get_text(
                    "stats",
                    total_users=total_users,
                    total_stars=total_stars,
                    total_referrals=total_referrals
                )
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]]
                await query.message.edit_text(
                    text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
                context.user_data["state"] = STATE_ADMIN_PANEL
                await log_analytics(user_id, "view_admin_stats", {})
                await query.answer()
                return STATES["admin_panel"]
            
            elif data == "broadcast_message" and is_admin:
                await query.message.reply_text(
                    "Введите текст для рассылки:",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                )
                context.user_data["state"] = STATE_ADMIN_BROADCAST
                await query.answer()
                return STATES["admin_broadcast"]
            
            elif data == "confirm_broadcast" and is_admin:
                broadcast_text = context.user_data.get("broadcast_text")
                if not broadcast_text:
                    await query.message.reply_text(
                        "Текст рассылки не задан. Пожалуйста, введите текст.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    await query.answer()
                    return STATES["admin_broadcast"]
                success_count, failed_count = await broadcast_message_to_users(broadcast_text)
                await query.message.edit_text(
                    f"Рассылка отправлена {success_count} пользователям, не доставлено: {failed_count}.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                )
                context.user_data.pop("broadcast_text", None)
                context.user_data["state"] = STATE_ADMIN_PANEL
                await log_analytics(user_id, "send_broadcast", {"success_count": success_count, "failed_count": failed_count})
                await query.answer()
                return await show_admin_panel(update, context)
            
            elif data == "cancel_broadcast" and is_admin:
                context.user_data.pop("broadcast_text", None)
                await query.message.edit_text(
                    "Рассылка отменена.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                )
                context.user_data["state"] = STATE_ADMIN_PANEL
                await query.answer()
                return await show_admin_panel(update, context)
            
            elif data == "admin_edit_profile" and is_admin:
                await query.message.reply_text(
                    "Введите ID пользователя для редактирования профиля:",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("📋 Все пользователи", callback_data="all_users")],
                        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]
                    ])
                )
                context.user_data["state"] = STATE_ADMIN_EDIT_PROFILE
                await query.answer()
                return STATES["admin_edit_profile"]
            
            elif data == "edit_profile_stars" and is_admin:
                context.user_data["edit_profile_field"] = "stars_bought"
                await query.message.reply_text(
                    "Введите новое количество звезд для пользователя:",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                )
                context.user_data["state"] = STATE_ADMIN_EDIT_PROFILE
                await query.answer()
                return STATES["admin_edit_profile"]
            
            elif data == "edit_profile_referrals" and is_admin:
                context.user_data["edit_profile_field"] = "referrals"
                await query.message.reply_text(
                    "Введите IDs рефералов через запятую (например: 123,456,789):",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                )
                context.user_data["state"] = STATE_ADMIN_EDIT_PROFILE
                await query.answer()
                return STATES["admin_edit_profile"]
            
            elif data == "edit_profile_ref_bonus" and is_admin:
                context.user_data["edit_profile_field"] = "ref_bonus_ton"
                await query.message.reply_text(
                    "Введите новый реферальный бонус (TON):",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                )
                context.user_data["state"] = STATE_ADMIN_EDIT_PROFILE
                await query.answer()
                return STATES["admin_edit_profile"]
            
            elif data == "all_users" and is_admin:
                page = context.user_data.get("user_list_page", 0)
                users = await conn.fetch("SELECT user_id, username, stars_bought, is_banned FROM users ORDER BY user_id LIMIT 10 OFFSET $1", page * 10)
                total_users = await conn.fetchval("SELECT COUNT(*) FROM users")
                text = ["Список пользователей (до 10):"]
                keyboard = []
                for user in users:
                    try:
                        chat = await telegram_app.bot.get_chat(user["user_id"])
                        username = f"@{chat.username}" if chat.username else f"ID {user['user_id']}"
                    except Exception as e:
                        logger.error(f"Failed to fetch username for user_id {user['user_id']}: {e}")
                        username = f"ID {user['user_id']}"
                    status = "🚫 Заблокирован" if user["is_banned"] else "✅ Активен"
                    text.append(f"{username}, Звезды: {user['stars_bought']}, {status}")
                    keyboard.append([InlineKeyboardButton(f"Копировать ID {user['user_id']}", callback_data=f"copy_user_id_{user['user_id']}")])
                keyboard.append([InlineKeyboardButton("🔍 Поиск по имени", callback_data="search_user")])
                if total_users > (page + 1) * 10:
                    keyboard.append([InlineKeyboardButton("➡️ Дальше", callback_data="next_user_page")])
                if page > 0:
                    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="prev_user_page")])
                keyboard.append([InlineKeyboardButton("🔙 Назад в админ-панель", callback_data="back_to_admin")])
                await query.message.edit_text(
                    "\n".join(text),
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
                context.user_data["state"] = STATE_ALL_USERS
                await log_analytics(user_id, "view_all_users", {"page": page})
                await query.answer()
                return STATES["all_users"]
            
            elif data == "search_user" and is_admin:
                await query.message.reply_text(
                    "Введите имя пользователя для поиска (например, @username):",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                )
                context.user_data["state"] = "search_user"
                await query.answer()
                return STATES["all_users"]
            
            elif data.startswith("copy_user_id_") and is_admin:
                user_id_to_copy = data.split("_")[-1]
                await query.message.reply_text(
                    f"ID {user_id_to_copy} скопирован в буфер обмена.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="all_users")]])
                )
                await query.answer(text=f"ID {user_id_to_copy}")
                return STATES["all_users"]
            
            elif data == "next_user_page" and is_admin:
                context.user_data["user_list_page"] = context.user_data.get("user_list_page", 0) + 1
                return await callback_query_handler(update, context)
            
            elif data == "prev_user_page" and is_admin:
                context.user_data["user_list_page"] = max(0, context.user_data.get("user_list_page", 0) - 1)
                return await callback_query_handler(update, context)
            
            elif data == "set_db_reminder" and is_admin:
                await query.message.reply_text(
                    "Введите дату напоминания в формате гггг-мм-дд (например, 2025-08-18):",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                )
                context.user_data["state"] = STATE_SET_DB_REMINDER
                await query.answer()
                return STATES["set_db_reminder"]
            
            elif data == "clear_db_reminder" and is_admin:
                await conn.execute(
                    "DELETE FROM reminders WHERE user_id = $1 AND reminder_type = $2",
                    user_id, "db_update"
                )
                await query.message.edit_text(
                    "Напоминание об обновлении БД удалено.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                )
                context.user_data["state"] = STATE_ADMIN_PANEL
                await log_analytics(user_id, "clear_db_reminder", {})
                await query.answer()
                return await show_admin_panel(update, context)
            
            elif data == "tech_break" and is_admin:
                await query.message.reply_text(
                    "Введите длительность технического перерыва в минутах и причину через пробел (например: 60 Обновление сервера):",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                )
                context.user_data["state"] = STATE_TECH_BREAK
                await query.answer()
                return STATES["tech_break"]
            
            elif data == "bot_settings" and is_admin:
                keyboard = [
                    [InlineKeyboardButton(f"Цена за 50 звезд ({PRICE_USD_PER_50:.2f} USD)", callback_data="set_price_usd")],
                    [InlineKeyboardButton(f"Процент накрутки ({MARKUP_PERCENTAGE:.2f}%)", callback_data="set_markup")],
                    [InlineKeyboardButton(f"Реферальный бонус ({REFERRAL_BONUS_PERCENTAGE:.2f}%)", callback_data="set_ref_bonus")],
                    [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]
                ]
                await query.message.edit_text(
                    "Выберите параметр для настройки:",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
                context.user_data["state"] = STATE_BOT_SETTINGS
                await query.answer()
                return STATES["bot_settings"]
            
            elif data in ["set_price_usd", "set_markup", "set_ref_bonus"] and is_admin:
                context.user_data["setting_field"] = data.replace("set_", "")
                await query.message.reply_text(
                    f"Введите новое значение для {context.user_data['setting_field']} (число):",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                )
                context.user_data["state"] = STATE_BOT_SETTINGS
                await query.answer()
                return STATES["bot_settings"]
            
            elif data == BAN_USER and is_admin:
                await query.message.reply_text(
                    "Введите ID пользователя для блокировки:",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("📋 Все пользователи", callback_data="all_users")],
                        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]
                    ])
                )
                context.user_data["state"] = STATE_BAN_USER
                await query.answer()
                return STATES["ban_user"]
            
            elif data == UNBAN_USER and is_admin:
                await query.message.reply_text(
                    "Введите ID пользователя для разблокировки:",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("📋 Все пользователи", callback_data="all_users")],
                        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]
                    ])
                )
                context.user_data["state"] = STATE_UNBAN_USER
                await query.answer()
                return STATES["unban_user"]
            
            elif data == "back_to_admin" and is_admin:
                return await show_admin_panel(update, context)
            
            elif data == "support_reviews":
                text = await get_text("tech_support")
                await query.message.edit_text(
                    text,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]),
                    parse_mode="HTML"
                )
                context.user_data["state"] = STATE_MAIN_MENU
                await query.answer()
                return STATES["main_menu"]
            
            else:
                await query.message.reply_text("Неизвестная команда.")
                await query.answer()
                return STATES["main_menu"]
    except asyncpg.exceptions.InterfaceError as e:
        logger.error(f"Database pool error: {e}")
        await query.message.reply_text("Ошибка базы данных. Пожалуйста, свяжитесь с поддержкой: @sacoectasy")
        await query.answer()
        return STATES["main_menu"]

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений."""
    global tech_break_info, PRICE_USD_PER_50, MARKUP_PERCENTAGE, REFERRAL_BONUS_PERCENTAGE
    user_id = update.effective_user.id
    text = update.message.text.strip()
    state = context.user_data.get("state", STATES["main_menu"])
    REQUESTS.labels(endpoint="message").inc()
    with RESPONSE_TIME.labels(endpoint="message").time():
        logger.info(f"Message received: user_id={user_id}, text={text}, state={state}")
        async with (await ensure_db_pool()) as conn:
            # Check if is_banned column exists
            column_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_name = 'users' AND column_name = 'is_banned'
                )
            """)
            if not column_exists:
                logger.error("is_banned column does not exist in users table")
                await update.message.reply_text(
                    "Произошла ошибка базы данных. Пожалуйста, свяжитесь с поддержкой: @sacoectasy"
                )
                return STATES["main_menu"]
            
            is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id)
            is_banned = await conn.fetchval("SELECT is_banned FROM users WHERE user_id = $1", user_id)
            if is_banned:
                text = await get_text("user_banned", support_channel=SUPPORT_CHANNEL)
                await update.message.reply_text(text)
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
                return STATES["main_menu"]

            if state == STATES["buy_stars_recipient"]:
                recipient = text.replace("@", "").strip()
                if not recipient:
                    await update.message.reply_text("Пожалуйста, введите корректное имя пользователя.")
                    return state
                buy_data = context.user_data.get("buy_data", {})
                buy_data["recipient"] = recipient
                context.user_data["buy_data"] = buy_data
                amount = buy_data.get("amount", "####")
                payment_method = buy_data.get("payment_method", "Криптовалютой")
                price_ton = await calculate_price_ton(amount) if amount != "####" else "0.0"
                keyboard = [
                    [InlineKeyboardButton(f"Пользователь @{recipient}", callback_data="select_user")],
                    [InlineKeyboardButton(f"Способ оплаты: {payment_method}", callback_data="select_payment")],
                    [InlineKeyboardButton(f"Количество звезд: {amount}", callback_data="select_amount")],
                    [
                        InlineKeyboardButton(f"Цена: {price_ton} TON", callback_data="price_info"),
                        InlineKeyboardButton("Оплатить", callback_data="confirm_payment")
                    ],
                    [InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]
                ]
                await update.message.reply_text(
                    "Выберите параметры покупки:",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
                context.user_data["state"] = STATES["buy_stars_recipient"]
                await log_analytics(user_id, "set_recipient", {"recipient": recipient})
                return STATES["buy_stars_recipient"]

            elif state == STATES["buy_stars_amount"]:
                try:
                    amount = int(text)
                    if amount < 1:
                        raise ValueError("Количество звезд должно быть больше 0")
                    buy_data = context.user_data.get("buy_data", {})
                    buy_data["amount"] = amount
                    context.user_data["buy_data"] = buy_data
                    recipient = buy_data.get("recipient", "####")
                    payment_method = buy_data.get("payment_method", "Криптовалютой")
                    price_ton = await calculate_price_ton(amount)
                    keyboard = [
                        [InlineKeyboardButton(f"Пользователь {recipient}", callback_data="select_user")],
                        [InlineKeyboardButton(f"Способ оплаты: {payment_method}", callback_data="select_payment")],
                        [InlineKeyboardButton(f"Количество звезд: {amount}", callback_data="select_amount")],
                        [
                            InlineKeyboardButton(f"Цена: {price_ton} TON", callback_data="price_info"),
                            InlineKeyboardButton("Оплатить", callback_data="confirm_payment")
                        ],
                        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]
                    ]
                    await update.message.reply_text(
                        "Выберите параметры покупки:",
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode="HTML"
                    )
                    context.user_data["state"] = STATES["buy_stars_recipient"]
                    await log_analytics(user_id, "set_custom_amount", {"amount": amount})
                    return STATES["buy_stars_recipient"]
                except ValueError:
                    await update.message.reply_text("Пожалуйста, введите корректное число звезд.")
                    return state

            elif state == STATES["admin_broadcast"] and is_admin:
                context.user_data["broadcast_text"] = text
                keyboard = [
                    [InlineKeyboardButton("✅ Отправить", callback_data="confirm_broadcast")],
                    [InlineKeyboardButton("❌ Отменить", callback_data="cancel_broadcast")]
                ]
                await update.message.reply_text(
                    f"Подтвердите рассылку сообщения:\n\n{text}",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
                context.user_data["state"] = STATES["admin_broadcast"]
                await log_analytics(user_id, "set_broadcast_text", {"text": text})
                return STATES["admin_broadcast"]

            elif state == STATES["admin_edit_profile"] and is_admin:
                field = context.user_data.get("edit_profile_field")
                target_user_id = context.user_data.get("edit_user_id")
                if not target_user_id:
                    try:
                        target_user_id = int(text)
                        user_exists = await conn.fetchrow("SELECT user_id FROM users WHERE user_id = $1", target_user_id)
                        if not user_exists:
                            await update.message.reply_text("Пользователь не найден.")
                            return state
                        context.user_data["edit_user_id"] = target_user_id
                        keyboard = [
                            [InlineKeyboardButton("Звезды", callback_data="edit_profile_stars")],
                            [InlineKeyboardButton("Рефералы", callback_data="edit_profile_referrals")],
                            [InlineKeyboardButton("Реф. бонус", callback_data="edit_profile_ref_bonus")],
                            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]
                        ]
                        await update.message.reply_text(
                            "Выберите поле для редактирования:",
                            reply_markup=InlineKeyboardMarkup(keyboard)
                        )
                        return state
                    except ValueError:
                        await update.message.reply_text("Пожалуйста, введите корректный ID пользователя.")
                        return state
                if field == "stars_bought":
                    try:
                        stars = int(text)
                        if stars < 0:
                            raise ValueError("Звезды не могут быть отрицательными")
                        await conn.execute(
                            "UPDATE users SET stars_bought = $1 WHERE user_id = $2",
                            stars, target_user_id
                        )
                        await update.message.reply_text(
                            f"Звезды пользователя {target_user_id} обновлены: {stars}",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                        )
                        context.user_data.pop("edit_profile_field", None)
                        context.user_data.pop("edit_user_id", None)
                        context.user_data["state"] = STATES["admin_panel"]
                        await log_analytics(user_id, "edit_user_stars", {"target_user_id": target_user_id, "stars": stars})
                        return await show_admin_panel(update, context)
                    except ValueError:
                        await update.message.reply_text("Пожалуйста, введите корректное число звезд.")
                        return state
                elif field == "referrals":
                    try:
                        ref_ids = [int(r) for r in text.split(",") if r.strip()]
                        for ref_id in ref_ids:
                            user_exists = await conn.fetchrow("SELECT user_id FROM users WHERE user_id = $1", ref_id)
                            if not user_exists:
                                await update.message.reply_text(f"Пользователь с ID {ref_id} не найден.")
                                return state
                        await conn.execute(
                            "UPDATE users SET referrals = $1 WHERE user_id = $2",
                            json.dumps(ref_ids), target_user_id
                        )
                        await update.message.reply_text(
                            f"Рефералы пользователя {target_user_id} обновлены.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                        )
                        context.user_data.pop("edit_profile_field", None)
                        context.user_data.pop("edit_user_id", None)
                        context.user_data["state"] = STATES["admin_panel"]
                        await log_analytics(user_id, "edit_user_referrals", {"target_user_id": target_user_id, "referrals": ref_ids})
                        return await show_admin_panel(update, context)
                    except ValueError:
                        await update.message.reply_text("Пожалуйста, введите корректные ID рефералов через запятую.")
                        return state
                elif field == "ref_bonus_ton":
                    try:
                        bonus = float(text)
                        if bonus < 0:
                            raise ValueError("Бонус не может быть отрицательным")
                        await conn.execute(
                            "UPDATE users SET ref_bonus_ton = $1 WHERE user_id = $2",
                            bonus, target_user_id
                        )
                        await update.message.reply_text(
                            f"Реферальный бонус пользователя {target_user_id} обновлен: {bonus} TON",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                        )
                        context.user_data.pop("edit_profile_field", None)
                        context.user_data.pop("edit_user_id", None)
                        context.user_data["state"] = STATES["admin_panel"]
                        await log_analytics(user_id, "edit_user_ref_bonus", {"target_user_id": target_user_id, "bonus": bonus})
                        return await show_admin_panel(update, context)
                    except ValueError:
                        await update.message.reply_text("Пожалуйста, введите корректное число для бонуса.")
                        return state

            elif state == STATES["search_user"] and is_admin:
                search_text = text.replace("@", "").strip()
                users = await conn.fetch(
                    "SELECT user_id, username, stars_bought, is_banned FROM users WHERE username ILIKE $1 LIMIT 10",
                    f"%{search_text}%"
                )
                text_lines = []
                keyboard = []
                for user in users:
                    try:
                        chat = await telegram_app.bot.get_chat(user["user_id"])
                        username = f"@{chat.username}" if chat.username else f"ID {user['user_id']}"
                    except Exception:
                        username = f"ID {user['user_id']}"
                    status = "🚫 Заблокирован" if user["is_banned"] else "✅ Активен"
                    text_lines.append(f"{username}, Звезды: {user['stars_bought']}, {status}")
                    keyboard.append([InlineKeyboardButton(f"Копировать ID {user['user_id']}", callback_data=f"copy_user_id_{user['user_id']}")])
                keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="all_users")])
                await update.message.reply_text(
                    "\n".join(text_lines) if text_lines else "Пользователи не найдены.",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
                context.user_data["state"] = STATES["all_users"]
                await log_analytics(user_id, "search_user", {"search_text": search_text})
                return STATES["all_users"]

            elif state == STATES["set_db_reminder"] and is_admin:
                try:
                    reminder_date = datetime.strptime(text, "%Y-%m-%d").date()
                    await conn.execute(
                        "INSERT INTO reminders (user_id, reminder_date, reminder_type) VALUES ($1, $2, $3)",
                        user_id, reminder_date, "db_update"
                    )
                    await update.message.reply_text(
                        f"Напоминание об обновлении БД установлено на {reminder_date}.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    context.user_data["state"] = STATES["admin_panel"]
                    await log_analytics(user_id, "set_db_reminder", {"reminder_date": str(reminder_date)})
                    return await show_admin_panel(update, context)
                except ValueError:
                    await update.message.reply_text(
                        "Пожалуйста, введите дату в формате гггг-мм-дд (например, 2025-08-18).",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    return state

            elif state == STATES["tech_break"] and is_admin:
                try:
                    minutes, reason = text.split(" ", 1)
                    minutes = int(minutes)
                    if minutes <= 0:
                        raise ValueError("Длительность должна быть больше 0")
                    tech_break_info["end_time"] = datetime.now(pytz.UTC) + timedelta(minutes=minutes)
                    tech_break_info["reason"] = reason
                    text = await get_text(
                        "tech_break_set",
                        end_time=tech_break_info["end_time"].strftime("%Y-%m-%d %H:%M:%S UTC"),
                        reason=reason
                    )
                    await update.message.reply_text(
                        text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    context.user_data["state"] = STATES["admin_panel"]
                    await log_analytics(user_id, "set_tech_break", {"minutes": minutes, "reason": reason})
                    return await show_admin_panel(update, context)
                except ValueError:
                    await update.message.reply_text(
                        "Пожалуйста, введите длительность в минутах и причину через пробел (например: 60 Обновление сервера).",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    return state

            elif state == STATES["bot_settings"] and is_admin:
                field = context.user_data.get("setting_field")
                try:
                    value = float(text)
                    if value < 0:
                        raise ValueError("Значение не может быть отрицательным")
                    if field == "price_usd":
                        global PRICE_USD_PER_50
                        PRICE_USD_PER_50 = value
                        await update.message.reply_text(
                            f"Цена за 50 звезд установлена: ${value:.2f}",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                        )
                    elif field == "markup":
                        global MARKUP_PERCENTAGE
                        MARKUP_PERCENTAGE = value
                        await update.message.reply_text(
                            f"Процент накрутки установлен: {value:.2f}%",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                        )
                    elif field == "ref_bonus":
                        global REFERRAL_BONUS_PERCENTAGE
                        REFERRAL_BONUS_PERCENTAGE = value
                        await update.message.reply_text(
                            f"Реферальный бонус установлен: {value:.2f}%",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                        )
                    context.user_data.pop("setting_field", None)
                    context.user_data["state"] = STATES["admin_panel"]
                    await log_analytics(user_id, f"set_{field}", {"value": value})
                    return await show_admin_panel(update, context)
                except ValueError:
                    await update.message.reply_text(
                        f"Пожалуйста, введите корректное число для {field}.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    return state

            elif state == STATES["feedback"]:
                await conn.execute(
                    "INSERT INTO feedback (user_id, message, timestamp) VALUES ($1, $2, $3)",
                    user_id, text, datetime.now(pytz.UTC)
                )
                await update.message.reply_text(
                    await get_text("feedback_success"),
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
                )
                context.user_data["state"] = STATES["main_menu"]
                await log_analytics(user_id, "submit_feedback", {"message": text})
                return await start(update, context)

            elif state == STATES["support_ticket"]:
                ticket_id = await conn.fetchval(
                    "INSERT INTO support_tickets (user_id, issue, status, timestamp) VALUES ($1, $2, $3, $4) RETURNING id",
                    user_id, text, "open", datetime.now(pytz.UTC)
                )
                await update.message.reply_text(
                    await get_text("support_ticket_success", ticket_id=ticket_id),
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
                )
                context.user_data["state"] = STATES["main_menu"]
                await log_analytics(user_id, "submit_support_ticket", {"ticket_id": ticket_id, "issue": text})
                return await start(update, context)

            elif state == STATES["ban_user"] and is_admin:
                try:
                    target_user_id = int(text)
                    user_exists = await conn.fetchrow("SELECT user_id, is_banned FROM users WHERE user_id = $1", target_user_id)
                    if not user_exists:
                        await update.message.reply_text(
                            "Пользователь не найден.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                        )
                        return state
                    if user_exists["is_banned"]:
                        await update.message.reply_text(
                            f"Пользователь {target_user_id} уже заблокирован.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                        )
                        return state
                    await conn.execute(
                        "UPDATE users SET is_banned = TRUE WHERE user_id = $1",
                        target_user_id
                    )
                    await update.message.reply_text(
                        await get_text("ban_success", user_id=target_user_id),
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    context.user_data["state"] = STATES["admin_panel"]
                    await log_analytics(user_id, "ban_user", {"target_user_id": target_user_id})
                    return await show_admin_panel(update, context)
                except ValueError:
                    await update.message.reply_text(
                        "Пожалуйста, введите корректный ID пользователя.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    return state

            elif state == STATES["unban_user"] and is_admin:
                try:
                    target_user_id = int(text)
                    user_exists = await conn.fetchrow("SELECT user_id, is_banned FROM users WHERE user_id = $1", target_user_id)
                    if not user_exists:
                        await update.message.reply_text(
                            "Пользователь не найден.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                        )
                        return state
                    if not user_exists["is_banned"]:
                        await update.message.reply_text(
                            f"Пользователь {target_user_id} не заблокирован.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                        )
                        return state
                    await conn.execute(
                        "UPDATE users SET is_banned = FALSE WHERE user_id = $1",
                        target_user_id
                    )
                    await update.message.reply_text(
                        await get_text("unban_success", user_id=target_user_id),
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    context.user_data["state"] = STATES["admin_panel"]
                    await log_analytics(user_id, "unban_user", {"target_user_id": target_user_id})
                    return await show_admin_panel(update, context)
                except ValueError:
                    await update.message.reply_text(
                        "Пожалуйста, введите корректный ID пользователя.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    return state

            else:
                await update.message.reply_text(
                    "Неизвестная команда. Используйте /start для возврата в главное меню."
                )
                context.user_data["state"] = STATES["main_menu"]
                return STATES["main_menu"]

async def show_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отображение админ-панели."""
    user_id = update.effective_user.id
    async with (await ensure_db_pool()) as conn:
        is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id)
        if not is_admin:
            await update.effective_message.reply_text("Доступ запрещен.")
            return STATES["main_menu"]
    keyboard = [
        [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton("📢 Рассылка", callback_data="broadcast_message")],
        [InlineKeyboardButton("👤 Редактировать профиль", callback_data="admin_edit_profile")],
        [InlineKeyboardButton("📋 Все пользователи", callback_data="all_users")],
        [InlineKeyboardButton("⏰ Установить напоминание БД", callback_data="set_db_reminder")],
        [InlineKeyboardButton("🛠 Технический перерыв", callback_data="tech_break")],
        [InlineKeyboardButton("⚙️ Настройки бота", callback_data="bot_settings")],
        [InlineKeyboardButton("🚫 Заблокировать пользователя", callback_data=BAN_USER)],
        [InlineKeyboardButton("✅ Разблокировать пользователя", callback_data=UNBAN_USER)],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]
    ]
    try:
        await update.effective_message.edit_text(
            await get_text("admin_panel"),
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML"
        )
    except BadRequest:
        await update.effective_message.reply_text(
            await get_text("admin_panel"),
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML"
        )
    context.user_data["state"] = STATES["admin_panel"]
    await log_analytics(user_id, "open_admin_panel")
    return STATES["admin_panel"]

async def calculate_price_ton(stars: int) -> float:
    """Расчет цены в TON для заданного количества звезд."""
    try:
        if stars == "####":
            return 0.0
        ton_price = telegram_app.bot_data.get("ton_price_info", {}).get("price", 0.0)
        if ton_price == 0.0:
            await update_ton_price()
            ton_price = telegram_app.bot_data.get("ton_price_info", {}).get("price", 0.0)
            if ton_price == 0.0:
                logger.error("Не удалось получить цену TON")
                return 0.0
        usd_price = (stars / 50) * PRICE_USD_PER_50 * (1 + MARKUP_PERCENTAGE / 100)
        ton_price = usd_price / ton_price
        return round(ton_price, 2)
    except Exception as e:
        logger.error(f"Ошибка расчета цены TON: {e}")
        return 0.0

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ошибок."""
    try:
        logger.error(f"Ошибка: {context.error}", exc_info=True)
        ERRORS.labels(type="general", endpoint="error_handler").inc()
        if update and update.effective_message:
            await update.effective_message.reply_text(
                "Произошла ошибка. Пожалуйста, попробуйте снова или свяжитесь с поддержкой: @sacoectasy"
            )
        if isinstance(context.error, TelegramError):
            await telegram_app.bot.send_message(
                chat_id=ADMIN_BACKUP_ID,
                text=f"⚠️ Ошибка Telegram API: {context.error}"
            )
    except Exception as e:
        logger.error(f"Ошибка в error_handler: {e}", exc_info=True)

async def webhook_handler(request):
    """Обработчик входящих вебхуков."""
    try:
        update = Update.de_json(await request.json(), telegram_app.bot)
        if update:
            await telegram_app.process_update(update)
        return web.Response(status=200)
    except Exception as e:
        logger.error(f"Ошибка обработки вебхука: {e}", exc_info=True)
        ERRORS.labels(type="webhook", endpoint="webhook_handler").inc()
        return web.Response(status=500)

async def main():
    """Основная функция запуска бота."""
    global telegram_app
    try:
        await check_environment()
        await init_db()
        await test_db_connection()

        telegram_app = (
            ApplicationBuilder()
            .token(BOT_TOKEN)
            .read_timeout(30)
            .write_timeout(30)
            .build()
        )

        telegram_app.add_handler(CommandHandler("start", start))
        telegram_app.add_handler(CommandHandler("tonprice", ton_price_command))
        telegram_app.add_handler(CallbackQueryHandler(callback_query_handler))
        telegram_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
        telegram_app.add_handler(MessageHandler(filters.ALL, debug_update))
        telegram_app.add_error_handler(error_handler)

        scheduler = AsyncIOScheduler(timezone=pytz.UTC)
        scheduler.add_job(update_ton_price, "interval", minutes=5, id="update_ton_price")
        scheduler.add_job(check_reminders, "interval", minutes=1, id="check_reminders")
        scheduler.add_job(keep_alive, "interval", minutes=15, args=[telegram_app], id="keep_alive")
        scheduler.add_job(heartbeat_check, "interval", minutes=5, args=[telegram_app], id="heartbeat_check")
        scheduler.add_job(backup_db, "interval", hours=24, id="backup_db")
        scheduler.start()

        await telegram_app.initialize()
        await telegram_app.bot.set_webhook(f"{WEBHOOK_URL}/webhook")
        logger.info(f"Webhook установлен: {WEBHOOK_URL}/webhook")

        app = web.Application()
        app.router.add_post("/webhook", webhook_handler)
        start_http_server(8000)  # Start Prometheus metrics server

        # Use the same event loop for aiohttp and telegram.ext.Application
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', PORT)
        await site.start()
        logger.info(f"aiohttp server started on port {PORT}")

        # Start the telegram application
        await telegram_app.start()
        logger.info("Telegram bot started")

        # Keep the application running
        try:
            await asyncio.Event().wait()  # Keep the event loop running indefinitely
        except asyncio.CancelledError:
            logger.info("Shutting down bot")
    except Exception as e:
        logger.error(f"Ошибка в main: {e}", exc_info=True)
        await telegram_app.bot.send_message(
            chat_id=ADMIN_BACKUP_ID,
            text=f"⚠️ Критическая ошибка при запуске бота: {e}"
        )
        raise
    finally:
        await close_db_pool()
        if telegram_app:
            await telegram_app.stop()
            await telegram_app.shutdown()
        if 'runner' in locals():
            await runner.cleanup()
        logger.info("Бот завершен")

if __name__ == "__main__":
    asyncio.run(main())
