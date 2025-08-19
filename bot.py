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
        is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id)
        is_banned = await conn.fetchval("SELECT is_banned FROM users WHERE user_id = $1", user_id)
        if is_banned:
            text = await get_text("user_banned", support_channel=SUPPORT_CHANNEL)
            await update.message.reply_text(text)
            return STATES[STATE_MAIN_MENU]
        if not is_admin and tech_break_info and tech_break_info["end_time"] > datetime.now(pytz.UTC):
            time_remaining = format_time_remaining(tech_break_info["end_time"])
            text = await get_text(
                "tech_break_active",
                end_time=tech_break_info["end_time"].strftime("%Y-%m-%d %H:%M:%S UTC"),
                minutes_left=time_remaining,
                reason=tech_break_info["reason"]
            )
            await update.message.reply_text(text)
            return STATES[STATE_MAIN_MENU]
    
    REQUESTS.labels(endpoint="tonprice").inc()
    with RESPONSE_TIME.labels(endpoint="tonprice").time():
        try:
            if "ton_price_info" not in telegram_app.bot_data or telegram_app.bot_data["ton_price_info"].get("price", 0.0) == 0.0:
                await update_ton_price()
            if "ton_price_info" not in telegram_app.bot_data or telegram_app.bot_data["ton_price_info"].get("price", 0.0) == 0.0:
                await update.message.reply_text("Ошибка получения цены TON. Попробуйте позже.")
                return STATES[STATE_MAIN_MENU]
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
        return STATES[STATE_MAIN_MENU]

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
    """Обработчик команды /start."""
    global tech_break_info
    user_id = update.effective_user.id
    username = update.effective_user.username or "Unknown"
    chat_id = update.effective_chat.id
    message_text = update.message.text if update.message else "CallbackQuery: back_to_menu"
    logger.info(f"Вызов /start для user_id={user_id}, message={message_text}")
    
    async with (await ensure_db_pool()) as conn:
        is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id)
        is_banned = await conn.fetchval("SELECT is_banned FROM users WHERE user_id = $1", user_id)
        if is_banned:
            text = await get_text("user_banned", support_channel=SUPPORT_CHANNEL)
            await update.message.reply_text(text)
            return STATES[STATE_MAIN_MENU]
        if not is_admin and tech_break_info and tech_break_info["end_time"] > datetime.now(pytz.UTC):
            time_remaining = format_time_remaining(tech_break_info["end_time"])
            text = await get_text(
                "tech_break_active",
                end_time=tech_break_info["end_time"].strftime("%Y-%m-%d %H:%M:%S UTC"),
                minutes_left=time_remaining,
                reason=tech_break_info["reason"]
            )
            await update.message.reply_text(text)
            return STATES[STATE_MAIN_MENU]
        
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
    return STATES[STATE_MAIN_MENU]

async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик callback-запросов."""
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data
    REQUESTS.labels(endpoint="callback_query").inc()
    with RESPONSE_TIME.labels(endpoint="callback_query").time():
        logger.info(f"Callback query received: user_id={user_id}, data={data}")
        async with (await ensure_db_pool()) as conn:
            is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id) or False
            is_banned = await conn.fetchval("SELECT is_banned FROM users WHERE user_id = $1", user_id) or False
            if is_banned:
                text = await get_text("user_banned", support_channel=SUPPORT_CHANNEL)
                await query.message.reply_text(text)
                await query.answer()
                context.user_data["state"] = STATES["main_menu"]
                return STATES["main_menu"]

            if data == "profile":
                user = await conn.fetchrow(
                    "SELECT stars_bought, referrals, ref_bonus_ton FROM users WHERE user_id = $1", user_id
                )
                stars_bought = user["stars_bought"] if user else 0
                referrals = json.loads(user["referrals"]) if user and user["referrals"] else []
                ref_bonus_ton = user["ref_bonus_ton"] if user else 0.0
                total_stars = await conn.fetchval("SELECT SUM(stars_bought) FROM users") or 0
                text = await get_text(
                    "profile_info",
                    stars_bought=stars_bought,
                    total_stars=total_stars,
                    referrals=len(referrals),
                    ref_bonus_ton=ref_bonus_ton
                )
                await query.message.edit_text(
                    text,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🛒 Купить звезды", callback_data="buy_stars")],
                        [InlineKeyboardButton("🤝 Рефералы", callback_data="referrals")],
                        [InlineKeyboardButton("📜 История транзакций", callback_data="transaction_history")],
                        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]
                    ]),
                    parse_mode="HTML"
                )
                await query.answer()
                context.user_data["state"] = STATES["main_menu"]
                await log_analytics(user_id, "view_profile", {})
                return STATES["main_menu"]

            elif data == "referrals":
                referral_link = f"https://t.me/{(await telegram_app.bot.get_me()).username}?start={user_id}"
                user = await conn.fetchrow(
                    "SELECT referrals, ref_bonus_ton FROM users WHERE user_id = $1", user_id
                )
                referrals = json.loads(user["referrals"]) if user and user["referrals"] else []
                text = await get_text(
                    "referrals_info",
                    referral_link=referral_link,
                    referrals=len(referrals),
                    ref_bonus_ton=user["ref_bonus_ton"] if user else 0.0
                )
                await query.message.edit_text(
                    text,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔗 Поделиться ссылкой", switch_inline_query=referral_link)],
                        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]
                    ]),
                    parse_mode="HTML"
                )
                await query.answer()
                context.user_data["state"] = STATES["main_menu"]
                await log_analytics(user_id, "view_referrals", {"referrals_count": len(referrals)})
                return STATES["main_menu"]

            elif data == "buy_stars":
                await query.message.edit_text(
                    "Введите @username получателя звезд или нажмите кнопку для отправки себе:",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("📍 Отправить себе", callback_data=f"set_recipient_{user_id}")],
                        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]
                    ])
                )
                await query.answer()
                context.user_data["state"] = STATES["buy_stars_recipient"]
                await log_analytics(user_id, "start_buy_stars", {})
                return STATES["buy_stars_recipient"]

            elif data.startswith("set_recipient_"):
                recipient_id = int(data.split("_")[-1])
                try:
                    chat = await telegram_app.bot.get_chat(recipient_id)
                    recipient = chat.username or str(recipient_id)
                except TelegramError:
                    recipient = str(recipient_id)
                context.user_data["buy_data"] = {"recipient": recipient}
                await query.message.edit_text(
                    "Выберите количество звезд или введите свое значение:",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("50", callback_data="set_amount_50"), InlineKeyboardButton("100", callback_data="set_amount_100")],
                        [InlineKeyboardButton("500", callback_data="set_amount_500"), InlineKeyboardButton("1000", callback_data="set_amount_1000")],
                        [InlineKeyboardButton("🔢 Ввести свое", callback_data="set_amount_custom")],
                        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]
                    ])
                )
                await query.answer()
                context.user_data["state"] = STATES["buy_stars_amount"]
                await log_analytics(user_id, "set_recipient", {"recipient": recipient})
                return STATES["buy_stars_amount"]

            elif data in ["set_amount_50", "set_amount_100", "set_amount_500", "set_amount_1000"]:
                amount = int(data.split("_")[-1])
                context.user_data["buy_data"] = context.user_data.get("buy_data", {})
                context.user_data["buy_data"]["amount"] = amount
                recipient = context.user_data["buy_data"].get("recipient", "####")
                payment_method = context.user_data["buy_data"].get("payment_method", "Криптовалютой")
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
                    "Подтвердите параметры покупки:",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
                await query.answer()
                context.user_data["state"] = STATES["buy_stars_confirm"]
                await log_analytics(user_id, "set_amount", {"amount": amount})
                return STATES["buy_stars_confirm"]

            elif data == "set_amount_custom":
                await query.message.edit_text(
                    "Введите количество звезд (целое число):",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
                )
                await query.answer()
                context.user_data["state"] = STATES["buy_stars_amount"]
                await log_analytics(user_id, "select_custom_amount", {})
                return STATES["buy_stars_amount"]

            elif data == "select_payment":
                await query.message.edit_text(
                    "Выберите способ оплаты:",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("Криптовалютой", callback_data="payment_crypto")],
                        [InlineKeyboardButton("Через @CryptoBot", callback_data="payment_cryptobot")],
                        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_confirm")]
                    ])
                )
                await query.answer()
                context.user_data["state"] = STATES["buy_stars_payment"]
                await log_analytics(user_id, "select_payment_method", {})
                return STATES["buy_stars_payment"]

            elif data in ["payment_crypto", "payment_cryptobot"]:
                payment_method = "Криптовалютой" if data == "payment_crypto" else "@CryptoBot"
                context.user_data["buy_data"]["payment_method"] = payment_method
                amount = context.user_data["buy_data"].get("amount", 0)
                recipient = context.user_data["buy_data"].get("recipient", "####")
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
                    "Подтвердите параметры покупки:",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
                await query.answer()
                context.user_data["state"] = STATES["buy_stars_confirm"]
                await log_analytics(user_id, "set_payment_method", {"method": payment_method})
                return STATES["buy_stars_confirm"]

            elif data == "confirm_payment":
                buy_data = context.user_data.get("buy_data", {})
                amount = buy_data.get("amount", 0)
                recipient = buy_data.get("recipient", "####")
                payment_method = buy_data.get("payment_method", "Криптовалютой")
                price_ton = await calculate_price_ton(amount)
                if payment_method == "@CryptoBot" and CRYPTOBOT_API_TOKEN:
                    try:
                        async with aiohttp.ClientSession(timeout=ClientTimeout(total=30)) as session:
                            headers = {"Crypto-Pay-API-Token": CRYPTOBOT_API_TOKEN}
                            async with session.post(
                                CRYPTOBOT_API_URL + "/createInvoice",
                                json={
                                    "amount": price_ton,
                                    "currency": "TON",
                                    "description": f"Покупка {amount} звезд для {recipient}"
                                },
                                headers=headers
                            ) as resp:
                                result = await resp.json()
                                if result.get("ok"):
                                    invoice = result["result"]
                                    context.user_data["invoice_id"] = invoice["invoice_id"]
                                    await query.message.edit_text(
                                        f"Счет создан!\nСумма: {price_ton} TON\nОплатите по ссылке:",
                                        reply_markup=InlineKeyboardMarkup([
                                            [InlineKeyboardButton("Оплатить", url=invoice["pay_url"])],
                                            [InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]
                                        ])
                                    )
                                    await query.answer()
                                    context.user_data["state"] = STATES["buy_stars_payment_check"]
                                    await log_analytics(user_id, "create_invoice", {"amount": amount, "price_ton": price_ton})
                                    return STATES["buy_stars_payment_check"]
                                else:
                                    raise Exception(f"Cryptobot error: {result.get('error')}")
                    except Exception as e:
                        logger.error(f"Ошибка создания счета в Cryptobot: {e}", exc_info=True)
                        ERRORS.labels(type="cryptobot", endpoint="create_invoice").inc()
                        await query.message.edit_text(
                            "Ошибка при создании счета. Попробуйте снова или выберите другой способ оплаты.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
                        )
                        await query.answer()
                        context.user_data["state"] = STATES["main_menu"]
                        await log_analytics(user_id, "create_invoice_failed", {"error": str(e)})
                        return STATES["main_menu"]
                else:
                    wallet_address = TON_WALLET_ADDRESS
                    await query.message.edit_text(
                        f"Оплатите {price_ton} TON на кошелек:\n<code>{wallet_address}</code>\n\nПосле оплаты отправьте хэш транзакции.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]),
                        parse_mode="HTML"
                    )
                    await query.answer()
                    context.user_data["state"] = STATES["buy_stars_payment_check"]
                    await log_analytics(user_id, "request_manual_payment", {"amount": amount, "price_ton": price_ton})
                    return STATES["buy_stars_payment_check"]

            elif data == "transaction_history":
                transactions = await conn.fetch(
                    "SELECT id, amount, recipient, status, timestamp FROM transactions WHERE user_id = $1 ORDER BY timestamp DESC LIMIT 10",
                    user_id
                )
                text_lines = []
                for tx in transactions:
                    status = "✅ Успешно" if tx["status"] == "completed" else "⏳ В ожидании"
                    text_lines.append(
                        f"ID: {tx['id']}, {tx['amount']} звезд для {tx['recipient']}, {status}, {tx['timestamp'].strftime('%Y-%m-%d %H:%M:%S UTC')}"
                    )
                text = "\n".join(text_lines) if text_lines else "История транзакций пуста."
                await query.message.edit_text(
                    text,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]]),
                    parse_mode="HTML"
                )
                await query.answer()
                context.user_data["state"] = STATES["main_menu"]
                await log_analytics(user_id, "view_transaction_history", {"transactions_count": len(transactions)})
                return STATES["main_menu"]

            elif data == "admin_panel" and is_admin:
                return await show_admin_panel(update, context)

            elif data == "admin_stats" and is_admin:
                total_users = await conn.fetchval("SELECT COUNT(*) FROM users")
                total_stars = await conn.fetchval("SELECT SUM(stars_bought) FROM users") or 0
                total_transactions = await conn.fetchval("SELECT COUNT(*) FROM transactions")
                completed_transactions = await conn.fetchval(
                    "SELECT COUNT(*) FROM transactions WHERE status = 'completed'"
                )
                text = await get_text(
                    "admin_stats",
                    total_users=total_users,
                    total_stars=total_stars,
                    total_transactions=total_transactions,
                    completed_transactions=completed_transactions
                )
                await query.message.edit_text(
                    text,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]]),
                    parse_mode="HTML"
                )
                await query.answer()
                context.user_data["state"] = STATES["admin_panel"]
                await log_analytics(user_id, "view_admin_stats", {})
                return STATES["admin_panel"]

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
                    "SELECT user_id, username, stars_bought, is_banned FROM users ORDER BY stars_bought DESC LIMIT 10"
                )
                text_lines = []
                keyboard = []
                for user in users:
                    try:
                        chat = await telegram_app.bot.get_chat(user["user_id"])
                        username = f"@{chat.username}" if chat.username else f"ID {user['user_id']}"
                    except Exception as e:
                        logger.error(f"Failed to fetch username for user_id {user['user_id']}: {e}")
                        username = f"ID {user['user_id']}"
                    status = "🚫 Заблокирован" if user["is_banned"] else "✅ Активен"
                    text_lines.append(f"{username}, Звезды: {user['stars_bought']}, {status}")
                    keyboard.append([InlineKeyboardButton(f"Копировать ID {user['user_id']}", callback_data=f"copy_user_id_{user['user_id']}")])
                keyboard.append([InlineKeyboardButton("🔙 Назад в админ-панель", callback_data="back_to_admin")])
                text = "\n".join(text_lines) if text_lines else "Пользователи не найдены."
                await query.message.edit_text(
                    text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
                await query.answer()
                context.user_data["state"] = STATES["all_users"]
                await log_analytics(user_id, "view_all_users", {"users_count": len(users)})
                return STATES["all_users"]

            elif data.startswith("copy_user_id_") and is_admin:
                user_id_to_copy = data.split("_")[-1]
                await query.message.edit_text(
                    f"ID пользователя: <code>{user_id_to_copy}</code>\nСкопируйте ID и вставьте для редактирования.",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("📋 Все пользователи", callback_data="all_users")],
                        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]
                    ]),
                    parse_mode="HTML"
                )
                await query.answer()
                context.user_data["state"] = STATES["admin_edit_profile"]
                await log_analytics(user_id, "copy_user_id", {"copied_user_id": user_id_to_copy})
                return STATES["admin_edit_profile"]

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

            elif data == "tech_break" and is_admin:
                await query.message.edit_text(
                    "Введите длительность технического перерыва в минутах и причину через пробел:",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                )
                await query.answer()
                context.user_data["state"] = STATES["tech_break"]
                await log_analytics(user_id, "start_tech_break", {})
                return STATES["tech_break"]

            elif data == "bot_settings" and is_admin:
                await query.message.edit_text(
                    "Выберите настройку для изменения:",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("Цена за 50 звезд ($)", callback_data="set_price_usd")],
                        [InlineKeyboardButton("Процент накрутки (%)", callback_data="set_markup")],
                        [InlineKeyboardButton("Реферальный бонус (%)", callback_data="set_ref_bonus")],
                        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]
                    ])
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
                text = await get_text("welcome_message", total_stars=total_stars, user_stars=user_stars)
                keyboard = [
                    [
                        InlineKeyboardButton("📰 Новости", url=NEWS_CHANNEL),
                        InlineKeyboardButton("📞 Поддержка и Отзывы", url=SUPPORT_CHANNEL)
                    ],
                    [
                        InlineKeyboardButton("👤 Профиль", callback_data="profile"),
                        InlineKeyboardButton("🤝 Рефералы", callback_data="referrals")
                    ],
                    [InlineKeyboardButton("🛒 Купить звезды", callback_data="buy_stars")],
                    [InlineKeyboardButton("📜 История транзакций", callback_data="transaction_history")]
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
            is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id) or False
            is_banned = await conn.fetchval("SELECT is_banned FROM users WHERE user_id = $1", user_id) or False
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
                recipient = text.replace("@", "")
                context.user_data["buy_data"] = context.user_data.get("buy_data", {})
                context.user_data["buy_data"]["recipient"] = recipient
                await update.message.reply_text(
                    "Выберите количество звезд или введите свое значение:",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("50", callback_data="set_amount_50"), InlineKeyboardButton("100", callback_data="set_amount_100")],
                        [InlineKeyboardButton("500", callback_data="set_amount_500"), InlineKeyboardButton("1000", callback_data="set_amount_1000")],
                        [InlineKeyboardButton("🔢 Ввести свое", callback_data="set_amount_custom")],
                        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]
                    ])
                )
                context.user_data["state"] = STATES["buy_stars_amount"]
                await log_analytics(user_id, "set_recipient", {"recipient": recipient})
                return STATES["buy_stars_amount"]
            
            elif state == STATES["buy_stars_amount"]:
                try:
                    amount = int(text)
                    if amount <= 0:
                        raise ValueError("Количество звезд должно быть положительным.")
                    context.user_data["buy_data"] = context.user_data.get("buy_data", {})
                    context.user_data["buy_data"]["amount"] = amount
                    recipient = context.user_data["buy_data"].get("recipient", "####")
                    payment_method = context.user_data["buy_data"].get("payment_method", "Криптовалютой")
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
                        "Подтвердите параметры покупки:",
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode="HTML"
                    )
                    context.user_data["state"] = STATES["buy_stars_confirm"]
                    await log_analytics(user_id, "set_amount", {"amount": amount})
                    return STATES["buy_stars_confirm"]
                except ValueError as e:
                    await update.message.reply_text(f"Ошибка: {str(e)}. Пожалуйста, введите целое число.")
                    return STATES["buy_stars_amount"]

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
                            referrals = [int(r) for r in text.split(",") if r.strip()]
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

            elif state == "search_user" and is_admin:
                search_text = text.replace("@", "")
                users = await conn.fetch(
                    "SELECT user_id, username, stars_bought, is_banned FROM users WHERE username ILIKE $1 OR user_id::text = $1 LIMIT 10",
                    f"%{search_text}%"
                )
                text_lines = []
                keyboard = []
                for user in users:
                    try:
                        chat = await telegram_app.bot.get_chat(user["user_id"])
                        username = f"@{chat.username}" if chat.username else f"ID {user['user_id']}"
                    except Exception as e:
                        logger.error(f"Failed to fetch username for user_id {user['user_id']}: {e}")
                        username = f"ID {user['user_id']}"
                    status = "🚫 Заблокирован" if user["is_banned"] else "✅ Активен"
                    text_lines.append(f"{username}, Звезды: {user['stars_bought']}, {status}")
                    keyboard.append([InlineKeyboardButton(f"Копировать ID {user['user_id']}", callback_data=f"copy_user_id_{user['user_id']}")])
                keyboard.append([InlineKeyboardButton("🔙 Назад в админ-панель", callback_data="back_to_admin")])
                text = "\n".join(text_lines) if text_lines else "Пользователи не найдены."
                await update.message.reply_text(
                    text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
                context.user_data["state"] = STATES["all_users"]
                await log_analytics(user_id, "search_user", {"search_text": search_text})
                return STATES["all_users"]

            elif state == STATES["set_db_reminder"] and is_admin:
                try:
                    reminder_date = datetime.strptime(text, "%Y-%m-%d").date()
                    if reminder_date < datetime.now(pytz.UTC).date():
                        raise ValueError("Дата напоминания не может быть в прошлом.")
                    await conn.execute(
                        "INSERT INTO reminders (user_id, reminder_date, reminder_type) VALUES ($1, $2, $3)",
                        user_id, reminder_date, "db_update"
                    )
                    await update.message.reply_text(
                        f"Напоминание установлено на {reminder_date}.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
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

            elif state == STATES["tech_break"] and is_admin:
                try:
                    minutes, reason = text.split(" ", 1)
                    minutes = int(minutes)
                    if minutes <= 0:
                        raise ValueError("Длительность должна быть положительной.")
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
                except ValueError as e:
                    await update.message.reply_text(
                        f"Ошибка: {str(e)}. Введите длительность в минутах и причину через пробел.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    return STATES["tech_break"]

            elif state == STATES["bot_settings"] and is_admin:
                setting_field = context.user_data.get("setting_field")
                try:
                    value = float(text)
                    if value < 0:
                        raise ValueError("Значение не может быть отрицательным.")
                    if setting_field == "price_usd":
                        global PRICE_USD_PER_50
                        PRICE_USD_PER_50 = value
                        await update.message.reply_text(
                            f"Цена за 50 звезд обновлена: ${value:.2f}.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                        )
                        await log_analytics(user_id, "set_price_usd", {"price_usd": value})
                    elif setting_field == "markup":
                        global MARKUP_PERCENTAGE
                        MARKUP_PERCENTAGE = value
                        await update.message.reply_text(
                            f"Процент накрутки обновлен: {value:.2f}%.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                        )
                        await log_analytics(user_id, "set_markup", {"markup": value})
                    elif setting_field == "ref_bonus":
                        global REFERRAL_BONUS_PERCENTAGE
                        REFERRAL_BONUS_PERCENTAGE = value
                        await update.message.reply_text(
                            f"Реферальный бонус обновлен: {value:.2f}%.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                        )
                        await log_analytics(user_id, "set_ref_bonus", {"ref_bonus": value})
                    context.user_data.pop("setting_field", None)
                    context.user_data["state"] = STATES["admin_panel"]
                    return await show_admin_panel(update, context)
                except ValueError as e:
                    await update.message.reply_text(
                        f"Ошибка: {str(e)}. Введите корректное число.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_admin")]])
                    )
                    return STATES["bot_settings"]

            else:
                await update.message.reply_text(
                    "Пожалуйста, используйте кнопки меню для взаимодействия с ботом.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]])
                )
                context.user_data["state"] = STATES["main_menu"]
                return STATES["main_menu"]


async def calculate_price_ton(stars: int) -> float:
    """Calculate the price in TON for a given number of stars."""
    if stars == "####" or not isinstance(stars, int):
        return 0.0
    ton_price = telegram_app.bot_data.get("ton_price_info", {}).get("price", 0.0)
    if ton_price == 0.0:
        await update_ton_price()
        ton_price = telegram_app.bot_data.get("ton_price_info", {}).get("price", 1.0)  # Fallback to 1.0 if still 0
    usd_price = (stars / 50) * PRICE_USD_PER_50 * (1 + MARKUP_PERCENTAGE / 100)
    ton_price = usd_price / ton_price
    return round(ton_price, 2)

async def show_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display the admin panel."""
    query = update.callback_query
    user_id = query.from_user.id if query else update.effective_user.id
    text = await get_text("admin_panel")
    keyboard = [
        [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton("📢 Рассылка", callback_data="broadcast_message")],
        [InlineKeyboardButton("👤 Редактировать профиль", callback_data="admin_edit_profile")],
        [InlineKeyboardButton("📋 Все пользователи", callback_data="all_users")],
        [InlineKeyboardButton("📅 Установить напоминание", callback_data="set_db_reminder")],
        [InlineKeyboardButton("🗑 Удалить напоминание", callback_data="clear_db_reminder")],
        [InlineKeyboardButton("⚠️ Технический перерыв", callback_data="tech_break")],
        [InlineKeyboardButton("⚙️ Настройки бота", callback_data="bot_settings")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu")]
    ]
    if query:
        await query.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML"
        )
        await query.answer()
    else:
        await update.message.reply_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="HTML"
        )
    context.user_data["state"] = STATES["admin_panel"]
    await log_analytics(user_id, "show_admin_panel", {})
    return STATES["admin_panel"]

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
                    "Произошла ошибка. Пожалуйста, попробуйте снова или свяжитесь с поддержкой: @sacoectasy"
                )
        except Exception as e:
            logger.error(f"Failed to send error message: {e}")

async def webhook_handler(request):
    """Handle incoming webhook updates."""
    try:
        update = Update.de_json(await request.json(), telegram_app.bot)
        await telegram_app.process_update(update)
        return web.Response(status=200)
    except Exception as e:
        logger.error(f"Webhook error: {e}", exc_info=True)
        ERRORS.labels(type="webhook", endpoint="webhook_handler").inc()
        return web.Response(status=500)


async def main():
    """Main function to start the bot."""
    global telegram_app
    try:
        await check_environment()
        await init_db()
        telegram_app = (
            ApplicationBuilder()
            .token(BOT_TOKEN)
            .concurrent_updates(True)
            .http_version("1.1")
            .build()
        )
        telegram_app.add_handler(CommandHandler("start", start))
        telegram_app.add_handler(CommandHandler("tonprice", ton_price_command))
        telegram_app.add_handler(CallbackQueryHandler(callback_query_handler))
        telegram_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
        telegram_app.add_handler(MessageHandler(filters.ALL, debug_update))
        telegram_app.add_error_handler(error_handler)

        scheduler = AsyncIOScheduler(timezone="UTC")
        scheduler.add_job(heartbeat_check, "interval", seconds=300, args=[telegram_app])
        scheduler.add_job(check_reminders, "interval", seconds=60)
        scheduler.add_job(backup_db, "interval", hours=24)
        scheduler.add_job(update_ton_price, "interval", minutes=30)
        scheduler.add_job(keep_alive, "interval", minutes=10, args=[telegram_app])
        scheduler.start()

        await telegram_app.initialize()
        await telegram_app.bot.set_webhook(f"{WEBHOOK_URL}/webhook")
        logger.info(f"Bot started with webhook: {WEBHOOK_URL}/webhook")
        
        app = web.Application()
        app.router.add_post("/webhook", webhook_handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", PORT)
        await site.start()
        logger.info(f"Webhook server started on port {PORT}")

        # Start Prometheus metrics server
        start_http_server(9090)
        logger.info("Prometheus metrics server started on port 9090")

        # Keep the bot running
        while True:
            await asyncio.sleep(3600)
    except Exception as e:
        logger.error(f"Fatal error in main: {e}", exc_info=True)
        # Do not close the pool to avoid breaking scheduled tasks
        # await close_db_pool()
        raise
        
if __name__ == "__main__":
    asyncio.run(main())
