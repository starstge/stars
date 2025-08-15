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
from datetime import datetime, timedelta  # Ensure timedelta is imported
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

# Константы callback_data и состояний
BACK_TO_MENU = "back_to_menu"
BACK_TO_ADMIN = "back_to_admin"
PROFILE = "profile"
REFERRALS = "referrals"
BUY_STARS = "buy_stars"
ADMIN_PANEL = "admin_panel"
STATE_MAIN_MENU = "main_menu"
STATE_PROFILE = "profile"
STATE_REFERRALS = "referrals"
STATE_BUY_STARS_RECIPIENT = "buy_stars_recipient"
STATE_BUY_STARS_AMOUNT = "buy_stars_amount"
STATE_BUY_STARS_PAYMENT_METHOD = "buy_stars_payment_method"
STATE_BUY_STARS_CRYPTO_TYPE = "buy_stars_crypto_type"
STATE_BUY_STARS_CONFIRM = "buy_stars_confirm"
STATE_ADMIN_PANEL = "admin_panel"
STATE_ADMIN_STATS = "admin_stats"
STATE_ADMIN_BROADCAST = "admin_broadcast"
STATE_ADMIN_EDIT_PROFILE = "admin_edit_profile"
STATE_TOP_REFERRALS = "top_referrals"
STATE_TOP_PURCHASES = "top_purchases"
STATE_SET_DB_REMINDER = "set_db_reminder"
STATE_ALL_USERS = "all_users"
STATE_TECH_BREAK = "tech_break"
STATE_BOT_SETTINGS = "bot_settings"
EDIT_PROFILE_STARS = "edit_profile_stars"
EDIT_PROFILE_REFERRALS = "edit_profile_referrals"
EDIT_PROFILE_REF_BONUS = "edit_profile_ref_bonus"
PAY_TON_SPACE = "pay_ton_space"
PAY_CRYPTOBOT = "pay_cryptobot"
PAY_CARD = "pay_card"
CHECK_PAYMENT = "check_payment"
BROADCAST_MESSAGE = "broadcast_message"
CONFIRM_BROADCAST = "confirm_broadcast"
CANCEL_BROADCAST = "cancel_broadcast"
BACK_TO_EDIT_PROFILE = "back_to_edit_profile"
SET_PRICE_USD = "set_price_usd"
SET_MARKUP = "set_markup"
SET_REF_BONUS = "set_ref_bonus"
SELECT_USER = "select_user"
SELECT_PAYMENT = "select_payment"
SELECT_AMOUNT = "select_amount"
CONFIRM_PAYMENT = "confirm_payment"
CHECK_PAYMENT = "check_payment"
STATE_BUY_STARS_PAYMENT = "buy_stars_payment"
STATE_BUY_STARS_CONFIRM = "buy_stars_confirm"

# Список всех состояний
STATES = {
    STATE_MAIN_MENU: 0,
    STATE_PROFILE: 1,
    STATE_REFERRALS: 2,
    STATE_BUY_STARS_RECIPIENT: 3,
    STATE_BUY_STARS_AMOUNT: 4,
    STATE_BUY_STARS_PAYMENT_METHOD: 5,
    STATE_BUY_STARS_CRYPTO_TYPE: 6,
    STATE_BUY_STARS_CONFIRM: 7,
    STATE_ADMIN_PANEL: 8,
    STATE_ADMIN_STATS: 9,
    STATE_ADMIN_BROADCAST: 10,
    STATE_ADMIN_EDIT_PROFILE: 11,
    STATE_TOP_REFERRALS: 12,
    STATE_TOP_PURCHASES: 13,
    STATE_SET_DB_REMINDER: 14,
    STATE_ALL_USERS: 15,
    STATE_TECH_BREAK: 16,
    STATE_BOT_SETTINGS: 17
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
                    is_admin BOOLEAN DEFAULT FALSE
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
        "bot_settings": "⚙️ Настройки бота:\nТекущая цена за 50 звезд: ${price_usd:.2f}\nНакрутка: {markup}%\nРеферальный бонус: {ref_bonus}%"
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
            users = await conn.fetch("SELECT user_id, username, stars_bought FROM users")
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
                        [InlineKeyboardButton("👤 Профиль", callback_data=PROFILE), InlineKeyboardButton("🤝 Рефералы", callback_data=REFERRALS)],
                        [InlineKeyboardButton("🛒 Купить звезды", callback_data=BUY_STARS)]
                    ]
                    if user_id == 6956377285:
                        keyboard.append([InlineKeyboardButton("🔧 Админ-панель", callback_data=ADMIN_PANEL)])
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
        users = await conn.fetch("SELECT user_id FROM users")
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
        # Skip technical break check for admin
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
        
        # Register user if not exists
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
            InlineKeyboardButton("👤 Профиль", callback_data=PROFILE),
            InlineKeyboardButton("🤝 Рефералы", callback_data=REFERRALS)
        ],
        [InlineKeyboardButton("🛒 Купить звезды", callback_data=BUY_STARS)]
    ]
    if is_admin:
        keyboard.append([InlineKeyboardButton("🔧 Админ-панель", callback_data=ADMIN_PANEL)])
    
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
    logger.info(f"Callback query received: user_id={user_id}, data={data}")
    
    async with (await ensure_db_pool()) as conn:
        is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id)
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
            return STATES[STATE_MAIN_MENU]
        
        if data == BUY_STARS:
            buy_data = context.user_data.get("buy_data", {})
            recipient = buy_data.get("recipient", "####")
            amount = buy_data.get("amount", "####")
            payment_method = buy_data.get("payment_method", "Криптовалютой")
            price_ton = await calculate_price_ton(amount) if amount != "####" else "0.0"
            keyboard = [
                [InlineKeyboardButton(f"Пользователь {recipient}", callback_data=SELECT_USER)],
                [InlineKeyboardButton(f"Способ оплаты: {payment_method}", callback_data=SELECT_PAYMENT)],
                [InlineKeyboardButton(f"Количество звезд: {amount}", callback_data=SELECT_AMOUNT)],
                [
                    InlineKeyboardButton(f"Цена: {price_ton} TON", callback_data="price_info"),
                    InlineKeyboardButton("Оплатить", callback_data=CONFIRM_PAYMENT)
                ],
                [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
            ]
            await query.message.edit_text(
                "Выберите параметры покупки:",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML"
            )
            context.user_data["state"] = STATE_BUY_STARS_RECIPIENT
            await log_analytics(user_id, "buy_stars_menu", {})
            await query.answer()
            return STATES[STATE_BUY_STARS_RECIPIENT]
        
        elif data == SELECT_USER:
            await query.message.reply_text("Введите имя пользователя (например, @username):")
            context.user_data["state"] = STATE_BUY_STARS_RECIPIENT
            await query.answer()
            return STATES[STATE_BUY_STARS_RECIPIENT]
        
        elif data == SELECT_PAYMENT:
            keyboard = [
                [InlineKeyboardButton("Криптовалютой", callback_data="set_payment_crypto")],
                [InlineKeyboardButton("🔙 Назад", callback_data=BUY_STARS)]
            ]
            await query.message.edit_text(
                "Выберите способ оплаты:",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML"
            )
            context.user_data["state"] = STATE_BUY_STARS_PAYMENT
            await query.answer()
            return STATES[STATE_BUY_STARS_PAYMENT]
        
        elif data == "set_payment_crypto":
            buy_data = context.user_data.get("buy_data", {})
            buy_data["payment_method"] = "Криптовалютой"
            context.user_data["buy_data"] = buy_data
            recipient = buy_data.get("recipient", "####")
            amount = buy_data.get("amount", "####")
            price_ton = await calculate_price_ton(amount) if amount != "####" else "0.0"
            keyboard = [
                [InlineKeyboardButton(f"Пользователь {recipient}", callback_data=SELECT_USER)],
                [InlineKeyboardButton(f"Способ оплаты: Криптовалютой", callback_data=SELECT_PAYMENT)],
                [InlineKeyboardButton(f"Количество звезд: {amount}", callback_data=SELECT_AMOUNT)],
                [
                    InlineKeyboardButton(f"Цена: {price_ton} TON", callback_data="price_info"),
                    InlineKeyboardButton("Оплатить", callback_data=CONFIRM_PAYMENT)
                ],
                [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
            ]
            await query.message.edit_text(
                "Выберите параметры покупки:",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML"
            )
            context.user_data["state"] = STATE_BUY_STARS_RECIPIENT
            await log_analytics(user_id, "set_payment_crypto", {})
            await query.answer()
            return STATES[STATE_BUY_STARS_RECIPIENT]
        
        elif data == SELECT_AMOUNT:
            await query.message.reply_text("Введите количество звезд (например, 50, 100, 250, 500):")
            context.user_data["state"] = STATE_BUY_STARS_AMOUNT
            await query.answer()
            return STATES[STATE_BUY_STARS_AMOUNT]
        
        elif data == CONFIRM_PAYMENT:
            buy_data = context.user_data.get("buy_data", {})
            recipient = buy_data.get("recipient", "####")
            amount = buy_data.get("amount", "####")
            if recipient == "####" or amount == "####":
                await query.message.reply_text("Пожалуйста, выберите пользователя и количество звезд.")
                await query.answer()
                return STATES[STATE_BUY_STARS_RECIPIENT]
            price_ton = await calculate_price_ton(amount)
            keyboard = [
                [InlineKeyboardButton("Оплатить", callback_data="process_payment")],
                [InlineKeyboardButton("Проверить оплату", callback_data=CHECK_PAYMENT)],
                [InlineKeyboardButton("🔙 Назад", callback_data=BUY_STARS)]
            ]
            await query.message.edit_text(
                f"Подтвердите покупку:\nПользователь: {recipient}\nКоличество звезд: {amount}\nЦена: {price_ton} TON",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML"
            )
            context.user_data["state"] = STATE_BUY_STARS_CONFIRM
            await log_analytics(user_id, "confirm_payment", {"recipient": recipient, "amount": amount, "price_ton": price_ton})
            await query.answer()
            return STATES[STATE_BUY_STARS_CONFIRM]
        
        elif data == CHECK_PAYMENT:
            await query.message.reply_text("Функция проверки оплаты пока не реализована.")
            await query.answer()
            return STATES[STATE_BUY_STARS_CONFIRM]
        
        elif data == BACK_TO_MENU:
            return await start(update, context)
        
        elif data == PROFILE:
            user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
            ref_count = len(json.loads(user["referrals"])) if user["referrals"] else 0
            text = await get_text(
                "profile",
                user_id=user_id,
                stars_bought=user["stars_bought"],
                ref_count=ref_count,
                ref_bonus_ton=user["ref_bonus_ton"]
            )
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
            await query.message.edit_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML"
            )
            context.user_data["state"] = STATE_MAIN_MENU
            await query.answer()
            return STATES[STATE_MAIN_MENU]
        
        elif data == REFERRALS:
            user = await conn.fetchrow("SELECT referrals FROM users WHERE user_id = $1", user_id)
            referrals = json.loads(user["referrals"]) if user["referrals"] else []
            text = await get_text("referrals", ref_count=len(referrals), referral_link=f"https://t.me/{telegram_app.bot.username}?start=ref_{user_id}")
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
            await query.message.edit_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="HTML"
            )
            context.user_data["state"] = STATE_MAIN_MENU
            await query.answer()
            return STATES[STATE_MAIN_MENU]
        
        elif data == ADMIN_PANEL and is_admin:
            return await show_admin_panel(update, context)
        
        else:
            await query.message.reply_text("Неизвестная команда.")
            await query.answer()
            return STATES[STATE_MAIN_MENU]


async def show_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отображение админ-панели."""
    user_id = update.effective_user.id
    async with (await ensure_db_pool()) as conn:
        is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id)
        if not is_admin:
            if update.callback_query:
                await update.callback_query.answer(text="Доступ только для админов.")
            else:
                await update.message.reply_text("Доступ только для админов.")
            return context.user_data.get("state", STATES[STATE_MAIN_MENU])
    
    text = await get_text("admin_panel")
    keyboard = [
        [InlineKeyboardButton("📊 Статистика", callback_data=STATE_ADMIN_STATS)],
        [InlineKeyboardButton("📢 Рассылка", callback_data=BROADCAST_MESSAGE)],
        [InlineKeyboardButton("✏️ Редактировать профиль", callback_data=STATE_ADMIN_EDIT_PROFILE)],
        [InlineKeyboardButton("🔔 Напоминание об обновлении БД", callback_data=STATE_SET_DB_REMINDER)],
        [InlineKeyboardButton("🛠 Технический перерыв", callback_data=STATE_TECH_BREAK)],
        [InlineKeyboardButton("⚙️ Настройки бота", callback_data=STATE_BOT_SETTINGS)],
        [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
    ]
    try:
        if update.callback_query:  # Corrected from "if update.:"
            query = update.callback_query
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            await query.answer()
            telegram_app.bot_data[f"last_admin_message_{user_id}"] = {
                "chat_id": query.message.chat_id,
                "message_id": query.message.message_id
            }
        else:
            sent_message = await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            telegram_app.bot_data[f"last_admin_message_{user_id}"] = {
                "chat_id": sent_message.chat_id,
                "message_id": sent_message.message_id
            }
    except Exception as e:
        logger.error(f"Ошибка отображения админ-панели для user_id={user_id}: {e}", exc_info=True)
        ERRORS.labels(type="telegram_api", endpoint="show_admin_panel").inc()
        return STATES[STATE_MAIN_MENU]
    
    await log_analytics(user_id, "view_admin_panel")
    context.user_data["state"] = STATE_ADMIN_PANEL
    return STATES[STATE_ADMIN_PANEL]

async def show_all_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать список всех пользователей."""
    user_id = update.effective_user.id
    async with (await ensure_db_pool()) as conn:
        is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id)
        if not is_admin:
            if update.callback_query:
                await update.callback_query.answer(text="Доступ только для админов.")
            else:
                await update.message.reply_text("Доступ только для админов.")
            return context.user_data.get("state", STATES[STATE_MAIN_MENU])
        users = await conn.fetch("SELECT user_id, username FROM users ORDER BY user_id")
        users_list = "\n".join(
            f"@{user['username'] or 'Unknown'} (<code>{user['user_id']}</code>)" for user in users
        ) if users else "Нет пользователей."
        text = await get_text("all_users", users_list=users_list)
        keyboard = [
            [InlineKeyboardButton("✏️ Редактировать профиль", callback_data=BACK_TO_EDIT_PROFILE)],
            [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        try:
            if update.callback_query:
                query = update.callback_query
                await query.edit_message_text(text, reply_markup=reply_markup, parse_mode="HTML")
                await query.answer()
                telegram_app.bot_data[f"last_admin_message_{user_id}"] = {
                    "chat_id": query.message.chat_id,
                    "message_id": query.message.message_id
                }
            else:
                sent_message = await update.message.reply_text(text, reply_markup=reply_markup, parse_mode="HTML")
                telegram_app.bot_data[f"last_admin_message_{user_id}"] = {
                    "chat_id": sent_message.chat_id,
                    "message_id": sent_message.message_id
                }
            await log_analytics(user_id, "view_all_users")
            context.user_data["state"] = STATE_ALL_USERS
            return STATES[STATE_ALL_USERS]
        except Exception as e:
            logger.error(f"Ошибка отправки списка пользователей для user_id={user_id}: {e}", exc_info=True)
            ERRORS.labels(type="telegram_api", endpoint="show_all_users").inc()
            return STATES[STATE_ADMIN_PANEL]

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений."""
    global tech_break_info
    user_id = update.effective_user.id
    text = update.message.text.strip()
    state = context.user_data.get("state", STATE_MAIN_MENU)
    REQUESTS.labels(endpoint="message").inc()
    with RESPONSE_TIME.labels(endpoint="message").time():
        logger.info(f"Message received: user_id={user_id}, text={text}, state={state}")
        async with (await ensure_db_pool()) as conn:
            is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id)
            # Skip technical break check for admin
            if not is_admin and tech_break_info and tech_break_info["end_time"] > datetime.now(pytz.UTC):
                time_remaining = format_time_remaining(tech_break_info["end_time"])
                text = await get_text(
                    "tech_break_active",
                    end_time=tech_break_info["end_time"].strftime("%Y-%m-%d %H:%M:%S UTC"),
                    minutes_left=time_remaining,
                    reason=tech_break_info["reason"]
                )
                await update.message.reply_text(text)
                return context.user_data.get("state", STATES[STATE_MAIN_MENU])
            
            if state == STATE_BUY_STARS_RECIPIENT:
                recipient = text.replace("@", "")
                buy_data = context.user_data.get("buy_data", {})
                buy_data["recipient"] = recipient
                context.user_data["buy_data"] = buy_data
                amount = buy_data.get("amount", "####")
                payment_method = buy_data.get("payment_method", "Криптовалютой")
                price_ton = await calculate_price_ton(amount) if amount != "####" else "0.0"
                keyboard = [
                    [InlineKeyboardButton(f"Пользователь {recipient}", callback_data=SELECT_USER)],
                    [InlineKeyboardButton(f"Способ оплаты: {payment_method}", callback_data=SELECT_PAYMENT)],
                    [InlineKeyboardButton(f"Количество звезд: {amount}", callback_data=SELECT_AMOUNT)],
                    [
                        InlineKeyboardButton(f"Цена: {price_ton} TON", callback_data="price_info"),
                        InlineKeyboardButton("Оплатить", callback_data=CONFIRM_PAYMENT)
                    ],
                    [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
                ]
                await update.message.reply_text(
                    "Выберите параметры покупки:",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode="HTML"
                )
                context.user_data["state"] = STATE_BUY_STARS_RECIPIENT
                await log_analytics(user_id, "set_recipient", {"recipient": recipient})
                return STATES[STATE_BUY_STARS_RECIPIENT]
            
            elif state == STATE_BUY_STARS_AMOUNT:
                try:
                    amount = int(text)
                    if amount <= 0:
                        raise ValueError("Количество звезд должно быть положительным.")
                    buy_data = context.user_data.get("buy_data", {})
                    buy_data["amount"] = amount
                    context.user_data["buy_data"] = buy_data
                    recipient = buy_data.get("recipient", "####")
                    payment_method = buy_data.get("payment_method", "Криптовалютой")
                    price_ton = await calculate_price_ton(amount)
                    keyboard = [
                        [InlineKeyboardButton(f"Пользователь {recipient}", callback_data=SELECT_USER)],
                        [InlineKeyboardButton(f"Способ оплаты: {payment_method}", callback_data=SELECT_PAYMENT)],
                        [InlineKeyboardButton(f"Количество звезд: {amount}", callback_data=SELECT_AMOUNT)],
                        [
                            InlineKeyboardButton(f"Цена: {price_ton} TON", callback_data="price_info"),
                            InlineKeyboardButton("Оплатить", callback_data=CONFIRM_PAYMENT)
                        ],
                        [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
                    ]
                    await update.message.reply_text(
                        "Выберите параметры покупки:",
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode="HTML"
                    )
                    context.user_data["state"] = STATE_BUY_STARS_RECIPIENT
                    await log_analytics(user_id, "set_amount", {"amount": amount})
                    return STATES[STATE_BUY_STARS_RECIPIENT]
                except ValueError:
                    await update.message.reply_text("Пожалуйста, введите корректное число звезд.")
                    return STATES[STATE_BUY_STARS_AMOUNT]
            
            elif state == STATE_ADMIN_EDIT_PROFILE and not context.user_data.get("edit_user_id"):
                if user_id != 6956377285:
                    await update.message.reply_text("Доступ только для админов.")
                    return context.user_data.get("state", STATES[STATE_ADMIN_PANEL])
                try:
                    target_user_id = int(text)
                    user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", target_user_id)
                    if not user:
                        await update.message.reply_text(
                            "Пользователь не найден.",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("📋 Все пользователи", callback_data=STATE_ALL_USERS)],
                                [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]
                            ])
                        )
                        return STATES[STATE_ADMIN_EDIT_PROFILE]
                    context.user_data["edit_user_id"] = target_user_id
                    ref_count = len(json.loads(user["referrals"])) if user["referrals"] else 0
                    profile_text = await get_text(
                        "profile",
                        user_id=target_user_id,
                        stars_bought=user["stars_bought"],
                        ref_count=ref_count,
                        ref_bonus_ton=user["ref_bonus_ton"]
                    )
                    keyboard = [
                        [InlineKeyboardButton("🌟 Звезды", callback_data=EDIT_PROFILE_STARS)],
                        [InlineKeyboardButton("🤝 Рефералы", callback_data=EDIT_PROFILE_REFERRALS)],
                        [InlineKeyboardButton("💰 Бонус TON", callback_data=EDIT_PROFILE_REF_BONUS)],
                        [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]
                    ]
                    await update.message.reply_text(
                        f"Редактирование профиля {target_user_id}:\n{profile_text}",
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode="HTML"
                    )
                    await log_analytics(user_id, "edit_profile_select_user", {"target_user_id": target_user_id})
                    return STATES[STATE_ADMIN_EDIT_PROFILE]
                except ValueError:
                    await update.message.reply_text(
                        "Пожалуйста, введите корректный ID пользователя (целое число).",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("📋 Все пользователи", callback_data=STATE_ALL_USERS)],
                            [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]
                        ])
                    )
                    return STATES[STATE_ADMIN_EDIT_PROFILE]
            
            elif state == STATE_ADMIN_EDIT_PROFILE and context.user_data.get("edit_user_id"):
                edit_field = context.user_data.get("edit_profile_field")
                target_user_id = context.user_data.get("edit_user_id")
                logger.info(f"Editing profile: target_user_id={target_user_id}, edit_field={edit_field}, input={text}")
                user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", target_user_id)
                if not user:
                    logger.error(f"User {target_user_id} not found during field update")
                    await update.message.reply_text(
                        f"Пользователь {target_user_id} не найден в базе данных.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                    )
                    context.user_data.pop("edit_user_id", None)
                    context.user_data.pop("edit_profile_field", None)
                    context.user_data["state"] = STATE_ADMIN_PANEL
                    return await show_admin_panel(update, context)
                try:
                    if edit_field == "stars_bought":
                        stars = int(text)
                        if stars < 0:
                            raise ValueError("Количество звезд не может быть отрицательным.")
                        await conn.execute(
                            "UPDATE users SET stars_bought = $1 WHERE user_id = $2",
                            stars, target_user_id
                        )
                        await update.message.reply_text(
                            f"Количество звезд для пользователя {target_user_id} обновлено: {stars}",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                        )
                        await log_analytics(user_id, "edit_profile_stars", {"target_user_id": target_user_id, "stars": stars})
                    elif edit_field == "referrals":
                        ref_ids = [int(x) for x in text.split(",") if x.strip().isdigit()]
                        await conn.execute(
                            "UPDATE users SET referrals = $1 WHERE user_id = $2",
                            json.dumps(ref_ids), target_user_id
                        )
                        await update.message.reply_text(
                            f"Рефералы для пользователя {target_user_id} обновлены: {ref_ids}",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                        )
                        await log_analytics(user_id, "edit_profile_referrals", {"target_user_id": target_user_id, "referrals": ref_ids})
                    elif edit_field == "ref_bonus_ton":
                        bonus = float(text)
                        if bonus < 0:
                            raise ValueError("Бонус не может быть отрицательным.")
                        await conn.execute(
                            "UPDATE users SET ref_bonus_ton = $1 WHERE user_id = $2",
                            bonus, target_user_id
                        )
                        await update.message.reply_text(
                            f"Реферальный бонус для пользователя {target_user_id} обновлен: {bonus} TON",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                        )
                        await log_analytics(user_id, "edit_profile_ref_bonus", {"target_user_id": target_user_id, "bonus": bonus})
                    else:
                        await update.message.reply_text(
                            "Ошибка: поле для редактирования не выбрано. Пожалуйста, выберите поле через меню.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                        )
                        return STATES[STATE_ADMIN_EDIT_PROFILE]
                    context.user_data.pop("edit_profile_field", None)
                    context.user_data["state"] = STATE_ADMIN_PANEL
                    return await show_admin_panel(update, context)
                except ValueError as e:
                    await update.message.reply_text(
                        f"Ошибка: {str(e)} Пожалуйста, введите корректные данные.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                    )
                    return STATES[STATE_ADMIN_EDIT_PROFILE]
            
            elif state == STATE_ADMIN_BROADCAST:
                if user_id != 6956377285:
                    await update.message.reply_text("Доступ только для админов.")
                    return context.user_data.get("state", STATES[STATE_ADMIN_PANEL])
                context.user_data["broadcast_text"] = text
                await update.message.reply_text(
                    f"Подтвердите рассылку:\n\n{text}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("✅ Подтвердить", callback_data=CONFIRM_BROADCAST)],
                        [InlineKeyboardButton("❌ Отменить", callback_data=CANCEL_BROADCAST)]
                    ])
                )
                await log_analytics(user_id, "set_broadcast_text", {"text": text})
                return STATES[STATE_ADMIN_BROADCAST]
            
            elif state == STATE_SET_DB_REMINDER:
                if user_id != 6956377285:
                    await update.message.reply_text("Доступ только для админов.")
                    return context.user_data.get("state", STATES[STATE_ADMIN_PANEL])
                try:
                    reminder_date = datetime.strptime(text, "%Y-%m-%d").date()
                    today = datetime.now(pytz.UTC).date()
                    if reminder_date < today:
                        await update.message.reply_text("Дата напоминания не может быть в прошлом.")
                        return STATES[STATE_SET_DB_REMINDER]
                    await conn.execute(
                        "INSERT INTO reminders (user_id, reminder_date, reminder_type) VALUES ($1, $2, $3)",
                        user_id, reminder_date, "db_update"
                    )
                    await update.message.reply_text(
                        f"Напоминание установлено на {reminder_date}.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                    )
                    await log_analytics(user_id, "set_db_reminder", {"reminder_date": str(reminder_date)})
                    context.user_data["state"] = STATE_ADMIN_PANEL
                    return await show_admin_panel(update, context)
                except ValueError:
                    await update.message.reply_text(
                        "Пожалуйста, введите дату в формате гггг-мм-дд (например, 2025-08-15).",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                    )
                    return STATES[STATE_SET_DB_REMINDER]
            
            elif state == STATE_TECH_BREAK:
                if user_id != 6956377285:
                    await update.message.reply_text("Доступ только для админов.")
                    return context.user_data.get("state", STATES[STATE_ADMIN_PANEL])
                try:
                    parts = text.split(" ", 1)
                    if len(parts) != 2 or not parts[0].isdigit():
                        await update.message.reply_text(
                            "Пожалуйста, введите длительность в минутах и причину через пробел (например: 60 Обновление сервера).",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                        )
                        return STATES[STATE_TECH_BREAK]
                    minutes = int(parts[0])
                    reason = parts[1]
                    end_time = datetime.now(pytz.UTC) + timedelta(minutes=minutes)
                    tech_break_info = {"end_time": end_time, "reason": reason}
                    text = await get_text(
                        "tech_break_set",
                        end_time=end_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
                        reason=reason
                    )
                    await update.message.reply_text(
                        text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                    )
                    await log_analytics(user_id, "set_tech_break", {"minutes": minutes, "reason": reason})
                    context.user_data["state"] = STATE_ADMIN_PANEL
                    return await show_admin_panel(update, context)
                except ValueError:
                    await update.message.reply_text(
                        "Пожалуйста, введите корректное число минут и причину через пробел (например: 60 Обновление сервера).",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                    )
                    return STATES[STATE_TECH_BREAK]
            
            elif state == STATE_BOT_SETTINGS:
                if user_id != 6956377285:
                    await update.message.reply_text("Доступ только для админов.")
                    return context.user_data.get("state", STATES[STATE_ADMIN_PANEL])
                setting_field = context.user_data.get("setting_field")
                if not setting_field:
                    await update.message.reply_text("Ошибка: поле для настройки не выбрано.")
                    return STATES[STATE_BOT_SETTINGS]
                try:
                    value = float(text)
                    if setting_field == "price_usd":
                        if value <= 0:
                            raise ValueError("Цена должна быть положительной.")
                        global PRICE_USD_PER_50
                        PRICE_USD_PER_50 = value
                        await update.message.reply_text(
                            f"Цена за 50 звезд установлена: ${value:.2f}",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                        )
                        await log_analytics(user_id, "set_price_usd", {"price_usd": value})
                    elif setting_field == "markup":
                        if value < 0:
                            raise ValueError("Процент накрутки не может быть отрицательным.")
                        global MARKUP_PERCENTAGE
                        MARKUP_PERCENTAGE = value
                        await update.message.reply_text(
                            f"Процент накрутки установлен: {value}%",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                        )
                        await log_analytics(user_id, "set_markup", {"markup_percentage": value})
                    elif setting_field == "ref_bonus":
                        if value < 0 or value > 100:
                            raise ValueError("Процент бонуса должен быть от 0 до 100.")
                        global REFERRAL_BONUS_PERCENTAGE
                        REFERRAL_BONUS_PERCENTAGE = value
                        await update.message.reply_text(
                            f"Процент реферального бонуса установлен: {value}%",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                        )
                        await log_analytics(user_id, "set_ref_bonus", {"ref_bonus_percentage": value})
                    context.user_data["state"] = STATE_ADMIN_PANEL
                    return await show_admin_panel(update, context)
                except ValueError as e:
                    await update.message.reply_text(
                        f"Ошибка: {str(e)} Пожалуйста, введите корректное число.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                    )
                    return STATES[STATE_BOT_SETTINGS]
            
            else:
                await update.message.reply_text("Пожалуйста, используйте меню.")
                return STATES[STATE_MAIN_MENU]

async def calculate_price_ton(amount):
    """Calculate the price in TON for the given amount of stars."""
    try:
        amount = int(amount)
        if amount <= 0:
            return "0.0"
        # Fetch TON price
        if "ton_price_info" not in telegram_app.bot_data or telegram_app.bot_data["ton_price_info"].get("price", 0.0) == 0.0:
            await update_ton_price()
        ton_price_usd = telegram_app.bot_data["ton_price_info"].get("price", 0.0)
        if ton_price_usd == 0.0:
            return "0.0"  # Fallback if price fetch fails
        price_usd = (amount / 50) * PRICE_USD_PER_50
        price_ton = price_usd / ton_price_usd
        return f"{price_ton:.2f}"
    except (ValueError, TypeError, KeyError):
        logger.error(f"Error calculating TON price for amount={amount}")
        return "0.0"

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений."""
    global tech_break_info
    user_id = update.effective_user.id
    text = update.message.text.strip()
    state = context.user_data.get("state", STATE_MAIN_MENU)
    REQUESTS.labels(endpoint="message").inc()
    with RESPONSE_TIME.labels(endpoint="message").time():
        logger.info(f"Message received: user_id={user_id}, text={text}, state={state}")
        async with (await ensure_db_pool()) as conn:
            is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id)
            # Skip technical break check for admin
            if not is_admin and tech_break_info and tech_break_info["end_time"] > datetime.now(pytz.UTC):
                time_remaining = format_time_remaining(tech_break_info["end_time"])
                text = await get_text(
                    "tech_break_active",
                    end_time=tech_break_info["end_time"].strftime("%Y-%m-%d %H:%M:%S UTC"),
                    minutes_left=time_remaining,  # Use formatted time
                    reason=tech_break_info["reason"]
                )
                await update.message.reply_text(text)
                return context.user_data.get("state", STATES[STATE_MAIN_MENU])
            if state == STATE_BUY_STARS_RECIPIENT:
                recipient = text.replace("@", "")
                context.user_data["buy_data"] = {"recipient": recipient}
                await update.message.reply_text(
                    "Выберите количество звезд:",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("50", callback_data="set_amount_50"), InlineKeyboardButton("100", callback_data="set_amount_100")],
                        [InlineKeyboardButton("250", callback_data="set_amount_250"), InlineKeyboardButton("500", callback_data="set_amount_500")],
                        [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
                    ])
                )
                context.user_data["state"] = STATE_BUY_STARS_AMOUNT
                await log_analytics(user_id, "set_recipient", {"recipient": recipient})
                return STATES[STATE_BUY_STARS_AMOUNT]
            elif state == STATE_ADMIN_EDIT_PROFILE and not context.user_data.get("edit_user_id"):
                if user_id != 6956377285:
                    await update.message.reply_text("Доступ только для админов.")
                    return context.user_data.get("state", STATES[STATE_ADMIN_PANEL])
                try:
                    target_user_id = int(text)
                    user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", target_user_id)
                    if not user:
                        await update.message.reply_text(
                            "Пользователь не найден.",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("📋 Все пользователи", callback_data=STATE_ALL_USERS)],
                                [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]
                            ])
                        )
                        return STATES[STATE_ADMIN_EDIT_PROFILE]
                    context.user_data["edit_user_id"] = target_user_id
                    ref_count = len(json.loads(user["referrals"])) if user["referrals"] else 0
                    profile_text = await get_text(
                        "profile",
                        user_id=target_user_id,
                        stars_bought=user["stars_bought"],
                        ref_count=ref_count,
                        ref_bonus_ton=user["ref_bonus_ton"]
                    )
                    keyboard = [
                        [InlineKeyboardButton("🌟 Звезды", callback_data=EDIT_PROFILE_STARS)],
                        [InlineKeyboardButton("🤝 Рефералы", callback_data=EDIT_PROFILE_REFERRALS)],
                        [InlineKeyboardButton("💰 Бонус TON", callback_data=EDIT_PROFILE_REF_BONUS)],
                        [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]
                    ]
                    await update.message.reply_text(
                        f"Редактирование профиля {target_user_id}:\n{profile_text}",
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode="HTML"
                    )
                    await log_analytics(user_id, "edit_profile_select_user", {"target_user_id": target_user_id})
                    return STATES[STATE_ADMIN_EDIT_PROFILE]
                except ValueError:
                    await update.message.reply_text(
                        "Пожалуйста, введите корректный ID пользователя (целое число).",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("📋 Все пользователи", callback_data=STATE_ALL_USERS)],
                            [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]
                        ])
                    )
                    return STATES[STATE_ADMIN_EDIT_PROFILE]
            elif state == STATE_ADMIN_EDIT_PROFILE and context.user_data.get("edit_user_id"):
                edit_field = context.user_data.get("edit_profile_field")
                target_user_id = context.user_data.get("edit_user_id")
                logger.info(f"Editing profile: target_user_id={target_user_id}, edit_field={edit_field}, input={text}")
                user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", target_user_id)
                if not user:
                    logger.error(f"User {target_user_id} not found during field update")
                    await update.message.reply_text(
                        f"Пользователь {target_user_id} не найден в базе данных.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                    )
                    context.user_data.pop("edit_user_id", None)
                    context.user_data.pop("edit_profile_field", None)
                    context.user_data["state"] = STATE_ADMIN_PANEL
                    return await show_admin_panel(update, context)
                try:
                    if edit_field == "stars_bought":
                        stars = int(text)
                        if stars < 0:
                            raise ValueError("Количество звезд не может быть отрицательным.")
                        await conn.execute(
                            "UPDATE users SET stars_bought = $1 WHERE user_id = $2",
                            stars, target_user_id
                        )
                        await update.message.reply_text(
                            f"Количество звезд для пользователя {target_user_id} обновлено: {stars}",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                        )
                        await log_analytics(user_id, "edit_profile_stars", {"target_user_id": target_user_id, "stars": stars})
                    elif edit_field == "referrals":
                        ref_ids = [int(x) for x in text.split(",") if x.strip().isdigit()]
                        await conn.execute(
                            "UPDATE users SET referrals = $1 WHERE user_id = $2",
                            json.dumps(ref_ids), target_user_id
                        )
                        await update.message.reply_text(
                            f"Рефералы для пользователя {target_user_id} обновлены: {ref_ids}",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                        )
                        await log_analytics(user_id, "edit_profile_referrals", {"target_user_id": target_user_id, "referrals": ref_ids})
                    elif edit_field == "ref_bonus_ton":
                        bonus = float(text)
                        if bonus < 0:
                            raise ValueError("Бонус не может быть отрицательным.")
                        await conn.execute(
                            "UPDATE users SET ref_bonus_ton = $1 WHERE user_id = $1",
                            bonus, target_user_id
                        )
                        await update.message.reply_text(
                            f"Реферальный бонус для пользователя {target_user_id} обновлен: {bonus} TON",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                        )
                        await log_analytics(user_id, "edit_profile_ref_bonus", {"target_user_id": target_user_id, "bonus": bonus})
                    else:
                        await update.message.reply_text(
                            "Ошибка: поле для редактирования не выбрано. Пожалуйста, выберите поле через меню.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                        )
                        return STATES[STATE_ADMIN_EDIT_PROFILE]
                    context.user_data.pop("edit_profile_field", None)
                    context.user_data["state"] = STATE_ADMIN_PANEL
                    return await show_admin_panel(update, context)
                except ValueError as e:
                    await update.message.reply_text(
                        f"Ошибка: {str(e)} Пожалуйста, введите корректные данные.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                    )
                    return STATES[STATE_ADMIN_EDIT_PROFILE]
            elif state == STATE_ADMIN_BROADCAST:
                if user_id != 6956377285:
                    await update.message.reply_text("Доступ только для админов.")
                    return context.user_data.get("state", STATES[STATE_ADMIN_PANEL])
                context.user_data["broadcast_text"] = text
                await update.message.reply_text(
                    f"Подтвердите рассылку:\n\n{text}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("✅ Подтвердить", callback_data=CONFIRM_BROADCAST)],
                        [InlineKeyboardButton("❌ Отменить", callback_data=CANCEL_BROADCAST)]
                    ])
                )
                await log_analytics(user_id, "set_broadcast_text", {"text": text})
                return STATES[STATE_ADMIN_BROADCAST]
            elif state == STATE_SET_DB_REMINDER:
                if user_id != 6956377285:
                    await update.message.reply_text("Доступ только для админов.")
                    return context.user_data.get("state", STATES[STATE_ADMIN_PANEL])
                try:
                    reminder_date = datetime.strptime(text, "%Y-%m-%d").date()
                    today = datetime.now(pytz.UTC).date()
                    if reminder_date < today:
                        await update.message.reply_text("Дата напоминания не может быть в прошлом.")
                        return STATES[STATE_SET_DB_REMINDER]
                    await conn.execute(
                        "INSERT INTO reminders (user_id, reminder_date, reminder_type) VALUES ($1, $2, $3)",
                        user_id, reminder_date, "db_update"
                    )
                    await update.message.reply_text(
                        f"Напоминание установлено на {reminder_date}.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                    )
                    await log_analytics(user_id, "set_db_reminder", {"reminder_date": str(reminder_date)})
                    context.user_data["state"] = STATE_ADMIN_PANEL
                    return await show_admin_panel(update, context)
                except ValueError:
                    await update.message.reply_text(
                        "Пожалуйста, введите дату в формате гггг-мм-дд (например, 2025-08-15).",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                    )
                    return STATES[STATE_SET_DB_REMINDER]
            elif state == STATE_TECH_BREAK:
                if user_id != 6956377285:
                    await update.message.reply_text("Доступ только для админов.")
                    return context.user_data.get("state", STATES[STATE_ADMIN_PANEL])
                try:
                    parts = text.split(" ", 1)
                    if len(parts) != 2 or not parts[0].isdigit():
                        await update.message.reply_text(
                            "Пожалуйста, введите длительность в минутах и причину через пробел (например: 60 Обновление сервера).",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                        )
                        return STATES[STATE_TECH_BREAK]
                    minutes = int(parts[0])
                    reason = parts[1]
                    end_time = datetime.now(pytz.UTC) + timedelta(minutes=minutes)
                    tech_break_info = {"end_time": end_time, "reason": reason}
                    text = await get_text(
                        "tech_break_set",
                        end_time=end_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
                        reason=reason
                    )
                    await update.message.reply_text(
                        text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                    )
                    await log_analytics(user_id, "set_tech_break", {"minutes": minutes, "reason": reason})
                    context.user_data["state"] = STATE_ADMIN_PANEL
                    return await show_admin_panel(update, context)
                except ValueError:
                    await update.message.reply_text(
                        "Пожалуйста, введите корректное число минут и причину через пробел (например: 60 Обновление сервера).",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                    )
                    return STATES[STATE_TECH_BREAK]
            elif state == STATE_BOT_SETTINGS:
                if user_id != 6956377285:
                    await update.message.reply_text("Доступ только для админов.")
                    return context.user_data.get("state", STATES[STATE_ADMIN_PANEL])
                setting_field = context.user_data.get("setting_field")
                if not setting_field:
                    await update.message.reply_text("Ошибка: поле для настройки не выбрано.")
                    return STATES[STATE_BOT_SETTINGS]
                try:
                    value = float(text)
                    if setting_field == "price_usd":
                        if value <= 0:
                            raise ValueError("Цена должна быть положительной.")
                        global PRICE_USD_PER_50
                        PRICE_USD_PER_50 = value
                        await update.message.reply_text(
                            f"Цена за 50 звезд установлена: ${value:.2f}",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                        )
                        await log_analytics(user_id, "set_price_usd", {"price_usd": value})
                    elif setting_field == "markup":
                        if value < 0:
                            raise ValueError("Процент накрутки не может быть отрицательным.")
                        global MARKUP_PERCENTAGE
                        MARKUP_PERCENTAGE = value
                        await update.message.reply_text(
                            f"Процент накрутки установлен: {value}%",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                        )
                        await log_analytics(user_id, "set_markup", {"markup_percentage": value})
                    elif setting_field == "ref_bonus":
                        if value < 0 or value > 100:
                            raise ValueError("Процент бонуса должен быть от 0 до 100.")
                        global REFERRAL_BONUS_PERCENTAGE
                        REFERRAL_BONUS_PERCENTAGE = value
                        await update.message.reply_text(
                            f"Процент реферального бонуса установлен: {value}%",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                        )
                        await log_analytics(user_id, "set_ref_bonus", {"ref_bonus_percentage": value})
                    context.user_data["state"] = STATE_ADMIN_PANEL
                    return await show_admin_panel(update, context)
                except ValueError as e:
                    await update.message.reply_text(
                        f"Ошибка: {str(e)} Пожалуйста, введите корректное число.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                    )
                    return STATES[STATE_BOT_SETTINGS]
            else:
                await update.message.reply_text("Пожалуйста, используйте меню.")
                return STATES[STATE_MAIN_MENU]

async def webhook_handler(request):
    try:
        if telegram_app is None:
            logger.error("telegram_app is None in webhook_handler")
            return web.Response(status=500, text="Application not initialized")
        data = await request.json()
        logger.debug(f"Incoming webhook data: {data}")
        update = telegram.Update.de_json(data, telegram_app.bot)
        if update is None:
            logger.error("Failed to parse update from webhook data")
            return web.Response(status=400, text="Invalid update data")
        logger.info(f"Processing update: update_id={update.update_id}, type={update.to_dict().get('message', {}).get('text', 'unknown')}")
        await telegram_app.process_update(update)
        return web.Response(status=200)
    except Exception as e:
        logger.error(f"Ошибка обработки вебхука: {e}", exc_info=True)
        ERRORS.labels(type="webhook", endpoint="webhook").inc()
        return web.Response(status=500, text=str(e))
        
async def main():
    """Основная функция запуска бота."""
    global telegram_app
    try:
        await check_environment()
        await init_db()
        
        telegram_app = (
            ApplicationBuilder()
            .token(BOT_TOKEN)
            .build()
        )
        
        await telegram_app.initialize()
        logger.info("Telegram Application initialized successfully")
        
        await telegram_app.start()
        logger.info("Telegram Application started")
        
        scheduler = AsyncIOScheduler(timezone=pytz.UTC)
        scheduler.add_job(heartbeat_check, 'interval', minutes=5, args=[telegram_app])
        scheduler.add_job(update_ton_price, 'interval', minutes=30)
        scheduler.add_job(keep_alive, 'interval', minutes=10, args=[telegram_app])
        scheduler.add_job(check_reminders, 'interval', minutes=60)
        scheduler.add_job(backup_db, 'interval', hours=24)
        scheduler.start()
        logger.info("Scheduler started")
        
        telegram_app.add_handler(CommandHandler("start", start, filters=filters.COMMAND))
        telegram_app.add_handler(CommandHandler("tonprice", ton_price_command, filters=filters.COMMAND))
        telegram_app.add_handler(CallbackQueryHandler(callback_query_handler))
        telegram_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
        telegram_app.add_handler(MessageHandler(filters.ALL, debug_update))
        telegram_app.add_error_handler(error_handler)
        logger.info("Handlers registered")
        
        webhook_url = f"{WEBHOOK_URL}/webhook"
        logger.info(f"Setting webhook to {webhook_url}")
        await telegram_app.bot.set_webhook(webhook_url, allowed_updates=telegram.Update.ALL_TYPES)
        logger.info(f"Webhook set to {webhook_url}")
        
        await broadcast_new_menu()
        
        app = web.Application()
        app.router.add_post("/webhook", webhook_handler)
        logger.info(f"Starting webhook server on port {PORT}")
        await web._run_app(app, host="0.0.0.0", port=PORT)
        
    except Exception as e:
        logger.error(f"Ошибка запуска бота: {e}", exc_info=True)
        ERRORS.labels(type="startup", endpoint="main").inc()
        if telegram_app:
            await telegram_app.stop()
            await telegram_app.shutdown()
        await close_db_pool()
        raise
    
    finally:
        if telegram_app:
            await telegram_app.stop()
            await telegram_app.shutdown()
            logger.info("Telegram Application stopped and shut down")
        await close_db_pool()
        logger.info("Database pool closed")
    
if __name__ == "__main__":
    start_http_server(8000)  # Prometheus metrics server
    asyncio.run(main())
