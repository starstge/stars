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
    ConversationHandler,
)
from telegram.error import BadRequest, TelegramError
import asyncpg
from datetime import datetime
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
NEWS_CHANNEL = "https://t.me/CheapStarsShopNews"
TWIN_ACCOUNT_ID = int(os.getenv("TWIN_ACCOUNT_ID", 6956377285))
ADMIN_BACKUP_ID = 6956377285
PRICE_USD_PER_50 = 0.81
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
PORT = int(os.getenv("PORT", 8080))
MARKUP_PERCENTAGE = float(os.getenv("MARKUP_PERCENTAGE", 10))

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
STATE_EXPORT_DATA = "export_data"
STATE_ALL_USERS = "all_users"
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
    STATE_EXPORT_DATA: 14,
    STATE_ALL_USERS: 15
}

# Глобальные переменные
_db_pool = None
_db_pool_lock = asyncio.Lock()
telegram_app = None
transaction_cache = TTLCache(maxsize=1000, ttl=3600)

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
        "welcome": "Привет, {username}! Добро пожаловать в Stars Shop! 🎉 Ты купил {stars_bought} звезд.",
        "referrals": "🤝 Твоя реферальная ссылка: {ref_link}\nРефералов: {ref_count}\nБонус: {ref_bonus_ton} TON",
        "profile": "👤 Профиль:\nЗвезд куплено: {stars_bought}\nРефералов: {ref_count}\nРеферальный бонус: {ref_bonus_ton} TON",
        "buy_success": "✅ Успешная покупка! {stars} звезд отправлено на {recipient}.",
        "buy_prompt": "Введите имя пользователя получателя (с @ или без):",
        "tech_support": "Свяжитесь с поддержкой: https://t.me/CheapStarsSupport",
        "news": "Новости канала: https://t.me/CheapStarsShopNews",
        "all_users": "Список всех пользователей:\n{users_list}",
        "top_referrals": "🏆 Топ-10 рефералов:\n{text}",
        "top_purchases": "🏆 Топ-10 покупок:\n{text}",
        "stats": "📊 Статистика:\nВсего пользователей: {total_users}\nВсего куплено звезд: {total_stars}\nВсего рефералов: {total_referrals}",
        "admin_panel": "🔧 Админ-панель:\nВыберите действие:"
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
    REQUESTS.labels(endpoint="tonprice").inc()
    with RESPONSE_TIME.labels(endpoint="tonprice").time():
        user_id = update.effective_user.id
        try:
            if "ton_price_info" not in telegram_app.bot_data or telegram_app.bot_data["ton_price_info"].get("price", 0.0) == 0.0:
                await update_ton_price()
            if "ton_price_info" not in telegram_app.bot_data or telegram_app.bot_data["ton_price_info"].get("price", 0.0) == 0.0:
                await update.message.reply_text("Ошибка получения цены TON. Попробуйте позже.")
                return
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

async def generate_payload(user_id):
    """Генерация уникального payload для платежа."""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    random_str = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    payload = f"{user_id}_{timestamp}_{random_str}"
    secret = os.getenv("BOT_TOKEN").encode()
    signaturestoffen

System: The response was cut off due to exceeding the maximum length. Below, I’ll provide the complete updated `bot.py` code, ensuring all requested changes are included:

1. **Fix for `NameError: name 'show_admin_panel' is not defined`**: Add the missing `show_admin_panel` function to display the admin panel with options for stats, broadcasting, editing profiles, and exporting data.
2. **Hardcoded Texts**: Retain the `get_text` function with all necessary templates, as requested, and remove the `texts` table from `init_db` and `backup_db` to align with the hardcoded approach.
3. **Revised Referral System**: Award 30% of the profit (assumed to be `amount_usd` converted to TON) to the referrer when a referral makes a purchase, implemented in the `CHECK_PAYMENT` section of `callback_query_handler`. Remove the fixed 0.1 TON bonus from the `start` function.
4. **Fix for `BACK_TO_MENU`**: Ensure the `start` function handles `CallbackQuery` updates correctly to avoid the previous `'NoneType' object has no attribute 'text'` error.
5. **Updated Document**: Include the correct referral link with `@CheapStarsShop_bot`.

The code below is complete, including all functions from your provided code, with the necessary fixes and updates. I’ll also provide deployment and testing instructions.

---

### Complete Updated Code

<xaiArtifact artifact_id="0122858a-fe0e-4b84-b7d0-b776559dbd93" artifact_version_id="ed1545da-e57b-4895-ad60-4457624ff370" title="bot.py" contentType="text/python">
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
    ConversationHandler,
)
from telegram.error import BadRequest, TelegramError
import asyncpg
from datetime import datetime
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
NEWS_CHANNEL = "https://t.me/CheapStarsShopNews"
TWIN_ACCOUNT_ID = int(os.getenv("TWIN_ACCOUNT_ID", 6956377285))
ADMIN_BACKUP_ID = 6956377285
PRICE_USD_PER_50 = 0.81
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
PORT = int(os.getenv("PORT", 8080))
MARKUP_PERCENTAGE = float(os.getenv("MARKUP_PERCENTAGE", 10))

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
STATE_EXPORT_DATA = "export_data"
STATE_ALL_USERS = "all_users"
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
    STATE_EXPORT_DATA: 14,
    STATE_ALL_USERS: 15
}

# Глобальные переменные
_db_pool = None
_db_pool_lock = asyncio.Lock()
telegram_app = None
transaction_cache = TTLCache(maxsize=1000, ttl=3600)

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
        "welcome": "Привет, {username}! Добро пожаловать в Stars Shop! 🎉 Ты купил {stars_bought} звезд.",
        "referrals": "🤝 Твоя реферальная ссылка: {ref_link}\nРефералов: {ref_count}\nБонус: {ref_bonus_ton} TON",
        "profile": "👤 Профиль:\nЗвезд куплено: {stars_bought}\nРефералов: {ref_count}\nРеферальный бонус: {ref_bonus_ton} TON",
        "buy_success": "✅ Успешная покупка! {stars} звезд отправлено на {recipient}.",
        "buy_prompt": "Введите имя пользователя получателя (с @ или без):",
        "tech_support": "Свяжитесь с поддержкой: https://t.me/CheapStarsSupport",
        "news": "Новости канала: https://t.me/CheapStarsShopNews",
        "all_users": "Список всех пользователей:\n{users_list}",
        "top_referrals": "🏆 Топ-10 рефералов:\n{text}",
        "top_purchases": "🏆 Топ-10 покупок:\n{text}",
        "stats": "📊 Статистика:\nВсего пользователей: {total_users}\nВсего куплено звезд: {total_stars}\nВсего рефералов: {total_referrals}",
        "admin_panel": "🔧 Админ-панель:\nВыберите действие:"
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
    REQUESTS.labels(endpoint="tonprice").inc()
    with RESPONSE_TIME.labels(endpoint="tonprice").time():
        user_id = update.effective_user.id
        try:
            if "ton_price_info" not in telegram_app.bot_data or telegram_app.bot_data["ton_price_info"].get("price", 0.0) == 0.0:
                await update_ton_price()
            if "ton_price_info" not in telegram_app.bot_data or telegram_app.bot_data["ton_price_info"].get("price", 0.0) == 0.0:
                await update.message.reply_text("Ошибка получения цены TON. Попробуйте позже.")
                return
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

async def generate_payload(user_id):
    """Генерация уникального payload для платежа."""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    random_str = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    payload = f"{user_id}_{timestamp}_{random_str}"
    secret = os.getenv("BOT_TOKEN").encode()
    signature = hmac.new(secret, payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}:{signature}"

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
            async with session.get("https://tonapi.io/v2/status", headers=headers) as resp:
                if resp.status != 200:
                    logger.warning(f"TON API health check failed: {resp.status}")
                    ERRORS.labels(type="api", endpoint="ton_health").inc()
            if CRYPTOBOT_API_TOKEN:
                async with session.get(CRYPTOBOT_API_URL + "/getMe", headers={"Crypto-Pay-API-Token": CRYPTOBOT_API_TOKEN}) as resp:
                    if resp.status != 200:
                        logger.warning(f"Cryptobot API health check failed: {resp.status}")
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

async def backup_db():
    """Создание бэкапа базы данных и отправка администратору."""
    try:
        async with (await ensure_db_pool()) as conn:
            users = await conn.fetch("SELECT * FROM users")
            analytics = await conn.fetch("SELECT * FROM analytics")
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
            users = await conn.fetch("SELECT user_id FROM users")
            total_stars = await conn.fetchval("SELECT SUM(stars_bought) FROM users") or 0
            for user in users:
                user_id = user["user_id"]
                try:
                    user_stars = await conn.fetchval("SELECT stars_bought FROM users WHERE user_id = $1", user_id) or 0
                    text = await get_text("welcome", username=user_id, stars_bought=user_stars)
                    text += "\n\n⚠️ Используйте новое меню ниже для корректной работы бота."
                    keyboard = [
                        [
                            InlineKeyboardButton("📰 Новости", url=NEWS_CHANNEL),
                            InlineKeyboardButton("📞 Поддержка", url=SUPPORT_CHANNEL)
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
    user_id = update.effective_user.id
    username = update.effective_user.username or "Unknown"
    chat_id = update.effective_chat.id
    message_text = update.message.text if update.message else "CallbackQuery: back_to_menu"
    logger.info(f"Вызов /start для user_id={user_id}, message={message_text}")
    
    args = context.args
    referrer_id = None
    if args and args[0].startswith("ref_"):
        try:
            referrer_id = int(args[0].split("_")[1])
            if referrer_id == user_id:
                referrer_id = None
        except (IndexError, ValueError):
            logger.warning(f"Некорректный реферальный параметр: {args[0]}")
    
    async with (await ensure_db_pool()) as conn:
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
            user_id=user_id,
            username=username,
            stars_bought=stars_bought
        )
    except Exception as e:
        logger.error(f"Ошибка в get_text для welcome: {e}")
        text = f"Привет, {username}! Добро пожаловать в Stars Shop! 🎉"
    
    keyboard = [
        [
            InlineKeyboardButton("📰 Новости", url=NEWS_CHANNEL),
            InlineKeyboardButton("📞 Поддержка", url=SUPPORT_CHANNEL)
        ],
        [
            InlineKeyboardButton("👤 Профиль", callback_data=PROFILE),
            InlineKeyboardButton("🤝 Рефералы", callback_data=REFERRALS)
        ],
        [InlineKeyboardButton("🛒 Купить звезды", callback_data=BUY_STARS)]
    ]
    if user_id == 6956377285:
        keyboard.append([InlineKeyboardButton("🔧 Админ-панель", callback_data=ADMIN_PANEL)])
    
    try:
        last_message = telegram_app.bot_data.get(f"last_message_{user_id}")
        if last_message and last_message["chat_id"] and last_message["message_id"]:
            logger.debug(f"Attempting to edit message: chat_id={last_message['chat_id']}, message_id={last_message['message_id']}")
            await telegram_app.bot.edit_message_text(
                text=text,
                chat_id=last_message["chat_id"],
                message_id=last_message["message_id"],
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        else:
            if update.message:
                await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
            else:
                sent_message = await telegram_app.bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                telegram_app.bot_data[f"last_message_{user_id}"] = {
                    "chat_id": sent_message.chat_id,
                    "message_id": sent_message.message_id
                }
    except BadRequest as e:
        logger.warning(f"Не удалось отредактировать сообщение: {e}")
        if update.message:
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            sent_message = await telegram_app.bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            telegram_app.bot_data[f"last_message_{user_id}"] = {
                "chat_id": sent_message.chat_id,
                "message_id": sent_message.message_id
            }
    
    telegram_app.bot_data[f"last_message_{user_id}"] = {
        "chat_id": chat_id,
        "message_id": update.message.message_id + 1 if update.message else None
    }
    await log_analytics(user_id, "start", {"referrer_id": referrer_id})
    context.user_data["state"] = STATE_MAIN_MENU
    logger.info(f"/start успешно обработан для user_id={user_id}")
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
        [InlineKeyboardButton("💾 Экспорт данных", callback_data=STATE_EXPORT_DATA)],
        [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
    ]
    try:
        if update.callback_query:
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

async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик callback-запросов."""
    query = update.callback_query
    user_id = update.effective_user.id
    data = query.data
    REQUESTS.labels(endpoint="callback_query").inc()
    with RESPONSE_TIME.labels(endpoint="callback_query").time():
        logger.info(f"Callback query received: user_id={user_id}, callback_data={data}, message_id={query.message.message_id}")
        if data and data.isdigit():
            logger.warning(f"Устаревший числовой callback_data: {data} для user_id={user_id}")
            await query.answer(text="Команда устарела. Пожалуйста, используйте новое меню.")
            await start(update, context)
            return STATES[STATE_MAIN_MENU]
        if data.startswith("set_amount_"):
            try:
                stars = int(data.split("_")[2])
                context.user_data["buy_data"] = context.user_data.get("buy_data", {})
                context.user_data["buy_data"]["stars"] = stars
                amount_usd = (stars / 50) * PRICE_USD_PER_50 * (1 + MARKUP_PERCENTAGE / 100)
                context.user_data["buy_data"]["amount_usd"] = round(amount_usd, 2)
                await query.message.reply_text(
                    f"Вы выбрали {stars} звезд. Стоимость: ${amount_usd:.2f}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("TON Space", callback_data=PAY_TON_SPACE)],
                        [InlineKeyboardButton("Cryptobot (Crypto)", callback_data=PAY_CRYPTOBOT)],
                        [InlineKeyboardButton("Cryptobot (Card)", callback_data=PAY_CARD)],
                        [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
                    ])
                )
                await query.answer()
                await log_analytics(user_id, "set_amount", {"stars": stars, "amount_usd": amount_usd})
                context.user_data["state"] = STATE_BUY_STARS_PAYMENT_METHOD
                return STATES[STATE_BUY_STARS_PAYMENT_METHOD]
            except ValueError:
                await query.message.reply_text("Ошибка обработки количества звезд.")
                await query.answer()
                return STATES[STATE_BUY_STARS_AMOUNT]
        if data == BACK_TO_MENU:
            context.user_data.clear()
            context.user_data["state"] = STATE_MAIN_MENU
            await start(update, context)
            return STATES[STATE_MAIN_MENU]
        elif data == BACK_TO_ADMIN:
            return await show_admin_panel(update, context)
        elif data == PROFILE:
            async with (await ensure_db_pool()) as conn:
                user = await conn.fetchrow("SELECT stars_bought, ref_bonus_ton, referrals FROM users WHERE user_id = $1", user_id)
                ref_count = len(json.loads(user["referrals"])) if user["referrals"] else 0
                text = await get_text(
                    "profile",
                    user_id=user_id,
                    stars_bought=user["stars_bought"],
                    ref_count=ref_count,
                    ref_bonus_ton=user["ref_bonus_ton"]
                )
                keyboard = [
                    [
                        InlineKeyboardButton("📈 Топ рефералов", callback_data=STATE_TOP_REFERRALS),
                        InlineKeyboardButton("🛒 Топ покупок", callback_data=STATE_TOP_PURCHASES)
                    ],
                    [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
                ]
                await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
                await query.answer()
                telegram_app.bot_data[f"last_message_{user_id}"] = {
                    "chat_id": query.message.chat_id,
                    "message_id": query.message.message_id
                }
                await log_analytics(user_id, "view_profile", {"ref_count": ref_count})
                context.user_data["state"] = STATE_PROFILE
                return STATES[STATE_PROFILE]
        elif data == REFERRALS:
            async with (await ensure_db_pool()) as conn:
                user = await conn.fetchrow("SELECT referrals, ref_bonus_ton FROM users WHERE user_id = $1", user_id)
                ref_count = len(json.loads(user["referrals"])) if user["referrals"] else 0
                ref_link = f"https://t.me/CheapStarsShop_bot?start=ref_{user_id}"
                text = await get_text("referrals", ref_count=ref_count, ref_bonus_ton=user["ref_bonus_ton"], ref_link=ref_link)
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
                await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
                await query.answer()
                telegram_app.bot_data[f"last_message_{user_id}"] = {
                    "chat_id": query.message.chat_id,
                    "message_id": query.message.message_id
                }
                await log_analytics(user_id, "view_referrals", {"ref_count": ref_count})
                context.user_data["state"] = STATE_REFERRALS
                return STATES[STATE_REFERRALS]
        elif data == BUY_STARS:
            await query.message.reply_text("Введите имя пользователя получателя (с @ или без):")
            context.user_data["state"] = STATE_BUY_STARS_RECIPIENT
            await query.answer()
            await log_analytics(user_id, "start_buy_stars")
            return STATES[STATE_BUY_STARS_RECIPIENT]
        elif data == ADMIN_PANEL:
            return await show_admin_panel(update, context)
        elif data == STATE_EXPORT_DATA:
            if user_id != 6956377285:
                await query.answer(text="Доступ только для главного админа.")
                return context.user_data.get("state", STATES[STATE_ADMIN_PANEL])
            try:
                backup_file, backup_data = await backup_db()
                await query.answer(text="Бэкап базы данных отправлен администратору.")
                await log_analytics(user_id, "copy_db")
                context.user_data["state"] = STATE_ADMIN_PANEL
                return await show_admin_panel(update, context)
            except Exception as e:
                logger.error(f"Ошибка при создании/отправке бэкапа: {e}", exc_info=True)
                await query.answer(text="Ошибка при создании бэкапа. Попробуйте позже.")
                return await show_admin_panel(update, context)
        elif data == STATE_ADMIN_STATS:
            async with (await ensure_db_pool()) as conn:
                total_users = await conn.fetchval("SELECT COUNT(*) FROM users") or 0
                total_stars = await conn.fetchval("SELECT SUM(stars_bought) FROM users") or 0
                total_referrals = await conn.fetchval("SELECT SUM(jsonb_array_length(referrals)) FROM users") or 0
                text = await get_text(
                    "stats",
                    total_users=total_users,
                    total_stars=total_stars,
                    total_referrals=total_referrals
                )
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
                await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
                await query.answer()
                telegram_app.bot_data[f"last_admin_message_{user_id}"] = {
                    "chat_id": query.message.chat_id,
                    "message_id": query.message.message_id
                }
                await log_analytics(user_id, "view_stats")
                context.user_data["state"] = STATE_ADMIN_STATS
                return STATES[STATE_ADMIN_STATS]
        elif data == STATE_ADMIN_EDIT_PROFILE:
            if user_id != 6956377285:
                await query.answer(text="Доступ только для главного админа.")
                return await show_admin_panel(update, context)
            await query.message.reply_text(
                "Введите ID пользователя для редактирования профиля:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📋 Все пользователи", callback_data=STATE_ALL_USERS)],
                    [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]
                ])
            )
            await query.answer()
            context.user_data["state"] = STATE_ADMIN_EDIT_PROFILE
            return STATES[STATE_ADMIN_EDIT_PROFILE]
        elif data == STATE_ALL_USERS:
            return await show_all_users(update, context)
        elif data == BACK_TO_EDIT_PROFILE:
            await query.message.reply_text(
                "Введите ID пользователя для редактирования профиля:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📋 Все пользователи", callback_data=STATE_ALL_USERS)],
                    [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]
                ])
            )
            await query.answer()
            context.user_data["state"] = STATE_ADMIN_EDIT_PROFILE
            return STATES[STATE_ADMIN_EDIT_PROFILE]
        elif data == STATE_TOP_REFERRALS:
            async with (await ensure_db_pool()) as conn:
                users = await conn.fetch("SELECT user_id, username, referrals FROM users ORDER BY jsonb_array_length(referrals) DESC LIMIT 10")
                text_lines = []
                if not users:
                    text_lines.append("Нет данных о рефералах.")
                for i, user in enumerate(users, 1):
                    ref_count = len(json.loads(user["referrals"])) if user["referrals"] != '[]' else 0
                    text_lines.append(f"{i}. @{user['username'] or 'Unknown'}: {ref_count} рефералов")
                text = await get_text("top_referrals", text="\n".join(text_lines))
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
                await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
                await query.answer()
                telegram_app.bot_data[f"last_message_{user_id}"] = {
                    "chat_id": query.message.chat_id,
                    "message_id": query.message.message_id
                }
                await log_analytics(user_id, "view_top_referrals")
                context.user_data["state"] = STATE_TOP_REFERRALS
                return STATES[STATE_TOP_REFERRALS]
        elif data == STATE_TOP_PURCHASES:
            async with (await ensure_db_pool()) as conn:
                users = await conn.fetch("SELECT user_id, username, stars_bought FROM users ORDER BY stars_bought DESC LIMIT 10")
                text_lines = []
                if not users:
                    text_lines.append("Нет данных о покупках.")
                for i, user in enumerate(users, 1):
                    text_lines.append(f"{i}. @{user['username'] or 'Unknown'}: {user['stars_bought']} звезд")
                text = await get_text("top_purchases", text="\n".join(text_lines))
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
                await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
                await query.answer()
                telegram_app.bot_data[f"last_message_{user_id}"] = {
                    "chat_id": query.message.chat_id,
                    "message_id": query.message.message_id
                }
                await log_analytics(user_id, "view_top_purchases")
                context.user_data["state"] = STATE_TOP_PURCHASES
                return STATES[STATE_TOP_PURCHASES]
        elif data == EDIT_PROFILE_STARS:
            context.user_data["edit_profile_field"] = "stars_bought"
            await query.message.reply_text("Введите новое количество звезд:")
            await query.answer()
            context.user_data["state"] = STATE_ADMIN_EDIT_PROFILE
            return STATES[STATE_ADMIN_EDIT_PROFILE]
        elif data == EDIT_PROFILE_REFERRALS:
            context.user_data["edit_profile_field"] = "referrals"
            await query.message.reply_text("Введите ID пользователей для рефералов (через запятую, например: 123,456):")
            await query.answer()
            context.user_data["state"] = STATE_ADMIN_EDIT_PROFILE
            return STATES[STATE_ADMIN_EDIT_PROFILE]
        elif data == EDIT_PROFILE_REF_BONUS:
            context.user_data["edit_profile_field"] = "ref_bonus_ton"
            await query.message.reply_text("Введите новый реферальный бонус в TON (например, 0.5):")
            await query.answer()
            context.user_data["state"] = STATE_ADMIN_EDIT_PROFILE
            return STATES[STATE_ADMIN_EDIT_PROFILE]
        elif data == CONFIRM_BROADCAST:
            broadcast_text = context.user_data.get("broadcast_text", "")
            if not broadcast_text:
                await query.message.reply_text(
                    "Текст рассылки не задан. Введите текст заново.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                )
                await query.answer()
                context.user_data["state"] = STATE_ADMIN_BROADCAST
                return STATES[STATE_ADMIN_BROADCAST]
            success_count, failed_count = await broadcast_message_to_users(broadcast_text)
            await query.message.reply_text(
                f"Рассылка завершена:\nУспешно: {success_count}\nНеудачно: {failed_count}",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
            )
            await query.answer()
            await log_analytics(user_id, "broadcast_sent", {"success": success_count, "failed": failed_count})
            context.user_data["state"] = STATE_ADMIN_PANEL
            return await show_admin_panel(update, context)
        elif data == CANCEL_BROADCAST:
            context.user_data["broadcast_text"] = ""
            await query.message.reply_text(
                "Рассылка отменена.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
            )
            await query.answer()
            context.user_data["state"] = STATE_ADMIN_PANEL
            return await show_admin_panel(update, context)
        elif data == PAY_TON_SPACE:
            buy_data = context.user_data.get("buy_data", {})
            stars = buy_data.get("stars", 50)
            recipient = buy_data.get("recipient", "@Unknown")
            amount_usd = buy_data.get("amount_usd", (stars / 50) * PRICE_USD_PER_50 * (1 + MARKUP_PERCENTAGE / 100))
            payload = await generate_payload(user_id)
            pay_url = f"{TON_SPACE_API_URL}/pay?amount={amount_usd}&payload={payload}"
            buy_data.update({
                "payment_method": "ton_space",
                "amount_usd": amount_usd,
                "pay_url": pay_url,
                "payload": payload
            })
            context.user_data["buy_data"] = buy_data
            await query.message.reply_text(
                f"Оплатите ${amount_usd:.2f} для {stars} звезд на {recipient}:\n{pay_url}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Проверить оплату", callback_data=CHECK_PAYMENT)],
                    [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
                ])
            )
            await query.answer()
            await log_analytics(user_id, "select_ton_space")
            context.user_data["state"] = STATE_BUY_STARS_CONFIRM
            return STATES[STATE_BUY_STARS_CONFIRM]
        elif data == PAY_CRYPTOBOT:
            buy_data = context.user_data.get("buy_data", {})
            stars = buy_data.get("stars", 50)
            recipient = buy_data.get("recipient", "@Unknown")
            amount_usd = buy_data.get("amount_usd", (stars / 50) * PRICE_USD_PER_50 * (1 + MARKUP_PERCENTAGE / 100))
            payload = await generate_payload(user_id)
            invoice_id, pay_url = await create_cryptobot_invoice(amount_usd, "USD", user_id, stars, recipient, payload)
            if not pay_url:
                await query.message.reply_text("Ошибка создания инвойса. Попробуйте другой метод.")
                context.user_data["state"] = STATE_BUY_STARS_PAYMENT_METHOD
                await query.answer()
                return STATES[STATE_BUY_STARS_PAYMENT_METHOD]
            buy_data.update({
                "payment_method": "cryptobot_usd",
                "amount_usd": amount_usd,
                "pay_url": pay_url,
                "invoice_id": invoice_id,
                "payload": payload
            })
            context.user_data["buy_data"] = buy_data
            await query.message.reply_text(
                f"Оплатите ${amount_usd:.2f} для {stars} звезд на {recipient}:\n{pay_url}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Проверить оплату", callback_data=CHECK_PAYMENT)],
                    [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
                ])
            )
            await query.answer()
            await log_analytics(user_id, "select_cryptobot_usd")
            context.user_data["state"] = STATE_BUY_STARS_CONFIRM
            return STATES[STATE_BUY_STARS_CONFIRM]
        elif data == PAY_CARD:
            await query.message.reply_text("Оплата картой пока не поддерживается.")
            await query.answer()
            context.user_data["state"] = STATE_BUY_STARS_PAYMENT_METHOD
            return STATES[STATE_BUY_STARS_PAYMENT_METHOD]
        elif data == CHECK_PAYMENT:
            buy_data = context.user_data.get("buy_data", {})
            invoice_id = buy_data.get("invoice_id")
            payload = buy_data.get("payload")
            if not invoice_id or not payload:
                await query.message.reply_text("Нет активной оплаты.")
                await query.answer()
                return STATES[STATE_BUY_STARS_CONFIRM]
            async with aiohttp.ClientSession(timeout=ClientTimeout(total=30)) as session:
                headers = {"Crypto-Pay-API-Token": CRYPTOBOT_API_TOKEN}
                async with session.get(f"{CRYPTOBOT_API_URL}/getInvoices?invoice_ids={invoice_id}", headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        invoice = data["result"]["items"][0]
                        if invoice["status"] == "paid":
                            stars = buy_data["stars"]
                            recipient = buy_data["recipient"]
                            amount_usd = buy_data["amount_usd"]
                            async with (await ensure_db_pool()) as conn:
                                await conn.execute(
                                    "UPDATE users SET stars_bought = stars_bought + $1 WHERE user_id = $2",
                                    stars, user_id
                                )
                                # Награждение реферала (30% от прибыли в TON)
                                ton_price = telegram_app.bot_data.get("ton_price_info", {"price": 0.0})["price"]
                                if ton_price == 0.0:
                                    logger.warning(f"Цена TON не доступна, пропуск начисления реферального бонуса для user_id={user_id}")
                                else:
                                    profit_ton = amount_usd / ton_price
                                    referral_bonus = profit_ton * 0.3
                                    referrer = await conn.fetchrow(
                                        "SELECT user_id FROM users WHERE referrals @> $1",
                                        json.dumps([user_id])
                                    )
                                    if referrer:
                                        referrer_id = referrer["user_id"]
                                        await conn.execute(
                                            "UPDATE users SET ref_bonus_ton = ref_bonus_ton + $1 WHERE user_id = $2",
                                            referral_bonus, referrer_id
                                        )
                                        logger.info(f"Начислен реферальный бонус {referral_bonus:.2f} TON для referrer_id={referrer_id}")
                                        try:
                                            await telegram_app.bot.send_message(
                                                chat_id=referrer_id,
                                                text=f"🎉 Вам начислен реферальный бонус {referral_bonus:.2f} TON за покупку вашего реферала!"
                                            )
                                        except Exception as e:
                                            logger.error(f"Ошибка отправки уведомления рефералу {referrer_id}: {e}")
                                text = await get_text("buy_success", stars=stars, recipient=recipient)
                                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
                                await query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
                                await query.answer()
                                await log_analytics(user_id, "payment_success", {
                                    "stars": stars,
                                    "recipient": recipient,
                                    "amount_usd": amount_usd,
                                    "referral_bonus": referral_bonus if referrer else 0.0
                                })
                                context.user_data.clear()
                                context.user_data["state"] = STATE_MAIN_MENU
                                transaction_cache[payload] = True
                                return STATES[STATE_MAIN_MENU]
                            else:
                                await query.message.reply_text("Оплата еще не подтверждена. Попробуйте снова.")
                                await query.answer()
                                return STATES[STATE_BUY_STARS_CONFIRM]
                    else:
                        logger.error(f"Cryptobot API error: {response.status} - {await response.text()}")
                        await query.message.reply_text("Ошибка проверки оплаты. Попробуйте позже.")
                        await query.answer()
                        return STATES[STATE_BUY_STARS_CONFIRM]
        elif data == BROADCAST_MESSAGE:
            if user_id != 6956377285:
                await query.answer(text="Доступ только для главного админа.")
                return await show_admin_panel(update, context)
            await query.message.reply_text(
                "Введите текст для рассылки:",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
            )
            await query.answer()
            context.user_data["state"] = STATE_ADMIN_BROADCAST
            return STATES[STATE_ADMIN_BROADCAST]
        else:
            await query.answer(text="Неизвестная команда.")
            return context.user_data.get("state", STATES[STATE_MAIN_MENU])

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений."""
    user_id = update.effective_user.id
    text = update.message.text.strip()
    state = context.user_data.get("state", STATE_MAIN_MENU)
    REQUESTS.labels(endpoint="message").inc()
    with RESPONSE_TIME.labels(endpoint="message").time():
        logger.info(f"Message received: user_id={user_id}, text={text}, state={state}")
        async with (await ensure_db_pool()) as conn:
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
            elif state == STATE_ADMIN_EDIT_PROFILE:
                if user_id != 6956377285:
                    await update.message.reply_text("Доступ только для админов.")
                    return context.user_data.get("state", STATES[STATE_ADMIN_PANEL])
                try:
                    target_user_id = int(text)
                    user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", target_user_id)
                    if not user:
                        await update.message.reply_text("Пользователь не найден.")
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
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                    await log_analytics(user_id, "edit_profile_select_user", {"target_user_id": target_user_id})
                    return STATES[STATE_ADMIN_EDIT_PROFILE]
                except ValueError:
                    await update.message.reply_text("Пожалуйста, введите корректный ID пользователя.")
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
            elif context.user_data.get("edit_profile_field") in ["stars_bought", "referrals", "ref_bonus_ton"]:
                if user_id != 6956377285:
                    await update.message.reply_text("Доступ только для админов.")
                    return context.user_data.get("state", STATES[STATE_ADMIN_PANEL])
                target_user_id = context.user_data.get("edit_user_id")
                if not target_user_id:
                    await update.message.reply_text("Пользователь не выбран. Введите ID пользователя.")
                    context.user_data["state"] = STATE_ADMIN_EDIT_PROFILE
                    return STATES[STATE_ADMIN_EDIT_PROFILE]
                field = context.user_data["edit_profile_field"]
                try:
                    if field == "stars_bought":
                        value = int(text)
                        await conn.execute(
                            "UPDATE users SET stars_bought = $1 WHERE user_id = $2",
                            value, target_user_id
                        )
                        await update.message.reply_text(f"Количество звезд обновлено: {value}")
                    elif field == "referrals":
                        referral_ids = [int(x.strip()) for x in text.split(",") if x.strip().isdigit()]
                        await conn.execute(
                            "UPDATE users SET referrals = $1 WHERE user_id = $2",
                            json.dumps(referral_ids), target_user_id
                        )
                        await update.message.reply_text(f"Рефералы обновлены: {referral_ids}")
                    elif field == "ref_bonus_ton":
                        value = float(text)
                        await conn.execute(
                            "UPDATE users SET ref_bonus_ton = $1 WHERE user_id = $2",
                            value, target_user_id
                        )
                        await update.message.reply_text(f"Реферальный бонус обновлен: {value} TON")
                    context.user_data["edit_profile_field"] = None
                    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_EDIT_PROFILE)]]
                    await update.message.reply_text(
                        "Действие завершено. Выберите следующее действие:",
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                    await log_analytics(user_id, f"edit_profile_{field}", {"target_user_id": target_user_id, "value": text})
                    return STATES[STATE_ADMIN_EDIT_PROFILE]
                except ValueError:
                    await update.message.reply_text("Пожалуйста, введите корректное значение.")
                    return STATES[STATE_ADMIN_EDIT_PROFILE]
            else:
                await update.message.reply_text("Используйте кнопки меню для взаимодействия.")
                return STATES[STATE_MAIN_MENU]

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ошибок."""
    try:
        raise context.error
    except Exception as e:
        logger.error(f"Ошибка в обработчике: {e}", exc_info=True)
        ERRORS.labels(type="telegram", endpoint="error_handler").inc()
        user_id = update.effective_user.id if update and update.effective_user else None
        if user_id:
            await log_analytics(user_id, "error", {"error": str(e)})
        if update and update.effective_message:
            try:
                await update.effective_message.reply_text("Произошла ошибка. Пожалуйста, попробуйте снова.")
            except Exception as reply_error:
                logger.error(f"Не удалось отправить сообщение об ошибке: {reply_error}")

async def main():
    """Основная функция для запуска бота."""
    global telegram_app
    try:
        await check_environment()
        await test_db_connection()
        await init_db()
        start_http_server(8000)
        telegram_app = (
            ApplicationBuilder()
            .token(BOT_TOKEN)
            .http_version("1.1")
            .get_updates_http_version("1.1")
            .connection_pool_size(20)
            .build()
        )
        scheduler = AsyncIOScheduler(timezone=pytz.UTC)
        scheduler.add_job(update_ton_price, "interval", minutes=5, next_run_time=datetime.now(pytz.UTC))
        scheduler.add_job(heartbeat_check, "interval", minutes=5, args=[telegram_app], next_run_time=datetime.now(pytz.UTC))
        scheduler.add_job(keep_alive, "interval", minutes=25, args=[telegram_app])
        scheduler.add_job(backup_db, "interval", hours=24)
        scheduler.start()
        conv_handler = ConversationHandler(
            entry_points=[
                CommandHandler("start", start),
                CommandHandler("tonprice", ton_price_command),
                CallbackQueryHandler(callback_query_handler)
            ],
            states={
                STATES[STATE_MAIN_MENU]: [
                    CallbackQueryHandler(callback_query_handler),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler)
                ],
                STATES[STATE_PROFILE]: [
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATES[STATE_REFERRALS]: [
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATES[STATE_BUY_STARS_RECIPIENT]: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler),
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATES[STATE_BUY_STARS_AMOUNT]: [
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATES[STATE_BUY_STARS_PAYMENT_METHOD]: [
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATES[STATE_BUY_STARS_CONFIRM]: [
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATES[STATE_ADMIN_PANEL]: [
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATES[STATE_ADMIN_STATS]: [
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATES[STATE_ADMIN_BROADCAST]: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler),
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATES[STATE_ADMIN_EDIT_PROFILE]: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler),
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATES[STATE_TOP_REFERRALS]: [
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATES[STATE_TOP_PURCHASES]: [
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATES[STATE_EXPORT_DATA]: [
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATES[STATE_ALL_USERS]: [
                    CallbackQueryHandler(callback_query_handler)
                ]
            },
            fallbacks=[
                CommandHandler("start", start),
                CallbackQueryHandler(callback_query_handler, pattern="^" + BACK_TO_MENU + "$"),
                CallbackQueryHandler(callback_query_handler, pattern="^" + BACK_TO_ADMIN + "$")
            ]
        )
        telegram_app.add_handler(conv_handler)
        telegram_app.add_error_handler(error_handler)
        async def webhook_handler(request):
            try:
                update = Update.de_json(await request.json(), telegram_app.bot)
                await telegram_app.process_update(update)
                return web.Response(status=200)
            except Exception as e:
                logger.error(f"Ошибка обработки вебхука: {e}", exc_info=True)
                ERRORS.labels(type="webhook", endpoint="webhook").inc()
                return web.Response(status=500)
        app = web.Application()
        app.router.add_post("/webhook", webhook_handler)
        await telegram_app.bot.set_webhook(f"{WEBHOOK_URL}/webhook")
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", PORT)
        await site.start()
        logger.info(f"Бот запущен на порту {PORT}, Webhook: {WEBHOOK_URL}/webhook")
        await telegram_app.initialize()
        await telegram_app.start()
        await telegram_app.updater.start_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path="/webhook",
            webhook_url=f"{WEBHOOK_URL}/webhook"
        )
        while True:
            await asyncio.sleep(3600)
    except Exception as e:
        logger.error(f"Критическая ошибка в main: {e}", exc_info=True)
        await telegram_app.bot.send_message(
            chat_id=ADMIN_BACKUP_ID,
            text=f"⚠️ Критическая ошибка бота: {str(e)}"
        )
        raise
    finally:
        await close_db_pool()
        if telegram_app and telegram_app.updater:
            await telegram_app.updater.stop()
            await telegram_app.stop()
            await telegram_app.shutdown()
        logger.info("Бот остановлен")

if __name__ == "__main__":
    asyncio.run(main())
