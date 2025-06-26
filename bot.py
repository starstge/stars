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
import base64
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Настройка логирования с ротацией
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler("bot.log", maxBytes=10_000_000, backupCount=5, encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)

# Метрики Prometheus
REQUESTS = Counter("bot_requests_total", "Total number of requests", ["endpoint"])
ERRORS = Counter("bot_errors_total", "Total number of errors", ["type"])
RESPONSE_TIME = Histogram("bot_response_time_seconds", "Response time of handlers", ["endpoint"])

# Загрузка .env
load_dotenv()

# Константы
BOT_TOKEN = os.getenv("BOT_TOKEN")
TON_API_KEY = os.getenv("TON_API_KEY")
POSTGRES_URL = os.getenv("POSTGRES_URL") or os.getenv("DATABASE_URL")
SPLIT_API_TOKEN = os.getenv("SPLIT_API_TOKEN")
CRYPTOBOT_API_TOKEN = os.getenv("CRYPTOBOT_API_TOKEN")
TON_SPACE_API_TOKEN = os.getenv("TON_SPACE_API_TOKEN")
OWNER_WALLET = os.getenv("OWNER_WALLET")
PROVIDER_TOKEN = os.getenv("PROVIDER_TOKEN")
SPLIT_API_URL = "https://api.split.tg/buy/stars"
CRYPTOBOT_API_URL = "https://pay.crypt.bot/api"
TON_SPACE_API_URL = "https://api.ton.space/v1"
SUPPORT_CHANNEL = os.getenv("SUPPORT_CHANNEL", "@CheapStarsShop_support")
NEWS_CHANNEL = os.getenv("NEWS_CHANNEL", "@cheapstarshop_news")
TWIN_ACCOUNT_ID = int(os.getenv("TWIN_ACCOUNT_ID", 6956377285))
YOUR_TEST_ACCOUNT_ID = 6956377285  # Replace with your test account ID
PRICE_USD_PER_50 = 0.81  # Цена за 50 звезд в USD
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
PORT = int(os.getenv("PORT", 8080))

# Константы callback_data
BACK_TO_MENU = "back_to_menu"
BACK_TO_ADMIN = "back_to_admin"
PROFILE = "profile"
REFERRALS = "referrals"
SUPPORT = "support"
REVIEWS = "reviews"
BUY_STARS = "buy_stars"
ADMIN_PANEL = "admin_panel"
ADMIN_STATS = "admin_stats"
ADMIN_EDIT_TEXTS = "admin_edit_texts"
ADMIN_USER_STATS = "admin_user_stats"
ADMIN_EDIT_MARKUP = "admin_edit_markup"
ADMIN_MANAGE_ADMINS = "admin_manage_admins"
ADMIN_EDIT_PROFIT = "admin_edit_profit"
CHECK_PAYMENT = "check_payment"
TOP_REFERRALS = "top_referrals"
TOP_PURCHASES = "top_purchases"
PAY_CRYPTO = "pay_crypto"
PAY_CARD = "pay_card"
PAY_TON_SPACE = "pay_ton_space"
PAY_CRYPTOBOT = "pay_cryptobot"
EDIT_TEXT_WELCOME = "edit_text_welcome"
EDIT_TEXT_BUY_PROMPT = "edit_text_buy_prompt"
EDIT_TEXT_PROFILE = "edit_text_profile"
EDIT_TEXT_REFERRALS = "edit_text_referrals"
EDIT_TEXT_TECH_SUPPORT = "edit_text_tech_support"
EDIT_TEXT_REVIEWS = "edit_text_reviews"
EDIT_TEXT_BUY_SUCCESS = "edit_text_buy_success"
ADD_ADMIN = "add_admin"
REMOVE_ADMIN = "remove_admin"
EDIT_USER_STARS = "edit_user_stars"
EDIT_USER_REF_BONUS = "edit_user_ref_bonus"
EDIT_USER_PURCHASES = "edit_user_purchases"
MARKUP_TON_SPACE = "markup_ton_space"
MARKUP_CRYPTOBOT_CRYPTO = "markup_cryptobot_crypto"
MARKUP_CRYPTOBOT_CARD = "markup_cryptobot_card"
MARKUP_REF_BONUS = "markup_ref_bonus"
SET_RECIPIENT = "set_recipient"
SET_AMOUNT = "set_amount"
SET_PAYMENT = "set_payment"
CONFIRM_PAYMENT = "confirm_payment"
LIST_USERS = "list_users"
SELECT_USER = "select_user_"
SELECT_CRYPTO_TYPE = "select_crypto_type"
EXPORT_DATA = "export_data"
VIEW_LOGS = "view_logs"

# Константы состояний
STATE_MAIN_MENU, STATE_BUY_STARS_RECIPIENT, STATE_BUY_STARS_AMOUNT, STATE_BUY_STARS_PAYMENT_METHOD, \
STATE_BUY_STARS_CRYPTO_TYPE, STATE_BUY_STARS_CONFIRM, STATE_ADMIN_PANEL, STATE_ADMIN_STATS, \
STATE_ADMIN_EDIT_TEXTS, STATE_EDIT_TEXT, STATE_ADMIN_EDIT_MARKUP, STATE_ADMIN_MANAGE_ADMINS, \
STATE_ADMIN_EDIT_PROFIT, STATE_PROFILE, STATE_TOP_REFERRALS, STATE_TOP_PURCHASES, \
STATE_REFERRALS, STATE_ADMIN_USER_STATS, STATE_EDIT_USER, STATE_LIST_USERS, \
STATE_EXPORT_DATA, STATE_VIEW_LOGS = range(22)

# Глобальные переменные
_db_pool = None
_db_pool_lock = asyncio.Lock()
app = None
transaction_cache = TTLCache(maxsize=1000, ttl=3600)  # Кэш транзакций на 1 час

async def keep_alive(application: Application):
    """Send /start command to keep the bot active."""
    chat_id = os.getenv("KEEP_ALIVE_CHAT_ID", "6956377285")  # Use environment variable or default to test account ID
    try:
        await application.bot.send_message(chat_id=chat_id, text="/start")
        logger.info(f"Sent /start to chat_id={chat_id} to keep bot active")
    except Exception as e:
        logger.error(f"Failed to send keep-alive /start to chat_id={chat_id}: {e}")
        ERRORS.labels(type="telegram_api").inc()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start."""
    REQUESTS.labels(endpoint="start").inc()
    with RESPONSE_TIME.labels(endpoint="start").time():
        user_id = update.effective_user.id
        username = update.effective_user.username
        async with (await ensure_db_pool()) as conn:
            # Инициализация пользователя
            await conn.execute(
                """
                INSERT INTO users (user_id, username, stars_bought, ref_bonus_ton, referrals, is_new)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (user_id) DO UPDATE SET username = $2
                """,
                user_id, username, 0, 0.0, json.dumps([]), True
            )
            # Получение статистики
            total_stars = await conn.fetchval("SELECT SUM(stars_bought) FROM users") or 0
            user_stars = await conn.fetchval("SELECT stars_bought FROM users WHERE user_id = $1", user_id) or 0
            # Формирование приветственного сообщения
            text = await get_text("welcome", stars_sold=total_stars, stars_bought=user_stars)
            keyboard = [
                [InlineKeyboardButton("👤 Профиль", callback_data=PROFILE)],
                [InlineKeyboardButton("🤝 Рефералы", callback_data=REFERRALS)],
                [InlineKeyboardButton("💸 Купить звезды", callback_data=BUY_STARS)],
                [InlineKeyboardButton("🛠 Техподдержка", callback_data=SUPPORT)],
                [InlineKeyboardButton("📝 Отзывы", callback_data=REVIEWS)],
                [InlineKeyboardButton("🔧 Админ-панель", callback_data=ADMIN_PANEL)]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            current_message = context.user_data.get("last_start_message", {})
            new_message = {"text": text, "reply_markup": reply_markup.to_dict()}
            try:
                if update.callback_query:
                    if current_message != new_message:
                        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
                        context.user_data["last_start_message"] = new_message
                    await update.callback_query.answer()
                else:
                    await update.message.reply_text(text, reply_markup=reply_markup)
                    context.user_data["last_start_message"] = new_message
            except BadRequest as e:
                if "Message is not modified" not in str(e):
                    logger.error(f"Ошибка отправки сообщения: {e}")
                    ERRORS.labels(type="telegram_api").inc()
            await log_analytics(user_id, "start")
            context.user_data["state"] = STATE_MAIN_MENU
            return STATE_MAIN_MENU


async def check_environment():
    """Проверка переменных окружения."""
    required_vars = ["BOT_TOKEN", "POSTGRES_URL", "SPLIT_API_TOKEN", "PROVIDER_TOKEN", "OWNER_WALLET", "WEBHOOK_URL"]
    optional_vars = ["TON_SPACE_API_TOKEN", "CRYPTOBOT_API_TOKEN", "TON_API_KEY"]
    for var in required_vars:
        if not os.getenv(var):
            logger.error(f"Отсутствует обязательная переменная окружения: {var}")
            raise ValueError(f"Переменная окружения {var} не установлена")
    for var in optional_vars:
        if not os.getenv(var):
            logger.warning(f"Опциональная переменная окружения {var} не установлена")

async def get_db_pool():
    """Получение пула соединений с базой данных."""
    global _db_pool
    async with _db_pool_lock:
        if _db_pool is None:
            logger.info("Создание нового пула базы данных")
            if not POSTGRES_URL:
                logger.error("POSTGRES_URL or DATABASE_URL not set in environment variables")
                raise ValueError("POSTGRES_URL or DATABASE_URL not set")
            try:
                _db_pool = await asyncpg.create_pool(POSTGRES_URL, min_size=1, max_size=10)
                logger.info("Пул базы данных успешно инициализирован")
            except Exception as e:
                logger.error(f"Ошибка создания пула базы данных: {e}", exc_info=True)
                raise
        # Проверка жизнеспособности пула
        try:
            async with _db_pool.acquire() as conn:
                await conn.execute("SELECT 1")
            logger.debug("Пул базы данных активен")
        except Exception as e:
            logger.warning(f"Пул базы данных недоступен: {e}. Пересоздание пула.")
            try:
                _db_pool = await asyncpg.create_pool(POSTGRES_URL, min_size=1, max_size=10)
                logger.info("Пул базы данных пересоздан")
            except Exception as e:
                logger.error(f"Ошибка пересоздания пула базы данных: {e}", exc_info=True)
                raise
        return _db_pool
        
async def close_db_pool():
    """Закрытие пула соединений с базой данных."""
    global _db_pool
    if _db_pool is not None and not _db_pool.closed:
        await _db_pool.close()
        logger.info("Пул базы данных закрыт")
        _db_pool = None

async def ensure_db_pool():
    """Обеспечение доступности пула базы данных с повторными попытками."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            return await get_db_pool()
        except Exception as e:
            logger.error(f"Попытка {attempt + 1}/{max_retries} подключения к БД не удалась: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
            else:
                raise

async def init_db():
    """Инициализация структуры базы данных."""
    logger.info("Инициализация базы данных")
    async with (await ensure_db_pool()) as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                stars_bought INTEGER DEFAULT 0,
                ref_bonus_ton FLOAT DEFAULT 0,
                referrals JSONB DEFAULT '[]',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_new BOOLEAN DEFAULT TRUE,
                is_admin BOOLEAN DEFAULT FALSE
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value JSONB
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS texts (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                stars INTEGER,
                amount_ton FLOAT,
                amount_usd FLOAT,
                payment_method TEXT,
                recipient TEXT,
                status TEXT,
                invoice_id TEXT,
                payload TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS admin_logs (
                id SERIAL PRIMARY KEY,
                admin_id BIGINT,
                action TEXT,
                details JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS analytics (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                action TEXT,
                details JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await conn.execute('''
            INSERT INTO settings (key, value) VALUES
                ('admin_ids', $1),
                ('stars_price_usd', $2),
                ('ton_exchange_rate', $3),
                ('markup_ton_space', $4),
                ('markup_cryptobot_crypto', $5),
                ('markup_cryptobot_card', $6),
                ('markup_ref_bonus', $7),
                ('min_stars_purchase', $8),
                ('ton_space_commission', $9),
                ('card_commission', $10),
                ('profit_percent', $11)
            ON CONFLICT (key) DO NOTHING
        ''', json.dumps([TWIN_ACCOUNT_ID, YOUR_TEST_ACCOUNT_ID]), 
             json.dumps(PRICE_USD_PER_50 / 50), 
             json.dumps(2.93), 
             json.dumps(20), 
             json.dumps(25), 
             json.dumps(25), 
             json.dumps(5), 
             json.dumps(10), 
             json.dumps(15), 
             json.dumps(10), 
             json.dumps(10))
        await conn.execute('''
            INSERT INTO texts (key, value) VALUES
                ('welcome', 'Добро пожаловать в @CheapStarsShop! Купите Telegram Stars за TON.\nЗвезд продано: {stars_sold}\nВы купили: {stars_bought} звезд'),
                ('buy_prompt', 'Оплатите {amount_ton:.6f} TON\nСсылка: {address}\nДля: @{recipient}\nЗвезды: {stars}\nМетод: {method}'),
                ('profile', '👤 Профиль\nЗвезд куплено: {stars_bought}\nРеф. бонус: {ref_bonus_ton} TON\nРефералов: {ref_count}'),
                ('referrals', '🤝 Рефералы\nВаша ссылка: {ref_link}\nРефералов: {ref_count}\nБонус: {ref_bonus_ton} TON'),
                ('tech_support', '🛠 Поддержка: {support_channel}'),
                ('reviews', '📝 Новости и отзывы: {news_channel}'),
                ('buy_success', 'Оплата прошла! @{recipient} получил {stars} звезд.'),
                ('user_info', 'Пользователь: @{username}\nID: {user_id}\nЗвезд куплено: {stars_bought}\nРеф. бонус: {ref_bonus_ton} TON\nРефералов: {ref_count}')
            ON CONFLICT (key) DO NOTHING
        ''')

async def get_setting(key):
    """Получение настройки из базы данных."""
    async with (await ensure_db_pool()) as conn:
        result = await conn.fetchrow("SELECT value FROM settings WHERE key = $1", key)
        return json.loads(result["value"]) if result else None

async def update_setting(key, value):
    """Обновление настройки в базе данных."""
    async with (await ensure_db_pool()) as conn:
        await conn.execute(
            "INSERT INTO settings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2",
            key, json.dumps(value)
        )
        logger.info(f"Настройка {key} обновлена: {value}")

async def get_text(key, **kwargs):
    """Получение текста из базы данных с форматированием."""
    async with (await ensure_db_pool()) as conn:
        result = await conn.fetchrow("SELECT value FROM texts WHERE key = $1", key)
        text = result["value"] if result else ""
        return text.format(**kwargs) if text else f"Текст для {key} не найден"

async def update_text(key, value):
    """Обновление текста в базе данных."""
    async with (await ensure_db_pool()) as conn:
        await conn.execute(
            "INSERT INTO texts (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2",
            key, value
        )
        logger.info(f"Текст {key} обновлен: {value}")

async def log_admin_action(admin_id, action, details=None):
    """Логирование действий администратора."""
    async with (await ensure_db_pool()) as conn:
        await conn.execute(
            "INSERT INTO admin_logs (admin_id, action, details) VALUES ($1, $2, $3)",
            admin_id, action, json.dumps(details or {})
        )
        logger.info(f"Действие админа: admin_id={admin_id}, action={action}")

async def log_analytics(user_id, action, details=None):
    """Логирование аналитики пользователя."""
    async with (await ensure_db_pool()) as conn:
        await conn.execute(
            "INSERT INTO analytics (user_id, action, details) VALUES ($1, $2, $3)",
            user_id, action, json.dumps(details or {})
        )
        logger.info(f"Аналитика: user_id={user_id}, action={action}")

async def update_ton_price(context: ContextTypes.DEFAULT_TYPE):
    """Обновление курса TON."""
    try:
        logger.info("Fetching TON price")
        async with aiohttp.ClientSession(timeout=ClientTimeout(total=10)) as session:
            url = "https://tonapi.io/v2/rates?tokens=ton&currencies=usd"
            headers = {"Authorization": f"Bearer {TON_API_KEY}"}
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    ton_price = float(data["rates"]["TON"]["prices"]["USD"])
                    context.bot_data["ton_price"] = ton_price
                    logger.info(f"TON price updated: {ton_price}")
                    await log_analytics(0, "ton_price_updated", {"price": ton_price, "source": "tonapi"})
                    return
                logger.error(f"tonapi.io error: {response.status}")
                ERRORS.labels(type="ton_api").inc()

            logger.info("Falling back to CoinGecko API")
            async with session.get("https://api.coingecko.com/api/v3/simple/price?ids=the-open-network&vs_currencies=usd") as response:
                if response.status == 200:
                    data = await response.json()
                    ton_price = float(data["the-open-network"]["usd"])
                    context.bot_data["ton_price"] = ton_price
                    logger.info(f"TON price updated via CoinGecko: {ton_price}")
                    await log_analytics(0, "ton_price_updated", {"price": ton_price, "source": "coingecko"})
                    return
                logger.error(f"CoinGecko API error: {response.status}")
                ERRORS.labels(type="ton_api").inc()

            context.bot_data["ton_price"] = await get_setting("ton_exchange_rate") or 2.85
            logger.info(f"Using fallback TON price: {context.bot_data['ton_price']}")
    except Exception as e:
        logger.error(f"Ошибка в update_ton_price: {e}", exc_info=True)
        ERRORS.labels(type="ton_api").inc()
        context.bot_data["ton_price"] = await get_setting("ton_exchange_rate") or 2.85
        logger.info(f"Using fallback TON price: {context.bot_data['ton_price']}")

async def get_account_state(address: str) -> dict:
    """Получение состояния аккаунта TON."""
    REQUESTS.labels(endpoint="get_account_state").inc()
    with RESPONSE_TIME.labels(endpoint="get_account_state").time():
        url = f"https://toncenter.com/api/v3/account/states?address={address}"
        headers = {"Authorization": f"Bearer {TON_API_KEY}"}
        async with aiohttp.ClientSession(timeout=ClientTimeout(total=10)) as session:
            try:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data["states"][0] if data["states"] else {}
                    logger.error(f"Ошибка TON API: {response.status}, {await response.text()}")
                    ERRORS.labels(type="ton_api").inc()
                    return {}
            except Exception as e:
                logger.error(f"Ошибка получения состояния аккаунта: {e}")
                ERRORS.labels(type="ton_api").inc()
                return {}

async def check_transaction(address: str, amount_ton: float, payload: str) -> dict:
    """Проверка транзакции TON."""
    REQUESTS.labels(endpoint="check_transaction").inc()
    with RESPONSE_TIME.labels(endpoint="check_transaction").time():
        url = f"https://toncenter.com/api/v3/transactions?account={address}&limit=10"
        headers = {"Authorization": f"Bearer {TON_API_KEY}"}
        async with aiohttp.ClientSession(timeout=ClientTimeout(total=10)) as session:
            try:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        transactions = (await response.json())["transactions"]
                        amount_nano = int(amount_ton * 1_000_000_000)
                        for tx in transactions:
                            if tx["in_msg"]["value"] == str(amount_nano) and tx["in_msg"].get("comment") == payload:
                                return tx
                        return {}
                    logger.error(f"Ошибка TON API: {response.status}")
                    ERRORS.labels(type="ton_api").inc()
                    return {}
            except Exception as e:
                logger.error(f"Ошибка проверки транзакции: {e}")
                ERRORS.labels(type="ton_api").inc()
                return {}

async def generate_payload(user_id):
    """Генерация уникального payload для транзакции."""
    rand = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return f"{user_id}_{rand}"

async def create_cryptobot_invoice(amount, currency, user_id, stars, recipient, payload):
    """Создание инвойса через CryptoBot."""
    REQUESTS.labels(endpoint="create_cryptobot_invoice").inc()
    with RESPONSE_TIME.labels(endpoint="create_cryptobot_invoice").time():
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if not CRYPTOBOT_API_TOKEN or not PROVIDER_TOKEN:
                    logger.warning("CRYPTOBOT_API_TOKEN или PROVIDER_TOKEN не установлены, переход к прямому TON-платежу")
                    return None, None
                headers = {"Crypto-Pay-API-Token": CRYPTOBOT_API_TOKEN}
                payload_data = {
                    "amount": str(amount),
                    "currency": currency,
                    "description": f"Покупка {stars} звезд для @{recipient.lstrip('@')}",
                    "payload": payload
                }
                async with aiohttp.ClientSession(timeout=ClientTimeout(total=10)) as session:
                    async with session.post(f"{CRYPTOBOT_API_URL}/createInvoice", headers=headers, json=payload_data) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data.get("ok"):
                                invoice = data["result"]
                                async with (await ensure_db_pool()) as conn:
                                    await conn.execute(
                                        "INSERT INTO transactions (user_id, stars, amount_usd, amount_ton, payment_method, recipient, status, invoice_id, payload) "
                                        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                                        user_id, stars, float(amount) if currency == "USD" else 0.0, float(amount) if currency == "TON" else 0.0, 
                                        f"cryptobot_{currency.lower()}", recipient, "pending", invoice["invoice_id"], payload
                                    )
                                logger.info(f"Инвойс CryptoBot создан: invoice_id={invoice['invoice_id']}")
                                return invoice["invoice_id"], invoice["pay_url"]
                            logger.error(f"Ошибка CryptoBot API: {data}")
                            ERRORS.labels(type="cryptobot_api").inc()
                            if attempt < max_retries - 1:
                                await asyncio.sleep(2)
                                continue
                            return None, None
                        logger.error(f"Ошибка CryptoBot API: status={resp.status}")
                        ERRORS.labels(type="cryptobot_api").inc()
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2)
                            continue
                        return None, None
            except Exception as e:
                logger.error(f"Ошибка создания инвойса CryptoBot: {e}")
                ERRORS.labels(type="cryptobot_api").inc()
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                continue
        return None, None

async def create_ton_space_invoice(amount_ton, user_id, stars, recipient, payload):
    """Создание инвойса через TON Space."""
    REQUESTS.labels(endpoint="create_ton_space_invoice").inc()
    with RESPONSE_TIME.labels(endpoint="create_ton_space_invoice").time():
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if not TON_SPACE_API_TOKEN:
                    logger.warning("TON_SPACE_API_TOKEN не установлен, переход к прямому TON-платежу")
                    pay_url = f"ton://transfer/{OWNER_WALLET}?amount={int(amount_ton * 1_000_000_000)}&text={payload}"
                    async with (await ensure_db_pool()) as conn:
                        await conn.execute(
                            "INSERT INTO transactions (user_id, stars, amount_ton, payment_method, recipient, status, invoice_id, payload) "
                            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                            user_id, stars, amount_ton, "ton_space_direct", recipient, "pending", payload, payload
                        )
                    logger.info(f"Прямой TON-платеж создан: payload={payload}")
                    return payload, pay_url
                headers = {"Authorization": f"Bearer {TON_SPACE_API_TOKEN}"}
                payload_data = {
                    "amount": str(amount_ton),
                    "currency": "TON",
                    "description": f"Покупка {stars} звезд для @{recipient.lstrip('@')}",
                    "callback_url": f"{WEBHOOK_URL}/callback",
                    "metadata": json.dumps({"user_id": user_id, "stars": stars, "recipient": recipient, "payload": payload})
                }
                async with aiohttp.ClientSession(timeout=ClientTimeout(total=10)) as session:
                    async with session.post(f"{TON_SPACE_API_URL}/invoices", headers=headers, json=payload_data) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data.get("status") == "success":
                                invoice = data["invoice"]
                                async with (await ensure_db_pool()) as conn:
                                    await conn.execute(
                                        "INSERT INTO transactions (user_id, stars, amount_ton, payment_method, recipient, status, invoice_id, payload) "
                                        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                                        user_id, stars, amount_ton, "ton_space_api", recipient, "pending", invoice["id"], payload
                                    )
                                logger.info(f"Инвойс TON Space создан: invoice_id={invoice['id']}")
                                return invoice["id"], invoice["pay_url"]
                            logger.error(f"Ошибка TON Space API: {data}")
                            ERRORS.labels(type="ton_space_api").inc()
                            if attempt < max_retries - 1:
                                await asyncio.sleep(2)
                                continue
                            return None, None
                        logger.error(f"Ошибка TON Space API: status={resp.status}")
                        ERRORS.labels(type="ton_space_api").inc()
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2)
                            continue
                        return None, None
            except Exception as e:
                logger.error(f"Ошибка создания инвойса TON Space: {e}")
                ERRORS.labels(type="ton_space_api").inc()
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                continue
        return None, None

async def check_ton_space_payment(invoice_id):
    """Проверка оплаты через TON Space."""
    REQUESTS.labels(endpoint="check_ton_space_payment").inc()
    with RESPONSE_TIME.labels(endpoint="check_ton_space_payment").time():
        if not TON_SPACE_API_TOKEN:
            logger.warning("TON_SPACE_API_TOKEN не установлен, невозможно проверить оплату")
            return False
        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with aiohttp.ClientSession(timeout=ClientTimeout(total=10)) as session:
                    headers = {"Authorization": f"Bearer {TON_SPACE_API_TOKEN}"}
                    async with session.get(f"{TON_SPACE_API_URL}/invoices/{invoice_id}", headers=headers) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data.get("status") == "success":
                                invoice = data["invoice"]
                                status = invoice["status"] == "paid"
                                logger.info(f"Оплата TON Space проверена: invoice_id={invoice_id}, status={status}")
                                return status
                            logger.error(f"Ошибка проверки оплаты TON Space: {data}")
                            ERRORS.labels(type="ton_space_api").inc()
                            if attempt < max_retries - 1:
                                await asyncio.sleep(2)
                                continue
                            return False
                        logger.error(f"Ошибка TON Space API: status={resp.status}")
                        ERRORS.labels(type="ton_space_api").inc()
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2)
                            continue
                        return False
            except Exception as e:
                logger.error(f"Ошибка проверки оплаты TON Space: {e}")
                ERRORS.labels(type="ton_space_api").inc()
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                continue
        return False

async def check_cryptobot_payment(invoice_id):
    """Проверка оплаты через CryptoBot."""
    REQUESTS.labels(endpoint="check_cryptobot_payment").inc()
    with RESPONSE_TIME.labels(endpoint="check_cryptobot_payment").time():
        if not CRYPTOBOT_API_TOKEN:
            logger.warning("CRYPTOBOT_API_TOKEN не установлен, невозможно проверить оплату")
            return False
        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with aiohttp.ClientSession(timeout=ClientTimeout(total=10)) as session:
                    headers = {"Crypto-Pay-API-Token": CRYPTOBOT_API_TOKEN}
                    async with session.get(f"{CRYPTOBOT_API_URL}/getInvoices?invoice_ids={invoice_id}", headers=headers) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data.get("ok"):
                                invoice = data["result"]["items"][0]
                                status = invoice["status"] == "paid"
                                logger.info(f"Оплата CryptoBot проверена: invoice_id={invoice_id}, status={status}")
                                return status
                            logger.error(f"Ошибка проверки оплаты CryptoBot: {data}")
                            ERRORS.labels(type="cryptobot_api").inc()
                            if attempt < max_retries - 1:
                                await asyncio.sleep(2)
                                continue
                            return False
                        logger.error(f"Ошибка CryptoBot API: status={resp.status}")
                        ERRORS.labels(type="cryptobot_api").inc()
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2)
                            continue
                        return False
            except Exception as e:
                logger.error(f"Ошибка проверки оплаты CryptoBot: {e}")
                ERRORS.labels(type="cryptobot_api").inc()
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                continue
        return False

async def issue_stars(recipient_username, stars, user_id):
    """Выдача звезд пользователю."""
    REQUESTS.labels(endpoint="issue_stars").inc()
    with RESPONSE_TIME.labels(endpoint="issue_stars").time():
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if not SPLIT_API_TOKEN:
                    logger.error("SPLIT_API_TOKEN не установлен")
                    ERRORS.labels(type="split_api").inc()
                    return False
                async with aiohttp.ClientSession(timeout=ClientTimeout(total=10)) as session:
                    headers = {"Authorization": f"Bearer {SPLIT_API_TOKEN}"}
                    payload = {
                        "username": recipient_username.lstrip("@"),
                        "stars": stars
                    }
                    async with session.post(SPLIT_API_URL, headers=headers, json=payload) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data.get("success"):
                                logger.info(f"Звезды выданы: {stars} для @{recipient_username} от user_id={user_id}")
                                return True
                            logger.error(f"Ошибка Split API: {data}")
                            ERRORS.labels(type="split_api").inc()
                            if attempt < max_retries - 1:
                                await asyncio.sleep(2)
                                continue
                            return False
                        logger.error(f"Ошибка Split API: status={resp.status}")
                        ERRORS.labels(type="split_api").inc()
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2)
                            continue
                        return False
            except Exception as e:
                logger.error(f"Ошибка выдачи звезд: {e}")
                ERRORS.labels(type="split_api").inc()
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                continue
        return False

async def start_bot():
    """Запуск бота с вебхуком и планировщиком."""
    global app
    try:
        await check_environment()
        await init_db()
        app = ApplicationBuilder().token(BOT_TOKEN).build()

        scheduler = AsyncIOScheduler()
        scheduler.add_job(
            keep_alive,
            trigger="interval",
            minutes=1,
            args=[app],  # Pass the Application instance directly
            timezone=pytz.UTC
        )
        scheduler.add_job(
            update_ton_price,
            trigger="interval",
            minutes=5,
            args=[app.context_types.context],
            timezone=pytz.UTC
        )
        scheduler.start()
        logger.info("Планировщик запущен")

        conv_handler = ConversationHandler(
            entry_points=[
                CommandHandler("start", start),
                CallbackQueryHandler(callback_query_handler)
            ],
            states={
                STATE_MAIN_MENU: [
                    CallbackQueryHandler(callback_query_handler),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, start)
                ],
                STATE_PROFILE: [CallbackQueryHandler(callback_query_handler)],
                STATE_REFERRALS: [CallbackQueryHandler(callback_query_handler)],
                STATE_BUY_STARS_RECIPIENT: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATE_BUY_STARS_AMOUNT: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATE_BUY_STARS_PAYMENT_METHOD: [CallbackQueryHandler(callback_query_handler)],
                STATE_BUY_STARS_CRYPTO_TYPE: [CallbackQueryHandler(callback_query_handler)],
                STATE_BUY_STARS_CONFIRM: [CallbackQueryHandler(callback_query_handler)],
                STATE_ADMIN_PANEL: [CallbackQueryHandler(callback_query_handler)],
                STATE_ADMIN_EDIT_TEXTS: [CallbackQueryHandler(callback_query_handler)],
                STATE_EDIT_TEXT: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATE_ADMIN_USER_STATS: [CallbackQueryHandler(callback_query_handler)],
                STATE_LIST_USERS: [CallbackQueryHandler(callback_query_handler)],
                STATE_EDIT_USER: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATE_ADMIN_EDIT_MARKUP: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATE_ADMIN_MANAGE_ADMINS: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATE_ADMIN_EDIT_PROFIT: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATE_EXPORT_DATA: [CallbackQueryHandler(callback_query_handler)],
                STATE_VIEW_LOGS: [CallbackQueryHandler(callback_query_handler)],
                STATE_TOP_REFERRALS: [CallbackQueryHandler(callback_query_handler)],
                STATE_TOP_PURCHASES: [CallbackQueryHandler(callback_query_handler)]
            },
            fallbacks=[CommandHandler("start", start)]
        )

        app.add_handler(conv_handler)
        app.add_error_handler(error_handler)

        start_http_server(9090)
        logger.info("Сервер Prometheus запущен на порту 9090")

        webhook_url = f"{WEBHOOK_URL}/webhook"
        await app.bot.set_webhook(webhook_url)
        logger.info(f"Вебхук установлен: {webhook_url}")

        web_app = web.Application()
        web_app.router.add_post("/webhook", handle_webhook)
        runner = web.AppRunner(web_app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", PORT)
        await site.start()
        logger.info(f"Веб-сервер запущен на порту {PORT}")

        await app.initialize()
        await app.start()
        logger.info("Бот успешно запущен")

        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            logger.info("Получен сигнал остановки")
            await app.stop()
            await app.shutdown()
            await close_db_pool()
            scheduler.shutdown()
            await runner.cleanup()
            logger.info("Бот остановлен")
    except Exception as e:
        logger.error(f"Ошибка запуска бота: {e}", exc_info=True)
        ERRORS.labels(type="startup").inc()
        raise

async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик профиля пользователя."""
    REQUESTS.labels(endpoint="profile").inc()
    with RESPONSE_TIME.labels(endpoint="profile").time():
        user_id = update.effective_user.id
        async with (await ensure_db_pool()) as conn:
            user = await conn.fetchrow("SELECT stars_bought, ref_bonus_ton, referrals FROM users WHERE user_id = $1", user_id)
            if user:
                stars_bought, ref_bonus_ton, referrals = user
                ref_count = len(json.loads(referrals)) if referrals != '[]' else 0
                text = await get_text("profile", stars_bought=stars_bought, ref_bonus_ton=ref_bonus_ton, ref_count=ref_count)
                keyboard = [
                    [InlineKeyboardButton("🏆 Топ рефералов", callback_data=TOP_REFERRALS),
                     InlineKeyboardButton("🏆 Топ покупок", callback_data=TOP_PURCHASES)],
                    [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                try:
                    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
                except BadRequest as e:
                    if "Message is not modified" not in str(e):
                        logger.error(f"Ошибка редактирования сообщения: {e}")
                        ERRORS.labels(type="telegram_api").inc()
                context.user_data["state"] = STATE_PROFILE
                await update.callback_query.answer()
                await log_analytics(user_id, "view_profile")
                return STATE_PROFILE
            await update.callback_query.answer(text="Пользователь не найден.")
            return STATE_MAIN_MENU

async def referrals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик реферального меню."""
    REQUESTS.labels(endpoint="referrals").inc()
    with RESPONSE_TIME.labels(endpoint="referrals").time():
        user_id = update.effective_user.id
        async with (await ensure_db_pool()) as conn:
            user = await conn.fetchrow("SELECT ref_bonus_ton, referrals FROM users WHERE user_id = $1", user_id)
            if user:
                ref_bonus_ton, referrals = user
                ref_count = len(json.loads(referrals)) if referrals != '[]' else 0
                ref_link = f"https://t.me/{context.bot.username}?start=ref_{user_id}"
                text = await get_text("referrals", ref_link=ref_link, ref_count=ref_count, ref_bonus_ton=ref_bonus_ton)
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                try:
                    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
                except BadRequest as e:
                    if "Message is not modified" not in str(e):
                        logger.error(f"Ошибка редактирования сообщения: {e}")
                        ERRORS.labels(type="telegram_api").inc()
                context.user_data["state"] = STATE_REFERRALS
                await update.callback_query.answer()
                await log_analytics(user_id, "view_referrals")
                return STATE_REFERRALS
            await update.callback_query.answer(text="Пользователь не найден.")
            return STATE_MAIN_MENU

async def support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик техподдержки."""
    REQUESTS.labels(endpoint="support").inc()
    with RESPONSE_TIME.labels(endpoint="support").time():
        text = await get_text("tech_support", support_channel=SUPPORT_CHANNEL)
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        try:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                logger.error(f"Ошибка редактирования сообщения: {e}")
                ERRORS.labels(type="telegram_api").inc()
        context.user_data["state"] = STATE_MAIN_MENU
        await update.callback_query.answer()
        await log_analytics(update.effective_user.id, "view_support")
        return STATE_MAIN_MENU

async def reviews(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик отзывов."""
    REQUESTS.labels(endpoint="reviews").inc()
    with RESPONSE_TIME.labels(endpoint="reviews").time():
        text = await get_text("reviews", news_channel=NEWS_CHANNEL)
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        try:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                logger.error(f"Ошибка редактирования сообщения: {e}")
                ERRORS.labels(type="telegram_api").inc()
        context.user_data["state"] = STATE_MAIN_MENU
        await update.callback_query.answer()
        await log_analytics(update.effective_user.id, "view_reviews")
        return STATE_MAIN_MENU

async def buy_stars(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик покупки звезд."""
    REQUESTS.labels(endpoint="buy_stars").inc()
    with RESPONSE_TIME.labels(endpoint="buy_stars").time():
        context.user_data["buy_data"] = {}
        text = "Выберите параметры покупки:"
        buy_data = context.user_data["buy_data"]
        recipient_display = buy_data.get("recipient", "-").lstrip("@")
        keyboard = [
            [InlineKeyboardButton(f"Кому звезды: @{recipient_display}", callback_data=SET_RECIPIENT)],
            [InlineKeyboardButton(f"Количество звезд: {buy_data.get('stars', '-')}", callback_data=SET_AMOUNT)],
            [InlineKeyboardButton(f"Метод оплаты: {buy_data.get('payment_method', '-')}", callback_data=SET_PAYMENT)],
            [InlineKeyboardButton(f"Цена: {buy_data.get('amount_ton', '-')}", callback_data=CONFIRM_PAYMENT)],
            [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        try:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                logger.error(f"Ошибка редактирования сообщения: {e}")
                ERRORS.labels(type="telegram_api").inc()
        context.user_data["state"] = STATE_BUY_STARS_RECIPIENT
        await update.callback_query.answer()
        await log_analytics(update.effective_user.id, "start_buy_stars")
        return STATE_BUY_STARS_RECIPIENT

async def set_recipient(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик установки получателя звезд."""
    REQUESTS.labels(endpoint="set_recipient").inc()
    with RESPONSE_TIME.labels(endpoint="set_recipient").time():
        text = "Введите username получателя звезд (например, @username):"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        try:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                logger.error(f"Ошибка редактирования сообщения: {e}")
                ERRORS.labels(type="telegram_api").inc()
        context.user_data["input_state"] = "recipient"
        context.user_data["state"] = STATE_BUY_STARS_RECIPIENT
        await update.callback_query.answer()
        await log_analytics(update.effective_user.id, "set_recipient")
        return STATE_BUY_STARS_RECIPIENT

async def set_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик установки количества звезд."""
    REQUESTS.labels(endpoint="set_amount").inc()
    with RESPONSE_TIME.labels(endpoint="set_amount").time():
        text = "Введите количество звезд (кратно 50):"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        try:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                logger.error(f"Ошибка редактирования сообщения: {e}")
                ERRORS.labels(type="telegram_api").inc()
        context.user_data["input_state"] = "amount"
        context.user_data["state"] = STATE_BUY_STARS_AMOUNT
        await update.callback_query.answer()
        await log_analytics(update.effective_user.id, "set_amount")
        return STATE_BUY_STARS_AMOUNT

async def set_payment_method(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик выбора метода оплаты."""
    REQUESTS.labels(endpoint="set_payment_method").inc()
    with RESPONSE_TIME.labels(endpoint="set_payment_method").time():
        buy_data = context.user_data.get("buy_data", {})
        if not buy_data.get("recipient") or not buy_data.get("stars"):
            try:
                await update.callback_query.message.reply_text("Сначала выберите получателя и количество звезд!")
            except Exception as e:
                logger.error(f"Ошибка отправки сообщения: {e}")
                ERRORS.labels(type="telegram_api").inc()
            await log_analytics(update.effective_user.id, "set_payment_method_error", {"error": "missing_data"})
            return STATE_BUY_STARS_RECIPIENT
        text = "Выберите метод оплаты:"
        keyboard = [
            [InlineKeyboardButton("Криптовалюта (+25%)", callback_data=SELECT_CRYPTO_TYPE)],
            [InlineKeyboardButton("Карта (+25%)", callback_data=PAY_CARD)],
            [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        try:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                logger.error(f"Ошибка редактирования сообщения: {e}")
                ERRORS.labels(type="telegram_api").inc()
        context.user_data["state"] = STATE_BUY_STARS_PAYMENT_METHOD
        await update.callback_query.answer()
        await log_analytics(update.effective_user.id, "select_payment_method")
        return STATE_BUY_STARS_PAYMENT_METHOD

async def select_crypto_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбор типа криптовалюты."""
    user_id = update.effective_user.id
    text = "Выберите криптовалюту:"
    reply_markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("TON Space", callback_data=PAY_TON_SPACE)],
        [InlineKeyboardButton("CryptoBot", callback_data=PAY_CRYPTOBOT)],
        [InlineKeyboardButton("Назад", callback_data=SET_PAYMENT)]
    ])
    try:
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    except BadRequest as e:
        if "Message is not modified" not in str(e):
            logger.error(f"Ошибка редактирования сообщения: {e}")
            ERRORS.labels(type="telegram_api").inc()
    await update.callback_query.answer()
    await log_analytics(user_id, "select_crypto_type")
    return STATE_BUY_STARS_CRYPTO_TYPE

async def confirm_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение оплаты."""
    user_id = update.effective_user.id
    buy_data = context.user_data.get("buy_data", {})
    required_fields = ["stars", "recipient", "payment_method", "amount_ton", "pay_url", "invoice_id", "payload"]
    missing_fields = [field for field in required_fields if field not in buy_data]
    if missing_fields:
        try:
            await update.callback_query.message.reply_text(
                f"Ошибка: отсутствуют данные для {', '.join(missing_fields)}."
            )
        except Exception as e:
            logger.error(f"Ошибка отправки сообщения: {e}")
            ERRORS.labels(type="telegram_api").inc()
        await log_analytics(user_id, "confirm_payment_error", {"missing_fields": missing_fields})
        return STATE_BUY_STARS_PAYMENT_METHOD
    stars = buy_data["stars"]
    recipient = buy_data["recipient"]
    payment_method = buy_data["payment_method"]
    amount_ton = buy_data["amount_ton"]
    pay_url = buy_data["pay_url"]
    text = (
        f"Подтверждение покупки:\n"
        f"Звезды: {stars} ⭐\n"
        f"Получатель: {recipient}\n"
        f"Метод оплаты: {payment_method}\n"
        f"Сумма: {amount_ton:.6f} TON\n"
        f"Ссылка для оплаты: {pay_url}\n"
        f"Нажмите 'Оплатить' после выполнения оплаты."
    )
    reply_markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("Оплатить", callback_data=CHECK_PAYMENT)],
        [InlineKeyboardButton("Назад", callback_data=BUY_STARS)]
    ])
    try:
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    except BadRequest as e:
        if "Message is not modified" not in str(e):
            logger.error(f"Ошибка редактирования сообщения: {e}")
            ERRORS.labels(type="telegram_api").inc()
    await update.callback_query.answer()
    await log_analytics(user_id, "confirm_payment")
    return STATE_BUY_STARS_CONFIRM

async def check_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик проверки оплаты."""
    REQUESTS.labels(endpoint="check_payment").inc()
    with RESPONSE_TIME.labels(endpoint="check_payment").time():
        user_id = update.effective_user.id
        buy_data = context.user_data.get("buy_data", {})
        required_fields = ["recipient", "stars", "payment_method", "invoice_id", "amount_ton", "payload"]
        missing_fields = [field for field in required_fields if field not in buy_data]
        if missing_fields:
            try:
                await update.callback_query.message.reply_text("Неполные данные для проверки оплаты. Начните заново.")
            except Exception as e:
                logger.error(f"Ошибка отправки сообщения: {e}")
                ERRORS.labels(type="telegram_api").inc()
            await log_analytics(user_id, "check_payment_error", {"error": "missing_data"})
            return STATE_BUY_STARS_RECIPIENT
        recipient = buy_data["recipient"]
        stars = buy_data["stars"]
        payment_method = buy_data["payment_method"]
        invoice_id = buy_data["invoice_id"]
        payload = buy_data["payload"]
        amount_ton = buy_data["amount_ton"]
        success = False
        async with (await ensure_db_pool()) as conn:
            if payment_method == "ton_space_api":
                success = await check_ton_space_payment(invoice_id)
            elif payment_method in ["cryptobot_usd", "cryptobot_ton"]:
                success = await check_cryptobot_payment(invoice_id)
            elif payment_method == "ton_space_direct":
                account_state = await get_account_state(OWNER_WALLET)
                if account_state.get("state") == "active":
                    tx = await check_transaction(OWNER_WALLET, amount_ton, payload)
                    success = bool(tx)
                    transaction_cache[payload] = tx
            if success:
                if await issue_stars(recipient, stars, user_id):
                    await conn.execute(
                        "UPDATE users SET stars_bought = stars_bought + $1 WHERE user_id = $2",
                        stars, user_id
                    )
                    await conn.execute(
                        "UPDATE transactions SET status = $1 WHERE invoice_id = $2",
                        "completed", invoice_id
                    )
                    text = await get_text("buy_success", recipient=recipient.lstrip("@"), stars=stars)
                    try:
                        await update.callback_query.message.reply_text(text)
                        await context.bot.send_message(
                            chat_id=NEWS_CHANNEL,
                            text=f"Успешная покупка: @{recipient.lstrip('@')} получил {stars} звезд от user_id={user_id}"
                        )
                    except Exception as e:
                        logger.error(f"Ошибка отправки сообщения: {e}")
                        ERRORS.labels(type="telegram_api").inc()
                    context.user_data.clear()
                    context.user_data["state"] = STATE_MAIN_MENU
                    await start(update, context)
                    await update.callback_query.answer()
                    await log_analytics(user_id, "payment_success", {"stars": stars, "recipient": recipient})
                    return STATE_MAIN_MENU
                else:
                    try:
                        await update.callback_query.message.reply_text("Ошибка выдачи звезд. Обратитесь в поддержку.")
                    except Exception as e:
                        logger.error(f"Ошибка отправки сообщения: {e}")
                        ERRORS.labels(type="telegram_api").inc()
                    await log_analytics(user_id, "issue_stars_error", {"recipient": recipient, "stars": stars})
            else:
                try:
                    await update.callback_query.message.reply_text("Оплата не подтверждена. Попробуйте снова.")
                except Exception as e:
                    logger.error(f"Ошибка отправки сообщения: {e}")
                    ERRORS.labels(type="telegram_api").inc()
                await log_analytics(user_id, "payment_not_confirmed", {"invoice_id": invoice_id})
            await update.callback_query.answer()
            return STATE_BUY_STARS_CONFIRM

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Панель администратора."""
    REQUESTS.labels(endpoint="admin_panel").inc()
    with RESPONSE_TIME.labels(endpoint="admin_panel").time():
        user_id = update.effective_user.id
        admin_ids = await get_setting("admin_ids") or [TWIN_ACCOUNT_ID, YOUR_TEST_ACCOUNT_ID]
        if user_id not in admin_ids:
            await update.callback_query.answer(text="Доступ запрещен.")
            return STATE_MAIN_MENU
        text = "Панель администратора:"
        keyboard = [
            [InlineKeyboardButton("📊 Статистика", callback_data=ADMIN_STATS)],
            [InlineKeyboardButton("📝 Редактировать тексты", callback_data=ADMIN_EDIT_TEXTS)],
            [InlineKeyboardButton("👥 Статистика пользователей", callback_data=ADMIN_USER_STATS)],
            [InlineKeyboardButton("💰 Редактировать наценку", callback_data=ADMIN_EDIT_MARKUP)],
            [InlineKeyboardButton("👤 Управление админами", callback_data=ADMIN_MANAGE_ADMINS)],
            [InlineKeyboardButton("📈 Редактировать прибыль", callback_data=ADMIN_EDIT_PROFIT)],
            [InlineKeyboardButton("📤 Экспорт данных", callback_data=EXPORT_DATA)],
            [InlineKeyboardButton("📜 Логи", callback_data=VIEW_LOGS)],
            [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        current_message = context.user_data.get("last_admin_panel_message", {})
        new_message = {"text": text, "reply_markup": reply_markup.to_dict()}
        try:
            if update.callback_query and current_message != new_message:
                await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
                context.user_data["last_admin_panel_message"] = new_message
            elif update.message:
                await update.message.reply_text(text, reply_markup=reply_markup)
                context.user_data["last_admin_panel_message"] = new_message
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                logger.error(f"Ошибка отправки сообщения: {e}")
                ERRORS.labels(type="telegram_api").inc()
        context.user_data["state"] = STATE_ADMIN_PANEL
        if update.callback_query:
            await update.callback_query.answer()
        await log_analytics(user_id, "admin_panel_access")
        return STATE_ADMIN_PANEL

async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показ статистики администратора."""
    REQUESTS.labels(endpoint="admin_stats").inc()
    with RESPONSE_TIME.labels(endpoint="admin_stats").time():
        user_id = update.effective_user.id
        async with (await ensure_db_pool()) as conn:
            total_users = await conn.fetchval("SELECT COUNT(*) FROM users")
            total_transactions = await conn.fetchval("SELECT COUNT(*) FROM transactions")
            total_profit = await conn.fetchval("SELECT SUM(amount_ton) FROM transactions WHERE status = 'completed'") or 0
            total_stars = await conn.fetchval("SELECT SUM(stars) FROM transactions WHERE status = 'completed'") or 0
            ton_price = context.bot_data.get("ton_price", 2.85)
            text = (
                f"📊 Статистика бота:\n"
                f"👥 Пользователей: {total_users}\n"
                f"💸 Транзакций: {total_transactions}\n"
                f"⭐ Проданных звезд: {total_stars}\n"
                f"💰 Прибыль: {total_profit:.2f} TON (${(total_profit * ton_price):.2f})\n"
            )
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            current_message = context.user_data.get("last_admin_stats_message", {})
            new_message = {"text": text, "reply_markup": reply_markup.to_dict()}
            try:
                if current_message != new_message:
                    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
                    context.user_data["last_admin_stats_message"] = new_message
            except BadRequest as e:
                if "Message is not modified" not in str(e):
                    logger.error(f"Ошибка редактирования сообщения: {e}")
                    ERRORS.labels(type="telegram_api").inc()
            await update.callback_query.answer()
            await log_analytics(user_id, "admin_stats")
            return STATE_ADMIN_PANEL

async def admin_edit_texts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик редактирования текстов."""
    REQUESTS.labels(endpoint="admin_edit_texts").inc()
    with RESPONSE_TIME.labels(endpoint="admin_edit_texts").time():
        user_id = update.effective_user.id
        text = "Выберите текст для редактирования:"
        keyboard = [
            [InlineKeyboardButton("Приветствие", callback_data=EDIT_TEXT_WELCOME)],
            [InlineKeyboardButton("Покупка", callback_data=EDIT_TEXT_BUY_PROMPT)],
            [InlineKeyboardButton("Профиль", callback_data=EDIT_TEXT_PROFILE)],
            [InlineKeyboardButton("Рефералы", callback_data=EDIT_TEXT_REFERRALS)],
            [InlineKeyboardButton("Поддержка", callback_data=EDIT_TEXT_TECH_SUPPORT)],
            [InlineKeyboardButton("Отзывы", callback_data=EDIT_TEXT_REVIEWS)],
            [InlineKeyboardButton("Успешная покупка", callback_data=EDIT_TEXT_BUY_SUCCESS)],
            [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        current_message = context.user_data.get("last_admin_edit_texts_message", {})
        new_message = {"text": text, "reply_markup": reply_markup.to_dict()}
        try:
            if update.callback_query and current_message != new_message:
                await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
                context.user_data["last_admin_edit_texts_message"] = new_message
            elif update.message:
                await update.message.reply_text(text, reply_markup=reply_markup)
                context.user_data["last_admin_edit_texts_message"] = new_message
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                logger.error(f"Ошибка отправки сообщения: {e}")
                ERRORS.labels(type="telegram_api").inc()
        context.user_data["state"] = STATE_ADMIN_EDIT_TEXTS
        if update.callback_query:
            await update.callback_query.answer()
        await log_analytics(user_id, "admin_edit_texts")
        return STATE_ADMIN_EDIT_TEXTS

async def edit_text_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ввода нового текста."""
    REQUESTS.labels(endpoint="edit_text_prompt").inc()
    with RESPONSE_TIME.labels(endpoint="edit_text_prompt").time():
        user_id = update.effective_user.id
        text_key = update.callback_query.data
        context.user_data["text_key"] = text_key
        text = f"Введите новый текст для '{text_key}':"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        try:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                logger.error(f"Ошибка редактирования сообщения: {e}")
                ERRORS.labels(type="telegram_api").inc()
        context.user_data["state"] = STATE_EDIT_TEXT
        await update.callback_query.answer()
        await log_analytics(user_id, "edit_text_prompt", {"text_key": text_key})
        return STATE_EDIT_TEXT

async def admin_user_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показ статистики пользователей."""
    REQUESTS.labels(endpoint="admin_user_stats").inc()
    with RESPONSE_TIME.labels(endpoint="admin_user_stats").time():
        user_id = update.effective_user.id
        async with (await ensure_db_pool()) as conn:
            users = await conn.fetch("SELECT user_id, username, stars_bought, ref_bonus_ton, referrals FROM users ORDER BY stars_bought DESC LIMIT 10")
            if not users:
                text = "Пользователи отсутствуют."
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
            else:
                text = "📊 Топ-10 пользователей по звездам:\n"
                for i, user in enumerate(users, 1):
                    ref_count = len(json.loads(user["referrals"])) if user["referrals"] != '[]' else 0
                    text += f"{i}. @{user['username'] or 'Unknown'} (ID: {user['user_id']}): {user['stars_bought']} ⭐, Бонус: {user['ref_bonus_ton']} TON, Рефералов: {ref_count}\n"
                keyboard = [
                    [InlineKeyboardButton("Список пользователей", callback_data=LIST_USERS)],
                    [InlineKeyboardButton("Поиск пользователя", callback_data="search_user")],
                    [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]
                ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            current_message = context.user_data.get("last_admin_user_stats_message", {})
            new_message = {"text": text, "reply_markup": reply_markup.to_dict()}
            try:
                if current_message != new_message:
                    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
                    context.user_data["last_admin_user_stats_message"] = new_message
            except BadRequest as e:
                if "Message is not modified" not in str(e):
                    logger.error(f"Ошибка редактирования сообщения: {e}")
                    ERRORS.labels(type="telegram_api").inc()
            await update.callback_query.answer()
            await log_analytics(user_id, "admin_user_stats")
            return STATE_ADMIN_USER_STATS

async def list_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик списка пользователей."""
    REQUESTS.labels(endpoint="list_users").inc()
    with RESPONSE_TIME.labels(endpoint="list_users").time():
        user_id = update.effective_user.id
        async with (await ensure_db_pool()) as conn:
            users = await conn.fetch("SELECT user_id, username, stars_bought, referrals FROM users ORDER BY user_id LIMIT 10")
            if not users:
                text = "Пользователи не найдены."
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=ADMIN_USER_STATS)]]
            else:
                text = "Список пользователей:\n"
                keyboard = []
                for user in users:
                    ref_count = len(json.loads(user["referrals"])) if user["referrals"] != '[]' else 0
                    text += f"ID: {user['user_id']}, @{user['username'] or 'Unknown'}, Звезд: {user['stars_bought']}, Рефералов: {ref_count}\n"
                    keyboard.append([InlineKeyboardButton(f"@{user['username'] or 'Unknown'}", callback_data=f"{SELECT_USER}{user['user_id']}")])
                keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=ADMIN_USER_STATS)])
            reply_markup = InlineKeyboardMarkup(keyboard)
            current_message = context.user_data.get("last_list_users_message", {})
            new_message = {"text": text, "reply_markup": reply_markup.to_dict()}
            try:
                if current_message != new_message:
                    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
                    context.user_data["last_list_users_message"] = new_message
            except BadRequest as e:
                if "Message is not modified" not in str(e):
                    logger.error(f"Ошибка редактирования сообщения: {e}")
                    ERRORS.labels(type="telegram_api").inc()
            context.user_data["state"] = STATE_LIST_USERS
            await update.callback_query.answer()
            await log_analytics(user_id, "list_users")
            return STATE_LIST_USERS

async def admin_edit_markup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик редактирования наценок."""
    REQUESTS.labels(endpoint="admin_edit_markup").inc()
    with RESPONSE_TIME.labels(endpoint="admin_edit_markup").time():
        user_id = update.effective_user.id
        text = "Выберите тип наценки:"
        keyboard = [
            [InlineKeyboardButton("TON Space", callback_data=MARKUP_TON_SPACE)],
            [InlineKeyboardButton("CryptoBot (крипта)", callback_data=MARKUP_CRYPTOBOT_CRYPTO)],
            [InlineKeyboardButton("CryptoBot (карта)", callback_data=MARKUP_CRYPTOBOT_CARD)],
            [InlineKeyboardButton("Реф. бонус", callback_data=MARKUP_REF_BONUS)],
            [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        current_message = context.user_data.get("last_admin_edit_markup_message", {})
        new_message = {"text": text, "reply_markup": reply_markup.to_dict()}
        try:
            if update.callback_query and current_message != new_message:
                await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
                context.user_data["last_admin_edit_markup_message"] = new_message
            elif update.message:
                await update.message.reply_text(text, reply_markup=reply_markup)
                context.user_data["last_admin_edit_markup_message"] = new_message
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                logger.error(f"Ошибка отправки сообщения: {e}")
                ERRORS.labels(type="telegram_api").inc()
        context.user_data["state"] = STATE_ADMIN_EDIT_MARKUP
        if update.callback_query:
            await update.callback_query.answer()
        await log_analytics(user_id, "admin_edit_markup")
        return STATE_ADMIN_EDIT_MARKUP

async def admin_manage_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик управления админами."""
    REQUESTS.labels(endpoint="admin_manage_admins").inc()
    with RESPONSE_TIME.labels(endpoint="admin_manage_admins").time():
        user_id = update.effective_user.id
        text = "Выберите действие:"
        keyboard = [
            [InlineKeyboardButton("Добавить админа", callback_data=ADD_ADMIN)],
            [InlineKeyboardButton("Удалить админа", callback_data=REMOVE_ADMIN)],
            [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        current_message = context.user_data.get("last_admin_manage_message", {})
        new_message = {"text": text, "reply_markup": reply_markup.to_dict()}
        try:
            if update.callback_query and current_message != new_message:
                await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
                context.user_data["last_admin_manage_message"] = new_message
            elif update.message:
                await update.message.reply_text(text, reply_markup=reply_markup)
                context.user_data["last_admin_manage_message"] = new_message
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                logger.error(f"Ошибка отправки сообщения: {e}")
                ERRORS.labels(type="telegram_api").inc()
        context.user_data["state"] = STATE_ADMIN_MANAGE_ADMINS
        if update.callback_query:
            await update.callback_query.answer()
        await log_analytics(user_id, "admin_manage_admins")
        return STATE_ADMIN_MANAGE_ADMINS

async def admin_edit_profit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик редактирования прибыли."""
    REQUESTS.labels(endpoint="admin_edit_profit").inc()
    with RESPONSE_TIME.labels(endpoint="admin_edit_profit").time():
        user_id = update.effective_user.id
        text = "Введите процент прибыли:"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        try:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                logger.error(f"Ошибка редактирования сообщения: {e}")
                ERRORS.labels(type="telegram_api").inc()
        context.user_data["input_state"] = "profit_percent"
        context.user_data["state"] = STATE_ADMIN_EDIT_PROFIT
        await update.callback_query.answer()
        await log_analytics(user_id, "admin_edit_profit")
        return STATE_ADMIN_EDIT_PROFIT

async def export_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Экспорт данных о транзакциях."""
    REQUESTS.labels(endpoint="export_data").inc()
    with RESPONSE_TIME.labels(endpoint="export_data").time():
        user_id = update.effective_user.id
        async with (await ensure_db_pool()) as conn:
            transactions = await conn.fetch("SELECT user_id, stars, amount_ton, amount_usd, payment_method, recipient, status, created_at FROM transactions")
            if not transactions:
                try:
                    await update.callback_query.edit_message_text(
                        "Транзакции отсутствуют.",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                    )
                except BadRequest as e:
                    if "Message is not modified" not in str(e):
                        logger.error(f"Ошибка редактирования сообщения: {e}")
                        ERRORS.labels(type="telegram_api").inc()
                await update.callback_query.answer()
                await log_analytics(user_id, "export_data_empty")
                return STATE_ADMIN_PANEL
            ton_price = context.bot_data.get("ton_price", 2.85)
            csv_content = "user_id,stars,amount_ton,amount_usd,payment_method,recipient,status,created_at\n"
            for t in transactions:
                amount_usd = t["amount_usd"] or (t["amount_ton"] * ton_price if t["amount_ton"] else 0)
                csv_content += f"{t['user_id']},{t['stars']},{t['amount_ton'] or 0},{amount_usd:.2f},{t['payment_method']},{t['recipient']},{t['status']},{t['created_at']}\n"
            file_name = f"transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            import io
            csv_file = io.BytesIO(csv_content.encode('utf-8'))
            csv_file.seek(0)
            try:
                await update.callback_query.message.reply_document(document=csv_file, filename=file_name)
            except Exception as e:
                logger.error(f"Ошибка отправки файла: {e}")
                ERRORS.labels(type="telegram_api").inc()
            await update.callback_query.answer()
            await log_analytics(user_id, "export_data")
            context.user_data["state"] = STATE_ADMIN_PANEL
            return await admin_panel(update, context)

async def view_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Просмотр логов администратора."""
    REQUESTS.labels(endpoint="view_logs").inc()
    with RESPONSE_TIME.labels(endpoint="view_logs").time():
        user_id = update.effective_user.id
        async with (await ensure_db_pool()) as conn:
            logs = await conn.fetch("SELECT admin_id, action, details, created_at FROM admin_logs ORDER BY created_at DESC LIMIT 10")
            if not logs:
                text = "Логи отсутствуют."
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
            else:
                text = "Последние действия администраторов:\n"
                for log in logs:
                    details = json.loads(log["details"]) if log["details"] else {}
                    text += f"[{log['created_at']}] Admin ID {log['admin_id']}: {log['action']} - {details}\n"
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            current_message = context.user_data.get("last_view_logs_message", {})
            new_message = {"text": text, "reply_markup": reply_markup.to_dict()}
            try:
                if current_message != new_message:
                    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
                    context.user_data["last_view_logs_message"] = new_message
            except BadRequest as e:
                if "Message is not modified" not in str(e):
                    logger.error(f"Ошибка редактирования сообщения: {e}")
                    ERRORS.labels(type="telegram_api").inc()
            await update.callback_query.answer()
            await log_analytics(user_id, "view_logs")
            return STATE_ADMIN_PANEL

async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстового ввода для различных состояний."""
    REQUESTS.labels(endpoint="handle_text_input").inc()
    with RESPONSE_TIME.labels(endpoint="handle_text_input").time():
        user_id = update.effective_user.id
        input_state = context.user_data.get("input_state")
        text = update.message.text.strip()
        if not input_state:
            await update.message.reply_text("Ошибка: неизвестное состояние ввода.")
            return context.user_data.get("state", STATE_MAIN_MENU)

        async with (await ensure_db_pool()) as conn:
            if input_state == "recipient":
                if not text.startswith("@"):
                    await update.message.reply_text("Введите корректный username, начиная с @.")
                    return STATE_BUY_STARS_RECIPIENT
                context.user_data["buy_data"]["recipient"] = text
                await log_analytics(user_id, "set_recipient", {"recipient": text})
                return await buy_stars(update, context)
            
            elif input_state == "amount":
                try:
                    stars = int(text)
                    if stars % 50 != 0 or stars < 10:
                        await update.message.reply_text("Количество звезд должно быть кратно 50 и не менее 10.")
                        return STATE_BUY_STARS_AMOUNT
                    context.user_data["buy_data"]["stars"] = stars
                    await log_analytics(user_id, "set_amount", {"stars": stars})
                    return await buy_stars(update, context)
                except ValueError:
                    await update.message.reply_text("Введите число звезд (например, 50, 100, ...).")
                    return STATE_BUY_STARS_AMOUNT
            
            elif input_state in ["welcome", "buy_prompt", "profile", "referrals", "tech_support", "reviews", "buy_success"]:
                text_key = context.user_data.get("text_key")
                if text_key:
                    await update_text(text_key, text)
                    await log_admin_action(user_id, "edit_text", {"text_key": text_key, "new_text": text})
                    await update.message.reply_text(f"Текст '{text_key}' обновлен.")
                    return await admin_edit_texts(update, context)
            
            elif input_state == "markup_ton_space":
                try:
                    markup = float(text)
                    await update_setting("markup_ton_space", markup)
                    await log_admin_action(user_id, "edit_markup", {"type": "ton_space", "value": markup})
                    await update.message.reply_text(f"Наценка TON Space обновлена: {markup}%")
                    return await admin_edit_markup(update, context)
                except ValueError:
                    await update.message.reply_text("Введите число (например, 20 для 20%).")
                    return STATE_ADMIN_EDIT_MARKUP
            
            elif input_state == "markup_cryptobot_crypto":
                try:
                    markup = float(text)
                    await update_setting("markup_cryptobot_crypto", markup)
                    await log_admin_action(user_id, "edit_markup", {"type": "cryptobot_crypto", "value": markup})
                    await update.message.reply_text(f"Наценка CryptoBot (крипта) обновлена: {markup}%")
                    return await admin_edit_markup(update, context)
                except ValueError:
                    await update.message.reply_text("Введите число (например, 25 для 25%).")
                    return STATE_ADMIN_EDIT_MARKUP
            
            elif input_state == "markup_cryptobot_card":
                try:
                    markup = float(text)
                    await update_setting("markup_cryptobot_card", markup)
                    await log_admin_action(user_id, "edit_markup", {"type": "cryptobot_card", "value": markup})
                    await update.message.reply_text(f"Наценка CryptoBot (карта) обновлена: {markup}%")
                    return await admin_edit_markup(update, context)
                except ValueError:
                    await update.message.reply_text("Введите число (например, 25 для 25%).")
                    return STATE_ADMIN_EDIT_MARKUP
            
            elif input_state == "markup_ref_bonus":
                try:
                    markup = float(text)
                    await update_setting("markup_ref_bonus", markup)
                    await log_admin_action(user_id, "edit_markup", {"type": "ref_bonus", "value": markup})
                    await update.message.reply_text(f"Реферальный бонус обновлен: {markup}%")
                    return await admin_edit_markup(update, context)
                except ValueError:
                    await update.message.reply_text("Введите число (например, 5 для 5%).")
                    return STATE_ADMIN_EDIT_MARKUP
            
            elif input_state == "add_admin":
                try:
                    new_admin_id = int(text)
                    admin_ids = await get_setting("admin_ids") or [TWIN_ACCOUNT_ID, YOUR_TEST_ACCOUNT_ID]
                    if new_admin_id not in admin_ids:
                        admin_ids.append(new_admin_id)
                        await update_setting("admin_ids", admin_ids)
                        await log_admin_action(user_id, "add_admin", {"new_admin_id": new_admin_id})
                        await update.message.reply_text(f"Админ {new_admin_id} добавлен.")
                    else:
                        await update.message.reply_text("Этот пользователь уже админ.")
                    return await admin_manage_admins(update, context)
                except ValueError:
                    await update.message.reply_text("Введите корректный ID пользователя.")
                    return STATE_ADMIN_MANAGE_ADMINS
            
            elif input_state == "remove_admin":
                try:
                    admin_id = int(text)
                    admin_ids = await get_setting("admin_ids") or [TWIN_ACCOUNT_ID, YOUR_TEST_ACCOUNT_ID]
                    if admin_id == user_id:
                        await update.message.reply_text("Нельзя удалить самого себя.")
                    elif admin_id in admin_ids:
                        admin_ids.remove(admin_id)
                        await update_setting("admin_ids", admin_ids)
                        await log_admin_action(user_id, "remove_admin", {"removed_admin_id": admin_id})
                        await update.message.reply_text(f"Админ {admin_id} удален.")
                    else:
                        await update.message.reply_text("Этот пользователь не админ.")
                    return await admin_manage_admins(update, context)
                except ValueError:
                    await update.message.reply_text("Введите корректный ID пользователя.")
                    return STATE_ADMIN_MANAGE_ADMINS
            
            elif input_state == "profit_percent":
                try:
                    profit = float(text)
                    await update_setting("profit_percent", profit)
                    await log_admin_action(user_id, "edit_profit", {"profit_percent": profit})
                    await update.message.reply_text(f"Процент прибыли обновлен: {profit}%")
                    return await admin_panel(update, context)
                except ValueError:
                    await update.message.reply_text("Введите число (например, 10 для 10%).")
                    return STATE_ADMIN_EDIT_PROFIT
            
            elif input_state == "edit_user_stars":
                try:
                    stars = int(text)
                    selected_user_id = context.user_data.get("selected_user_id")
                    if selected_user_id:
                        await conn.execute(
                            "UPDATE users SET stars_bought = $1 WHERE user_id = $2",
                            stars, selected_user_id
                        )
                        await log_admin_action(user_id, "edit_user_stars", {"user_id": selected_user_id, "stars": stars})
                        await update.message.reply_text(f"Звезды для пользователя {selected_user_id} обновлены: {stars}.")
                        return await list_users(update, context)
                    else:
                        await update.message.reply_text("Пользователь не выбран.")
                        return STATE_ADMIN_USER_STATS
                except ValueError:
                    await update.message.reply_text("Введите корректное число звезд.")
                    return STATE_EDIT_USER
            
            elif input_state == "edit_user_ref_bonus":
                try:
                    bonus = float(text)
                    selected_user_id = context.user_data.get("selected_user_id")
                    if selected_user_id:
                        await conn.execute(
                            "UPDATE users SET ref_bonus_ton = $1 WHERE user_id = $2",
                            bonus, selected_user_id
                        )
                        await log_admin_action(user_id, "edit_user_ref_bonus", {"user_id": selected_user_id, "bonus": bonus})
                        await update.message.reply_text(f"Реф. бонус для пользователя {selected_user_id} обновлен: {bonus} TON.")
                        return await list_users(update, context)
                    else:
                        await update.message.reply_text("Пользователь не выбран.")
                        return STATE_ADMIN_USER_STATS
                except ValueError:
                    await update.message.reply_text("Введите корректное число TON.")
                    return STATE_EDIT_USER

        return context.user_data.get("state", STATE_MAIN_MENU)

async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик callback-запросов."""
    query = update.callback_query
    data = query.data
    user_id = update.effective_user.id
    REQUESTS.labels(endpoint="callback_query").inc()
    with RESPONSE_TIME.labels(endpoint="callback_query").time():
        if data == BACK_TO_MENU:
            return await start(update, context)
        elif data == BACK_TO_ADMIN:
            return await admin_panel(update, context)
        elif data == PROFILE:
            return await profile(update, context)
        elif data == REFERRALS:
            return await referrals(update, context)
        elif data == SUPPORT:
            return await support(update, context)
        elif data == REVIEWS:
            return await reviews(update, context)
        elif data == BUY_STARS:
            return await buy_stars(update, context)
        elif data == ADMIN_PANEL:
            admin_ids = await get_setting("admin_ids") or [TWIN_ACCOUNT_ID, YOUR_TEST_ACCOUNT_ID]
            if user_id in admin_ids:
                return await admin_panel(update, context)
            await query.answer(text="Доступ запрещен.")
            return STATE_MAIN_MENU
        elif data == ADMIN_STATS:
            return await admin_stats(update, context)
        elif data == ADMIN_EDIT_TEXTS:
            return await admin_edit_texts(update, context)
        elif data == ADMIN_USER_STATS:
            return await admin_user_stats(update, context)
        elif data == ADMIN_EDIT_MARKUP:
            return await admin_edit_markup(update, context)
        elif data == ADMIN_MANAGE_ADMINS:
            return await admin_manage_admins(update, context)
        elif data == ADMIN_EDIT_PROFIT:
            return await admin_edit_profit(update, context)
        elif data == EXPORT_DATA:
            return await export_data(update, context)
        elif data == VIEW_LOGS:
            return await view_logs(update, context)
        elif data == SET_RECIPIENT:
            return await set_recipient(update, context)
        elif data == SET_AMOUNT:
            return await set_amount(update, context)
        elif data == SET_PAYMENT:
            return await set_payment_method(update, context)
        elif data == SELECT_CRYPTO_TYPE:
            return await select_crypto_type(update, context)
        elif data == CONFIRM_PAYMENT:
            return await confirm_payment(update, context)
        elif data == CHECK_PAYMENT:
            return await check_payment(update, context)
        elif data in [EDIT_TEXT_WELCOME, EDIT_TEXT_BUY_PROMPT, EDIT_TEXT_PROFILE, EDIT_TEXT_REFERRALS,
                      EDIT_TEXT_TECH_SUPPORT, EDIT_TEXT_REVIEWS, EDIT_TEXT_BUY_SUCCESS]:
            context.user_data["input_state"] = data
            return await edit_text_prompt(update, context)
        elif data in [MARKUP_TON_SPACE, MARKUP_CRYPTOBOT_CRYPTO, MARKUP_CRYPTOBOT_CARD, MARKUP_REF_BONUS]:
            context.user_data["input_state"] = data
            text = f"Введите процент наценки для '{data}':"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            try:
                await query.edit_message_text(text, reply_markup=reply_markup)
            except BadRequest as e:
                if "Message is not modified" not in str(e):
                    logger.error(f"Ошибка редактирования сообщения: {e}")
                    ERRORS.labels(type="telegram_api").inc()
            await query.answer()
            await log_analytics(user_id, "edit_markup_prompt", {"markup_type": data})
            return STATE_ADMIN_EDIT_MARKUP
        elif data == ADD_ADMIN:
            context.user_data["input_state"] = "add_admin"
            text = "Введите ID нового администратора:"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            try:
                await query.edit_message_text(text, reply_markup=reply_markup)
            except BadRequest as e:
                if "Message is not modified" not in str(e):
                    logger.error(f"Ошибка редактирования сообщения: {e}")
                    ERRORS.labels(type="telegram_api").inc()
            await query.answer()
            await log_analytics(user_id, "add_admin_prompt")
            return STATE_ADMIN_MANAGE_ADMINS
        elif data == REMOVE_ADMIN:
            context.user_data["input_state"] = "remove_admin"
            text = "Введите ID администратора для удаления:"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            try:
                await query.edit_message_text(text, reply_markup=reply_markup)
            except BadRequest as e:
                if "Message is not modified" not in str(e):
                    logger.error(f"Ошибка редактирования сообщения: {e}")
                    ERRORS.labels(type="telegram_api").inc()
            await query.answer()
            await log_analytics(user_id, "remove_admin_prompt")
            return STATE_ADMIN_MANAGE_ADMINS
        elif data.startswith(SELECT_USER):
            selected_user_id = int(data[len(SELECT_USER):])
            context.user_data["selected_user_id"] = selected_user_id
            async with (await ensure_db_pool()) as conn:
                user = await conn.fetchrow("SELECT username, stars_bought, ref_bonus_ton, referrals FROM users WHERE user_id = $1", selected_user_id)
                if user:
                    ref_count = len(json.loads(user["referrals"])) if user["referrals"] != '[]' else 0
                    text = await get_text(
                        "user_info",
                        username=user["username"] or "Unknown",
                        user_id=selected_user_id,
                        stars_bought=user["stars_bought"],
                        ref_bonus_ton=user["ref_bonus_ton"],
                        ref_count=ref_count
                    )
                    keyboard = [
                        [InlineKeyboardButton("Редактировать звезды", callback_data=EDIT_USER_STARS)],
                        [InlineKeyboardButton("Редактировать реф. бонус", callback_data=EDIT_USER_REF_BONUS)],
                        [InlineKeyboardButton("🔙 Назад", callback_data=LIST_USERS)]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    try:
                        await query.edit_message_text(text, reply_markup=reply_markup)
                    except BadRequest as e:
                        if "Message is not modified" not in str(e):
                            logger.error(f"Ошибка редактирования сообщения: {e}")
                            ERRORS.labels(type="telegram_api").inc()
                    context.user_data["state"] = STATE_EDIT_USER
                    await query.answer()
                    await log_analytics(user_id, "select_user", {"selected_user_id": selected_user_id})
                    return STATE_EDIT_USER
            await query.answer(text="Пользователь не найден.")
            return STATE_ADMIN_USER_STATS
        elif data == EDIT_USER_STARS:
            context.user_data["input_state"] = "edit_user_stars"
            text = "Введите новое количество звезд:"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=LIST_USERS)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            try:
                await query.edit_message_text(text, reply_markup=reply_markup)
            except BadRequest as e:
                if "Message is not modified" not in str(e):
                    logger.error(f"Ошибка редактирования сообщения: {e}")
                    ERRORS.labels(type="telegram_api").inc()
            await query.answer()
            await log_analytics(user_id, "edit_user_stars_prompt")
            return STATE_EDIT_USER
        elif data == EDIT_USER_REF_BONUS:
            context.user_data["input_state"] = "edit_user_ref_bonus"
            text = "Введите новый реферальный бонус (TON):"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=LIST_USERS)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            try:
                await query.edit_message_text(text, reply_markup=reply_markup)
            except BadRequest as e:
                if "Message is not modified" not in str(e):
                    logger.error(f"Ошибка редактирования сообщения: {e}")
                    ERRORS.labels(type="telegram_api").inc()
            await query.answer()
            await log_analytics(user_id, "edit_user_ref_bonus_prompt")
            return STATE_EDIT_USER
        elif data == PAY_TON_SPACE:
            buy_data = context.user_data.get("buy_data", {})
            stars = buy_data.get("stars")
            recipient = buy_data.get("recipient")
            if not stars or not recipient:
                await query.message.reply_text("Сначала выберите получателя и количество звезд!")
                return STATE_BUY_STARS_RECIPIENT
            ton_price = context.bot_data.get("ton_price", 2.85)
            stars_price_usd = await get_setting("stars_price_usd") or (PRICE_USD_PER_50 / 50)
            markup = await get_setting("markup_ton_space") or 20
            amount_usd = stars * stars_price_usd
            amount_ton = (amount_usd / ton_price) * (1 + markup / 100)
            payload = await generate_payload(user_id)
            invoice_id, pay_url = await create_ton_space_invoice(amount_ton, user_id, stars, recipient, payload)
            if not pay_url:
                await query.message.reply_text("Ошибка создания инвойса. Попробуйте другой метод.")
                return STATE_BUY_STARS_PAYMENT_METHOD
            buy_data.update({
                "payment_method": "ton_space_api" if TON_SPACE_API_TOKEN else "ton_space_direct",
                "amount_ton": amount_ton,
                "pay_url": pay_url,
                "invoice_id": invoice_id,
                "payload": payload
            })
            context.user_data["buy_data"] = buy_data
            await log_analytics(user_id, "select_ton_space", {"stars": stars, "amount_ton": amount_ton})
            return await confirm_payment(update, context)
        elif data == PAY_CRYPTOBOT:
            buy_data = context.user_data.get("buy_data", {})
            stars = buy_data.get("stars")
            recipient = buy_data.get("recipient")
            if not stars or not recipient:
                await query.message.reply_text("Сначала выберите получателя и количество звезд!")
                return STATE_BUY_STARS_RECIPIENT
            ton_price = context.bot_data.get("ton_price", 2.85)
            stars_price_usd = await get_setting("stars_price_usd") or (PRICE_USD_PER_50 / 50)
            markup = await get_setting("markup_cryptobot_crypto") or 25
            amount_usd = stars * stars_price_usd
            amount_ton = (amount_usd / ton_price) * (1 + markup / 100)
            payload = await generate_payload(user_id)
            invoice_id, pay_url = await create_cryptobot_invoice(amount_ton, "TON", user_id, stars, recipient, payload)
            if not pay_url:
                await query.message.reply_text("Ошибка создания инвойса. Попробуйте другой метод.")
                return STATE_BUY_STARS_PAYMENT_METHOD
            buy_data.update({
                "payment_method": "cryptobot_ton",
                "amount_ton": amount_ton,
                "pay_url": pay_url,
                "invoice_id": invoice_id,
                "payload": payload
            })
            context.user_data["buy_data"] = buy_data
            await log_analytics(user_id, "select_cryptobot_ton", {"stars": stars, "amount_ton": amount_ton})
            return await confirm_payment(update, context)
        elif data == PAY_CARD:
            buy_data = context.user_data.get("buy_data", {})
            stars = buy_data.get("stars")
            recipient = buy_data.get("recipient")
            if not stars or not recipient:
                await query.message.reply_text("Сначала выберите получателя и количество звезд!")
                return STATE_BUY_STARS_RECIPIENT
            stars_price_usd = await get_setting("stars_price_usd") or (PRICE_USD_PER_50 / 50)
            markup = await get_setting("markup_cryptobot_card") or 25
            amount_usd = stars * stars_price_usd * (1 + markup / 100)
            payload = await generate_payload(user_id)
            invoice_id, pay_url = await create_cryptobot_invoice(amount_usd, "USD", user_id, stars, recipient, payload)
            if not pay_url:
                await query.message.reply_text("Ошибка создания инвойса. Попробуйте другой метод.")
                return STATE_BUY_STARS_PAYMENT_METHOD
            buy_data.update({
                "payment_method": "cryptobot_usd",
                "amount_ton": 0.0,
                "amount_usd": amount_usd,
                "pay_url": pay_url,
                "invoice_id": invoice_id,
                "payload": payload
            })
            context.user_data["buy_data"] = buy_data
            await log_analytics(user_id, "select_cryptobot_usd", {"stars": stars, "amount_usd": amount_usd})
            return await confirm_payment(update, context)
        elif data == TOP_REFERRALS:
            async with (await ensure_db_pool()) as conn:
                users = await conn.fetch("SELECT user_id, username, referrals FROM users ORDER BY jsonb_array_length(referrals) DESC LIMIT 10")
                text = "🏆 Топ-10 рефералов:\n"
                for i, user in enumerate(users, 1):
                    ref_count = len(json.loads(user["referrals"])) if user["referrals"] != '[]' else 0
                    text += f"{i}. @{user['username'] or 'Unknown'} (ID: {user['user_id']}): {ref_count} рефералов\n"
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=PROFILE)]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                try:
                    await query.edit_message_text(text, reply_markup=reply_markup)
                except BadRequest as e:
                    if "Message is not modified" not in str(e):
                        logger.error(f"Ошибка редактирования сообщения: {e}")
                        ERRORS.labels(type="telegram_api").inc()
                await query.answer()
                await log_analytics(user_id, "view_top_referrals")
                return STATE_PROFILE
        elif data == TOP_PURCHASES:
            async with (await ensure_db_pool()) as conn:
                users = await conn.fetch("SELECT user_id, username, stars_bought FROM users ORDER BY stars_bought DESC LIMIT 10")
                text = "🏆 Топ-10 покупок:\n"
                for i, user in enumerate(users, 1):
                    text += f"{i}. @{user['username'] or 'Unknown'} (ID: {user['user_id']}): {user['stars_bought']} звезд\n"
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=PROFILE)]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                try:
                    await query.edit_message_text(text, reply_markup=reply_markup)
                except BadRequest as e:
                    if "Message is not modified" not in str(e):
                        logger.error(f"Ошибка редактирования сообщения: {e}")
                        ERRORS.labels(type="telegram_api").inc()
                await query.answer()
                await log_analytics(user_id, "view_top_purchases")
                return STATE_PROFILE
        else:
            await query.answer(text="Неизвестная команда.")
            return context.user_data.get("state", STATE_MAIN_MENU)

async def handle_webhook(request):
    """Обработчик входящих вебхуков от Telegram."""
    try:
        update = Update.de_json(await request.json(), app.bot)
        await app.process_update(update)
        return web.Response(status=200)
    except Exception as e:
        logger.error(f"Ошибка обработки вебхука: {e}", exc_info=True)
        ERRORS.labels(type="webhook").inc()
        return web.Response(status=500)

async def start_bot():
    """Запуск бота с вебхуком и планировщиком."""
    global app
    try:
        await check_environment()
        await init_db()
        app = ApplicationBuilder().token(BOT_TOKEN).build()

        scheduler = AsyncIOScheduler()
        scheduler.add_job(
            update_ton_price,
            trigger="interval",
            minutes=5,
            args=[app.context_types.context],
            timezone=pytz.UTC
        )
        scheduler.add_job(
            keep_alive,
            trigger="interval",
            minutes=1,
            args=[app.context_types.context],
            timezone=pytz.UTC
        )
        scheduler.start()
        logger.info("Планировщик запущен")

        conv_handler = ConversationHandler(
            entry_points=[
                CommandHandler("start", start),
                CallbackQueryHandler(callback_query_handler)
            ],
            states={
                STATE_MAIN_MENU: [
                    CallbackQueryHandler(callback_query_handler),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, start)
                ],
                STATE_PROFILE: [CallbackQueryHandler(callback_query_handler)],
                STATE_REFERRALS: [CallbackQueryHandler(callback_query_handler)],
                STATE_BUY_STARS_RECIPIENT: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATE_BUY_STARS_AMOUNT: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATE_BUY_STARS_PAYMENT_METHOD: [CallbackQueryHandler(callback_query_handler)],
                STATE_BUY_STARS_CRYPTO_TYPE: [CallbackQueryHandler(callback_query_handler)],
                STATE_BUY_STARS_CONFIRM: [CallbackQueryHandler(callback_query_handler)],
                STATE_ADMIN_PANEL: [CallbackQueryHandler(callback_query_handler)],
                STATE_ADMIN_EDIT_TEXTS: [CallbackQueryHandler(callback_query_handler)],
                STATE_EDIT_TEXT: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATE_ADMIN_USER_STATS: [CallbackQueryHandler(callback_query_handler)],
                STATE_LIST_USERS: [CallbackQueryHandler(callback_query_handler)],
                STATE_EDIT_USER: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATE_ADMIN_EDIT_MARKUP: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATE_ADMIN_MANAGE_ADMINS: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATE_ADMIN_EDIT_PROFIT: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATE_EXPORT_DATA: [CallbackQueryHandler(callback_query_handler)],
                STATE_VIEW_LOGS: [CallbackQueryHandler(callback_query_handler)],
                STATE_TOP_REFERRALS: [CallbackQueryHandler(callback_query_handler)],
                STATE_TOP_PURCHASES: [CallbackQueryHandler(callback_query_handler)]
            },
            fallbacks=[CommandHandler("start", start)]
        )

        app.add_handler(conv_handler)
        app.add_error_handler(error_handler)

        start_http_server(9090)
        logger.info("Сервер Prometheus запущен на порту 9090")

        webhook_url = f"{WEBHOOK_URL}/webhook"
        await app.bot.set_webhook(webhook_url)
        logger.info(f"Вебхук установлен: {webhook_url}")

        web_app = web.Application()
        web_app.router.add_post("/webhook", handle_webhook)
        runner = web.AppRunner(web_app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", PORT)
        await site.start()
        logger.info(f"Веб-сервер запущен на порту {PORT}")

        await app.initialize()
        await app.start()
        logger.info("Бот успешно запущен")

        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            logger.info("Получен сигнал остановки")
            await app.stop()
            await app.shutdown()
            await close_db_pool()
            scheduler.shutdown()
            await runner.cleanup()
            logger.info("Бот остановлен")
    except Exception as e:
        logger.error(f"Ошибка запуска бота: {e}", exc_info=True)
        ERRORS.labels(type="startup").inc()
        raise

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ошибок."""
    logger.error(f"Ошибка: {context.error}", exc_info=True)
    ERRORS.labels(type="bot").inc()
    try:
        if update and update.effective_chat:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="Произошла ошибка. Пожалуйста, попробуйте снова или свяжитесь с поддержкой."
            )
    except Exception as e:
        logger.error(f"Ошибка отправки сообщения об ошибке: {e}")

if __name__ == "__main__":
    asyncio.run(start_bot())
