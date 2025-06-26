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
    """Получение пула базы данных."""
    global _db_pool
    async with _db_pool_lock:
        if _db_pool is None or _db_pool._closed:
            logger.info("Создание нового пула базы данных")
            parsed_url = urlparse(POSTGRES_URL)
            dbname = parsed_url.path.lstrip('/')
            user = parsed_url.username
            password = parsed_url.password
            host = parsed_url.hostname
            port = parsed_url.port or 5432
            _db_pool = await asyncpg.create_pool(
                min_size=1,
                max_size=10,
                host=host,
                port=port,
                database=dbname,
                user=user,
                password=password,
                command_timeout=60
            )
            logger.info("Пул базы данных успешно инициализирован")
        return _db_pool

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

async def close_db_pool():
    """Закрытие пула базы данных."""
    global _db_pool
    async with _db_pool_lock:
        if _db_pool and not _db_pool._closed:
            logger.info("Закрытие пула базы данных")
            await _db_pool.close()
            logger.info("Пул базы данных успешно закрыт")
            _db_pool = None

async def init_db():
    """Инициализация структуры базы данных."""
    logger.info("Инициализация базы данных")
    async with (await ensure_db_pool()) as conn:
        # Таблица пользователей
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

        # Таблица настроек
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value JSONB
            )
        ''')

        # Таблица текстов
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS texts (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')

        # Таблица транзакций
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

        # Таблица логов администратора
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS admin_logs (
                id SERIAL PRIMARY KEY,
                admin_id BIGINT,
                action TEXT,
                details JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Таблица аналитики
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS analytics (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                action TEXT,
                details JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Начальные настройки
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
        ''', json.dumps([TWIN_ACCOUNT_ID]), 
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

        # Начальные тексты
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
    REQUESTS.labels(endpoint="update_ton_price").inc()
    with RESPONSE_TIME.labels(endpoint="update_ton_price").time():
        url = "https://toncenter.com/api/v3/exchange/rate?currency=usd"
        headers = {"Authorization": f"Bearer {TON_API_KEY}"}
        async with aiohttp.ClientSession(timeout=ClientTimeout(total=10)) as session:
            try:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        ton_price = float(data.get("rate", 2.93))
                        await update_setting("ton_exchange_rate", ton_price)
                        context.bot_data["ton_price"] = ton_price
                        logger.info(f"Курс TON обновлен: {ton_price} USD")
                    else:
                        logger.error(f"Ошибка TON API: {response.status}")
                        context.bot_data["ton_price"] = await get_setting("ton_exchange_rate") or 2.93
                        ERRORS.labels(type="ton_api").inc()
            except Exception as e:
                logger.error(f"Ошибка обновления курса TON: {e}")
                context.bot_data["ton_price"] = await get_setting("ton_exchange_rate") or 2.93
                ERRORS.labels(type="ton_api").inc()

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

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start."""
    REQUESTS.labels(endpoint="start").inc()
    with RESPONSE_TIME.labels(endpoint="start").time():
        user_id = update.effective_user.id
        username = update.effective_user.username or f"user_{user_id}"
        ref_id = context.args[0].split("_")[1] if context.args and context.args[0].startswith("ref_") else None
        logger.info(f"Команда /start: user_id={user_id}, username={username}, ref_id={ref_id}")

        async with (await ensure_db_pool()) as conn:
            user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
            stars_sold = await conn.fetchval("SELECT SUM(stars) FROM transactions WHERE status = 'completed'") or 0
            stars_bought = user["stars_bought"] if user else 0
            if not user:
                await conn.execute(
                    "INSERT INTO users (user_id, username, created_at, is_new) VALUES ($1, $2, $3, $4)",
                    user_id, username, datetime.now(pytz.UTC), True
                )
                if ref_id and int(ref_id) != user_id:
                    referrer = await conn.fetchrow("SELECT referrals, is_new FROM users WHERE user_id = $1", int(ref_id))
                    if referrer and referrer["is_new"]:
                        referrals = json.loads(referrer["referrals"])
                        if user_id not in referrals:
                            referrals.append(user_id)
                            await conn.execute(
                                "UPDATE users SET referrals = $1 WHERE user_id = $2",
                                json.dumps(referrals), int(ref_id)
                            )
                            ref_bonus = float(await get_setting("markup_ref_bonus") or 5) / 100
                            await conn.execute(
                                "UPDATE users SET ref_bonus_ton = ref_bonus_ton + $1 WHERE user_id = $2",
                                ref_bonus, int(ref_id)
                            )
                            try:
                                await context.bot.send_message(
                                    chat_id=NEWS_CHANNEL,
                                    text=f"Новый пользователь @{username} через реферала ID {ref_id}"
                                )
                            except Exception as e:
                                logger.error(f"Ошибка отправки сообщения в канал: {e}")
                                ERRORS.labels(type="telegram_api").inc()
            else:
                await conn.execute(
                    "UPDATE users SET username = $1 WHERE user_id = $2",
                    username, user_id
                )

        text = await get_text("welcome", stars_sold=stars_sold, stars_bought=stars_bought)
        keyboard = [
            [InlineKeyboardButton("👤 Профиль", callback_data=PROFILE),
             InlineKeyboardButton("🤝 Рефералы", callback_data=REFERRALS)],
            [InlineKeyboardButton("🛠 Поддержка", callback_data=SUPPORT),
             InlineKeyboardButton("📝 Отзывы", callback_data=REVIEWS)],
            [InlineKeyboardButton("⭐ Купить звёзды", callback_data=BUY_STARS)]
        ]
        admin_ids = await get_setting("admin_ids") or [TWIN_ACCOUNT_ID]
        if user_id in admin_ids:
            keyboard.append([InlineKeyboardButton("🔧 Админ-панель", callback_data=ADMIN_PANEL)])
        reply_markup = InlineKeyboardMarkup(keyboard)
        if update.callback_query:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        else:
            await update.message.reply_text(text, reply_markup=reply_markup)
        context.user_data.clear()
        context.user_data["state"] = STATE_MAIN_MENU
        await log_analytics(user_id, "start_command", {"ref_id": ref_id})
        return STATE_MAIN_MENU

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
                await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
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
                await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
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
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
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
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
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
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
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
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
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
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
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
            await update.callback_query.message.reply_text("Сначала выберите получателя и количество звезд!")
            await log_analytics(update.effective_user.id, "set_payment_method_error", {"error": "missing_data"})
            return STATE_BUY_STARS_RECIPIENT
        text = "Выберите метод оплаты:"
        keyboard = [
            [InlineKeyboardButton("Криптовалюта (+25%)", callback_data=SELECT_CRYPTO_TYPE)],
            [InlineKeyboardButton("Карта (+25%)", callback_data=PAY_CARD)],
            [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["state"] = STATE_BUY_STARS_PAYMENT_METHOD
        await update.callback_query.answer()
        await log_analytics(update.effective_user.id, "select_payment_method")
        return STATE_BUY_STARS_PAYMENT_METHOD

async def select_crypto_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик выбора типа криптовалюты."""
    REQUESTS.labels(endpoint="select_crypto_type").inc()
    with RESPONSE_TIME.labels(endpoint="select_crypto_type").time():
        text = "Выберите метод оплаты криптовалютой:"
        keyboard = [
            [InlineKeyboardButton("TON Space (+20%)", callback_data=PAY_TON_SPACE)],
            [InlineKeyboardButton("CryptoBot (+25%)", callback_data=PAY_CRYPTOBOT)],
            [InlineKeyboardButton("🔙 Назад", callback_data=SET_PAYMENT)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["state"] = STATE_BUY_STARS_CRYPTO_TYPE
        await update.callback_query.answer()
        await log_analytics(update.effective_user.id, "select_crypto_type")
        return STATE_BUY_STARS_CRYPTO_TYPE

async def confirm_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик подтверждения оплаты."""
    REQUESTS.labels(endpoint="confirm_payment").inc()
    with RESPONSE_TIME.labels(endpoint="confirm_payment").time():
        user_id = update.effective_user.id
        buy_data = context.user_data.get("buy_data", {})
        recipient = buy_data.get("recipient")
        stars = buy_data.get("stars")
        amount_ton = buy_data.get("amount_ton")
        pay_url = buy_data.get("pay_url")
        if not all([recipient, stars, amount_ton, pay_url]):
            logger.error(f"Неполные данные для оплаты: {buy_data}")
            await update.callback_query.message.reply_text("Неполные данные для оплаты. Начните заново.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BUY_STARS)]]))
            await log_analytics(user_id, "confirm_payment_error", {"error": "missing_data"})
            return STATE_BUY_STARS_RECIPIENT
        text = await get_text(
            "buy_prompt",
            recipient=recipient.lstrip("@"),
            stars=stars,
            amount_ton=f"{amount_ton:.6f}",
            address=pay_url,
            method=buy_data.get("payment_method", "unknown")
        )
        keyboard = [
            [InlineKeyboardButton("Оплатить", url=pay_url)],
            [InlineKeyboardButton("Проверить оплату", callback_data=CHECK_PAYMENT)],
            [InlineKeyboardButton("🔙 Назад", callback_data=BUY_STARS)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        current_message = context.user_data.get("last_confirm_message", {})
        new_message = {"text": text, "reply_markup": reply_markup.to_dict()}
        if update.callback_query and current_message != new_message:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
            context.user_data["last_confirm_message"] = new_message
        elif not update.callback_query:
            await update.message.reply_text(text, reply_markup=reply_markup)
            context.user_data["last_confirm_message"] = new_message
        await log_analytics(user_id, "confirm_payment", {"stars": stars, "recipient": recipient})
        return STATE_BUY_STARS_CONFIRM

async def check_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик проверки оплаты."""
    REQUESTS.labels(endpoint="check_payment").inc()
    with RESPONSE_TIME.labels(endpoint="check_payment").time():
        user_id = update.effective_user.id
        buy_data = context.user_data.get("buy_data", {})
        recipient = buy_data.get("recipient")
        stars = buy_data.get("stars")
        payment_method = buy_data.get("payment_method")
        invoice_id = buy_data.get("invoice_id")
        payload = buy_data.get("payload")
        amount_ton = buy_data.get("amount_ton")
        if not all([recipient, stars, payment_method, invoice_id]):
            logger.error(f"Неполные данные для проверки оплаты: {buy_data}")
            await update.callback_query.message.reply_text("Неполные данные для проверки оплаты. Начните заново.")
            await log_analytics(user_id, "check_payment_error", {"error": "missing_data"})
            return STATE_BUY_STARS_RECIPIENT
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
                    transaction_cache[payload] = tx  # Сохранение в кэш
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
                    await update.callback_query.message.reply_text(text)
                    try:
                        await context.bot.send_message(
                            chat_id=NEWS_CHANNEL,
                            text=f"Успешная покупка: @{recipient.lstrip('@')} получил {stars} звезд от user_id={user_id}"
                        )
                    except Exception as e:
                        logger.error(f"Ошибка отправки сообщения в канал: {e}")
                        ERRORS.labels(type="telegram_api").inc()
                    context.user_data.clear()
                    context.user_data["state"] = STATE_MAIN_MENU
                    await start(update, context)
                    await update.callback_query.answer()
                    await log_analytics(user_id, "payment_success", {"stars": stars, "recipient": recipient})
                    return STATE_MAIN_MENU
                else:
                    await update.callback_query.message.reply_text("Ошибка выдачи звезд. Обратитесь в поддержку.")
                    await log_analytics(user_id, "issue_stars_error", {"recipient": recipient, "stars": stars})
            else:
                await update.callback_query.message.reply_text("Оплата не подтверждена. Попробуйте снова.")
                await log_analytics(user_id, "payment_not_confirmed", {"invoice_id": invoice_id})
            await update.callback_query.answer()
            return STATE_BUY_STARS_CONFIRM

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик админ-панели."""
    REQUESTS.labels(endpoint="admin_panel").inc()
    with RESPONSE_TIME.labels(endpoint="admin_panel").time():
        user_id = update.effective_user.id
        admin_ids = await get_setting("admin_ids") or [TWIN_ACCOUNT_ID]
        if user_id not in admin_ids:
            await update.callback_query.edit_message_text("Доступ запрещен!")
            context.user_data["state"] = STATE_MAIN_MENU
            await update.callback_query.answer()
            await log_analytics(user_id, "admin_access_denied")
            return STATE_MAIN_MENU
        text = "🛠️ Админ-панель"
        keyboard = [
            [InlineKeyboardButton("📊 Статистика", callback_data=ADMIN_STATS)],
            [InlineKeyboardButton("📝 Изменить тексты", callback_data=ADMIN_EDIT_TEXTS)],
            [InlineKeyboardButton("👥 Статистика пользователей", callback_data=ADMIN_USER_STATS)],
            [InlineKeyboardButton("💸 Изменить наценку", callback_data=ADMIN_EDIT_MARKUP)],
            [InlineKeyboardButton("👤 Добавить/удалить админа", callback_data=ADMIN_MANAGE_ADMINS)],
            [InlineKeyboardButton("💰 Изменить параметры прибыли", callback_data=ADMIN_EDIT_PROFIT)],
            [InlineKeyboardButton("📤 Экспорт данных", callback_data=EXPORT_DATA)],
            [InlineKeyboardButton("📜 Просмотр логов", callback_data=VIEW_LOGS)],
            [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["state"] = STATE_ADMIN_PANEL
        await update.callback_query.answer()
        await log_analytics(user_id, "admin_panel_access")
        return STATE_ADMIN_PANEL

async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик статистики админа."""
    REQUESTS.labels(endpoint="admin_stats").inc()
    with RESPONSE_TIME.labels(endpoint="admin_stats").time():
        async with (await ensure_db_pool()) as conn:
            profit_percent = float(await get_setting("profit_percent") or 10)
            total_profit = await conn.fetchval(
                "SELECT COALESCE(SUM(amount_ton * $1 / 100), 0) FROM transactions WHERE status = 'completed'",
                profit_percent
            )
            total_stars = await conn.fetchval("SELECT COALESCE(SUM(stars), 0) FROM transactions WHERE status = 'completed'")
            total_users = await conn.fetchval("SELECT COALESCE(COUNT(*), 0) FROM users")
            text = f"📊 Статистика\nПрибыль: {total_profit:.6f} TON\nЗвезд продано: {total_stars}\nПользователей: {total_users}"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
            context.user_data["state"] = STATE_ADMIN_STATS
            await update.callback_query.answer()
            await log_analytics(update.effective_user.id, "view_admin_stats")
            return STATE_ADMIN_STATS

async def admin_edit_texts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик редактирования текстов."""
    REQUESTS.labels(endpoint="admin_edit_texts").inc()
    with RESPONSE_TIME.labels(endpoint="admin_edit_texts").time():
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
        if update.callback_query and current_message != new_message:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
            context.user_data["last_admin_edit_texts_message"] = new_message
        elif update.message:
            await update.message.reply_text(text, reply_markup=reply_markup)
            context.user_data["last_admin_edit_texts_message"] = new_message
        context.user_data["state"] = STATE_ADMIN_EDIT_TEXTS
        if update.callback_query:
            await update.callback_query.answer()
        await log_analytics(update.effective_user.id, "admin_edit_texts")
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
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["state"] = STATE_EDIT_TEXT
        await update.callback_query.answer()
        await log_analytics(user_id, "edit_text_prompt", {"text_key": text_key})
        return STATE_EDIT_TEXT

async def admin_user_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик статистики пользователей."""
    REQUESTS.labels(endpoint="admin_user_stats").inc()
    with RESPONSE_TIME.labels(endpoint="admin_user_stats").time():
        text = "Статистика пользователей\n\nВыберите действие:"
        keyboard = [
            [InlineKeyboardButton("Поиск пользователя", callback_data="search_user")],
            [InlineKeyboardButton("Список пользователей", callback_data=LIST_USERS)],
            [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        if update.callback_query:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        else:
            await update.message.reply_text(text, reply_markup=reply_markup)
        context.user_data["state"] = STATE_ADMIN_USER_STATS
        if update.callback_query:
            await update.callback_query.answer()
        await log_analytics(update.effective_user.id, "admin_user_stats")
        return STATE_ADMIN_USER_STATS

async def list_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик списка пользователей."""
    REQUESTS.labels(endpoint="list_users").inc()
    with RESPONSE_TIME.labels(endpoint="list_users").time():
        async with (await ensure_db_pool()) as conn:
            users = await conn.fetch("SELECT user_id, username, stars_bought, referrals FROM users LIMIT 10")
            if not users:
                text = "Пользователи не найдены."
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=ADMIN_USER_STATS)]]
            else:
                text = "Список пользователей:\n"
                keyboard = []
                for user in users:
                    ref_count = len(json.loads(user["referrals"])) if user["referrals"] != '[]' else 0
                    text += f"ID: {user['user_id']}, @{user['username']}, Звезд: {user['stars_bought']}, Рефералов: {ref_count}\n"
                    keyboard.append([InlineKeyboardButton(f"@{user['username']}", callback_data=f"{SELECT_USER}{user['user_id']}")])
                keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=ADMIN_USER_STATS)])
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
            context.user_data["state"] = STATE_LIST_USERS
            await update.callback_query.answer()
            await log_analytics(update.effective_user.id, "list_users")
            return STATE_LIST_USERS

async def admin_edit_markup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик редактирования наценок."""
    REQUESTS.labels(endpoint="admin_edit_markup").inc()
    with RESPONSE_TIME.labels(endpoint="admin_edit_markup").time():
        text = "Выберите тип наценки:"
        keyboard = [
            [InlineKeyboardButton("TON Space", callback_data=MARKUP_TON_SPACE)],
            [InlineKeyboardButton("CryptoBot (крипта)", callback_data=MARKUP_CRYPTOBOT_CRYPTO)],
            [InlineKeyboardButton("CryptoBot (карта)", callback_data=MARKUP_CRYPTOBOT_CARD)],
            [InlineKeyboardButton("Реф. бонус", callback_data=MARKUP_REF_BONUS)],
            [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["state"] = STATE_ADMIN_EDIT_MARKUP
        await update.callback_query.answer()
        await log_analytics(update.effective_user.id, "admin_edit_markup")
        return STATE_ADMIN_EDIT_MARKUP

async def admin_manage_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик управления админами."""
    REQUESTS.labels(endpoint="admin_manage_admins").inc()
    with RESPONSE_TIME.labels(endpoint="admin_manage_admins").time():
        text = "Выберите действие:"
        keyboard = [
            [InlineKeyboardButton("Добавить админа", callback_data=ADD_ADMIN)],
            [InlineKeyboardButton("Удалить админа", callback_data=REMOVE_ADMIN)],
            [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        current_message = context.user_data.get("last_admin_manage_message", {})
        new_message = {"text": text, "reply_markup": reply_markup.to_dict()}
        if update.callback_query and current_message != new_message:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
            context.user_data["last_admin_manage_message"] = new_message
        elif not update.callback_query:
            await update.message.reply_text(text, reply_markup=reply_markup)
            context.user_data["last_admin_manage_message"] = new_message
        context.user_data["state"] = STATE_ADMIN_MANAGE_ADMINS
        if update.callback_query:
            await update.callback_query.answer()
        await log_analytics(update.effective_user.id, "admin_manage_admins")
        return STATE_ADMIN_MANAGE_ADMINS

async def admin_edit_profit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик редактирования прибыли."""
    REQUESTS.labels(endpoint="admin_edit_profit").inc()
    with RESPONSE_TIME.labels(endpoint="admin_edit_profit").time():
        text = "Введите процент прибыли:"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["input_state"] = "profit_percent"
        context.user_data["state"] = STATE_ADMIN_EDIT_PROFIT
        await update.callback_query.answer()
        await log_analytics(update.effective_user.id, "admin_edit_profit")
        return STATE_ADMIN_EDIT_PROFIT

async def export_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик экспорта данных."""
    REQUESTS.labels(endpoint="export_data").inc()
    with RESPONSE_TIME.labels(endpoint="export_data").time():
        user_id = update.effective_user.id
        async with (await ensure_db_pool()) as conn:
            users = await conn.fetch("SELECT user_id, username, stars_bought, ref_bonus_ton, referrals FROM users")
            transactions = await conn.fetch("SELECT user_id, stars, amount_ton, amount_usd, payment_method, recipient, status, created_at FROM transactions")
            data = {
                "users": [
                    {
                        "user_id": user["user_id"],
                        "username": user["username"],
                        "stars_bought": user["stars_bought"],
                        "ref_bonus_ton": user["ref_bonus_ton"],
                        "referrals_count": len(json.loads(user["referrals"])) if user["referrals"] != '[]' else 0
                    } for user in users
                ],
                "transactions": [
                    {
                        "user_id": tx["user_id"],
                        "stars": tx["stars"],
                        "amount_ton": tx["amount_ton"],
                        "amount_usd": tx["amount_usd"],
                        "payment_method": tx["payment_method"],
                        "recipient": tx["recipient"],
                        "status": tx["status"],
                        "created_at": tx["created_at"].isoformat()
                    } for tx in transactions
                ]
            }
            export_filename = f"export_{datetime.now(pytz.UTC).strftime('%Y%m%d_%H%M%S')}.json"
            with open(export_filename, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            await update.callback_query.message.reply_document(document=open(export_filename, "rb"), caption="Экспорт данных")
            os.remove(export_filename)
            context.user_data["state"] = STATE_ADMIN_PANEL
            await update.callback_query.answer()
            await log_analytics(user_id, "export_data")
            return STATE_ADMIN_PANEL

async def view_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик просмотра логов."""
    REQUESTS.labels(endpoint="view_logs").inc()
    with RESPONSE_TIME.labels(endpoint="view_logs").time():
        user_id = update.effective_user.id
        async with (await ensure_db_pool()) as conn:
            logs = await conn.fetch("SELECT action, details, created_at FROM admin_logs WHERE admin_id = $1 ORDER BY created_at DESC LIMIT 10", user_id)
            text = "Последние действия администратора:\n"
            for log in logs:
                text += f"[{log['created_at']}] {log['action']}: {json.dumps(log['details'], ensure_ascii=False)}\n"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
            context.user_data["state"] = STATE_VIEW_LOGS
            await update.callback_query.answer()
            await log_analytics(user_id, "view_logs")
            return STATE_VIEW_LOGS

async def top_referrals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик топа рефералов."""
    REQUESTS.labels(endpoint="top_referrals").inc()
    with RESPONSE_TIME.labels(endpoint="top_referrals").time():
        async with (await ensure_db_pool()) as conn:
            users = await conn.fetch("SELECT username, referrals FROM users ORDER BY jsonb_array_length(referrals::jsonb) DESC LIMIT 5")
            text = "🏆 Топ рефералов:\n"
            for i, user in enumerate(users, 1):
                ref_count = len(json.loads(user['referrals'])) if user['referrals'] != '[]' else 0
                text += f"{i}. @{user['username']}: {ref_count} рефералов\n"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=PROFILE)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
            context.user_data["state"] = STATE_TOP_REFERRALS
            await update.callback_query.answer()
            await log_analytics(update.effective_user.id, "top_referrals")
            return STATE_TOP_REFERRALS

async def top_purchases(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик топа покупок."""
    REQUESTS.labels(endpoint="top_purchases").inc()
    with RESPONSE_TIME.labels(endpoint="top_purchases").time():
        async with (await ensure_db_pool()) as conn:
            users = await conn.fetch("SELECT username, stars_bought FROM users ORDER BY stars_bought DESC LIMIT 5")
            text = "🏆 Топ покупок:\n"
            for i, user in enumerate(users, 1):
                text += f"{i}. @{user['username']}: {user['stars_bought']} звезд\n"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=PROFILE)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
            context.user_data["state"] = STATE_TOP_PURCHASES
            await update.callback_query.answer()
            await log_analytics(update.effective_user.id, "top_purchases")
            return STATE_TOP_PURCHASES

async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстового ввода."""
    REQUESTS.labels(endpoint="handle_text_input").inc()
    with RESPONSE_TIME.labels(endpoint="handle_text_input").time():
        user_id = update.effective_user.id
        if update.edited_message:
            logger.info(f"Игнорирование отредактированного сообщения от user_id={user_id}")
            return context.user_data.get("state", STATE_MAIN_MENU)
        if not update.message:
            logger.error(f"Отсутствует сообщение в update для user_id={user_id}")
            return context.user_data.get("state", STATE_MAIN_MENU)
        text = update.message.text.strip()
        state = context.user_data.get("state", STATE_MAIN_MENU)
        input_state = context.user_data.get("input_state")
        logger.info(f"Текстовый ввод: user_id={user_id}, state={state}, input_state={input_state}, text={text}")

        async with (await ensure_db_pool()) as conn:
            if state == STATE_BUY_STARS_RECIPIENT and input_state == "recipient":
                if not text.startswith("@"):
                    await update.message.reply_text("Username должен начинаться с @!")
                    await log_analytics(user_id, "set_recipient_error", {"error": "invalid_username"})
                    return STATE_BUY_STARS_RECIPIENT
                buy_data = context.user_data.get("buy_data", {})
                buy_data["recipient"] = text
                context.user_data["buy_data"] = buy_data
                context.user_data.pop("input_state", None)
                text = "Выберите параметры покупки:"
                recipient_display = buy_data["recipient"].lstrip("@")
                keyboard = [
                    [InlineKeyboardButton(f"Кому звезды: @{recipient_display}", callback_data=SET_RECIPIENT)],
                    [InlineKeyboardButton(f"Количество звезд: {buy_data.get('stars', '-')}", callback_data=SET_AMOUNT)],
                    [InlineKeyboardButton(f"Метод оплаты: {buy_data.get('payment_method', '-')}", callback_data=SET_PAYMENT)],
                    [InlineKeyboardButton(f"Цена: {buy_data.get('amount_ton', '-')}", callback_data=CONFIRM_PAYMENT)],
                    [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await update.message.reply_text(text, reply_markup=reply_markup)
                await log_analytics(user_id, "set_recipient_success", {"recipient": text})
                return STATE_BUY_STARS_RECIPIENT

            elif state == STATE_BUY_STARS_AMOUNT and input_state == "amount":
                try:
                    stars = int(text)
                    min_stars = await get_setting("min_stars_purchase") or 10
                    if stars < min_stars or stars % 50 != 0:
                        await update.message.reply_text(f"Введите количество звезд не менее {min_stars}, кратное 50!")
                        await log_analytics(user_id, "set_amount_error", {"error": "invalid_amount"})
                        return STATE_BUY_STARS_AMOUNT
                    buy_data = context.user_data.get("buy_data", {})
                    buy_data["stars"] = stars
                    context.user_data["buy_data"] = buy_data
                    context.user_data.pop("input_state", None)
                    text = "Выберите параметры покупки:"
                    recipient_display = buy_data.get("recipient", "-").lstrip("@")
                    keyboard = [
                        [InlineKeyboardButton(f"Кому звезды: @{recipient_display}", callback_data=SET_RECIPIENT)],
                        [InlineKeyboardButton(f"Количество звезд: {stars}", callback_data=SET_AMOUNT)],
                        [InlineKeyboardButton(f"Метод оплаты: {buy_data.get('payment_method', '-')}", callback_data=SET_PAYMENT)],
                        [InlineKeyboardButton(f"Цена: {buy_data.get('amount_ton', '-')}", callback_data=CONFIRM_PAYMENT)],
                        [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    await update.message.reply_text(text, reply_markup=reply_markup)
                    await log_analytics(user_id, "set_amount_success", {"stars": stars})
                    return STATE_BUY_STARS_RECIPIENT
                except ValueError:
                    await update.message.reply_text("Введите корректное количество звезд!")
                    await log_analytics(user_id, "set_amount_error", {"error": "invalid_value"})
                    return STATE_BUY_STARS_AMOUNT

            elif state == STATE_EDIT_TEXT:
                text_key = context.user_data.get("text_key")
                if not text_key:
                    await update.message.reply_text("Ошибка: ключ текста не найден.")
                    context.user_data["state"] = STATE_ADMIN_EDIT_TEXTS
                    await log_analytics(user_id, "edit_text_error", {"error": "missing_text_key"})
                    return await admin_edit_texts(update, context)
                await update_text(text_key, text)
                await log_admin_action(user_id, f"Редактирование текста: {text_key}", {"text_key": text_key, "new_value": text})
                await update.message.reply_text(f"Текст '{text_key}' обновлен!")
                context.user_data.pop("input_state", None)
                context.user_data.pop("text_key", None)
                context.user_data["state"] = STATE_ADMIN_EDIT_TEXTS
                await log_analytics(user_id, "edit_text_success", {"text_key": text_key})
                return await admin_edit_texts(update, context)

            elif state == STATE_ADMIN_EDIT_MARKUP and input_state == "edit_markup":
                markup_type = context.user_data.get("markup_type")
                if not markup_type:
                    await update.message.reply_text("Тип наценки не выбран!")
                    context.user_data["state"] = STATE_ADMIN_EDIT_MARKUP
                    await log_analytics(user_id, "edit_markup_error", {"error": "missing_markup_type"})
                    return await admin_edit_markup(update, context)
                try:
                    markup = float(text)
                    if markup < 0:
                        raise ValueError("Наценка не может быть отрицательной")
                    await update_setting(markup_type, markup)
                    await log_admin_action(user_id, f"Обновлена наценка {markup_type}", {"markup_type": markup_type, "new_value": markup})
                    await update.message.reply_text(f"Наценка '{markup_type}' обновлена: {markup}%")
                    context.user_data.pop("markup_type", None)
                    context.user_data.pop("input_state", None)
                    context.user_data["state"] = STATE_ADMIN_EDIT_MARKUP
                    await log_analytics(user_id, "edit_markup_success", {"markup_type": markup_type, "value": markup})
                    return await admin_edit_markup(update, context)
                except ValueError:
                    await update.message.reply_text("Введите корректное число для наценки!")
                    await log_analytics(user_id, "edit_markup_error", {"error": "invalid_value"})
                    return STATE_ADMIN_EDIT_MARKUP

            elif state == STATE_ADMIN_MANAGE_ADMINS and input_state == "add_admin":
                try:
                    new_admin_id = int(text)
                    admin_ids = await get_setting("admin_ids") or [TWIN_ACCOUNT_ID]
                    if new_admin_id in admin_ids:
                        await update.message.reply_text("Пользователь уже является админом!")
                        await log_analytics(user_id, "add_admin_error", {"error": "already_admin"})
                    else:
                        admin_ids.append(new_admin_id)
                        await update_setting("admin_ids", admin_ids)
                        await log_admin_action(user_id, "Добавлен администратор", {"new_admin_id": new_admin_id})
                        await update.message.reply_text(f"Администратор {new_admin_id} добавлен!")
                        await log_analytics(user_id, "add_admin_success", {"new_admin_id": new_admin_id})
                    context.user_data.pop("input_state", None)
                    context.user_data["state"] = STATE_ADMIN_MANAGE_ADMINS
                    return await admin_manage_admins(update, context)
                except ValueError:
                    await update.message.reply_text("Введите корректный user_id!")
                    await log_analytics(user_id, "add_admin_error", {"error": "invalid_user_id"})
                    return STATE_ADMIN_MANAGE_ADMINS

            elif state == STATE_ADMIN_MANAGE_ADMINS and input_state == "remove_admin":
                try:
                    admin_id_to_remove = int(text)
                    admin_ids = await get_setting("admin_ids") or [TWIN_ACCOUNT_ID]
                    if admin_id_to_remove not in admin_ids:
                        await update.message.reply_text("Пользователь не является админом!")
                        await log_analytics(user_id, "remove_admin_error", {"error": "not_admin"})
                    elif admin_id_to_remove == TWIN_ACCOUNT_ID:
                        await update.message.reply_text("Нельзя удалить главного администратора!")
                        await log_analytics(user_id, "remove_admin_error", {"error": "main_admin"})
                    else:
                        admin_ids.remove(admin_id_to_remove)
                        await update_setting("admin_ids", admin_ids)
                        await log_admin_action(user_id, "Удален администратор", {"admin_id": admin_id_to_remove})
                        await update.message.reply_text(f"Администратор {admin_id_to_remove} удален!")
                        await log_analytics(user_id, "remove_admin_success", {"admin_id": admin_id_to_remove})
                    context.user_data.pop("input_state", None)
                    context.user_data["state"] = STATE_ADMIN_MANAGE_ADMINS
                    return await admin_manage_admins(update, context)
                except ValueError:
                    await update.message.reply_text("Введите корректный user_id!")
                    await log_analytics(user_id, "remove_admin_error", {"error": "invalid_user_id"})
                    return STATE_ADMIN_MANAGE_ADMINS

            elif state == STATE_ADMIN_EDIT_PROFIT and input_state == "profit_percent":
                try:
                    profit_percent = float(text)
                    if profit_percent < 0:
                        raise ValueError("Процент прибыли не может быть отрицательным")
                    await update_setting("profit_percent", profit_percent)
                    await log_admin_action(user_id, "Обновлен процент прибыли", {"profit_percent": profit_percent})
                    await update.message.reply_text(f"Процент прибыли обновлен: {profit_percent}%")
                    context.user_data.pop("input_state", None)
                    context.user_data["state"] = STATE_ADMIN_PANEL
                    await log_analytics(user_id, "edit_profit_success", {"profit_percent": profit_percent})
                    return await admin_panel(update, context)
                except ValueError:
                    await update.message.reply_text("Введите корректное число для процента прибыли!")
                    await log_analytics(user_id, "edit_profit_error", {"error": "invalid_value"})
                    return STATE_ADMIN_EDIT_PROFIT

            elif state == STATE_ADMIN_USER_STATS and input_state == "search_user":
                try:
                    search_user_id = int(text)
                    user = await conn.fetchrow("SELECT user_id, username, stars_bought, ref_bonus_ton, referrals FROM users WHERE user_id = $1", search_user_id)
                    if not user:
                        await update.message.reply_text("Пользователь не найден!")
                        await log_analytics(user_id, "search_user_error", {"error": "user_not_found"})
                        return STATE_ADMIN_USER_STATS
                    ref_count = len(json.loads(user["referrals"])) if user["referrals"] != '[]' else 0
                    text = await get_text(
                        "user_info",
                        username=user["username"],
                        user_id=user["user_id"],
                        stars_bought=user["stars_bought"],
                        ref_bonus_ton=user["ref_bonus_ton"],
                        ref_count=ref_count
                    )
                    keyboard = [
                        [InlineKeyboardButton("Изменить звезды", callback_data=EDIT_USER_STARS)],
                        [InlineKeyboardButton("Изменить реф. бонус", callback_data=EDIT_USER_REF_BONUS)],
                        [InlineKeyboardButton("Изменить покупки", callback_data=EDIT_USER_PURCHASES)],
                        [InlineKeyboardButton("🔙 Назад", callback_data=ADMIN_USER_STATS)]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    context.user_data["edit_user_id"] = search_user_id
                    await update.message.reply_text(text, reply_markup=reply_markup)
                    context.user_data["state"] = STATE_EDIT_USER
                    await log_analytics(user_id, "search_user_success", {"search_user_id": search_user_id})
                    return STATE_EDIT_USER
                except ValueError:
                    await update.message.reply_text("Введите корректный user_id!")
                    await log_analytics(user_id, "search_user_error", {"error": "invalid_user_id"})
                    return STATE_ADMIN_USER_STATS

            elif state == STATE_EDIT_USER and input_state in ["edit_stars", "edit_ref_bonus", "edit_purchases"]:
                try:
                    value = float(text)
                    edit_user_id = context.user_data.get("edit_user_id")
                    if not edit_user_id:
                        await update.message.reply_text("Пользователь не выбран!")
                        context.user_data["state"] = STATE_ADMIN_USER_STATS
                        await log_analytics(user_id, "edit_user_error", {"error": "no_user_selected"})
                        return await admin_user_stats(update, context)
                    if input_state == "edit_stars":
                        await conn.execute("UPDATE users SET stars_bought = $1 WHERE user_id = $2", int(value), edit_user_id)
                        await log_admin_action(user_id, "Изменено количество звезд", {"user_id": edit_user_id, "new_stars": value})
                        await update.message.reply_text(f"Звезды для user_id={edit_user_id} обновлены: {value}")
                        await log_analytics(user_id, "edit_stars_success", {"user_id": edit_user_id, "value": value})
                    elif input_state == "edit_ref_bonus":
                        await conn.execute("UPDATE users SET ref_bonus_ton = $1 WHERE user_id = $2", value, edit_user_id)
                        await log_admin_action(user_id, "Изменен реферальный бонус", {"user_id": edit_user_id, "new_ref_bonus": value})
                        await update.message.reply_text(f"Реф. бонус для user_id={edit_user_id} обновлен: {value} TON")
                        await log_analytics(user_id, "edit_ref_bonus_success", {"user_id": edit_user_id, "value": value})
                    elif input_state == "edit_purchases":
                        await conn.execute("UPDATE users SET stars_bought = $1 WHERE user_id = $2", int(value), edit_user_id)
                        await log_admin_action(user_id, "Изменено количество покупок", {"user_id": edit_user_id, "new_purchases": value})
                        await update.message.reply_text(f"Покупки для user_id={edit_user_id} обновлены: {value} звезд")
                        await log_analytics(user_id, "edit_purchases_success", {"user_id": edit_user_id, "value": value})
                    context.user_data.pop("input_state", None)
                    context.user_data["state"] = STATE_ADMIN_USER_STATS
                    return await admin_user_stats(update, context)
                except ValueError:
                    await update.message.reply_text("Введите корректное число!")
                    await log_analytics(user_id, f"{input_state}_error", {"error": "invalid_value"})
                    return STATE_EDIT_USER

            else:
                await update.message.reply_text("Неизвестная команда или состояние. Вернитесь в главное меню.")
                await log_analytics(user_id, "unknown_text_input", {"text": text, "state": state})
                return await start(update, context)

async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик callback-запросов."""
    REQUESTS.labels(endpoint="callback_query_handler").inc()
    with RESPONSE_TIME.labels(endpoint="callback_query_handler").time():
        query = update.callback_query
        user_id = query.from_user.id
        data = query.data
        logger.info(f"Callback: user_id={user_id}, data={data}")
        state = context.user_data.get("state", STATE_MAIN_MENU)

        if data == BACK_TO_MENU:
            context.user_data.clear()
            context.user_data["state"] = STATE_MAIN_MENU
            await log_analytics(user_id, "back_to_menu")
            return await start(update, context)

        elif data == BACK_TO_ADMIN:
            context.user_data["state"] = STATE_ADMIN_PANEL
            await log_analytics(user_id, "back_to_admin")
            return await admin_panel(update, context)

        elif data == PROFILE:
            await log_analytics(user_id, "profile_clicked")
            return await profile(update, context)

        elif data == REFERRALS:
            await log_analytics(user_id, "referrals_clicked")
            return await referrals(update, context)

        elif data == SUPPORT:
            await log_analytics(user_id, "support_clicked")
            return await support(update, context)

        elif data == REVIEWS:
            await log_analytics(user_id, "reviews_clicked")
            return await reviews(update, context)

        elif data == BUY_STARS:
            await log_analytics(user_id, "buy_stars_clicked")
            return await buy_stars(update, context)

        elif data == ADMIN_PANEL:
            await log_analytics(user_id, "admin_panel_clicked")
            return await admin_panel(update, context)

        elif data == ADMIN_STATS:
            await log_analytics(user_id, "admin_stats_clicked")
            return await admin_stats(update, context)

        elif data == ADMIN_EDIT_TEXTS:
            await log_analytics(user_id, "admin_edit_texts_clicked")
            return await admin_edit_texts(update, context)

        elif data == ADMIN_USER_STATS:
            await log_analytics(user_id, "admin_user_stats_clicked")
            return await admin_user_stats(update, context)

        elif data == ADMIN_EDIT_MARKUP:
            await log_analytics(user_id, "admin_edit_markup_clicked")
            return await admin_edit_markup(update, context)

        elif data == ADMIN_MANAGE_ADMINS:
            await log_analytics(user_id, "admin_manage_admins_clicked")
            return await admin_manage_admins(update, context)

        elif data == ADMIN_EDIT_PROFIT:
            await log_analytics(user_id, "admin_edit_profit_clicked")
            return await admin_edit_profit(update, context)

        elif data == EXPORT_DATA:
            await log_analytics(user_id, "export_data_clicked")
            return await export_data(update, context)

        elif data == VIEW_LOGS:
            await log_analytics(user_id, "view_logs_clicked")
            return await view_logs(update, context)

        elif data == TOP_REFERRALS:
            await log_analytics(user_id, "top_referrals_clicked")
            return await top_referrals(update, context)

        elif data == TOP_PURCHASES:
            await log_analytics(user_id, "top_purchases_clicked")
            return await top_purchases(update, context)

        elif data == SET_RECIPIENT:
            await log_analytics(user_id, "set_recipient_clicked")
            return await set_recipient(update, context)

        elif data == SET_AMOUNT:
            await log_analytics(user_id, "set_amount_clicked")
            return await set_amount(update, context)

        elif data == SET_PAYMENT:
            await log_analytics(user_id, "set_payment_clicked")
            return await set_payment_method(update, context)

        elif data == SELECT_CRYPTO_TYPE:
            await log_analytics(user_id, "select_crypto_type_clicked")
            return await select_crypto_type(update, context)

        elif data == PAY_TON_SPACE:
            buy_data = context.user_data.get("buy_data", {})
            stars = buy_data.get("stars")
            recipient = buy_data.get("recipient")
            if not stars or not recipient:
                await query.message.reply_text("Сначала выберите получателя и количество звезд!")
                await log_analytics(user_id, "pay_ton_space_error", {"error": "missing_data"})
                return state
            ton_price = context.bot_data.get("ton_price", await get_setting("ton_exchange_rate") or 2.93)
            stars_price_usd = float(await get_setting("stars_price_usd") or (PRICE_USD_PER_50 / 50))
            markup = float(await get_setting("markup_ton_space") or 20)
            commission = float(await get_setting("ton_space_commission") or 15)
            amount_usd = stars * stars_price_usd * (1 + markup / 100)
            amount_ton = (amount_usd / ton_price) * (1 + commission / 100)
            payload = await generate_payload(user_id)
            invoice_id, pay_url = await create_ton_space_invoice(amount_ton, user_id, stars, recipient, payload)
            if not invoice_id:
                invoice_id = payload
                pay_url = f"ton://transfer/{OWNER_WALLET}?amount={int(amount_ton * 1_000_000_000)}&text={payload}"
                async with (await ensure_db_pool()) as conn:
                    await conn.execute(
                        "INSERT INTO transactions (user_id, stars, amount_ton, payment_method, recipient, status, invoice_id, payload) "
                        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                        user_id, stars, amount_ton, "ton_space_direct", recipient, "pending", invoice_id, payload
                    )
            buy_data["amount_ton"] = amount_ton
            buy_data["payment_method"] = "ton_space_direct" if not TON_SPACE_API_TOKEN else "ton_space_api"
            buy_data["invoice_id"] = invoice_id
            buy_data["pay_url"] = pay_url
            buy_data["payload"] = payload
            context.user_data["buy_data"] = buy_data
            await log_analytics(user_id, "pay_ton_space", {"stars": stars, "amount_ton": amount_ton})
            return await confirm_payment(update, context)

        elif data == PAY_CRYPTOBOT:
            buy_data = context.user_data.get("buy_data", {})
            stars = buy_data.get("stars")
            recipient = buy_data.get("recipient")
            if not stars or not recipient:
                await query.message.reply_text("Сначала выберите получателя и количество звезд!")
                await log_analytics(user_id, "pay_cryptobot_error", {"error": "missing_data"})
                return state
            ton_price = context.bot_data.get("ton_price", await get_setting("ton_exchange_rate") or 2.93)
            stars_price_usd = float(await get_setting("stars_price_usd") or (PRICE_USD_PER_50 / 50))
            markup = float(await get_setting("markup_cryptobot_crypto") or 25)
            amount_usd = stars * stars_price_usd * (1 + markup / 100)
            amount_ton = amount_usd / ton_price
            payload = await generate_payload(user_id)
            invoice_id, pay_url = await create_cryptobot_invoice(amount_ton, "TON", user_id, stars, recipient, payload)
            if not invoice_id:
                invoice_id = payload
                pay_url = f"ton://transfer/{OWNER_WALLET}?amount={int(amount_ton * 1_000_000_000)}&text={payload}"
                async with (await ensure_db_pool()) as conn:
                    await conn.execute(
                        "INSERT INTO transactions (user_id, stars, amount_ton, payment_method, recipient, status, invoice_id, payload) "
                        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                        user_id, stars, amount_ton, "ton_space_direct", recipient, "pending", invoice_id, payload
                    )
            buy_data["amount_ton"] = amount_ton
            buy_data["payment_method"] = "cryptobot_ton"
            buy_data["invoice_id"] = invoice_id
            buy_data["pay_url"] = pay_url
            buy_data["payload"] = payload
            context.user_data["buy_data"] = buy_data
            await log_analytics(user_id, "pay_cryptobot", {"stars": stars, "amount_ton": amount_ton})
            return await confirm_payment(update, context)

        elif data == PAY_CARD:
            buy_data = context.user_data.get("buy_data", {})
            stars = buy_data.get("stars")
            recipient = buy_data.get("recipient")
            if not stars or not recipient:
                await query.message.reply_text("Сначала выберите получателя и количество звезд!")
                await log_analytics(user_id, "pay_card_error", {"error": "missing_data"})
                return state
            stars_price_usd = float(await get_setting("stars_price_usd") or (PRICE_USD_PER_50 / 50))
            markup = float(await get_setting("markup_cryptobot_card") or 25)
            commission = float(await get_setting("card_commission") or 10)
            amount_usd = stars * stars_price_usd * (1 + markup / 100) * (1 + commission / 100)
            payload = await generate_payload(user_id)
            invoice_id, pay_url = await create_cryptobot_invoice(amount_usd, "USD", user_id, stars, recipient, payload)
            if not invoice_id:
                await query.message.reply_text("Ошибка создания инвойса. Попробуйте другой метод оплаты.")
                await log_analytics(user_id, "pay_card_error", {"error": "invoice_creation_failed"})
                return state
            buy_data["amount_usd"] = amount_usd
            buy_data["amount_ton"] = 0.0
            buy_data["payment_method"] = "cryptobot_usd"
            buy_data["invoice_id"] = invoice_id
            buy_data["pay_url"] = pay_url
            buy_data["payload"] = payload
            context.user_data["buy_data"] = buy_data
            await log_analytics(user_id, "pay_card", {"stars": stars, "amount_usd": amount_usd})
            return await confirm_payment(update, context)

        elif data == CHECK_PAYMENT:
            await log_analytics(user_id, "check_payment_clicked")
            return await check_payment(update, context)

        elif data in [EDIT_TEXT_WELCOME, EDIT_TEXT_BUY_PROMPT, EDIT_TEXT_PROFILE, EDIT_TEXT_REFERRALS,
                      EDIT_TEXT_TECH_SUPPORT, EDIT_TEXT_REVIEWS, EDIT_TEXT_BUY_SUCCESS]:
            await log_analytics(user_id, "edit_text_clicked", {"text_key": data})
            return await edit_text_prompt(update, context)

        elif data == "search_user":
            context.user_data["input_state"] = "search_user"
            await query.message.reply_text("Введите user_id пользователя:")
            await log_analytics(user_id, "search_user_clicked")
            return STATE_ADMIN_USER_STATS

        elif data == LIST_USERS:
            await log_analytics(user_id, "list_users_clicked")
            return await list_users(update, context)

        elif data.startswith(SELECT_USER):
            user_id_to_edit = int(data[len(SELECT_USER):])
            async with (await ensure_db_pool()) as conn:
                user = await conn.fetchrow("SELECT user_id, username, stars_bought, ref_bonus_ton, referrals FROM users WHERE user_id = $1", user_id_to_edit)
                if not user:
                    await query.message.reply_text("Пользователь не найден!")
                    await log_analytics(user_id, "select_user_error", {"error": "user_not_found"})
                    return state
                ref_count = len(json.loads(user["referrals"])) if user["referrals"] != '[]' else 0
                text = await get_text(
                    "user_info",
                    username=user["username"],
                    user_id=user["user_id"],
                    stars_bought=user["stars_bought"],
                    ref_bonus_ton=user["ref_bonus_ton"],
                    ref_count=ref_count
                )
                keyboard = [
                    [InlineKeyboardButton("Изменить звезды", callback_data=EDIT_USER_STARS)],
                    [InlineKeyboardButton("Изменить реф. бонус", callback_data=EDIT_USER_REF_BONUS)],
                    [InlineKeyboardButton("Изменить покупки", callback_data=EDIT_USER_PURCHASES)],
                    [InlineKeyboardButton("🔙 Назад", callback_data=ADMIN_USER_STATS)]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                context.user_data["edit_user_id"] = user_id_to_edit
                await query.edit_message_text(text, reply_markup=reply_markup)
                context.user_data["state"] = STATE_EDIT_USER
                await query.answer()
                await log_analytics(user_id, "select_user", {"selected_user_id": user_id_to_edit})
                return STATE_EDIT_USER

        elif data == EDIT_USER_STARS:
            context.user_data["input_state"] = "edit_stars"
            await query.message.reply_text("Введите новое количество звезд:")
            await log_analytics(user_id, "edit_stars_clicked")
            return STATE_EDIT_USER

        elif data == EDIT_USER_REF_BONUS:
            context.user_data["input_state"] = "edit_ref_bonus"
            await query.message.reply_text("Введите новый реферальный бонус (TON):")
            await log_analytics(user_id, "edit_ref_bonus_clicked")
            return STATE_EDIT_USER

        elif data == EDIT_USER_PURCHASES:
            context.user_data["input_state"] = "edit_purchases"
            await query.message.reply_text("Введите новое количество купленных звезд:")
            await log_analytics(user_id, "edit_purchases_clicked")
            return STATE_EDIT_USER

        elif data == ADD_ADMIN:
            context.user_data["input_state"] = "add_admin"
            await query.message.reply_text("Введите user_id нового администратора:")
            await log_analytics(user_id, "add_admin_clicked")
            return STATE_ADMIN_MANAGE_ADMINS

        elif data == REMOVE_ADMIN:
            context.user_data["input_state"] = "remove_admin"
            await query.message.reply_text("Введите user_id администратора для удаления:")
            await log_analytics(user_id, "remove_admin_clicked")
            return STATE_ADMIN_MANAGE_ADMINS

        elif data in [MARKUP_TON_SPACE, MARKUP_CRYPTOBOT_CRYPTO, MARKUP_CRYPTOBOT_CARD, MARKUP_REF_BONUS]:
            context.user_data["input_state"] = "edit_markup"
            context.user_data["markup_type"] = data
            await query.message.reply_text(f"Введите новую наценку для '{data}' (%):")
            await log_analytics(user_id, "edit_markup_clicked", {"markup_type": data})
            return STATE_ADMIN_EDIT_MARKUP

        else:
            await query.answer(text="Неизвестный запрос.")
            await log_analytics(user_id, "unknown_callback", {"data": data})
            return state

async def webhook_handler(request: web.Request):
    """Обработчик вебхуков."""
    REQUESTS.labels(endpoint="webhook_handler").inc()
    with RESPONSE_TIME.labels(endpoint="webhook_handler").time():
        try:
            update = Update.de_json(await request.json(), app.bot)
            await app.process_update(update)
            return web.json_response({"status": "ok"})
        except Exception as e:
            logger.error(f"Ошибка обработки вебхука: {e}")
            ERRORS.labels(type="webhook").inc()
            return web.json_response({"status": "error"}, status=500)

async def callback_webhook_handler(request: web.Request):
    """Обработчик callback-вебхуков от платежных систем."""
    REQUESTS.labels(endpoint="callback_webhook_handler").inc()
    with RESPONSE_TIME.labels(endpoint="callback_webhook_handler").time():
        try:
            data = await request.json()
            logger.info(f"Получен callback: {data}")
            if data.get("source") == "ton_space":
                invoice_id = data.get("invoice_id")
                status = data.get("status")
                metadata = data.get("metadata", {})
                user_id = metadata.get("user_id")
                stars = metadata.get("stars")
                recipient = metadata.get("recipient")
                if status == "paid" and user_id and stars and recipient:
                    async with (await ensure_db_pool()) as conn:
                        if await issue_stars(recipient, stars, user_id):
                            await conn.execute(
                                "UPDATE users SET stars_bought = stars_bought + $1 WHERE user_id = $2",
                                stars, user_id
                            )
                            await conn.execute(
                                "UPDATE transactions SET status = $1 WHERE invoice_id = $2",
                                "completed", invoice_id
                            )
                            try:
                                await app.bot.send_message(
                                    chat_id=user_id,
                                    text=await get_text("buy_success", recipient=recipient.lstrip("@"), stars=stars)
                                )
                                await app.bot.send_message(
                                    chat_id=NEWS_CHANNEL,
                                    text=f"Успешная покупка: @{recipient.lstrip('@')} получил {stars} звезд от user_id={user_id}"
                                )
                            except Exception as e:
                                logger.error(f"Ошибка отправки сообщения: {e}")
                                ERRORS.labels(type="telegram_api").inc()
                            await log_analytics(user_id, "callback_payment_success", {"source": "ton_space", "stars": stars})
            elif data.get("source") == "cryptobot":
                invoice_id = data.get("invoice_id")
                status = data.get("status")
                payload = data.get("payload")
                async with (await ensure_db_pool()) as conn:
                    tx = await conn.fetchrow("SELECT user_id, stars, recipient FROM transactions WHERE invoice_id = $1", invoice_id)
                    if tx and status == "paid":
                        user_id, stars, recipient = tx["user_id"], tx["stars"], tx["recipient"]
                        if await issue_stars(recipient, stars, user_id):
                            await conn.execute(
                                "UPDATE users SET stars_bought = stars_bought + $1 WHERE user_id = $2",
                                stars, user_id
                            )
                            await conn.execute(
                                "UPDATE transactions SET status = $1 WHERE invoice_id = $2",
                                "completed", invoice_id
                            )
                            try:
                                await app.bot.send_message(
                                    chat_id=user_id,
                                    text=await get_text("buy_success", recipient=recipient.lstrip("@"), stars=stars)
                                )
                                await app.bot.send_message(
                                    chat_id=NEWS_CHANNEL,
                                    text=f"Успешная покупка: @{recipient.lstrip('@')} получил {stars} звезд от user_id={user_id}"
                                )
                            except Exception as e:
                                logger.error(f"Ошибка отправки сообщения: {e}")
                                ERRORS.labels(type="telegram_api").inc()
                            await log_analytics(user_id, "callback_payment_success", {"source": "cryptobot", "stars": stars})
            return web.json_response({"status": "ok"})
        except Exception as e:
            logger.error(f"Ошибка обработки callback-вебхука: {e}")
            ERRORS.labels(type="callback_webhook").inc()
            return web.json_response({"status": "error"}, status=500)

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ошибок."""
    REQUESTS.labels(endpoint="error_handler").inc()
    with RESPONSE_TIME.labels(endpoint="error_handler").time():
        try:
            error = context.error
            logger.error(f"Ошибка в обработке update: {error}", exc_info=True)
            ERRORS.labels(type="bot_error").inc()
            if update and update.effective_user:
                user_id = update.effective_user.id
                await log_analytics(user_id, "bot_error", {"error": str(error)})
                if isinstance(error, BadRequest) and "Message is not modified" not in str(error):
                    try:
                        await update.effective_message.reply_text("Произошла ошибка, попробуйте снова или обратитесь в поддержку.")
                    except Exception as e:
                        logger.error(f"Ошибка отправки сообщения об ошибке: {e}")
                        ERRORS.labels(type="telegram_api").inc()
        except Exception as e:
            logger.error(f"Ошибка в error_handler: {e}")
            ERRORS.labels(type="error_handler").inc()

async def periodic_tasks(context: ContextTypes.DEFAULT_TYPE):
    """Периодические задачи, такие как обновление курса TON."""
    while True:
        try:
            await update_ton_price(context)
        except Exception as e:
            logger.error(f"Ошибка в periodic_tasks: {e}")
            ERRORS.labels(type="periodic_tasks").inc()
        await asyncio.sleep(3600)  # Обновление каждые 60 минут

async def cleanup_transactions(context: ContextTypes.DEFAULT_TYPE):
    """Очистка устаревших транзакций."""
    REQUESTS.labels(endpoint="cleanup_transactions").inc()
    with RESPONSE_TIME.labels(endpoint="cleanup_transactions").time():
        async with (await ensure_db_pool()) as conn:
            await conn.execute(
                "UPDATE transactions SET status = 'expired' WHERE status = 'pending' AND created_at < NOW() - INTERVAL '1 hour'"
            )
            logger.info("Очистка устаревших транзакций выполнена")
            await log_analytics(0, "cleanup_transactions")

async def start_bot():
    """Запуск бота."""
    global app
    try:
        logger.info("Starting bot initialization")
        await check_environment()
        await init_db()
        app = ApplicationBuilder().token(BOT_TOKEN).build()
        conv_handler = ConversationHandler(
            entry_points=[
                CommandHandler("start", start),
                CallbackQueryHandler(callback_query_handler)
            ],
            states={
                STATE_MAIN_MENU: [
                    CallbackQueryHandler(profile, pattern=f"^{PROFILE}$"),
                    CallbackQueryHandler(referrals, pattern=f"^{REFERRALS}$"),
                    CallbackQueryHandler(support, pattern=f"^{SUPPORT}$"),
                    CallbackQueryHandler(reviews, pattern=f"^{REVIEWS}$"),
                    CallbackQueryHandler(buy_stars, pattern=f"^{BUY_STARS}$"),
                    CallbackQueryHandler(admin_panel, pattern=f"^{ADMIN_PANEL}$"),
                ],
                # ... (other states remain unchanged, omitted for brevity) ...
                STATE_ADMIN_EDIT_PROFIT: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(admin_panel, pattern=f"^{BACK_TO_ADMIN}$"),
                ],
                STATE_EXPORT_DATA: [
                    CallbackQueryHandler(admin_panel, pattern=f"^{BACK_TO_ADMIN}$"),
                ],
                STATE_VIEW_LOGS: [
                    CallbackQueryHandler(admin_panel, pattern=f"^{BACK_TO_ADMIN}$"),
                ],
            },
            fallbacks=[
                CommandHandler("start", start),
                CallbackQueryHandler(start, pattern=f"^{BACK_TO_MENU}$"),
            ],
            per_message=True  # Fix PTBUserWarning
        )
        app.add_handler(conv_handler)
        app.add_error_handler(error_handler)

        # Запуск периодических задач
        app.job_queue.run_repeating(periodic_tasks, interval=3600, first=10)
        app.job_queue.run_repeating(cleanup_transactions, interval=3600, first=60)

        # Запуск Prometheus сервера
        logger.info("Starting Prometheus server")
        start_http_server(8000)
        logger.info("Prometheus сервер запущен на порту 8000")

        # Запуск вебхука
        logger.info("Initializing aiohttp web application")
        web_app = web.Application()
        web_app.router.add_post("/callback/webhook", webhook_handler)  # Match Telegram webhook path
        web_app.router.add_post("/callback", callback_webhook_handler)
        
        # Явно замораживаем сигналы
        from aiosignal import Signal
        for signal in (web_app.on_startup, web_app.on_shutdown, web_app.on_cleanup):
            if isinstance(signal, Signal) and not signal.frozen:
                signal.freeze()
                logger.info(f"Signal {signal} frozen")
        
        logger.info("Starting aiohttp web application")
        await web_app.startup()
        runner = web.AppRunner(web_app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", PORT)
        await site.start()
        logger.info(f"Веб-сервер запущен на порту {PORT}")

        # Инициализация и запуск Telegram бота
        logger.info("Initializing Telegram bot")
        await app.initialize()
        await app.bot.set_webhook(f"{WEBHOOK_URL}/callback/webhook")
        logger.info(f"Вебхук установлен: {WEBHOOK_URL}/callback/webhook")
        
        await app.start()
        logger.info("Бот успешно запущен")
        while True:
            await asyncio.sleep(3600)  # Держим приложение активным
    except Exception as e:
        logger.error(f"Ошибка запуска бота: {e}", exc_info=True)
        ERRORS.labels(type="start_bot").inc()
        await close_db_pool()
        raise
        
async def shutdown():
    """Остановка бота."""
    logger.info("Остановка бота")
    global app
    try:
        if app is not None and app.running:
            logger.info("Shutting down Telegram bot")
            await app.shutdown()
            logger.info("Application shutdown complete")
        else:
            logger.info("Application not running, skipping shutdown")
        await close_db_pool()
        logger.info("Database pool closed")
    except Exception as e:
        logger.error(f"Ошибка при остановке бота: {e}", exc_info=True)
        ERRORS.labels(type="shutdown").inc()

def main():
    """Основная функция для запуска бота."""
    try:
        asyncio.run(start_bot())
    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки")
        asyncio.run(shutdown())
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        asyncio.run(shutdown())
        raise

if __name__ == "__main__":
    main()
