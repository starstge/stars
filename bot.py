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
import io

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
ERRORS = Counter("bot_errors_total", "Total number of errors", ["type", "endpoint"])
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
YOUR_TEST_ACCOUNT_ID = 6956377285
PRICE_USD_PER_50 = 0.81  # Цена за 50 звезд в USD
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
PORT = int(os.getenv("PORT", 8080))
MARKUP_PERCENTAGE = float(os.getenv("MARKUP_PERCENTAGE", 10))
ADMIN_BACKUP_ID = 6956377285  # ID для отправки бэкапов

# Константы callback_data
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
STATE_ADMIN_EDIT_TEXTS = "admin_edit_texts"
STATE_EDIT_TEXT = "edit_text"
STATE_ADMIN_USER_STATS = "admin_user_stats"
STATE_LIST_USERS = "list_users"
STATE_EDIT_USER = "edit_user"
STATE_ADMIN_EDIT_MARKUP = "admin_edit_markup"
STATE_ADMIN_MANAGE_ADMINS = "admin_manage_admins"
STATE_ADMIN_EDIT_PROFIT = "admin_edit_profit"
STATE_EXPORT_DATA = "export_data"
STATE_VIEW_LOGS = "view_logs"
STATE_TOP_REFERRALS = "top_referrals"
STATE_TOP_PURCHASES = "top_purchases"
EDIT_TEXT_WELCOME = "edit_text_welcome"
EDIT_TEXT_BUY_PROMPT = "edit_text_buy_prompt"
EDIT_TEXT_PROFILE = "edit_text_profile"
EDIT_TEXT_REFERRALS = "edit_text_referrals"
EDIT_TEXT_TECH_SUPPORT = "edit_text_tech_support"
EDIT_TEXT_NEWS = "edit_text_news"
EDIT_TEXT_BUY_SUCCESS = "edit_text_buy_success"
MARKUP_TON_SPACE = "markup_ton_space"
MARKUP_CRYPTOBOT_CRYPTO = "markup_cryptobot_crypto"
MARKUP_CRYPTOBOT_CARD = "markup_cryptobot_card"
MARKUP_REF_BONUS = "markup_ref_bonus"
ADD_ADMIN = "add_admin"
REMOVE_ADMIN = "remove_admin"
SELECT_USER = "select_user_"
EDIT_USER_STARS = "edit_user_stars"
EDIT_USER_REF_BONUS = "edit_user_ref_bonus"
PAY_TON_SPACE = "pay_ton_space"
PAY_CRYPTOBOT = "pay_cryptobot"
PAY_CARD = "pay_card"
SET_RECIPIENT = "set_recipient"
SET_AMOUNT = "set_amount"
SET_PAYMENT = "set_payment"
SELECT_CRYPTO_TYPE = "select_crypto_type"
CONFIRM_PAYMENT = "confirm_payment"
CHECK_PAYMENT = "check_payment"
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

async def ensure_db_pool():
    """Получение пула соединений с базой данных с ретраем."""
    global _db_pool
    async with _db_pool_lock:
        if _db_pool is None or _db_pool._closed:  # Исправлено: closed -> _closed
            logger.info("Создание нового пула базы данных (или пересоздание, если закрыт)")
            if not POSTGRES_URL:
                logger.error("POSTGRES_URL or DATABASE_URL not set")
                raise ValueError("POSTGRES_URL or DATABASE_URL not set")
            for attempt in range(3):
                try:
                    _db_pool = await asyncpg.create_pool(
                        POSTGRES_URL,
                        min_size=1,
                        max_size=10,
                        timeout=30,
                        command_timeout=60,
                        max_inactive_connection_lifetime=300
                    )
                    logger.info("Пул DB создан/пересоздан успешно")
                    return _db_pool
                except Exception as e:
                    logger.error(f"Ошибка создания пула DB (попытка {attempt+1}): {e}")
                    await asyncio.sleep(5)
            raise ValueError("Не удалось подключиться к DB после 3 попыток")
        logger.debug("Пул DB уже существует и активен")
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
                CREATE TABLE IF NOT EXISTS texts (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
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
            # Проверяем, существует ли колонка details, и переименовываем только если data не существует
            columns = await conn.fetch(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'analytics'"
            )
            column_names = [col['column_name'] for col in columns]
            if 'details' in column_names and 'data' not in column_names:
                await conn.execute("""
                    ALTER TABLE analytics RENAME COLUMN details TO data;
                """)
            # Вставка начальных текстов
            default_texts = {
                "welcome": "Добро пожаловать! Всего продано: {stars_sold} звезд. Вы купили: {stars_bought} звезд.",
                "profile": "Ваш профиль:\nID: {user_id}\nКуплено звезд: {stars_bought}\nРеферальный бонус: {ref_bonus_ton:.2f} TON\nРефералов: {ref_count}",
                "referrals": "Ваши рефералы: {ref_count}\nБонус: {ref_bonus_ton:.2f} TON\nВаша реферальная ссылка: {ref_link}",
                "buy_prompt": "Введите имя пользователя получателя (с @ или без):",
                "tech_support": "Свяжитесь с поддержкой: https://t.me/CheapStarsShop_support",
                "news": "Новости канала: https://t.me/cheapstarshop_news",
                "buy_success": "Успешно куплено {stars} звезд для {recipient}!"
            }
            for key, value in default_texts.items():
                await conn.execute(
                    "INSERT INTO texts (key, value) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING",
                    key, value
                )
        logger.info("База данных инициализирована")
    except Exception as e:
        logger.error(f"Ошибка инициализации базы данных: {e}", exc_info=True)
        raise
async def close_db_pool():
    """Закрытие пула соединений."""
    global _db_pool
    async with _db_pool_lock:
        if _db_pool is not None and not _db_pool._closed:  # Исправлено: closed -> _closed
            await _db_pool.close()
            _db_pool = None
            logger.info("Пул DB закрыт")

async def get_text(key, **kwargs):
    """Получение текста из базы данных."""
    async with (await ensure_db_pool()) as conn:
        text = await conn.fetchval("SELECT value FROM texts WHERE key = $1", key)
        if text:
            return text.format(**kwargs)
        return f"Текст для {key} не найден"

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
    """Обновление курса TON (заглушка, предполагается API-запрос)."""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get("https://api.coingecko.com/api/v3/simple/price?ids=the-open-network&vs_currencies=usd") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    ton_price = data["the-open-network"]["usd"]
                    app.bot_data["ton_price"] = ton_price
                    logger.info(f"TON price updated: {ton_price} USD")
                else:
                    logger.error(f"Failed to fetch TON price: {resp.status}")
                    ERRORS.labels(type="api", endpoint="update_ton_price").inc()
        except Exception as e:
            logger.error(f"Error updating TON price: {e}")
            ERRORS.labels(type="api", endpoint="update_ton_price").inc()

async def generate_payload(user_id):
    """Генерация уникального payload для платежа."""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    random_str = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    payload = f"{user_id}_{timestamp}_{random_str}"
    secret = os.getenv("BOT_TOKEN").encode()  # Используем BOT_TOKEN как секрет
    signature = hmac.new(secret, payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}:{signature}"

async def verify_payload(payload, signature):
    """Проверка подписи payload."""
    secret = os.getenv("BOT_TOKEN").encode()
    expected_signature = hmac.new(secret, payload.encode(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(signature, expected_signature)

async def create_cryptobot_invoice(amount_usd, currency, user_id, stars, recipient, payload):
    """Создание инвойса в Cryptobot."""
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
                        logger.error(f"Cryptobot API error: {response.status}")
                        ERRORS.labels(type="api", endpoint="create_cryptobot_invoice").inc()
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Cryptobot invoice creation failed (attempt {attempt+1}): {e}")
                ERRORS.labels(type="api", endpoint="create_cryptobot_invoice").inc()
                await asyncio.sleep(2)
        return None, None

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

async def test_db_connection():
    """Тестирование подключения к базе данных."""
    async with (await ensure_db_pool()) as conn:
        version = await conn.fetchval("SELECT version();")
        logger.info(f"DB connected: {version}")

async def heartbeat_check(app):
    """Проверка работоспособности DB и API."""
    try:
        await test_db_connection()
        async with aiohttp.ClientSession(timeout=ClientTimeout(total=30)) as session:
            if TON_SPACE_API_TOKEN:
                async with session.get(TON_SPACE_API_URL + "/health", headers={"Authorization": f"Bearer {TON_SPACE_API_TOKEN}"}) as resp:
                    if resp.status != 200:
                        logger.warning(f"TON Space API health check failed: {resp.status}")
                        ERRORS.labels(type="api", endpoint="ton_space_health").inc()
            if CRYPTOBOT_API_TOKEN:
                async with session.get(CRYPTOBOT_API_URL + "/getMe", headers={"Crypto-Pay-API-Token": CRYPTOBOT_API_TOKEN}) as resp:
                    if resp.status != 200:
                        logger.warning(f"Cryptobot API health check failed: {resp.status}")
                        ERRORS.labels(type="api", endpoint="cryptobot_health").inc()
        logger.info("Heartbeat check passed")
    except Exception as e:
        logger.error(f"Heartbeat check failed: {e}")
        ERRORS.labels(type="heartbeat", endpoint="heartbeat").inc()
        await app.bot.send_message(
            chat_id=ADMIN_BACKUP_ID,
            text=f"⚠️ Бот: Проблема с подключением: {str(e)}"
        )

async def keep_alive(app):
    """Отправка команды /start для поддержания активности бота."""
    chat_id = str(TWIN_ACCOUNT_ID)
    try:
        await app.bot.send_message(chat_id=chat_id, text="/start")  # Исправлено: app -> app.bot
        logger.info(f"Sent /start to chat_id={chat_id} to keep bot active")
    except Exception as e:
        logger.error(f"Failed to send keep-alive /start to chat_id={chat_id}: {e}")
        ERRORS.labels(type="telegram_api", endpoint="keep_alive").inc()

async def backup_db():
    """Создание бэкапа базы данных в формате JSON."""
    try:
        async with (await ensure_db_pool()) as conn:
            users = await conn.fetch("SELECT * FROM users")
            texts = await conn.fetch("SELECT * FROM texts")
            analytics = await conn.fetch("SELECT * FROM analytics")
            backup_data = {
                "users": [dict(row) for row in users],
                "texts": [dict(row) for row in texts],
                "analytics": [dict(row) for row in analytics]
            }
            backup_file = f"db_backup_{datetime.now(pytz.UTC).strftime('%Y-%m-%d_%H-%M-%S')}.json"
            with open(backup_file, 'w', encoding='utf-8') as f:
                json.dump(backup_data, f, ensure_ascii=False, indent=2)
            logger.info(f"Бэкап создан: {backup_file}")
            return backup_file
    except Exception as e:
        logger.error(f"Ошибка создания бэкапа: {e}", exc_info=True)
        raise
        
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start."""
    REQUESTS.labels(endpoint="start").inc()
    with RESPONSE_TIME.labels(endpoint="start").time():
        user_id = update.effective_user.id
        logger.info(f"Вызов /start для user_id={user_id}, message={update.message.text if update.message else 'No message'}")
        username = update.effective_user.username or f"User_{user_id}"
        try:
            async with (await ensure_db_pool()) as conn:
                logger.debug(f"Добавление/обновление пользователя {user_id}")
                await conn.execute(
                    """
                    INSERT INTO users (user_id, username, stars_bought, ref_bonus_ton, referrals, is_new, is_admin)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (user_id) DO UPDATE SET
                        username = $2,
                        is_admin = $7
                    """,
                    user_id, username, 0, 0.0, json.dumps([]), True, user_id == 6956377285
                )
                total_stars = await conn.fetchval("SELECT SUM(stars_bought) FROM users") or 0
                user_stars = await conn.fetchval("SELECT stars_bought FROM users WHERE user_id = $1", user_id) or 0
                text = await get_text("welcome", stars_sold=total_stars, stars_bought=user_stars)
                keyboard = [
                    [
                        InlineKeyboardButton("📰 Новости", url="https://t.me/cheapstarshop_news"),
                        InlineKeyboardButton("🛠 Поддержка и отзывы", url="https://t.me/CheapStarsShop_support")
                    ],
                    [InlineKeyboardButton("👤 Профиль", callback_data=PROFILE), InlineKeyboardButton("🤝 Рефералы", callback_data=REFERRALS)],
                    [InlineKeyboardButton("💸 Купить звезды", callback_data=BUY_STARS)]
                ]
                if user_id == 6956377285:
                    keyboard.append([InlineKeyboardButton("🔧 Админ-панель", callback_data=ADMIN_PANEL)])
                reply_markup = InlineKeyboardMarkup(keyboard)
                current_message = context.user_data.get("last_start_message", {})
                new_message = {"text": text, "reply_markup": reply_markup.to_dict()}
                try:
                    if update.callback_query:
                        query = update.callback_query
                        if current_message != new_message:
                            await query.edit_message_text(text, reply_markup=reply_markup)
                        await query.answer()
                    else:
                        logger.debug(f"Отправка главного меню для user_id={user_id}")
                        await update.message.reply_text(text, reply_markup=reply_markup)
                    context.user_data["last_start_message"] = new_message
                except BadRequest as e:
                    if "Message is not modified" not in str(e):
                        logger.error(f"Ошибка отправки сообщения: {e}")
                        ERRORS.labels(type="telegram_api", endpoint="start").inc()
                await log_analytics(user_id, "start")
                context.user_data["state"] = STATE_MAIN_MENU
                logger.info(f"/start успешно обработан для user_id={user_id}")
                return STATE_MAIN_MENU
        except Exception as e:
            logger.error(f"Ошибка в start для user_id={user_id}: {e}", exc_info=True)
            ERRORS.labels(type="start", endpoint="start").inc()
            await update.message.reply_text("Произошла ошибка. Попробуйте снова или свяжитесь с поддержкой.")
            return STATE_MAIN_MENU

async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик callback-запросов."""
    query = update.callback_query
    user_id = update.effective_user.id
    data = query.data
    REQUESTS.labels(endpoint="callback_query").inc()
    with RESPONSE_TIME.labels(endpoint="callback_query").time():
        if data == BACK_TO_MENU:
            context.user_data.clear()  # Очищаем user_data для сброса состояния
            context.user_data["state"] = STATE_MAIN_MENU
            await start(update, context)
            return STATE_MAIN_MENU
        elif data == PROFILE:
            async with (await ensure_db_pool()) as conn:
                user = await conn.fetchrow("SELECT stars_bought, ref_bonus_ton, referrals FROM users WHERE user_id = $1", user_id)
                ref_count = len(json.loads(user["referrals"])) if user["referrals"] else 0
                text = await get_text(
                    "profile",
                    user_id=user_id,
                    stars_bought=user["stars_bought"],
                    ref_bonus_ton=user["ref_bonus_ton"],
                    ref_count=ref_count
                )
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
                await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
                await query.answer()
                await log_analytics(user_id, "view_profile")
                context.user_data["state"] = STATE_PROFILE
                return STATE_PROFILE
        elif data == REFERRALS:
            async with (await ensure_db_pool()) as conn:
                user = await conn.fetchrow("SELECT referrals, ref_bonus_ton FROM users WHERE user_id = $1", user_id)
                ref_count = len(json.loads(user["referrals"])) if user["referrals"] else 0
                ref_link = f"https://t.me/CheapStarsShop_bot?start=ref_{user_id}"  # Исправлено на _bot
                text = await get_text("referrals", ref_count=ref_count, ref_bonus_ton=user["ref_bonus_ton"], ref_link=ref_link)
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
                await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
                await query.answer()
                await log_analytics(user_id, "view_referrals")
                context.user_data["state"] = STATE_REFERRALS
                return STATE_REFERRALS
        elif data == BUY_STARS:
            await query.message.reply_text("Введите имя пользователя получателя (с @ или без):")
            context.user_data["state"] = STATE_BUY_STARS_RECIPIENT
            await query.answer()
            await log_analytics(user_id, "start_buy_stars")
            return STATE_BUY_STARS_RECIPIENT
        elif data == ADMIN_PANEL:
            async with (await ensure_db_pool()) as conn:
                is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id)
                if not is_admin:
                    await query.answer(text="Доступ только для админов.")
                    return context.user_data.get("state", STATE_MAIN_MENU)
                keyboard = [
                    [InlineKeyboardButton("📊 Статистика", callback_data=STATE_ADMIN_STATS)],
                    [InlineKeyboardButton("💸 Наценка", callback_data=STATE_ADMIN_EDIT_MARKUP)],
                    [InlineKeyboardButton("📈 Топ рефералов", callback_data=STATE_TOP_REFERRALS)],
                    [InlineKeyboardButton("🛒 Топ покупок", callback_data=STATE_TOP_PURCHASES)],
                    [InlineKeyboardButton("📂 Копировать базу данных", callback_data=COPY_DB)],
                    [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
                ]
                await query.edit_message_text("Админ-панель", reply_markup=InlineKeyboardMarkup(keyboard))
                await query.answer()
                await log_analytics(user_id, "open_admin_panel")
                context.user_data["state"] = STATE_ADMIN_PANEL
                return STATE_ADMIN_PANEL
        elif data == COPY_DB:
            if user_id != 6956377285:
                await query.answer(text="Доступ только для главного админа.")
                return context.user_data.get("state", STATE_MAIN_MENU)
            try:
                backup_file = await backup_db()
                with open(backup_file, 'rb') as f:
                    await query.message.reply_document(document=f, filename=backup_file)
                await query.answer(text="Бэкап базы данных отправлен.")
                await log_analytics(user_id, "copy_db")
            except Exception as e:
                logger.error(f"Ошибка при создании/отправке бэкапа: {e}", exc_info=True)
                await query.answer(text="Ошибка при создании бэкапа. Попробуйте позже.")
            context.user_data["state"] = STATE_ADMIN_PANEL
            return STATE_ADMIN_PANEL
        elif data == STATE_ADMIN_STATS:
            async with (await ensure_db_pool()) as conn:
                total_users = await conn.fetchval("SELECT COUNT(*) FROM users")
                total_stars = await conn.fetchval("SELECT SUM(stars_bought) FROM users") or 0
                total_referrals = await conn.fetchval("SELECT SUM(jsonb_array_length(referrals)) FROM users") or 0
                text = (
                    f"📊 Статистика:\n"
                    f"Всего пользователей: {total_users}\n"
                    f"Всего куплено звезд: {total_stars}\n"
                    f"Всего рефералов: {total_referrals}"
                )
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=ADMIN_PANEL)]]
                await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
                await query.answer()
                await log_analytics(user_id, "view_stats")
                context.user_data["state"] = STATE_ADMIN_STATS
                return STATE_ADMIN_STATS
        elif data == STATE_ADMIN_EDIT_MARKUP:
            await query.message.reply_text("Введите новый процент наценки (например, 10 для 10%):")
            await query.answer()
            context.user_data["state"] = STATE_ADMIN_EDIT_MARKUP
            return STATE_ADMIN_EDIT_MARKUP
        elif data == STATE_TOP_REFERRALS:
            async with (await ensure_db_pool()) as conn:
                users = await conn.fetch("SELECT user_id, username, referrals FROM users ORDER BY jsonb_array_length(referrals) DESC LIMIT 10")
                text = "🏆 Топ-10 рефералов:\n"
                for i, user in enumerate(users, 1):
                    ref_count = len(json.loads(user["referrals"])) if user["referrals"] != '[]' else 0
                    text += f"{i}. @{user['username'] or 'Unknown'} (ID: {user['user_id']}): {ref_count} рефералов\n"
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=ADMIN_PANEL)]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                try:
                    await query.edit_message_text(text, reply_markup=reply_markup)
                except BadRequest as e:
                    if "Message is not modified" not in str(e):
                        logger.error(f"Ошибка редактирования сообщения: {e}")
                        ERRORS.labels(type="telegram_api", endpoint="top_referrals").inc()
                await query.answer()
                await log_analytics(user_id, "view_top_referrals")
                context.user_data["state"] = STATE_TOP_REFERRALS
                return STATE_TOP_REFERRALS
        elif data == STATE_TOP_PURCHASES:
            async with (await ensure_db_pool()) as conn:
                users = await conn.fetch("SELECT user_id, username, stars_bought FROM users ORDER BY stars_bought DESC LIMIT 10")
                text = "🏆 Топ-10 покупок:\n"
                for i, user in enumerate(users, 1):
                    text += f"{i}. @{user['username'] or 'Unknown'} (ID: {user['user_id']}): {user['stars_bought']} звезд\n"
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=ADMIN_PANEL)]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                try:
                    await query.edit_message_text(text, reply_markup=reply_markup)
                except BadRequest as e:
                    if "Message is not modified" not in str(e):
                        logger.error(f"Ошибка редактирования сообщения: {e}")
                        ERRORS.labels(type="telegram_api", endpoint="top_purchases").inc()
                await query.answer()
                await log_analytics(user_id, "view_top_purchases")
                context.user_data["state"] = STATE_TOP_PURCHASES
                return STATE_TOP_PURCHASES
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
            return STATE_BUY_STARS_CONFIRM
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
                return STATE_BUY_STARS_PAYMENT_METHOD
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
            return STATE_BUY_STARS_CONFIRM
        elif data == PAY_CARD:
            await query.message.reply_text("Оплата картой пока не поддерживается.")
            await query.answer()
            context.user_data["state"] = STATE_BUY_STARS_PAYMENT_METHOD
            return STATE_BUY_STARS_PAYMENT_METHOD
        elif data == CHECK_PAYMENT:
            buy_data = context.user_data.get("buy_data", {})
            invoice_id = buy_data.get("invoice_id")
            payload = buy_data.get("payload")
            if not invoice_id or not payload:
                await query.message.reply_text("Нет активной оплаты.")
                await query.answer()
                return STATE_BUY_STARS_CONFIRM
            async with aiohttp.ClientSession(timeout=ClientTimeout(total=30)) as session:
                headers = {"Crypto-Pay-API-Token": CRYPTOBOT_API_TOKEN}
                async with session.get(f"{CRYPTOBOT_API_URL}/getInvoices?invoice_ids={invoice_id}", headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        invoice = data["result"]["items"][0]
                        if invoice["status"] == "paid":
                            stars = buy_data["stars"]
                            recipient = buy_data["recipient"]
                            async with (await ensure_db_pool()) as conn:
                                await conn.execute(
                                    "UPDATE users SET stars_bought = stars_bought + $1 WHERE user_id = $2",
                                    stars, user_id
                                )
                            await query.message.reply_text(
                                await get_text("buy_success", stars=stars, recipient=recipient),
                                reply_markup=InlineKeyboardMarkup([
                                    [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
                                ])
                            )
                            transaction_cache[payload] = {"status": "completed", "user_id": user_id, "stars": stars}
                            await log_analytics(user_id, "payment_success")
                            context.user_data["state"] = STATE_MAIN_MENU
                        else:
                            await query.message.reply_text("Оплата не подтверждена. Попробуйте еще раз.")
                            await log_analytics(user_id, "payment_check_failed")
                    else:
                        await query.message.reply_text("Ошибка проверки оплаты.")
                        await log_analytics(user_id, "payment_check_error")
                await query.answer()
                return STATE_BUY_STARS_CONFIRM
        elif data.startswith("set_amount_"):
            stars = int(data.replace("set_amount_", ""))
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
            await log_analytics(user_id, "set_amount")
            context.user_data["state"] = STATE_BUY_STARS_PAYMENT_METHOD
            return STATE_BUY_STARS_PAYMENT_METHOD
        elif data in ["7", "8", "10", "11", "14"]:  # Игнорируем устаревшие callback_data
            logger.warning(f"Устаревший callback_data: {data}")
            await query.answer(text="Эта команда устарела. Пожалуйста, используйте меню.")
            return context.user_data.get("state", STATE_MAIN_MENU)
        else:
            logger.warning(f"Неизвестный callback_data: {data}")
            await query.answer(text="Команда не распознана. Пожалуйста, используйте меню.")
            return context.user_data.get("state", STATE_MAIN_MENU)
            
async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений для состояний."""
    user_id = update.effective_user.id
    state = context.user_data.get("state", STATE_MAIN_MENU)
    REQUESTS.labels(endpoint="handle_text_input").inc()
    with RESPONSE_TIME.labels(endpoint="handle_text_input").time():
        text = update.message.text.strip()
        if state == STATE_BUY_STARS_RECIPIENT:
            if not text.startswith("@"):
                text = f"@{text}"
            context.user_data["buy_data"] = context.user_data.get("buy_data", {})
            context.user_data["buy_data"]["recipient"] = text
            await update.message.reply_text(
                await get_text("buy_prompt"),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("50 звезд", callback_data="set_amount_50")],
                    [InlineKeyboardButton("100 звезд", callback_data="set_amount_100")],
                    [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
                ])
            )
            context.user_data["state"] = STATE_BUY_STARS_AMOUNT
            await log_analytics(user_id, "set_recipient", {"recipient": text})
            return STATE_BUY_STARS_AMOUNT
        elif state == STATE_BUY_STARS_AMOUNT:
            try:
                stars = int(text)
                if stars < 1:
                    await update.message.reply_text("Введите количество звезд (положительное число).")
                    return state
                context.user_data["buy_data"]["stars"] = stars
                amount_usd = (stars / 50) * PRICE_USD_PER_50 * (1 + MARKUP_PERCENTAGE / 100)
                context.user_data["buy_data"]["amount_usd"] = round(amount_usd, 2)
                await update.message.reply_text(
                    f"Вы выбрали {stars} звезд. Стоимость: ${amount_usd:.2f}",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("TON Space", callback_data=PAY_TON_SPACE)],
                        [InlineKeyboardButton("Cryptobot (Crypto)", callback_data=PAY_CRYPTOBOT)],
                        [InlineKeyboardButton("Cryptobot (Card)", callback_data=PAY_CARD)],
                        [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
                    ])
                )
                context.user_data["state"] = STATE_BUY_STARS_PAYMENT_METHOD
                await log_analytics(user_id, "set_amount", {"stars": stars, "amount_usd": amount_usd})
                return STATE_BUY_STARS_PAYMENT_METHOD
            except ValueError:
                await update.message.reply_text("Введите число для количества звезд.")
                return state
        elif state in [STATE_EDIT_TEXT, STATE_ADMIN_EDIT_MARKUP, STATE_ADMIN_MANAGE_ADMINS, STATE_ADMIN_EDIT_PROFIT, STATE_EDIT_USER]:
            if state == STATE_EDIT_TEXT:
                key = context.user_data.get("edit_text_key")
                async with (await ensure_db_pool()) as conn:
                    await conn.execute("UPDATE texts SET value = $1 WHERE key = $2", text, key)
                await update.message.reply_text(f"Текст для {key} обновлен.", reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]
                ]))
                await log_analytics(user_id, "edit_text", {"key": key})
            elif state == STATE_ADMIN_EDIT_MARKUP:
                try:
                    markup = float(text)
                    os.environ["MARKUP_PERCENTAGE"] = str(markup)
                    await update.message.reply_text(f"Наценка обновлена: {markup}%", reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]
                    ]))
                    await log_analytics(user_id, "edit_markup", {"markup": markup})
                except ValueError:
                    await update.message.reply_text("Введите число для наценки.")
                    return state
            elif state == STATE_ADMIN_MANAGE_ADMINS:
                try:
                    admin_id = int(text)
                    action = context.user_data.get("admin_action")
                    async with (await ensure_db_pool()) as conn:
                        await conn.execute(
                            "UPDATE users SET is_admin = $1 WHERE user_id = $2",
                            action == ADD_ADMIN, admin_id
                        )
                    await update.message.reply_text(
                        f"Пользователь {admin_id} {'добавлен в админы' if action == ADD_ADMIN else 'удален из админов'}.",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]
                        ])
                    )
                    await log_analytics(user_id, "manage_admins", {"admin_id": admin_id, "action": action})
                except ValueError:
                    await update.message.reply_text("Введите ID пользователя.")
                    return state
            context.user_data["state"] = STATE_ADMIN_PANEL
            return STATE_ADMIN_PANEL
        else:
            await update.message.reply_text("Команда не распознана. Используйте меню.")
            context.user_data["state"] = STATE_MAIN_MENU
            return STATE_MAIN_MENU

async def handle_webhook(request):
    """Обработчик входящих вебхуков от Telegram."""
    try:
        data = await request.json()
        logger.debug(f"Получен вебхук: {data}")
        update = Update.de_json(data, app.bot)
        if update:
            logger.info(f"Обработка обновления: update_id={update.update_id}")
            await app.process_update(update)
            return web.Response(status=200)
        else:
            logger.warning("Получен пустой или некорректный update")
            return web.Response(status=400)
    except Exception as e:
        logger.error(f"Ошибка обработки вебхука: {e}", exc_info=True)
        ERRORS.labels(type="webhook", endpoint="handle_webhook").inc()
        return web.Response(status=500)
        
async def start_bot():
    """Запуск бота с вебхуком и планировщиком."""
    global app
    try:
        await check_environment()
        await init_db()
        await test_db_connection()
        app = ApplicationBuilder().token(BOT_TOKEN).build()

        scheduler = AsyncIOScheduler()
        scheduler.add_job(
            update_ton_price,
            trigger="interval",
            minutes=5,
            timezone=pytz.UTC
        )
        scheduler.add_job(
            keep_alive,
            trigger="interval",
            minutes=10,
            args=[app],
            timezone=pytz.UTC
        )
        scheduler.add_job(
            heartbeat_check,
            trigger="interval",
            minutes=5,
            args=[app],
            timezone=pytz.UTC
        )
        scheduler.add_job(
            backup_db,
            trigger="cron",
            hour=0,
            minute=0,
            timezone=pytz.UTC
        )
        scheduler.start()
        logger.info("Планировщик запущен")

        conv_handler = ConversationHandler(
            entry_points=[
                CallbackQueryHandler(callback_query_handler)
            ],
            states={
                STATE_MAIN_MENU: [
                    CallbackQueryHandler(callback_query_handler),
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
            fallbacks=[
                CommandHandler("start", start)
            ]
        )

        app.add_handler(CommandHandler("start", start))
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
        ERRORS.labels(type="startup", endpoint="start_bot").inc()
        raise
        
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ошибок."""
    logger.error(f"Ошибка: {context.error}", exc_info=True)
    ERRORS.labels(type="bot", endpoint="error_handler").inc()
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
