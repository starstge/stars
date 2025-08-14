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

# Настройка логирования с ротацией
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.DEBUG,  # Изменено на DEBUG для более детального логирования
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
NEWS_CHANNEL = "https://t.me/cheapstarshop_news"
TWIN_ACCOUNT_ID = int(os.getenv("TWIN_ACCOUNT_ID", 6956377285))
YOUR_TEST_ACCOUNT_ID = 6956377285
PRICE_USD_PER_50 = 0.81  # Цена за 50 звезд в USD
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
PORT = int(os.getenv("PORT", 8080))
MARKUP_PERCENTAGE = float(os.getenv("MARKUP_PERCENTAGE", 10))
ADMIN_BACKUP_ID = 6956377285  # ID для отправки бэкапов

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
SET_RECIPIENT = "set_recipient"
SET_AMOUNT = "set_amount"
SET_PAYMENT = "set_payment"
SELECT_CRYPTO_TYPE = "select_crypto_type"
CONFIRM_PAYMENT = "confirm_payment"
CHECK_PAYMENT = "check_payment"
BROADCAST_MESSAGE = "broadcast_message"
CONFIRM_BROADCAST = "confirm_broadcast"
CANCEL_BROADCAST = "cancel_broadcast"
BACK_TO_EDIT_PROFILE = "back_to_edit_profile"

# Список всех состояний для ConversationHandler
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
app = None
transaction_cache = TTLCache(maxsize=1000, ttl=3600)  # Кэш транзакций на 1 час

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
        else:
            logger.debug("Используется существующий пул базы данных")
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
            columns = await conn.fetch(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'analytics'"
            )
            column_names = [col['column_name'] for col in columns]
            if 'details' in column_names and 'data' not in column_names:
                await conn.execute("""
                    ALTER TABLE analytics RENAME COLUMN details TO data;
                """)
            default_texts = {
                "welcome": "Добро пожаловать! Всего продано: {stars_sold} звезд. Вы купили: {stars_bought} звезд.",
                "profile": "Ваш профиль:\nID: {user_id}\nКуплено звезд: {stars_bought}\nРеферальный бонус: {ref_bonus_ton:.2f} TON\nРефералов: {ref_count}",
                "referrals": "Ваши рефералы: {ref_count}\nБонус: {ref_bonus_ton:.2f} TON\nВаша реферальная ссылка: {ref_link}",
                "buy_prompt": "Введите имя пользователя получателя (с @ или без):",
                "tech_support": "Свяжитесь с поддержкой: https://t.me/CheapStarsShop_support",
                "news": "Новости канала: https://t.me/cheapstarshop_news",
                "buy_success": "Успешно куплено {stars} звезд для {recipient}!",
                "all_users": "Список всех пользователей:\n{users_list}"
            }
            for key, value in default_texts.items():
                await conn.execute(
                    "INSERT INTO texts (key, value) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING",
                    key, value
                )
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
    """Обновление цены TON с использованием TonAPI."""
    try:
        headers = {"Authorization": f"Bearer {TON_API_KEY}"}
        url = "https://tonapi.io/v2/rates?tokens=ton&currencies=usd"
        logger.debug(f"Запрос к TonAPI: {url}")
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            ton_price = data["rates"]["TON"]["prices"]["USD"]
            diff_24h = data["rates"]["TON"].get("diff_24h", {}).get("USD", "0.0")
            try:
                # Replace Unicode minus sign (U+2212) with standard minus (-)
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
        else:
            logger.error(f"Ошибка получения цены TON: {response.status_code} - {response.text}")
            ERRORS.labels(type="api", endpoint="update_ton_price").inc()
    except Exception as e:
        logger.error(f"Ошибка получения цены TON: {e}", exc_info=True)
        ERRORS.labels(type="api", endpoint="update_ton_price").inc()

async def ton_price_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /tonprice."""
    REQUESTS.labels(endpoint="tonprice").inc()
    with RESPONSE_TIME.labels(endpoint="tonprice").time():
        user_id = update.effective_user.id
        try:
            if "ton_price_info" not in app.bot_data or app.bot_data["ton_price_info"].get("price", 0.0) == 0.0:
                await update_ton_price()
            if "ton_price_info" not in app.bot_data or app.bot_data["ton_price_info"].get("price", 0.0) == 0.0:
                await update.message.reply_text("Ошибка получения цены TON. Попробуйте позже.")
                return
            price = app.bot_data["ton_price_info"]["price"]
            diff_24h = app.bot_data["ton_price_info"]["diff_24h"]
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
    required_vars = ["BOT_TOKEN", "POSTGRES_URL", "SPLIT_API_TOKEN", "PROVIDER_TOKEN", "OWNER_WALLET", "WEBHOOK_URL"]
    optional_vars = ["TON_SPACE_API_TOKEN", "CRYPTOBOT_API_TOKEN", "TON_API_KEY"]
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
    for var in optional_vars:
        value = os.getenv(var)
        if not value:
            logger.warning(f"Опциональная переменная окружения {var} не установлена")
        else:
            logger.debug(f"Опциональная переменная окружения {var} установлена")

async def test_db_connection():
    """Тестирование подключения к базе данных."""
    try:
        async with (await ensure_db_pool()) as conn:
            version = await conn.fetchval("SELECT version();")
            logger.info(f"DB connected: {version}")
    except Exception as e:
        logger.error(f"Ошибка подключения к базе данных: {e}", exc_info=True)
        raise

async def heartbeat_check(app):
    """Проверка работоспособности DB и API."""
    try:
        await test_db_connection()
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
    """Создание бэкапа базы данных в формате JSON."""
    try:
        async with (await ensure_db_pool()) as conn:
            users = await conn.fetch("SELECT * FROM users")
            texts = await conn.fetch("SELECT * FROM texts")
            analytics = await conn.fetch("SELECT * FROM analytics")
            backup_data = {
                "users": [
                    {
                        **dict(row),
                        "referrals": json.loads(row["referrals"]) if row["referrals"] else []
                    } for row in users
                ],
                "texts": [dict(row) for row in texts],
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
            with open(backup_file, 'w', encoding='utf-8') as f:
                json.dump(backup_data, f, ensure_ascii=False, indent=2)
            logger.info(f"Бэкап создан: {backup_file}")
            return backup_file, backup_data
    except Exception as e:
        logger.error(f"Ошибка создания бэкапа: {e}", exc_info=True)
        raise

async def broadcast_new_menu():
    """Рассылка нового меню всем пользователям для устранения устаревших callback_data."""
    try:
        async with (await ensure_db_pool()) as conn:
            users = await conn.fetch("SELECT user_id FROM users")
            total_stars = await conn.fetchval("SELECT SUM(stars_bought) FROM users") or 0
            for user in users:
                user_id = user["user_id"]
                try:
                    user_stars = await conn.fetchval("SELECT stars_bought FROM users WHERE user_id = $1", user_id) or 0
                    text = await get_text("welcome", stars_sold=total_stars, stars_bought=user_stars)
                    text += "\n\n⚠️ Используйте новое меню ниже для корректной работы бота."
                    keyboard = [
                        [
                            InlineKeyboardButton("📰 Новости", url=NEWS_CHANNEL),
                            InlineKeyboardButton("🛠 Поддержка и отзывы", url=SUPPORT_CHANNEL)
                        ],
                        [InlineKeyboardButton("👤 Профиль", callback_data=PROFILE), InlineKeyboardButton("🤝 Рефералы", callback_data=REFERRALS)],
                        [InlineKeyboardButton("💸 Купить звезды", callback_data=BUY_STARS)]
                    ]
                    if user_id == 6956377285:
                        keyboard.append([InlineKeyboardButton("🔧 Админ-панель", callback_data=ADMIN_PANEL)])
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    last_message = app.bot_data.get(f"last_message_{user_id}", {})
                    if last_message.get("message_id") and last_message.get("chat_id"):
                        try:
                            await app.bot.edit_message_text(
                                chat_id=last_message["chat_id"],
                                message_id=last_message["message_id"],
                                text=text,
                                reply_markup=reply_markup
                            )
                            logger.info(f"Сообщение обновлено для user_id={user_id}, message_id={last_message['message_id']}")
                        except BadRequest as e:
                            if "Message to edit not found" in str(e) or "Message is not modified" in str(e):
                                sent_message = await app.bot.send_message(
                                    chat_id=user_id,
                                    text=text,
                                    reply_markup=reply_markup
                                )
                                app.bot_data[f"last_message_{user_id}"] = {
                                    "chat_id": sent_message.chat_id,
                                    "message_id": sent_message.message_id
                                }
                                logger.info(f"Новое сообщение отправлено для user_id={user_id}, message_id={sent_message.message_id}")
                            else:
                                logger.error(f"Ошибка редактирования сообщения для user_id={user_id}: {e}")
                                ERRORS.labels(type="telegram_api", endpoint="broadcast_new_menu").inc()
                    else:
                        sent_message = await app.bot.send_message(
                            chat_id=user_id,
                            text=text,
                            reply_markup=reply_markup
                        )
                        app.bot_data[f"last_message_{user_id}"] = {
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
                await app.bot.send_message(chat_id=user["user_id"], text=message)
                success_count += 1
                await asyncio.sleep(0.05)
            except TelegramError as e:
                logger.error(f"Ошибка отправки сообщения пользователю {user['user_id']}: {e}")
                failed_count += 1
        return success_count, failed_count

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start."""
    REQUESTS.labels(endpoint="start").inc()
    with RESPONSE_TIME.labels(endpoint="start").time():
        user_id = update.effective_user.id
        username = update.effective_user.username or f"User_{user_id}"
        logger.info(f"Вызов /start для user_id={user_id}, message={update.message.text if update.message else 'No message'}")
        try:
            async with (await ensure_db_pool()) as conn:
                ref_id = None
                if update.message and update.message.text.startswith("/start ref_"):
                    try:
                        ref_id = int(update.message.text.split("ref_")[1])
                        referrer_exists = await conn.fetchval(
                            "SELECT EXISTS(SELECT 1 FROM users WHERE user_id = $1)", ref_id
                        )
                        if referrer_exists and ref_id != user_id:
                            user_refs = await conn.fetchval(
                                "SELECT referrals FROM  users WHERE user_id = $1", ref_id
                            )
                            user_refs = json.loads(user_refs) if user_refs else []
                            if user_id not in user_refs:
                                user_refs.append(user_id)
                                await conn.execute(
                                    "UPDATE users SET referrals = $1 WHERE user_id = $2",
                                    json.dumps(user_refs), ref_id
                                )
                                logger.info(f"Добавлен реферал user_id={user_id} для referrer_id={ref_id}")
                                await log_analytics(user_id, "referred", {"referrer_id": ref_id})
                    except (ValueError, IndexError):
                        logger.warning(f"Некорректная реферальная ссылка: {update.message.text}")
                        ref_id = None

                await conn.execute(
                    """
                    INSERT INTO users (user_id, username, stars_bought, ref_bonus_ton, referrals, is_new, is_admin)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (user_id) DO UPDATE SET
                        username = $2,
                        is_admin = COALESCE(users.is_admin, $7)
                    """,
                    user_id, username, 0, 0.0, json.dumps([]), True, user_id == 6956377285
                )
                total_stars = await conn.fetchval("SELECT SUM(stars_bought) FROM users") or 0
                user_stars = await conn.fetchval("SELECT stars_bought FROM users WHERE user_id = $1", user_id) or 0
                text = await get_text("welcome", stars_sold=total_stars, stars_bought=user_stars)
                keyboard = [
                    [
                        InlineKeyboardButton("📰 Новости", url=NEWS_CHANNEL),
                        InlineKeyboardButton("🛠 Поддержка и отзывы", url=SUPPORT_CHANNEL)
                    ],
                    [InlineKeyboardButton("👤 Профиль", callback_data=PROFILE), InlineKeyboardButton("🤝 Рефералы", callback_data=REFERRALS)],
                    [InlineKeyboardButton("💸 Купить звезды", callback_data=BUY_STARS)]
                ]
                if user_id == 6956377285:
                    keyboard.append([InlineKeyboardButton("🔧 Админ-панель", callback_data=ADMIN_PANEL)])
                reply_markup = InlineKeyboardMarkup(keyboard)
                context.user_data.clear()
                context.user_data["last_start_message"] = {"text": text, "reply_markup": reply_markup.to_dict()}
                try:
                    if update.callback_query:
                        query = update.callback_query
                        await query.edit_message_text(text, reply_markup=reply_markup)
                        await query.answer()
                        app.bot_data[f"last_message_{user_id}"] = {
                            "chat_id": query.message.chat_id,
                            "message_id": query.message.message_id
                        }
                    else:
                        sent_message = await update.message.reply_text(text, reply_markup=reply_markup)
                        app.bot_data[f"last_message_{user_id}"] = {
                            "chat_id": sent_message.chat_id,
                            "message_id": sent_message.message_id
                        }
                except BadRequest as e:
                    if "Message is not modified" not in str(e):
                        logger.error(f"Ошибка отправки сообщения: {e}")
                        ERRORS.labels(type="telegram_api", endpoint="start").inc()
                        sent_message = await update.message.reply_text(text, reply_markup=reply_markup)
                        app.bot_data[f"last_message_{user_id}"] = {
                            "chat_id": sent_message.chat_id,
                            "message_id": sent_message.message_id
                        }
                await log_analytics(user_id, "start", {"referrer_id": ref_id} if ref_id else None)
                context.user_data["state"] = STATE_MAIN_MENU
                logger.info(f"/start успешно обработан для user_id={user_id}")
                return STATES[STATE_MAIN_MENU]
        except Exception as e:
            logger.error(f"Ошибка в start для user_id={user_id}: {e}", exc_info=True)
            ERRORS.labels(type="start", endpoint="start").inc()
            await update.message.reply_text("Произошла ошибка. Попробуйте снова или свяжитесь с поддержкой.")
            return STATES[STATE_MAIN_MENU]

async def show_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать админ-панель."""
    user_id = update.effective_user.id
    async with (await ensure_db_pool()) as conn:
        is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id)
        if not is_admin:
            if update.callback_query:
                await update.callback_query.answer(text="Доступ только для админов.")
            else:
                await update.message.reply_text("Доступ только для админов.")
            return context.user_data.get("state", STATES[STATE_MAIN_MENU])
        keyboard = [
            [InlineKeyboardButton("📊 Статистика", callback_data=STATE_ADMIN_STATS)],
            [InlineKeyboardButton("📢 Рассылка", callback_data=STATE_ADMIN_BROADCAST)],
            [InlineKeyboardButton("📈 Топ рефералов", callback_data=STATE_TOP_REFERRALS)],
            [InlineKeyboardButton("🛒 Топ покупок", callback_data=STATE_TOP_PURCHASES)],
            [InlineKeyboardButton("📂 Копировать базу данных", callback_data=STATE_EXPORT_DATA)],
            [InlineKeyboardButton("✏️ Редактировать профиль", callback_data=STATE_ADMIN_EDIT_PROFILE)],
            [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
        ]
        text = "🔧 Админ-панель"
        reply_markup = InlineKeyboardMarkup(keyboard)
        try:
            if update.callback_query:
                query = update.callback_query
                try:
                    await query.edit_message_text(text, reply_markup=reply_markup)
                    await query.answer()
                    app.bot_data[f"last_admin_message_{user_id}"] = {
                        "chat_id": query.message.chat_id,
                        "message_id": query.message.message_id
                    }
                    logger.info(f"Отображение админ-панели для user_id={user_id}, кнопки: {[f'{btn.text}:{btn.callback_data}' for row in keyboard for btn in row]}")
                except BadRequest as e:
                    if "Message is not modified" not in str(e):
                        logger.error(f"Ошибка редактирования сообщения админ-панели для user_id={user_id}: {e}")
                        ERRORS.labels(type="telegram_api", endpoint="show_admin_panel").inc()
                        sent_message = await query.message.reply_text(text, reply_markup=reply_markup)
                        app.bot_data[f"last_admin_message_{user_id}"] = {
                            "chat_id": sent_message.chat_id,
                            "message_id": sent_message.message_id
                        }
                        logger.info(f"Новое сообщение админ-панели отправлено для user_id={user_id}, message_id={sent_message.message_id}")
            else:
                sent_message = await update.message.reply_text(text, reply_markup=reply_markup)
                app.bot_data[f"last_admin_message_{user_id}"] = {
                    "chat_id": sent_message.chat_id,
                    "message_id": sent_message.message_id
                }
                logger.info(f"Новое сообщение админ-панели отправлено для user_id={user_id}, message_id={sent_message.message_id}")
        except BadRequest as e:
            logger.error(f"Ошибка отправки сообщения админ-панели для user_id={user_id}: {e}")
            ERRORS.labels(type="telegram_api", endpoint="show_admin_panel").inc()
            sent_message = await update.message.reply_text(text, reply_markup=reply_markup)
            app.bot_data[f"last_admin_message_{user_id}"] = {
                "chat_id": sent_message.chat_id,
                "message_id": sent_message.message_id
            }
            logger.info(f"Новое сообщение админ-панели отправлено для user_id={user_id}, message_id={sent_message.message_id}")
        await log_analytics(user_id, "open_admin_panel", {"message_id": app.bot_data.get(f"last_admin_message_{user_id}", {}).get("message_id")})
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
                try:
                    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode="HTML")
                    await query.answer()
                    app.bot_data[f"last_admin_message_{user_id}"] = {
                        "chat_id": query.message.chat_id,
                        "message_id": query.message.message_id
                    }
                except BadRequest as e:
                    if "Message is not modified" not in str(e):
                        logger.error(f"Ошибка редактирования сообщения списка пользователей для user_id={user_id}: {e}")
                        ERRORS.labels(type="telegram_api", endpoint="show_all_users").inc()
                        sent_message = await query.message.reply_text(text, reply_markup=reply_markup, parse_mode="HTML")
                        app.bot_data[f"last_admin_message_{user_id}"] = {
                            "chat_id": sent_message.chat_id,
                            "message_id": sent_message.message_id
                        }
            else:
                sent_message = await update.message.reply_text(text, reply_markup=reply_markup, parse_mode="HTML")
                app.bot_data[f"last_admin_message_{user_id}"] = {
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
        if data.isdigit():
            logger.warning(f"Устаревший числовой callback_data: {data} для user_id={user_id}, message_id={query.message.message_id}")
            await query.answer(text="Команда устарела. Пожалуйста, используйте новое меню.")
            async with (await ensure_db_pool()) as conn:
                total_stars = await conn.fetchval("SELECT SUM(stars_bought) FROM users") or 0
                user_stars = await conn.fetchval("SELECT stars_bought FROM users WHERE user_id = $1", user_id) or 0
                text = await get_text("welcome", stars_sold=total_stars, stars_bought=user_stars)
                text += "\n\n⚠️ Ваше меню устарело. Используйте новое меню ниже."
                keyboard = [
                    [
                        InlineKeyboardButton("📰 Новости", url=NEWS_CHANNEL),
                        InlineKeyboardButton("🛠 Поддержка и отзывы", url=SUPPORT_CHANNEL)
                    ],
                    [InlineKeyboardButton("👤 Профиль", callback_data=PROFILE), InlineKeyboardButton("🤝 Рефералы", callback_data=REFERRALS)],
                    [InlineKeyboardButton("💸 Купить звезды", callback_data=BUY_STARS)]
                ]
                if user_id == 6956377285:
                    keyboard.append([InlineKeyboardButton("🔧 Админ-панель", callback_data=ADMIN_PANEL)])
                reply_markup = InlineKeyboardMarkup(keyboard)
                try:
                    await query.edit_message_text(text, reply_markup=reply_markup)
                    app.bot_data[f"last_message_{user_id}"] = {
                        "chat_id": query.message.chat_id,
                        "message_id": query.message.message_id
                    }
                except BadRequest as e:
                    if "Message is not modified" not in str(e):
                        logger.error(f"Ошибка редактирования сообщения для user_id={user_id}: {e}")
                        ERRORS.labels(type="telegram_api", endpoint="callback_query").inc()
                        sent_message = await query.message.reply_text(text, reply_markup=reply_markup)
                        app.bot_data[f"last_message_{user_id}"] = {
                            "chat_id": sent_message.chat_id,
                            "message_id": sent_message.message_id
                        }
                context.user_data["state"] = STATE_MAIN_MENU
                return STATES[STATE_MAIN_MENU]
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
                    ref_bonus_ton=user["ref_bonus_ton"],
                    ref_count=ref_count
                )
                keyboard = [
                    [
                        InlineKeyboardButton("📈 Топ рефералов", callback_data=STATE_TOP_REFERRALS),
                        InlineKeyboardButton("🛒 Топ покупок", callback_data=STATE_TOP_PURCHASES)
                    ],
                    [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
                ]
                try:
                    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
                    await query.answer()
                    app.bot_data[f"last_message_{user_id}"] = {
                        "chat_id": query.message.chat_id,
                        "message_id": query.message.message_id
                    }
                except BadRequest as e:
                    if "Message is not modified" not in str(e):
                        logger.error(f"Ошибка редактирования профиля для user_id={user_id}: {e}")
                        ERRORS.labels(type="telegram_api", endpoint="profile").inc()
                        sent_message = await query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
                        app.bot_data[f"last_message_{user_id}"] = {
                            "chat_id": sent_message.chat_id,
                            "message_id": sent_message.message_id
                        }
                await log_analytics(user_id, "view_profile", {"ref_count": ref_count, "referrals": user["referrals"]})
                context.user_data["state"] = STATE_PROFILE
                return STATES[STATE_PROFILE]
        elif data == REFERRALS:
            async with (await ensure_db_pool()) as conn:
                user = await conn.fetchrow("SELECT referrals, ref_bonus_ton FROM users WHERE user_id = $1", user_id)
                ref_count = len(json.loads(user["referrals"])) if user["referrals"] else 0
                ref_link = f"https://t.me/CheapStarsShop_bot?start=ref_{user_id}"
                text = await get_text("referrals", ref_count=ref_count, ref_bonus_ton=user["ref_bonus_ton"], ref_link=ref_link)
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
                try:
                    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
                    await query.answer()
                    app.bot_data[f"last_message_{user_id}"] = {
                        "chat_id": query.message.chat_id,
                        "message_id": query.message.message_id
                    }
                except BadRequest as e:
                    if "Message is not modified" not in str(e):
                        logger.error(f"Ошибка редактирования рефералов для user_id={user_id}: {e}")
                        ERRORS.labels(type="telegram_api", endpoint="referrals").inc()
                        sent_message = await query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
                        app.bot_data[f"last_message_{user_id}"] = {
                            "chat_id": sent_message.chat_id,
                            "message_id": sent_message.message_id
                        }
                await log_analytics(user_id, "view_referrals", {"ref_count": ref_count, "referrals": user["referrals"]})
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
                try:
                    with open(backup_file, 'rb') as f:
                        await query.message.reply_document(document=f, filename=backup_file)
                    await query.answer(text="Бэкап базы данных отправлен.")
                except TelegramError as e:
                    logger.error(f"Ошибка отправки файла бэкапа: {e}")
                    ERRORS.labels(type="telegram_api", endpoint="export_data").inc()
                    text = json.dumps(backup_data, ensure_ascii=False, indent=2)
                    if len(text) > 4096:
                        await query.message.reply_text("Бэкап слишком большой для отправки текстом. Свяжитесь с поддержкой.")
                    else:
                        await query.message.reply_text(f"Бэкап базы данных:\n```\n{text}\n```", parse_mode="Markdown")
                        await query.answer(text="Бэкап отправлен как текст.")
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
                text = (
                    f"📊 Статистика:\n"
                    f"Всего пользователей: {total_users}\n"
                    f"Всего куплено звезд: {total_stars}\n"
                    f"Всего рефералов: {total_referrals}"
                )
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
                try:
                    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
                    await query.answer()
                    app.bot_data[f"last_admin_message_{user_id}"] = {
                        "chat_id": query.message.chat_id,
                        "message_id": query.message.message_id
                    }
                except BadRequest as e:
                    if "Message is not modified" not in str(e):
                        logger.error(f"Ошибка редактирования статистики для user_id={user_id}: {e}")
                        ERRORS.labels(type="telegram_api", endpoint="admin_stats").inc()
                        sent_message = await query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
                        app.bot_data[f"last_admin_message_{user_id}"] = {
                            "chat_id": sent_message.chat_id,
                            "message_id": sent_message.message_id
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
                text = "🏆 Топ-10 рефералов:\n"
                if not users:
                    text += "Нет данных о рефералах."
                for i, user in enumerate(users, 1):
                    ref_count = len(json.loads(user["referrals"])) if user["referrals"] != '[]' else 0
                    text += f"{i}. @{user['username'] or 'Unknown'}: {ref_count} рефералов\n"
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN if context.user_data.get("state") == STATE_ADMIN_PANEL else BACK_TO_MENU)]]
                try:
                    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
                    await query.answer()
                    app.bot_data[f"last_message_{user_id}"] = {
                        "chat_id": query.message.chat_id,
                        "message_id": query.message.message_id
                    }
                except BadRequest as e:
                    if "Message is not modified" not in str(e):
                        logger.error(f"Ошибка редактирования топа рефералов для user_id={user_id}: {e}")
                        ERRORS.labels(type="telegram_api", endpoint="top_referrals").inc()
                        sent_message = await query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
                        app.bot_data[f"last_message_{user_id}"] = {
                            "chat_id": sent_message.chat_id,
                            "message_id": sent_message.message_id
                        }
                await log_analytics(user_id, "view_top_referrals")
                context.user_data["state"] = STATE_TOP_REFERRALS
                return STATES[STATE_TOP_REFERRALS]
        elif data == STATE_TOP_PURCHASES:
            async with (await ensure_db_pool()) as conn:
                users = await conn.fetch("SELECT user_id, username, stars_bought FROM users ORDER BY stars_bought DESC LIMIT 10")
                text = "🏆 Топ-10 покупок:\n"
                if not users:
                    text += "Нет данных о покупках."
                for i, user in enumerate(users, 1):
                    text += f"{i}. @{user['username'] or 'Unknown'}: {user['stars_bought']} звезд\n"
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN if context.user_data.get("state") == STATE_ADMIN_PANEL else BACK_TO_MENU)]]
                try:
                    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
                    await query.answer()
                    app.bot_data[f"last_message_{user_id}"] = {
                        "chat_id": query.message.chat_id,
                        "message_id": query.message.message_id
                    }
                except BadRequest as e:
                    if "Message is not modified" not in str(e):
                        logger.error(f"Ошибка редактирования топа покупок для user_id={user_id}: {e}")
                        ERRORS.labels(type="telegram_api", endpoint="top_purchases").inc()
                        sent_message = await query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
                        app.bot_data[f"last_message_{user_id}"] = {
                            "chat_id": sent_message.chat_id,
                            "message_id": sent_message.message_id
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
                return STATES[STATE_BUY_STARS_CONFIRM]
        elif data == "set_amount_50" or data == "set_amount_100":
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
            return STATES[STATE_BUY_STARS_PAYMENT_METHOD]
        else:
            logger.warning(f"Неизвестный callback_data: {data} для user_id={user_id}, message_id={query.message.message_id}")
            await query.answer(text="Команда устарела. Пожалуйста, используйте новое меню.")
            async with (await ensure_db_pool()) as conn:
                total_stars = await conn.fetchval("SELECT SUM(stars_bought) FROM users") or 0
                user_stars = await conn.fetchval("SELECT stars_bought FROM users WHERE user_id = $1", user_id) or 0
                text = await get_text("welcome", stars_sold=total_stars, stars_bought=user_stars)
                text += "\n\n⚠️ Ваше меню устарело. Используйте новое меню ниже."
                keyboard = [
                    [
                        InlineKeyboardButton("📰 Новости", url=NEWS_CHANNEL),
                        InlineKeyboardButton("🛠 Поддержка и отзывы", url=SUPPORT_CHANNEL)
                    ],
                    [InlineKeyboardButton("👤 Профиль", callback_data=PROFILE), InlineKeyboardButton("🤝 Рефералы", callback_data=REFERRALS)],
                    [InlineKeyboardButton("💸 Купить звезды", callback_data=BUY_STARS)]
                ]
                if user_id == 6956377285:
                    keyboard.append([InlineKeyboardButton("🔧 Админ-панель", callback_data=ADMIN_PANEL)])
                reply_markup = InlineKeyboardMarkup(keyboard)
                try:
                    await query.edit_message_text(text, reply_markup=reply_markup)
                    app.bot_data[f"last_message_{user_id}"] = {
                        "chat_id": query.message.chat_id,
                        "message_id": query.message.message_id
                    }
                except BadRequest as e:
                    if "Message is not modified" not in str(e):
                        logger.error(f"Ошибка редактирования сообщения для user_id={user_id}: {e}")
                        ERRORS.labels(type="telegram_api", endpoint="callback_query").inc()
                        sent_message = await query.message.reply_text(text, reply_markup=reply_markup)
                        app.bot_data[f"last_message_{user_id}"] = {
                            "chat_id": sent_message.chat_id,
                            "message_id": sent_message.message_id
                        }
                context.user_data["state"] = STATE_MAIN_MENU
                return STATES[STATE_MAIN_MENU]

async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений для состояний."""
    user_id = update.effective_user.id
    state = context.user_data.get("state", STATE_MAIN_MENU)
    REQUESTS.labels(endpoint="handle_text_input").inc()
    with RESPONSE_TIME.labels(endpoint="handle_text_input").time():
        text = update.message.text.strip()
        logger.info(f"Text input received: user_id={user_id}, state={state}, text={text}")
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
            return STATES[STATE_BUY_STARS_AMOUNT]
        elif state == STATE_BUY_STARS_AMOUNT:
            try:
                stars = int(text)
                if stars < 1:
                    await update.message.reply_text("Введите количество звезд (положительное число).")
                    return STATES[state]
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
                return STATES[STATE_BUY_STARS_PAYMENT_METHOD]
            except ValueError:
                await update.message.reply_text("Введите число для количества звезд.")
                return STATES[state]
        elif state == STATE_ADMIN_BROADCAST:
            context.user_data["broadcast_text"] = text
            await update.message.reply_text(
                f"Текст рассылки:\n{text}\n\nОтправить это сообщение всем пользователям?",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Отправить", callback_data=CONFIRM_BROADCAST)],
                    [InlineKeyboardButton("❌ Отменить", callback_data=CANCEL_BROADCAST)]
                ])
            )
            await log_analytics(user_id, "set_broadcast_text", {"text": text})
            return STATES[STATE_ADMIN_BROADCAST]
        elif state == STATE_ADMIN_EDIT_PROFILE:
            edit_field = context.user_data.get("edit_profile_field")
            if not edit_field:
                try:
                    target_user_id = int(text)
                    async with (await ensure_db_pool()) as conn:
                        user = await conn.fetchrow("SELECT username, stars_bought, ref_bonus_ton, referrals FROM users WHERE user_id = $1", target_user_id)
                        if not user:
                            await update.message.reply_text(
                                "Пользователь не найден.",
                                reply_markup=InlineKeyboardMarkup([
                                    [InlineKeyboardButton("📋 Все пользователи", callback_data=STATE_ALL_USERS)],
                                    [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]
                                ])
                            )
                            context.user_data["state"] = STATE_ADMIN_EDIT_PROFILE
                            return STATES[STATE_ADMIN_EDIT_PROFILE]
                        ref_count = len(json.loads(user["referrals"])) if user["referrals"] else 0
                        text = (
                            f"Профиль пользователя @{user['username'] or 'Unknown'} (ID: <code>{target_user_id}</code>):\n"
                            f"Куплено звезд: {user['stars_bought']}\n"
                            f"Реферальный бонус: {user['ref_bonus_ton']:.2f} TON\n"
                            f"Рефералов: {ref_count}"
                        )
                        context.user_data["edit_user_id"] = target_user_id
                        keyboard = [
                            [InlineKeyboardButton("✨ Изменить звезды", callback_data=EDIT_PROFILE_STARS)],
                            [InlineKeyboardButton("🤝 Изменить рефералов", callback_data=EDIT_PROFILE_REFERRALS)],
                            [InlineKeyboardButton("💰 Изменить бонус", callback_data=EDIT_PROFILE_REF_BONUS)],
                            [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]
                        ]
                        await update.message.reply_text(
                            text,
                            reply_markup=InlineKeyboardMarkup(keyboard),
                            parse_mode="HTML"
                        )
                        await log_analytics(user_id, "view_edit_profile", {"target_user_id": target_user_id})
                        context.user_data["state"] = STATE_ADMIN_EDIT_PROFILE
                        return STATES[STATE_ADMIN_EDIT_PROFILE]
                except ValueError:
                    await update.message.reply_text(
                        "Пожалуйста, введите корректный ID пользователя (число).",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("📋 Все пользователи", callback_data=STATE_ALL_USERS)],
                            [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]
                        ])
                    )
                    return STATES[STATE_ADMIN_EDIT_PROFILE]
            else:
                async with (await ensure_db_pool()) as conn:
                    target_user_id = context.user_data.get("edit_user_id")
                    if not target_user_id:
                        await update.message.reply_text(
                            "Ошибка: ID пользователя не задан. Выберите пользователя заново.",
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                        )
                        context.user_data["state"] = STATE_ADMIN_EDIT_PROFILE
                        return STATES[STATE_ADMIN_EDIT_PROFILE]
                    try:
                        if edit_field == "stars_bought":
                            stars = int(text)
                            if stars < 0:
                                raise ValueError("Количество звезд не может быть отрицательным")
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
                            ref_ids = [int(rid.strip()) for rid in text.split(",") if rid.strip().isdigit()]
                            await conn.execute(
                                "UPDATE users SET referrals = $1 WHERE user_id = $2",
                                json.dumps(ref_ids), target_user_id
                            )
                            await update.message.reply_text(
                                f"Рефералы для пользователя {target_user_id} обновлены: {len(ref_ids)} рефералов",
                                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                            )
                            await log_analytics(user_id, "edit_profile_referrals", {"target_user_id": target_user_id, "referrals": ref_ids})
                        elif edit_field == "ref_bonus_ton":
                            bonus = float(text)
                            if bonus < 0:
                                raise ValueError("Бонус не может быть отрицательным")
                            await conn.execute(
                                "UPDATE users SET ref_bonus_ton = $1 WHERE user_id = $2",
                                bonus, target_user_id
                            )
                            await update.message.reply_text(
                                f"Реферальный бонус для пользователя {target_user_id} обновлен: {bonus:.2f} TON",
                                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]])
                            )
                            await log_analytics(user_id, "edit_profile_ref_bonus", {"target_user_id": target_user_id, "bonus": bonus})
                        context.user_data["edit_profile_field"] = None
                        context.user_data["edit_user_id"] = None
                        context.user_data["state"] = STATE_ADMIN_PANEL
                        return await show_admin_panel(update, context)
                    except ValueError as e:
                        await update.message.reply_text(
                            f"Ошибка: {str(e)}. Пожалуйста, введите корректное значение.",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("📋 Все пользователи", callback_data=STATE_ALL_USERS)],
                                [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]
                            ])
                        )
                        await log_analytics(user_id, "edit_profile_error", {"error": str(e), "field": edit_field})
                        return STATES[STATE_ADMIN_EDIT_PROFILE]
        else:
            await update.message.reply_text(
                "Пожалуйста, используйте кнопки меню.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]])
            )
            await log_analytics(user_id, "invalid_text_input", {"state": state, "text": text})
            return STATES[state]

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ошибок."""
    logger.error(f"Update {update} caused error {context.error}", exc_info=True)
    ERRORS.labels(type="bot_error", endpoint="error_handler").inc()
    if update:
        user_id = update.effective_user.id if update.effective_user else None
        try:
            await log_analytics(user_id, "error", {"error": str(context.error)})
            if update.message:
                await update.message.reply_text(
                    "Произошла ошибка. Попробуйте снова или свяжитесь с поддержкой.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛠 Поддержка", url=SUPPORT_CHANNEL)]])
                )
            elif update.callback_query:
                await update.callback_query.answer(text="Произошла ошибка. Попробуйте снова.")
        except Exception as e:
            logger.error(f"Ошибка при отправке сообщения об ошибке: {e}", exc_info=True)

async def root_handler(request: web.Request):
    """Обработчик корневого маршрута для проверки работоспособности (UptimeRobot)."""
    if request.method == "HEAD":
        logger.debug("Получен запрос HEAD от UptimeRobot")
        return web.Response(status=200)
    return web.Response(status=200, text="Bot is running")

async def webhook_handler(request: web.Request):
    """Обработчик вебхука."""
    REQUESTS.labels(endpoint="webhook").inc()
    with RESPONSE_TIME.labels(endpoint="webhook").time():
        if telegram_app is None or not telegram_app._initialized:
            logger.error("Application не инициализирован")
            ERRORS.labels(type="app_not_initialized", endpoint="webhook").inc()
            return web.Response(status=503, text="Application not initialized")
        try:
            update = Update.de_json(await request.json(), telegram_app.bot)
            await telegram_app.process_update(update)
            return web.Response(status=200)
        except Exception as e:
            logger.error(f"Ошибка обработки вебхука: {e}", exc_info=True)
            ERRORS.labels(type="webhook_error", endpoint="webhook").inc()
            return web.Response(status=500)
        
# Глобальная переменная для Telegram-бота
telegram_app = None

async def on_startup(web_app: web.Application):
    """Инициализация приложения при старте."""
    global telegram_app
    logger.info("Запуск on_startup")
    try:
        await check_environment()
        logger.info("Проверка переменных окружения пройдена")
    except ValueError as e:
        logger.error(f"Ошибка проверки переменных окружения: {e}", exc_info=True)
        raise
    try:
        await init_db()
        logger.info("База данных инициализирована")
    except Exception as e:
        logger.error(f"Ошибка инициализации базы данных: {e}", exc_info=True)
        raise
    try:
        await test_db_connection()
        logger.info("Подключение к базе данных проверено")
    except Exception as e:
        logger.error(f"Ошибка проверки подключения к базе данных: {e}", exc_info=True)
        raise
    try:
        await update_ton_price()
        logger.info("Цена TON обновлена")
    except Exception as e:
        logger.warning(f"Не удалось обновить цену TON: {e}", exc_info=True)
        # Не прерываем инициализацию из-за некритичной ошибки
    try:
        webhook_url = f"{WEBHOOK_URL}/webhook"
        logger.info(f"Установка вебхука: {webhook_url}")
        await telegram_app.bot.set_webhook(webhook_url)
        webhook_info = await telegram_app.bot.get_webhook_info()
        logger.info(f"Вебхук установлен: {webhook_info.url}")
    except Exception as e:
        logger.error(f"Ошибка установки вебхука: {e}", exc_info=True)
        raise
    try:
        scheduler = AsyncIOScheduler(timezone=pytz.UTC)
        scheduler.add_job(update_ton_price, 'interval', minutes=5)
        scheduler.add_job(heartbeat_check, 'interval', minutes=10, args=[telegram_app])
        scheduler.add_job(keep_alive, 'interval', minutes=15, args=[telegram_app])
        scheduler.add_job(backup_db, 'interval', hours=24)
        scheduler.start()
        telegram_app.bot_data["scheduler"] = scheduler
        logger.info("Планировщик задач запущен")
    except Exception as e:
        logger.error(f"Ошибка настройки планировщика: {e}", exc_info=True)
        raise
    logger.info("on_startup успешно завершен")

async def on_shutdown(web_app: web.Application):
    """Очистка при завершении работы."""
    logger.info("Запуск on_shutdown")
    try:
        if telegram_app is not None and "scheduler" in telegram_app.bot_data:
            telegram_app.bot_data["scheduler"].shutdown()
            logger.info("Планировщик задач остановлен")
        await close_db_pool()
        logger.info("Пул базы данных закрыт")
        if telegram_app is not None:
            await telegram_app.bot.delete_webhook(drop_pending_updates=True)
            logger.info("Вебхук удален")
    except Exception as e:
        logger.error(f"Ошибка в on_shutdown: {e}", exc_info=True)

def main():
    """Главная функция для запуска бота."""
    global telegram_app
    logger.info("Запуск main")
    try:
        telegram_app = ApplicationBuilder().token(BOT_TOKEN).build()
        logger.info("ApplicationBuilder инициализирован")
    except Exception as e:
        logger.error(f"Ошибка создания приложения: {e}", exc_info=True)
        raise

    try:
        conv_handler = ConversationHandler(
            entry_points=[
                CommandHandler("start", start),
                CommandHandler("tonprice", ton_price_command),
                CallbackQueryHandler(callback_query_handler)
            ],
            states={
                STATES[STATE_MAIN_MENU]: [
                    CallbackQueryHandler(callback_query_handler),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
                ],
                STATES[STATE_PROFILE]: [CallbackQueryHandler(callback_query_handler)],
                STATES[STATE_REFERRALS]: [CallbackQueryHandler(callback_query_handler)],
                STATES[STATE_BUY_STARS_RECIPIENT]: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATES[STATE_BUY_STARS_AMOUNT]: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATES[STATE_BUY_STARS_PAYMENT_METHOD]: [CallbackQueryHandler(callback_query_handler)],
                STATES[STATE_BUY_STARS_CRYPTO_TYPE]: [CallbackQueryHandler(callback_query_handler)],
                STATES[STATE_BUY_STARS_CONFIRM]: [CallbackQueryHandler(callback_query_handler)],
                STATES[STATE_ADMIN_PANEL]: [CallbackQueryHandler(callback_query_handler)],
                STATES[STATE_ADMIN_STATS]: [CallbackQueryHandler(callback_query_handler)],
                STATES[STATE_ADMIN_BROADCAST]: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATES[STATE_ADMIN_EDIT_PROFILE]: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATES[STATE_TOP_REFERRALS]: [CallbackQueryHandler(callback_query_handler)],
                STATES[STATE_TOP_PURCHASES]: [CallbackQueryHandler(callback_query_handler)],
                STATES[STATE_EXPORT_DATA]: [CallbackQueryHandler(callback_query_handler)],
                STATES[STATE_ALL_USERS]: [CallbackQueryHandler(callback_query_handler)]
            },
            fallbacks=[CommandHandler("start", start)],
            per_chat=True,
            per_user=True
        )
        telegram_app.add_handler(conv_handler)
        telegram_app.add_error_handler(error_handler)
        logger.info("Обработчики добавлены")
    except Exception as e:
        logger.error(f"Ошибка настройки обработчиков: {e}", exc_info=True)
        raise

    try:
        web_app = web.Application()
        web_app.router.add_get("/", root_handler)
        web_app.router.add_post("/webhook", webhook_handler)
        web_app.on_startup.append(on_startup)
        web_app.on_shutdown.append(on_shutdown)
        logger.info("Веб-приложение настроено")
    except Exception as e:
        logger.error(f"Ошибка настройки веб-приложения: {e}", exc_info=True)
        raise

    try:
        logger.info(f"Запуск веб-сервера на порту {PORT}")
        web.run_app(web_app, host="0.0.0.0", port=PORT)
    except Exception as e:
        logger.error(f"Ошибка запуска веб-сервера: {e}", exc_info=True)
        raise
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"Критическая ошибка в main: {e}", exc_info=True)
        raise
