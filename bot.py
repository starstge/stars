import asyncpg
import os
import time
import json
import logging
import asyncio
import aiohttp
import pytz
from datetime import datetime
from urllib.parse import urlparse
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler,
    filters, ContextTypes, ConversationHandler, JobQueue
)
from telegram.error import BadRequest

# Логирование
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Загрузка .env
load_dotenv()

# Константы
BOT_TOKEN = os.getenv("BOT_TOKEN")
TON_API_KEY = os.getenv("TON_API_KEY")
POSTGRES_URL = os.getenv("POSTGRES_URL") or os.getenv("DATABASE_URL")
SPLIT_API_TOKEN = os.getenv("SPLIT_API_TOKEN")
OWNER_WALLET = os.getenv("OWNER_WALLET")
CRYPTOBOT_API_TOKEN = os.getenv("CRYPTOBOT_API_TOKEN")
TON_SPACE_API_TOKEN = os.getenv("TON_SPACE_API_TOKEN")
MARKUP_PERCENTAGE = float(os.getenv("MARKUP_PERCENTAGE", 10))
SPLIT_API_URL = "https://api.split.tg/buy/stars"
CRYPTOBOT_API_URL = "https://pay.crypt.bot/api"
TON_SPACE_API_URL = "https://api.ton.space/v1"  # Hypothetical endpoint
CHANNEL_ID = "-1002703640431"

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

# Константы состояний для ConversationHandler
STATE_MAIN_MENU = 0
STATE_BUY_STARS_RECIPIENT = 1
STATE_BUY_STARS_AMOUNT = 2
STATE_BUY_STARS_PAYMENT_METHOD = 3
STATE_BUY_STARS_CONFIRM = 4
STATE_ADMIN_PANEL = 5
STATE_ADMIN_STATS = 6
STATE_ADMIN_EDIT_TEXTS = 7
STATE_EDIT_TEXT = 8
STATE_ADMIN_EDIT_MARKUP = 9
STATE_EDIT_MARKUP_TYPE = 10
STATE_ADMIN_MANAGE_ADMINS = 11
STATE_ADMIN_EDIT_PROFIT = 12
STATE_PROFILE = 13
STATE_TOP_REFERRALS = 14
STATE_TOP_PURCHASES = 15
STATE_REFERRALS = 16
STATE_USER_SEARCH = 17
STATE_EDIT_USER = 18
STATE_LIST_USERS = 19
STATE_ADMIN_USER_STATS = 20

# Глобальный пул базы данных
db_pool = None
_db_pool_lock = asyncio.Lock()

async def check_environment():
    required_vars = ["BOT_TOKEN", "POSTGRES_URL"]  # Only critical variables
    optional_vars = ["TON_SPACE_API_TOKEN", "CRYPTOBOT_API_TOKEN"]  # Optional payment-related variables
    for var_name in required_vars:
        if not os.getenv(var_name):
            logger.error(f"Missing required environment variable: {var_name}")
            raise ValueError(f"Environment variable {var_name} is not set")
    for var_name in optional_vars:
        if not os.getenv(var_name):
            logger.warning(f"Optional environment variable {var_name} is not set. Related payment features may be disabled.")

async def get_db_pool():
    """Инициализирует и возвращает пул соединений с базой данных."""
    global db_pool
    async with _db_pool_lock:
        if db_pool is None or db_pool._closed:
            logger.info("Creating new database pool")
            try:
                parsed_url = urlparse(POSTGRES_URL)
                dbname = parsed_url.path.lstrip('/')
                user = parsed_url.username
                password = parsed_url.password
                host = parsed_url.hostname
                port = parsed_url.port or 5432
                db_pool = await asyncpg.create_pool(
                    min_size=1,
                    max_size=10,
                    host=host,
                    port=port,
                    database=dbname,
                    user=user,
                    password=password,
                    command_timeout=60
                )
                logger.info("Database pool initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize database pool: {e}", exc_info=True)
                raise
        return db_pool

async def ensure_db_pool():
    """Обеспечивает доступность пула базы данных с повторными попытками."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            return await get_db_pool()
        except Exception as e:
            logger.error(f"Attempt {attempt + 1}/{max_retries} to connect to DB failed: {e}", exc_info=True)
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
            else:
                raise

async def close_db_pool():
    """Закрывает пул соединений с базой данных."""
    global db_pool
    async with _db_pool_lock:
        if db_pool and not db_pool._closed:
            logger.info("Closing database pool")
            await db_pool.close()
            logger.info("Database pool closed successfully")
        db_pool = None

async def init_db():
    """Инициализирует базу данных."""
    logger.info("Initializing database")
    async with (await ensure_db_pool()) as conn:
        try:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    stars_bought INTEGER DEFAULT 0,
                    ref_bonus_ton FLOAT DEFAULT 0,
                    referrals TEXT DEFAULT '[]',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_new BOOLEAN DEFAULT TRUE
                )
            """)
            try:
                await conn.execute("ALTER TABLE users ADD COLUMN is_new BOOLEAN DEFAULT TRUE")
                logger.info("Added is_new column to users table")
            except asyncpg.exceptions.DuplicateColumnError:
                logger.info("Column is_new already exists in users table")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value JSONB
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS texts (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    stars INTEGER,
                    amount_ton FLOAT,
                    payment_method TEXT,
                    recipient TEXT,
                    status TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    invoice_id TEXT
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS admin_logs (
                    id SERIAL PRIMARY KEY,
                    admin_id BIGINT,
                    action TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            default_settings = [
                ("admin_ids", [6956377285]),
                ("stars_price_usd", 0.81),
                ("ton_exchange_rate", 2.93),
                ("markup_ton_space", 20),
                ("markup_cryptobot_crypto", 25),
                ("markup_cryptobot_card", 25),
                ("markup_ref_bonus", 5),
                ("min_stars_purchase", 10),
                ("ton_space_commission", 15),
                ("card_commission", 10),
                ("profit_percent", 10)
            ]
            for key, value in default_settings:
                await conn.execute(
                    "INSERT INTO settings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING",
                    key, json.dumps(value)
                )
            default_texts = {
                "welcome": "Добро пожаловать! Купите Telegram Stars за TON.\nЗвезд продано: {stars_sold}\nВы купили: {stars_bought} звезд",
                "buy_prompt": "Оплатите {amount_ton:.6f} TON\nСсылка: {address}\nMemo: {memo}\nДля: @{recipient}\nЗвезды: {stars}\nМетод: {method}",
                "profile": "👤 Профиль\nЗвезд куплено: {stars_bought}\nРеф. бонус: {ref_bonus_ton} TON\nРефералов: {ref_count}",
                "referrals": "🤝 Рефералы\nВаша ссылка: {ref_link}\nРефералов: {ref_count}\nБонус: {ref_bonus_ton} TON",
                "tech_support": "🛠 Поддержка: @stars_support",
                "reviews": "📝 Отзывы: @stars_reviews",
                "buy_success": "Оплата прошла! @{recipient} получил {stars} звезд.",
                "user_info": "Пользователь: @{username}\nID: {user_id}\nЗвезд куплено: {stars_bought}\nРеф. бонус: {ref_bonus_ton} TON\nРефералов: {ref_count}"
            }
            for key, value in default_texts.items():
                await conn.execute(
                    "INSERT INTO texts (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2",
                    key, value
                )
            logger.info("Database initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}", exc_info=True)
            raise

async def get_setting(key):
    """Получает значение настройки."""
    async with (await ensure_db_pool()) as conn:
        try:
            result = await conn.fetchrow("SELECT value FROM settings WHERE key = $1", key)
            return json.loads(result["value"]) if result else None
        except Exception as e:
            logger.error(f"Error getting setting {key}: {e}", exc_info=True)
            return None

async def update_setting(key, value):
    """Обновляет настройку."""
    async with (await ensure_db_pool()) as conn:
        try:
            await conn.execute(
                "INSERT INTO settings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2",
                key, json.dumps(value)
            )
            logger.info(f"Setting {key} updated to {value}")
        except Exception as e:
            logger.error(f"Error updating setting {key}: {e}", exc_info=True)
            raise

async def get_text(key, **kwargs):
    """Получает текст по ключу."""
    async with (await ensure_db_pool()) as conn:
        try:
            result = await conn.fetchrow("SELECT value FROM texts WHERE key = $1", key)
            text = result["value"] if result else ""
            return text.format(**kwargs)
        except Exception as e:
            logger.error(f"Error getting text {key}: {e}", exc_info=True)
            return ""

async def update_text(key, value):
    """Обновляет текст."""
    async with (await ensure_db_pool()) as conn:
        try:
            await conn.execute(
                "INSERT INTO texts (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2",
                key, value
            )
            logger.info(f"Text {key} updated to {value}")
        except Exception as e:
            logger.error(f"Error updating text {key}: {e}", exc_info=True)
            raise

async def log_admin_action(admin_id, action):
    """Логирует действие админа."""
    async with (await ensure_db_pool()) as conn:
        try:
            await conn.execute(
                "INSERT INTO admin_logs (admin_id, action) VALUES ($1, $2)",
                admin_id, action
            )
            logger.info(f"Admin action logged: admin_id={admin_id}, action={action}")
        except Exception as e:
            logger.error(f"Error logging admin action: {e}", exc_info=True)

async def update_ton_price(context: ContextTypes.DEFAULT_TYPE):
    """Обновляет курс TON с использованием CoinGecko API."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.coingecko.com/api/v3/simple/price?ids=the-open-network&vs_currencies=usd") as response:
                if response.status == 200:
                    data = await response.json()
                    ton_price = float(data.get("the-open-network", {}).get("usd", 2.93))
                    await update_setting("ton_exchange_rate", ton_price)
                    context.bot_data["ton_price"] = ton_price
                    logger.info(f"TON price updated from CoinGecko: {ton_price} USD")
                elif response.status == 429:
                    logger.warning("Rate limit exceeded for CoinGecko API. Using cached TON price.")
                    ton_price = context.bot_data.get("ton_price", await get_setting("ton_exchange_rate") or 2.93)
                    logger.info(f"Using cached TON price: {ton_price} USD")
                else:
                    logger.error(f"Failed to fetch TON price from CoinGecko: {response.status}")
                    ton_price = context.bot_data.get("ton_price", await get_setting("ton_exchange_rate") or 2.93)
                    logger.info(f"Using cached TON price: {ton_price} USD")
    except Exception as e:
        logger.error(f"Error updating TON price from CoinGecko: {e}", exc_info=True)
        ton_price = context.bot_data.get("ton_price", await get_setting("ton_exchange_rate") or 2.93)
        logger.info(f"Using cached TON price: {ton_price} USD")

async def create_cryptobot_invoice(amount_usd, currency, user_id, stars, recipient):
    """Создает инвойс через CryptoBot с повторными попытками."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            api_token = CRYPTOBOT_API_TOKEN
            if not api_token:
                logger.error("CRYPTOBOT_API_TOKEN is not set")
                return None, None

            headers = {"Crypto-Pay-API-Token": api_token}
            payload = {
                "asset": currency,
                "amount": str(amount_usd),
                "description": f"Purchase of {stars} stars for @{recipient.lstrip('@')}",
                "paid_btn_name": "openChannel",
                "paid_btn_url": f"https://t.me/{os.getenv('BOT_USERNAME')}",
                "payload": json.dumps({"user_id": user_id, "stars": stars, "recipient": recipient})
            }
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.post(f"{CRYPTOBOT_API_URL}/createInvoice", headers=headers, json=payload) as resp:
                    data = await resp.json()
                    if resp.status == 200 and data.get("ok"):
                        invoice = data["result"]
                        logger.info(f"CryptoBot invoice created: invoice_id={invoice['invoice_id']}, user_id={user_id}")
                        return invoice["invoice_id"], invoice["pay_url"]
                    else:
                        logger.error(f"CryptoBot API error: status={resp.status}, response={data}")
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2)
                            continue
                        return None, None
        except Exception as e:
            logger.error(f"Error creating CryptoBot invoice for user_id={user_id}, attempt {attempt + 1}/{max_retries}: {e}", exc_info=True)
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
                continue
            return None, None
    logger.error(f"Failed to create CryptoBot invoice after {max_retries} attempts for user_id={user_id}")
    return None, None

async def check_cryptobot_payment(invoice_id):
    """Проверяет статус оплаты через CryptoBot с повторными попытками."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            if not CRYPTOBOT_API_TOKEN:
                logger.error("CRYPTOBOT_API_TOKEN is not set")
                return False
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                headers = {"Crypto-Pay-API-Token": CRYPTOBOT_API_TOKEN}
                async with session.get(f"{CRYPTOBOT_API_URL}/getInvoices?invoice_ids={invoice_id}", headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("ok"):
                            invoice = data["result"]["items"][0]
                            status = invoice["status"] == "paid"
                            logger.info(f"CryptoBot payment checked: invoice_id={invoice_id}, status={status}")
                            return status
                        else:
                            logger.error(f"CryptoBot payment check failed: {data}")
                            if attempt < max_retries - 1:
                                await asyncio.sleep(2)
                                continue
                            return False
                    else:
                        logger.error(f"CryptoBot API error: status={resp.status}")
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2)
                            continue
                        return False
        except Exception as e:
            logger.error(f"Error checking CryptoBot payment for invoice_id={invoice_id}, attempt {attempt + 1}/{max_retries}: {e}", exc_info=True)
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
                continue
            return False
    logger.error(f"Failed to check CryptoBot payment after {max_retries} attempts for invoice_id={invoice_id}")
    return False

async def create_ton_space_invoice(amount_ton, user_id, stars, recipient):
    """Создает инвойс через TON Space API с повторными попытками."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            api_token = TON_SPACE_API_TOKEN
            if not api_token:
                logger.error("TON_SPACE_API_TOKEN is not set")
                return None, None

            headers = {"Authorization": f"Bearer {api_token}"}
            payload = {
                "amount": str(amount_ton),
                "currency": "TON",
                "description": f"Purchase of {stars} stars for @{recipient.lstrip('@')}",
                "callback_url": f"https://t.me/{os.getenv('BOT_USERNAME')}",
                "metadata": json.dumps({"user_id": user_id, "stars": stars, "recipient": recipient})
            }
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.post(f"{TON_SPACE_API_URL}/invoices", headers=headers, json=payload) as resp:
                    data = await resp.json()
                    if resp.status == 200 and data.get("status") == "success":
                        invoice = data["invoice"]
                        logger.info(f"TON Space invoice created: invoice_id={invoice['id']}, user_id={user_id}")
                        return invoice["id"], invoice["pay_url"]
                    else:
                        logger.error(f"TON Space API error: status={resp.status}, response={data}")
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2)
                            continue
                        return None, None
        except Exception as e:
            logger.error(f"Error creating TON Space invoice for user_id={user_id}, attempt {attempt + 1}/{max_retries}: {e}", exc_info=True)
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
                continue
            return None, None
    logger.error(f"Failed to create TON Space invoice after {max_retries} attempts for user_id={user_id}")
    return None, None

async def check_ton_space_payment(invoice_id):
    """Проверяет статус оплаты через TON Space с повторными попытками."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            if not TON_SPACE_API_TOKEN:
                logger.error("TON_SPACE_API_TOKEN is not set")
                return False
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                headers = {"Authorization": f"Bearer {TON_SPACE_API_TOKEN}"}
                async with session.get(f"{TON_SPACE_API_URL}/invoices/{invoice_id}", headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("status") == "success":
                            invoice = data["invoice"]
                            status = invoice["status"] == "paid"
                            logger.info(f"TON Space payment checked: invoice_id={invoice_id}, status={status}")
                            return status
                        else:
                            logger.error(f"TON Space payment check failed: {data}")
                            if attempt < max_retries - 1:
                                await asyncio.sleep(2)
                                continue
                            return False
                    else:
                        logger.error(f"TON Space API error: status={resp.status}")
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2)
                            continue
                        return False
        except Exception as e:
            logger.error(f"Error checking TON Space payment for invoice_id={invoice_id}, attempt {attempt + 1}/{max_retries}: {e}", exc_info=True)
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
                continue
            return False
    logger.error(f"Failed to check TON Space payment after {max_retries} attempts for invoice_id={invoice_id}")
    return False

async def issue_stars(recipient_username, stars, user_id):
    """Выдает звезды через Split API с повторными попытками."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            if not SPLIT_API_TOKEN:
                logger.error("SPLIT_API_TOKEN is not set")
                return False
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                headers = {"Authorization": f"Bearer {SPLIT_API_TOKEN}"}
                payload = {
                    "username": recipient_username.lstrip("@"),
                    "stars": stars
                }
                async with session.post(SPLIT_API_URL, headers=headers, json=payload) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("success"):
                            logger.info(f"Stars issued: {stars} to @{recipient_username} by user_id={user_id}")
                            return True
                        else:
                            logger.error(f"Split API failed: {data}")
                            if attempt < max_retries - 1:
                                await asyncio.sleep(2)
                                continue
                            return False
                    else:
                        logger.error(f"Split API error: status={resp.status}")
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2)
                            continue
                        return False
        except Exception as e:
            logger.error(f"Error issuing stars for user_id={user_id}, attempt {attempt + 1}/{max_retries}: {e}", exc_info=True)
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
                continue
            return False
    logger.error(f"Failed to issue stars after {max_retries} attempts for user_id={user_id}")
    return False

async def check_ton_payment(address, memo, amount_ton):
    """Проверяет оплату TON."""
    try:
        async with aiohttp.ClientSession() as session:
            headers = {"Authorization": f"Bearer {TON_API_KEY}"}
            async with session.get(f"https://api.tonscan.org/v1/transactions?address={address}", headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for tx in data.get("transactions", []):
                        if tx.get("memo") == memo and abs(float(tx.get("amount_ton", 0)) - amount_ton) < 0.0001:
                            logger.info(f"TON payment confirmed: memo={memo}, amount={amount_ton}")
                            return True
                    logger.info(f"TON payment not found: memo={memo}, amount={amount_ton}")
                    return False
                else:
                    logger.error(f"TON API error: {resp.status}")
                    return False
    except Exception as e:
        logger.error(f"Error checking TON payment: {e}", exc_info=True)
        return False

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start."""
    user_id = update.effective_user.id
    username = update.effective_user.username or f"user_{user_id}"
    ref_id = context.args[0].split("_")[1] if context.args and context.args[0].startswith("ref_") else None
    logger.info(f"Start command received: user_id={user_id}, username={username}, ref_id={ref_id}")

    try:
        async with (await ensure_db_pool()) as conn:
            user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
            stars_sold = await conn.fetchval("SELECT SUM(stars) FROM transactions WHERE status = 'completed'") or 0
            stars_bought = user["stars_bought"] if user else 0
            if not user:
                await conn.execute(
                    "INSERT INTO users (user_id, username, created_at, is_new) VALUES ($1, $2, $3, $4)",
                    user_id, username, datetime.now(pytz.UTC), True
                )
                logger.info(f"New user created: user_id={user_id}, username={username}")
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
                            logger.info(f"Referral added: user_id={user_id} to referrer={ref_id}")
                            try:
                                await context.bot.send_message(
                                    chat_id=CHANNEL_ID,
                                    text=f"Новый пользователь @{username} через реферала ID {ref_id}"
                                )
                            except Exception as e:
                                logger.error(f"Failed to send channel message: {e}", exc_info=True)
            else:
                await conn.execute(
                    "UPDATE users SET username = $1 WHERE user_id = $2",
                    username, user_id
                )
                logger.info(f"User updated: user_id={user_id}, username={username}")

        text = await get_text("welcome", stars_sold=stars_sold, stars_bought=stars_bought)
        keyboard = [
            [InlineKeyboardButton("👤 Профиль", callback_data=PROFILE),
             InlineKeyboardButton("🤝 Рефералы", callback_data=REFERRALS)],
            [InlineKeyboardButton("🛠 Поддержка", callback_data=SUPPORT),
             InlineKeyboardButton("📝 Отзывы", callback_data=REVIEWS)],
            [InlineKeyboardButton("⭐ Купить звёзды", callback_data=BUY_STARS)]
        ]
        admin_ids = await get_setting("admin_ids") or [6956377285]
        if user_id in admin_ids:
            keyboard.append([InlineKeyboardButton("🔧 Админ-панель", callback_data=ADMIN_PANEL)])
        reply_markup = InlineKeyboardMarkup(keyboard)
        if update.callback_query:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        else:
            await update.message.reply_text(text, reply_markup=reply_markup)
        context.user_data.clear()
        context.user_data["state"] = STATE_MAIN_MENU
        logger.info(f"Start command completed for user_id={user_id}")
        return STATE_MAIN_MENU
    except Exception as e:
        logger.error(f"Error in start handler for user_id={user_id}: {e}", exc_info=True)
        if update.message:
            await update.message.reply_text("Произошла ошибка. Попробуйте снова.")
        context.user_data.clear()
        return STATE_MAIN_MENU

async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик профиля."""
    user_id = update.effective_user.id
    logger.info(f"Profile command for user_id={user_id}")
    try:
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
                logger.info(f"Profile displayed for user_id={user_id}")
                return STATE_PROFILE
            else:
                await update.callback_query.answer(text="Пользователь не найден.")
                return STATE_MAIN_MENU
    except Exception as e:
        logger.error(f"Error in profile handler for user_id={user_id}: {e}", exc_info=True)
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def referrals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик рефералов."""
    user_id = update.effective_user.id
    logger.info(f"Referrals command for user_id={user_id}")
    try:
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
                logger.info(f"Referrals displayed for user_id={user_id}")
                return STATE_REFERRALS
            else:
                await update.callback_query.answer(text="Пользователь не найден.")
                return STATE_MAIN_MENU
    except Exception as e:
        logger.error(f"Error in referrals handler for user_id={user_id}: {e}", exc_info=True)
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик поддержки."""
    user_id = update.effective_user.id
    logger.info(f"Support command for user_id={user_id}")
    try:
        text = await get_text("tech_support")
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["state"] = STATE_MAIN_MENU
        await update.callback_query.answer()
        logger.info(f"Support displayed for user_id={user_id}")
        return STATE_MAIN_MENU
    except Exception as e:
        logger.error(f"Error in support handler for user_id={user_id}: {e}", exc_info=True)
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def reviews(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик отзывов."""
    user_id = update.effective_user.id
    logger.info(f"Reviews command for user_id={user_id}")
    try:
        text = await get_text("reviews")
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["state"] = STATE_MAIN_MENU
        await update.callback_query.answer()
        logger.info(f"Reviews displayed for user_id={user_id}")
        return STATE_MAIN_MENU
    except Exception as e:
        logger.error(f"Error in reviews handler for user_id={user_id}: {e}", exc_info=True)
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def buy_stars(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начинает процесс покупки звезд."""
    user_id = update.effective_user.id
    logger.info(f"Buy stars command for user_id={user_id}")
    try:
        context.user_data["buy_data"] = {}
        text = "Выберите параметры покупки:"
        buy_data = context.user_data["buy_data"]
        recipient_display = buy_data.get("recipient", "-").lstrip("@")  # Исправление: убираем @ для отображения
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
        logger.info(f"Buy stars menu displayed for user_id={user_id}")
        return STATE_BUY_STARS_RECIPIENT
    except Exception as e:
        logger.error(f"Error in buy_stars handler for user_id={user_id}: {e}", exc_info=True)
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def set_recipient(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запрашивает username получателя."""
    user_id = update.effective_user.id
    logger.info(f"Set recipient command for user_id={user_id}")
    try:
        text = "Введите username получателя звезд (например, @username):"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["input_state"] = "recipient"
        context.user_data["state"] = STATE_BUY_STARS_RECIPIENT
        await update.callback_query.answer()
        logger.info(f"Recipient prompt displayed for user_id={user_id}")
        return STATE_BUY_STARS_RECIPIENT
    except Exception as e:
        logger.error(f"Error in set_recipient handler for user_id={user_id}: {e}", exc_info=True)
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def set_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запрашивает количество звезд."""
    user_id = update.effective_user.id
    logger.info(f"Set amount command for user_id={user_id}")
    try:
        text = "Введите количество звезд:"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["input_state"] = "amount"
        context.user_data["state"] = STATE_BUY_STARS_AMOUNT
        await update.callback_query.answer()
        logger.info(f"Amount prompt displayed for user_id={user_id}")
        return STATE_BUY_STARS_AMOUNT
    except Exception as e:
        logger.error(f"Error in set_amount handler for user_id={user_id}: {e}", exc_info=True)
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def set_payment_method(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает выбор метода оплаты."""
    user_id = update.effective_user.id
    logger.info(f"Set payment method command for user_id={user_id}")
    try:
        text = "Выберите метод оплаты:"
        keyboard = [
            [InlineKeyboardButton("Криптой", callback_data=PAY_CRYPTO)],
            [InlineKeyboardButton("Картой", callback_data=PAY_CARD)],
            [InlineKeyboardButton("TON Space", callback_data=PAY_TON_SPACE)],
            [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["state"] = STATE_BUY_STARS_PAYMENT_METHOD
        await update.callback_query.answer()
        logger.info(f"Payment method menu displayed for user_id={user_id}")
        return STATE_BUY_STARS_PAYMENT_METHOD
    except Exception as e:
        logger.error(f"Error in set_payment_method handler for user_id={user_id}: {e}", exc_info=True)
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def confirm_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    buy_data = context.user_data.get("buy_data", {})
    logger.info(f"Confirm payment requested by user_id={user_id}")
    try:
        recipient = buy_data.get("recipient")
        stars = buy_data.get("stars")
        amount_ton = buy_data.get("amount_ton")
        pay_url = buy_data.get("pay_url")
        if not all([recipient, stars, amount_ton, pay_url]):
            logger.error(f"Incomplete buy_data for user_id={user_id}: {buy_data}")
            error_text = "Неполные данные для оплаты. Начните заново."
            error_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BUY_STARS)]])
            if update.callback_query:
                await update.callback_query.message.reply_text(error_text, reply_markup=error_markup)
            else:
                await update.message.reply_text(error_text, reply_markup=error_markup)
            return STATE_BUY_STARS
        text = await get_text(
            "buy_prompt",  # Исправлено: используем buy_prompt вместо confirm_payment
            recipient=recipient.lstrip("@"),
            stars=stars,
            amount_ton=f"{amount_ton:.6f}",
            address=pay_url,
            memo="N/A",  # Memo не используется для CryptoBot/TON Space
            method=buy_data.get("payment_method", "unknown")
        )
        keyboard = [
            [InlineKeyboardButton("Оплатить", url=pay_url)],
            [InlineKeyboardButton("Проверить оплату", callback_data=CHECK_PAYMENT)],
            [InlineKeyboardButton("🔙 Назад", callback_data=BUY_STARS)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        # Store current message content to avoid redundant edits
        current_message = context.user_data.get("last_confirm_message", {})
        new_message = {"text": text, "reply_markup": reply_markup.to_dict()}
        if update.callback_query and current_message != new_message:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
            context.user_data["last_confirm_message"] = new_message
        elif not update.callback_query:
            await update.message.reply_text(text, reply_markup=reply_markup)
            context.user_data["last_confirm_message"] = new_message
        return STATE_BUY_STARS_CONFIRM
    except Exception as e:
        logger.error(f"Error in confirm_payment handler for user_id={user_id}: {e}", exc_info=True)
        error_text = "Произошла ошибка."
        error_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]])
        if update.callback_query:
            await update.callback_query.message.reply_text(error_text, reply_markup=error_markup)
        else:
            await update.message.reply_text(error_text, reply_markup=error_markup)
        return STATE_MAIN_MENU

async def check_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Проверяет статус оплаты."""
    user_id = update.effective_user.id
    logger.info(f"Checking payment command for user_id={user_id}")
    try:
        buy_data = context.user_data.get("buy_data", {})
        recipient = buy_data.get("recipient")
        stars = buy_data.get("stars")
        payment_method = buy_data.get("payment_method")
        invoice_id = buy_data.get("invoice_id")
        if not all([recipient, stars, payment_method, invoice_id]):
            logger.error(f"Incomplete buy_data for payment check by user_id={user_id}: {buy_data}")
            await update.callback_query.message.reply_text("Неполные данные для проверки оплаты. Начните заново.")
            return STATE_BUY_STARS
        success = False

        async with (await ensure_db_pool()) as conn:
            if payment_method == "ton_space_api":
                success = await check_ton_space_payment(invoice_id)
            elif payment_method in ["cryptobot_crypto", "cryptobot_card"]:
                success = await check_cryptobot_payment(invoice_id)

            if success:
                if await issue_stars(recipient, stars, user_id):
                    await conn.execute(
                        "UPDATE users SET stars_bought = stars_bought + $1 WHERE user_id = $2",
                        stars, user_id
                    )
                    await conn.execute(
                        "INSERT INTO transactions (user_id, stars, amount_ton, payment_method, recipient, status, invoice_id) "
                        "VALUES ($1, $2, $3, $4, $5, $6, $7)",
                        user_id, stars, buy_data["amount_ton"], payment_method, recipient, "completed", invoice_id
                    )
                    text = await get_text("buy_success", recipient=recipient.lstrip("@"), stars=stars)
                    await update.callback_query.message.reply_text(text)
                    context.user_data.clear()
                    context.user_data["state"] = STATE_MAIN_MENU
                    await start(update, context)
                    await update.callback_query.answer()
                    logger.info(f"Payment successful for user_id={user_id}, stars={stars}, recipient={recipient}")
                    return STATE_MAIN_MENU
                else:
                    await update.callback_query.message.reply_text("Ошибка выдачи звезд. Обратитесь в поддержку.")
                    logger.error(f"Failed to issue stars for user_id={user_id}")
            else:
                await update.callback_query.message.reply_text("Оплата не подтверждена. Попробуйте снова.")
                logger.info(f"Payment not confirmed for user_id={user_id}")
            await update.callback_query.answer()
            return STATE_BUY_STARS_CONFIRM
    except Exception as e:
        logger.error(f"Error in check_payment handler for user_id={user_id}: {e}", exc_info=True)
        await update.callback_query.message.reply_text("Произошла ошибка.")
        return STATE_BUY_STARS_CONFIRM

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик админ-панели."""
    user_id = update.effective_user.id
    logger.info(f"Admin panel command for user_id={user_id}")
    try:
        admin_ids = await get_setting("admin_ids") or [6956377285]
        if user_id not in admin_ids:
            await update.callback_query.edit_message_text("Доступ запрещен!")
            context.user_data["state"] = STATE_MAIN_MENU
            await update.callback_query.answer()
            logger.info(f"Access denied to admin panel for user_id={user_id}")
            return STATE_MAIN_MENU
        text = "🛠️ Админ-панель"
        keyboard = [
            [InlineKeyboardButton("📊 Статистика", callback_data=ADMIN_STATS)],
            [InlineKeyboardButton("📝 Изменить тексты", callback_data=ADMIN_EDIT_TEXTS)],
            [InlineKeyboardButton("👥 Статистика пользователей", callback_data=ADMIN_USER_STATS)],
            [InlineKeyboardButton("💸 Изменить наценку", callback_data=ADMIN_EDIT_MARKUP)],
            [InlineKeyboardButton("👤 Добавить/удалить админа", callback_data=ADMIN_MANAGE_ADMINS)],
            [InlineKeyboardButton("💰 Изменить параметры прибыли", callback_data=ADMIN_EDIT_PROFIT)],
            [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["state"] = STATE_ADMIN_PANEL
        await update.callback_query.answer()
        logger.info(f"Admin panel displayed for user_id={user_id}")
        return STATE_ADMIN_PANEL
    except Exception as e:
        logger.error(f"Error in admin_panel handler for user_id={user_id}: {e}", exc_info=True)
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отображает статистику."""
    user_id = update.effective_user.id
    logger.info(f"Admin stats command for user_id={user_id}")
    try:
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
            logger.info(f"Admin stats displayed for user_id={user_id}")
            return STATE_ADMIN_STATS
    except asyncpg.exceptions.InterfaceError as e:
        logger.error(f"Database pool error in admin_stats for user_id={user_id}: {e}", exc_info=True)
        try:
            global db_pool
            db_pool = None
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
                logger.info(f"Admin stats displayed after retry for user_id={user_id}")
                return STATE_ADMIN_STATS
        except Exception as retry_e:
            logger.error(f"Retry failed in admin_stats for user_id={user_id}: {retry_e}", exc_info=True)
            await update.callback_query.edit_message_text(
                "Ошибка базы данных.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]])
            )
            return STATE_MAIN_MENU
    except Exception as e:
        logger.error(f"Error in admin_stats handler for user_id={user_id}: {e}", exc_info=True)
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def admin_edit_texts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Редактирование текстов."""
    user_id = update.effective_user.id
    logger.info(f"Admin edit texts command for user_id={user_id}")
    try:
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
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["state"] = STATE_ADMIN_EDIT_TEXTS
        await update.callback_query.answer()
        logger.info(f"Edit texts menu displayed for user_id={user_id}")
        return STATE_ADMIN_EDIT_TEXTS
    except Exception as e:
        logger.error(f"Error in admin_edit_texts handler for user_id={user_id}: {e}", exc_info=True)
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def edit_text_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запрашивает новый текст."""
    user_id = update.effective_user.id
    logger.info(f"Edit text prompt command for user_id={user_id}")
    try:
        text_key = update.callback_query.data
        context.user_data["text_key"] = text_key
        text = f"Введите новый текст для '{text_key}':"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["state"] = STATE_EDIT_TEXT
        await update.callback_query.answer()
        logger.info(f"Edit text prompt displayed for user_id={user_id}, text_key={text_key}")
        return STATE_EDIT_TEXT
    except Exception as e:
        logger.error(f"Error in edit_text_prompt handler for user_id={user_id}: {e}", exc_info=True)
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def admin_user_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    logger.info(f"Admin user stats requested by user_id={user_id}")
    try:
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
        return STATE_USER_SEARCH
    except Exception as e:
        logger.error(f"Error in admin_user_stats handler for user_id={user_id}: {e}", exc_info=True)
        error_text = "Произошла ошибка."
        error_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]])
        if update.callback_query:
            await update.callback_query.message.reply_text(error_text, reply_markup=error_markup)
        else:
            await update.message.reply_text(error_text, reply_markup=error_markup)
        return STATE_MAIN_MENU

async def list_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отображает список пользователей."""
    user_id = update.effective_user.id
    logger.info(f"List users command for user_id={user_id}")
    try:
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
            logger.info(f"User list displayed for user_id={user_id}")
            return STATE_LIST_USERS
    except Exception as e:
        logger.error(f"Error in list_users handler for user_id={user_id}: {e}", exc_info=True)
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def admin_edit_markup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Изменение наценки."""
    user_id = update.effective_user.id
    logger.info(f"Admin edit markup command for user_id={user_id}")
    try:
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
        logger.info(f"Edit markup menu displayed for user_id={user_id}")
        return STATE_ADMIN_EDIT_MARKUP
    except Exception as e:
        logger.error(f"Error in admin_edit_markup handler for user_id={user_id}: {e}", exc_info=True)
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def admin_manage_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Управление админами."""
    user_id = update.effective_user.id
    logger.info(f"Admin manage admins command for user_id={user_id}")
    try:
        text = "Выберите действие:"
        keyboard = [
            [InlineKeyboardButton("Добавить админа", callback_data=ADD_ADMIN)],
            [InlineKeyboardButton("Удалить админа", callback_data=REMOVE_ADMIN)],
            [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["state"] = STATE_ADMIN_MANAGE_ADMINS
        await update.callback_query.answer()
        logger.info(f"Manage admins menu displayed for user_id={user_id}")
        return STATE_ADMIN_MANAGE_ADMINS
    except Exception as e:
        logger.error(f"Error in admin_manage_admins handler for user_id={user_id}: {e}", exc_info=True)
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def admin_edit_profit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Изменение прибыли."""
    user_id = update.effective_user.id
    logger.info(f"Admin edit profit command for user_id={user_id}")
    try:
        text = "Введите процент прибыли:"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["input_state"] = "profit_percent"
        context.user_data["state"] = STATE_ADMIN_EDIT_PROFIT
        await update.callback_query.answer()
        logger.info(f"Edit profit prompt displayed for user_id={user_id}")
        return STATE_ADMIN_EDIT_PROFIT
    except Exception as e:
        logger.error(f"Error in admin_edit_profit handler for user_id={user_id}: {e}", exc_info=True)
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def top_referrals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Топ рефералов."""
    user_id = update.effective_user.id
    logger.info(f"Tops: referrals command for user_id={user_id}")
    try:
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
            logger.info(f"Top referrals displayed for user_id={user_id}")
            return STATE_TOP_REFERRALS
    except Exception as e:
        logger.error(f"Error in top_referrals handler for user_id={user_id}: {e}", exc_info=True)
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def top_purchases(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Топы покупок."""
    user_id = update.effective_user.id
    logger.info(f"Top purchases command for user_id={user_id}")
    try:
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
            logger.info(f"Top purchases displayed for user_id={user_id}")
            return STATE_TOP_PURCHASES
    except Exception as e:
        logger.error(f"Error in top_purchases handler for user_id={user_id}: {e}", exc_info=True)
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает текстовый ввод."""
    user_id = update.effective_user.id
    if update.edited_message:
        logger.info(f"Игнорируем отредактированное сообщение от user_id={user_id}")
        return context.user_data.get("state", STATE_MAIN_MENU)  # Игнорируем отредактированные сообщения
    if not update.message:
        logger.error(f"Отсутствует сообщение в update для user_id={user_id}: {update}")
        return context.user_data.get("state", STATE_MAIN_MENU)
    text = update.message.text.strip()
    state = context.user_data.get("state", STATE_MAIN_MENU)
    input_state = context.user_data.get("input_state")
    logger.info(f"Получен текстовый ввод: user_id={user_id}, state={state}, input_state={input_state}, text={text}")

    try:
        async with (await ensure_db_pool()) as conn:
            if state == STATE_BUY_STARS_RECIPIENT and input_state == "recipient":
                if not text.startswith("@"):
                    await update.message.reply_text("Username должен начинаться с @!")
                    logger.info(f"Неверный формат получателя от user_id={user_id}: {text}")
                    return STATE_BUY_STARS_RECIPIENT
                buy_data = context.user_data.get("buy_data", {})
                buy_data["recipient"] = text
                context.user_data["buy_data"] = buy_data
                context.user_data.pop("input_state", None)
                text = "Выберите параметры покупки:"
                recipient_display = buy_data["recipient"].lstrip("@")  # Исправление: убираем @ для отображения
                keyboard = [
                    [InlineKeyboardButton(f"Кому звезды: @{recipient_display}", callback_data=SET_RECIPIENT)],
                    [InlineKeyboardButton(f"Количество звезд: {buy_data.get('stars', '-')}", callback_data=SET_AMOUNT)],
                    [InlineKeyboardButton(f"Метод оплаты: {buy_data.get('payment_method', '-')}", callback_data=SET_PAYMENT)],
                    [InlineKeyboardButton(f"Цена: {buy_data.get('amount_ton', '-')}", callback_data=CONFIRM_PAYMENT)],
                    [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await update.message.reply_text(text, reply_markup=reply_markup)
                logger.info(f"Установлен получатель для user_id={user_id}: {text}")
                return STATE_BUY_STARS_RECIPIENT

            elif state == STATE_BUY_STARS_AMOUNT and input_state == "amount":
                try:
                    stars = int(text)
                    min_stars = await get_setting("min_stars_purchase") or 10
                    if stars < min_stars:
                        await update.message.reply_text(f"Введите количество звезд не менее {min_stars}!")
                        logger.info(f"Недопустимое количество звезд от user_id={user_id}: {stars}")
                        return STATE_BUY_STARS_AMOUNT
                    buy_data = context.user_data.get("buy_data", {})
                    buy_data["stars"] = stars
                    stars_price_usd = float(await get_setting("stars_price_usd") or 0.81)
                    ton_price = float(context.bot_data.get("ton_price", await get_setting("ton_exchange_rate") or 2.93))
                    markup_key = {
                        "ton_space_api": "markup_ton_space",
                        "cryptobot_crypto": "markup_cryptobot_crypto",
                        "cryptobot_card": "markup_cryptobot_card"
                    }.get(buy_data.get("payment_method"), "markup_ton_space")
                    markup = float(await get_setting(markup_key) or 20)
                    commission_key = {
                        "ton_space_api": "ton_space_commission",
                        "cryptobot_card": "card_commission"
                    }.get(buy_data.get("payment_method"), "ton_space_commission")
                    commission = float(await get_setting(commission_key) or 15)
                    # Исправление: последовательный расчет цены
                    base_price_usd = stars * stars_price_usd
                    markup_amount = base_price_usd * (markup / 100)
                    commission_amount = (base_price_usd + markup_amount) * (commission / 100)
                    amount_usd = base_price_usd + markup_amount + commission_amount
                    amount_ton = amount_usd / ton_price if ton_price > 0 else 0
                    buy_data["amount_usd"] = amount_usd
                    buy_data["amount_ton"] = amount_ton
                    context.user_data["buy_data"] = buy_data
                    context.user_data.pop("input_state", None)
                    text = "Выберите параметры покупки:"
                    recipient_display = buy_data.get("recipient", "-").lstrip("@")  # Исправление: убираем @ для отображения
                    keyboard = [
                        [InlineKeyboardButton(f"Кому звезды: @{recipient_display}", callback_data=SET_RECIPIENT)],
                        [InlineKeyboardButton(f"Количество звезд: {stars}", callback_data=SET_AMOUNT)],
                        [InlineKeyboardButton(f"Метод оплаты: {buy_data.get('payment_method', '-')}", callback_data=SET_PAYMENT)],
                        [InlineKeyboardButton(f"Цена: {amount_ton:.6f} TON", callback_data=CONFIRM_PAYMENT)],
                        [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    await update.message.reply_text(text, reply_markup=reply_markup)
                    logger.info(f"Установлено количество звезд для user_id={user_id}: {stars}, amount_ton={amount_ton}")
                    return STATE_BUY_STARS_RECIPIENT
                except ValueError:
                    await update.message.reply_text("Введите корректное количество звезд!")
                    logger.info(f"Неверный ввод звезд от user_id={user_id}: {text}")
                    return STATE_BUY_STARS_AMOUNT

            elif state == STATE_EDIT_TEXT:
                text_key = context.user_data.get("text_key")
                await update_text(text_key, text)
                await log_admin_action(user_id, f"Edited text: {text_key}")
                await update.message.reply_text(f"Текст '{text_key}' обновлен!")
                context.user_data.pop("input_state", None)
                context.user_data["state"] = STATE_ADMIN_EDIT_TEXTS
                logger.info(f"Текст обновлен для user_id={user_id}: {text_key}")
                return await admin_edit_texts(update, context)

            elif state == STATE_USER_SEARCH and input_state == "search_user":
                if text.isdigit():
                    result = await conn.fetchrow(
                        "SELECT user_id, username, stars_bought, ref_bonus_ton, referrals FROM users WHERE user_id = $1",
                        int(text)
                    )
                else:
                    result = await conn.fetchrow(
                        "SELECT user_id, username, stars_bought, ref_bonus_ton, referrals FROM users WHERE username = $1",
                        text.lstrip("@")
                    )
                if result:
                    selected_user_id, username, stars_bought, ref_bonus_ton, referrals = result
                    context.user_data["selected_user"] = {
                        "user_id": selected_user_id,
                        "username": username,
                        "stars_bought": stars_bought,
                        "ref_bonus_ton": ref_bonus_ton,
                        "ref_count": len(json.loads(referrals)) if referrals != '[]' else 0
                    }
                    context.user_data["state"] = STATE_EDIT_USER
                    text = await get_text(
                        "user_info",
                        username=username,
                        user_id=selected_user_id,
                        stars_bought=stars_bought,
                        ref_bonus_ton=ref_bonus_ton,
                        ref_count=context.user_data["selected_user"]["ref_count"]
                    )
                    keyboard = [
                        [InlineKeyboardButton("Изменить звезды", callback_data=EDIT_USER_STARS)],
                        [InlineKeyboardButton("Изменить реф. бонус", callback_data=EDIT_USER_REF_BONUS)],
                        [InlineKeyboardButton("Изменить покупки", callback_data=EDIT_USER_PURCHASES)],
                        [InlineKeyboardButton("🔙 Назад", callback_data=ADMIN_USER_STATS)]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    await update.message.reply_text(text, reply_markup=reply_markup)
                    logger.info(f"Пользователь найден для user_id={user_id}: {username}")
                    return STATE_EDIT_USER
                else:
                    await update.message.reply_text("Пользователь не найден!")
                    logger.info(f"Пользователь не найден для user_id={user_id}: {text}")
                    return STATE_USER_SEARCH

            elif state == STATE_ADMIN_EDIT_MARKUP and input_state == "edit_markup":
                markup_type = context.user_data.get("markup_type")
                if not markup_type:
                    await update.message.reply_text("Тип наценки не выбран!")
                    logger.error(f"Тип наценки не выбран для user_id={user_id}")
                    context.user_data["state"] = STATE_ADMIN_EDIT_MARKUP
                    return await admin_edit_markup(update, context)
                try:
                    markup = float(text)
                    if markup < 0:
                        raise ValueError
                    await update_setting(markup_type, markup)
                    await log_admin_action(user_id, f"Updated markup {markup_type} to {markup}%")
                    await update.message.reply_text(f"Наценка '{markup_type}' обновлена: {markup}%")
                    context.user_data.pop("markup_type", None)
                    context.user_data.pop("input_state", None)
                    context.user_data["state"] = STATE_ADMIN_EDIT_MARKUP
                    logger.info(f"Наценка обновлена для user_id={user_id}: {markup_type}={markup}")
                    return await admin_edit_markup(update, context)
                except ValueError:
                    await update.message.reply_text("Введите корректное число для наценки!")
                    logger.info(f"Неверный ввод наценки от user_id={user_id}: {text}")
                    return STATE_ADMIN_EDIT_MARKUP

            elif state == STATE_ADMIN_EDIT_PROFIT and input_state == "profit_percent":
                try:
                    profit = float(text)
                    if profit < 0:
                        raise ValueError
                    await update_setting("profit_percent", profit)
                    await log_admin_action(user_id, f"Updated profit to {profit}%")
                    await update.message.reply_text(f"Профит обновлен: {profit}%")
                    context.user_data.pop("input_state", None)
                    context.user_data["state"] = STATE_ADMIN_PANEL
                    logger.info(f"Профит обновлен для user_id={user_id}: {profit}%")
                    return await admin_panel(update, context)
                except ValueError:
                    await update.message.reply_text("Введите корректное число для прибыли!")
                    logger.info(f"Неверный ввод прибыли от user_id={user_id}: {text}")
                    return STATE_ADMIN_EDIT_PROFIT

            elif state == STATE_ADMIN_MANAGE_ADMINS and input_state == "add_admin":
                try:
                    new_admin_username = None
                    if text.isdigit():
                        new_admin_id = int(text)
                        try:
                            new_admin = await context.bot.get_chat(new_admin_id)
                            new_admin_username = new_admin.username or f"user_{new_admin_id}"
                        except telegram.error.BadRequest:
                            raise ValueError("Чат с указанным ID не найден.")
                    else:
                        new_admin_username = text.lstrip("@")
                        try:
                            new_admin = await context.bot.get_chat(f"@{new_admin_username}")
                            new_admin_id = new_admin.id
                        except telegram.error.BadRequest:
                            raise ValueError("Пользователь с указанным username не найден.")
                    admin_ids = await get_setting("admin_ids") or [6956377285]
                    if new_admin_id not in admin_ids:
                        admin_ids.append(new_admin_id)
                        await update_setting("admin_ids", admin_ids)
                        await log_admin_action(user_id, f"Added admin: {new_admin_id} (@{new_admin_username})")
                        await update.message.reply_text(f"Админ @{new_admin_username} добавлен!")
                        logger.info(f"Админ добавлен user_id={user_id}: @{new_admin_username}")
                    else:
                        await update.message.reply_text("Этот пользователь уже админ!")
                        logger.info(f"Админ уже существует для user_id={user_id}: {new_admin_id}")
                    context.user_data.pop("input_state", None)
                    context.user_data["state"] = STATE_ADMIN_MANAGE_ADMINS
                    return await admin_manage_admins(update, context)
                except ValueError as ve:
                    await update.message.reply_text(str(ve))
                    logger.info(f"Неверный ввод админа от user_id={user_id}: {text}")
                    return STATE_ADMIN_MANAGE_ADMINS
                except Exception as e:
                    await update.message.reply_text("Ошибка при добавлении админа. Попробуйте снова.")
                    logger.error(f"Ошибка добавления админа для user_id={user_id}: {e}", exc_info=True)
                    return STATE_ADMIN_MANAGE_ADMINS

            elif state == STATE_ADMIN_MANAGE_ADMINS and input_state == "remove_admin":
                try:
                    if text.isdigit():
                        admin_id = int(text)
                        admin_username = (await context.bot.get_chat(admin_id)).username or f"user_{admin_id}"
                    else:
                        admin_username = text.lstrip("@")
                        admin_id = (await context.bot.get_chat(f"@{admin_username}")).id
                    admin_ids = await get_setting("admin_ids") or [6956377285]
                    if admin_id in admin_ids:
                        admin_ids.remove(admin_id)
                        await update_setting("admin_ids", admin_ids)
                        await log_admin_action(user_id, f"Removed admin: {admin_id} (@{admin_username})")
                        await update.message.reply_text(f"Админ @{admin_username} удален!")
                        logger.info(f"Админ удален user_id={user_id}: @{admin_username}")
                    else:
                        await update.message.reply_text("Админ не найден!")
                        logger.info(f"Админ не найден для удаления user_id={user_id}: {text}")
                    context.user_data.pop("input_state", None)
                    context.user_data["state"] = STATE_ADMIN_MANAGE_ADMINS
                    return await admin_manage_admins(update, context)
                except Exception as e:
                    await update.message.reply_text("Пользователь не найден или ошибка. Попробуйте снова.")
                    logger.error(f"Ошибка удаления админа для user_id={user_id}: {e}", exc_info=True)
                    return STATE_ADMIN_MANAGE_ADMINS

            elif state == STATE_EDIT_USER and input_state == "edit_stars":
                try:
                    stars = int(text)
                    selected_user = context.user_data.get("selected_user", {})
                    if not selected_user:
                        await update.message.reply_text("Пользователь не выбран!")
                        logger.error(f"Пользователь не выбран для user_id={user_id}")
                        return STATE_USER_SEARCH
                    await conn.execute(
                        "UPDATE users SET stars_bought = $1 WHERE user_id = $2",
                        stars, selected_user["user_id"]
                    )
                    await log_admin_action(user_id, f"Updated stars for user {selected_user['user_id']} to {stars}")
                    await update.message.reply_text(f"Звезды для @{selected_user['username']} обновлены: {stars}")
                    context.user_data.pop("input_state", None)
                    context.user_data["state"] = STATE_USER_SEARCH
                    logger.info(f"Звезды обновлены user_id={user_id} для user {selected_user['user_id']}: {stars}")
                    return await admin_user_stats(update, context)
                except ValueError:
                    await update.message.reply_text("Введите корректное число звезд!")
                    logger.info(f"Неверный ввод звезд для user_id={user_id}: {text}")
                    return STATE_EDIT_USER

            elif state == STATE_EDIT_USER and input_state == "edit_ref_bonus":
                try:
                    ref_bonus = float(text)
                    if ref_bonus < 0:
                        raise ValueError
                    selected_user = context.user_data.get("selected_user", {})
                    if not selected_user:
                        await update.message.reply_text("Пользователь не выбран!")
                        logger.error(f"Пользователь не выбран для user_id={user_id}")
                        return STATE_USER_SEARCH
                    await conn.execute(
                        "UPDATE users SET ref_bonus_ton = $1 WHERE user_id = $2",
                        ref_bonus, selected_user["user_id"]
                    )
                    await log_admin_action(user_id, f"Updated ref bonus for user {selected_user['user_id']} to {ref_bonus} TON")
                    await update.message.reply_text(f"Реф. бонус для @{selected_user['username']} обновлен: {ref_bonus} TON")
                    context.user_data.pop("input_state", None)
                    context.user_data["state"] = STATE_USER_SEARCH
                    logger.info(f"Реф. бонус обновлен user_id={user_id} для user {selected_user['user_id']}: {ref_bonus}")
                    return await admin_user_stats(update, context)
                except ValueError:
                    await update.message.reply_text("Введите корректное число для реф. бонуса!")
                    logger.info(f"Неверный ввод реф. бонуса для user_id={user_id}: {text}")
                    return STATE_EDIT_USER

            elif state == STATE_EDIT_USER and input_state == "edit_purchases":
                try:
                    stars = int(text)
                    selected_user = context.user_data.get("selected_user", {})
                    if not selected_user:
                        await update.message.reply_text("Пользователь не выбран!")
                        logger.error(f"Пользователь не выбран для user_id={user_id}")
                        return STATE_USER_SEARCH
                    await conn.execute(
                        "UPDATE users SET stars_bought = $1 WHERE user_id = $2",
                        stars, selected_user["user_id"]
                    )
                    await log_admin_action(user_id, f"Updated purchases for user {selected_user['user_id']} to {stars} stars")
                    await update.message.reply_text(f"Покупки для @{selected_user['username']} обновлены: {stars} звезд")
                    context.user_data.pop("input_state", None)
                    context.user_data["state"] = STATE_USER_SEARCH
                    logger.info(f"Покупки обновлены user_id={user_id} для user {selected_user['user_id']}: {stars}")
                    return await admin_user_stats(update, context)
                except ValueError:
                    await update.message.reply_text("Введите корректное число звезд!")
                    logger.info(f"Неверный ввод покупок для user_id={user_id}: {text}")
                    return STATE_EDIT_USER

            else:
                await update.message.reply_text("Неизвестная команда или состояние.")
                logger.info(f"Неизвестное состояние ввода для user_id={user_id}: state={state}, input_state={input_state}")
                return state
    except Exception as e:
        logger.error(f"Ошибка в handle_text_input для user_id={user_id}: {e}", exc_info=True)
        await update.message.reply_text("Произошла ошибка. Попробуйте снова.")
        return STATE_MAIN_MENU

async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает callback-запросы."""
    user_id = update.effective_user.id
    query = update.callback_query
    data = query.data
    logger.info(f"Callback query received: user_id={user_id}, data={data}")

    try:
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
            return await admin_panel(update, context)
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
        elif data == TOP_REFERRALS:
            return await top_referrals(update, context)
        elif data == TOP_PURCHASES:
            return await top_purchases(update, context)
        elif data == SET_RECIPIENT:
            return await set_recipient(update, context)
        elif data == SET_AMOUNT:
            return await set_amount(update, context)
        elif data == SET_PAYMENT:
            return await set_payment_method(update, context)
        elif data == CHECK_PAYMENT:
            return await check_payment(update, context)
        elif data in [EDIT_TEXT_WELCOME, EDIT_TEXT_BUY_PROMPT, EDIT_TEXT_PROFILE, EDIT_TEXT_REFERRALS,
                      EDIT_TEXT_TECH_SUPPORT, EDIT_TEXT_REVIEWS, EDIT_TEXT_BUY_SUCCESS]:
            return await edit_text_prompt(update, context)
        elif data in [MARKUP_TON_SPACE, MARKUP_CRYPTOBOT_CRYPTO, MARKUP_CRYPTOBOT_CARD, MARKUP_REF_BONUS]:
            context.user_data["markup_type"] = data
            text = f"Введите процент наценки для '{data}':"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text, reply_markup=reply_markup)
            context.user_data["input_state"] = "edit_markup"
            context.user_data["state"] = STATE_ADMIN_EDIT_MARKUP
            await query.answer()
            logger.info(f"Markup edit prompt for user_id={user_id}, markup_type={data}")
            return STATE_ADMIN_EDIT_MARKUP
        elif data == ADD_ADMIN:
            text = "Введите ID или username нового админа (например, @username или 123456789):"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text, reply_markup=reply_markup)
            context.user_data["input_state"] = "add_admin"
            context.user_data["state"] = STATE_ADMIN_MANAGE_ADMINS
            await query.answer()
            logger.info(f"Add admin prompt for user_id={user_id}")
            return STATE_ADMIN_MANAGE_ADMINS
        elif data == REMOVE_ADMIN:
            text = "Введите ID или username админа для удаления (например, @username или 123456789):"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text, reply_markup=reply_markup)
            context.user_data["input_state"] = "remove_admin"
            context.user_data["state"] = STATE_ADMIN_MANAGE_ADMINS
            await query.answer()
            logger.info(f"Remove admin prompt for user_id={user_id}")
            return STATE_ADMIN_MANAGE_ADMINS
        elif data == "search_user":
            text = "Введите ID или username пользователя (например, @username или 123456789):"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.message.reply_text(text, reply_markup=reply_markup)
            context.user_data["input_state"] = "search_user"
            context.user_data["state"] = STATE_USER_SEARCH
            await query.answer()
            logger.info(f"Search user prompt for user_id={user_id}")
            return STATE_USER_SEARCH
        elif data.startswith(SELECT_USER):
            selected_user_id = int(data[len(SELECT_USER):])
            async with (await ensure_db_pool()) as conn:
                result = await conn.fetchrow(
                    "SELECT user_id, username, stars_bought, ref_bonus_ton, referrals FROM users WHERE user_id = $1",
                    selected_user_id
                )
                if result:
                    selected_user_id, username, stars_bought, ref_bonus_ton, referrals = result
                    context.user_data["selected_user"] = {
                        "user_id": selected_user_id,
                        "username": username,
                        "stars_bought": stars_bought,
                        "ref_bonus_ton": ref_bonus_ton,
                        "ref_count": len(json.loads(referrals)) if referrals != '[]' else 0
                    }
                    context.user_data["state"] = STATE_EDIT_USER
                    text = await get_text(
                        "user_info",
                        username=username,
                        user_id=selected_user_id,
                        stars_bought=stars_bought,
                        ref_bonus_ton=ref_bonus_ton,
                        ref_count=context.user_data["selected_user"]["ref_count"]
                    )
                    keyboard = [
                        [InlineKeyboardButton("Изменить звезды", callback_data=EDIT_USER_STARS)],
                        [InlineKeyboardButton("Изменить реф. бонус", callback_data=EDIT_USER_REF_BONUS)],
                        [InlineKeyboardButton("Изменить покупки", callback_data=EDIT_USER_PURCHASES)],
                        [InlineKeyboardButton("🔙 Назад", callback_data=ADMIN_USER_STATS)]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    await query.edit_message_text(text, reply_markup=reply_markup)
                    await query.answer()
                    logger.info(f"Selected user for edit: user_id={user_id}, selected_user_id={selected_user_id}")
                    return STATE_EDIT_USER
                else:
                    await query.message.reply_text("Пользователь не найден!")
                    logger.info(f"Selected user not found for user_id={user_id}: {selected_user_id}")
                    return STATE_USER_SEARCH
        elif data == LIST_USERS:
            return await list_users(update, context)
        elif data == EDIT_USER_STARS:
            text = "Введите новое количество звезд:"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=ADMIN_USER_STATS)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.message.reply_text(text, reply_markup=reply_markup)
            context.user_data["input_state"] = "edit_stars"
            context.user_data["state"] = STATE_EDIT_USER
            await query.answer()
            logger.info(f"Edit stars prompt for user_id={user_id}")
            return STATE_EDIT_USER
        elif data == EDIT_USER_REF_BONUS:
            text = "Введите новый реферальный бонус (в TON):"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=ADMIN_USER_STATS)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.message.reply_text(text, reply_markup=reply_markup)
            context.user_data["input_state"] = "edit_ref_bonus"
            context.user_data["state"] = STATE_EDIT_USER
            await query.answer()
            logger.info(f"Edit ref bonus prompt for user_id={user_id}")
            return STATE_EDIT_USER
        elif data == EDIT_USER_PURCHASES:
            text = "Введите новое количество купленных звезд:"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=ADMIN_USER_STATS)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.message.reply_text(text, reply_markup=reply_markup)
            context.user_data["input_state"] = "edit_purchases"
            context.user_data["state"] = STATE_EDIT_USER
            await query.answer()
            logger.info(f"Edit purchases prompt for user_id={user_id}")
            return STATE_EDIT_USER
        elif data in [PAY_CRYPTO, PAY_CARD, PAY_TON_SPACE]:
            buy_data = context.user_data.get("buy_data", {})
            recipient = buy_data.get("recipient")
            stars = buy_data.get("stars")
            if not recipient or not stars:
                await query.message.reply_text("Сначала выберите получателя и количество звезд!")
                logger.error(f"Missing recipient or stars for user_id={user_id}: {buy_data}")
                return STATE_BUY_STARS_RECIPIENT
            async with (await ensure_db_pool()) as conn:
                stars_price_usd = float(await get_setting("stars_price_usd") or 0.81)
                ton_price = float(context.bot_data.get("ton_price", await get_setting("ton_exchange_rate") or 2.93))
                payment_method = {
                    PAY_CRYPTO: "cryptobot_crypto",
                    PAY_CARD: "cryptobot_card",
                    PAY_TON_SPACE: "ton_space_api"
                }[data]
                markup_key = {
                    "ton_space_api": "markup_ton_space",
                    "cryptobot_crypto": "markup_cryptobot_crypto",
                    "cryptobot_card": "markup_cryptobot_card"
                }.get(payment_method, "markup_ton_space")
                markup = float(await get_setting(markup_key) or 20)
                commission_key = {
                    "ton_space_api": "ton_space_commission",
                    "cryptobot_card": "card_commission"
                }.get(payment_method, "ton_space_commission")
                commission = float(await get_setting(commission_key) or 15)
                # Исправление: последовательный расчет цены
                base_price_usd = stars * stars_price_usd
                markup_amount = base_price_usd * (markup / 100)
                commission_amount = (base_price_usd + markup_amount) * (commission / 100)
                amount_usd = base_price_usd + markup_amount + commission_amount
                amount_ton = amount_usd / ton_price if ton_price > 0 else 0
                buy_data["amount_usd"] = amount_usd
                buy_data["amount_ton"] = amount_ton
                buy_data["payment_method"] = payment_method
                invoice_id, pay_url = None, None
                if payment_method == "ton_space_api":
                    invoice_id, pay_url = await create_ton_space_invoice(amount_ton, user_id, stars, recipient)
                elif payment_method in ["cryptobot_crypto", "cryptobot_card"]:
                    currency = "TON" if payment_method == "cryptobot_crypto" else "USD"
                    invoice_id, pay_url = await create_cryptobot_invoice(amount_usd, currency, user_id, stars, recipient)
                if invoice_id and pay_url:
                    buy_data["invoice_id"] = invoice_id
                    buy_data["pay_url"] = pay_url
                    context.user_data["buy_data"] = buy_data
                    await conn.execute(
                        "INSERT INTO transactions (user_id, stars, amount_ton, payment_method, recipient, status, invoice_id) "
                        "VALUES ($1, $2, $3, $4, $5, $6, $7)",
                        user_id, stars, amount_ton, payment_method, recipient, "pending", invoice_id
                    )
                    text = "Выберите параметры покупки:"
                    recipient_display = recipient.lstrip("@")  # Исправление: убираем @ для отображения
                    keyboard = [
                        [InlineKeyboardButton(f"Кому звезды: @{recipient_display}", callback_data=SET_RECIPIENT)],
                        [InlineKeyboardButton(f"Количество звезд: {stars}", callback_data=SET_AMOUNT)],
                        [InlineKeyboardButton(f"Метод оплаты: {payment_method}", callback_data=SET_PAYMENT)],
                        [InlineKeyboardButton(f"Цена: {amount_ton:.6f} TON", callback_data=CONFIRM_PAYMENT)],
                        [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    await query.edit_message_text(text, reply_markup=reply_markup)
                    context.user_data["state"] = STATE_BUY_STARS_RECIPIENT
                    await query.answer()
                    logger.info(f"Payment method set for user_id={user_id}: {payment_method}, amount_ton={amount_ton}")
                    return STATE_BUY_STARS_RECIPIENT
                else:
                    await query.message.reply_text("Ошибка создания инвойса. Попробуйте снова.")
                    logger.error(f"Failed to create invoice for user_id={user_id}, payment_method={payment_method}")
                    return STATE_BUY_STARS_RECIPIENT
        else:
            await query.message.reply_text("Неизвестная команда.")
            logger.info(f"Unknown callback data for user_id={user_id}: {data}")
            return context.user_data.get("state", STATE_MAIN_MENU)
    except Exception as e:
        logger.error(f"Error in callback_query_handler for user_id={user_id}: {e}", exc_info=True)
        await query.message.reply_text("Произошла ошибка.")
        return STATE_MAIN_MENU

async def check_channel_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Проверяет подписку на канал."""
    user_id = update.effective_user.id
    try:
        member = await context.bot.get_chat_member(CHANNEL_ID, user_id)
        if member.status in ["member", "administrator", "creator"]:
            return True
        else:
            keyboard = [[InlineKeyboardButton("Подписаться", url=f"https://t.me/stars_channel")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text("Пожалуйста, подпишитесь на канал!", reply_markup=reply_markup)
            return False
    except Exception as e:
        logger.error(f"Error checking channel subscription for user_id={user_id}: {e}", exc_info=True)
        return False

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ошибки."""
    logger.error(f"Update {update} caused error {context.error}", exc_info=True)
    if update and update.message:
        await update.message.reply_text("Произошла ошибка. Попробуйте снова.")


def main():
    """Запускает бота."""
    try:
        logger.info("Starting bot")
        app = ApplicationBuilder().token(BOT_TOKEN).build()

        conv_handler = ConversationHandler(
            entry_points=[
                CommandHandler("start", start, filters=filters.ChatType.PRIVATE),
                CallbackQueryHandler(callback_query_handler)
            ],
            states={
                STATE_MAIN_MENU: [
                    CallbackQueryHandler(start, pattern=f"^{BACK_TO_MENU}$"),
                    CallbackQueryHandler(profile, pattern=f"^{PROFILE}$"),
                    CallbackQueryHandler(referrals, pattern=f"^{REFERRALS}$"),
                    CallbackQueryHandler(support, pattern=f"^{SUPPORT}$"),
                    CallbackQueryHandler(reviews, pattern=f"^{REVIEWS}$"),
                    CallbackQueryHandler(buy_stars, pattern=f"^{BUY_STARS}$"),
                    CallbackQueryHandler(admin_panel, pattern=f"^{ADMIN_PANEL}$")
                ],
                STATE_BUY_STARS_RECIPIENT: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(set_recipient, pattern=f"^{SET_RECIPIENT}$"),
                    CallbackQueryHandler(set_amount, pattern=f"^{SET_AMOUNT}$"),
                    CallbackQueryHandler(set_payment_method, pattern=f"^{SET_PAYMENT}$"),
                    CallbackQueryHandler(confirm_payment, pattern=f"^{CONFIRM_PAYMENT}$"),
                    CallbackQueryHandler(start, pattern=f"^{BACK_TO_MENU}$"),
                    CallbackQueryHandler(check_payment, pattern=f"^{CHECK_PAYMENT}$"),
                    CallbackQueryHandler(callback_query_handler, pattern=f"^{PAY_CRYPTO}|{PAY_CARD}|{PAY_TON_SPACE}$")
                ],
                STATE_BUY_STARS_AMOUNT: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(start, pattern=f"^{BACK_TO_MENU}$")
                ],
                STATE_BUY_STARS_PAYMENT_METHOD: [
                    CallbackQueryHandler(callback_query_handler, pattern=f"^{PAY_CRYPTO}|{PAY_CARD}|{PAY_TON_SPACE}$"),
                    CallbackQueryHandler(start, pattern=f"^{BACK_TO_MENU}$")
                ],
                STATE_BUY_STARS_CONFIRM: [
                    CallbackQueryHandler(check_payment, pattern=f"^{CHECK_PAYMENT}$"),
                    CallbackQueryHandler(buy_stars, pattern=f"^{BUY_STARS}$"),
                    CallbackQueryHandler(start, pattern=f"^{BACK_TO_MENU}$")
                ],
                STATE_ADMIN_PANEL: [
                    CallbackQueryHandler(admin_stats, pattern=f"^{ADMIN_STATS}$"),
                    CallbackQueryHandler(admin_edit_texts, pattern=f"^{ADMIN_EDIT_TEXTS}$"),
                    CallbackQueryHandler(admin_user_stats, pattern=f"^{ADMIN_USER_STATS}$"),
                    CallbackQueryHandler(admin_edit_markup, pattern=f"^{ADMIN_EDIT_MARKUP}$"),
                    CallbackQueryHandler(admin_manage_admins, pattern=f"^{ADMIN_MANAGE_ADMINS}$"),
                    CallbackQueryHandler(admin_edit_profit, pattern=f"^{ADMIN_EDIT_PROFIT}$"),
                    CallbackQueryHandler(start, pattern=f"^{BACK_TO_MENU}$")
                ],
                STATE_ADMIN_STATS: [
                    CallbackQueryHandler(admin_panel, pattern=f"^{BACK_TO_ADMIN}$"),
                    CallbackQueryHandler(start, pattern=f"^{BACK_TO_MENU}$")
                ],
                STATE_ADMIN_EDIT_TEXTS: [
                    CallbackQueryHandler(edit_text_prompt, pattern=f"^{EDIT_TEXT_WELCOME}|{EDIT_TEXT_BUY_PROMPT}|{EDIT_TEXT_PROFILE}|{EDIT_TEXT_REFERRALS}|{EDIT_TEXT_TECH_SUPPORT}|{EDIT_TEXT_REVIEWS}|{EDIT_TEXT_BUY_SUCCESS}$"),
                    CallbackQueryHandler(admin_panel, pattern=f"^{BACK_TO_ADMIN}$"),
                    CallbackQueryHandler(start, pattern=f"^{BACK_TO_MENU}$")
                ],
                STATE_EDIT_TEXT: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(admin_panel, pattern=f"^{BACK_TO_ADMIN}$"),
                    CallbackQueryHandler(start, pattern=f"^{BACK_TO_MENU}$")
                ],
                STATE_ADMIN_EDIT_MARKUP: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(admin_edit_markup, pattern=f"^{MARKUP_TON_SPACE}|{MARKUP_CRYPTOBOT_CRYPTO}|{MARKUP_CRYPTOBOT_CARD}|{MARKUP_REF_BONUS}$"),
                    CallbackQueryHandler(admin_panel, pattern=f"^{BACK_TO_ADMIN}$"),
                    CallbackQueryHandler(start, pattern=f"^{BACK_TO_MENU}$")
                ],
                STATE_ADMIN_MANAGE_ADMINS: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(admin_manage_admins, pattern=f"^{ADD_ADMIN}|{REMOVE_ADMIN}$"),
                    CallbackQueryHandler(admin_panel, pattern=f"^{BACK_TO_ADMIN}$"),
                    CallbackQueryHandler(start, pattern=f"^{BACK_TO_MENU}$")
                ],
                STATE_ADMIN_EDIT_PROFIT: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(admin_panel, pattern=f"^{BACK_TO_ADMIN}$"),
                    CallbackQueryHandler(start, pattern=f"^{BACK_TO_MENU}$")
                ],
                STATE_PROFILE: [
                    CallbackQueryHandler(top_referrals, pattern=f"^{TOP_REFERRALS}$"),
                    CallbackQueryHandler(top_purchases, pattern=f"^{TOP_PURCHASES}$"),
                    CallbackQueryHandler(start, pattern=f"^{BACK_TO_MENU}$")
                ],
                STATE_TOP_REFERRALS: [
                    CallbackQueryHandler(profile, pattern=f"^{PROFILE}$"),
                    CallbackQueryHandler(start, pattern=f"^{BACK_TO_MENU}$")
                ],
                STATE_TOP_PURCHASES: [
                    CallbackQueryHandler(profile, pattern=f"^{PROFILE}$"),
                    CallbackQueryHandler(start, pattern=f"^{BACK_TO_MENU}$")
                ],
                STATE_REFERRALS: [
                    CallbackQueryHandler(start, pattern=f"^{BACK_TO_MENU}$")
                ],
                STATE_USER_SEARCH: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(admin_user_stats, pattern=f"^{ADMIN_USER_STATS}$"),
                    CallbackQueryHandler(list_users, pattern=f"^{LIST_USERS}$"),
                    CallbackQueryHandler(start, pattern=f"^{BACK_TO_MENU}$")
                ],
                STATE_LIST_USERS: [
                    CallbackQueryHandler(callback_query_handler, pattern=f"^{SELECT_USER}"),
                    CallbackQueryHandler(admin_user_stats, pattern=f"^{ADMIN_USER_STATS}$"),
                    CallbackQueryHandler(start, pattern=f"^{BACK_TO_MENU}$")
                ],
                STATE_EDIT_USER: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(admin_user_stats, pattern=f"^{ADMIN_USER_STATS}$"),
                    CallbackQueryHandler(start, pattern=f"^{BACK_TO_MENU}$")
                ]
            },
            fallbacks=[CommandHandler("start", start, filters=filters.ChatType.PRIVATE)],
            per_message=False  # Исправление: изменили на False
        )

        app.add_handler(conv_handler)
        app.add_error_handler(error_handler)

        async def update_ton_price_job(context: ContextTypes.DEFAULT_TYPE):
            await update_ton_price(context)

        app.job_queue.run_repeating(update_ton_price_job, interval=3600, first=10)

        # Создаем новый событийный цикл
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(app.run_polling(allowed_updates=Update.ALL_TYPES))
            logger.info("Bot started successfully")
        finally:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()
            logger.info("Event loop closed")
    except Exception as e:
        logger.error(f"Failed to start bot: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    import asyncio
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(init_db())
        main()
    except Exception as e:
        logger.error(f"Main execution failed: {e}", exc_info=True)
    finally:
        loop.run_until_complete(close_db_pool())
        loop.close()
        logger.info("Main event loop closed")
