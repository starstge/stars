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
    filters, ContextTypes, ConversationHandler, JobQueue, ApplicationContext
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

# Глобальный пул базы данных
db_pool = None
_db_pool_lock = asyncio.Lock()

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
    async with (await get_db_pool()) as conn:
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
    async with (await get_db_pool()) as conn:
        try:
            result = await conn.fetchrow("SELECT value FROM settings WHERE key = $1", key)
            return json.loads(result["value"]) if result else None
        except Exception as e:
            logger.error(f"Error getting setting {key}: {e}", exc_info=True)
            return None

async def update_setting(key, value):
    """Обновляет настройку."""
    async with (await get_db_pool()) as conn:
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
    async with (await get_db_pool()) as conn:
        try:
            result = await conn.fetchrow("SELECT value FROM texts WHERE key = $1", key)
            text = result["value"] if result else ""
            return text.format(**kwargs)
        except Exception as e:
            logger.error(f"Error getting text {key}: {e}", exc_info=True)
            return ""

async def update_text(key, value):
    """Обновляет текст."""
    async with (await get_db_pool()) as conn:
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
    async with (await get_db_pool()) as conn:
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
    """Создает инвойс через CryptoBot."""
    try:
        api_token = CRYPTOBOT_API_TOKEN
        if not api_token:
            logger.error("CRYPTOBOT_API_TOKEN is not set")
            return None, None

        headers = {"Crypto-Pay-API-Token": api_token}
        payload = {
            "asset": "USD",
            "amount": str(amount_usd),
            "description": f"Purchase of {stars} stars for @{recipient}",
            "paid_btn_name": "openChannel",
            "paid_btn_url": f"https://t.me/{os.getenv('BOT_USERNAME')}",
            "payload": json.dumps({"user_id": user_id, "stars": stars, "recipient": recipient})
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{CRYPTOBOT_API_URL}/createInvoice", headers=headers, json=payload) as resp:
                data = await resp.json()
                if resp.status == 200 and data.get("ok"):
                    invoice = data["result"]
                    logger.info(f"CryptoBot invoice created: invoice_id={invoice['invoice_id']}, user_id={user_id}")
                    return invoice["invoice_id"], invoice["pay_url"]
                else:
                    logger.error(f"CryptoBot API error: status={resp.status}, response={data}")
                    return None, None
    except Exception as e:
        logger.error(f"Error creating CryptoBot invoice for user_id={user_id}: {e}", exc_info=True)
        return None, None

async def check_cryptobot_payment(invoice_id):
    """Проверяет статус оплаты через CryptoBot."""
    try:
        async with aiohttp.ClientSession() as session:
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
                        return False
                else:
                    logger.error(f"CryptoBot API error: {resp.status}")
                    return False
    except Exception as e:
        logger.error(f"Error checking CryptoBot payment: {e}", exc_info=True)
        return False

async def create_ton_space_invoice(amount_ton, user_id, stars, recipient):
    """Создает инвойс через TON Space API."""
    try:
        api_token = TON_SPACE_API_TOKEN
        if not api_token:
            logger.error("TON_SPACE_API_TOKEN is not set")
            return None, None

        headers = {"Authorization": f"Bearer {api_token}"}
        payload = {
            "amount": str(amount_ton),
            "currency": "TON",
            "description": f"Purchase of {stars} stars for @{recipient}",
            "callback_url": f"https://t.me/{os.getenv('BOT_USERNAME')}",
            "metadata": json.dumps({"user_id": user_id, "stars": stars, "recipient": recipient})
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{TON_SPACE_API_URL}/invoices", headers=headers, json=payload) as resp:
                data = await resp.json()
                if resp.status == 200 and data.get("status") == "success":
                    invoice = data["invoice"]
                    logger.info(f"TON Space invoice created: invoice_id={invoice['id']}, user_id={user_id}")
                    return invoice["id"], invoice["pay_url"]
                else:
                    logger.error(f"TON Space API error: status={resp.status}, response={data}")
                    return None, None
    except Exception as e:
        logger.error(f"Error creating TON Space invoice for user_id={user_id}: {e}", exc_info=True)
        return None, None

async def check_ton_space_payment(invoice_id):
    """Проверяет статус оплаты через TON Space."""
    try:
        async with aiohttp.ClientSession() as session:
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
                        return False
                else:
                    logger.error(f"TON Space API error: {resp.status}")
                    return False
    except Exception as e:
        logger.error(f"Error checking TON Space payment: {e}", exc_info=True)
        return False

async def issue_stars(recipient_username, stars, user_id):
    """Выдает звезды через Split API."""
    try:
        async with aiohttp.ClientSession() as session:
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
                        return False
                else:
                    logger.error(f"Split API error: {resp.status}")
                    return False
    except Exception as e:
        logger.error(f"Error issuing stars: {e}", exc_info=True)
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
        async with (await get_db_pool()) as conn:
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
        async with (await get_db_pool()) as conn:
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
        async with (await get_db_pool()) as conn:
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
        keyboard = [
            [InlineKeyboardButton(f"Кому звезды: @{buy_data.get('recipient', '-')}", callback_data=SET_RECIPIENT)],
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
    """Подтверждает детали оплаты."""
    user_id = update.effective_user.id
    logger.info(f"Confirm payment command for user_id={user_id}")
    try:
        buy_data = context.user_data.get("buy_data", {})
        stars = buy_data.get("stars")
        recipient = buy_data.get("recipient")
        payment_method = buy_data.get("payment_method")
        if not all([recipient, stars, payment_method]):
            text = "Заполните все параметры:"
            keyboard = [
                [InlineKeyboardButton(f"Кому звезды: @{buy_data.get('recipient', '-')}", callback_data=SET_RECIPIENT)],
                [InlineKeyboardButton(f"Количество звезд: {buy_data.get('stars', '-')}", callback_data=SET_AMOUNT)],
                [InlineKeyboardButton(f"Метод оплаты: {buy_data.get('payment_method', '-')}", callback_data=SET_PAYMENT)],
                [InlineKeyboardButton(f"Цена: {buy_data.get('amount_ton', '-')}", callback_data=CONFIRM_PAYMENT)],
                [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
            context.user_data["state"] = STATE_BUY_STARS_RECIPIENT
            await update.callback_query.answer()
            logger.info(f"Incomplete buy data for user_id={user_id}")
            return STATE_BUY_STARS_RECIPIENT
        if payment_method == "ton_space_api":
            text = await get_text(
                "buy_prompt",
                amount_ton=buy_data["amount_ton"],
                stars=stars,
                recipient=recipient,
                address=buy_data["pay_url"],
                memo="N/A",
                method="TON Space"
            )
            keyboard = [
                [InlineKeyboardButton(f"Кому звезды: @{recipient}", callback_data=SET_RECIPIENT)],
                [InlineKeyboardButton(f"Количество звезд: {stars}", callback_data=SET_AMOUNT)],
                [InlineKeyboardButton("Метод оплаты: TON Space", callback_data=SET_PAYMENT)],
                [InlineKeyboardButton(f"Цена: ${buy_data['amount_usd']:.2f} ({buy_data['amount_ton']:.6f} TON)", callback_data=CONFIRM_PAYMENT)],
                [InlineKeyboardButton("Проверить оплату", callback_data=CHECK_PAYMENT)],
                [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
            ]
        elif payment_method == "cryptobot_crypto":
            text = await get_text(
                "buy_prompt",
                amount_ton=buy_data["amount_ton"],
                stars=stars,
                recipient=recipient,
                address=buy_data["pay_url"],
                memo="N/A",
                method="CryptoBot Crypto"
            )
            keyboard = [
                [InlineKeyboardButton(f"Кому звезды: @{recipient}", callback_data=SET_RECIPIENT)],
                [InlineKeyboardButton(f"Количество звезд: {stars}", callback_data=SET_AMOUNT)],
                [InlineKeyboardButton("Метод оплаты: Криптой", callback_data=SET_PAYMENT)],
                [InlineKeyboardButton(f"Цена: ${buy_data['amount_usd']:.2f} ({buy_data['amount_ton']:.6f} TON)", callback_data=CONFIRM_PAYMENT)],
                [InlineKeyboardButton("Проверить оплату", callback_data=CHECK_PAYMENT)],
                [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
            ]
        else:
            text = await get_text(
                "buy_prompt",
                amount_ton=buy_data["amount_ton"],
                stars=stars,
                recipient=recipient,
                address=buy_data["pay_url"],
                memo="N/A",
                method="Карта (CryptoBot)"
            )
            keyboard = [
                [InlineKeyboardButton(f"Кому звезды: @{recipient}", callback_data=SET_RECIPIENT)],
                [InlineKeyboardButton(f"Количество звезд: {stars}", callback_data=SET_AMOUNT)],
                [InlineKeyboardButton("Метод оплаты: Карта", callback_data=SET_PAYMENT)],
                [InlineKeyboardButton(f"Цена: ${buy_data['amount_usd']:.2f} ({buy_data['amount_ton']:.6f} TON)", callback_data=CONFIRM_PAYMENT)],
                [InlineKeyboardButton("Проверить оплату", callback_data=CHECK_PAYMENT)],
                [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
            ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["state"] = STATE_BUY_STARS_CONFIRM
        await update.callback_query.answer()
        logger.info(f"Payment confirmation displayed for user_id={user_id}")
        return STATE_BUY_STARS_CONFIRM
    except Exception as e:
        logger.error(f"Error in confirm_payment handler for user_id={user_id}: {e}", exc_info=True)
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
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
        success = False

        async with (await get_db_pool()) as conn:
            if payment_method == "ton_space_api":
                success = await check_ton_space_payment(buy_data["invoice_id"])
            elif payment_method == "cryptobot_crypto":
                success = await check_cryptobot_payment(buy_data["invoice_id"])
            elif payment_method == "cryptobot_card":
                success = await check_cryptobot_payment(buy_data["invoice_id"])

            if success:
                if await issue_stars(recipient, stars, user_id):
                    await conn.execute(
                        "UPDATE users SET stars_bought = stars_bought + $1 WHERE user_id = $2",
                        stars, user_id
                    )
                    await conn.execute(
                        "INSERT INTO transactions (user_id, stars, amount_ton, payment_method, recipient, status, invoice_id) "
                        "VALUES ($1, $2, $3, $4, $5, $6, $7)",
                        user_id, stars, buy_data["amount_ton"], payment_method, recipient, "completed", buy_data.get("invoice_id")
                    )
                    text = await get_text("buy_success", recipient=recipient, stars=stars)
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
        async with (await get_db_pool()) as conn:
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
            async with (await get_db_pool()) as conn:
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
    """Статистика пользователей."""
    user_id = update.effective_user.id
    logger.info(f"Admin user stats command for user_id={user_id}")
    try:
        text = "Выберите действие:"
        keyboard = [
            [InlineKeyboardButton("Список пользователей", callback_data=LIST_USERS)],
            [InlineKeyboardButton("Поиск по ID/username", callback_data="search_user")],
            [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["state"] = STATE_USER_SEARCH
        await update.callback_query.answer()
        logger.info(f"User stats menu displayed for user_id={user_id}")
        return STATE_USER_SEARCH
    except Exception as e:
        logger.error(f"Error in admin_user_stats handler for user_id={user_id}: {e}", exc_info=True)
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def list_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отображает список пользователей."""
    user_id = update.effective_user.id
    logger.info(f"List users command for user_id={user_id}")
    try:
        async with (await get_db_pool()) as conn:
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
        async with (await get_db_pool()) as conn:
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
        async with (await get_db_pool()) as conn:
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
    text = update.message.text.strip()
    state = context.user_data.get("state", STATE_MAIN_MENU)
    input_state = context.user_data.get("input_state")
    logger.info(f"Text input received: user_id={user_id}, state={state}, input_state={input_state}, text={text}")

    try:
        if state == STATE_BUY_STARS_RECIPIENT and input_state == "recipient":
            if not text.startswith("@"):
                await update.message.reply_text("Username должен начинаться с @!")
                logger.info(f"Invalid recipient format by user_id={user_id}: {text}")
                return STATE_BUY_STARS_RECIPIENT
            buy_data = context.user_data.get("buy_data", {})
            buy_data["recipient"] = text
            context.user_data["buy_data"] = buy_data
            context.user_data.pop("input_state", None)
            text = "Выберите параметры покупки:"
            keyboard = [
                [InlineKeyboardButton(f"Кому звезды: @{buy_data['recipient']}", callback_data=SET_RECIPIENT)],
                [InlineKeyboardButton(f"Количество звезд: {buy_data.get('stars', '-')}", callback_data=SET_AMOUNT)],
                [InlineKeyboardButton(f"Метод оплаты: {buy_data.get('payment_method', '-')}", callback_data=SET_PAYMENT)],
                [InlineKeyboardButton(f"Цена: {buy_data.get('amount_ton', '-')}", callback_data=CONFIRM_PAYMENT)],
                [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(text, reply_markup=reply_markup)
            logger.info(f"Recipient set for user_id={user_id}: {text}")
            return STATE_BUY_STARS_RECIPIENT

        elif state == STATE_BUY_STARS_AMOUNT and input_state == "amount":
            try:
                stars = int(text)
                min_stars = await get_setting("min_stars_purchase") or 10
                if stars < min_stars:
                    await update.message.reply_text(f"Введите количество звезд не менее {min_stars}!")
                    logger.info(f"Invalid stars amount by user_id={user_id}: {stars}")
                    return STATE_BUY_STARS_AMOUNT
                buy_data = context.user_data.get("buy_data", {})
                buy_data["stars"] = stars
                stars_price_usd = float(await get_setting("stars_price_usd") or 0.81)
                ton_price = float(await get_setting("ton_exchange_rate") or 2.93)
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
                amount_usd = stars * stars_price_usd * (1 + markup / 100) * (1 + commission / 100)
                amount_ton = amount_usd / ton_price
                buy_data["amount_usd"] = amount_usd
                buy_data["amount_ton"] = amount_ton
                context.user_data["buy_data"] = buy_data
                context.user_data.pop("input_state", None)
                text = "Выберите параметры покупки:"
                keyboard = [
                    [InlineKeyboardButton(f"Кому звезды: @{buy_data.get('recipient', '-')}", callback_data=SET_RECIPIENT)],
                    [InlineKeyboardButton(f"Количество звезд: {stars}", callback_data=SET_AMOUNT)],
                    [InlineKeyboardButton(f"Метод оплаты: {buy_data.get('payment_method', '-')}", callback_data=SET_PAYMENT)],
                    [InlineKeyboardButton(f"Цена: {amount_ton:.6f} TON", callback_data=CONFIRM_PAYMENT)],
                    [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await update.message.reply_text(text, reply_markup=reply_markup)
                logger.info(f"Stars amount set for user_id={user_id}: {stars}, amount_ton={amount_ton}")
                return STATE_BUY_STARS_RECIPIENT
            except ValueError:
                await update.message.reply_text("Введите корректное количество звезд!")
                logger.info(f"Invalid stars input by user_id={user_id}: {text}")
                return STATE_BUY_STARS_AMOUNT

        elif state == STATE_EDIT_TEXT:
            text_key = context.user_data.get("text_key")
            await update_text(text_key, text)
            await log_admin_action(user_id, f"Edited text: {text_key}")
            await update.message.reply_text(f"Текст '{text_key}' обновлен!")
            context.user_data["state"] = STATE_ADMIN_EDIT_TEXTS
            logger.info(f"Text updated by user_id={user_id}: {text_key}")
            return await admin_edit_texts(update, context)

        elif state == STATE_USER_SEARCH and input_state == "search_user":
            try:
                async with (await get_db_pool()) as conn:
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
                        logger.info(f"User found for user_id={user_id}: {username}")
                        return STATE_EDIT_USER
                    else:
                        await update.message.reply_text("Пользователь не найден!")
                        logger.info(f"User not found for user_id={user_id}: {text}")
                        return STATE_USER_SEARCH
            except asyncpg.exceptions.PostgresError as e:
                logger.error(f"Database error in user search for user_id={user_id}: {e}", exc_info=True)
                await update.message.reply_text("Ошибка базы данных. Попробуйте снова.")
                return STATE_USER_SEARCH

        elif state == STATE_ADMIN_EDIT_MARKUP and input_state == "edit_markup":
            markup_type = context.user_data.get("markup_type")
            if not markup_type:
                await update.message.reply_text("Тип наценки не выбран!")
                logger.error(f"No markup type selected for user_id={user_id}")
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
                logger.info(f"Markup updated by user_id={user_id}: {markup_type}={markup}")
                return await admin_edit_markup(update, context)
            except ValueError:
                await update.message.reply_text("Введите корректное число для наценки!")
                logger.info(f"Invalid markup input by user_id={user_id}: {text}")
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
                logger.info(f"Profit updated by user_id={user_id}: {profit}%")
                return await admin_panel(update, context)
            except ValueError:
                await update.message.reply_text("Введите корректное число для прибыли!")
                logger.info(f"Invalid profit input by user_id={user_id}: {text}")
                return STATE_ADMIN_EDIT_PROFIT

        elif state == STATE_ADMIN_MANAGE_ADMINS and input_state == "add_admin":
            try:
                if text.isdigit():
                    new_admin_id = int(text)
                    new_admin_username = (await context.bot.get_chat(new_admin_id)).username
                else:
                    new_admin_username = text.lstrip("@")
                    new_admin_id = (await context.bot.get_chat(f"@{new_admin_username}")).id
                admin_ids = await get_setting("admin_ids") or [6956377285]
                if new_admin_id not in admin_ids:
                    admin_ids.append(new_admin_id)
                    await update_setting("admin_ids", admin_ids)
                    await log_admin_action(user_id, f"Added admin: {new_admin_id} (@{new_admin_username})")
                    await update.message.reply_text(f"Админ @{new_admin_username} добавлен!")
                    logger.info(f"Admin added by user_id={user_id}: {new_admin_id}")
                else:
                    await update.message.reply_text("Этот пользователь уже админ!")
                    logger.info(f"Admin already exists for user_id={user_id}: {new_admin_id}")
                context.user_data.pop("input_state", None)
                context.user_data["state"] = STATE_ADMIN_MANAGE_ADMINS
                return await admin_manage_admins(update, context)
            except BadRequest as e:
                logger.error(f"Telegram API error adding admin for user_id={user_id}: {e}", exc_info=True)
                await update.message.reply_text("Ошибка! Проверьте ID или username.")
                return STATE_ADMIN_MANAGE_ADMINS
            except Exception as e:
                logger.error(f"Unexpected error adding admin for user_id={user_id}: {e}", exc_info=True)
                await update.message.reply_text("Произошла ошибка.")
                return STATE_ADMIN_MANAGE_ADMINS

        elif state == STATE_ADMIN_MANAGE_ADMINS and input_state == "remove_admin":
            try:
                if text.isdigit():
                    remove_admin_id = int(text)
                    remove_admin_username = (await context.bot.get_chat(remove_admin_id)).username
                else:
                    remove_admin_username = text.lstrip("@")
                    remove_admin_id = (await context.bot.get_chat(f"@{remove_admin_username}")).id
                admin_ids = await get_setting("admin_ids") or [6956377285]
                if remove_admin_id in admin_ids:
                    admin_ids.remove(remove_admin_id)
                    await update_setting("admin_ids", admin_ids)
                    await log_admin_action(user_id, f"Removed admin: {remove_admin_id} (@{remove_admin_username})")
                    await update.message.reply_text(f"Админ @{remove_admin_username} удалён!")
                    logger.info(f"Admin removed by user_id={user_id}: {remove_admin_id}")
                else:
                    await update.message.reply_text("Этот пользователь не является админом!")
                    logger.info(f"Admin not found for user_id={user_id}: {remove_admin_id}")
                context.user_data.pop("input_state", None)
                context.user_data["state"] = STATE_ADMIN_MANAGE_ADMINS
                return await admin_manage_admins(update, context)
            except BadRequest as e:
                logger.error(f"Telegram API error removing admin for user_id={user_id}: {e}", exc_info=True)
                await update.message.reply_text("Ошибка! Проверьте ID или username.")
                return STATE_ADMIN_MANAGE_ADMINS
            except Exception as e:
                logger.error(f"Unexpected error removing admin for user_id={user_id}: {e}", exc_info=True)
                await update.message.reply_text("Произошла ошибка.")
                return STATE_ADMIN_MANAGE_ADMINS

        elif state == STATE_EDIT_USER:
            field = context.user_data.get("edit_user_field")
            selected_user = context.user_data.get("selected_user")
            if not selected_user:
                await update.message.reply_text("Пользователь не выбран!")
                logger.error(f"No selected user for user_id={user_id}")
                context.user_data["state"] = STATE_ADMIN_USER_STATS
                return await admin_user_stats(update, context)
            try:
                async with (await get_db_pool()) as conn:
                    if field == EDIT_USER_STARS:
                        stars = int(text)
                        await conn.execute(
                            "UPDATE users SET stars_bought = $1 WHERE user_id = $2",
                            stars, selected_user["user_id"]
                        )
                        await log_admin_action(user_id, f"Updated stars for @{selected_user['username']} to {stars}")
                        await update.message.reply_text(f"Звезды для @{selected_user['username']} обновлены: {stars}")
                        logger.info(f"Stars updated by user_id={user_id} for {selected_user['user_id']}: {stars}")
                    elif field == EDIT_USER_REF_BONUS:
                        ref_bonus = float(text)
                        await conn.execute(
                            "UPDATE users SET ref_bonus_ton = $1 WHERE user_id = $2",
                            ref_bonus, selected_user["user_id"]
                        )
                        await log_admin_action(user_id, f"Updated ref_bonus for @{selected_user['username']} to {ref_bonus}")
                        await update.message.reply_text(f"Реф. бонус для @{selected_user['username']} обновлен: {ref_bonus} TON")
                        logger.info(f"Ref bonus updated by user_id={user_id} for {selected_user['user_id']}: {ref_bonus}")
                    elif field == EDIT_USER_PURCHASES:
                        purchases = int(text)
                        await conn.execute(
                            "UPDATE users SET stars_bought = $1 WHERE user_id = $2",
                            purchases, selected_user["user_id"]
                        )
                        await log_admin_action(user_id, f"Updated purchases for @{selected_user['username']} to {purchases}")
                        await update.message.reply_text(f"Покупки для @{selected_user['username']} обновлены: {purchases} звезд")
                        logger.info(f"Purchases updated by user_id={user_id} for {selected_user['user_id']}: {purchases}")
                    context.user_data.pop("edit_user_field", None)
                    context.user_data["state"] = STATE_EDIT_USER
                    text = await get_text(
                        "user_info",
                        username=selected_user["username"],
                        user_id=selected_user["user_id"],
                        stars_bought=stars if field == EDIT_USER_STARS else selected_user["stars_bought"],
                        ref_bonus_ton=ref_bonus if field == EDIT_USER_REF_BONUS else selected_user["ref_bonus_ton"],
                        ref_count=selected_user["ref_count"]
                    )
                    keyboard = [
                        [InlineKeyboardButton("Изменить звезды", callback_data=EDIT_USER_STARS)],
                        [InlineKeyboardButton("Изменить реф. бонус", callback_data=EDIT_USER_REF_BONUS)],
                        [InlineKeyboardButton("Изменить покупки", callback_data=EDIT_USER_PURCHASES)],
                        [InlineKeyboardButton("🔙 Назад", callback_data=ADMIN_USER_STATS)]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    await update.message.reply_text(text, reply_markup=reply_markup)
                    return STATE_EDIT_USER
            except ValueError:
                await update.message.reply_text("Введите корректное значение!")
                logger.info(f"Invalid input for user edit by user_id={user_id}: {text}")
                return STATE_EDIT_USER
            except asyncpg.exceptions.PostgresError as e:
                logger.error(f"Database error updating user for user_id={user_id}: {e}", exc_info=True)
                await update.message.reply_text("Ошибка базы данных. Попробуйте снова.")
                return STATE_EDIT_USER

        else:
            await update.message.reply_text("Неизвестная команда или состояние!")
            logger.info(f"Unknown state or command for user_id={user_id}: state={state}, input_state={input_state}")
            return STATE_MAIN_MENU

    except Exception as e:
        logger.error(f"Error in handle_text_input for user_id={user_id}: {e}", exc_info=True)
        await update.message.reply_text("Произошла ошибка. Попробуйте снова.")
        return state

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает нажатия на кнопки."""
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data
    logger.info(f"Button callback received: user_id={user_id}, data={data}")

    try:
        if data == BACK_TO_MENU:
            await start(update, context)
            return STATE_MAIN_MENU

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

        elif data == CONFIRM_PAYMENT:
            return await confirm_payment(update, context)

        elif data == CHECK_PAYMENT:
            return await check_payment(update, context)

        elif data in [EDIT_TEXT_WELCOME, EDIT_TEXT_BUY_PROMPT, EDIT_TEXT_PROFILE, EDIT_TEXT_REFERRALS,
                      EDIT_TEXT_TECH_SUPPORT, EDIT_TEXT_REVIEWS, EDIT_TEXT_BUY_SUCCESS]:
            return await edit_text_prompt(update, context)

        elif data == LIST_USERS:
            return await list_users(update, context)

        elif data == "search_user":
            context.user_data["input_state"] = "search_user"
            context.user_data["state"] = STATE_USER_SEARCH
            await query.edit_message_text("Введите ID или username пользователя:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=ADMIN_USER_STATS)]]))
            await query.answer()
            logger.info(f"Search user prompt displayed for user_id={user_id}")
            return STATE_USER_SEARCH

        elif data.startswith(SELECT_USER):
            selected_user_id = int(data[len(SELECT_USER):])
            async with (await get_db_pool()) as conn:
                result = await conn.fetchrow(
                    "SELECT user_id, username, stars_bought, ref_bonus_ton, referrals FROM users WHERE user_id = $1",
                    selected_user_id
                )
                if result:
                    username, stars_bought, ref_bonus_ton, referrals = result[1:]
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
                    logger.info(f"User selected for edit by user_id={user_id}: {selected_user_id}")
                    return STATE_EDIT_USER
                else:
                    await query.answer("Пользователь не найден!")
                    return STATE_LIST_USERS

        elif data in [MARKUP_TON_SPACE, MARKUP_CRYPTOBOT_CRYPTO, MARKUP_CRYPTOBOT_CARD, MARKUP_REF_BONUS]:
            context.user_data["markup_type"] = data
            context.user_data["input_state"] = "edit_markup"
            context.user_data["state"] = STATE_ADMIN_EDIT_MARKUP
            await query.edit_message_text(f"Введите новую наценку для {data} (%):", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=ADMIN_EDIT_MARKUP)]]))
            await query.answer()
            logger.info(f"Markup edit prompt displayed for user_id={user_id}: {data}")
            return STATE_ADMIN_EDIT_MARKUP

        elif data == ADD_ADMIN:
            context.user_data["input_state"] = "add_admin"
            context.user_data["state"] = STATE_ADMIN_MANAGE_ADMINS
            await query.edit_message_text("Введите ID или username нового админа:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=ADMIN_MANAGE_ADMINS)]]))
            await query.answer()
            logger.info(f"Add admin prompt displayed for user_id={user_id}")
            return STATE_ADMIN_MANAGE_ADMINS

        elif data == REMOVE_ADMIN:
            context.user_data["input_state"] = "remove_admin"
            context.user_data["state"] = STATE_ADMIN_MANAGE_ADMINS
            await query.edit_message_text("Введите ID или username админа для удаления:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=ADMIN_MANAGE_ADMINS)]]))
            await query.answer()
            logger.info(f"Remove admin prompt displayed for user_id={user_id}")
            return STATE_ADMIN_MANAGE_ADMINS

        elif data in [EDIT_USER_STARS, EDIT_USER_REF_BONUS, EDIT_USER_PURCHASES]:
            context.user_data["edit_user_field"] = data
            context.user_data["state"] = STATE_EDIT_USER
            field_name = {"edit_user_stars": "звезд", "edit_user_ref_bonus": "реф. бонус (TON)", "edit_user_purchases": "покупок (звезд)"}
            await query.edit_message_text(f"Введите новое значение для {field_name[data]}:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=ADMIN_USER_STATS)]]))
            await query.answer()
            logger.info(f"Edit user field prompt displayed for user_id={user_id}: {data}")
            return STATE_EDIT_USER

        elif data == PAY_CRYPTO:
            buy_data = context.user_data.get("buy_data", {})
            buy_data["payment_method"] = "cryptobot_crypto"
            if buy_data.get("stars") and buy_data.get("recipient"):
                stars = buy_data["stars"]
                recipient = buy_data["recipient"]
                stars_price_usd = float(await get_setting("stars_price_usd") or 0.81)
                markup = float(await get_setting("markup_cryptobot_crypto") or 25)
                ton_price = float(await get_setting("ton_exchange_rate") or 2.93)
                amount_usd = stars * stars_price_usd * (1 + markup / 100)
                amount_ton = amount_usd / ton_price
                buy_data["amount_usd"] = amount_usd
                buy_data["amount_ton"] = amount_ton
                invoice_id, pay_url = await create_cryptobot_invoice(amount_usd, "USD", user_id, stars, recipient)
                if invoice_id:
                    buy_data["invoice_id"] = invoice_id
                    buy_data["pay_url"] = pay_url
                    context.user_data["buy_data"] = buy_data
                    return await confirm_payment(update, context)
                else:
                    await query.answer("Ошибка создания инвойса!")
                    logger.error(f"Failed to create CryptoBot invoice for user_id={user_id}")
            context.user_data["buy_data"] = buy_data
            return await buy_stars(update, context)

        elif data == PAY_CARD:
            buy_data = context.user_data.get("buy_data", {})
            buy_data["payment_method"] = "cryptobot_card"
            if buy_data.get("stars") and buy_data.get("recipient"):
                stars = buy_data["stars"]
                recipient = buy_data["recipient"]
                stars_price_usd = float(await get_setting("stars_price_usd") or 0.81)
                markup = float(await get_setting("markup_cryptobot_card") or 25)
                commission = float(await get_setting("card_commission") or 10)
                ton_price = float(await get_setting("ton_exchange_rate") or 2.93)
                amount_usd = stars * stars_price_usd * (1 + markup / 100) * (1 + commission / 100)
                amount_ton = amount_usd / ton_price
                buy_data["amount_usd"] = amount_usd
                buy_data["amount_ton"] = amount_ton
                invoice_id, pay_url = await create_cryptobot_invoice(amount_usd, "USD", user_id, stars, recipient)
                if invoice_id:
                    buy_data["invoice_id"] = invoice_id
                    buy_data["pay_url"] = pay_url
                    context.user_data["buy_data"] = buy_data
                    return await confirm_payment(update, context)
                else:
                    await query.answer("Ошибка создания инвойса!")
                    logger.error(f"Failed to create CryptoBot card invoice for user_id={user_id}")
            context.user_data["buy_data"] = buy_data
            return await buy_stars(update, context)

        elif data == PAY_TON_SPACE:
            buy_data = context.user_data.get("buy_data", {})
            buy_data["payment_method"] = "ton_space_api"
            if buy_data.get("stars") and buy_data.get("recipient"):
                stars = buy_data["stars"]
                recipient = buy_data["recipient"]
                stars_price_usd = float(await get_setting("stars_price_usd") or 0.81)
                markup = float(await get_setting("markup_ton_space") or 20)
                commission = float(await get_setting("ton_space_commission") or 15)
                ton_price = float(await get_setting("ton_exchange_rate") or 2.93)
                amount_usd = stars * stars_price_usd * (1 + markup / 100) * (1 + commission / 100)
                amount_ton = amount_usd / ton_price
                buy_data["amount_usd"] = amount_usd
                buy_data["amount_ton"] = amount_ton
                invoice_id, pay_url = await create_ton_space_invoice(amount_ton, user_id, stars, recipient)
                if invoice_id:
                    buy_data["invoice_id"] = invoice_id
                    buy_data["pay_url"] = pay_url
                    context.user_data["buy_data"] = buy_data
                    return await confirm_payment(update, context)
                else:
                    await query.answer("Ошибка создания инвойса!")
                    logger.error(f"Failed to create TON Space invoice for user_id={user_id}")
            context.user_data["buy_data"] = buy_data
            return await buy_stars(update, context)

        else:
            await query.answer("Неизвестная команда!")
            logger.info(f"Unknown callback data for user_id={user_id}: {data}")
            return context.user_data.get("state", STATE_MAIN_MENU)

    except Exception as e:
        logger.error(f"Error in button_callback for user_id={user_id}: {e}", exc_info=True)
        await query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def shutdown(context: ApplicationContext):
    """Очистка при завершении работы."""
    logger.info("Shutting down bot")
    try:
        await close_db_pool()
        logger.info("Shutdown completed successfully")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}", exc_info=True)

def main():
    """Запускает бота."""
    try:
        logger.info("Starting bot")
        application = ApplicationBuilder().token(BOT_TOKEN).build()
        application.job_queue.run_repeating(update_ton_price, interval=600, first=10)

        conv_handler = ConversationHandler(
            entry_points=[CommandHandler("start", start)],
            states={
                STATE_MAIN_MENU: [CallbackQueryHandler(button_callback)],
                STATE_PROFILE: [CallbackQueryHandler(button_callback)],
                STATE_REFERRALS: [CallbackQueryHandler(button_callback)],
                STATE_TOP_REFERRALS: [CallbackQueryHandler(button_callback)],
                STATE_TOP_PURCHASES: [CallbackQueryHandler(button_callback)],
                STATE_BUY_STARS_RECIPIENT: [CallbackQueryHandler(button_callback), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)],
                STATE_BUY_STARS_AMOUNT: [CallbackQueryHandler(button_callback), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)],
                STATE_BUY_STARS_PAYMENT_METHOD: [CallbackQueryHandler(button_callback)],
                STATE_BUY_STARS_CONFIRM: [CallbackQueryHandler(button_callback)],
                STATE_ADMIN_PANEL: [CallbackQueryHandler(button_callback)],
                STATE_ADMIN_STATS: [CallbackQueryHandler(button_callback)],
                STATE_ADMIN_EDIT_TEXTS: [CallbackQueryHandler(button_callback)],
                STATE_EDIT_TEXT: [CallbackQueryHandler(button_callback), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)],
                STATE_ADMIN_USER_STATS: [CallbackQueryHandler(button_callback)],
                STATE_USER_SEARCH: [CallbackQueryHandler(button_callback), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)],
                STATE_LIST_USERS: [CallbackQueryHandler(button_callback)],
                STATE_EDIT_USER: [CallbackQueryHandler(button_callback), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)],
                STATE_ADMIN_EDIT_MARKUP: [CallbackQueryHandler(button_callback), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)],
                STATE_ADMIN_MANAGE_ADMINS: [CallbackQueryHandler(button_callback), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)],
                STATE_ADMIN_EDIT_PROFIT: [CallbackQueryHandler(button_callback), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)]
            },
            fallbacks=[CommandHandler("start", start)]
        )

        application.add_handler(conv_handler)
        application.run_polling()
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(init_db())
    main()
