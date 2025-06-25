import asyncio
import asyncpg
import os
import time
import json
import logging
import aiohttp
import pytz
from datetime import datetime
from urllib.parse import urlparse
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler,
    filters, ContextTypes, ConversationHandler, JobQueue
)
from telegram.error import BadRequest
import random
import string

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
CRYPTOBOT_API_TOKEN = os.getenv("CRYPTOBOT_API_TOKEN")
TON_SPACE_API_TOKEN = os.getenv("TON_SPACE_API_TOKEN")
OWNER_WALLET = os.getenv("OWNER_WALLET") or "YOUR_TON_WALLET_ADDRESS"
PROVIDER_TOKEN = os.getenv("PROVIDER_TOKEN")  # For Telegram Crypto Pay
SPLIT_API_URL = "https://api.split.tg/buy/stars"
CRYPTOBOT_API_URL = "https://pay.crypt.bot/api"
TON_SPACE_API_URL = "https://api.ton.space/v1"
SUPPORT_CHANNEL = "@CheapStarsShop_support"
NEWS_CHANNEL = "@cheapstarshop_news"
TWIN_ACCOUNT_ID = 6956377285  # Your twin account ID for auto-start
PRICE_USD_PER_50 = 0.81  # Price for 50 stars in USD

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

# Константы состояний для ConversationHandler
STATE_MAIN_MENU = 0
STATE_BUY_STARS_RECIPIENT = 1
STATE_BUY_STARS_AMOUNT = 2
STATE_BUY_STARS_PAYMENT_METHOD = 3
STATE_BUY_STARS_CRYPTO_TYPE = 4
STATE_BUY_STARS_CONFIRM = 5
STATE_ADMIN_PANEL = 6
STATE_ADMIN_STATS = 7
STATE_ADMIN_EDIT_TEXTS = 8
STATE_EDIT_TEXT = 9
STATE_ADMIN_EDIT_MARKUP = 10
STATE_EDIT_MARKUP_TYPE = 11
STATE_ADMIN_MANAGE_ADMINS = 12
STATE_ADMIN_EDIT_PROFIT = 13
STATE_PROFILE = 14
STATE_TOP_REFERRALS = 15
STATE_TOP_PURCHASES = 16
STATE_REFERRALS = 17
STATE_USER_SEARCH = 18
STATE_EDIT_USER = 19
STATE_LIST_USERS = 20
STATE_ADMIN_USER_STATS = 21

# Глобальный пул базы данных
db_pool = None
_db_pool_lock = asyncio.Lock()

async def check_environment():
    required_vars = ["BOT_TOKEN", "POSTGRES_URL", "SPLIT_API_TOKEN", "PROVIDER_TOKEN"]
    optional_vars = ["TON_SPACE_API_TOKEN", "CRYPTOBOT_API_TOKEN", "TON_API_KEY"]
    for var_name in required_vars:
        if not os.getenv(var_name):
            logger.error(f"Missing required environment variable: {var_name}")
            raise ValueError(f"Environment variable {var_name} is not set")
    for var_name in optional_vars:
        if not os.getenv(var_name):
            logger.warning(f"Optional environment variable {var_name} is not set.")

async def get_db_pool():
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
    max_retries = 3
    for attempt in range(max_retries):
        try:
            return await get_db_pool()
        except Exception as e:
            logger.error(f"Attempt {attempt + 1}/{max_retries} to connect to DB failed: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
            else:
                raise

async def close_db_pool():
    global db_pool
    async with _db_pool_lock:
        if db_pool and not db_pool._closed:
            logger.info("Closing database pool")
            await db_pool.close()
            logger.info("Database pool closed successfully")
        db_pool = None

async def init_db():
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
            except asyncpg.exceptions.DuplicateColumnError:
                pass
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
                    amount_usd FLOAT,
                    payment_method TEXT,
                    recipient TEXT,
                    status TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    invoice_id TEXT,
                    payload TEXT
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
                ("stars_price_usd", PRICE_USD_PER_50 / 50),  # Price per star
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
                "welcome": "Добро пожаловать в @CheapStarsShop! Купите Telegram Stars за TON.\nЗвезд продано: {stars_sold}\nВы купили: {stars_bought} звезд",
                "buy_prompt": "Оплатите {amount_ton:.6f} TON\nСсылка: {address}\nДля: @{recipient}\nЗвезды: {stars}\nМетод: {method}",
                "profile": "👤 Профиль\nЗвезд куплено: {stars_bought}\nРеф. бонус: {ref_bonus_ton} TON\nРефералов: {ref_count}",
                "referrals": "🤝 Рефералы\nВаша ссылка: {ref_link}\nРефералов: {ref_count}\nБонус: {ref_bonus_ton} TON",
                "tech_support": f"🛠 Поддержка: {SUPPORT_CHANNEL}",
                "reviews": f"📝 Новости и отзывы: {NEWS_CHANNEL}",
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
    async with (await ensure_db_pool()) as conn:
        try:
            result = await conn.fetchrow("SELECT value FROM settings WHERE key = $1", key)
            return json.loads(result["value"]) if result else None
        except Exception as e:
            logger.error(f"Error getting setting {key}: {e}")
            return None

async def update_setting(key, value):
    async with (await ensure_db_pool()) as conn:
        try:
            await conn.execute(
                "INSERT INTO settings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2",
                key, json.dumps(value)
            )
            logger.info(f"Setting {key} updated to {value}")
        except Exception as e:
            logger.error(f"Error updating setting {key}: {e}")
            raise

async def get_text(key, **kwargs):
    async with (await ensure_db_pool()) as conn:
        try:
            result = await conn.fetchrow("SELECT value FROM texts WHERE key = $1", key)
            text = result["value"] if result else ""
            return text.format(**kwargs)
        except Exception as e:
            logger.error(f"Error getting text {key}: {e}")
            return ""

async def update_text(key, value):
    async with (await ensure_db_pool()) as conn:
        try:
            await conn.execute(
                "INSERT INTO texts (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2",
                key, value
            )
            logger.info(f"Text {key} updated to {value}")
        except Exception as e:
            logger.error(f"Error updating text {key}: {e}")
            raise

async def log_admin_action(admin_id, action):
    async with (await ensure_db_pool()) as conn:
        try:
            await conn.execute(
                "INSERT INTO admin_logs (admin_id, action) VALUES ($1, $2)",
                admin_id, action
            )
            logger.info(f"Admin action logged: admin_id={admin_id}, action={action}")
        except Exception as e:
            logger.error(f"Error logging admin action: {e}")

async def update_ton_price(context: ContextTypes.DEFAULT_TYPE):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.coingecko.com/api/v3/simple/price?ids=the-open-network&vs_currencies=usd") as response:
                if response.status == 200:
                    data = await response.json()
                    ton_price = float(data.get("the-open-network", {}).get("usd", 2.93))
                    await update_setting("ton_exchange_rate", ton_price)
                    context.bot_data["ton_price"] = ton_price
                    logger.info(f"TON price updated: {ton_price} USD")
                else:
                    logger.error(f"Failed to fetch TON price: {response.status}")
                    ton_price = context.bot_data.get("ton_price", await get_setting("ton_exchange_rate") or 2.93)
                    logger.info(f"Using cached TON price: {ton_price} USD")
        return ton_price
    except Exception as e:
        logger.error(f"Error updating TON price: {e}")
        ton_price = context.bot_data.get("ton_price", await get_setting("ton_exchange_rate") or 2.93)
        logger.info(f"Using cached TON price: {ton_price} USD")
        return ton_price

async def generate_payload(user_id):
    rand = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return f"{user_id}_{rand}"

async def create_cryptobot_invoice(amount_usd, currency, user_id, stars, recipient, payload):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            if not CRYPTOBOT_API_TOKEN or not PROVIDER_TOKEN:
                logger.error("CRYPTOBOT_API_TOKEN or PROVIDER_TOKEN is not set")
                return None, None
            headers = {"Crypto-Pay-API-Token": CRYPTOBOT_API_TOKEN}
            amount_in_cents = int(amount_usd * 100)
            prices = [LabeledPrice(label=f"{stars} звёзд", amount=amount_in_cents)]
            async with (await ensure_db_pool()) as conn:
                await conn.execute(
                    "INSERT INTO transactions (user_id, stars, amount_usd, payment_method, recipient, status, payload) "
                    "VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    user_id, stars, amount_usd, f"cryptobot_{currency.lower()}", recipient, "pending", payload
                )
            return payload, f"https://t.me/{os.getenv('BOT_USERNAME')}?start=pay_{payload}"
        except Exception as e:
            logger.error(f"Error creating CryptoBot invoice: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
            continue
    return None, None

async def create_ton_space_invoice(amount_ton, user_id, stars, recipient, payload):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            if not TON_SPACE_API_TOKEN:
                logger.error("TON_SPACE_API_TOKEN is not set")
                return None, None
            headers = {"Authorization": f"Bearer {TON_SPACE_API_TOKEN}"}
            payload_data = {
                "amount": str(amount_ton),
                "currency": "TON",
                "description": f"Purchase of {stars} stars for @{recipient.lstrip('@')}",
                "callback_url": f"https://t.me/{os.getenv('BOT_USERNAME')}",
                "metadata": json.dumps({"user_id": user_id, "stars": stars, "recipient": recipient, "payload": payload})
            }
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.post(f"{TON_SPACE_API_URL}/invoices", headers=headers, json=payload_data) as resp:
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
            logger.error(f"Error creating TON Space invoice: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
            continue
    return None, None

async def check_ton_space_payment(invoice_id):
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
            logger.error(f"Error checking TON Space payment: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
                continue
            return False
    return False

async def issue_stars(recipient_username, stars, user_id):
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
            logger.error(f"Error issuing stars: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
                continue
            return False
    return False

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or f"user_{user_id}"
    ref_id = context.args[0].split("_")[1] if context.args and context.args[0].startswith("ref_") else None
    logger.info(f"Start command: user_id={user_id}, username={username}, ref_id={ref_id}")

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
                                logger.error(f"Failed to send channel message: {e}")
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
        return STATE_MAIN_MENU
    except Exception as e:
        logger.error(f"Error in start: {e}")
        if update.message:
            await update.message.reply_text("Произошла ошибка. Попробуйте снова.")
        context.user_data.clear()
        return STATE_MAIN_MENU

async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
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
                return STATE_PROFILE
            else:
                await update.callback_query.answer(text="Пользователь не найден.")
                return STATE_MAIN_MENU
    except Exception as e:
        logger.error(f"Error in profile: {e}")
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def referrals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
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
                return STATE_REFERRALS
            else:
                await update.callback_query.answer(text="Пользователь не найден.")
                return STATE_MAIN_MENU
    except Exception as e:
        logger.error(f"Error in referrals: {e}")
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
        text = await get_text("tech_support")
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["state"] = STATE_MAIN_MENU
        await update.callback_query.answer()
        return STATE_MAIN_MENU
    except Exception as e:
        logger.error(f"Error in support: {e}")
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def reviews(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
        text = await get_text("reviews")
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["state"] = STATE_MAIN_MENU
        await update.callback_query.answer()
        return STATE_MAIN_MENU
    except Exception as e:
        logger.error(f"Error in reviews: {e}")
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def buy_stars(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
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
        return STATE_BUY_STARS_RECIPIENT
    except Exception as e:
        logger.error(f"Error in buy_stars: {e}")
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def set_recipient(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
        text = "Введите username получателя звезд (например, @username):"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["input_state"] = "recipient"
        context.user_data["state"] = STATE_BUY_STARS_RECIPIENT
        await update.callback_query.answer()
        return STATE_BUY_STARS_RECIPIENT
    except Exception as e:
        logger.error(f"Error in set_recipient: {e}")
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def set_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
        text = "Введите количество звезд (кратно 50):"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["input_state"] = "amount"
        context.user_data["state"] = STATE_BUY_STARS_AMOUNT
        await update.callback_query.answer()
        return STATE_BUY_STARS_AMOUNT
    except Exception as e:
        logger.error(f"Error in set_amount: {e}")
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def set_payment_method(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
        buy_data = context.user_data.get("buy_data", {})
        if not buy_data.get("recipient") or not buy_data.get("stars"):
            await update.callback_query.message.reply_text("Сначала выберите получателя и количество звезд!")
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
        return STATE_BUY_STARS_PAYMENT_METHOD
    except Exception as e:
        logger.error(f"Error in set_payment_method: {e}")
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def select_crypto_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
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
        return STATE_BUY_STARS_CRYPTO_TYPE
    except Exception as e:
        logger.error(f"Error in select_crypto_type: {e}")
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def confirm_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    buy_data = context.user_data.get("buy_data", {})
    try:
        recipient = buy_data.get("recipient")
        stars = buy_data.get("stars")
        amount_ton = buy_data.get("amount_ton")
        pay_url = buy_data.get("pay_url")
        if not all([recipient, stars, amount_ton, pay_url]):
            logger.error(f"Incomplete buy_data: {buy_data}")
            await update.callback_query.message.reply_text("Неполные данные для оплаты. Начните заново.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BUY_STARS)]]))
            return STATE_BUY_STARS
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
        return STATE_BUY_STARS_CONFIRM
    except Exception as e:
        logger.error(f"Error in confirm_payment: {e}")
        await update.callback_query.message.reply_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def check_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
        buy_data = context.user_data.get("buy_data", {})
        recipient = buy_data.get("recipient")
        stars = buy_data.get("stars")
        payment_method = buy_data.get("payment_method")
        invoice_id = buy_data.get("invoice_id")
        if not all([recipient, stars, payment_method, invoice_id]):
            logger.error(f"Incomplete buy_data: {buy_data}")
            await update.callback_query.message.reply_text("Неполные данные для проверки оплаты. Начните заново.")
            return STATE_BUY_STARS
        success = False
        async with (await ensure_db_pool()) as conn:
            if payment_method == "ton_space_api":
                success = await check_ton_space_payment(invoice_id)
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
                    context.user_data.clear()
                    context.user_data["state"] = STATE_MAIN_MENU
                    await start(update, context)
                    await update.callback_query.answer()
                    return STATE_MAIN_MENU
                else:
                    await update.callback_query.message.reply_text("Ошибка выдачи звезд. Обратитесь в поддержку.")
            else:
                await update.callback_query.message.reply_text("Оплата не подтверждена. Попробуйте снова.")
            await update.callback_query.answer()
            return STATE_BUY_STARS_CONFIRM
    except Exception as e:
        logger.error(f"Error in check_payment: {e}")
        await update.callback_query.message.reply_text("Произошла ошибка.")
        return STATE_BUY_STARS_CONFIRM

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
        admin_ids = await get_setting("admin_ids") or [6956377285]
        if user_id not in admin_ids:
            await update.callback_query.edit_message_text("Доступ запрещен!")
            context.user_data["state"] = STATE_MAIN_MENU
            await update.callback_query.answer()
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
        return STATE_ADMIN_PANEL
    except Exception as e:
        logger.error(f"Error in admin_panel: {e}")
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
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
            return STATE_ADMIN_STATS
    except Exception as e:
        logger.error(f"Error in admin_stats: {e}")
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def admin_edit_texts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
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
        current_message = context.user_data.get("last_admin_edit_texts_message", {})
        new_message = {"text": text, "reply_markup": reply_markup.to_dict()}
        if update.callback_query and current_message != new_message:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
            context.user_data["last_admin_edit_texts_message"] = new_message
            await update.callback_query.answer()
        elif update.message:
            await update.message.reply_text(text, reply_markup=reply_markup)
            context.user_data["last_admin_edit_texts_message"] = new_message
        context.user_data["state"] = STATE_ADMIN_EDIT_TEXTS
        return STATE_ADMIN_EDIT_TEXTS
    except Exception as e:
        logger.error(f"Error in admin_edit_texts: {e}")
        error_text = "Произошла ошибка."
        error_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]])
        if update.callback_query:
            await update.callback_query.message.reply_text(error_text, reply_markup=error_markup)
        elif update.message:
            await update.message.reply_text(error_text, reply_markup=error_markup)
        return STATE_MAIN_MENU

async def edit_text_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
        text_key = update.callback_query.data
        context.user_data["text_key"] = text_key
        text = f"Введите новый текст для '{text_key}':"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["state"] = STATE_EDIT_TEXT
        await update.callback_query.answer()
        return STATE_EDIT_TEXT
    except Exception as e:
        logger.error(f"Error in edit_text_prompt: {e}")
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def admin_user_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
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
        return STATE_ADMIN_USER_STATS
    except Exception as e:
        logger.error(f"Error in admin_user_stats: {e}")
        error_text = "Произошла ошибка."
        error_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]])
        if update.callback_query:
            await update.callback_query.message.reply_text(error_text, reply_markup=error_markup)
        else:
            await update.message.reply_text(error_text, reply_markup=error_markup)
        return STATE_MAIN_MENU

async def list_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
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
            return STATE_LIST_USERS
    except Exception as e:
        logger.error(f"Error in list_users: {e}")
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def admin_edit_markup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
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
        return STATE_ADMIN_EDIT_MARKUP
    except Exception as e:
        logger.error(f"Error in admin_edit_markup: {e}")
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def admin_manage_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
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
        return STATE_ADMIN_MANAGE_ADMINS
    except Exception as e:
        logger.error(f"Error in admin_manage_admins: {e}")
        error_text = "Произошла ошибка."
        error_markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]])
        if update.callback_query:
            await update.callback_query.message.reply_text(error_text, reply_markup=error_markup)
        else:
            await update.message.reply_text(error_text, reply_markup=error_markup)
        return STATE_MAIN_MENU

async def admin_edit_profit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
        text = "Введите процент прибыли:"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["input_state"] = "profit_percent"
        context.user_data["state"] = STATE_ADMIN_EDIT_PROFIT
        await update.callback_query.answer()
        return STATE_ADMIN_EDIT_PROFIT
    except Exception as e:
        logger.error(f"Error in admin_edit_profit: {e}")
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def top_referrals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
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
            return STATE_TOP_REFERRALS
    except Exception as e:
        logger.error(f"Error in top_referrals: {e}")
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def top_purchases(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
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
            return STATE_TOP_PURCHASES
    except Exception as e:
        logger.error(f"Error in top_purchases: {e}")
        await update.callback_query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if update.edited_message:
        logger.info(f"Ignoring edited message from user_id={user_id}")
        return context.user_data.get("state", STATE_MAIN_MENU)
    if not update.message:
        logger.error(f"No message in update for user_id={user_id}")
        return context.user_data.get("state", STATE_MAIN_MENU)
    text = update.message.text.strip()
    state = context.user_data.get("state", STATE_MAIN_MENU)
    input_state = context.user_data.get("input_state")
    logger.info(f"Text input: user_id={user_id}, state={state}, input_state={input_state}, text={text}")

    try:
        async with (await ensure_db_pool()) as conn:
            if state == STATE_BUY_STARS_RECIPIENT and input_state == "recipient":
                if not text.startswith("@"):
                    await update.message.reply_text("Username должен начинаться с @!")
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
                return STATE_BUY_STARS_RECIPIENT

            elif state == STATE_BUY_STARS_AMOUNT and input_state == "amount":
                try:
                    stars = int(text)
                    min_stars = await get_setting("min_stars_purchase") or 10
                    if stars < min_stars or stars % 50 != 0:
                        await update.message.reply_text(f"Введите количество звезд не менее {min_stars}, кратное 50!")
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
                    return STATE_BUY_STARS_RECIPIENT
                except ValueError:
                    await update.message.reply_text("Введите корректное количество звезд!")
                    return STATE_BUY_STARS_AMOUNT

            elif state == STATE_EDIT_TEXT:
                text_key = context.user_data.get("text_key")
                if not text_key:
                    await update.message.reply_text("Ошибка: ключ текста не найден.")
                    context.user_data["state"] = STATE_ADMIN_EDIT_TEXTS
                    return await admin_edit_texts(update, context)
                await update_text(text_key, text)
                await log_admin_action(user_id, f"Edited text: {text_key}")
                await update.message.reply_text(f"Текст '{text_key}' обновлен!")
                context.user_data.pop("input_state", None)
                context.user_data.pop("text_key", None)
                context.user_data["state"] = STATE_ADMIN_EDIT_TEXTS
                return await admin_edit_texts(update, context)

            elif state == STATE_ADMIN_EDIT_MARKUP and input_state == "edit_markup":
                markup_type = context.user_data.get("markup_type")
                if not markup_type:
                    await update.message.reply_text("Тип наценки не выбран!")
                    context.user_data["state"] = STATE_ADMIN_EDIT_MARKUP
                    return await admin_edit_markup(update, context)
                try:
                    markup = float(text)
                    if markup < 0:
                        raise ValueError("Наценка не может быть отрицательной")
                    await update_setting(markup_type, markup)
                    await log_admin_action(user_id, f"Updated markup {markup_type} to {markup}%")
                    await update.message.reply_text(f"Наценка '{markup_type}' обновлена: {markup}%")
                    context.user_data.pop("markup_type", None)
                    context.user_data.pop("input_state", None)
                    context.user_data["state"] = STATE_ADMIN_EDIT_MARKUP
                    return await admin_edit_markup(update, context)
                except ValueError as e:
                    await update.message.reply_text(str(e) if str(e) else "Введите корректное число для наценки!")
                    return STATE_ADMIN_EDIT_MARKUP

            elif state == STATE_ADMIN_EDIT_PROFIT and input_state == "profit_percent":
                try:
                    profit = float(text)
                    if profit < 0:
                        raise ValueError("Профит не может быть отрицательным")
                    await update_setting("profit_percent", profit)
                    await log_admin_action(user_id, f"Updated profit to {profit}%")
                    await update.message.reply_text(f"Профит обновлен: {profit}%")
                    context.user_data.pop("input_state", None)
                    context.user_data["state"] = STATE_ADMIN_PANEL
                    return await admin_panel(update, context)
                except ValueError as e:
                    await update.message.reply_text(str(e) if str(e) else "Введите корректное число для прибыли!")
                    return STATE_ADMIN_EDIT_PROFIT

            elif state == STATE_ADMIN_MANAGE_ADMINS and input_state == "add_admin":
                try:
                    new_admin_username = None
                    if text.isdigit():
                        new_admin_id = int(text)
                        try:
                            new_admin = await context.bot.get_chat(new_admin_id)
                            new_admin_username = new_admin.username or f"user_{new_admin_id}"
                        except BadRequest:
                            raise ValueError("Чат с указанным ID не найден")
                    else:
                        new_admin_username = text.lstrip("@")
                        try:
                            new_admin = await context.bot.get_chat(f"@{new_admin_username}")
                            new_admin_id = new_admin.id
                        except BadRequest:
                            raise ValueError("Пользователь с указанным username не найден")
                    admin_ids = await get_setting("admin_ids") or [6956377285]
                    if new_admin_id not in admin_ids:
                        admin_ids.append(new_admin_id)
                        await update_setting("admin_ids", admin_ids)
                        await log_admin_action(user_id, f"Added admin: {new_admin_id} (@{new_admin_username})")
                        await update.message.reply_text(f"Админ @{new_admin_username} добавлен!")
                    else:
                        await update.message.reply_text("Этот пользователь уже админ!")
                    context.user_data.pop("input_state", None)
                    context.user_data["state"] = STATE_ADMIN_MANAGE_ADMINS
                    return await admin_manage_admins(update, context)
                except ValueError as ve:
                    await update.message.reply_text(str(ve))
                    return STATE_ADMIN_MANAGE_ADMINS
                except Exception as e:
                    logger.error(f"Error adding admin: {e}")
                    await update.message.reply_text("Ошибка при добавлении админа.")
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
                    else:
                        await update.message.reply_text("Админ не найден!")
                    context.user_data.pop("input_state", None)
                    context.user_data["state"] = STATE_ADMIN_MANAGE_ADMINS
                    return await admin_manage_admins(update, context)
                except BadRequest:
                    await update.message.reply_text("Пользователь не найден!")
                    return STATE_ADMIN_MANAGE_ADMINS
                except Exception as e:
                    logger.error(f"Error removing admin: {e}")
                    await update.message.reply_text("Ошибка при удалении админа.")
                    return STATE_ADMIN_MANAGE_ADMINS

            elif state == STATE_ADMIN_USER_STATS and input_state == "search_user":
                try:
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
                            "username": username or f"user_{selected_user_id}",
                            "stars_bought": stars_bought,
                            "ref_bonus_ton": ref_bonus_ton,
                            "ref_count": len(json.loads(referrals)) if referrals and referrals != '[]' else 0
                        }
                        context.user_data["state"] = STATE_EDIT_USER
                        text = await get_text(
                            "user_info",
                            username=context.user_data["selected_user"]["username"],
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
                        return STATE_EDIT_USER
                    else:
                        await update.message.reply_text("Пользователь не найден!")
                        return STATE_ADMIN_USER_STATS
                except Exception as e:
                    logger.error(f"Error searching user: {e}")
                    await update.message.reply_text("Ошибка при поиске пользователя.")
                    return STATE_ADMIN_USER_STATS

            elif state == STATE_EDIT_USER and input_state in ("edit_stars", "edit_purchases"):
                try:
                    stars = int(text)
                    if stars < 0:
                        raise ValueError("Количество звезд не может быть отрицательным")
                    selected_user = context.user_data.get("selected_user", {})
                    if not selected_user:
                        await update.message.reply_text("Пользователь не выбран!")
                        return STATE_ADMIN_USER_STATS
                    await conn.execute(
                        "UPDATE users SET stars_bought = $1 WHERE user_id = $2",
                        stars, selected_user["user_id"]
                    )
                    action = "stars" if input_state == "edit_stars" else "purchases"
                    await log_admin_action(user_id, f"Updated {action} for user {selected_user['user_id']} to {stars}")
                    await update.message.reply_text(f"Звезды для @{selected_user['username']} обновлены: {stars}")
                    context.user_data.pop("input_state", None)
                    context.user_data["state"] = STATE_ADMIN_USER_STATS
                    return await admin_user_stats(update, context)
                except ValueError as e:
                    await update.message.reply_text(str(e) if str(e) else "Введите корректное число звезд!")
                    return STATE_EDIT_USER

            elif state == STATE_EDIT_USER and input_state == "edit_ref_bonus":
                try:
                    ref_bonus = float(text)
                    if ref_bonus < 0:
                        raise ValueError("Реферальный бонус не может быть отрицательным")
                    selected_user = context.user_data.get("selected_user", {})
                    if not selected_user:
                        await update.message.reply_text("Пользователь не выбран!")
                        return STATE_ADMIN_USER_STATS
                    await conn.execute(
                        "UPDATE users SET ref_bonus_ton = $1 WHERE user_id = $2",
                        ref_bonus, selected_user["user_id"]
                    )
                    await log_admin_action(user_id, f"Updated ref bonus for user {selected_user['user_id']} to {ref_bonus} TON")
                    await update.message.reply_text(f"Реф. бонус для @{selected_user['username']} обновлен: {ref_bonus} TON")
                    context.user_data.pop("input_state", None)
                    context.user_data["state"] = STATE_ADMIN_USER_STATS
                    return await admin_user_stats(update, context)
                except ValueError as e:
                    await update.message.reply_text(str(e) if str(e) else "Введите корректное число для реф. бонуса!")
                    return STATE_EDIT_USER

            else:
                await update.message.reply_text("Неизвестная команда или состояние.")
                return state
    except Exception as e:
        logger.error(f"Error in handle_text_input: {e}")
        await update.message.reply_text("Произошла ошибка. Попробуйте снова.")
        return STATE_MAIN_MENU

async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    query = update.callback_query
    data = query.data
    logger.info(f"Callback query: user_id={user_id}, data={data}")

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
        elif data == SELECT_CRYPTO_TYPE:
            return await select_crypto_type(update, context)
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
            return STATE_ADMIN_EDIT_MARKUP
        elif data == ADD_ADMIN:
            text = "Введите ID или username нового админа (например, @username или 123456789):"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text, reply_markup=reply_markup)
            context.user_data["input_state"] = "add_admin"
            context.user_data["state"] = STATE_ADMIN_MANAGE_ADMINS
            await query.answer()
            return STATE_ADMIN_MANAGE_ADMINS
        elif data == REMOVE_ADMIN:
            text = "Введите ID или username админа для удаления (например, @username или 123456789):"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text, reply_markup=reply_markup)
            context.user_data["input_state"] = "remove_admin"
            context.user_data["state"] = STATE_ADMIN_MANAGE_ADMINS
            await query.answer()
            return STATE_ADMIN_MANAGE_ADMINS
        elif data == "search_user":
            text = "Введите ID или username пользователя (например, @username или 123456789):"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.message.reply_text(text, reply_markup=reply_markup)
            context.user_data["input_state"] = "search_user"
            context.user_data["state"] = STATE_ADMIN_USER_STATS
            await query.answer()
            return STATE_ADMIN_USER_STATS
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
                        "username": username or f"user_{selected_user_id}",
                        "stars_bought": stars_bought,
                        "ref_bonus_ton": ref_bonus_ton,
                        "ref_count": len(json.loads(referrals)) if referrals and referrals != '[]' else 0
                    }
                    context.user_data["state"] = STATE_EDIT_USER
                    text = await get_text(
                        "user_info",
                        username=context.user_data["selected_user"]["username"],
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
                    return STATE_EDIT_USER
                else:
                    await query.message.reply_text("Пользователь не найден!")
                    await query.answer()
                    return STATE_ADMIN_USER_STATS
        elif data in [EDIT_USER_STARS, EDIT_USER_PURCHASES]:
            text = "Введите новое количество звезд:"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=ADMIN_USER_STATS)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            context.user_data["input_state"] = "edit_stars" if data == EDIT_USER_STARS else "edit_purchases"
            context.user_data["state"] = STATE_EDIT_USER
            await query.edit_message_text(text, reply_markup=reply_markup)
            await query.answer()
            return STATE_EDIT_USER
        elif data == EDIT_USER_REF_BONUS:
            text = "Введите новый реферальный бонус (в TON):"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=ADMIN_USER_STATS)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            context.user_data["input_state"] = "edit_ref_bonus"
            context.user_data["state"] = STATE_EDIT_USER
            await query.edit_message_text(text, reply_markup=reply_markup)
            await query.answer()
            return STATE_EDIT_USER
        elif data == LIST_USERS:
            return await list_users(update, context)
        elif data in [PAY_CARD, PAY_CRYPTOBOT, PAY_TON_SPACE]:
            buy_data = context.user_data.get("buy_data", {})
            recipient = buy_data.get("recipient")
            stars = buy_data.get("stars")
            if not recipient or not stars:
                await query.message.reply_text("Сначала выберите получателя и количество звезд!")
                await query.answer()
                return STATE_BUY_STARS_RECIPIENT
            ton_price = context.bot_data.get("ton_price", await get_setting("ton_exchange_rate") or 2.93)
            stars_price_usd = (await get_setting("stars_price_usd") or (PRICE_USD_PER_50 / 50)) * stars
            markup = 0
            payment_method = ""
            if data == PAY_CARD:
                markup = float(await get_setting("markup_cryptobot_card") or 25)
                payment_method = "cryptobot_card"
            elif data == PAY_CRYPTOBOT:
                markup = float(await get_setting("markup_cryptobot_crypto") or 25)
                payment_method = "cryptobot_crypto"
            elif data == PAY_TON_SPACE:
                markup = float(await get_setting("markup_ton_space") or 20)
                payment_method = "ton_space_api"
            amount_usd = stars_price_usd * (1 + markup / 100)
            amount_ton = amount_usd / ton_price
            payload = await generate_payload(user_id)
            invoice_id = None
            pay_url = None
            if data in [PAY_CARD, PAY_CRYPTOBOT]:
                currency = "USD" if data == PAY_CARD else "TON"
                invoice_id, pay_url = await create_cryptobot_invoice(amount_usd, currency, user_id, stars, recipient, payload)
            elif data == PAY_TON_SPACE:
                invoice_id, pay_url = await create_ton_space_invoice(amount_ton, user_id, stars, recipient, payload)
            if invoice_id and pay_url:
                buy_data.update({
                    "payment_method": payment_method,
                    "amount_ton": amount_ton,
                    "amount_usd": amount_usd,
                    "invoice_id": invoice_id,
                    "pay_url": pay_url
                })
                context.user_data["buy_data"] = buy_data
                async with (await ensure_db_pool()) as conn:
                    await conn.execute(
                        "INSERT INTO transactions (user_id, stars, amount_ton, amount_usd, payment_method, recipient, status, invoice_id, payload) "
                        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                        user_id, stars, amount_ton, amount_usd, payment_method, recipient, "pending", invoice_id, payload
                    )
                return await confirm_payment(update, context)
            else:
                await query.message.reply_text("Ошибка создания инвойса. Попробуйте снова.")
                await query.answer()
                return STATE_BUY_STARS_PAYMENT_METHOD
        else:
            await query.answer(text="Неизвестная команда.")
            return context.user_data.get("state", STATE_MAIN_MENU)
    except Exception as e:
        logger.error(f"Error in callback_query_handler: {e}", exc_info=True)
        await query.message.reply_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def auto_start_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        async with (await ensure_db_pool()) as conn:
            user = await conn.fetchrow("SELECT is_new FROM users WHERE user_id = $1", TWIN_ACCOUNT_ID)
            if user and user["is_new"]:
                await context.bot.send_message(chat_id=TWIN_ACCOUNT_ID, text="/start")
                await conn.execute("UPDATE users SET is_new = FALSE WHERE user_id = $1", TWIN_ACCOUNT_ID)
                logger.info(f"Auto-start sent to twin account {TWIN_ACCOUNT_ID}")
    except Exception as e:
        logger.error(f"Error in auto_start_job: {e}")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Update {update} caused error {context.error}", exc_info=True)
    if update and (update.message or update.callback_query):
        try:
            text = "Произошла ошибка. Попробуйте снова или свяжитесь с поддержкой."
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            if update.callback_query:
                await update.callback_query.message.reply_text(text, reply_markup=reply_markup)
            elif update.message:
                await update.message.reply_text(text, reply_markup=reply_markup)
        except Exception as e:
            logger.error(f"Error in error_handler: {e}")
    context.user_data.clear()
    context.user_data["state"] = STATE_MAIN_MENU

async def main():
    try:
        await check_environment()
        await init_db()
        app = ApplicationBuilder().token(BOT_TOKEN).build()
        app.bot_data["ton_price"] = await get_setting("ton_exchange_rate") or 2.93

        conv_handler = ConversationHandler(
            entry_points=[CommandHandler("start", start), CallbackQueryHandler(callback_query_handler)],
            states={
                STATE_MAIN_MENU: [
                    CallbackQueryHandler(callback_query_handler),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
                ],
                STATE_BUY_STARS_RECIPIENT: [
                    CallbackQueryHandler(callback_query_handler),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
                ],
                STATE_BUY_STARS_AMOUNT: [
                    CallbackQueryHandler(callback_query_handler),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
                ],
                STATE_BUY_STARS_PAYMENT_METHOD: [
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATE_BUY_STARS_CRYPTO_TYPE: [
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATE_BUY_STARS_CONFIRM: [
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATE_ADMIN_PANEL: [
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATE_ADMIN_STATS: [
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATE_ADMIN_EDIT_TEXTS: [
                    CallbackQueryHandler(callback_query_handler),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
                ],
                STATE_EDIT_TEXT: [
                    CallbackQueryHandler(callback_query_handler),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
                ],
                STATE_ADMIN_EDIT_MARKUP: [
                    CallbackQueryHandler(callback_query_handler),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
                ],
                STATE_ADMIN_MANAGE_ADMINS: [
                    CallbackQueryHandler(callback_query_handler),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
                ],
                STATE_ADMIN_EDIT_PROFIT: [
                    CallbackQueryHandler(callback_query_handler),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
                ],
                STATE_PROFILE: [
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATE_TOP_REFERRALS: [
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATE_TOP_PURCHASES: [
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATE_REFERRALS: [
                    CallbackQueryHandler(callback_query_handler)
                ],
                STATE_ADMIN_USER_STATS: [
                    CallbackQueryHandler(callback_query_handler),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
                ],
                STATE_EDIT_USER: [
                    CallbackQueryHandler(callback_query_handler),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
                ],
                STATE_LIST_USERS: [
                    CallbackQueryHandler(callback_query_handler)
                ]
            },
            fallbacks=[CommandHandler("start", start)],
            per_user=True,
            per_chat=True
        )

        app.add_handler(conv_handler)
        app.add_error_handler(error_handler)

        job_queue = app.job_queue
        job_queue.run_repeating(update_ton_price, interval=3600, first=0)
        job_queue.run_repeating(auto_start_job, interval=3600, first=60)

        logger.info("Starting bot polling")
        await app.run_polling(allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
    finally:
        await close_db_pool()

if __name__ == "__main__":
    asyncio.run(main())
