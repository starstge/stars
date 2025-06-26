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

# Логирование
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot.log", encoding="utf-8")
    ]
)
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

# Константы состояний
STATE_MAIN_MENU, STATE_BUY_STARS_RECIPIENT, STATE_BUY_STARS_AMOUNT, STATE_BUY_STARS_PAYMENT_METHOD, \
STATE_BUY_STARS_CRYPTO_TYPE, STATE_BUY_STARS_CONFIRM, STATE_ADMIN_PANEL, STATE_ADMIN_STATS, \
STATE_ADMIN_EDIT_TEXTS, STATE_EDIT_TEXT, STATE_ADMIN_EDIT_MARKUP, STATE_ADMIN_MANAGE_ADMINS, \
STATE_ADMIN_EDIT_PROFIT, STATE_PROFILE, STATE_TOP_REFERRALS, STATE_TOP_PURCHASES, \
STATE_REFERRALS, STATE_ADMIN_USER_STATS, STATE_EDIT_USER, STATE_LIST_USERS = range(20)

# Глобальный пул базы данных
_db_pool = None
_db_pool_lock = asyncio.Lock()

# Глобальное приложение
app = None

async def check_environment():
    required_vars = ["BOT_TOKEN", "POSTGRES_URL", "SPLIT_API_TOKEN", "PROVIDER_TOKEN", "OWNER_WALLET", "WEBHOOK_URL"]
    optional_vars = ["TON_SPACE_API_TOKEN", "CRYPTOBOT_API_TOKEN", "TON_API_KEY"]
    for var in required_vars:
        if not os.getenv(var):
            logger.error(f"Missing required environment variable: {var}")
            raise ValueError(f"Environment variable {var} is not set")
    for var in optional_vars:
        if not os.getenv(var):
            logger.warning(f"Optional environment variable {var} is not set")

async def get_db_pool():
    global _db_pool
    async with _db_pool_lock:
        if _db_pool is None or _db_pool._closed:
            logger.info("Creating new database pool")
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
            logger.info("Database pool initialized successfully")
        return _db_pool

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
    global _db_pool
    async with _db_pool_lock:
        if _db_pool and not _db_pool._closed:
            logger.info("Closing database pool")
            await _db_pool.close()
            logger.info("Database pool closed successfully")
            _db_pool = None

async def init_db():
    logger.info("Initializing database")
    async with (await ensure_db_pool()) as conn:
        # Create users table
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

        # Create settings table
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value JSONB
            )
        ''')

        # Create texts table
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS texts (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')

        # Create transactions table
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

        # Create admin_logs table
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS admin_logs (
                id SERIAL PRIMARY KEY,
                admin_id BIGINT,
                action TEXT,
                details JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Insert initial settings
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
        ''', json.dumps([TWIN_ACCOUNT_ID]), PRICE_USD_PER_50 / 50, 2.93, 20, 25, 25, 5, 10, 15, 10, 10)

        # Insert initial texts
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
    async with (await ensure_db_pool()) as conn:
        result = await conn.fetchrow("SELECT value FROM settings WHERE key = $1", key)
        return json.loads(result["value"]) if result else None

async def update_setting(key, value):
    async with (await ensure_db_pool()) as conn:
        await conn.execute(
            "INSERT INTO settings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2",
            key, json.dumps(value)
        )
        logger.info(f"Setting {key} updated to {value}")

async def get_text(key, **kwargs):
    async with (await ensure_db_pool()) as conn:
        result = await conn.fetchrow("SELECT value FROM texts WHERE key = $1", key)
        text = result["value"] if result else ""
        return text.format(**kwargs) if text else f"Text for {key} not found"

async def update_text(key, value):
    async with (await ensure_db_pool()) as conn:
        await conn.execute(
            "INSERT INTO texts (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2",
            key, value
        )
        logger.info(f"Text {key} updated to {value}")

async def log_admin_action(admin_id, action, details=None):
    async with (await ensure_db_pool()) as conn:
        await conn.execute(
            "INSERT INTO admin_logs (admin_id, action, details) VALUES ($1, $2, $3)",
            admin_id, action, json.dumps(details or {})
        )
        logger.info(f"Admin action logged: admin_id={admin_id}, action={action}")

async def update_ton_price(context: ContextTypes.DEFAULT_TYPE):
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
                    logger.info(f"TON price updated: {ton_price} USD")
                else:
                    logger.error(f"TON API error: {response.status}")
                    context.bot_data["ton_price"] = await get_setting("ton_exchange_rate") or 2.93
        except Exception as e:
            logger.error(f"Error updating TON price: {e}")
            context.bot_data["ton_price"] = await get_setting("ton_exchange_rate") or 2.93

async def get_account_state(address: str) -> dict:
    url = f"https://toncenter.com/api/v3/account/states?address={address}"
    headers = {"Authorization": f"Bearer {TON_API_KEY}"}
    async with aiohttp.ClientSession(timeout=ClientTimeout(total=10)) as session:
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    return data["states"][0] if data["states"] else {}
                logger.error(f"TON API error: {response.status}, {await response.text()}")
                return {}
        except Exception as e:
            logger.error(f"Error fetching account state: {e}")
            return {}

async def check_transaction(address: str, amount_ton: float, payload: str) -> dict:
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
                logger.error(f"TON API error: {response.status}")
                return {}
        except Exception as e:
            logger.error(f"Error fetching transactions: {e}")
            return {}

async def generate_payload(user_id):
    rand = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return f"{user_id}_{rand}"

async def create_cryptobot_invoice(amount, currency, user_id, stars, recipient, payload):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            if not CRYPTOBOT_API_TOKEN or not PROVIDER_TOKEN:
                logger.error("CRYPTOBOT_API_TOKEN or PROVIDER_TOKEN is not set")
                return None, None
            headers = {"Crypto-Pay-API-Token": CRYPTOBOT_API_TOKEN}
            payload_data = {
                "amount": str(amount),
                "currency": currency,
                "description": f"Purchase of {stars} stars for @{recipient.lstrip('@')}",
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
                            logger.info(f"CryptoBot invoice created: invoice_id={invoice['invoice_id']}")
                            return invoice["invoice_id"], invoice["pay_url"]
                        logger.error(f"CryptoBot API error: {data}")
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2)
                            continue
                        return None, None
                    logger.error(f"CryptoBot API error: status={resp.status}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2)
                        continue
                    return None, None
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
                logger.warning("TON_SPACE_API_TOKEN not set, using direct TON payment")
                pay_url = f"ton://transfer/{OWNER_WALLET}?amount={int(amount_ton * 1_000_000_000)}&text={payload}"
                async with (await ensure_db_pool()) as conn:
                    await conn.execute(
                        "INSERT INTO transactions (user_id, stars, amount_ton, payment_method, recipient, status, invoice_id, payload) "
                        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                        user_id, stars, amount_ton, "ton_space_direct", recipient, "pending", payload, payload
                    )
                return payload, pay_url
            headers = {"Authorization": f"Bearer {TON_SPACE_API_TOKEN}"}
            payload_data = {
                "amount": str(amount_ton),
                "currency": "TON",
                "description": f"Purchase of {stars} stars for @{recipient.lstrip('@')}",
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
                            logger.info(f"TON Space invoice created: invoice_id={invoice['id']}")
                            return invoice["id"], invoice["pay_url"]
                        logger.error(f"TON Space API error: {data}")
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2)
                            continue
                        return None, None
                    logger.error(f"TON Space API error: status={resp.status}")
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
    if not TON_SPACE_API_TOKEN:
        logger.warning("TON_SPACE_API_TOKEN not set, cannot check payment")
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
                            logger.info(f"TON Space payment checked: invoice_id={invoice_id}, status={status}")
                            return status
                        logger.error(f"TON Space payment check failed: {data}")
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2)
                            continue
                        return False
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

async def check_cryptobot_payment(invoice_id):
    if not CRYPTOBOT_API_TOKEN:
        logger.warning("CRYPTOBOT_API_TOKEN not set, cannot check payment")
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
                            logger.info(f"CryptoBot payment checked: invoice_id={invoice_id}, status={status}")
                            return status
                        logger.error(f"CryptoBot payment check failed: {data}")
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2)
                            continue
                        return False
                    logger.error(f"CryptoBot API error: status={resp.status}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2)
                        continue
                    return False
        except Exception as e:
            logger.error(f"Error checking CryptoBot payment: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
            continue
    return False

async def issue_stars(recipient_username, stars, user_id):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            if not SPLIT_API_TOKEN:
                logger.error("SPLIT_API_TOKEN is not set")
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
                            logger.info(f"Stars issued: {stars} to @{recipient_username} by user_id={user_id}")
                            return True
                        logger.error(f"Split API failed: {data}")
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2)
                            continue
                        return False
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

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or f"user_{user_id}"
    ref_id = context.args[0].split("_")[1] if context.args and context.args[0].startswith("ref_") else None
    logger.info(f"Start command: user_id={user_id}, username={username}, ref_id={ref_id}")

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
    return STATE_MAIN_MENU

async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
            return STATE_PROFILE
        await update.callback_query.answer(text="Пользователь не найден.")
        return STATE_MAIN_MENU

async def referrals(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
            return STATE_REFERRALS
        await update.callback_query.answer(text="Пользователь не найден.")
        return STATE_MAIN_MENU

async def support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = await get_text("tech_support", support_channel=SUPPORT_CHANNEL)
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    context.user_data["state"] = STATE_MAIN_MENU
    await update.callback_query.answer()
    return STATE_MAIN_MENU

async def reviews(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = await get_text("reviews", news_channel=NEWS_CHANNEL)
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    context.user_data["state"] = STATE_MAIN_MENU
    await update.callback_query.answer()
    return STATE_MAIN_MENU

async def buy_stars(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

async def set_recipient(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "Введите username получателя звезд (например, @username):"
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    context.user_data["input_state"] = "recipient"
    context.user_data["state"] = STATE_BUY_STARS_RECIPIENT
    await update.callback_query.answer()
    return STATE_BUY_STARS_RECIPIENT

async def set_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "Введите количество звезд (кратно 50):"
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    context.user_data["input_state"] = "amount"
    context.user_data["state"] = STATE_BUY_STARS_AMOUNT
    await update.callback_query.answer()
    return STATE_BUY_STARS_AMOUNT

async def set_payment_method(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

async def select_crypto_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

async def confirm_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    buy_data = context.user_data.get("buy_data", {})
    recipient = buy_data.get("recipient")
    stars = buy_data.get("stars")
    amount_ton = buy_data.get("amount_ton")
    pay_url = buy_data.get("pay_url")
    if not all([recipient, stars, amount_ton, pay_url]):
        logger.error(f"Incomplete buy_data: {buy_data}")
        await update.callback_query.message.reply_text("Неполные данные для оплаты. Начните заново.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BUY_STARS)]]))
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
    return STATE_BUY_STARS_CONFIRM

async def check_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    buy_data = context.user_data.get("buy_data", {})
    recipient = buy_data.get("recipient")
    stars = buy_data.get("stars")
    payment_method = buy_data.get("payment_method")
    invoice_id = buy_data.get("invoice_id")
    payload = buy_data.get("payload")
    amount_ton = buy_data.get("amount_ton")
    if not all([recipient, stars, payment_method, invoice_id]):
        logger.error(f"Incomplete buy_data: {buy_data}")
        await update.callback_query.message.reply_text("Неполные данные для проверки оплаты. Начните заново.")
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
                    logger.error(f"Failed to send channel message: {e}")
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

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    admin_ids = await get_setting("admin_ids") or [TWIN_ACCOUNT_ID]
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

async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

async def admin_edit_texts(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    return STATE_ADMIN_EDIT_TEXTS

async def edit_text_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text_key = update.callback_query.data
    context.user_data["text_key"] = text_key
    text = f"Введите новый текст для '{text_key}':"
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    context.user_data["state"] = STATE_EDIT_TEXT
    await update.callback_query.answer()
    return STATE_EDIT_TEXT

async def admin_user_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    return STATE_ADMIN_USER_STATS

async def list_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

async def admin_edit_markup(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

async def admin_manage_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

async def admin_edit_profit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "Введите процент прибыли:"
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    context.user_data["input_state"] = "profit_percent"
    context.user_data["state"] = STATE_ADMIN_EDIT_PROFIT
    await update.callback_query.answer()
    return STATE_ADMIN_EDIT_PROFIT

async def top_referrals(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

async def top_purchases(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
            await log_admin_action(user_id, f"Edited text: {text_key}", {"text_key": text_key, "new_value": text})
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
                await log_admin_action(user_id, f"Updated markup {markup_type}", {"markup_type": markup_type, "new_value": markup})
                await update.message.reply_text(f"Наценка '{markup_type}' обновлена: {markup}%")
                context.user_data.pop("markup_type", None)
                context.user_data.pop("input_state", None)
                context.user_data["state"] = STATE_ADMIN_EDIT_MARKUP
                return await admin_edit_markup(update, context)
            except ValueError:
                await update.message.reply_text("Введите корректное число для наценки!")
                return STATE_ADMIN_EDIT_MARKUP

        elif state == STATE_ADMIN_EDIT_PROFIT and input_state == "profit_percent":
            try:
                profit = float(text)
                if profit < 0:
                    raise ValueError("Профит не может быть отрицательным")
                await update_setting("profit_percent", profit)
                await log_admin_action(user_id, f"Updated profit", {"new_value": profit})
                await update.message.reply_text(f"Профит обновлен: {profit}%")
                context.user_data.pop("input_state", None)
                context.user_data["state"] = STATE_ADMIN_PANEL
                return await admin_panel(update, context)
            except ValueError:
                await update.message.reply_text("Введите корректное число для прибыли!")
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
                admin_ids = await get_setting("admin_ids") or [TWIN_ACCOUNT_ID]
                if new_admin_id not in admin_ids:
                    admin_ids.append(new_admin_id)
                    await update_setting("admin_ids", admin_ids)
                    await log_admin_action(user_id, f"Added admin: {new_admin_id}", {"username": new_admin_username})
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
                admin_ids = await get_setting("admin_ids") or [TWIN_ACCOUNT_ID]
                if admin_id in admin_ids:
                    admin_ids.remove(admin_id)
                    await update_setting("admin_ids", admin_ids)
                    await log_admin_action(user_id, f"Removed admin: {admin_id}", {"username": admin_username})
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
                await log_admin_action(user_id, f"Updated {action} for user {selected_user['user_id']}", {"new_value": stars})
                await update.message.reply_text(f"Звезды для @{selected_user['username']} обновлены: {stars}")
                context.user_data.pop("input_state", None)
                context.user_data["state"] = STATE_ADMIN_USER_STATS
                return await admin_user_stats(update, context)
            except ValueError:
                await update.message.reply_text("Введите корректное число звезд!")
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
                await log_admin_action(user_id, f"Updated ref bonus for user {selected_user['user_id']}", {"new_value": ref_bonus})
                await update.message.reply_text(f"Реф. бонус для @{selected_user['username']} обновлен: {ref_bonus} TON")
                context.user_data.pop("input_state", None)
                context.user_data["state"] = STATE_ADMIN_USER_STATS
                return await admin_user_stats(update, context)
            except ValueError:
                await update.message.reply_text("Введите корректное число для реф. бонуса!")
                return STATE_EDIT_USER

        await update.message.reply_text("Неизвестная команда или состояние.")
        return state

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
                payment_method = "cryptobot_usd"
            elif data == PAY_CRYPTOBOT:
                markup = float(await get_setting("markup_cryptobot_crypto") or 25)
                payment_method = "cryptobot_ton"
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
                invoice_id, pay_url = await create_cryptobot_invoice(amount_usd if currency == "USD" else amount_ton, currency, user_id, stars, recipient, payload)
            elif data == PAY_TON_SPACE:
                invoice_id, pay_url = await create_ton_space_invoice(amount_ton, user_id, stars, recipient, payload)
            if invoice_id and pay_url:
                buy_data.update({
                    "payment_method": payment_method,
                    "amount_ton": amount_ton,
                    "amount_usd": amount_usd,
                    "invoice_id": invoice_id,
                    "pay_url": pay_url,
                    "payload": payload
                })
                context.user_data["buy_data"] = buy_data
                return await confirm_payment(update, context)
            await query.message.reply_text("Ошибка создания инвойса. Попробуйте снова.")
            await query.answer()
            return STATE_BUY_STARS_PAYMENT_METHOD
        await query.answer(text="Неизвестная команда.")
        return context.user_data.get("state", STATE_MAIN_MENU)
    except Exception as e:
        logger.error(f"Error in callback_query_handler: {e}", exc_info=True)
        await query.message.reply_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        await query.answer()
        return STATE_MAIN_MENU

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Update {update} caused error {context.error}", exc_info=True)
    if update and (update.message or update.callback_query):
        try:
            await (update.message or update.callback_query.message).reply_text(
                "Произошла ошибка. Попробуйте снова.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]])
            )
        except Exception as e:
            logger.error(f"Error sending error message: {e}")
    return STATE_MAIN_MENU

async def webhook_callback(request: web.Request):
    try:
        update = Update.de_json(await request.json(), app.bot)
        if not update:
            logger.warning("Invalid update received")
            return web.Response(status=400)
        await app.process_update(update)
        return web.Response(status=200)
    except Exception as e:
        logger.error(f"Webhook error: {e}", exc_info=True)
        return web.Response(status=500)

async def health_check(request: web.Request):
    return web.Response(text="OK", status=200)

async def main():
    global app
    try:
        await check_environment()
        await init_db()
        app = ApplicationBuilder().token(BOT_TOKEN).build()
        app.bot_data["ton_price"] = await get_setting("ton_exchange_rate") or 2.93

        conv_handler = ConversationHandler(
            entry_points=[CommandHandler("start", start)],
            states={
                STATE_MAIN_MENU: [
                    CallbackQueryHandler(callback_query_handler),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
                ],
                STATE_PROFILE: [CallbackQueryHandler(callback_query_handler)],
                STATE_REFERRALS: [CallbackQueryHandler(callback_query_handler)],
                STATE_TOP_REFERRALS: [CallbackQueryHandler(callback_query_handler)],
                STATE_TOP_PURCHASES: [CallbackQueryHandler(callback_query_handler)],
                STATE_BUY_STARS_RECIPIENT: [
                    CallbackQueryHandler(callback_query_handler),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
                ],
                STATE_BUY_STARS_AMOUNT: [
                    CallbackQueryHandler(callback_query_handler),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
                ],
                STATE_BUY_STARS_PAYMENT_METHOD: [CallbackQueryHandler(callback_query_handler)],
                STATE_BUY_STARS_CRYPTO_TYPE: [CallbackQueryHandler(callback_query_handler)],
                STATE_BUY_STARS_CONFIRM: [CallbackQueryHandler(callback_query_handler)],
                STATE_ADMIN_PANEL: [CallbackQueryHandler(callback_query_handler)],
                STATE_ADMIN_STATS: [CallbackQueryHandler(callback_query_handler)],
                STATE_ADMIN_EDIT_TEXTS: [CallbackQueryHandler(callback_query_handler)],
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
                STATE_ADMIN_USER_STATS: [
                    CallbackQueryHandler(callback_query_handler),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
                ],
                STATE_LIST_USERS: [CallbackQueryHandler(callback_query_handler)],
                STATE_EDIT_USER: [
                    CallbackQueryHandler(callback_query_handler),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
                ],
            },
            fallbacks=[CommandHandler("start", start)],
            allow_reentry=True
        )

        app.add_handler(conv_handler)
        app.add_error_handler(error_handler)

        async with app:
            await app.bot.set_webhook(WEBHOOK_URL)
            logger.info(f"Webhook set to {WEBHOOK_URL}")

            app.bot_data["webhook_app"] = web.Application()
            app.bot_data["webhook_app"].router.add_post("/callback", webhook_callback)
            app.bot_data["webhook_app"].router.add_get("/health", health_check)

            async def periodic_update_ton_price(context: ContextTypes.DEFAULT_TYPE):
                while True:
                    await update_ton_price(context)
                    await asyncio.sleep(3600)  # Обновление каждые 60 минут

            app.create_task(periodic_update_ton_price(app))
            await app.initialize()
            await app.start()

            runner = web.AppRunner(app.bot_data["webhook_app"])
            await runner.setup()
            site = web.TCPSite(runner, "0.0.0.0", PORT)
            await site.start()
            logger.info(f"Webhook server running on port {PORT}")

            try:
                await asyncio.Event().wait()  # Бесконечное ожидание
            except asyncio.CancelledError:
                logger.info("Shutting down webhook server")
            finally:
                await runner.cleanup()
                await app.stop()
                await app.shutdown()
                await close_db_pool()

    except Exception as e:
        logger.error(f"Main error: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(main())
