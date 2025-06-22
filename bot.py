import asyncpg
import os
import time
import json
import logging
import asyncio
import aiohttp
import aiohttp.web
import pytz
from datetime import datetime
from urllib.parse import urlparse
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler,
    filters, ContextTypes, ConversationHandler, JobQueue
)

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
MARKUP_PERCENTAGE = float(os.getenv("MARKUP_PERCENTAGE", 10))
SPLIT_API_URL = "https://api.split.tg/buy/stars"
CRYPTOBOT_API_URL = "https://pay.crypt.bot/api"
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
STATE_USER_SEARCH = 17
STATE_EDIT_USER = 18
STATE_ADMIN_EDIT_MARKUP = 9
STATE_EDIT_MARKUP_TYPE = 10
STATE_ADMIN_MANAGE_ADMINS = 11
STATE_ADMIN_EDIT_PROFIT = 12
STATE_PROFILE = 13
STATE_TOP_REFERRALS = 14
STATE_TOP_PURCHASES = 15
STATE_REFERRALS = 16

# Глобальный пул базы данных
db_pool = None
_db_pool_lock = asyncio.Lock()

async def get_db_pool():
    """Инициализирует и возвращает пул соединений с базой данных."""
    global db_pool
    async with _db_pool_lock:
        if db_pool is None or db_pool._closed:
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
                logger.error(f"Failed to initialize database pool: {e}")
                raise
        return db_pool

async def init_db():
    """Инициализирует базу данных."""
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
            # Проверка и добавление столбца is_new, если он отсутствует
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
                ("markup_ton_space", 10),
                ("markup_cryptobot_crypto", 10),
                ("markup_cryptobot_card", 10),
                ("markup_ref_bonus", 5),
                ("min_stars_purchase", 10),
                ("ton_space_commission", 20),
                ("card_commission", 30),
                ("profit_percent", 10)
            ]
            for key, value in default_settings:
                await conn.execute(
                    "INSERT INTO settings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING",
                    key, json.dumps(value)
                )
            default_texts = {
                "welcome": "Добро пожаловать! Купите Telegram Stars за TON.\nЗвезд продано: {stars_sold}",
                "buy_prompt": "Отправьте {amount_ton:.6f} TON на адрес:\n{address}\nMemo: {memo}\nДля: @{recipient}\nЗвезды: {stars}\nМетод: {method}",
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
            logger.error(f"Failed to initialize database: {e}")
            raise

async def get_setting(key):
    """Получает значение настройки."""
    async with (await get_db_pool()) as conn:
        result = await conn.fetchrow("SELECT value FROM settings WHERE key = $1", key)
        return json.loads(result["value"]) if result else None

async def update_setting(key, value):
    """Обновляет настройку."""
    async with (await get_db_pool()) as conn:
        await conn.execute(
            "INSERT INTO settings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2",
            key, json.dumps(value)
        )

async def get_text(key, user_id, **kwargs):
    """Получает текст по ключу."""
    async with (await get_db_pool()) as conn:
        result = await conn.fetchrow("SELECT value FROM texts WHERE key = $1", key)
        text = result["value"] if result else ""
        return text.format(**kwargs)

async def update_text(key, value):
    """Обновляет текст."""
    async with (await get_db_pool()) as conn:
        await conn.execute(
            "INSERT INTO texts (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $2",
            key, value
        )

async def log_admin_action(admin_id, action):
    """Логирует действие админа."""
    async with (await get_db_pool()) as conn:
        await conn.execute(
            "INSERT INTO admin_logs (admin_id, action) VALUES ($1, $2)",
            admin_id, action
        )

async def update_ton_price(context: ContextTypes.DEFAULT_TYPE):
    """Обновляет курс TON с использованием CoinGecko API."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.coingecko.com/api/v3/simple/price?ids=the-open-network&vs_currencies=usd") as response:
                if response.status == 200:
                    data = await response.json()
                    ton_price = float(data.get("the-open-network", {}).get("usd", 2.93))
                    await update_setting("ton_exchange_rate", ton_price)
                    context.bot_data["ton_price"] = ton_price  # Кэшируем в памяти
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
        logger.error(f"Error updating TON price from CoinGecko: {e}")
        ton_price = context.bot_data.get("ton_price", await get_setting("ton_exchange_rate") or 2.93)
        logger.info(f"Using cached TON price: {ton_price} USD")

async def create_cryptobot_invoice(amount_usd, currency, user_id, stars, recipient):
    """Создает инвойс через CryptoBot."""
    try:
        async with aiohttp.ClientSession() as session:
            headers = {"Crypto-Pay-API-Token": CRYPTOBOT_API_TOKEN}
            payload = {
                "amount": f"{amount_usd:.2f}",
                "currency": currency,
                "description": f"Purchase of {stars} Telegram Stars for @{recipient}",
                "order_id": f"order_{user_id}_{int(time.time())}"
            }
            async with session.post(f"{CRYPTOBOT_API_URL}/createInvoice", headers=headers, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("ok"):
                        invoice_id = data["result"]["invoice_id"]
                        pay_url = data["result"]["pay_url"]
                        logger.info(f"CryptoBot invoice created: invoice_id={invoice_id}")
                        return invoice_id, pay_url
                    else:
                        logger.error(f"CryptoBot invoice creation failed: {data}")
                        return None, None
                else:
                    logger.error(f"CryptoBot API error: {response.status}")
                    return None, None
    except Exception as e:
        logger.error(f"Error creating CryptoBot invoice: {e}")
        return None, None

async def check_cryptobot_payment(invoice_id):
    """Проверяет статус оплаты через CryptoBot."""
    try:
        async with aiohttp.ClientSession() as session:
            headers = {"Crypto-Pay-API-Token": CRYPTOBOT_API_TOKEN}
            async with session.get(f"{CRYPTOBOT_API_URL}/getInvoices?invoice_ids={invoice_id}", headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("ok"):
                        invoice = data["result"]["items"][0]
                        return invoice["status"] == "paid"
                    else:
                        logger.error(f"CryptoBot payment check failed: {data}")
                        return False
                else:
                    logger.error(f"CryptoBot API error: {response.status}")
                    return False
    except Exception as e:
        logger.error(f"Error checking CryptoBot payment: {e}")
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
            async with session.post(SPLIT_API_URL, headers=headers, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("success"):
                        logger.info(f"Stars issued: {stars} to @{recipient_username} by user_id={user_id}")
                        return True
                    else:
                        logger.error(f"Split API failed: {data}")
                        return False
                else:
                    logger.error(f"Split API error: {response.status}")
                    return False
    except Exception as e:
        logger.error(f"Error issuing stars: {e}")
        return False

async def check_ton_payment(address, memo, amount_ton):
    """Проверяет оплату TON."""
    try:
        async with aiohttp.ClientSession() as session:
            headers = {"Authorization": f"Bearer {TON_API_KEY}"}
            async with session.get(f"https://api.tonscan.org/v1/transactions?address={address}", headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    for tx in data.get("transactions", []):
                        if tx.get("memo") == memo and abs(float(tx.get("amount_ton", 0)) - amount_ton) < 0.0001:
                            logger.info(f"TON payment confirmed: memo={memo}, amount={amount_ton}")
                            return True
                    return False
                else:
                    logger.error(f"TON API error: {response.status}")
                    return False
    except Exception as e:
        logger.error(f"Error checking TON payment: {e}")
        return False

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start."""
    user_id = update.effective_user.id
    username = update.effective_user.username or str(user_id)
    ref_id = context.args[0].split("_")[1] if context.args and context.args[0].startswith("ref_") else None
    logger.info(f"Start command: user_id={user_id}, username={username}, ref_id={ref_id}")

    async with (await get_db_pool()) as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
        stars_sold = await conn.fetchval("SELECT SUM(stars) FROM transactions WHERE status = 'completed'") or 0
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
                        logger.info(f"Referral added: user_id={user_id} to referrer={ref_id}")
                        try:
                            await context.bot.send_message(
                                chat_id=CHANNEL_ID,
                                text=f"Новый пользователь @{username} через реферала ID {ref_id}"
                            )
                        except Exception as e:
                            logger.error(f"Failed to send channel message: {e}")
        else:
            await conn.execute(
                "UPDATE users SET username = $1 WHERE user_id = $2",
                username, user_id
            )

    text = await get_text("welcome", user_id, stars_sold=stars_sold)
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
    context.user_data["state"] = STATE_MAIN_MENU
    return STATE_MAIN_MENU

async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик профиля."""
    user_id = update.effective_user.id
    async with (await get_db_pool()) as conn:
        user = await conn.fetchrow("SELECT stars_bought, ref_bonus_ton, referrals FROM users WHERE user_id = $1", user_id)
        if user:
            stars_bought, ref_bonus_ton, referrals = user
            ref_count = len(json.loads(referrals)) if referrals != '[]' else 0
            text = await get_text("profile", user_id, stars_bought=stars_bought, ref_bonus_ton=ref_bonus_ton, ref_count=ref_count)
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

async def referrals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик рефералов."""
    user_id = update.effective_user.id
    async with (await get_db_pool()) as conn:
        user = await conn.fetchrow("SELECT ref_bonus_ton, referrals FROM users WHERE user_id = $1", user_id)
        if user:
            ref_bonus_ton, referrals = user
            ref_count = len(json.loads(referrals)) if referrals != '[]' else 0
            ref_link = f"https://t.me/{context.bot.username}?start=ref_{user_id}"
            text = await get_text("referrals", user_id, ref_link=ref_link, ref_count=ref_count, ref_bonus_ton=ref_bonus_ton)
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
            context.user_data["state"] = STATE_REFERRALS
            await update.callback_query.answer()
            return STATE_REFERRALS

async def support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик поддержки."""
    user_id = update.effective_user.id
    text = await get_text("tech_support", user_id)
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    context.user_data["state"] = STATE_MAIN_MENU
    await update.callback_query.answer()
    return STATE_MAIN_MENU

async def reviews(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик отзывов."""
    user_id = update.effective_user.id
    text = await get_text("reviews", user_id)
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    context.user_data["state"] = STATE_MAIN_MENU
    await update.callback_query.answer()
    return STATE_MAIN_MENU

async def buy_stars(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начинает процесс покупки звезд."""
    user_id = update.effective_user.id
    context.user_data["buy_data"] = {}
    text = "Введите username получателя звезд (например, @username):"
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    context.user_data["state"] = STATE_BUY_STARS_RECIPIENT
    await update.callback_query.answer()
    return STATE_BUY_STARS_RECIPIENT

async def set_recipient(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает выбор получателя."""
    user_id = update.effective_user.id
    buy_data = context.user_data.get("buy_data", {})
    text = "Введите username получателя звезд (например, @username):"
    keyboard = [
        [InlineKeyboardButton(f"Кому звезды: @{buy_data.get('recipient', '-')}", callback_data="set_recipient")],
        [InlineKeyboardButton(f"Количество звезд: {buy_data.get('stars', '-')}", callback_data="set_amount")],
        [InlineKeyboardButton(f"Метод оплаты: {buy_data.get('payment_method', '-')}", callback_data="set_payment")],
        [InlineKeyboardButton(f"Цена: {buy_data.get('amount_ton', '-')}", callback_data="confirm_payment")],
        [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    context.user_data["state"] = STATE_BUY_STARS_RECIPIENT
    await update.callback_query.answer()
    return STATE_BUY_STARS_RECIPIENT

async def set_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ввод количества звезд."""
    user_id = update.effective_user.id
    buy_data = context.user_data.get("buy_data", {})
    text = "Введите количество звезд:"
    keyboard = [
        [InlineKeyboardButton(f"Кому звезды: @{buy_data.get('recipient', '-')}", callback_data="set_recipient")],
        [InlineKeyboardButton(f"Количество звезд: {buy_data.get('stars', '-')}", callback_data="set_amount")],
        [InlineKeyboardButton(f"Метод оплаты: {buy_data.get('payment_method', '-')}", callback_data="set_payment")],
        [InlineKeyboardButton(f"Цена: {buy_data.get('amount_ton', '-')}", callback_data="confirm_payment")],
        [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    context.user_data["state"] = STATE_BUY_STARS_AMOUNT
    await update.callback_query.answer()
    return STATE_BUY_STARS_AMOUNT

async def set_payment_method(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает выбор метода оплаты."""
    user_id = update.effective_user.id
    buy_data = context.user_data.get("buy_data", {})
    text = "Выберите метод оплаты:"
    keyboard = [
        [InlineKeyboardButton("Крипта (TON)", callback_data=PAY_CRYPTO)],
        [InlineKeyboardButton("Карта", callback_data=PAY_CARD)],
        [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    context.user_data["state"] = STATE_BUY_STARS_PAYMENT_METHOD
    await update.callback_query.answer()
    return STATE_BUY_STARS_PAYMENT_METHOD

async def confirm_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждает детали оплаты."""
    user_id = update.effective_user.id
    buy_data = context.user_data.get("buy_data", {})
    stars = buy_data.get("stars")
    if not stars:
        await update.callback_query.edit_message_text("Сначала выберите количество звезд!")
        context.user_data["state"] = STATE_BUY_STARS_AMOUNT
        await update.callback_query.answer()
        return STATE_BUY_STARS_AMOUNT
    if buy_data.get("payment_method") == "ton_space":
        text = await get_text(
            "buy_prompt", user_id, amount_ton=buy_data["amount_ton"], stars=stars,
            recipient=buy_data["recipient"], address=buy_data["address"],
            memo=buy_data["memo"], method="TON Space"
        )
        keyboard = [
            [InlineKeyboardButton(f"Кому звезды: @{buy_data['recipient']}", callback_data="set_recipient")],
            [InlineKeyboardButton(f"Количество звезд: {stars}", callback_data="set_amount")],
            [InlineKeyboardButton("Метод оплаты: Крипта (TON)", callback_data="set_payment")],
            [InlineKeyboardButton(f"Цена: {buy_data['amount_ton']:.6f} TON", callback_data="confirm_payment")],
            [InlineKeyboardButton("Проверить оплату", callback_data=CHECK_PAYMENT)],
            [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
        ]
    else:
        text = await get_text(
            "buy_prompt", user_id, amount_ton=buy_data["amount_ton"], stars=stars,
            recipient=buy_data["recipient"], address=buy_data["pay_url"],
            memo="N/A", method="Карта (CryptoBot)"
        )
        keyboard = [
            [InlineKeyboardButton(f"Кому звезды: @{buy_data['recipient']}", callback_data="set_recipient")],
            [InlineKeyboardButton(f"Количество звезд: {stars}", callback_data="set_amount")],
            [InlineKeyboardButton("Метод оплаты: Карта", callback_data="set_payment")],
            [InlineKeyboardButton(f"Цена: ${buy_data['amount_usd']:.2f} ({buy_data['amount_ton']:.6f} TON)", callback_data="confirm_payment")],
            [InlineKeyboardButton("Проверить оплату", callback_data=CHECK_PAYMENT)],
            [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
        ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    context.user_data["state"] = STATE_BUY_STARS_CONFIRM
    await update.callback_query.answer()
    return STATE_BUY_STARS_CONFIRM

async def check_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Проверяет статус оплаты."""
    user_id = update.effective_user.id
    buy_data = context.user_data.get("buy_data", {})
    recipient = buy_data.get("recipient")
    stars = buy_data.get("stars")
    payment_method = buy_data.get("payment_method")
    success = False

    async with (await get_db_pool()) as conn:
        if payment_method == "ton_space":
            success = await check_ton_payment(buy_data["address"], buy_data["memo"], buy_data["amount_ton"])
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
                text = await get_text("buy_success", user_id, recipient=recipient, stars=stars)
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

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик админ-панели."""
    user_id = update.effective_user.id
    admin_ids = await get_setting("admin_ids") or [6956377285]
    if user_id not in admin_ids:
        await update.callback_query.edit_message_text("Доступ запрещен!")
        context.user_data["state"] = STATE_MAIN_MENU
        await update.callback_query.answer()
        return STATE_MAIN_MENU
    text = "🛠 Админ-панель"
    keyboard = [
        [InlineKeyboardButton("📊 Статистика", callback_data=ADMIN_STATS)],
        [InlineKeyboardButton("📝 Изменить тексты", callback_data=ADMIN_EDIT_TEXTS)],
        [InlineKeyboardButton("👥 Статистика пользователей", callback_data=ADMIN_USER_STATS)],
        [InlineKeyboardButton("💸 Изменить наценку", callback_data=ADMIN_EDIT_MARKUP)],
        [InlineKeyboardButton("👤 Добавить/снять админа", callback_data=ADMIN_MANAGE_ADMINS)],
        [InlineKeyboardButton("💰 Изменить прибыль", callback_data=ADMIN_EDIT_PROFIT)],
        [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    context.user_data["state"] = STATE_ADMIN_PANEL
    await update.callback_query.answer()
    return STATE_ADMIN_PANEL

async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отображает статистику."""
    user_id = update.effective_user.id
    async with (await get_db_pool()) as conn:
        total_profit = await conn.fetchval("SELECT SUM(amount_ton * $1 / 100) FROM transactions WHERE status = 'completed'", float(await get_setting("profit_percent") or 10)) or 0
        total_stars = await conn.fetchval("SELECT SUM(stars) FROM transactions WHERE status = 'completed'") or 0
        total_users = await conn.fetchval("SELECT COUNT(*) FROM users") or 0
        text = f"📊 Статистика\nПрибыль: {total_profit:.6f} TON\nЗвезд продано: {total_stars}\nПользователей: {total_users}"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["state"] = STATE_ADMIN_STATS
        await update.callback_query.answer()
        return STATE_ADMIN_STATS

async def admin_edit_texts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Редактирование текстов."""
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
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    context.user_data["state"] = STATE_ADMIN_EDIT_TEXTS
    await update.callback_query.answer()
    return STATE_ADMIN_EDIT_TEXTS

async def edit_text_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запрашивает новый текст."""
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
    """Статистика пользователей."""
    user_id = update.effective_user.id
    text = "Введите ID или username пользователя:"
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    context.user_data["state"] = STATE_USER_SEARCH
    await update.callback_query.answer()
    return STATE_USER_SEARCH

async def admin_edit_markup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Изменение наценки."""
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
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    context.user_data["state"] = STATE_ADMIN_EDIT_MARKUP
    await update.callback_query.answer()
    return STATE_ADMIN_EDIT_MARKUP

async def admin_manage_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Управление админами."""
    user_id = update.effective_user.id
    text = "Выберите действие:"
    keyboard = [
        [InlineKeyboardButton("Добавить админа", callback_data=ADD_ADMIN)],
        [InlineKeyboardButton("Снять админа", callback_data=REMOVE_ADMIN)],
        [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    context.user_data["state"] = STATE_ADMIN_MANAGE_ADMINS
    await update.callback_query.answer()
    return STATE_ADMIN_MANAGE_ADMINS

async def admin_edit_profit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Изменение прибыли."""
    user_id = update.effective_user.id
    text = "Введите процент прибыли:"
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    context.user_data["input_state"] = "profit_percent"
    context.user_data["state"] = STATE_ADMIN_EDIT_PROFIT
    await update.callback_query.answer()
    return STATE_ADMIN_EDIT_PROFIT

async def top_referrals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Топ рефералов."""
    user_id = update.effective_user.id
    async with (await get_db_pool()) as conn:
        users = await conn.fetch("SELECT username, referrals FROM users ORDER BY jsonb_array_length(referrals::jsonb) DESC LIMIT 5")
        text = "🏆 Топ рефералов:\n"
        for i, user in enumerate(users, 1):
            ref_count = len(json.loads(user["referrals"])) if user["referrals"] != '[]' else 0
            text += f"{i}. @{user['username']}: {ref_count} рефералов\n"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=PROFILE)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["state"] = STATE_TOP_REFERRALS
        await update.callback_query.answer()
        return STATE_TOP_REFERRALS

async def top_purchases(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Топ покупок."""
    user_id = update.effective_user.id
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
        return STATE_TOP_PURCHASES

async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает текстовый ввод."""
    user_id = update.effective_user.id
    text = update.message.text.strip()
    state = context.user_data.get("state", STATE_MAIN_MENU)
    input_state = context.user_data.get("input_state")
    logger.info(f"Text input received: user_id={user_id}, state={state}, input_state={input_state}, text={text}")

    try:
        if state == STATE_BUY_STARS_RECIPIENT:
            if not text.startswith("@"):
                await update.message.reply_text("Username должен начинаться с @!")
                return STATE_BUY_STARS_RECIPIENT
            buy_data = context.user_data.get("buy_data", {})
            buy_data["recipient"] = text
            context.user_data["buy_data"] = buy_data
            text = "Выберите следующее действие:"
            keyboard = [
                [InlineKeyboardButton(f"Кому звезды: {text}", callback_data="set_recipient")],
                [InlineKeyboardButton(f"Количество звезд: {buy_data.get('stars', '-')}", callback_data="set_amount")],
                [InlineKeyboardButton(f"Метод оплаты: {buy_data.get('payment_method', '-')}", callback_data="set_payment")],
                [InlineKeyboardButton(f"Цена: {buy_data.get('amount_ton', '-')}", callback_data="confirm_payment")],
                [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(text, reply_markup=reply_markup)
            return STATE_BUY_STARS_RECIPIENT

        elif state == STATE_BUY_STARS_AMOUNT:
            try:
                stars = int(text)
                min_stars = await get_setting("min_stars_purchase") or 10
                if stars < min_stars:
                    await update.message.reply_text(f"Введите количество звезд не менее {min_stars}!")
                    return STATE_BUY_STARS_AMOUNT
                buy_data = context.user_data.get("buy_data", {})
                buy_data["stars"] = stars
                context.user_data["buy_data"] = buy_data
                text = "Выберите следующее действие:"
                keyboard = [
                    [InlineKeyboardButton(f"Кому звезды: @{buy_data.get('recipient', '-')}", callback_data="set_recipient")],
                    [InlineKeyboardButton(f"Количество звезд: {stars}", callback_data="set_amount")],
                    [InlineKeyboardButton(f"Метод оплаты: {buy_data.get('payment_method', '-')}", callback_data="set_payment")],
                    [InlineKeyboardButton(f"Цена: {buy_data.get('amount_ton', '-')}", callback_data="confirm_payment")],
                    [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await update.message.reply_text(text, reply_markup=reply_markup)
                return STATE_BUY_STARS_AMOUNT
            except ValueError:
                await update.message.reply_text("Введите корректное количество звезд!")
                return STATE_BUY_STARS_AMOUNT

        elif state == STATE_EDIT_TEXT:
            text_key = context.user_data.get("text_key")
            await update_text(text_key, text)
            await log_admin_action(user_id, f"Edited text: {text_key}")
            await update.message.reply_text(f"Текст '{text_key}' обновлен!")
            context.user_data["state"] = STATE_ADMIN_EDIT_TEXTS
            return await admin_edit_texts(update, context)

        elif state == STATE_USER_SEARCH:
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
                    text = await get_text("user_info", selected_user_id, username=username, user_id=selected_user_id,
                                        stars_bought=stars_bought, ref_bonus_ton=ref_bonus_ton,
                                        ref_count=context.user_data["selected_user"]["ref_count"])
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
                    return STATE_USER_SEARCH

        elif state == STATE_EDIT_MARKUP_TYPE:
            markup_type = context.user_data.get("markup_type")
            try:
                markup = float(text)
                if markup < 0:
                    raise ValueError
                setting_key = markup_type
                await update_setting(setting_key, markup)
                await log_admin_action(user_id, f"Updated markup {markup_type} to {markup}%")
                await update.message.reply_text(f"Наценка '{markup_type}' обновлена: {markup}%")
                context.user_data.pop("markup_type", None)
                context.user_data["state"] = STATE_ADMIN_EDIT_MARKUP
                return await admin_edit_markup(update, context)
            except ValueError:
                await update.message.reply_text("Введите корректное число для наценки!")
                return STATE_EDIT_MARKUP_TYPE

        elif state == STATE_ADMIN_EDIT_PROFIT and input_state == "profit_percent":
            try:
                profit = float(text)
                if profit < 0:
                    raise ValueError
                await update_setting("profit_percent", profit)
                await log_admin_action(user_id, f"Updated profit to {profit}%")
                await update.message.reply_text(f"Прибыль обновлена: {profit}%")
                context.user_data.pop("input_state", None)
                context.user_data["state"] = STATE_ADMIN_PANEL
                return await admin_panel(update, context)
            except ValueError:
                await update.message.reply_text("Введите корректное число для прибыли!")
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
                else:
                    await update.message.reply_text("Этот пользователь уже админ!")
                context.user_data.pop("input_state", None)
                context.user_data["state"] = STATE_ADMIN_MANAGE_ADMINS
                return await admin_manage_admins(update, context)
            except Exception as e:
                logger.error(f"Error adding admin: {e}")
                await update.message.reply_text("Ошибка! Проверьте ID или username.")
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
                    await update.message.reply_text(f"Админ @{remove_admin_username} удален!")
                else:
                    await update.message.reply_text("Этот пользователь не является админом!")
                context.user_data.pop("input_state", None)
                context.user_data["state"] = STATE_ADMIN_MANAGE_ADMINS
                return await admin_manage_admins(update, context)
            except Exception as e:
                logger.error(f"Error removing admin: {e}")
                await update.message.reply_text("Ошибка! Проверьте ID или username.")
                return STATE_ADMIN_MANAGE_ADMINS

        elif state == STATE_EDIT_USER:
            field = context.user_data.get("edit_user_field")
            selected_user = context.user_data.get("selected_user")
            try:
                if field == EDIT_USER_STARS:
                    stars = int(text)
                    async with (await get_db_pool()) as conn:
                        await conn.execute(
                            "UPDATE users SET stars_bought = $1 WHERE user_id = $2",
                            stars, selected_user["user_id"]
                        )
                    await log_admin_action(user_id, f"Updated stars for @{selected_user['username']} to {stars}")
                    await update.message.reply_text(f"Звезды для @{selected_user['username']} обновлены: {stars}")
                elif field == EDIT_USER_REF_BONUS:
                    ref_bonus = float(text)
                    async with (await get_db_pool()) as conn:
                        await conn.execute(
                            "UPDATE users SET ref_bonus_ton = $1 WHERE user_id = $2",
                            ref_bonus, selected_user["user_id"]
                        )
                    await log_admin_action(user_id, f"Updated ref_bonus for @{selected_user['username']} to {ref_bonus}")
                    await update.message.reply_text(f"Реф. бонус для @{selected_user['username']} обновлен: {ref_bonus} TON")
                elif field == EDIT_USER_PURCHASES:
                    purchases = int(text)
                    async with (await get_db_pool()) as conn:
                        await conn.execute(
                            "UPDATE users SET stars_bought = $1 WHERE user_id = $2",
                            purchases, selected_user["user_id"]
                        )
                    await log_admin_action(user_id, f"Updated purchases for @{selected_user['username']} to {purchases}")
                    await update.message.reply_text(f"Покупки для @{selected_user['username']} обновлены: {purchases} звезд")
                context.user_data.pop("edit_user_field", None)
                context.user_data["state"] = STATE_ADMIN_USER_STATS
                return await admin_user_stats(update, context)
            except ValueError:
                await update.message.reply_text("Введите корректное значение!")
                return STATE_EDIT_USER

        context.user_data["state"] = STATE_MAIN_MENU
        return STATE_MAIN_MENU
    except Exception as e:
        logger.error(f"Error in handle_text_input for user_id={user_id}: {e}")
        await update.message.reply_text("Произошла ошибка.")
        context.user_data["state"] = STATE_MAIN_MENU
        return STATE_MAIN_MENU

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает нажатия кнопок."""
    query = update.callback_query
    data = query.data
    user_id = update.effective_user.id
    logger.info(f"Button pressed: user_id={user_id}, callback_data={data}")
    try:
        if data == BACK_TO_MENU:
            context.user_data["state"] = STATE_MAIN_MENU
            await query.answer()
            return await start(update, context)
        elif data == BACK_TO_ADMIN:
            context.user_data["state"] = STATE_ADMIN_PANEL
            await query.answer()
            return await admin_panel(update, context)
        elif data == PROFILE:
            context.user_data["state"] = STATE_PROFILE
            return await profile(update, context)
        elif data == REFERRALS:
            context.user_data["state"] = STATE_REFERRALS
            return await referrals(update, context)
        elif data == SUPPORT:
            context.user_data["state"] = STATE_MAIN_MENU
            return await support(update, context)
        elif data == REVIEWS:
            context.user_data["state"] = STATE_MAIN_MENU
            return await reviews(update, context)
        elif data == BUY_STARS:
            context.user_data["state"] = STATE_BUY_STARS_RECIPIENT
            return await buy_stars(update, context)
        elif data == ADMIN_PANEL:
            context.user_data["state"] = STATE_ADMIN_PANEL
            return await admin_panel(update, context)
        elif data == ADMIN_STATS:
            context.user_data["state"] = STATE_ADMIN_PANEL
            return await admin_stats(update, context)
        elif data == ADMIN_EDIT_TEXTS:
            context.user_data["state"] = STATE_ADMIN_EDIT_TEXTS
            return await admin_edit_texts(update, context)
        elif data == ADMIN_USER_STATS:
            context.user_data["state"] = STATE_USER_SEARCH
            return await admin_user_stats(update, context)
        elif data == ADMIN_EDIT_MARKUP:
            context.user_data["state"] = STATE_ADMIN_EDIT_MARKUP
            return await admin_edit_markup(update, context)
        elif data == ADMIN_MANAGE_ADMINS:
            context.user_data["state"] = STATE_ADMIN_MANAGE_ADMINS
            return await admin_manage_admins(update, context)
        elif data == ADMIN_EDIT_PROFIT:
            context.user_data["state"] = STATE_ADMIN_EDIT_PROFIT
            return await admin_edit_profit(update, context)
        elif data == CHECK_PAYMENT:
            context.user_data["state"] = STATE_BUY_STARS_CONFIRM
            return await check_payment(update, context)
        elif data == TOP_REFERRALS:
            context.user_data["state"] = STATE_PROFILE
            return await top_referrals(update, context)
        elif data == TOP_PURCHASES:
            context.user_data["state"] = STATE_PROFILE
            return await top_purchases(update, context)
        elif data.startswith("edit_text_"):
            context.user_data["state"] = STATE_EDIT_TEXT
            return await edit_text_prompt(update, context)
        elif data == ADD_ADMIN:
            text = "Введите ID или username нового админа:"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text, reply_markup=reply_markup)
            context.user_data["input_state"] = "add_admin"
            context.user_data["state"] = STATE_ADMIN_MANAGE_ADMINS
            await query.answer()
            return STATE_ADMIN_MANAGE_ADMINS
        elif data == REMOVE_ADMIN:
            text = "Введите ID или username админа для удаления:"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text, reply_markup=reply_markup)
            context.user_data["input_state"] = "remove_admin"
            context.user_data["state"] = STATE_ADMIN_MANAGE_ADMINS
            await query.answer()
            return STATE_ADMIN_MANAGE_ADMINS
        elif data == "set_recipient":
            context.user_data["state"] = STATE_BUY_STARS_RECIPIENT
            return await set_recipient(update, context)
        elif data == "set_amount":
            context.user_data["state"] = STATE_BUY_STARS_AMOUNT
            return await set_amount(update, context)
        elif data == "set_payment":
            context.user_data["state"] = STATE_BUY_STARS_PAYMENT_METHOD
            return await set_payment_method(update, context)
        elif data == "confirm_payment":
            context.user_data["state"] = STATE_BUY_STARS_CONFIRM
            return await confirm_payment(update, context)
        elif data == PAY_CRYPTO:
            buy_data = context.user_data.get("buy_data", {})
            buy_data["payment_method"] = "ton_space"
            stars = buy_data["stars"]
            base_price_usd = float(await get_setting("stars_price_usd") or 0.81) * (stars / 50)
            markup = float(await get_setting("markup_ton_space") or 10)
            commission = float(await get_setting("ton_space_commission") or 20) / 100
            amount_usd = base_price_usd * (1 + markup / 100) * (1 + commission)
            ton_price = float(await get_setting("ton_exchange_rate") or 2.93)
            amount_ton = amount_usd / ton_price
            buy_data["amount_ton"] = amount_ton
            buy_data["memo"] = f"order_{user_id}_{int(time.time())}"
            buy_data["address"] = OWNER_WALLET or "UQB_XcBjornHoP0aIf6ofn-wT8ru5QPsgYKtyPrlbgKsXrrX"
            context.user_data["buy_data"] = buy_data
            context.user_data["state"] = STATE_BUY_STARS_CONFIRM
            text = await get_text(
                "buy_prompt", user_id, amount_ton=amount_ton, stars=stars,
                recipient=buy_data["recipient"], address=buy_data["address"],
                memo=buy_data["memo"], method="TON Space"
            )
            keyboard = [
                [InlineKeyboardButton(f"Кому звезды: @{buy_data['recipient']}", callback_data="set_recipient")],
                [InlineKeyboardButton(f"Количество звезд: {stars}", callback_data="set_amount")],
                [InlineKeyboardButton("Метод оплаты: Крипта (TON)", callback_data="set_payment")],
                [InlineKeyboardButton(f"Цена: {amount_ton:.6f} TON", callback_data="confirm_payment")],
                [InlineKeyboardButton("Проверить оплату", callback_data=CHECK_PAYMENT)],
                [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text, reply_markup=reply_markup)
            await query.answer()
            return STATE_BUY_STARS_CONFIRM
        elif data == PAY_CARD:
            buy_data = context.user_data.get("buy_data", {})
            buy_data["payment_method"] = "cryptobot_card"
            stars = buy_data["stars"]
            base_price_usd = float(await get_setting("stars_price_usd") or 0.81) * (stars / 50)
            markup = float(await get_setting("markup_cryptobot_card") or 10)
            commission = float(await get_setting("card_commission") or 30) / 100
            amount_usd = base_price_usd * (1 + markup / 100) * (1 + commission)
            ton_price = float(await get_setting("ton_exchange_rate") or 2.93)
            amount_ton = amount_usd / ton_price
            buy_data["amount_usd"] = amount_usd
            buy_data["amount_ton"] = amount_ton
            invoice_id, pay_url = await create_cryptobot_invoice(amount_usd, "USD", user_id, stars, buy_data["recipient"])
            if invoice_id:
                buy_data["invoice_id"] = invoice_id
                buy_data["pay_url"] = pay_url
                context.user_data["buy_data"] = buy_data
                context.user_data["state"] = STATE_BUY_STARS_CONFIRM
                text = await get_text(
                    "buy_prompt", user_id, amount_ton=amount_ton, stars=stars,
                    recipient=buy_data["recipient"], address=pay_url,
                    memo="N/A", method="Карта (CryptoBot)"
                )
                keyboard = [
                    [InlineKeyboardButton(f"Кому звезды: @{buy_data['recipient']}", callback_data="set_recipient")],
                    [InlineKeyboardButton(f"Количество звезд: {stars}", callback_data="set_amount")],
                    [InlineKeyboardButton("Метод оплаты: Карта", callback_data="set_payment")],
                    [InlineKeyboardButton(f"Цена: ${amount_usd:.2f} ({amount_ton:.6f} TON)", callback_data="confirm_payment")],
                    [InlineKeyboardButton("Проверить оплату", callback_data=CHECK_PAYMENT)],
                    [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(text, reply_markup=reply_markup)
                await query.answer()
                return STATE_BUY_STARS_CONFIRM
            else:
                await query.edit_message_text("Ошибка при создании инвойса. Попробуйте позже.")
                context.user_data["state"] = STATE_BUY_STARS_PAYMENT_METHOD
                await query.answer()
                return STATE_BUY_STARS_PAYMENT_METHOD
        elif data in (MARKUP_TON_SPACE, MARKUP_CRYPTOBOT_CRYPTO, MARKUP_CRYPTOBOT_CARD, MARKUP_REF_BONUS):
            context.user_data["markup_type"] = data
            context.user_data["state"] = STATE_EDIT_MARKUP_TYPE
            text = f"Введите новый процент наценки для '{data}':"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text, reply_markup=reply_markup)
            await query.answer()
            return STATE_EDIT_MARKUP_TYPE
        elif data in (EDIT_USER_STARS, EDIT_USER_REF_BONUS, EDIT_USER_PURCHASES):
            context.user_data["edit_user_field"] = data
            context.user_data["state"] = STATE_EDIT_USER
            text = f"Введите новое значение для '{data}' пользователя @{context.user_data['selected_user']['username']}:"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=ADMIN_USER_STATS)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text, reply_markup=reply_markup)
            await query.answer()
            return STATE_EDIT_USER
        await query.answer()
        context.user_data["state"] = STATE_MAIN_MENU
        return STATE_MAIN_MENU
    except Exception as e:
        logger.error(f"Error in button_handler for user_id={user_id}, callback_data={data}: {e}")
        await query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        context.user_data["state"] = STATE_MAIN_MENU
        await query.answer()
        return STATE_MAIN_MENU

async def webhook_handler(request):
    """Обрабатывает входящие webhook-запросы."""
    app = request.app['application']
    try:
        data = await request.json()
        logger.info(f"Received webhook data: {data}")
        update = Update.de_json(data, app.bot)
        await app.process_update(update)
        logger.info("Webhook update processed successfully")
        return aiohttp.web.Response(status=200)
    except Exception as e:
        logger.error(f"Error processing webhook: {e}")
        return aiohttp.web.Response(status=500)

async def root_handler(request):
    """Обрабатывает корневой маршрут."""
    return aiohttp.web.Response(text="Stars Market Bot is running!")

async def main():
    """Запускает бот с webhook."""
    try:
        await init_db()
        app = ApplicationBuilder().token(BOT_TOKEN).build()
        conv_handler = ConversationHandler(
            entry_points=[
                CommandHandler("start", start),
                CommandHandler("cancel", start),
            ],
            states={
                STATE_MAIN_MENU: [
                    CallbackQueryHandler(button_handler, pattern=f"^{PROFILE}$|^{REFERRALS}$|^{SUPPORT}$|^{REVIEWS}$|^{BUY_STARS}$|^{ADMIN_PANEL}$|^{BACK_TO_MENU}$|^cancel$")
                ],
                STATE_BUY_STARS_RECIPIENT: [
                    CallbackQueryHandler(button_handler, pattern=f"^(set_recipient|{BACK_TO_MENU})$"),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                ],
                STATE_BUY_STARS_AMOUNT: [
                    CallbackQueryHandler(button_handler, pattern=f"^(set_amount|{BACK_TO_MENU})$"),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                ],
                STATE_BUY_STARS_PAYMENT_METHOD: [
                    CallbackQueryHandler(button_handler, pattern=f"^(set_payment|{PAY_CRYPTO}|{PAY_CARD}|{BACK_TO_MENU})$"),
                ],
                STATE_BUY_STARS_CONFIRM: [
                    CallbackQueryHandler(button_handler, pattern=f"^(confirm_payment|{CHECK_PAYMENT}|{BACK_TO_MENU})$"),
                ],
                STATE_ADMIN_PANEL: [
                    CallbackQueryHandler(button_handler, pattern=f"^{ADMIN_STATS}$|^{ADMIN_EDIT_TEXTS}$|^{ADMIN_USER_STATS}$|^{ADMIN_EDIT_MARKUP}$|^{ADMIN_MANAGE_ADMINS}$|^{ADMIN_EDIT_PROFIT}$|^{BACK_TO_MENU}$")
                ],
                STATE_ADMIN_STATS: [
                    CallbackQueryHandler(button_handler, pattern=f"^{BACK_TO_ADMIN}$")
                ],
                STATE_ADMIN_EDIT_TEXTS: [
                    CallbackQueryHandler(button_handler, pattern=f"^(edit_text_|{BACK_TO_ADMIN})$")
                ],
                STATE_EDIT_TEXT: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(button_handler, pattern=f"^{BACK_TO_ADMIN}$")
                ],
                STATE_USER_SEARCH: [
                    CallbackQueryHandler(button_handler, pattern=f"^{BACK_TO_ADMIN}$"),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                ],
                STATE_EDIT_USER: [
                    CallbackQueryHandler(button_handler, pattern=f"^{EDIT_USER_STARS}$|^{EDIT_USER_REF_BONUS}$|^{EDIT_USER_PURCHASES}$|^{ADMIN_USER_STATS}$"),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                ],
                STATE_ADMIN_EDIT_MARKUP: [
                    CallbackQueryHandler(button_handler, pattern=f"^{MARKUP_TON_SPACE}$|^{MARKUP_CRYPTOBOT_CRYPTO}$|^{MARKUP_CRYPTOBOT_CARD}$|^{MARKUP_REF_BONUS}$|^{BACK_TO_ADMIN}$"),
                ],
                STATE_EDIT_MARKUP_TYPE: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(button_handler, pattern=f"^{BACK_TO_ADMIN}$"),
                ],
                STATE_ADMIN_MANAGE_ADMINS: [
                    CallbackQueryHandler(button_handler, pattern=f"^{ADD_ADMIN}$|^{REMOVE_ADMIN}$|^{BACK_TO_ADMIN}$"),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                ],
                STATE_ADMIN_EDIT_PROFIT: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                    CallbackQueryHandler(button_handler, pattern=f"^{BACK_TO_ADMIN}$"),
                ],
                STATE_PROFILE: [
                    CallbackQueryHandler(button_handler, pattern=f"^{TOP_REFERRALS}$|^{TOP_PURCHASES}$|^{BACK_TO_MENU}$"),
                ],
                STATE_TOP_REFERRALS: [
                    CallbackQueryHandler(button_handler, pattern=f"^{PROFILE}$"),
                ],
                STATE_TOP_PURCHASES: [
                    CallbackQueryHandler(button_handler, pattern=f"^{PROFILE}$"),
                ],
                STATE_REFERRALS: [
                    CallbackQueryHandler(button_handler, pattern=f"^{BACK_TO_MENU}$"),
                ],
            },
            fallbacks=[
                CommandHandler("start", start),
                CommandHandler("cancel", start),
                CallbackQueryHandler(button_handler, pattern=r"^cancel|^" + BACK_TO_MENU + "$"),
            ],
            per_message=False,
        )
        app.add_handler(conv_handler)
        app.job_queue.run_repeating(update_ton_price, interval=900, first=10)  # Увеличен интервал до 15 минут
        logger.info("Bot application initialized")

        # Настройка webhook
        webhook_app = aiohttp.web.Application()
        webhook_app['application'] = app
        webhook_app['bot'] = app.bot
        webhook_app.router.add_post('/webhook', webhook_handler)
        webhook_app.router.add_get('/', root_handler)

        # Формирование webhook URL
        render_hostname = os.getenv("RENDER_EXTERNAL_HOSTNAME", "stars-ejwz.onrender.com")
        webhook_url = f"https://{render_hostname}/webhook"
        port = int(os.getenv("PORT", 8080))
        logger.info(f"Setting webhook URL: {webhook_url}")

        await app.initialize()
        try:
            await app.bot.set_webhook(webhook_url)
            logger.info(f"Webhook set successfully: {webhook_url}")
        except Exception as e:
            logger.error(f"Failed to set webhook: {e}")
            raise

        await app.start()

        # Запуск HTTP-сервера
        runner = aiohttp.web.AppRunner(webhook_app)
        await runner.setup()
        site = aiohttp.web.TCPSite(runner, '0.0.0.0', port)
        await site.start()
        logger.info(f"Webhook server started on port {port}")

        try:
            while True:
                await asyncio.sleep(3600)  # Держим сервер запущенным
        except KeyboardInterrupt:
            logger.info("Shutting down bot")
            await app.bot.delete_webhook()
            await app.stop()
            await runner.cleanup()
            await app.shutdown()
            if db_pool:
                await db_pool.close()
                logger.info("Database pool closed")
    except Exception as e:
        logger.error(f"Error in main: {e}")
        if db_pool:
            await db_pool.close()
        raise

if __name__ == "__main__":
    asyncio.run(main())
