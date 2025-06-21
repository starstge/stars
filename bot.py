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
    filters, ContextTypes, ConversationHandler
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
STATE_ADMIN_USER_STATS = 9
STATE_ADMIN_EDIT_MARKUP = 10
STATE_ADMIN_MANAGE_ADMINS = 11
STATE_ADMIN_EDIT_PROFIT = 12
STATE_PROFILE = 13
STATE_TOP_REFERRALS = 14
STATE_TOP_PURCHASES = 15
STATE_REFERRALS = 16
STATE_USER_SEARCH = 17
STATE_EDIT_USER = 18
STATE_EDIT_MARKUP_TYPE = 19

# Пул соединений
db_pool = None

async def get_db_pool():
    """Инициализирует и возвращает пул соединений с базой данных."""
    global db_pool
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
    try:
        async with (await get_db_pool()) as conn:
            # Создание таблиц
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT NOT NULL,
                    stars_bought INTEGER DEFAULT 0,
                    ref_bonus_ton FLOAT DEFAULT 0.0,
                    referrer_id BIGINT,
                    referrals JSONB DEFAULT '[]',
                    bonus_history JSONB DEFAULT '[]',
                    address TEXT,
                    memo TEXT,
                    amount_ton FLOAT,
                    cryptobot_invoice_id TEXT,
                    language TEXT DEFAULT 'ru'
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS admin_log (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    action TEXT NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS texts (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
            """)
            # Вставка данных в settings
            await conn.execute("""
                INSERT INTO settings (key, value)
                VALUES
                    ('admin_ids', '[6956377285]'),
                    ('ref_bonus_percent', '30'),
                    ('profit_percent', '20'),
                    ('total_stars_sold', '0'),
                    ('total_profit_usd', '0'),
                    ('total_profit_ton', '0'),
                    ('stars_price_usd', '0.81'),
                    ('stars_per_purchase', '50'),
                    ('ton_exchange_rate', '2.93'),
                    ('review_channel', '@sacoectasy'),
                    ('support_channel', '@support_channel'),
                    ('cryptobot_commission', '25'),
                    ('ton_space_commission', '20'),
                    ('card_commission', '30'),
                    ('card_payment_enabled', 'true'),
                    ('min_stars_purchase', '10'),
                    ('markup_ton_space', '10'),
                    ('markup_cryptobot_crypto', '10'),
                    ('markup_cryptobot_card', '10'),
                    ('markup_ref_bonus', '30')
                ON CONFLICT (key) DO NOTHING;
            """)
            # Вставка данных в texts
            await conn.execute("""
                INSERT INTO texts (key, value)
                VALUES
                    ('welcome', '🌟 Добро пожаловать! Купите Telegram Stars за TON.\nЗвезд продано: {{total_stars_sold}}'),
                    ('buy_prompt', '💸 Оплатите {{amount_ton:.6f}} TON\nЗвёзд: {{stars}}\nПолучатель: @{{recipient}}\nАдрес: {{address}}\nМемо: {{memo}}\nМетод: {{method}}'),
                    ('buy_success', '🎉 Оплата прошла! @{{recipient}} получил {{stars}} звёзд!'),
                    ('profile', '👤 Профиль\nИмя: @{{username}}\nКуплено звезд: {{stars_bought}}\nРеф. бонус: {{ref_bonus_ton:.6f}} TON'),
                    ('referrals', '🤝 Реферальная система\nРефералов: {{ref_count}}\nРеф. бонус: {{ref_bonus_ton:.6f}} TON\nСсылка: {{ref_link}}'),
                    ('tech_support', '🛠 Свяжитесь с поддержкой: {{support_channel}}'),
                    ('reviews', '📝 Отзывы: {{review_channel}}'),
                    ('admin_panel', '🛠 Админ-панель'),
                    ('stats', '📊 Статистика\nПрибыль: {{total_profit_ton:.6f}} TON\nЗвёзд продано: {{total_stars_sold}}\nПользователей: {{user_count}}'),
                    ('edit_text_menu', '📝 Изменить текст'),
                    ('user_stats', '👤 Статистика пользователей\nВведите ID или username для поиска (или /cancel):'),
                    ('user_info', '👤 @{{username}}\nЗвёзд куплено: {{stars_bought}}\nРеф. бонус: {{ref_bonus_ton:.6f}} TON\nРефералов: {{ref_count}}'),
                    ('edit_markup', '💸 Изменить наценку'),
                    ('manage_admins', '👑 Управление админами'),
                    ('edit_profit', '📈 Изменить прибыль (%)'),
                    ('back_btn', '🔙 Назад'),
                    ('cancel_btn', '❌ Отмена')
                ON CONFLICT (key) DO NOTHING;
            """)
            logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

async def get_setting(key):
    """Получает значение настройки из базы данных."""
    try:
        async with (await get_db_pool()) as conn:
            value = await conn.fetchval("SELECT value FROM settings WHERE key = $1", key)
            if value is not None:
                if key in ('admin_ids', 'referrals', 'bonus_history'):
                    return json.loads(value)
                if key == 'card_payment_enabled':
                    return value.lower() == 'true'
                if key in ('ref_bonus_percent', 'profit_percent', 'stars_price_usd',
                           'ton_exchange_rate', 'cryptobot_commission', 'ton_space_commission',
                           'card_commission', 'min_stars_purchase', 'markup_ton_space',
                           'markup_cryptobot_crypto', 'markup_cryptobot_card', 'markup_ref_bonus',
                           'total_stars_sold', 'total_profit_usd', 'total_profit_ton'):
                    return float(value)
                return value
    except Exception as e:
        logger.error(f"Error getting setting {key}: {e}")
    return None

async def update_setting(key, value):
    """Обновляет настройку в базе данных."""
    try:
        async with (await get_db_pool()) as conn:
            await conn.execute(
                "INSERT INTO settings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $3",
                key, json.dumps(value) if isinstance(value, (list, dict)) else str(value),
                json.dumps(value) if isinstance(value, (list, dict)) else str(value)
            )
    except Exception as e:
        logger.error(f"Error updating setting {key}: {e}")

async def get_text(key, user_id, **kwargs):
    """Получает текст из таблицы texts."""
    try:
        async with (await get_db_pool()) as conn:
            text = await conn.fetchval("SELECT value FROM texts WHERE key = $1", key)
            return text.format(**kwargs) if text else f"Текст не найден: {key}"
    except Exception as e:
        logger.error(f"Error getting text {key} for user_id={user_id}: {e}")
        return f"Ошибка получения текста: {key}"

async def update_text(key, value):
    """Обновляет текст в таблице texts."""
    try:
        async with (await get_db_pool()) as conn:
            await conn.execute(
                "INSERT INTO texts (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = $3",
                key, value, value
            )
    except Exception as e:
        logger.error(f"Error updating text {key}: {e}")

async def is_admin(user_id):
    """Проверяет, является ли пользователь админом."""
    admin_ids = await get_setting("admin_ids") or [6956377285]
    return user_id in admin_ids

async def log_admin_action(admin_id, action):
    """Логирует действие админа."""
    try:
        async with (await get_db_pool()) as conn:
            await conn.execute(
                "INSERT INTO admin_log (user_id, action) VALUES ($1, $2)",
                admin_id, action
            )
    except Exception as e:
        logger.error(f"Error logging admin action for user_id={admin_id}: {e}")

async def update_ton_price(context: ContextTypes.DEFAULT_TYPE):
    """Обновляет курс TON."""
    try:
        async with aiohttp.ClientSession() as session:
            for attempt in range(3):
                try:
                    async with session.get("https://api.coingecko.com/api/v3/simple/price?ids=toncoin&vs_currencies=usd") as response:
                        if response.status == 200:
                            data = await response.json()
                            ton_price = data.get("toncoin", {}).get("usd", 2.93)
                            await update_setting("ton_exchange_rate", ton_price)
                            logger.info(f"Updated TON price: ${ton_price}")
                            return
                        elif response.status == 429:
                            await asyncio.sleep(2 ** attempt)
                except Exception as e:
                    logger.error(f"TON price update failed on attempt {attempt + 1}: {e}")
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)
    except Exception as e:
        logger.error(f"TON price update failed: {e}")

async def check_ton_payment(address, memo, amount_ton):
    """Проверяет оплату TON через TON API."""
    headers = {"Authorization": f"Bearer {TON_API_KEY}"}
    async with aiohttp.ClientSession() as session:
        for attempt in range(3):
            try:
                async with session.get(f"https://tonapi.io/v2/transactions?address={address}", headers=headers) as response:
                    if response.status == 200:
                        transactions = await response.json()
                        for tx in transactions.get("transactions", []):
                            if tx.get("memo") == memo and float(tx.get("amount", 0)) / 1e9 >= amount_ton:
                                return True
                        return False
                    elif response.status == 429:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        logger.error(f"TON API failed: {response.status}")
                        return False
            except Exception as e:
                logger.error(f"TON payment check error: {e}")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
        return False

async def create_cryptobot_invoice(amount_usd, currency, user_id, stars, recipient):
    """Создает инвойс в CryptoBot для оплаты фиатом или криптой."""
    headers = {"Crypto-Pay-API-Token": CRYPTOBOT_API_TOKEN}
    payload = {
        "amount": amount_usd,
        "currency": currency,  # "USD" для фиата, "TON" или "USDT" для крипты
        "description": f"Покупка {stars} звезд для @{recipient}",
        "payload": json.dumps({"user_id": user_id, "stars": stars, "recipient": recipient})
    }
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(f"{CRYPTOBOT_API_URL}/createInvoice", headers=headers, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("result", {}).get("invoice_id"), data.get("result", {}).get("pay_url")
                logger.error(f"CryptoBot API failed: {response.status} {await response.text()}")
                return None, None
        except Exception as e:
            logger.error(f"Error creating CryptoBot invoice: {e}")
            return None, None

async def check_cryptobot_payment(invoice_id):
    """Проверяет статус оплаты инвойса в CryptoBot."""
    headers = {"Crypto-Pay-API-Token": CRYPTOBOT_API_TOKEN}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{CRYPTOBOT_API_URL}/getInvoices?invoice_ids={invoice_id}", headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    invoice = data.get("result", {}).get("items", [{}])[0]
                    return invoice.get("status") == "paid"
                logger.error(f"CryptoBot API check failed: {response.status}")
                return False
        except Exception as e:
            logger.error(f"Error checking CryptoBot payment: {e}")
            return False

async def issue_stars(recipient_id, stars):
    """Выдает звезды через Split API."""
    headers = {"Authorization": f"Bearer {SPLIT_API_TOKEN}"}
    payload = {
        "user_id": recipient_id,
        "stars": stars
    }
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(SPLIT_API_URL, headers=headers, json=payload) as response:
                if response.status == 200:
                    logger.info(f"Stars issued: {stars} to user_id={recipient_id}")
                    return True
                logger.error(f"Split API failed: {response.status} {await response.text()}")
                return False
        except Exception as e:
            logger.error(f"Error issuing stars via Split API: {e}")
            return False

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start и возврата в главное меню."""
    user_id = update.effective_user.id
    username = update.effective_user.username or f"user_{user_id}"
    args = context.args
    ref_id = int(args[0].split('_')[-1]) if args and args[0].startswith('ref_') else None
    logger.info(f"/start command received: user_id={user_id}, username={username}, ref_id={ref_id}")

    try:
        async with (await get_db_pool()) as conn:
            # Проверяем, новый ли пользователь
            result = await conn.fetchrow(
                """
                INSERT INTO users (user_id, username, referrer_id, language)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (user_id) DO UPDATE SET username = $5
                RETURNING (xmax = 0) AS is_new
                """,
                user_id, username, ref_id if ref_id else None, 'ru', username
            )
            is_new_user = result['is_new'] if result else False

            if is_new_user and ref_id:
                await conn.execute(
                    """
                    UPDATE users
                    SET referrals = referrals || $1
                    WHERE user_id = $2
                    """,
                    json.dumps({"user_id": user_id, "username": username}), ref_id
                )

        # Отправка уведомления в канал только для новых пользователей
        if is_new_user:
            channel_id = "-1002703640431"  # Проверь, что это правильный chat_id
            join_time = datetime.now(pytz.timezone('Europe/Kiev')).strftime('%Y-%m-%d %H:%M:%S %Z')
            channel_message = f"Новый пользователь: @{username}\nВремя: {join_time}"
            try:
                await context.bot.send_message(chat_id=channel_id, text=channel_message)
                logger.info(f"Sent new user notification to channel: {channel_message}")
            except Exception as e:
                logger.error(f"Failed to send channel message: {e}")

        # Формирование меню
        keyboard = [
            [
                InlineKeyboardButton("👤 Профиль", callback_data=PROFILE),
                InlineKeyboardButton("🤝 Рефералы", callback_data=REFERRALS)
            ],
            [
                InlineKeyboardButton("🛠 Поддержка", callback_data=SUPPORT),
                InlineKeyboardButton("📝 Отзывы", callback_data=REVIEWS)
            ],
            [InlineKeyboardButton("⭐ Купить звёзды", callback_data=BUY_STARS)],
        ]
        if await is_admin(user_id):
            keyboard.append([InlineKeyboardButton("🔧 Админ-панель", callback_data=ADMIN_PANEL)])
        reply_markup = InlineKeyboardMarkup(keyboard)
        text = await get_text("welcome", user_id, total_stars_sold=(await get_setting("total_stars_sold")) or 0)
        logger.info(f"Sending welcome message to user_id={user_id}: {text}")
        if update.callback_query:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        else:
            await update.message.reply_text(text, reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"Error in start handler for user_id={user_id}: {e}")
        await update.message.reply_text("Произошла ошибка, попробуйте позже.")
    return STATE_MAIN_MENU

async def admin_panel(update, context):
    """Отображает админ-панель."""
    user_id = update.effective_user.id
    if not await is_admin(user_id):
        await update.callback_query.answer("Доступ запрещен!")
        return STATE_MAIN_MENU
    keyboard = [
        [InlineKeyboardButton("📊 Статистика", callback_data=ADMIN_STATS)],
        [InlineKeyboardButton("📝 Изменить тексты", callback_data=ADMIN_EDIT_TEXTS)],
        [InlineKeyboardButton("👥 Статистика пользователей", callback_data=ADMIN_USER_STATS)],
        [InlineKeyboardButton("💸 Изменить наценку", callback_data=ADMIN_EDIT_MARKUP)],
        [InlineKeyboardButton("👤 Добавить/снять админа", callback_data=ADMIN_MANAGE_ADMINS)],
        [InlineKeyboardButton("💰 Изменить прибыль", callback_data=ADMIN_EDIT_PROFIT)],
        [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    text = await get_text("admin_panel", user_id)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    return STATE_ADMIN_PANEL

async def admin_stats(update, context):
    """Показывает статистику бота."""
    try:
        async with (await get_db_pool()) as conn:
            total_profit_ton = float(await conn.fetchval("SELECT value FROM settings WHERE key = 'total_profit_ton'") or 0)
            total_stars_sold = int(await conn.fetchval("SELECT value FROM settings WHERE key = 'total_stars_sold'") or 0)
            user_count = await conn.fetchval("SELECT COUNT(*) FROM users")
        text = await get_text("stats", update.effective_user.id, total_profit_ton=total_profit_ton,
                              total_stars_sold=total_stars_sold, user_count=user_count)
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        return STATE_ADMIN_PANEL
    except Exception as e:
        logger.error(f"Error in admin_stats: {e}")
        await update.callback_query.edit_message_text("Ошибка при получении статистики.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]))
        return STATE_ADMIN_PANEL

async def admin_edit_texts(update, context):
    """Подменю для изменения текстов."""
    keyboard = [
        [InlineKeyboardButton("Приветствие", callback_data=EDIT_TEXT_WELCOME)],
        [InlineKeyboardButton("Покупка", callback_data=EDIT_TEXT_BUY_PROMPT)],
        [InlineKeyboardButton("Успешная покупка", callback_data=EDIT_TEXT_BUY_SUCCESS)],
        [InlineKeyboardButton("Профиль", callback_data=EDIT_TEXT_PROFILE)],
        [InlineKeyboardButton("Рефералы", callback_data=EDIT_TEXT_REFERRALS)],
        [InlineKeyboardButton("Поддержка", callback_data=EDIT_TEXT_TECH_SUPPORT)],
        [InlineKeyboardButton("Отзывы", callback_data=EDIT_TEXT_REVIEWS)],
        [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    text = await get_text("edit_text_menu", update.effective_user.id)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    return STATE_ADMIN_EDIT_TEXTS

async def edit_text_prompt(update, context):
    """Запрашивает новый текст для редактирования."""
    query = update.callback_query
    text_key = query.data.replace("edit_text_", "")
    context.user_data["text_key"] = text_key
    text = f"Введите новый текст для '{text_key}':"
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(text, reply_markup=reply_markup)
    return STATE_EDIT_TEXT

async def admin_user_stats(update, context):
    """Показывает список пользователей и поиск."""
    try:
        async with (await get_db_pool()) as conn:
            users = await conn.fetch("SELECT user_id, username FROM users ORDER BY username LIMIT 50")
        text = "👤 Список пользователей (до 50):\n"
        for user in users:
            text += f"@{user['username']} (ID: {user['user_id']})\n"
        text += "\nВведите ID или username для поиска (или /cancel):"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        return STATE_USER_SEARCH
    except Exception as e:
        logger.error(f"Error in admin_user_stats: {e}")
        await update.callback_query.edit_message_text("Ошибка при загрузке пользователей.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]))
        return STATE_USER_SEARCH

async def admin_edit_markup(update, context):
    """Запрашивает тип наценки."""
    text = await get_text("edit_markup", update.effective_user.id)
    keyboard = [
        [InlineKeyboardButton("TON Space", callback_data=MARKUP_TON_SPACE)],
        [InlineKeyboardButton("CryptoBot (крипта)", callback_data=MARKUP_CRYPTOBOT_CRYPTO)],
        [InlineKeyboardButton("CryptoBot (карта)", callback_data=MARKUP_CRYPTOBOT_CARD)],
        [InlineKeyboardButton("Реф. бонус", callback_data=MARKUP_REF_BONUS)],
        [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    return STATE_ADMIN_EDIT_MARKUP

async def admin_manage_admins(update, context):
    """Меню управления админами."""
    keyboard = [
        [InlineKeyboardButton("Добавить админа", callback_data=ADD_ADMIN)],
        [InlineKeyboardButton("Снять админа", callback_data=REMOVE_ADMIN)],
        [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    text = await get_text("manage_admins", update.effective_user.id)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    return STATE_ADMIN_MANAGE_ADMINS

async def admin_edit_profit(update, context):
    """Запрашивает новую прибыль."""
    text = await get_text("edit_profit", update.effective_user.id)
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    context.user_data["input_state"] = "profit_percent"
    return STATE_ADMIN_EDIT_PROFIT

async def profile(update, context):
    """Показывает профиль пользователя."""
    user_id = update.effective_user.id
    try:
        async with (await get_db_pool()) as conn:
            result = await conn.fetchrow(
                "SELECT username, stars_bought, ref_bonus_ton FROM users WHERE user_id = $1",
                user_id
            )
            username = result['username'] if result else "Неизвестно"
            stars_bought = result['stars_bought'] if result else 0
            ref_bonus_ton = float(result['ref_bonus_ton']) if result else 0
        text = await get_text("profile", user_id, username=username, stars_bought=stars_bought, ref_bonus_ton=ref_bonus_ton)
        keyboard = [
            [InlineKeyboardButton("🏆 Топ рефералов", callback_data=TOP_REFERRALS)],
            [InlineKeyboardButton("⭐ Топ покупок", callback_data=TOP_PURCHASES)],
            [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        return STATE_PROFILE
    except Exception as e:
        logger.error(f"Error in profile for user_id={user_id}: {e}")
        await update.callback_query.edit_message_text("Ошибка при загрузке профиля.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_PROFILE

async def top_referrals(update, context):
    """Показывает топ-10 пользователей по рефералам."""
    try:
        async with (await get_db_pool()) as conn:
            results = await conn.fetch(
                "SELECT username, referrals FROM users WHERE referrals != '[]' ORDER BY jsonb_array_length(referrals) DESC LIMIT 10"
            )
        text = "🏆 Топ-10 рефералов:\n"
        for i, row in enumerate(results, 1):
            ref_count = len(json.loads(row['referrals']))
            text += f"{i}. @{row['username']}: {ref_count} рефералов\n"
        if not results:
            text += "Нет рефералов."
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=PROFILE)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        return STATE_PROFILE
    except Exception as e:
        logger.error(f"Error in top_referrals: {e}")
        await update.callback_query.edit_message_text("Ошибка при загрузке топа.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=PROFILE)]]))
        return STATE_PROFILE

async def top_purchases(update, context):
    """Показывает топ-10 пользователей по покупкам звезд."""
    try:
        async with (await get_db_pool()) as conn:
            results = await conn.fetch(
                "SELECT username, stars_bought FROM users WHERE stars_bought > 0 ORDER BY stars_bought DESC LIMIT 10"
            )
        text = "⭐ Топ-10 покупок:\n"
        for i, row in enumerate(results, 1):
            text += f"{i}. @{row['username']}: {row['stars_bought']} звезд\n"
        if not results:
            text += "Нет покупок."
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=PROFILE)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        return STATE_PROFILE
    except Exception as e:
        logger.error(f"Error in top_purchases: {e}")
        await update.callback_query.edit_message_text("Ошибка при загрузке топа.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=PROFILE)]]))
        return STATE_PROFILE

async def referrals(update, context):
    """Показывает реферальную информацию."""
    user_id = update.effective_user.id
    try:
        async with (await get_db_pool()) as conn:
            result = await conn.fetchrow(
                "SELECT referrals, ref_bonus_ton FROM users WHERE user_id = $1",
                user_id
            )
            ref_count = len(json.loads(result['referrals'])) if result and result['referrals'] != '[]' else 0
            ref_bonus_ton = float(result['ref_bonus_ton']) if result else 0
        bot_username = (await context.bot.get_me()).username
        ref_link = f"t.me/{bot_username}?start=ref_{user_id}"
        text = await get_text("referrals", user_id, ref_count=ref_count, ref_bonus_ton=ref_bonus_ton, ref_link=ref_link)
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        return STATE_REFERRALS
    except Exception as e:
        logger.error(f"Error in referrals for user_id={user_id}: {e}")
        await update.callback_query.edit_message_text("Ошибка при загрузке рефералов.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_REFERRALS

async def support(update, context):
    """Показывает информацию о техподдержке."""
    user_id = update.effective_user.id
    try:
        text = await get_text("tech_support", user_id, support_channel=await get_setting("support_channel"))
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        return STATE_MAIN_MENU
    except Exception as e:
        logger.error(f"Error in support for user_id={user_id}: {e}")
        await update.callback_query.edit_message_text("Ошибка при загрузке техподдержки.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def reviews(update, context):
    """Показывает информацию об отзывах."""
    user_id = update.effective_user.id
    try:
        text = await get_text("reviews", user_id, review_channel=await get_setting("review_channel"))
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        return STATE_MAIN_MENU
    except Exception as e:
        logger.error(f"Error in reviews for user_id={user_id}: {e}")
        await update.callback_query.edit_message_text("Ошибка при загрузке отзывов.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def buy_stars(update, context):
    """Начинает процесс покупки звезд."""
    user_id = update.effective_user.id
    context.user_data["buy_data"] = {}
    text = "Введите username получателя звезд (например, @username):"
    keyboard = [
        [InlineKeyboardButton("Кому звезды: -", callback_data="set_recipient")],
        [InlineKeyboardButton("Количество звезд: -", callback_data="set_amount")],
        [InlineKeyboardButton("Метод оплаты: -", callback_data="set_payment")],
        [InlineKeyboardButton("Цена: -", callback_data="confirm_payment")],
        [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    return STATE_BUY_STARS_RECIPIENT

async def set_recipient(update, context):
    """Запрашивает username получателя."""
    text = "Введите username получателя звезд (например, @username):"
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    return STATE_BUY_STARS_RECIPIENT

async def set_amount(update, context):
    """Запрашивает количество звезд."""
    if not context.user_data.get("buy_data", {}).get("recipient"):
        await update.callback_query.answer("Сначала выберите получателя!")
        return STATE_BUY_STARS_RECIPIENT
    text = "Введите количество звезд для покупки:"
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    return STATE_BUY_STARS_AMOUNT

async def set_payment_method(update, context):
    """Запрашивает метод оплаты."""
    buy_data = context.user_data.get("buy_data", {})
    if not buy_data.get("recipient"):
        await update.callback_query.answer("Сначала выберите получателя!")
        return STATE_BUY_STARS_RECIPIENT
    if not buy_data.get("stars"):
        await update.callback_query.answer("Сначала выберите количество звезд!")
        return STATE_BUY_STARS_AMOUNT
    text = "Выберите метод оплаты:"
    keyboard = [
        [InlineKeyboardButton("Крипта (TON)", callback_data=PAY_CRYPTO)],
        [InlineKeyboardButton("Карта (фиат)", callback_data=PAY_CARD)],
        [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    return STATE_BUY_STARS_PAYMENT_METHOD

async def confirm_payment(update, context):
    """Показывает финальное подтверждение и проверяет оплату."""
    buy_data = context.user_data.get("buy_data", {})
    if not buy_data.get("recipient"):
        await update.callback_query.answer("Сначала выберите получателя!")
        return STATE_BUY_STARS_RECIPIENT
    if not buy_data.get("stars"):
        await update.callback_query.answer("Сначала выберите количество звезд!")
        return STATE_BUY_STARS_AMOUNT
    if not buy_data.get("payment_method"):
        await update.callback_query.answer("Сначала выберите метод оплаты!")
        return STATE_BUY_STARS_PAYMENT_METHOD
    await update.callback_query.answer("Нажмите 'Проверить оплату' для проверки!")
    return STATE_BUY_STARS_CONFIRM

async def check_payment(update, context):
    """Проверяет оплату и выдает звезды."""
    user_id = update.effective_user.id
    buy_data = context.user_data.get("buy_data", {})
    recipient = buy_data.get("recipient")
    stars = buy_data.get("stars")
    payment_method = buy_data.get("payment_method")
    try:
        payment_confirmed = False
        if payment_method == "ton_space":
            payment_confirmed = await check_ton_payment(buy_data["address"], buy_data["memo"], buy_data["amount_ton"])
        elif payment_method in ("cryptobot_ton", "cryptobot_card"):
            payment_confirmed = await check_cryptobot_payment(buy_data["invoice_id"])

        if payment_confirmed:
            # Выдача звезд через Split API
            recipient_id = await context.bot.get_chat_id(f"@{recipient}")
            if await issue_stars(recipient_id, stars):
                async with (await get_db_pool()) as conn:
                    await conn.execute(
                        "UPDATE users SET stars_bought = stars_bought + $1 WHERE username = $2",
                        stars, recipient.lstrip("@")
                    )
                    await conn.execute(
                        "UPDATE settings SET value = value::integer + $1 WHERE key = 'total_stars_sold'",
                        stars
                    )
                    profit_ton = float(buy_data["amount_ton"] * (await get_setting("profit_percent") or 20) / 100)
                    await conn.execute(
                        "UPDATE settings SET value = value::float + $1 WHERE key = 'total_profit_ton'",
                        profit_ton
                    )
                    referrer_id = await conn.fetchval("SELECT referrer_id FROM users WHERE username = $1", recipient.lstrip("@"))
                    if referrer_id:
                        ref_bonus = float(buy_data["amount_ton"] * (await get_setting("markup_ref_bonus") or 30) / 100)
                        await conn.execute(
                            "UPDATE users SET ref_bonus_ton = ref_bonus_ton + $1 WHERE user_id = $2",
                            ref_bonus, referrer_id
                        )
                text = await get_text("buy_success", user_id, recipient=recipient, stars=stars)
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
                context.user_data.pop("buy_data", None)
                return STATE_MAIN_MENU
            else:
                text = "Ошибка при выдаче звезд. Свяжитесь с поддержкой."
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
                return STATE_BUY_STARS_CONFIRM
        else:
            text = "Оплата не подтверждена. Попробуйте снова."
            keyboard = [
                [InlineKeyboardButton("Проверить оплату", callback_data=CHECK_PAYMENT)],
                [InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
            return STATE_BUY_STARS_CONFIRM
    except Exception as e:
        logger.error(f"Error in check_payment for user_id={user_id}: {e}")
        await update.callback_query.edit_message_text("Ошибка при проверке оплаты.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_BUY_STARS_CONFIRM

async def button_handler(update, context):
    """Обрабатывает нажатия кнопок."""
    query = update.callback_query
    data = query.data
    user_id = update.effective_user.id
    logger.info(f"Button pressed: user_id={user_id}, callback_data={data}")
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
        elif data == CHECK_PAYMENT:
            return await check_payment(update, context)
        elif data == TOP_REFERRALS:
            return await top_referrals(update, context)
        elif data == TOP_PURCHASES:
            return await top_purchases(update, context)
        elif data.startswith("edit_text_"):
            return await edit_text_prompt(update, context)
        elif data == ADD_ADMIN:
            text = "Введите ID или username нового админа:"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text, reply_markup=reply_markup)
            context.user_data["input_state"] = "add_admin"
            return STATE_ADMIN_MANAGE_ADMINS
        elif data == REMOVE_ADMIN:
            text = "Введите ID или username админа для удаления:"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text, reply_markup=reply_markup)
            context.user_data["input_state"] = "remove_admin"
            return STATE_ADMIN_MANAGE_ADMINS
        elif data == "set_recipient":
            return await set_recipient(update, context)
        elif data == "set_amount":
            return await set_amount(update, context)
        elif data == "set_payment":
            return await set_payment_method(update, context)
        elif data == "confirm_payment":
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
            buy_data["amount_ton"] = amount_ton
            invoice_id, pay_url = await create_cryptobot_invoice(amount_usd, "USD", user_id, stars, buy_data["recipient"])
            if invoice_id:
                buy_data["invoice_id"] = invoice_id
                buy_data["pay_url"] = pay_url
                context.user_data["buy_data"] = buy_data
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
                return STATE_BUY_STARS_CONFIRM
            else:
                await query.edit_message_text("Ошибка при создании инвойса. Попробуйте позже.")
                return STATE_BUY_STARS_PAYMENT_METHOD
        elif data in (MARKUP_TON_SPACE, MARKUP_CRYPTOBOT_CRYPTO, MARKUP_CRYPTOBOT_CARD, MARKUP_REF_BONUS):
            context.user_data["markup_type"] = data
            text = f"Введите новый процент наценки для '{data}':"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_ADMIN)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text, reply_markup=reply_markup)
            return STATE_EDIT_MARKUP_TYPE
        elif data in (EDIT_USER_STARS, EDIT_USER_REF_BONUS, EDIT_USER_PURCHASES):
            context.user_data["edit_user_field"] = data
            text = f"Введите новое значение для '{data}' пользователя @{context.user_data['selected_user']['username']}:"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=ADMIN_USER_STATS)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text, reply_markup=reply_markup)
            return STATE_EDIT_USER
        await query.answer()
        return STATE_MAIN_MENU
    except Exception as e:
        logger.error(f"Error in button_handler for user_id={user_id}, callback_data={data}: {e}")
        await query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_MAIN_MENU

async def handle_text_input(update, context):
    """Обрабатывает текстовый ввод."""
    user_id = update.effective_user.id
    text = update.message.text.strip()
    state = update.current_state or context.user_data.get("state", STATE_MAIN_MENU)
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
                [InlineKeyboardButton(f"Кому звезды: @{text}", callback_data="set_recipient")],
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
                    user_id, username, stars_bought, ref_bonus_ton, referrals = result
                    ref_count = len(json.loads(referrals)) if referrals != '[]' else 0
                    context.user_data["selected_user"] = {
                        "user_id": user_id,
                        "username": username,
                        "stars_bought": stars_bought,
                        "ref_bonus_ton": ref_bonus_ton,
                        "ref_count": ref_count
                    }
                    text = await get_text("user_info", user_id, username=username, stars_bought=stars_bought,
                                        ref_bonus_ton=ref_bonus_ton, ref_count=ref_count)
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
                setting_key = markup_type  # Например, markup_ton_space
                await update_setting(setting_key, markup)
                await log_admin_action(user_id, f"Updated markup {markup_type} to {markup}%")
                await update.message.reply_text(f"Наценка '{markup_type}' обновлена: {markup}%")
                context.user_data.pop("markup_type", None)
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
                return await admin_panel(update, context)
            except ValueError:
                await update.message.reply_text("Введите корректное число для прибыли!")
                return STATE_ADMIN_EDIT_PROFIT

        elif state == STATE_ADMIN_MANAGE_ADMINS and input_state == "add_admin":
            try:
                if text.isdigit():
                    new_admin_id = int(text)
                    new_admin_username = await context.bot.get_chat(new_admin_id).username
                else:
                    new_admin_username = text.lstrip("@")
                    new_admin_id = await context.bot.get_chat(f"@{new_admin_username}").id
                admin_ids = await get_setting("admin_ids") or [6956377285]
                if new_admin_id not in admin_ids:
                    admin_ids.append(new_admin_id)
                    await update_setting("admin_ids", admin_ids)
                    await log_admin_action(user_id, f"Added admin: {new_admin_id} (@{new_admin_username})")
                    await update.message.reply_text(f"Админ @{new_admin_username} добавлен!")
                else:
                    await update.message.reply_text("Этот пользователь уже админ!")
                context.user_data.pop("input_state", None)
                return await admin_manage_admins(update, context)
            except Exception as e:
                logger.error(f"Error adding admin: {e}")
                await update.message.reply_text("Ошибка! Проверьте ID или username.")
                return STATE_ADMIN_MANAGE_ADMINS

        elif state == STATE_ADMIN_MANAGE_ADMINS and input_state == "remove_admin":
            try:
                if text.isdigit():
                    remove_admin_id = int(text)
                    remove_admin_username = await context.bot.get_chat(remove_admin_id).username
                else:
                    remove_admin_username = text.lstrip("@")
                    remove_admin_id = await context.bot.get_chat(f"@{remove_admin_username}").id
                admin_ids = await get_setting("admin_ids") or [6956377285]
                if remove_admin_id in admin_ids:
                    admin_ids.remove(remove_admin_id)
                    await update_setting("admin_ids", admin_ids)
                    await log_admin_action(user_id, f"Removed admin: {remove_admin_id} (@{remove_admin_username})")
                    await update.message.reply_text(f"Админ @{remove_admin_username} удален!")
                else:
                    await update.message.reply_text("Этот пользователь не является админом!")
                context.user_data.pop("input_state", None)
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
                return await admin_user_stats(update, context)
            except ValueError:
                await update.message.reply_text("Введите корректное значение!")
                return STATE_EDIT_USER

        return STATE_MAIN_MENU
    except Exception as e:
        logger.error(f"Error in handle_text_input for user_id={user_id}: {e}")
        await update.message.reply_text("Произошла ошибка.")
        return STATE_MAIN_MENU

async def webhook_handler(request):
    """Обрабатывает входящие webhook-запросы от Telegram."""
    try:
        json_data = await request.json()
        logger.info(f"Received webhook data: {json_data}")
        update = Update.de_json(json_data, request.app['bot'])
        if update is None:
            logger.error("Failed to parse update from JSON")
            return aiohttp.web.Response(text="OK")
        await request.app['application'].process_update(update)
        logger.info("Webhook update processed successfully")
    except json.JSONDecodeError:
        logger.error("Invalid JSON received in webhook")
        return aiohttp.web.Response(text="OK")
    except KeyError as e:
        logger.error(f"KeyError in webhook handler: {e}")
        return aiohttp.web.Response(text="OK")
    except Exception as e:
        logger.error(f"Webhook processing error: {e}")
        return aiohttp.web.Response(text="OK")
    return aiohttp.web.Response(text="OK")

async def root_handler(request):
    """Обработчик корневого пути для Render."""
    return aiohttp.web.Response(text="Stars Bot is running")

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
                STATE_ADMIN_USER_STATS: [
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
        app.job_queue.run_repeating(update_ton_price, interval=600, first=10)
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
