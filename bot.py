import os
import time
import json
import logging
import asyncio
import aiohttp
import psycopg2
from urllib.parse import urlparse
from dotenv import load_dotenv
from functools import lru_cache
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler,
    filters, ContextTypes, ConversationHandler
)
from psycopg2.pool import SimpleConnectionPool

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

# Константы состояний для ConversationHandler
STATE_CHOOSE_LANGUAGE = 0
STATE_BUY_STARS_AMOUNT = 1
STATE_BUY_STARS_PAYMENT_METHOD = 2
STATE_ADMIN_PANEL = 3
STATE_ADMIN_STATS = 4
STATE_ADMIN_EDIT_TEXTS = 5
STATE_EDIT_TEXT = 6
STATE_ADMIN_USER_STATS = 7
STATE_ADMIN_EDIT_MARKUP = 8
STATE_ADMIN_MANAGE_ADMINS = 9
STATE_ADMIN_EDIT_PROFIT = 10
STATE_PROFILE = 11
STATE_TOP_REFERRALS = 12
STATE_TOP_PURCHASES = 13
STATE_REFERRALS = 14
STATE_USER_SEARCH = 15

# Пул соединений
db_pool = None


def init_db():
    """Инициализирует пул соединений с базой данных."""
    global db_pool
    try:
        parsed_url = urlparse(POSTGRES_URL)
        dbname = parsed_url.path.lstrip('/')
        user = parsed_url.username
        password = parsed_url.password
        host = parsed_url.hostname
        port = parsed_url.port or 5432
        db_pool = SimpleConnectionPool(
            minconn=1, maxconn=10, host=host, port=port, dbname=dbname, user=user, password=password
        )
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS settings (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    );
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
                    CREATE TABLE IF NOT EXISTS admin_log (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT,
                        action TEXT NOT NULL,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    CREATE TABLE IF NOT EXISTS texts (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    );
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
                        ('ton_commission', '20'),
                        ('card_commission', '30'),
                        ('card_payment_enabled', 'false'),
                        ('min_stars_purchase', '10'),
                        ('markup_percentage', '{MARKUP_PERCENTAGE}')
                    ON CONFLICT (key) DO NOTHING;
                    INSERT INTO texts (key, value)
                    VALUES
                        ('welcome', '🌟 Привет! Это Stars Bot — твой помощник для покупки Telegram Stars! 🚀\nПродано звёзд: {{total_stars_sold}}'),
                        ('buy_prompt', '💸 Оплатите {{amount_ton:.6f}} TON\nЗвёзд: {{stars}}\nАдрес: {{address}}\nМемо: {{memo}}\nДля: @{{username}}'),
                        ('buy_success', '🎉 Оплата прошла! @{{username}} получил {{stars}} звёзд!'),
                        ('profile', '👤 Профиль\nИмя: @{{username}}\nКуплено звезд: {{stars_bought}}\nРеф. бонус: {{ref_bonus_ton:.6f}} TON'),
                        ('referrals', '🤝 Реферальная система\nРефералов: {{ref_count}}\nРеф. бонус: {{ref_bonus_ton:.6f}} TON\nСсылка: {{ref_link}}'),
                        ('tech_support', '🛠 Свяжитесь с поддержкой: {{support_channel}}'),
                        ('reviews', '📝 Отзывы: {{review_channel}}'),
                        ('admin_panel', '🛠 Админ-панель'),
                        ('stats', '📊 Статистика\nПрибыль: {{total_profit_ton:.6f}} TON\nЗвёзд продано: {{total_stars_sold}}\nПользователей: {{user_count}}'),
                        ('edit_text_menu', '📝 Изменить текст'),
                        ('user_stats', '👤 Статистика пользователей\nВведите ID или username для поиска (или /cancel):'),
                        ('user_info', '👤 @{{username}}\nЗвёзд куплено: {{stars_bought}}\nРеф. бонус: {{ref_bonus_ton:.6f}} TON\nРефералов: {{ref_count}}'),
                        ('edit_markup', '💸 Изменить наценку (%)'),
                        ('manage_admins', '👑 Управление админами'),
                        ('edit_profit', '📈 Изменить прибыль (%)'),
                        ('back_btn', '🔙 Назад'),
                        ('cancel_btn', '❌ Отмена')
                    ON CONFLICT (key) DO NOTHING;
                """)
                conn.commit()
        logger.info("Database pool initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

def get_db_connection():
    """Получает соединение из пула."""
    if not db_pool:
        raise ValueError("Database pool not initialized")
    return db_pool.getconn()

def release_db_connection(conn):
    """Возвращает соединение в пул."""
    db_pool.putconn(conn)

@lru_cache(maxsize=128)
def get_setting(key):
    """Получает настройку из базы."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM settings WHERE key = %s", (key,))
            result = cur.fetchone()
            if result:
                if key in ('admin_ids', 'referrals', 'bonus_history'):
                    return json.loads(result[0])
                if key == 'card_payment_enabled':
                    return result[0].lower() == 'true'
                return (float(result[0]) if key in ('ref_bonus_percent', 'profit_percent', 'stars_price_usd',
                                                    'ton_exchange_rate', 'cryptobot_commission', 'ton_commission',
                                                    'card_commission', 'min_stars_purchase', 'markup_percentage',
                                                    'total_stars_sold', 'total_profit_usd', 'total_profit_ton')
                        else result[0])
    return None

def update_setting(key, value):
    """Обновляет настройку в базе."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = %s",
                (key, json.dumps(value) if isinstance(value, list) else str(value),
                 json.dumps(value) if isinstance(value, list) else str(value))
            )
            conn.commit()

async def get_text(key: str, user_id: int, **kwargs) -> str:
    """Получает текст из таблицы texts."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT value FROM texts WHERE key = %s", (key,))
            result = await cur.fetchone()
            return result[0].format(**kwargs) if result else f"Текст не найден: {key}"

def update_text(key, value):
    """Обновляет текст в таблице texts."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO texts (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = %s",
                (key, value, value)
            )
            conn.commit()

async def is_admin(user_id):
    """Проверяет, является ли пользователь админом."""
    admin_ids = get_setting("admin_ids") or [6956377285]
    return user_id in admin_ids

def log_admin_action(admin_id, action):
    """Логирует действие админа."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO admin_log (user_id, action) VALUES (%s, %s)", (admin_id, action))
            conn.commit()

async def update_ton_price(context: ContextTypes.DEFAULT_TYPE):
    """Обновляет курс TON."""
    async with aiohttp.ClientSession() as session:
        for attempt in range(3):
            try:
                async with session.get("https://api.coingecko.com/api/v3/simple/price?ids=toncoin&vs_currencies=usd") as response:
                    if response.status == 200:
                        data = await response.json()
                        ton_price = data.get("toncoin", {}).get("usd", 2.93)
                        update_setting("ton_exchange_rate", ton_price)
                        logger.info(f"Updated TON price: ${ton_price}")
                        return
                    elif response.status == 429:
                        await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"TON price update failed: {e}")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)

async def check_ton_payment(address, memo, amount_ton):
    """Проверяет оплату TON."""
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

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start и возврата в главное меню."""
    user_id = update.effective_user.id
    username = update.effective_user.username or f"user_{user_id}"
    args = context.args
    ref_id = int(args[0].split('_')[-1]) if args and args[0].startswith('ref_') else None
    logger.info(f"/start command by user {user_id}, ref_id={ref_id}")

    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO users (user_id, username, referrer_id, language) VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (user_id) DO UPDATE SET username = %s",
                (user_id, username, ref_id, 'ru', username)
            )
            if ref_id:
                await cur.execute(
                    "UPDATE users SET referrals = referrals || %s WHERE user_id = %s",
                    (json.dumps({"user_id": user_id, "username": username}), ref_id)
                )
            await conn.commit()

    keyboard = [
        [InlineKeyboardButton("👤 Профиль", callback_data=PROFILE)],
        [InlineKeyboardButton("🤝 Рефералы", callback_data=REFERRALS)],
        [InlineKeyboardButton("🛠 Поддержка", callback_data=SUPPORT)],
        [InlineKeyboardButton("📝 Отзывы", callback_data=REVIEWS)],
        [InlineKeyboardButton("⭐ Купить звезды", callback_data=BUY_STARS)],
    ]
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT value FROM settings WHERE key = 'admin_ids'")
            result = await cur.fetchone()
            admin_ids = json.loads(result[0]) if result else []
            if user_id in admin_ids:
                keyboard.append([InlineKeyboardButton("🔧 Админ-панель", callback_data=ADMIN_PANEL)])
    reply_markup = InlineKeyboardMarkup(keyboard)
    text = await get_text("welcome", user_id, total_stars_sold=get_setting("total_stars_sold") or 0)
    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    else:
        await update.message.reply_text(text, reply_markup=reply_markup)
    return ConversationHandler.END

async def admin_panel(update, context):
    """Отображает админ-панель."""
    user_id = update.effective_user.id
    if not await is_admin(user_id):
        await update.callback_query.answer("Доступ запрещен!")
        return ConversationHandler.END
    keyboard = [
        [InlineKeyboardButton("📊 Статистика", callback_data=ADMIN_STATS)],
        [InlineKeyboardButton("📝 Изменить тексты", callback_data=ADMIN_EDIT_TEXTS)],
        [InlineKeyboardButton("👥 Статистика пользователей", callback_data=ADMIN_USER_STATS)],
        [InlineKeyboardButton("💸 Изменить наценку", callback_data=ADMIN_EDIT_MARKUP)],
        [InlineKeyboardButton("👤 Добавить/удалить админа", callback_data=ADMIN_MANAGE_ADMINS)],
        [InlineKeyboardButton("💰 Изменить прибыль", callback_data=ADMIN_EDIT_PROFIT)],
        [InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_MENU)],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    text = await get_text("admin_panel", user_id)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    return ADMIN_PANEL

async def admin_stats(update, context):
    """Показывает статистику бота."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT value FROM settings WHERE key = 'total_profit_ton'")
            total_profit_ton = float((await cur.fetchone())[0]) if await cur.rowcount else 0
            await cur.execute("SELECT value FROM settings WHERE key = 'total_stars_sold'")
            total_stars_sold = int((await cur.fetchone())[0]) if await cur.rowcount else 0
            await cur.execute("SELECT COUNT(*) FROM users")
            user_count = (await cur.fetchone())[0]
    text = await get_text("stats", update.effective_user.id, total_profit_ton=total_profit_ton,
                          total_stars_sold=total_stars_sold, user_count=user_count)
    keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_ADMIN)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    return ADMIN_PANEL

async def admin_edit_texts(update, context):
    """Подменю для изменения текстов."""
    keyboard = [
        [InlineKeyboardButton("Приветствие", callback_data="edit_text_welcome")],
        [InlineKeyboardButton("Покупка", callback_data="edit_text_buy_prompt")],
        [InlineKeyboardButton("Профиль", callback_data="edit_text_profile")],
        [InlineKeyboardButton("Рефералы", callback_data="edit_text_referrals")],
        [InlineKeyboardButton("Поддержка", callback_data="edit_text_tech_support")],
        [InlineKeyboardButton("Отзывы", callback_data="edit_text_reviews")],
        [InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_ADMIN)],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    text = await get_text("edit_text_menu", update.effective_user.id)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    return ADMIN_EDIT_TEXTS

async def edit_text_prompt(update, context):
    """Запрашивает новый текст для редактирования."""
    query = update.callback_query
    text_key = query.data.replace("edit_text_", "")
    context.user_data["text_key"] = text_key
    text = f"Введите новый текст для '{text_key}':"
    keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_ADMIN)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(text, reply_markup=reply_markup)
    return EDIT_TEXT

async def admin_user_stats(update, context):
    """Запрашивает поиск пользователей."""
    text = await get_text("user_stats", update.effective_user.id)
    keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_ADMIN)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    return USER_SEARCH

async def admin_edit_markup(update, context):
    """Запрашивает новую наценку."""
    text = await get_text("edit_markup", update.effective_user.id)
    keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_ADMIN)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    context.user_data["input_state"] = "markup_percentage"
    return ADMIN_EDIT_MARKUP

async def admin_manage_admins(update, context):
    """Меню управления админами."""
    keyboard = [
        [InlineKeyboardButton("Добавить админа", callback_data="add_admin")],
        [InlineKeyboardButton("Удалить админа", callback_data="remove_admin")],
        [InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_ADMIN)],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    text = await get_text("manage_admins", update.effective_user.id)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    return ADMIN_MANAGE_ADMINS

async def admin_edit_profit(update, context):
    """Запрашивает новую прибыль."""
    text = await get_text("edit_profit", update.effective_user.id)
    keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_ADMIN)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    context.user_data["input_state"] = "profit_percent"
    return ADMIN_EDIT_PROFIT

async def profile(update, context):
    """Показывает профиль пользователя."""
    user_id = update.effective_user.id
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT username, stars_bought, ref_bonus_ton FROM users WHERE user_id = %s", (user_id,))
            result = await cur.fetchone()
            username = result[0] if result else "Неизвестно"
            stars_bought = result[1] if result else 0
            ref_bonus_ton = float(result[2]) if result else 0
    text = await get_text("profile", user_id, username=username, stars_bought=stars_bought, ref_bonus_ton=ref_bonus_ton)
    keyboard = [
        [InlineKeyboardButton("🏆 Топ рефералов", callback_data=TOP_REFERRALS)],
        [InlineKeyboardButton("⭐ Топ покупок", callback_data=TOP_PURCHASES)],
        [InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_MENU)],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    return PROFILE

async def top_referrals(update, context):
    """Показывает топ-10 пользователей по рефералам."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT username, referrals FROM users WHERE referrals != '[]' ORDER BY jsonb_array_length(referrals) DESC LIMIT 10")
            results = await cur.fetchall()
    text = "🏆 Топ-10 рефералов:\n"
    for i, (username, referrals) in enumerate(results, 1):
        ref_count = len(json.loads(referrals))
        text += f"{i}. @{username}: {ref_count} рефералов\n"
    if not results:
        text += "Нет рефералов."
    keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data=PROFILE)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    return PROFILE

async def top_purchases(update, context):
    """Показывает топ-10 пользователей по покупкам звезд."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT username, stars_bought FROM users WHERE stars_bought > 0 ORDER BY stars_bought DESC LIMIT 10")
            results = await cur.fetchall()
    text = "⭐ Топ-10 покупок:\n"
    for i, (username, stars_bought) in enumerate(results, 1):
        text += f"{i}. @{username}: {stars_bought} звезд\n"
    if not results:
        text += "Нет покупок."
    keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data=PROFILE)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    return PROFILE

async def referrals(update, context):
    """Показывает реферальную информацию."""
    user_id = update.effective_user.id
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT referrals, ref_bonus_ton FROM users WHERE user_id = %s", (user_id,))
            result = await cur.fetchone()
            ref_count = len(json.loads(result[0])) if result and result[0] != '[]' else 0
            ref_bonus_ton = float(result[1]) if result else 0
    bot_username = (await context.bot.get_me()).username
    ref_link = f"t.me/{bot_username}?start=ref_{user_id}"
    text = await get_text("referrals", user_id, ref_count=ref_count, ref_bonus_ton=ref_bonus_ton, ref_link=ref_link)
    keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_MENU)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    return REFERRALS

async def buy_stars(update, context):
    """Начинает процесс покупки звезд."""
    user_id = update.effective_user.id
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT username FROM users WHERE user_id = %s", (user_id,))
            username = (await cur.fetchone())[0] if await cur.rowcount else None
    context.user_data["username"] = username
    text = "Введите количество звезд для покупки:"
    keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_MENU)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    return BUY_STARS_AMOUNT

async def check_payment(update, context):
    """Проверяет оплату по кнопке."""
    user_id = update.effective_user.id
    username = context.user_data.get("username")
    amount_ton = context.user_data.get("amount_ton")
    memo = context.user_data.get("memo")
    address = context.user_data.get("address")
    stars = context.user_data.get("stars")
    payment_confirmed = await check_ton_payment(address, memo, amount_ton)
    if payment_confirmed:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE users SET stars_bought = stars_bought + %s WHERE user_id = %s",
                    (stars, user_id)
                )
                await cur.execute(
                    "UPDATE settings SET value = value::integer + %s WHERE key = 'total_stars_sold'",
                    (stars,)
                )
                profit_ton = float(amount_ton * (get_setting("profit_percent") or 20) / 100)
                await cur.execute(
                    "UPDATE settings SET value = value::float + %s WHERE key = 'total_profit_ton'",
                    (profit_ton,)
                )
                if referrer_id := await cur.execute("SELECT referrer_id FROM users WHERE user_id = %s", (user_id,)):
                    referrer_id = (await cur.fetchone())[0]
                    if referrer_id:
                        ref_bonus = float(amount_ton * (get_setting("ref_bonus_percent") or 30) / 100)
                        await cur.execute(
                            "UPDATE users SET ref_bonus_ton = ref_bonus_ton + %s WHERE user_id = %s",
                            (ref_bonus, referrer_id)
                        )
                await conn.commit()
        text = await get_text("buy_success", user_id, username=username, stars=stars)
        keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_MENU)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        return ConversationHandler.END
    else:
        text = "Оплата не подтверждена. Попробуйте снова."
        keyboard = [
            [InlineKeyboardButton("Проверить", callback_data=CHECK_PAYMENT)],
            [InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_MENU)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        return BUY_STARS_PAYMENT_METHOD

async def button_handler(update, context):
    """Обрабатывает нажатия кнопок."""
    query = update.callback_query
    data = query.data
    if data == BACK_TO_MENU:
        return await start(update, context)
    elif data == BACK_TO_ADMIN:
        return await admin_panel(update, context)
    elif data == PROFILE:
        return await profile(update, context)
    elif data == REFERRALS:
        return await referrals(update, context)
    elif data == SUPPORT:
        text = await get_text("tech_support", update.effective_user.id, support_channel=get_setting("support_channel"))
        keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_MENU)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text, reply_markup=reply_markup)
        return ConversationHandler.END
    elif data == REVIEWS:
        text = await get_text("reviews", update.effective_user.id, review_channel=get_setting("review_channel"))
        keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_MENU)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text, reply_markup=reply_markup)
        return ConversationHandler.END
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
    elif data == "add_admin":
        text = "Введите ID нового админа:"
        keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_ADMIN)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["input_state"] = "add_admin"
        return ADMIN_MANAGE_ADMINS
    elif data == "remove_admin":
        text = "Введите ID админа для удаления:"
        keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_ADMIN)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text, reply_markup=reply_markup)
        context.user_data["input_state"] = "remove_admin"
        return ADMIN_MANAGE_ADMINS
    elif data.startswith("lang_"):
        language = data.split("_")[1]
        update_user_language(update.effective_user.id, language)
        return await start(update, context)
    elif data == "cancel":
        return await start(update, context)
    await query.answer()
    return ConversationHandler.END

async def handle_text_input(update, context):
    """Обрабатывает текстовый ввод."""
    user_id = update.effective_user.id
    text = update.message.text.strip()
    state = context.user_data.get("state", update.current_state)
    input_state = context.user_data.get("input_state")

    if state == BUY_STARS_AMOUNT:
        try:
            stars = int(text)
            min_stars = get_setting("min_stars_purchase") or 10
            if stars < min_stars:
                await update.message.reply_text(f"Введите количество звезд не менее {min_stars}!")
                return BUY_STARS_AMOUNT
            context.user_data["stars"] = stars
            base_price_usd = float(get_setting("stars_price_usd") or 0.81) * (stars / 50)
            markup = float(get_setting("markup_percentage") or MARKUP_PERCENTAGE)
            commission = float(get_setting("ton_commission") or 20) / 100
            amount_usd = base_price_usd * (1 + markup / 100) * (1 + commission)
            ton_price = float(get_setting("ton_exchange_rate") or 2.93)
            amount_ton = amount_usd / ton_price
            context.user_data["amount_ton"] = amount_ton
            context.user_data["memo"] = f"order_{user_id}_{int(time.time())}"
            context.user_data["address"] = OWNER_WALLET or "UQB_XcBjornHoP0aIf6ofn-wT8ru5QPsgYKtyPrlbgKsXrrX"
            username = context.user_data.get("username", "Не указан")
            text = await get_text(
                "buy_prompt", user_id, amount_ton=amount_ton, stars=stars,
                address=context.user_data["address"], memo=context.user_data["memo"], username=username
            )
            keyboard = [
                [InlineKeyboardButton("Проверить", callback_data=CHECK_PAYMENT)],
                [InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_MENU)]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(text, reply_markup=reply_markup)
            return BUY_STARS_PAYMENT_METHOD
        except ValueError:
            await update.message.reply_text("Введите корректное количество звезд!")
            return BUY_STARS_AMOUNT

    elif state == EDIT_TEXT:
        text_key = context.user_data.get("text_key")
        update_text(text_key, text)
        log_admin_action(user_id, f"Edited text: {text_key}")
        await update.message.reply_text(f"Текст '{text_key}' обновлен!")
        return await admin_edit_texts(update, context)

    elif state == USER_SEARCH:
        async with get_db_connection() as conn:
            async with conn.cursor() as cur:
                if text.isdigit():
                    await cur.execute(
                        "SELECT username, stars_bought, ref_bonus_ton, referrals FROM users WHERE user_id = %s",
                        (int(text),)
                    )
                else:
                    await cur.execute(
                        "SELECT username, stars_bought, ref_bonus_ton, referrals FROM users WHERE username = %s",
                        (text.lstrip("@"),)
                    )
                result = await cur.fetchone()
        if result:
            username, stars_bought, ref_bonus_ton, referrals = result
            ref_count = len(json.loads(referrals)) if referrals != '[]' else 0
            text = await get_text("user_info", user_id, username=username, stars_bought=stars_bought,
                                  ref_bonus_ton=ref_bonus_ton, ref_count=ref_count)
            keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_ADMIN)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(text, reply_markup=reply_markup)
        else:
            await update.message.reply_text("Пользователь не найден!")
        return USER_SEARCH

    elif state == ADMIN_EDIT_MARKUP and input_state == "markup_percentage":
        try:
            markup = float(text)
            if markup < 0:
                raise ValueError
            update_setting("markup_percentage", markup)
            log_admin_action(user_id, f"Updated markup to {markup}%")
            await update.message.reply_text(f"Наценка обновлена: {markup}%")
            context.user_data.pop("input_state", None)
            return await admin_panel(update, context)
        except ValueError:
            await update.message.reply_text("Введите корректное число для наценки!")
            return ADMIN_EDIT_MARKUP

    elif state == ADMIN_EDIT_PROFIT and input_state == "profit_percent":
        try:
            profit = float(text)
            if profit < 0:
                raise ValueError
            update_setting("profit_percent", profit)
            log_admin_action(user_id, f"Updated profit to {profit}%")
            await update.message.reply_text(f"Прибыль обновлена: {profit}%")
            context.user_data.pop("input_state", None)
            return await admin_panel(update, context)
        except ValueError:
            await update.message.reply_text("Введите корректное число для прибыли!")
            return ADMIN_EDIT_PROFIT

    elif state == ADMIN_MANAGE_ADMINS and input_state == "add_admin":
        try:
            new_admin_id = int(text)
            admin_ids = get_setting("admin_ids") or [6956377285]
            if new_admin_id not in admin_ids:
                admin_ids.append(new_admin_id)
                update_setting("admin_ids", admin_ids)
                log_admin_action(user_id, f"Added admin: {new_admin_id}")
                await update.message.reply_text(f"Админ {new_admin_id} добавлен!")
            else:
                await update.message.reply_text("Этот пользователь уже админ!")
            context.user_data.pop("input_state", None)
            return await admin_manage_admins(update, context)
        except ValueError:
            await update.message.reply_text("Введите корректный ID!")
            return ADMIN_MANAGE_ADMINS

    elif state == ADMIN_MANAGE_ADMINS and input_state == "remove_admin":
        try:
            remove_admin_id = int(text)
            admin_ids = get_setting("admin_ids") or [6956377285]
            if remove_admin_id in admin_ids:
                admin_ids.remove(remove_admin_id)
                update_setting("admin_ids", admin_ids)
                log_admin_action(user_id, f"Removed admin: {remove_admin_id}")
                await update.message.reply_text(f"Админ {remove_admin_id} удален!")
            else:
                await update.message.reply_text("Этот пользователь не является админом!")
            context.user_data.pop("input_state", None)
            return await admin_manage_admins(update, context)
        except ValueError:
            await update.message.reply_text("Введите корректный ID!")
            return ADMIN_MANAGE_ADMINS

    return ConversationHandler.END

async def main():
    """Запускает бот."""
    init_db()
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            STATE_CHOOSE_LANGUAGE: [CallbackQueryHandler(button_handler, pattern=r"^lang_|^cancel$")],
            STATE_BUY_STARS_AMOUNT: [
                CallbackQueryHandler(button_handler, pattern=r"^cancel$|^" + BACK_TO_MENU + "$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
            ],
            STATE_BUY_STARS_PAYMENT_METHOD: [
                CallbackQueryHandler(button_handler, pattern=r"^" + CHECK_PAYMENT + "$|^" + BACK_TO_MENU + "$"),
            ],
            STATE_ADMIN_PANEL: [CallbackQueryHandler(button_handler, pattern=r"^admin_|^" + BACK_TO_MENU + "$")],
            STATE_ADMIN_STATS: [CallbackQueryHandler(button_handler, pattern=r"^" + BACK_TO_ADMIN + "$")],
            STATE_ADMIN_EDIT_TEXTS: [CallbackQueryHandler(button_handler, pattern=r"^edit_text_|^" + BACK_TO_ADMIN + "$")],
            STATE_EDIT_TEXT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                CallbackQueryHandler(button_handler, pattern=r"^" + BACK_TO_ADMIN + "$"),
            ],
            STATE_ADMIN_USER_STATS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                CallbackQueryHandler(button_handler, pattern=r"^" + BACK_TO_ADMIN + "$"),
            ],
            STATE_ADMIN_EDIT_MARKUP: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                CallbackQueryHandler(button_handler, pattern=r"^" + BACK_TO_ADMIN + "$"),
            ],
            STATE_ADMIN_MANAGE_ADMINS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                CallbackQueryHandler(button_handler, pattern=r"^(add_admin|remove_admin|^" + BACK_TO_ADMIN + "$)"),
            ],
            STATE_ADMIN_EDIT_PROFIT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                CallbackQueryHandler(button_handler, pattern=r"^" + BACK_TO_ADMIN + "$"),
            ],
            STATE_PROFILE: [CallbackQueryHandler(button_handler, pattern=r"^top_|^" + BACK_TO_MENU + "$")],
            STATE_TOP_REFERRALS: [CallbackQueryHandler(button_handler, pattern=r"^" + PROFILE + "$")],
            STATE_TOP_PURCHASES: [CallbackQueryHandler(button_handler, pattern=r"^" + PROFILE + "$")],
            STATE_REFERRALS: [CallbackQueryHandler(button_handler, pattern=r"^" + BACK_TO_MENU + "$")],
        },
        fallbacks=[
            CommandHandler("start", start),
            CommandHandler("cancel", start),
            CallbackQueryHandler(button_handler, pattern=r"^cancel|^" + BACK_TO_MENU + "$"),
        ],
        per_message=True,
    )
    app.add_handler(conv_handler)
    app.job_queue.run_repeating(update_ton_price, interval=600, first=10)
    await app.initialize()
    await app.updater.start_polling()
    await app.start()
    try:
        while True:
            await asyncio.sleep(3600)
    except KeyboardInterrupt:
        await app.stop()
        await app.updater.stop()
        await app.shutdown()
        
if __name__ == "__main__":
    asyncio.run(main())
