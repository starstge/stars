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
                    ('ton_commission', '20'),
                    ('card_commission', '30'),
                    ('card_payment_enabled', 'false'),
                    ('min_stars_purchase', '10'),
                    ('markup_percentage', $1)
                ON CONFLICT (key) DO NOTHING;
            """, str(MARKUP_PERCENTAGE))
            # Вставка данных в texts
            await conn.execute("""
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
                           'ton_exchange_rate', 'cryptobot_commission', 'ton_commission',
                           'card_commission', 'min_stars_purchase', 'markup_percentage',
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
                    asyncio.sleep(2 ** attempt)
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
            await conn.execute(
                """
                INSERT INTO users (user_id, username, referrer_id, language)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (user_id) DO UPDATE SET username = $5
                """,
                user_id, username, ref_id, 'ru', username
            )
            if ref_id:
                await conn.execute(
                    """
                    UPDATE users
                    SET referrals = referrals || $1
                    WHERE user_id = $2
                    """,
                    json.dumps({"user_id": user_id, "username": username}), ref_id
                )

        # Отправка сообщения в канал о новом пользователе
        channel_id = "-1002703640431"  # Проверь, что это правильный chat_id
        join_time = datetime.now(pytz.timezone('Europe/Kiev')).strftime('%Y-%m-%d %H:%M:%S %Z')
        channel_message = f"Новый пользователь: @{username}\nВремя: {join_time}"
        try:
            await context.bot.send_message(chat_id=channel_id, text=channel_message)
            logger.info(f"Sent new user notification to channel: {channel_message}")
        except Exception as e:
            logger.error(f"Failed to send channel message: {e}")

        keyboard = [
            [InlineKeyboardButton("👤 Профиль", callback_data=PROFILE)],
            [InlineKeyboardButton("🤝 Рефералы", callback_data=REFERRALS)],
            [InlineKeyboardButton("🛠 Поддержка", callback_data=SUPPORT)],
            [InlineKeyboardButton("📝 Отзывы", callback_data=REVIEWS)],
            [InlineKeyboardButton("⭐ Купить звезды", callback_data=BUY_STARS)],
        ]
        async with (await get_db_pool()) as conn:
            admin_ids = json.loads((await conn.fetchval("SELECT value FROM settings WHERE key = 'admin_ids'")) or '[]')
            if user_id in admin_ids:
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
        keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_ADMIN)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        return STATE_ADMIN_PANEL
    except Exception as e:
        logger.error(f"Error in admin_stats: {e}")
        await update.callback_query.edit_message_text("Ошибка при получении статистики.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_ADMIN)]]))
        return STATE_ADMIN_PANEL

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
    return STATE_ADMIN_EDIT_TEXTS

async def edit_text_prompt(update, context):
    """Запрашивает новый текст для редактирования."""
    query = update.callback_query
    text_key = query.data.replace("edit_text_", "")
    context.user_data["text_key"] = text_key
    text = f"Введите новый текст для '{text_key}':"
    keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_ADMIN)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(text, reply_markup=reply_markup)
    return STATE_EDIT_TEXT

async def admin_user_stats(update, context):
    """Запрашивает поиск пользователей."""
    text = await get_text("user_stats", update.effective_user.id)
    keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_ADMIN)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    return STATE_USER_SEARCH

async def admin_edit_markup(update, context):
    """Запрашивает новую наценку."""
    text = await get_text("edit_markup", update.effective_user.id)
    keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_ADMIN)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    context.user_data["input_state"] = "markup_percentage"
    return STATE_ADMIN_EDIT_MARKUP

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
    return STATE_ADMIN_MANAGE_ADMINS

async def admin_edit_profit(update, context):
    """Запрашивает новую прибыль."""
    text = await get_text("edit_profit", update.effective_user.id)
    keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_ADMIN)]]
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
            [InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_MENU)],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        return STATE_PROFILE
    except Exception as e:
        logger.error(f"Error in profile for user_id={user_id}: {e}")
        await update.callback_query.edit_message_text("Ошибка при загрузке профиля.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_MENU)]]))
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
        keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data=PROFILE)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        return STATE_PROFILE
    except Exception as e:
        logger.error(f"Error in top_referrals: {e}")
        await update.callback_query.edit_message_text("Ошибка при загрузке топа.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Назад", callback_data=PROFILE)]]))
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
        keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data=PROFILE)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        return STATE_PROFILE
    except Exception as e:
        logger.error(f"Error in top_purchases: {e}")
        await update.callback_query.edit_message_text("Ошибка при загрузке топа.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Назад", callback_data=PROFILE)]]))
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
        keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_MENU)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        return STATE_REFERRALS
    except Exception as e:
        logger.error(f"Error in referrals for user_id={user_id}: {e}")
        await update.callback_query.edit_message_text("Ошибка при загрузке рефералов.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_REFERRALS

async def buy_stars(update, context):
    """Начинает процесс покупки звезд."""
    user_id = update.effective_user.id
    try:
        async with (await get_db_pool()) as conn:
            username = await conn.fetchval("SELECT username FROM users WHERE user_id = $1", user_id)
        context.user_data["username"] = username or "Неизвестно"
        text = "Введите количество звезд для покупки:"
        keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_MENU)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        return STATE_BUY_STARS_AMOUNT
    except Exception as e:
        logger.error(f"Error in buy_stars for user_id={user_id}: {e}")
        await update.callback_query.edit_message_text("Ошибка при начале покупки.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_BUY_STARS_AMOUNT

async def check_payment(update, context):
    """Проверяет оплату по кнопке."""
    user_id = update.effective_user.id
    username = context.user_data.get("username")
    amount_ton = context.user_data.get("amount_ton")
    memo = context.user_data.get("memo")
    address = context.user_data.get("address")
    stars = context.user_data.get("stars")
    try:
        payment_confirmed = await check_ton_payment(address, memo, amount_ton)
        if payment_confirmed:
            async with (await get_db_pool()) as conn:
                await conn.execute(
                    "UPDATE users SET stars_bought = stars_bought + $1 WHERE user_id = $2",
                    stars, user_id
                )
                await conn.execute(
                    "UPDATE settings SET value = value::integer + $1 WHERE key = 'total_stars_sold'",
                    stars
                )
                profit_ton = float(amount_ton * (await get_setting("profit_percent") or 20) / 100)
                await conn.execute(
                    "UPDATE settings SET value = value::float + $1 WHERE key = 'total_profit_ton'",
                    profit_ton
                )
                referrer_id = await conn.fetchval("SELECT referrer_id FROM users WHERE user_id = $1", user_id)
                if referrer_id:
                    ref_bonus = float(amount_ton * (await get_setting("ref_bonus_percent") or 30) / 100)
                    await conn.execute(
                        "UPDATE users SET ref_bonus_ton = ref_bonus_ton + $1 WHERE user_id = $2",
                        ref_bonus, referrer_id
                    )
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
            return STATE_BUY_STARS_PAYMENT_METHOD
    except Exception as e:
        logger.error(f"Error in check_payment for user_id={user_id}: {e}")
        await update.callback_query.edit_message_text("Ошибка при проверке оплаты.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_MENU)]]))
        return STATE_BUY_STARS_PAYMENT_METHOD

async def button_handler(update, context):
    """Обрабатывает нажатия кнопок."""
    query = update.callback_query
    data = query.data
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
            text = await get_text("tech_support", update.effective_user.id, support_channel=await get_setting("support_channel"))
            keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_MENU)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text, reply_markup=reply_markup)
            return ConversationHandler.END
        elif data == REVIEWS:
            text = await get_text("reviews", update.effective_user.id, review_channel=await get_setting("review_channel"))
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
            return STATE_ADMIN_MANAGE_ADMINS
        elif data == "remove_admin":
            text = "Введите ID админа для удаления:"
            keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_ADMIN)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(text, reply_markup=reply_markup)
            context.user_data["input_state"] = "remove_admin"
            return STATE_ADMIN_MANAGE_ADMINS
        elif data.startswith("lang_"):
            language = data.split("_")[1]
            async with (await get_db_pool()) as conn:
                await conn.execute(
                    "UPDATE users SET language = $1 WHERE user_id = $2",
                    language, update.effective_user.id
                )
            return await start(update, context)
        elif data == "cancel":
            return await start(update, context)
        await query.answer()
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error in button_handler: {e}")
        await query.edit_message_text("Произошла ошибка.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Назад", callback_data=BACK_TO_MENU)]]))
        return ConversationHandler.END

async def handle_text_input(update, context):
    """Обрабатывает текстовый ввод."""
    user_id = update.effective_user.id
    text = update.message.text.strip()
    state = context.user_data.get("state", update.current_state)
    input_state = context.user_data.get("input_state")

    try:
        if state == STATE_BUY_STARS_AMOUNT:
            try:
                stars = int(text)
                min_stars = await get_setting("min_stars_purchase") or 10
                if stars < min_stars:
                    await update.message.reply_text(f"Введите количество звезд не менее {min_stars}!")
                    return STATE_BUY_STARS_AMOUNT
                context.user_data["stars"] = stars
                base_price_usd = float(await get_setting("stars_price_usd") or 0.81) * (stars / 50)
                markup = float(await get_setting("markup_percentage") or MARKUP_PERCENTAGE)
                commission = float(await get_setting("ton_commission") or 20) / 100
                amount_usd = base_price_usd * (1 + markup / 100) * (1 + commission)
                ton_price = float(await get_setting("ton_exchange_rate") or 2.93)
                amount_ton = amount_usd / ton_price
                context.user_data["amount_ton"] = amount_ton
                context.user_data["memo"] = f"order_{user_id}_{int(time.time())}"
                context.user_data["address"] = OWNER_WALLET or "UQB_XcBjornHoP0aIf6ofn-wT8ru5QPsgYKtyPrlbgKsXrrX"
                username = context.user_data.get("username", "Неизвестно")
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
                return STATE_BUY_STARS_PAYMENT_METHOD
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
                        "SELECT username, stars_bought, ref_bonus_ton, referrals FROM users WHERE user_id = $1",
                        int(text)
                    )
                else:
                    result = await conn.fetchrow(
                        "SELECT username, stars_bought, ref_bonus_ton, referrals FROM users WHERE username = $1",
                        text.lstrip("@")
                    )
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
                return STATE_USER_SEARCH

        elif state == STATE_ADMIN_EDIT_MARKUP and input_state == "markup_percentage":
            try:
                markup = float(text)
                if markup < 0:
                    raise ValueError
                await update_setting("markup_percentage", markup)
                await log_admin_action(user_id, f"Updated markup to {markup}%")
                await update.message.reply_text(f"Наценка обновлена: {markup}%")
                context.user_data.pop("input_state", None)
                return await admin_panel(update, context)
            except ValueError:
                await update.message.reply_text("Введите корректное число для наценки!")
                return STATE_ADMIN_EDIT_MARKUP

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
                new_admin_id = int(text)
                admin_ids = await get_setting("admin_ids") or [6956377285]
                if new_admin_id not in admin_ids:
                    admin_ids.append(new_admin_id)
                    await update_setting("admin_ids", admin_ids)
                    await log_admin_action(user_id, f"Added admin: {new_admin_id}")
                    await update.message.reply_text(f"Админ {new_admin_id} добавлен!")
                else:
                    await update.message.reply_text("Этот пользователь уже админ!")
                context.user_data.pop("input_state", None)
                return await admin_manage_admins(update, context)
            except ValueError:
                await update.message.reply_text("Введите корректный ID!")
                return STATE_ADMIN_MANAGE_ADMINS

        elif state == STATE_ADMIN_MANAGE_ADMINS and input_state == "remove_admin":
            try:
                remove_admin_id = int(text)
                admin_ids = await get_setting("admin_ids") or [6956377285]
                if remove_admin_id in admin_ids:
                    admin_ids.remove(remove_admin_id)
                    await update_setting("admin_ids", admin_ids)
                    await log_admin_action(user_id, f"Removed admin: {remove_admin_id}")
                    await update.message.reply_text(f"Админ {remove_admin_id} удален!")
                else:
                    await update.message.reply_text("Этот пользователь не является админом!")
                context.user_data.pop("input_state", None)
                return await admin_manage_admins(update, context)
            except ValueError:
                await update.message.reply_text("Введите корректный ID!")
                return STATE_ADMIN_MANAGE_ADMINS

        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error in handle_text_input for user_id={user_id}: {e}")
        await update.message.reply_text("Произошла ошибка.")
        return ConversationHandler.END

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
                await asyncio.sleep(3600)
        except KeyboardInterrupt:
            logger.info("Shutting down bot")
            await app.bot.delete_webhook()
            await app.stop()
            await runner.cleanup()
            await app.shutdown()
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
