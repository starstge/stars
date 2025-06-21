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

# Загрузка .env
load_dotenv()

# Логирование
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Константы
BOT_TOKEN = os.getenv("BOT_TOKEN")
TON_API_KEY = os.getenv("TON_API_KEY")
POSTGRES_URL = os.getenv("POSTGRES_URL") or os.getenv("DATABASE_URL")
SPLIT_API_TOKEN = os.getenv("SPLIT_API_TOKEN")
OWNER_WALLET = os.getenv("OWNER_WALLET")
CRYPTOBOT_API_TOKEN = os.getenv("CRYPTOBOT_API_TOKEN")
MARKUP_PERCENTAGE = float(os.getenv("MARKUP_PERCENTAGE", 10))
SPLIT_API_URL = "https://api.split.tg/buy/stars"

# Состояния для ConversationHandler
(CHOOSE_LANGUAGE, BUY_STARS_USERNAME, BUY_STARS_AMOUNT, BUY_STARS_PAYMENT_METHOD,
 EDIT_TEXT, SET_PRICE, SET_PERCENT, SET_COMMISSIONS, SET_REVIEW_CHANNEL,
 SET_CARD_PAYMENT, SET_MARKUP, ADD_ADMIN, REMOVE_ADMIN, USER_SEARCH,
 EDIT_USER_STARS, EDIT_USER_REF_BONUS, RESET_PROFIT, SET_TEXT_KEY,
 EDIT_USER_DATA, SET_REF_BONUS_PERCENT) = range(20)

# Пул соединений
db_pool = None

def init_db():
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
                cur.execute("""
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
                        ('markup_percentage', '{}')
                    ON CONFLICT (key) DO NOTHING;
                    INSERT INTO texts (key, value)
                    VALUES
                        ('welcome_ru', '🌟 Привет! Это Stars Bot — твой помощник для покупки Telegram Stars! 🚀\nПродано звёзд: {total_stars_sold}'),
                        ('welcome_en', '🌟 Hello! Welcome to Stars Bot — your assistant for buying Telegram Stars! 🚀\nStars sold: {total_stars_sold}'),
                        ('buy_stars_prompt_ru', '✨ Кому отправить звёзды? Выбери параметры:'),
                        ('buy_stars_prompt_en', '✨ Who to send stars to? Choose options:'),
                        ('buy_username_prompt_ru', '👤 Введи username получателя (без @):'),
                        ('buy_username_prompt_en', '👤 Enter recipient username (without @):'),
                        ('buy_amount_prompt_ru', '🌟 Сколько звёзд купить? (минимум {min_stars}):'),
                        ('buy_amount_prompt_en', '🌟 How many stars to buy? (minimum {min_stars}):'),
                        ('buy_payment_method_prompt_ru', '💳 Выбери способ оплаты:'),
                        ('buy_payment_method_prompt_en', '💳 Choose payment method:'),
                        ('buy_success_ru', '🎉 Оплата прошла! @{username} получил {stars} звёзд!'),
                        ('buy_success_en', '🎉 Payment successful! @{username} received {stars} stars!'),
                        ('buy_invalid_username_ru', '⚠️ Неверный username. Введи без @.'),
                        ('buy_invalid_username_en', '⚠️ Invalid username. Enter without @.'),
                        ('buy_card_disabled_ru', '⚠️ Оплата картой отключена.'),
                        ('buy_card_disabled_en', '⚠️ Card payment is disabled.'),
                        ('buy_ton_prompt_ru', '💸 Оплатите {amount_ton:.2f} TON\nЗвёзд: {stars}\nАдрес: {address}\nМемо: {memo}\nДля: @{username}'),
                        ('buy_ton_prompt_en', '💸 Pay {amount_ton:.2f} TON\nStars: {stars}\nAddress: {address}\nMemo: {memo}\nTo: @{username}'),
                        ('buy_cryptobot_prompt_ru', '💸 Оплатите ${amount_usd:.2f}\nЗвёзд: {stars}\nДля: @{username}'),
                        ('buy_cryptobot_prompt_en', '💸 Pay ${amount_usd:.2f}\nStars: {stars}\nTo: @{username}'),
                        ('buy_card_prompt_ru', '💸 Оплатите ${amount_usd:.2f}\nЗвёзд: {stars}\nДля: @{username}'),
                        ('buy_card_prompt_en', '💸 Pay ${amount_usd:.2f}\nStars: {stars}\nTo: @{username}'),
                        ('profile_ru', '👤 Профиль\nUsername: @{username}\nЗвёзд куплено: {stars_bought}\nРеф. бонус: {ref_bonus_ton} TON'),
                        ('profile_en', '👤 Profile\nUsername: @{username}\nStars bought: {stars_bought}\nRef. bonus: {ref_bonus_ton} TON'),
                        ('referrals_ru', '👥 Реферальная система\nРефералов: {ref_count}\nРеф. бонус: {ref_bonus_ton} TON\nСсылка: {ref_link}'),
                        ('referrals_en', '👥 Referral system\nReferrals: {ref_count}\nRef. bonus: {ref_bonus_ton} TON\nLink: {ref_link}'),
                        ('admin_panel_ru', '🛠 Админ-панель'),
                        ('admin_panel_en', '🛠 Admin panel'),
                        ('stats_ru', '📊 Статистика\nПрибыль: ${total_profit_usd:.2f} | {total_profit_ton:.2f} TON\nЗвёзд продано: {total_stars_sold}\nПользователей: {user_count}'),
                        ('stats_en', '📊 Statistics\nProfit: ${total_profit_usd:.2f} | {total_profit_ton:.2f} TON\nStars sold: {total_stars_sold}\nUsers: {user_count}'),
                        ('edit_text_menu_ru', '📝 Изменить текст'),
                        ('edit_text_menu_en', '📝 Edit text'),
                        ('user_stats_ru', '👤 Статистика пользователей\nВведите username для поиска:'),
                        ('user_stats_en', '👤 User statistics\nEnter username to search:'),
                        ('user_info_ru', '👤 @{username}\nЗвёзд куплено: {stars_bought}\nРеф. бонус: {ref_bonus_ton} TON\nРефералов: {ref_count}'),
                        ('user_info_en', '👤 @{username}\nStars bought: {stars_bought}\nRef. bonus: {ref_bonus_ton} TON\nReferrals: {ref_count}'),
                        ('edit_markup_ru', '💸 Изменить наценку'),
                        ('edit_markup_en', '💸 Edit markup'),
                        ('manage_admins_ru', '👑 Управление админами'),
                        ('manage_admins_en', '👑 Manage admins'),
                        ('edit_profit_ru', '📈 Изменить прибыль'),
                        ('edit_profit_en', '📈 Edit profit'),
                        ('back_btn_ru', '🔙 Назад'),
                        ('back_btn_en', '🔙 Back'),
                        ('cancel_btn_ru', '❌ Отмена'),
                        ('cancel_btn_en', '❌ Cancel')
                    ON CONFLICT (key) DO NOTHING;
                """, (str(MARKUP_PERCENTAGE),))
                conn.commit()
        logger.info("Database pool initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

def get_db_connection():
    if not db_pool:
        raise ValueError("Database pool not initialized")
    return db_pool.getconn()

def release_db_connection(conn):
    db_pool.putconn(conn)

@lru_cache(maxsize=128)
def get_setting(key):
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
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = %s",
                (key, json.dumps(value) if isinstance(value, list) else str(value),
                 json.dumps(value) if isinstance(value, list) else str(value))
            )
            conn.commit()

async def get_text(key: str, user_id: int, **kwargs) -> str:
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT value FROM texts WHERE key = %s", (key,))
            result = await cur.fetchone()
            return result[0].format(**kwargs) if result else f"Текст не найден: {key}"

def update_text(key, value):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO texts (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = %s",
                (key, value, value)
            )
            conn.commit()

async def is_admin(user_id):
    admin_ids = get_setting("admin_ids") or [6956377285]
    return user_id in admin_ids

def log_admin_action(admin_id, action):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO admin_log (user_id, action) VALUES (%s, %s)", (admin_id, action))
            conn.commit()

def get_user_language(user_id):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT language FROM users WHERE user_id = %s", (user_id,))
            result = cur.fetchone()
            return result[0] if result else 'ru'

def update_user_language(user_id, language):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (user_id, username, language) VALUES (%s, %s, %s) "
                "ON CONFLICT (user_id) DO UPDATE SET language = %s",
                (user_id, f"user_{user_id}", language, language)
            )
            conn.commit()

def get_user_data(user_id, field):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT {field} FROM users WHERE user_id = %s", (user_id,))
            result = cur.fetchone()
            if result:
                if field in ("referrals", "bonus_history"):
                    return json.loads(result[0])
                return result[0]
    return None

async def issue_stars_api(session, username, stars):
    headers = {"Authorization": f"Bearer {SPLIT_API_TOKEN}", "Content-Type": "application/json"}
    payload = {"username": username.lstrip("@"), "payment_method": "ton_connect", "quantity": stars}
    for attempt in range(3):
        try:
            async with session.post(SPLIT_API_URL, headers=headers, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("success"):
                        logger.info(f"Stars issued: @{username}, {stars}")
                        return True
                    logger.error(f"Split API error: {data.get('error')}")
                elif response.status == 429:
                    await asyncio.sleep(2 ** attempt)
                else:
                    logger.error(f"Split API failed: {response.status}")
                return False
        except Exception as e:
            logger.error(f"Split API error: {e}")
            if attempt < 2:
                asyncio.sleep(2 ** attempt)
    return False

async def create_ton_payment(user_id, username, stars):
    base_price_usd = float(get_setting("stars_price_usd") or 0.81) * (stars / 50)
    markup = float(get_setting("markup_percentage") or MARKUP_PERCENTAGE)
    commission = float(get_setting("ton_commission") or 20) / 100
    amount_usd = base_price_usd * (1 + markup / 100) * (1 + commission)
    ton_price = float(get_setting("ton_exchange_rate") or 2.93)
    amount_ton = amount_usd / ton_price
    memo = f"order_{user_id}_{int(time.time())}"
    address = OWNER_WALLET

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE users 
                SET stars_bought = %s, username = %s, address = %s, memo = %s, amount_ton = %s
                WHERE user_id = %s
                """,
                (stars, username, address, memo, amount_ton, user_id)
            )
            conn.commit()

    return {
        "address": address,
        "amount_ton": amount_ton,
        "memo": memo,
        "url": f"https://ton.app/wallet/pay?address={address}&amount={amount_ton}&memo={memo}"
    }

async def check_ton_payment(address, memo, amount_ton):
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

async def create_cryptobot_invoice(user_id, username, stars, is_fiat):
    base_price_usd = float(get_setting("stars_price_usd") or 0.81) * (stars / 50)
    markup = float(get_setting("markup_percentage") or MARKUP_PERCENTAGE)
    commission = float(get_setting("card_commission" if is_fiat else "cryptobot_commission") or 30) / 100
    amount_usd = base_price_usd * (1 + markup / 100) * (1 + commission)

    headers = {"Authorization": f"Bearer {CRYPTOBOT_API_TOKEN}", "Content-Type": "application/json"}
    payload = {
        "amount": str(amount_usd) if not is_fiat else None,
        "currency": "USDT" if not is_fiat else None,
        "prices": [{"label": f"{stars} Stars", "amount": amount_usd * 100, "currency": "USD"}] if is_fiat else None,
        "description": f"Purchase {stars} Telegram Stars for @{username}",
        "metadata": {"user_id": str(user_id), "stars": stars, "username": username}
    }
    async with aiohttp.ClientSession() as session:
        for attempt in range(3):
            try:
                async with session.post("https://pay.crypt.bot/api/v3/invoices", headers=headers, json=payload) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get("ok"):
                            invoice = data.get("result", {})
                            with get_db_connection() as conn:
                                with conn.cursor() as cur:
                                    cur.execute(
                                        """
                                        UPDATE users
                                        SET stars_bought = %s,
                                            username = %s,
                                            cryptobot_invoice_id = %s
                                        WHERE user_id = %s
                                        """,
                                        (stars, username, invoice.get('invoice_id'), user_id)
                                    )
                                    conn.commit()
                            return invoice
                        logger.error(f"CryptoBot error: {data.get('error')}")
                    elif response.status == 429:
                        await asyncio.sleep(2 ** attempt * 5)
                    else:
                        logger.error(f"CryptoBot failed: {response.status}")
                    return None
            except Exception as e:
                logger.error(f"CryptoBot invoice error: {e}")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt * 5)
        return None

async def check_cryptobot_payment(invoice_id):
    headers = {"Authorization": f"Bearer {CRYPTOBOT_API_TOKEN}"}
    async with aiohttp.ClientSession() as session:
        for attempt in range(3):
            try:
                async with session.get(f"https://pay.crypt.bot/api/v3/invoices?invoice_ids={invoice_id}", headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get("ok"):
                            return data.get("result", {}).get("items", [{}])[0].get("status") == "paid"
                        logger.error(f"CryptoBot check error: {data.get('error')}")
                    elif response.status == 429:
                        await asyncio.sleep(2 ** attempt)
                    return False
            except Exception as e:
                logger.error(f"CryptoBot check error: {e}")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
        return False

async def notify_admin_purchase(context: ContextTypes.DEFAULT_TYPE, buyer_id: int, username: str, stars: int, amount: float, currency: str):
    admin_ids = get_setting("admin_ids") or [6956377285]
    for admin_id in admin_ids:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=f"New purchase!\nUser ID: {buyer_id}\nUsername: {username}\nStars: {stars}\nAmount: {amount:.2f} {currency}"
            )
        except Exception as e:
            logger.error(f"Failed to notify admin {admin_id}: {e}")

async def update_ton_price(context: ContextTypes.DEFAULT_TYPE):
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

async def clear_user_data(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    message_id = context.user_data.get('last_message_id')
    if message_id:
        try:
            await context.bot.delete_message(chat_id=user_id, message_id=message_id)
        except Exception:
            pass
    context.user_data.clear()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or f"user_{user_id}"
    args = context.args
    ref_id = int(args[0].split('_')[-1]) if args and args[0].startswith('ref_') else None
    logger.info(f"/start command by user {user_id}, ref_id={ref_id}")

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (user_id, username, referrer_id, language) VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (user_id) DO UPDATE SET username = %s",
                (user_id, username, ref_id, 'ru', username)
            )
            if ref_id:
                cur.execute(
                    "UPDATE users SET referrals = referrals || %s WHERE user_id = %s",
                    (json.dumps({"user_id": user_id, "username": username}), ref_id)
                )
            cur.execute("SELECT language FROM users WHERE user_id = %s", (user_id,))
            language = cur.fetchone()[0]
            conn.commit()

    await clear_user_data(context, user_id)
    if not language:
        keyboard = [
            [InlineKeyboardButton("Русский", callback_data="lang_ru"),
             InlineKeyboardButton("English", callback_data="lang_en")],
            [InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await update.message.reply_text(
            "Choose language:", reply_markup=reply_markup
        )
        context.user_data['last_message_id'] = message.message_id
        return CHOOSE_LANGUAGE
    await show_main_menu(update, context)
    return ConversationHandler.END
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ConversationHandler

async def start(update, context):
    """Обработчик команды /start и возврата в главное меню."""
    user_id = update.effective_user.id
    keyboard = [
        [InlineKeyboardButton("👤 Профиль", callback_data="profile")],
        [InlineKeyboardButton("🤝 Рефералы", callback_data="referrals")],
        [InlineKeyboardButton("🛠 Поддержка", callback_data="support")],
        [InlineKeyboardButton("📝 Отзывы", callback_data="reviews")],
        [InlineKeyboardButton("⭐ Купить звезды", callback_data="buy_stars")],
    ]
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT value FROM settings WHERE key = 'admin_ids'")
            result = await cur.fetchone()
            admin_ids = eval(result[0]) if result else []
            if user_id in admin_ids:
                keyboard.append([InlineKeyboardButton("🔧 Админ-панель", callback_data="admin_panel")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    text = await get_text("welcome", user_id, total_stars_sold=0)
    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    else:
        await update.message.reply_text(text, reply_markup=reply_markup)
    return ConversationHandler.END

async def admin_panel(update, context):
    """Отображает админ-панель."""
    user_id = update.effective_user.id
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT value FROM settings WHERE key = 'admin_ids'")
            result = await cur.fetchone()
            admin_ids = eval(result[0]) if result else []
    if user_id not in admin_ids:
        await update.callback_query.answer("Доступ запрещен!")
        return ConversationHandler.END
    keyboard = [
        [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton("📝 Изменить тексты", callback_data="admin_edit_texts")],
        [InlineKeyboardButton("👥 Статистика пользователей", callback_data="admin_user_stats")],
        [InlineKeyboardButton("💸 Изменить наценку", callback_data="admin_edit_markup")],
        [InlineKeyboardButton("👤 Добавить/удалить админа", callback_data="admin_manage_admins")],
        [InlineKeyboardButton("💰 Изменить прибыль", callback_data="admin_edit_profit")],
        [InlineKeyboardButton("⬅ Назад", callback_data="back_to_menu")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    text = "Админ-панель:"
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
    text = (
        f"📊 Статистика бота:\n"
        f"Прибыль: {total_profit_ton:.6f} TON\n"
        f"Проданные звезды: {total_stars_sold}\n"
        f"Пользователей: {user_count}"
    )
    keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data="back_to_admin")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    return ADMIN_PANEL

async def admin_edit_texts(update, context):
    """Подменю для изменения текстов."""
    keyboard = [
        [InlineKeyboardButton("Меню", callback_data="edit_text_welcome")],
        [InlineKeyboardButton("Отзывы", callback_data="edit_text_reviews")],
        [InlineKeyboardButton("⬅ Назад", callback_data="back_to_admin")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    text = "Выберите текст для редактирования:"
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    return ADMIN_EDIT_TEXTS

async def admin_user_stats(update, context):
    """Показывает список пользователей с поиском."""
    text = "Введите ID или имя пользователя для поиска (или /cancel для отмены):"
    keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data="back_to_admin")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    return ADMIN_USER_STATS
async def referrals(update, context):
    """Показывает реферальную информацию."""
    user_id = update.effective_user.id
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT referrals, ref_bonus_ton FROM users WHERE user_id = %s", (user_id,))
            result = await cur.fetchone()
            referrals = len(eval(result[0])) if result and result[0] else 0
            ref_bonus_ton = float(result[1]) if result and result[1] else 0
    bot_username = (await context.bot.get_me()).username
    ref_link = f"t.me/{bot_username}?start=ref_{user_id}"
    text = (
        f"🤝 Реферальная система:\n"
        f"Рефералов: {referrals}\n"
        f"Бонус: {ref_bonus_ton:.6f} TON\n"
        f"Ссылка: {ref_link}"
    )
    keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data="back_to_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    return REFERRALS


async def profile(update, context):
    """Показывает профиль пользователя."""
    user_id = update.effective_user.id
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT username, stars_bought FROM users WHERE user_id = %s", (user_id,))
            result = await cur.fetchone()
            username = result[0] if result else "Неизвестно"
            stars_bought = result[1] if result else 0
    text = f"👤 Профиль:\nИмя: {username}\nКуплено звезд: {stars_bought}"
    keyboard = [
        [InlineKeyboardButton("🏆 Топ рефералов", callback_data="top_referrals")],
        [InlineKeyboardButton("⭐ Топ покупок", callback_data="top_purchases")],
        [InlineKeyboardButton("⬅ Назад", callback_data="back_to_menu")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    return PROFILE


async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    keyboard = [
        [InlineKeyboardButton(get_text("buy_stars_prompt", user_id), callback_data="buy_stars")],
        [InlineKeyboardButton(get_text("profile", user_id), callback_data="profile"),
         InlineKeyboardButton(get_text("referrals", user_id), callback_data="referrals")],
        [InlineKeyboardButton("Tech Support", callback_data="tech_support"),
         InlineKeyboardButton("Reviews", callback_data="reviews")]
    ]
    if await is_admin(user_id):
        keyboard.append([InlineKeyboardButton(get_text("admin_panel", user_id), callback_data="admin_panel")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await clear_user_data(context, user_id)
    message = await (update.message or update.callback_query.message).reply_text(
        get_text("welcome", user_id, total_stars_sold=get_setting("total_stars_sold") or 0),
        reply_markup=reply_markup
    )
    context.user_data['last_message_id'] = message.message_id

async def show_buy_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    target_username = context.user_data.get('buy_username', '####')
    stars = context.user_data.get('buy_stars', '####')
    payment_method = context.user_data.get('payment_method', '####')
    base_price_usd = float(get_setting("stars_price_usd") or 0.81) * (int(stars) / 50 if stars != '####' else 1)
    markup = float(get_setting("markup_percentage") or MARKUP_PERCENTAGE)
    commission = (float(get_setting(f"{payment_method.lower().replace('@', '')}_commission") or 25) / 100
                 if payment_method != '####' and payment_method != 'Card' else
                 float(get_setting("card_commission") or 30) / 100 if payment_method == 'Card' else 0)
    amount_usd = base_price_usd * (1 + markup / 100) * (1 + commission) if stars != '####' else 0
    keyboard = [
        [InlineKeyboardButton(f"👤 @{target_username}", callback_data="set_username")],
        [InlineKeyboardButton(f"🌟 {stars}", callback_data="set_amount")],
        [InlineKeyboardButton(f"💸 {payment_method}", callback_data="set_payment_method")],
        [InlineKeyboardButton(f"💰 ${amount_usd:.2f}", callback_data="noop")],
        [InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel"),
         InlineKeyboardButton("✅ Оплатить", callback_data="confirm_payment")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await clear_user_data(context, user_id)
    message = await (update.message or update.callback_query.message).reply_text(
        get_text("buy_stars_prompt", user_id), reply_markup=reply_markup
    )
    context.user_data['last_message_id'] = message.message_id

async def show_admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    keyboard = [
        [InlineKeyboardButton(get_text("stats", user_id), callback_data="admin_stats")],
        [InlineKeyboardButton(get_text("edit_text_menu", user_id), callback_data="edit_text_menu")],
        [InlineKeyboardButton(get_text("user_stats", user_id), callback_data="user_stats")],
        [InlineKeyboardButton(get_text("edit_markup", user_id), callback_data="edit_markup")],
        [InlineKeyboardButton(get_text("manage_admins", user_id), callback_data="manage_admins")],
        [InlineKeyboardButton(get_text("edit_profit", user_id), callback_data="edit_profit")],
        [InlineKeyboardButton(get_text("back_btn", user_id), callback_data="back")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await clear_user_data(context, user_id)
    message = await update.callback_query.message.reply_text(
        get_text("admin_panel", user_id), reply_markup=reply_markup
    )
    context.user_data['last_message_id'] = message.message_id

async def show_admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    total_profit_usd = float(get_setting("total_profit_usd") or 0)
    total_profit_ton = float(get_setting("total_profit_ton") or 0)
    total_stars_sold = int(get_setting("total_stars_sold") or 0)
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM users")
            user_count = cur.fetchone()[0]
    keyboard = [[InlineKeyboardButton(get_text("back_btn", user_id), callback_data="admin_panel")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await clear_user_data(context, user_id)
    message = await update.callback_query.message.reply_text(
        get_text("stats", user_id, total_profit_usd=total_profit_usd, total_profit_ton=total_profit_ton,
                 total_stars_sold=total_stars_sold, user_count=user_count),
        reply_markup=reply_markup
    )
    context.user_data['last_message_id'] = message.message_id

async def show_edit_text_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    keyboard = [
        [InlineKeyboardButton(text="Welcome", callback_data="edit_text_welcome")],
        [InlineKeyboardButton(text="Buy Stars Prompt", callback_data="edit_text_buy_stars_prompt")],
        [InlineKeyboardButton(text="Profile", callback_data="edit_text_profile")],
        [InlineKeyboardButton(text="Referrals", callback_data="edit_text_referrals")],
        [InlineKeyboardButton(text="Support", callback_data="edit_text_support")],
        [InlineKeyboardButton(text="Reviews", callback_data="edit_text_reviews")],
        [InlineKeyboardButton(get_text("back_btn", user_id), callback_data="admin_panel")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await clear_user_data(context, user_id)
    message = await update.callback_query.message.reply_text(
        get_text("edit_text_menu", user_id), reply_markup=reply_markup
    )
    context.user_data['last_message_id'] = message.message_id

async def show_user_stats_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    keyboard = [[InlineKeyboardButton(get_text("back_btn", user_id), callback_data="admin_panel")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await clear_user_data(context, user_id)
    message = await update.callback_query.message.reply_text(
        get_text("user_stats", user_id),
        reply_markup=reply_markup
    )
    context.user_data['last_message_id'] = message.message_id
    context.user_data['input_state'] = 'user_search'
    return USER_SEARCH

async def show_user_info(update: Update, context: ContextTypes.DEFAULT_TYPE, username: str):
    user_id = update.effective_user.id
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT user_id, username, stars_bought, ref_bonus_ton, referrals FROM users WHERE username = %s",
                (username,)
            )
            user_data = cur.fetchone()
    if not user_data:
        await clear_user_data(context, user_id)
        message = await (update.message or update.callback_query.message).reply_text("User not found.")
        context.user_data['last_message_id'] = message.message_id
        return

    target_user_id, username, stars_bought, ref_bonus_ton, referrals = user_data
    ref_count = len(json.loads(referrals) if referrals else [])
    keyboard = [
        [InlineKeyboardButton("Change Stars", callback_data=f"edit_user_stars_{target_user_id}")],
        [InlineKeyboardButton("Change Ref. Bonus", callback_data=f"edit_user_ref_bonus_{target_user_id}")],
        [InlineKeyboardButton(get_text("back_btn", user_id), callback_data="user_stats")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await clear_user_data(context, user_id)
    message = await (update.message or update.callback_query.message).reply_text(
        get_text("user_info", user_id, username=username, stars_bought=stars_bought,
                 ref_bonus_ton=ref_bonus_ton, ref_count=ref_count),
        reply_markup=reply_markup
    )
    context.user_data['last_message_id'] = message.message_id

async def show_edit_markup_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    keyboard = [
        [InlineKeyboardButton("TON Wallet", callback_data="edit_markup_ton")],
        [InlineKeyboardButton("CryptoBot (Crypto)", callback_data="edit_markup_cryptobot_crypto"),
         InlineKeyboardButton("CryptoBot (Card)", callback_data="edit_markup_cryptobot_fiat")],
        [InlineKeyboardButton("Referral Bonus", callback_data="edit_markup_ref")],
        [InlineKeyboardButton(get_text("back_btn", user_id), callback_data="admin_panel")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await clear_user_data(context, user_id)
    message = await update.callback_query.message.reply_text(
        get_text("edit_markup", user_id), reply_markup=reply_markup
    )
    context.user_data['last_message_id'] = message.message_id

async def show_manage_admins_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    keyboard = [
        [InlineKeyboardButton("Add Admin", callback_data="add_admin"),
         InlineKeyboardButton("Remove Admin", callback_data="remove_admin")],
        [InlineKeyboardButton(get_text("back_btn", user_id), callback_data="admin_panel")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await clear_user_data(context, user_id)
    message = await update.callback_query.message.reply_text(
        get_text("manage_admins", user_id), reply_markup=reply_markup
    )
    context.user_data['last_message_id'] = message.message_id

async def show_edit_profit_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    keyboard = [[InlineKeyboardButton(get_text("back_btn", user_id), callback_data="admin_panel")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await clear_user_data(context, user_id)
    message = await update.callback_query.message.reply_text(
        "Enter new profit percentage:",
        reply_markup=reply_markup
    )
    context.user_data['last_message_id'] = message.message_id
    context.user_data['input_state'] = 'profit_percent'
    return RESET_PROFIT
import time

async def buy_stars(update, context):
    """Начинает процесс покупки звезд."""
    user_id = update.effective_user.id
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT username FROM users WHERE user_id = %s", (user_id,))
            username = (await cur.fetchone())[0] if await cur.rowcount else None
    context.user_data["username"] = username
    text = "Введите количество звезд для покупки:"
    keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data="back_to_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
    return BUY_STARS_AMOUNT

async def check_payment(update, context):
    """Проверяет оплату по нажатию кнопки."""
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
                await conn.commit()
        text = await get_text("buy_success", user_id, stars=stars)
        keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data="back_to_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        return ConversationHandler.END
    else:
        text = "Оплата не подтверждена. Попробуйте снова."
        keyboard = [
            [InlineKeyboardButton("Проверить", callback_data="check_payment")],
            [InlineKeyboardButton("⬅ Назад", callback_data="back_to_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup)
        return BUY_STARS_PAYMENT_METHOD


async def button_handler(update, context):
    """Обрабатывает нажатия кнопок."""
    query = update.callback_query
    data = query.data
    if data == "back_to_menu":
        return await start(update, context)
    elif data == "back_to_admin":
        return await admin_panel(update, context)
    elif data == "profile":
        return await profile(update, context)
    elif data == "referrals":
        return await referrals(update, context)
    elif data == "support":
        text = await get_text("tech_support", update.effective_user.id, support_channel="@Support")
        keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data="back_to_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text, reply_markup=reply_markup)
        return ConversationHandler.END
    elif data == "reviews":
        text = await get_text("reviews", update.effective_user.id)
        keyboard = [[InlineKeyboardButton("⬅ Назад", callback_data="back_to_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text, reply_markup=reply_markup)
        return ConversationHandler.END
    elif data == "buy_stars":
        return await buy_stars(update, context)
    elif data == "admin_panel":
        return await admin_panel(update, context)
    elif data == "admin_stats":
        return await admin_stats(update, context)
    elif data == "admin_edit_texts":
        return await admin_edit_texts(update, context)
    elif data == "admin_user_stats":
        return await admin_user_stats(update, context)
    elif data == "check_payment":
        return await check_payment(update, context)
    await query.answer()
    return ConversationHandler.END

async def handle_text_input(update, context):
    """Обрабатывает текстовый ввод."""
    state = context.user_data.get("state", update.current_state)
    text = update.message.text
    if state == BUY_STARS_AMOUNT:
        try:
            stars = int(text)
            if stars <= 0:
                raise ValueError
            context.user_data["stars"] = stars
            amount_ton = stars * 0.0001  # Настройте расчет
            context.user_data["amount_ton"] = amount_ton
            context.user_data["memo"] = f"order_{update.effective_user.id}_{int(time.time())}"
            context.user_data["address"] = "UQB_XcBjornHoP0aIf6ofn-wT8ru5QPsgYKtyPrlbgKsXrrX"  # Настройте адрес
            username = context.user_data.get("username", "Не указан")
            text = await get_text(
                "buy_prompt",
                update.effective_user.id,
                amount_ton=amount_ton,
                stars=stars,
                address=context.user_data["address"],
                memo=context.user_data["memo"],
                username=username
            )
            keyboard = [
                [InlineKeyboardButton("Проверить", callback_data="check_payment")],
                [InlineKeyboardButton("⬅ Назад", callback_data="back_to_menu")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(text, reply_markup=reply_markup)
            return BUY_STARS_PAYMENT_METHOD
        except ValueError:
            await update.message.reply_text("Введите корректное количество звезд!")
            return BUY_STARS_AMOUNT
    return ConversationHandler.END

async def main():
    """Запускает бот."""
    init_db()
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            CHOOSE_LANGUAGE: [CallbackQueryHandler(button_handler, pattern=r"^lang_|^cancel$")],
            BUY_STARS_USERNAME: [
                CallbackQueryHandler(button_handler, pattern=r"^set_username|payment_|confirm_|check_|cancel$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
            ],
            BUY_STARS_AMOUNT: [
                CallbackQueryHandler(button_handler, pattern=r"^set_amount|cancel$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
            ],
            BUY_STARS_PAYMENT_METHOD: [
                CallbackQueryHandler(button_handler, pattern=r"^payment_|check_payment$"),
            ],
            ADMIN_PANEL: [CallbackQueryHandler(button_handler, pattern=r"^admin_|back_to_menu$")],
            ADMIN_STATS: [CallbackQueryHandler(button_handler, pattern=r"^back_to_admin$")],
            ADMIN_EDIT_TEXTS: [CallbackQueryHandler(button_handler, pattern=r"^edit_text_|back_to_admin$")],
            ADMIN_USER_STATS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                CallbackQueryHandler(button_handler, pattern=r"^back_to_admin$"),
            ],
            PROFILE: [CallbackQueryHandler(button_handler, pattern=r"^top_|back_to_menu$")],
            REFERRALS: [CallbackQueryHandler(button_handler, pattern=r"^back_to_menu$")],
        },
        fallbacks=[
            CommandHandler("start", start),
            CommandHandler("cancel", start),
            CallbackQueryHandler(button_handler, pattern=r"^cancel|back_to_menu$"),
        ],
        per_message=True,  # Исправляет PTBUserWarning
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
