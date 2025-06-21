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

def get_text(key, user_id, **kwargs):
    language = get_user_language(user_id)
    key_with_lang = f"{key}_{language}"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM texts WHERE key = %s", (key_with_lang,))
            result = cur.fetchone()
            return result[0].format(**kwargs) if result else f"Text not found: {key_with_lang}"

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

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        logger.error("No callback query found")
        return
    await query.answer()
    user_id = update.effective_user.id
    data = query.data
    logger.info(f"Callback received: {data} from user {user_id}")

    if data in ["cancel", "back"]:
        await show_main_menu(update, context)
        return ConversationHandler.END

    if data.startswith("lang_"):
        language = data.split("_")[1]
        update_user_language(user_id, language)
        await show_main_menu(update, context)
        return ConversationHandler.END

    elif data == "buy_stars":
        context.user_data['buy_username'] = context.user_data.get('buy_username', '')
        context.user_data['buy_stars'] = context.user_data.get('buy_stars', '')
        context.user_data['payment_method'] = context.user_data.get('payment_method', '')
        await show_buy_menu(update, context)
        return BUY_STARS_USERNAME

    elif data == "set_username":
        keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await clear_user_data(context, user_id)
        message = await query.message.reply_text(
            get_text("buy_username_prompt", user_id), reply_markup=reply_markup
        )
        context.user_data['last_message_id'] = message.message_id
        context.user_data['input_state'] = 'buy_username'
        return BUY_STARS_USERNAME

    elif data == "set_amount":
        keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await clear_user_data(context, user_id)
        message = await query.message.reply_text(
            get_text("buy_amount_prompt", user_id, min_stars=get_setting("min_stars_purchase") or 10),
            reply_markup=reply_markup
        )
        context.user_data['last_message_id'] = message.message_id
        context.user_data['input_state'] = 'buy_amount'
        return BUY_STARS_AMOUNT

    elif data == "set_payment_method":
        keyboard = [
            [InlineKeyboardButton("💳 Card", callback_data="payment_card")],
            [InlineKeyboardButton("💸 TON", callback_data="payment_ton")],
            [InlineKeyboardButton("💸 CryptoBot (Crypto)", callback_data="payment_cryptobot_crypto")],
            [InlineKeyboardButton("💸 CryptoBot (Card)", callback_data="payment_cryptobot_fiat")],
            [InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await clear_user_data(context, user_id)
        message = await query.message.reply_text(
            get_text("buy_payment_method_prompt", user_id), reply_markup=reply_markup
        )
        context.user_data['last_message_id'] = message.message_id
        return BUY_STARS_PAYMENT_METHOD

    elif data == "payment_card":
        if not get_setting("card_payment_enabled"):
            await clear_user_data(context, user_id)
            message = await query.message.reply_text(get_text("buy_card_disabled", user_id))
            context.user_data['last_message_id'] = message.message_id
            await show_buy_menu(update, context)
            return BUY_STARS_USERNAME
        context.user_data['payment_method'] = 'Card'
        await show_buy_menu(update, context)
        return BUY_STARS_USERNAME

    elif data == "payment_ton":
        context.user_data['payment_method'] = 'TON Wallet'
        await show_buy_menu(update, context)
        return BUY_STARS_USERNAME

    elif data == "payment_cryptobot_crypto":
        context.user_data['payment_method'] = '@CryptoBot'
        await show_buy_menu(update, context)
        return BUY_STARS_USERNAME

    elif data == "payment_cryptobot_fiat":
        context.user_data['payment_method'] = 'CryptoBot Card'
        await show_buy_menu(update, context)
        return BUY_STARS_USERNAME

    elif data == "confirm_payment":
        target_username = context.user_data.get('buy_username')
        stars = context.user_data.get('buy_stars')
        payment_method = context.user_data.get('payment_method')
        if not (target_username and stars and payment_method):
            await clear_user_data(context, user_id)
            message = await query.message.reply_text("Please fill all fields")
            context.user_data['last_message_id'] = message.message_id
            await show_buy_menu(update, context)
            return BUY_STARS_USERNAME

        async with aiohttp.ClientSession() as session:
            if payment_method in ['@CryptoBot', 'CryptoBot Card']:
                invoice_info = await create_cryptobot_invoice(user_id, target_username, int(stars), is_fiat=payment_method == 'CryptoBot Card')
                if invoice_info:
                    keyboard = [
                        [InlineKeyboardButton("✅ Pay", url=invoice_info.get('pay_url'))],
                        [InlineKeyboardButton("🔄 Check", callback_data="check_payment")],
                        [InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    await clear_user_data(context, user_id)
                    message = await query.message.reply_text(
                        get_text(
                            f"buy_{'card' if payment_method == 'CryptoBot Card' else 'cryptobot'}_prompt",
                            user_id,
                            amount_usd=invoice_info.get('amount', 0),
                            stars=stars,
                            username=target_username
                        ),
                        reply_markup=reply_markup
                    )
                    context.user_data['last_message_id'] = message.message_id
                else:
                    await clear_user_data(context, user_id)
                    message = await query.message.reply_text("Failed to create invoice")
                    context.user_data['last_message_id'] = message.message_id
                    await show_main_menu(update, context)
                return ConversationHandler.END

            elif payment_method == 'TON Wallet':
                payment_info = await create_ton_payment(user_id, target_username, int(stars))
                keyboard = [
                    [InlineKeyboardButton("✅ Pay", url=payment_info["url"])],
                    [InlineKeyboardButton("🔄 Check", callback_data="check_payment")],
                    [InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await clear_user_data(context, user_id)
                message = await query.message.reply_text(
                    get_text(
                        "buy_ton_prompt",
                        user_id,
                        amount_ton=payment_info["amount_ton"],
                        stars=stars,
                        address=payment_info["address"],
                        memo=payment_info["memo"],
                        username=target_username
                    ),
                    reply_markup=reply_markup
                )
                context.user_data['last_message_id'] = message.message_id
                return ConversationHandler.END

    elif data == "check_payment":
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT username, stars_bought, address, memo, amount_ton, cryptobot_invoice_id 
                    FROM users WHERE user_id = %s
                    """,
                    (user_id,)
                )
                result = cur.fetchone()
                if not result:
                    await clear_user_data(context, user_id)
                    message = await query.message.reply_text("No active orders found.")
                    context.user_data['last_message_id'] = message.message_id
                    await show_main_menu(update, context)
                    return ConversationHandler.END

            target_username, stars, address, memo, amount_ton, invoice_id = result
            paid = False
            async with aiohttp.ClientSession() as session:
                if invoice_id:
                    paid = await check_cryptobot_payment(invoice_id)
                elif address and memo and amount_ton:
                    paid = await check_ton_payment(address, memo, amount_ton)

                if paid and await issue_stars_api(session, target_username, stars):
                    with get_db_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                UPDATE users SET stars_bought = NULL, address = NULL, memo = NULL, 
                                amount_ton = NULL, cryptobot_invoice_id = NULL WHERE user_id = %s
                                """,
                                (user_id,)
                            )
                            total_stars_sold = int(get_setting("total_stars_sold") or 0) + stars
                            base_price_usd = float(get_setting("stars_price_usd") or 0.81) * (stars / 50)
                            markup = float(get_setting("markup_percentage") or MARKUP_PERCENTAGE)
                            total_profit_usd = float(get_setting("total_profit_usd") or 0) + (base_price_usd * (markup / 100))
                            total_profit_ton = float(get_setting("total_profit_ton") or 0) + (amount_ton or 0)
                            update_setting("total_stars_sold", total_stars_sold)
                            update_setting("total_profit_usd", total_profit_usd)
                            update_setting("total_profit_ton", total_profit_ton)
                            conn.commit()
                    currency = "TON" if amount_ton else "USD"
                    amount = amount_ton if amount_ton else base_price_usd * (1 + markup / 100)
                    await notify_admin_purchase(context, user_id, target_username, stars, amount, currency)
                    await clear_user_data(context, user_id)
                    message = await query.message.reply_text(
                        get_text("buy_success", user_id, username=target_username, stars=stars)
                    )
                    context.user_data['last_message_id'] = message.message_id
                    await show_main_menu(update, context)
                else:
                    await clear_user_data(context, user_id)
                    message = await query.message.reply_text("Payment not confirmed")
                    context.user_data['last_message_id'] = message.message_id
                    await show_main_menu(update, context)
                return ConversationHandler.END

    elif data == "profile":
        stars_bought = get_user_data(user_id, "stars_bought") or 0
        ref_bonus_ton = get_user_data(user_id, "ref_bonus_ton") or 0
        username = get_user_data(user_id, "username") or f"user_{user_id}"
        keyboard = [
            [InlineKeyboardButton("Top Referrals", callback_data="top_referrals"),
             InlineKeyboardButton("Top Purchases", callback_data="top_purchases")],
            [InlineKeyboardButton(get_text("back_btn", user_id), callback_data="back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await clear_user_data(context, user_id)
        message = await query.message.reply_text(
            get_text("profile", user_id, username=username, stars_bought=stars_bought, ref_bonus_ton=ref_bonus_ton),
            reply_markup=reply_markup
        )
        context.user_data['last_message_id'] = message.message_id

    elif data == "referrals":
        ref_bonus_ton = get_user_data(user_id, "ref_bonus_ton") or 0
        referrals = get_user_data(user_id, "referrals") or []
        ref_count = len(referrals)
        ref_link = f"https://t.me/{context.bot.username}?start=ref_{user_id}"
        keyboard = [[InlineKeyboardButton(get_text("back_btn", user_id), callback_data="back")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await clear_user_data(context, user_id)
        message = await query.message.reply_text(
            get_text("referrals", user_id, ref_count=ref_count, ref_bonus_ton=ref_bonus_ton, ref_link=ref_link),
            reply_markup=reply_markup
        )
        context.user_data['last_message_id'] = message.message_id

    elif data == "tech_support":
        support_channel = get_setting("support_channel") or "@support_channel"
        keyboard = [[InlineKeyboardButton(get_text("back_btn", user_id), callback_data="back")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await clear_user_data(context, user_id)
        message = await query.message.reply_text(
            f"Contact support: {support_channel}",
            reply_markup=reply_markup
        )
        context.user_data['last_message_id'] = message.message_id

    elif data == "reviews":
        review_channel = get_setting("review_channel") or "@sacoectasy"
        keyboard = [[InlineKeyboardButton(get_text("back_btn", user_id), callback_data="back")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await clear_user_data(context, user_id)
        message = await query.message.reply_text(
            f"Read reviews: {review_channel}",
            reply_markup=reply_markup
        )
        context.user_data['last_message_id'] = message.message_id

    elif data == "admin_panel":
        await show_admin_panel(update, context)
        return ConversationHandler.END

    elif data == "admin_stats":
        await show_admin_stats(update, context)
        return ConversationHandler.END

    elif data == "edit_text_menu":
        await show_edit_text_menu(update, context)
        return ConversationHandler.END

    elif data.startswith("edit_text_"):
        text_key = data.split("_")[-1]
        context.user_data['edit_text_key'] = text_key
        keyboard = [[InlineKeyboardButton(get_text("back_btn", user_id), callback_data="edit_text_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await clear_user_data(context, user_id)
        message = await query.message.reply_text(
            f"Enter new text for {text_key}:",
            reply_markup=reply_markup
        )
        context.user_data['last_message_id'] = message.message_id
        context.user_data['input_state'] = 'edit_text'
        return SET_TEXT_KEY

    elif data == "user_stats":
        await show_user_stats_menu(update, context)
        return USER_SEARCH

    elif data == "edit_markup":
        await show_edit_markup_menu(update, context)
        return ConversationHandler.END

    elif data.startswith("edit_markup_"):
        markup_type = data.split("_")[-1]
        context.user_data['markup_type'] = markup_type
        keyboard = [[InlineKeyboardButton(get_text("back_btn", user_id), callback_data="edit_markup")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await clear_user_data(context, user_id)
        message = await query.message.reply_text(
            f"Enter new percentage for {markup_type}:",
            reply_markup=reply_markup
        )
        context.user_data['last_message_id'] = message.message_id
        context.user_data['input_state'] = 'set_markup'
        return SET_MARKUP

    elif data == "manage_admins":
        await show_manage_admins_menu(update, context)
        return ConversationHandler.END

    elif data in ["add_admin", "remove_admin"]:
        context.user_data['admin_action'] = data
        keyboard = [[InlineKeyboardButton(get_text("back_btn", user_id), callback_data="manage_admins")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await clear_user_data(context, user_id)
        message = await query.message.reply_text(
            f"Enter user ID to {'add' if data == 'add_admin' else 'remove'} admin:",
            reply_markup=reply_markup
        )
        context.user_data['last_message_id'] = message.message_id
        context.user_data['input_state'] = data
        return ADD_ADMIN if data == "add_admin" else REMOVE_ADMIN

    elif data == "edit_profit":
        await show_edit_profit_menu(update, context)
        return RESET_PROFIT

    elif data.startswith("edit_user_stars_"):
        target_user_id = int(data.split("_")[-1])
        context.user_data['target_user_id'] = target_user_id
        keyboard = [[InlineKeyboardButton(get_text("back_btn", user_id), callback_data="user_stats")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await clear_user_data(context, user_id)
        message = await query.message.reply_text(
            "Enter new star balance:",
            reply_markup=reply_markup
        )
        context.user_data['last_message_id'] = message.message_id
        context.user_data['input_state'] = 'edit_user_stars'
        return EDIT_USER_STARS

    elif data.startswith("edit_user_ref_bonus_"):
        target_user_id = int(data.split("_")[-1])
        context.user_data['target_user_id'] = target_user_id
        keyboard = [[InlineKeyboardButton(get_text("back_btn", user_id), callback_data="user_stats")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await clear_user_data(context, user_id)
        message = await query.message.reply_text(
            "Enter new ref bonus (TON):",
            reply_markup=reply_markup
        )
        context.user_data['last_message_id'] = message.message_id
        context.user_data['input_state'] = 'edit_user_ref_bonus'
        return EDIT_USER_REF_BONUS

async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()
    input_state = context.user_data.get('input_state')

    if input_state == 'buy_username':
        if text.startswith('@'):
            text = text[1:]
        context.user_data['buy_username'] = text
        await show_buy_menu(update, context)
        return BUY_STARS_USERNAME

    elif input_state == 'buy_amount':
        try:
            stars = int(text)
            min_stars = int(get_setting("min_stars_purchase") or 10)
            if stars < min_stars:
                raise ValueError
            context.user_data['buy_stars'] = str(stars)
            await show_buy_menu(update, context)
            return BUY_STARS_USERNAME
        except ValueError:
            message = await update.message.reply_text("Invalid amount. Enter a number.")
            context.user_data['last_message_id'] = message.message_id
            return BUY_STARS_AMOUNT

    elif input_state == 'user_search':
        await show_user_info(update, context, text)
        return ConversationHandler.END

    elif input_state == 'edit_text':
        text_key = context.user_data.get('edit_text_key')
        language = get_user_language(user_id)
        update_text(f"{text_key}_{language}", text)
        log_admin_action(user_id, f"Updated text: {text_key}_{language}")
        await show_edit_text_menu(update, context)
        return ConversationHandler.END

    elif input_state == 'set_markup':
        try:
            percentage = float(text)
            markup_type = context.user_data.get('markup_type')
            setting_key = {
                'ton': 'ton_commission',
                'cryptobot_crypto': 'cryptobot_commission',
                'cryptobot_fiat': 'card_commission',
                'ref': 'ref_bonus_percent'
            }.get(markup_type)
            update_setting(setting_key, percentage)
            log_admin_action(user_id, f"Updated markup {setting_key}: {percentage}%")
            await show_edit_markup_menu(update, context)
            return ConversationHandler.END
        except ValueError:
            message = await update.message.reply_text("Invalid percentage. Enter a number.")
            context.user_data['last_message_id'] = message.message_id
            return SET_MARKUP

    elif input_state in ['add_admin', 'remove_admin']:
        try:
            target_id = int(text)
            admin_ids = get_setting("admin_ids") or [6956377285]
            if input_state == 'add_admin' and target_id not in admin_ids:
                admin_ids.append(target_id)
                update_setting("admin_ids", admin_ids)
                log_admin_action(user_id, f"Added admin: {target_id}")
            elif input_state == 'remove_admin' and target_id in admin_ids:
                admin_ids.remove(target_id)
                update_setting("admin_ids", admin_ids)
                log_admin_action(user_id, f"Removed admin: {target_id}")
            await show_manage_admins_menu(update, context)
            return ConversationHandler.END
        except ValueError:
            message = await update.message.reply_text("Invalid user ID.")
            context.user_data['last_message_id'] = message.message_id
            return ADD_ADMIN if input_state == 'add_admin' else REMOVE_ADMIN

    elif input_state == 'edit_user_stars':
        try:
            stars = int(text)
            target_user_id = context.user_data.get('target_user_id')
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE users SET stars_bought = %s WHERE user_id = %s",
                        (stars, target_user_id)
                    )
                    conn.commit()
            log_admin_action(user_id, f"Updated stars for user {target_user_id}: {stars}")
            await show_user_stats_menu(update, context)
            return ConversationHandler.END
        except ValueError:
            message = await update.message.reply_text("Invalid number.")
            context.user_data['last_message_id'] = message.message_id
            return EDIT_USER_STARS

    elif input_state == 'edit_user_ref_bonus':
        try:
            ref_bonus = float(text)
            target_user_id = context.user_data.get('target_user_id')
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE users SET ref_bonus_ton = %s WHERE user_id = %s",
                        (ref_bonus, target_user_id)
                    )
                    conn.commit()
            log_admin_action(user_id, f"Updated ref bonus for user {target_user_id}: {ref_bonus} TON")
            await show_user_stats_menu(update, context)
            return ConversationHandler.END
        except ValueError:
            message = await update.message.reply_text("Invalid number.")
            context.user_data['last_message_id'] = message.message_id
            return EDIT_USER_REF_BONUS

    elif input_state == 'profit_percent':
        try:
            percentage = float(text)
            update_setting("profit_percent", percentage)
            log_admin_action(user_id, f"Updated profit: {percentage}%")
            await show_admin_panel(update, context)
            return ConversationHandler.END
        except ValueError:
            message = await update.message.reply_text("Invalid percentage.")
            context.user_data['last_message_id'] = message.message_id
            return RESET_PROFIT

async def main():
    init_db()
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    conv_handler = ConversationHandler(
        entry_points={
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
                CallbackQueryHandler(button_handler, pattern=r"^payment_|cancel$"),
            ],
            SET_TEXT_KEY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                CallbackQueryHandler(button_handler, pattern=r"^edit_text_|cancel$"),
            ],
            USER_SEARCH: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                CallbackQueryHandler(button_handler, pattern=r"^user_stats|back$"),
            ],
            SET_MARKUP: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                CallbackQueryHandler(button_handler, pattern=r"^edit_markup_|cancel$"),
            ],
            ADD_ADMIN: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                CallbackQueryHandler(button_handler, pattern=r"^add_admin|cancel$"),
            ],
            REMOVE_ADMIN: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                CallbackQueryHandler(button_handler, pattern=r"^remove_admin|cancel$"),
            ],
            EDIT_USER_STARS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                CallbackQueryHandler(button_handler, pattern=r"^edit_user_stars_|cancel$"),
            ],
            EDIT_USER_REF_BONUS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                CallbackQueryHandler(button_handler, pattern=r"^edit_user_ref_bonus_|cancel$"),
            ],
            RESET_PROFIT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input),
                CallbackQueryHandler(button_handler, pattern=r"^edit_profit|cancel$"),
            ],
        },
        fallbacks=[
            CommandHandler("start", start),
            CommandHandler("cancel", start),
            CallbackQueryHandler(button_handler, pattern=r"^cancel|back$"),
        ],
    )

    app.add_handler(conv_handler)
    app.job_queue.run_repeating(update_ton_price, interval=600, first=10)
    await app.run_polling()

if __name__ == "__main__":
    asyncio.run(main())
