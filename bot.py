import os
import time
import json
import logging
import asyncio
import aiohttp
import psycopg2
from urllib.parse import urlparse
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
    ConversationHandler,
)

load_dotenv()

# Логирование
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Константы
BOT_TOKEN = os.getenv("BOT_TOKEN")
TONAPI_KEY = os.getenv("TONAPI_KEY")
POSTGRES_URL = os.getenv("POSTGRES_URL")
SPLIT_API_ID = os.getenv("SPLIT_API_ID")
SPLIT_API_TOKEN = os.getenv("SPLIT_API_TOKEN")
OWNER_WALLET = os.getenv("OWNER_WALLET")

# Состояния для ConversationHandler
EDIT_TEXT, SET_PRICE, SET_PERCENT, SET_REVIEW_CHANNEL, CHOOSE_LANGUAGE = range(5)

def get_db_connection():
    postgres_url = os.getenv("POSTGRES_URL") or os.getenv("DATABASE_URL")
    if not postgres_url:
        logger.error("POSTGRES_URL or DATABASE_URL not set in environment variables")
        raise ValueError("POSTGRES_URL or DATABASE_URL is not set")
    
    try:
        # Парсим URL для извлечения компонентов
        parsed_url = urlparse(postgres_url)
        dbname = parsed_url.path.lstrip('/')
        user = parsed_url.username
        password = parsed_url.password
        host = parsed_url.hostname
        port = parsed_url.port or 5432
        
        logger.info(f"Connecting to PostgreSQL: host={host}, port={port}, dbname={dbname}")
        
        return psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise

# Инициализация базы данных
def init_db():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    stars_bought INTEGER DEFAULT 0,
                    ref_bonus_ton FLOAT DEFAULT 0,
                    referrer_id BIGINT,
                    referrals JSONB DEFAULT '[]',
                    bonus_history JSONB DEFAULT '[]',
                    address TEXT,
                    memo TEXT,
                    amount_ton FLOAT,
                    language TEXT DEFAULT 'ru'
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS admin_log (
                    id SERIAL PRIMARY KEY,
                    admin_id BIGINT,
                    action TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS texts (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
            """)
            cur.execute("""
                INSERT INTO settings (key, value)
                VALUES
                    ('admin_ids', '[8028944732]'),
                    ('ref_bonus_percent', '30'),
                    ('profit_percent', '20'),
                    ('total_stars_sold', '0'),
                    ('total_profit_usd', '0'),
                    ('total_profit_ton', '0'),
                    ('stars_price_usd', '0.972'),
                    ('stars_per_purchase', '50'),
                    ('ton_exchange_rate', '2.93'),
                    ('review_channel', '@sacoectasy')
                ON CONFLICT (key) DO NOTHING;
            """)
            cur.execute("""
                INSERT INTO texts (key, value)
                VALUES
                    ('welcome_ru', 'Добро пожаловать! Купите Telegram Stars за TON.\nЗвезд продано: {total_stars_sold}'),
                    ('welcome_en', 'Welcome! Buy Telegram Stars with TON.\nStars sold: {total_stars_sold}'),
                    ('buy_prompt_ru', 'Оплатите {amount_ton:.6f} TON для {stars} звезд через TON Space.\nАдрес: {address}\nMemo: {memo}'),
                    ('buy_prompt_en', 'Pay {amount_ton:.6f} TON for {stars} stars via TON Space.\nAddress: {address}\nMemo: {memo}'),
                    ('buy_success_ru', 'Оплата прошла! Вы получили {stars} звезд.'),
                    ('buy_success_en', 'Payment successful! You received {stars} stars.'),
                    ('ref_info_ru', 'Ваш реф. бонус: {ref_bonus_ton:.6f} TON\nРеф. ссылка: t.me/{bot_username}?start=ref_{user_id}'),
                    ('ref_info_en', 'Your ref. bonus: {ref_bonus_ton:.6f} TON\nRef. link: t.me/{bot_username}?start=ref_{user_id}'),
                    ('tech_support_ru', 'Свяжитесь с техподдержкой: {support_channel}'),
                    ('tech_support_en', 'Contact support: {support_channel}'),
                    ('reviews_ru', 'Оставьте отзыв: {review_channel}'),
                    ('reviews_en', 'Leave a review: {review_channel}'),
                    ('choose_language_ru', 'Выберите язык:'),
                    ('choose_language_en', 'Choose language:')
                ON CONFLICT (key) DO NOTHING;
            """)
            conn.commit()

# Получение настроек
def get_setting(key):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM settings WHERE key = %s;", (key,))
            result = cur.fetchone()
            if result:
                if key in ('admin_ids', 'referrals', 'bonus_history'):
                    return json.loads(result[0])
                return float(result[0]) if key in ('ref_bonus_percent', 'profit_percent', 'stars_price_usd', 'ton_exchange_rate') else result[0]
    return None

# Обновление настроек
def update_setting(key, value):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = %s;",
                (key, json.dumps(value) if isinstance(value, list) else str(value), json.dumps(value) if isinstance(value, list) else str(value))
            )
            conn.commit()

# Получение текста
def get_text(key, user_id, **kwargs):
    language = get_user_language(user_id)
    key_with_lang = f"{key}_{language}"
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM texts WHERE key = %s;", (key_with_lang,))
            result = cur.fetchone()
            if result:
                return result[0].format(**kwargs)
    return f"Text not found: {key_with_lang}"

# Обновление текста
def update_text(key, value):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO texts (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = %s;",
                (key, value, value)
            )
            conn.commit()

# Проверка админа
def is_admin(user_id):
    admin_ids = get_setting("admin_ids") or [8028944732]
    return user_id in admin_ids

# Логирование действий админа
def log_admin_action(admin_id, action):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO admin_log (admin_id, action) VALUES (%s, %s);", (admin_id, action))
            conn.commit()

# Получение языка пользователя
def get_user_language(user_id):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT language FROM users WHERE user_id = %s;", (user_id,))
            result = cur.fetchone()
            return result[0] if result else 'ru'

# Обновление языка пользователя
def update_user_language(user_id, language):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET language = %s WHERE user_id = %s;",
                (language, user_id)
            )
            conn.commit()

# Обновление курса TON через CoinGecko
async def update_ton_price(context: ContextTypes.DEFAULT_TYPE):
    async with aiohttp.ClientSession() as session:
        async with session.get("https://api.coingecko.com/api/v3/simple/price?ids=the-open-network&vs_currencies=usd") as response:
            if response.status == 200:
                data = await response.json()
                ton_price = data.get("the-open-network", {}).get("usd", 2.93)
                update_setting("ton_exchange_rate", ton_price)
                logger.info(f"TON price updated: ${ton_price}")
            else:
                logger.error(f"Failed to update TON price: {response.status}")

# Выдача звёзд через API Split.tg
async def issue_stars_api(username, stars):
    api_url = "https://api.split.tg/buy/stars"
    headers = {
        "Authorization": f"Bearer {SPLIT_API_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    payload = {
        "username": username.lstrip("@"),
        "payment_method": "ton_connect",
        "quantity": stars
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(api_url, headers=headers, json=payload) as response:
            response_text = await response.text()
            if response.status == 200:
                data = json.loads(response_text)
                if data.get("ok", False):
                    logger.info(f"Звезды выданы: @{username}, {stars}")
                    return True
                else:
                    logger.error(f"Split.tg API error: {data.get('error_message')}")
                    return False
            else:
                logger.error(f"Split.tg API request failed: {response.status}, {response_text}")
                return False

# Генерация TON-адреса (TON Space)
async def generate_ton_address(user_id):
    return {"address": OWNER_WALLET, "memo": f"order_{user_id}_{int(time.time())}"}

# Проверка оплаты TON (через tonapi.io)
async def check_ton_payment(address, memo, amount_ton):
    headers = {"Authorization": f"Bearer {TONAPI_KEY}"}
    async with aiohttp.ClientSession() as session:
        async with session.get(f"https://tonapi.io/v2/transactions?address={address}", headers=headers) as response:
            if response.status == 200:
                transactions = await response.json()
                for tx in transactions.get("transactions", []):
                    if tx.get("memo") == memo and float(tx.get("amount", 0)) / 1e9 >= amount_ton:
                        return True
    return False

# Проверка оплаты (фоновый процесс)
async def payment_checker(context: ContextTypes.DEFAULT_TYPE):
    while True:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id, username, stars_bought, address, memo, amount_ton FROM users WHERE stars_bought > 0 AND address IS NOT NULL;")
                pending = cur.fetchall()
        for user_id, username, stars, address, memo, amount_ton in pending:
            if await check_ton_payment(address, memo, amount_ton):
                if await issue_stars_api(username, stars):
                    with get_db_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE users SET stars_bought = 0 WHERE user_id = %s;",
                                (user_id,)
                            )
                            total_stars_sold = int(get_setting("total_stars_sold") or 0) + stars
                            total_profit_usd = float(get_setting("total_profit_usd") or 0) + (stars / 50 * float(get_setting("stars_price_usd")))
                            total_profit_ton = float(get_setting("total_profit_ton") or 0) + amount_ton
                            update_setting("total_stars_sold", total_stars_sold)
                            update_setting("total_profit_usd", total_profit_usd)
                            update_setting("total_profit_ton", total_profit_ton)
                            conn.commit()
                    await context.bot.send_message(
                        user_id,
                        get_text("buy_success", user_id, stars=stars)
                    )
                    with get_db_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute("SELECT referrer_id FROM users WHERE user_id = %s;", (user_id,))
                            result = cur.fetchone()
                            referrer_id = result[0] if result else None
                    if referrer_id:
                        ref_bonus_percent = float(get_setting("ref_bonus_percent")) / 100
                        ref_bonus_ton = amount_ton * ref_bonus_percent
                        with get_db_connection() as conn:
                            with conn.cursor() as cur:
                                cur.execute(
                                    "UPDATE users SET ref_bonus_ton = ref_bonus_ton + %s, bonus_history = bonus_history || %s WHERE user_id = %s;",
                                    (ref_bonus_ton, json.dumps({"amount": ref_bonus_ton, "timestamp": time.time()}), referrer_id)
                                )
                                conn.commit()
                        await context.bot.send_message(
                            referrer_id,
                            get_text("ref_bonus", referrer_id, ref_bonus_ton=ref_bonus_ton, username=username)
                        )
        await asyncio.sleep(60)

# Команда /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or f"user_{user_id}"
    ref_id = context.args[0].split("ref_")[1] if context.args and "ref_" in context.args[0] else None
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (user_id, username, referrer_id) VALUES (%s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET username = %s;",
                (user_id, username, ref_id, username)
            )
            if ref_id:
                cur.execute(
                    "UPDATE users SET referrals = referrals || %s WHERE user_id = %s;",
                    (json.dumps({"user_id": user_id, "username": username}), ref_id)
                )
            cur.execute("SELECT language FROM users WHERE user_id = %s;", (user_id,))
            language = cur.fetchone()[0]
            conn.commit()
    
    if not language:
        keyboard = [
            [InlineKeyboardButton("Русский", callback_data="lang_ru")],
            [InlineKeyboardButton("English", callback_data="lang_en")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            get_text("choose_language", user_id),
            reply_markup=reply_markup
        )
        return CHOOSE_LANGUAGE
    
    await show_main_menu(update, context)

# Показ главного меню
async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    keyboard = [
        [InlineKeyboardButton(get_text("buy_stars_btn", user_id), callback_data="buy_stars")],
        [InlineKeyboardButton(get_text("profile_btn", user_id), callback_data="profile")],
        [InlineKeyboardButton(get_text("referrals_btn", user_id), callback_data="referrals")],
        [InlineKeyboardButton(get_text("tech_support_btn", user_id), callback_data="tech_support")],
        [InlineKeyboardButton(get_text("reviews_btn", user_id), callback_data="reviews")],
        [InlineKeyboardButton(get_text("admin_panel_btn", user_id), callback_data="admin_panel")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await (update.message or update.callback_query.message).reply_text(
        get_text("welcome", user_id, total_stars_sold=get_setting("total_stars_sold") or 0),
        reply_markup=reply_markup
    )

# Обработка кнопок
async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    username = query.from_user.username or f"user_{user_id}"
    
    await query.answer()
    
    if query.data.startswith("lang_"):
        language = query.data.split("_")[1]
        update_user_language(user_id, language)
        await show_main_menu(update, context)
        return ConversationHandler.END
    
    if query.data == "buy_stars":
        stars = int(get_setting("stars_per_purchase"))
        price_usd = float(get_setting("stars_price_usd"))
        ton_exchange_rate = float(get_setting("ton_exchange_rate"))
        amount_ton = price_usd / ton_exchange_rate
        
        payment_info = await generate_ton_address(user_id)
        address = payment_info["address"]
        memo = payment_info["memo"]
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET stars_bought = %s, username = %s, address = %s, memo = %s, amount_ton = %s WHERE user_id = %s;",
                    (stars, username, address, memo, amount_ton, user_id)
                )
                conn.commit()
        
        await query.message.reply_text(
            get_text("buy_prompt", user_id, amount_ton=amount_ton, stars=stars, address=address, memo=memo)
        )
    
    elif query.data == "profile":
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT stars_bought, ref_bonus_ton FROM users WHERE user_id = %s;", (user_id,))
                result = cur.fetchone()
                stars_bought, ref_bonus_ton = result if result else (0, 0)
        
        await query.message.reply_text(
            get_text("profile", user_id, username=username, stars_bought=stars_bought, ref_bonus_ton=ref_bonus_ton)
        )
    
    elif query.data == "referrals":
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT ref_bonus_ton, referrals FROM users WHERE user_id = %s;", (user_id,))
                result = cur.fetchone()
                ref_bonus_ton, referrals = result if result else (0, [])
        
        keyboard = [
            [InlineKeyboardButton(get_text("top_referrals_btn", user_id), callback_data="top_referrals")],
            [InlineKeyboardButton(get_text("top_purchases_btn", user_id), callback_data="top_purchases")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.reply_text(
            get_text("ref_info", user_id, ref_bonus_ton=ref_bonus_ton, bot_username=context.bot.name.lstrip("@"), user_id=user_id),
            reply_markup=reply_markup
        )
    
    elif query.data == "top_referrals":
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT username, jsonb_array_length(referrals) as ref_count
                    FROM users
                    WHERE jsonb_array_length(referrals) > 0
                    ORDER BY ref_count DESC
                    LIMIT 10;
                """)
                top_referrals = cur.fetchall()
        
        text = get_text("top_referrals", user_id) + "\n"
        for i, (username, ref_count) in enumerate(top_referrals, 1):
            text += f"{i}. @{username}: {ref_count} рефералов\n"
        await query.message.reply_text(text or get_text("no_referrals", user_id))
    
    elif query.data == "top_purchases":
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT username, stars_bought
                    FROM users
                    WHERE stars_bought > 0
                    ORDER BY stars_bought DESC
                    LIMIT 10;
                """)
                top_purchases = cur.fetchall()
        
        text = get_text("top_purchases", user_id) + "\n"
        for i, (username, stars_bought) in enumerate(top_purchases, 1):
            text += f"{i}. @{username}: {stars_bought} звёзд\n"
        await query.message.reply_text(text or get_text("no_purchases", user_id))
    
    elif query.data == "tech_support":
        support_channel = get_setting("review_channel") or "@sacoectasy"
        await query.message.reply_text(
            get_text("tech_support", user_id, support_channel=support_channel)
        )
    
    elif query.data == "reviews":
        review_channel = get_setting("review_channel") or "@sacoectasy"
        await query.message.reply_text(
            get_text("reviews", user_id, review_channel=review_channel)
        )
    
    elif query.data == "admin_panel" and is_admin(user_id):
        keyboard = [
            [InlineKeyboardButton(get_text("edit_text_btn", user_id), callback_data="edit_text")],
            [InlineKeyboardButton(get_text("set_price_btn", user_id), callback_data="set_price")],
            [InlineKeyboardButton(get_text("set_percent_btn", user_id), callback_data="set_percent")],
            [InlineKeyboardButton(get_text("set_review_channel_btn", user_id), callback_data="set_review_channel")],
            [InlineKeyboardButton(get_text("stats_btn", user_id), callback_data="stats")],
            [InlineKeyboardButton(get_text("reset_profit_btn", user_id), callback_data="reset_profit")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.reply_text(get_text("admin_panel", user_id), reply_markup=reply_markup)
    
    elif query.data == "edit_text" and is_admin(user_id):
        await query.message.reply_text(
            get_text("edit_text_prompt", user_id)
        )
        return EDIT_TEXT
    
    elif query.data == "set_price" and is_admin(user_id):
        await query.message.reply_text(
            get_text("set_price_prompt", user_id)
        )
        return SET_PRICE
    
    elif query.data == "set_percent" and is_admin(user_id):
        await query.message.reply_text(
            get_text("set_percent_prompt", user_id)
        )
        return SET_PERCENT
    
    elif query.data == "set_review_channel" and is_admin(user_id):
        await query.message.reply_text(
            get_text("set_review_channel_prompt", user_id)
        )
        return SET_REVIEW_CHANNEL
    
    elif query.data == "stats" and is_admin(user_id):
        total_stars_sold = get_setting("total_stars_sold") or 0
        total_profit_usd = get_setting("total_profit_usd") or 0
        total_profit_ton = get_setting("total_profit_ton") or 0
        await query.message.reply_text(
            get_text("stats", user_id, total_stars_sold=total_stars_sold, total_profit_usd=total_profit_usd, total_profit_ton=total_profit_ton)
        )
    
    elif query.data == "reset_profit" and is_admin(user_id):
        update_setting("total_profit_usd", 0)
        update_setting("total_profit_ton", 0)
        log_admin_action(user_id, "Reset profit")
        await query.message.reply_text(get_text("reset_profit", user_id))

# Обработка редактирования текста
async def edit_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(get_text("access_denied", update.effective_user.id))
        return ConversationHandler.END
    
    try:
        key, value = update.message.text.split(":", 1)
        key = key.strip()
        value = value.strip()
        if not key.endswith(("_ru", "_en")):
            await update.message.reply_text(get_text("invalid_text_key", update.effective_user.id))
            return EDIT_TEXT
        update_text(key, value)
        log_admin_action(update.effective_user.id, f"Edited text {key}")
        await update.message.reply_text(get_text("text_updated", update.effective_user.id, key=key))
    except ValueError:
        await update.message.reply_text(get_text("text_format", update.effective_user.id))
    return ConversationHandler.END

# Обработка установки цены
async def set_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(get_text("access_denied", update.effective_user.id))
        return ConversationHandler.END
    
    try:
        price_usd, stars = update.message.text.split(":")
        price_usd = float(price_usd.strip())
        stars = int(stars.strip())
        if price_usd <= 0 or stars <= 0:
            await update.message.reply_text(get_text("invalid_price", update.effective_user.id))
            return SET_PRICE
        update_setting("stars_price_usd", price_usd)
        update_setting("stars_per_purchase", stars)
        log_admin_action(update.effective_user.id, f"Set price: {price_usd} USD for {stars} stars")
        await update.message.reply_text(get_text("price_set", update.effective_user.id, price_usd=price_usd, stars=stars))
    except ValueError:
        await update.message.reply_text(get_text("price_format", update.effective_user.id))
    return ConversationHandler.END

# Обработка установки процентов
async def set_percent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(get_text("access_denied", update.effective_user.id))
        return ConversationHandler.END
    
    try:
        ref_bonus, profit = update.message.text.split(":")
        ref_bonus = float(ref_bonus.strip())
        profit = float(profit.strip())
        if not (0 <= ref_bonus <= 100 and 10 <= profit <= 50):
            await update.message.reply_text(get_text("invalid_percent", update.effective_user.id))
            return SET_PERCENT
        update_setting("ref_bonus_percent", ref_bonus)
        update_setting("profit_percent", profit)
        log_admin_action(update.effective_user.id, f"Set percentages: ref_bonus {ref_bonus}%, profit {profit}%")
        await update.message.reply_text(get_text("percent_set", update.effective_user.id, ref_bonus=ref_bonus, profit=profit))
    except ValueError:
        await update.message.reply_text(get_text("percent_format", update.effective_user.id))
    return ConversationHandler.END

# Обработка установки канала отзывов
async def set_review_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(get_text("access_denied", update.effective_user.id))
        return ConversationHandler.END
    
    try:
        channel = update.message.text.strip()
        if not channel.startswith("@"):
            await update.message.reply_text(get_text("invalid_channel", update.effective_user.id))
            return SET_REVIEW_CHANNEL
        update_setting("review_channel", channel)
        log_admin_action(update.effective_user.id, f"Set review channel: {channel}")
        await update.message.reply_text(get_text("channel_set", update.effective_user.id, channel=channel))
    except ValueError:
        await update.message.reply_text(get_text("channel_format", update.effective_user.id))
    return ConversationHandler.END

# Отмена ConversationHandler
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(get_text("cancel", update.effective_user.id))
    return ConversationHandler.END

# Основная функция
async def main():
    try:
        init_db()
        application = Application.builder().token(BOT_TOKEN).build()
        conv_handler = ConversationHandler(
            entry_points=[
                CallbackQueryHandler(button, pattern="edit_text$"),
                CallbackQueryHandler(button, pattern="set_price$"),
                CallbackQueryHandler(button, pattern="set_percent$"),
                CallbackQueryHandler(button, pattern="set_review_channel$"),
                CallbackQueryHandler(button, pattern="lang_"),
            ],
            states={
                EDIT_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_text)],
                SET_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_price)],
                SET_PERCENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_percent)],
                SET_REVIEW_CHANNEL: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_review_channel)],
                CHOOSE_LANGUAGE: [CallbackQueryHandler(button)],
            },
            fallbacks=[CommandHandler("cancel", cancel)],
            per_message=True,
        )
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CallbackQueryHandler(button))
        application.add_handler(conv_handler)
        application.job_queue.run_repeating(payment_checker, interval=60)
        application.job_queue.run_repeating(update_ton_price, interval=60)
        await application.initialize()
        await application.start()
        await application.updater.start_polling(allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        logger.error(f"Ошибка в main: {e}")
        raise
    finally:
        if 'application' in locals():
            await application.updater.stop()
            await application.stop()
            await application.shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Ошибка: {e}")
