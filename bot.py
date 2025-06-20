import os
import time
import json
import logging
import asyncio
import aiohttp
import psycopg2
from urllib.parse import urlparse
from dotenv import load_dotenv
from aiohttp import web
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
    ConversationHandler,
)

# Загрузка .env
load_dotenv()

# Логирование
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Константы
BOT_TOKEN = os.getenv("BOT_TOKEN")
TONAPI_KEY = os.getenv("TONAPI_KEY")
POSTGRES_URL = os.getenv("POSTGRES_URL") or os.getenv("DATABASE_URL")
SPLIT_API_TOKEN = os.getenv("SPLIT_API_TOKEN")
OWNER_WALLET = os.getenv("OWNER_WALLET")
CRYPTOBOT_API_TOKEN = os.getenv("CRYPTOBOT_API_TOKEN")

# Состояния для ConversationHandler
EDIT_TEXT, SET_PRICE, SET_PERCENT, SET_COMMISSIONS, SET_REVIEW_CHANNEL, CHOOSE_LANGUAGE, BUY_STARS_USERNAME, BUY_STARS_AMOUNT, BUY_STARS_PAYMENT_METHOD, BUY_STARS_CONFIRM = range(10)

def get_db_connection():
    if not POSTGRES_URL:
        logger.error("POSTGRES_URL or DATABASE_URL not set")
        raise ValueError("POSTGRES_URL or DATABASE_URL is not set")
    
    try:
        parsed_url = urlparse(POSTGRES_URL)
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
                    cryptobot_invoice_id TEXT,
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
                    ('cryptobot_commission', '25'),
                    ('ton_commission', '20'),
                    ('card_commission', '30'),
                    ('card_payment_enabled', 'false')
                ON CONFLICT (key) DO NOTHING;
            """)
            cur.execute("""
                INSERT INTO texts (key, value)
                VALUES
                    ('welcome_ru', 'Добро пожаловать! Купите Telegram Stars за TON или USDT.\nЗвезд продано: {total_stars_sold}'),
                    ('welcome_en', 'Welcome! Buy Telegram Stars with TON or USDT.\nStars sold: {total_stars_sold}'),
                    ('buy_stars_prompt_ru', '⭐️ Кому отправить Telegram Stars?'),
                    ('buy_stars_prompt_en', '⭐️ Who to send Telegram Stars to?'),
                    ('buy_username_prompt_ru', 'Введите username получателя (без @):'),
                    ('buy_username_prompt_en', 'Enter recipient username (without @):'),
                    ('buy_amount_prompt_ru', 'Введите количество звезд:'),
                    ('buy_amount_prompt_en', 'Enter number of stars:'),
                    ('buy_payment_method_prompt_ru', 'Выберите способ оплаты:'),
                    ('buy_payment_method_prompt_en', 'Choose payment method:'),
                    ('buy_crypto_method_prompt_ru', 'Выберите криптовалюту:'),
                    ('buy_crypto_method_prompt_en', 'Choose cryptocurrency:'),
                    ('buy_cryptobot_prompt_ru', 'Оплатите {amount_usd:.2f} USDT через @CryptoBot.\nНажмите "Оплатить" для создания чека.\n\n{stars} звезд для @{username}'),
                    ('buy_cryptobot_prompt_en', 'Pay {amount_usd:.2f} USDT via @CryptoBot.\nClick "Pay" to create invoice.\n\n{stars} stars for @{username}'),
                    ('buy_ton_prompt_ru', 'Оплатите {amount_ton:.6f} TON через TON Wallet.\nАдрес: {address}\nMemo: {memo}\n\n{stars} звезд для @{username}'),
                    ('buy_ton_prompt_en', 'Pay {amount_ton:.6f} TON via TON Wallet.\nAddress: {address}\nMemo: {memo}\n\n{stars} stars for @{username}'),
                    ('buy_success_ru', 'Оплата прошла! @{username} получил {stars} звезд.'),
                    ('buy_success_en', 'Payment successful! @{username} received {stars} stars.'),
                    ('buy_invalid_username_ru', 'Неверный username. Введите без @.'),
                    ('buy_invalid_username_en', 'Invalid username. Enter without @.'),
                    ('buy_invalid_amount_ru', 'Количество звезд должно быть больше 0.'),
                    ('buy_invalid_amount_en', 'Number of stars must be greater than 0.'),
                    ('buy_card_disabled_ru', 'Оплата картой пока недоступна.'),
                    ('buy_card_disabled_en', 'Card payment is currently unavailable.'),
                    ('ref_info_ru', 'Ваш реф. бонус: {ref_bonus_ton:.6f} TON\nРеф. ссылка: t.me/{bot_username}?start=ref_{user_id}'),
                    ('ref_info_en', 'Your ref. bonus: {ref_bonus_ton:.6f} TON\nRef. link: t.me/{bot_username}?start=ref_{user_id}'),
                    ('tech_support_ru', 'Свяжитесь с техподдержкой: {support_channel}'),
                    ('tech_support_en', 'Contact support: {support_channel}'),
                    ('reviews_ru', 'Оставьте отзыв: {review_channel}'),
                    ('reviews_en', 'Leave a review: {review_channel}'),
                    ('choose_language_ru', 'Выберите язык:'),
                    ('choose_language_en', 'Choose language:'),
                    ('profile_ru', 'Профиль: @{username}\nКуплено звезд: {stars_bought}\nРеф. бонус: {ref_bonus_ton:.6f} TON'),
                    ('profile_en', 'Profile: @{username}\nStars bought: {stars_bought}\nRef. bonus: {ref_bonus_ton:.6f} TON'),
                    ('top_referrals_ru', 'Топ-10 рефералов:'),
                    ('top_referrals_en', 'Top-10 referrers:'),
                    ('top_purchases_ru', 'Топ-10 покупок:'),
                    ('top_purchases_en', 'Top-10 purchases:'),
                    ('no_referrals_ru', 'Нет рефералов.'),
                    ('no_referrals_en', 'No referrers.'),
                    ('no_purchases_ru', 'Нет покупок.'),
                    ('no_purchases_en', 'No purchases.'),
                    ('buy_stars_btn_ru', 'Купить звезды'),
                    ('buy_stars_btn_en', 'Buy stars'),
                    ('profile_btn_ru', 'Профиль'),
                    ('profile_btn_en', 'Profile'),
                    ('referrals_btn_ru', 'Рефералы'),
                    ('referrals_btn_en', 'Referrals'),
                    ('tech_support_btn_ru', 'Техподдержка'),
                    ('tech_support_btn_en', 'Support'),
                    ('reviews_btn_ru', 'Отзывы'),
                    ('reviews_btn_en', 'Reviews'),
                    ('admin_panel_btn_ru', 'Админ-панель'),
                    ('admin_panel_btn_en', 'Admin panel'),
                    ('edit_text_btn_ru', 'Редактировать текст'),
                    ('edit_text_btn_en', 'Edit text'),
                    ('set_price_btn_ru', 'Установить цену'),
                    ('set_price_btn_en', 'Set price'),
                    ('set_percent_btn_ru', 'Установить проценты'),
                    ('set_percent_btn_en', 'Set percentages'),
                    ('set_commissions_btn_ru', 'Установить комиссии'),
                    ('set_commissions_btn_en', 'Set commissions'),
                    ('set_review_channel_btn_ru', 'Установить канал отзывов'),
                    ('set_review_channel_btn_en', 'Set review channel'),
                    ('stats_btn_ru', 'Статистика'),
                    ('stats_btn_en', 'Statistics'),
                    ('reset_profit_btn_ru', 'Сбросить прибыль'),
                    ('reset_profit_btn_en', 'Reset profit'),
                    ('edit_text_prompt_ru', 'Введите: key:value\nНапример: welcome_ru:Новый текст'),
                    ('edit_text_prompt_en', 'Enter: key:value\nExample: welcome_en:New text'),
                    ('set_price_prompt_ru', 'Введите: price_usd:stars\nНапример: 0.81:50'),
                    ('set_price_prompt_en', 'Enter: price_usd:stars\nExample: 0.81:50'),
                    ('set_percent_prompt_ru', 'Введите: ref_bonus:profit\nНапример: 30:20'),
                    ('set_percent_prompt_en', 'Enter: ref_bonus:profit\nExample: 30:20'),
                    ('set_commissions_prompt_ru', 'Введите: cryptobot:ton:card\nНапример: 25:20:30'),
                    ('set_commissions_prompt_en', 'Enter: cryptobot:ton:card\nExample: 25:20:30'),
                    ('set_review_channel_prompt_ru', 'Введите: @channel\nНапример: @sacoectasy'),
                    ('set_review_channel_prompt_en', 'Enter: @channel\nExample: @sacoectasy'),
                    ('access_denied_ru', 'Доступ запрещён.'),
                    ('access_denied_en', 'Access denied.'),
                    ('invalid_text_key_ru', 'Неверный ключ текста. Используйте _ru или _en.'),
                    ('invalid_text_key_en', 'Invalid text key. Use _ru or _en.'),
                    ('text_updated_ru', 'Текст обновлён: {key}'),
                    ('text_updated_en', 'Text updated: {key}'),
                    ('text_format_ru', 'Формат: key:value'),
                    ('text_format_en', 'Format: key:value'),
                    ('invalid_price_ru', 'Неверная цена или количество звезд.'),
                    ('invalid_price_en', 'Invalid price or stars amount.'),
                    ('price_set_ru', 'Цена установлена: ${price_usd} за {stars} звезд'),
                    ('price_set_en', 'Price set: ${price_usd} for {stars} stars'),
                    ('price_format_ru', 'Формат: price_usd:stars'),
                    ('price_format_en', 'Format: price_usd:stars'),
                    ('invalid_percent_ru', 'Проценты должны быть: ref_bonus 0-100, profit 10-50.'),
                    ('invalid_percent_en', 'Percentages must be: ref_bonus 0-100, profit 10-50.'),
                    ('percent_set_ru', 'Проценты установлены: реф. бонус {ref_bonus}%, прибыль {profit}%'),
                    ('percent_set_en', 'Percentages set: ref. bonus {ref_bonus}%, profit {profit}%'),
                    ('percent_format_ru', 'Формат: ref_bonus:profit'),
                    ('percent_format_en', 'Format: ref_bonus:profit'),
                    ('invalid_commissions_ru', 'Комиссии должны быть: cryptobot 0-100, ton 0-100, card 0-100.'),
                    ('invalid_commissions_en', 'Commissions must be: cryptobot 0-100, ton 0-100, card 0-100.'),
                    ('commissions_set_ru', 'Комиссии установлены: @CryptoBot {cryptobot}%, TON {ton}%, Card {card}%'),
                    ('commissions_set_en', 'Commissions set: @CryptoBot {cryptobot}%, TON {ton}%, Card {card}%'),
                    ('commissions_format_ru', 'Формат: cryptobot:ton:card'),
                    ('commissions_format_en', 'Format: cryptobot:ton:card'),
                    ('invalid_channel_ru', 'Неверный канал. Используйте @channel.'),
                    ('invalid_channel_en', 'Invalid channel. Use @channel.'),
                    ('channel_set_ru', 'Канал установлен: {channel}'),
                    ('channel_set_en', 'Channel set: {channel}'),
                    ('channel_format_ru', 'Формат: @channel'),
                    ('channel_format_en', 'Format: @channel'),
                    ('stats_ru', 'Статистика:\nЗвезд продано: {total_stars_sold}\nПрибыль USD: ${total_profit_usd}\nПрибыль TON: {total_profit_ton}'),
                    ('stats_en', 'Statistics:\nStars sold: {total_stars_sold}\nProfit USD: ${total_profit_usd}\nProfit TON: {total_profit_ton}'),
                    ('reset_profit_ru', 'Прибыль сброшена.'),
                    ('reset_profit_en', 'Profit reset.'),
                    ('cancel_ru', 'Операция отменена.'),
                    ('cancel_en', 'Operation cancelled.'),
                    ('set_card_payment_prompt_ru', 'Введите: enabled (true/false)\nНапример: true'),
                    ('set_card_payment_prompt_en', 'Enter: enabled (true/false)\nExample: true'),
                    ('card_payment_set_ru', 'Оплата картой: {status}'),
                    ('card_payment_set_en', 'Card payment: {status}')
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
                if key == 'card_payment_enabled':
                    return result[0].lower() == 'true'
                return float(result[0]) if key in ('ref_bonus_percent', 'profit_percent', 'stars_price_usd', 'ton_exchange_rate', 'cryptobot_commission', 'ton_commission', 'card_commission') else result[0]
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
    admin_ids = get_setting("admin_ids") or [6956377285]
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

# Обновление курса TON через CoinGecko с ретраем
async def update_ton_price(context: ContextTypes.DEFAULT_TYPE):
    async with aiohttp.ClientSession() as session:
        for attempt in range(3):
            try:
                async with session.get("https://api.coingecko.com/api/v3/simple/price?ids=the-open-network&vs_currencies=usd") as response:
                    if response.status == 200:
                        data = await response.json()
                        ton_price = data.get("the-open-network", {}).get("usd", 2.93)
                        update_setting("ton_exchange_rate", ton_price)
                        logger.info(f"TON price updated: ${ton_price}")
                        return
                    elif response.status == 429:
                        wait_time = 2 ** attempt * 10
                        logger.warning(f"Rate limit hit, retrying in {wait_time} seconds...")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(f"Failed to update TON price: {response.status}")
                        return
            except Exception as e:
                logger.error(f"Exception in update_ton_price: {e}")
                return
        logger.error("Failed to update TON price after 3 attempts")

# Создание инвойса через @CryptoBot
async def create_cryptobot_invoice(amount_usd, username, stars):
    api_url = "https://pay.crypt.bot/api/createInvoice"
    headers = {
        "Crypto-Pay-API-Token": CRYPTOBOT_API_TOKEN
    }
    payload = {
        "amount": amount_usd,
        "currency": "USDT",
        "description": f"Purchase {stars} Telegram Stars for @{username}",
        "swap_to": "USDT"
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(api_url, headers=headers, json=payload) as response:
            if response.status == 200:
                data = await response.json()
                if data.get("ok"):
                    return data.get("result", {}).get("invoice_id")
                else:
                    logger.error(f"Failed to create CryptoBot invoice: {data.get('error')}")
                    return None
            else:
                logger.error(f"CryptoBot API request failed: {response.status}")
                return None

# Проверка оплаты через @CryptoBot
async def check_cryptobot_payment(invoice_id):
    api_url = f"https://pay.crypt.bot/api/getInvoices?invoice_ids={invoice_id}"
    headers = {
        "Crypto-Pay-API-Token": CRYPTOBOT_API_TOKEN
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(api_url, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                if data.get("ok"):
                    invoice = data.get("result", {}).get("items", [{}])[0]
                    return invoice.get("status") == "paid"
                else:
                    logger.error(f"Failed to check CryptoBot invoice: {data.get('error')}")
                    return False
            else:
                logger.error(f"CryptoBot API request failed: {response.status}")
                return False

# Выдача звёзд через split.tg
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

# Генерация TON-адреса
async def generate_ton_address(user_id):
    return {"address": OWNER_WALLET, "memo": f"order_{user_id}_{int(time.time())}"}

# Проверка оплаты TON
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
                cur.execute("SELECT user_id, username, stars_bought, address, memo, amount_ton, cryptobot_invoice_id FROM users WHERE stars_bought > 0 AND (address IS NOT NULL OR cryptobot_invoice_id IS NOT NULL);")
                pending = cur.fetchall()
        for user_id, username, stars, address, memo, amount_ton, invoice_id in pending:
            paid = False
            if invoice_id:
                paid = await check_cryptobot_payment(invoice_id)
            elif address and memo and amount_ton:
                paid = await check_ton_payment(address, memo, amount_ton)
            
            if paid:
                if await issue_stars_api(username, stars):
                    with get_db_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE users SET stars_bought = 0, address = NULL, memo = NULL, amount_ton = NULL, cryptobot_invoice_id = NULL WHERE user_id = %s;",
                                (user_id,)
                            )
                            total_stars_sold = int(get_setting("total_stars_sold") or 0) + stars
                            total_profit_usd = float(get_setting("total_profit_usd") or 0) + (stars / 50 * float(get_setting("stars_price_usd")))
                            total_profit_ton = float(get_setting("total_profit_ton") or 0) + (amount_ton if amount_ton else 0)
                            update_setting("total_stars_sold", total_stars_sold)
                            update_setting("total_profit_usd", total_profit_usd)
                            update_setting("total_profit_ton", total_profit_ton)
                            conn.commit()
                    await context.bot.send_message(
                        user_id,
                        get_text("buy_success", user_id, username=username, stars=stars)
                    )
                    with get_db_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute("SELECT referrer_id FROM users WHERE user_id = %s;", (user_id,))
                            result = cur.fetchone()
                            referrer_id = result[0] if result else None
                    if referrer_id:
                        ref_bonus_percent = float(get_setting("ref_bonus_percent")) / 100
                        ref_bonus_ton = (amount_ton if amount_ton else (stars / 50 * float(get_setting("stars_price_usd")))) * ref_bonus_percent
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
                "INSERT INTO users (user_id, username, referrer_id, language) VALUES (%s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET username = %s;",
                (user_id, username, ref_id, 'ru', username)
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
    ]
    if is_admin(user_id):
        keyboard.append([InlineKeyboardButton(get_text("admin_panel_btn", user_id), callback_data="admin_panel")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    message = await (update.message or update.callback_query.message).reply_text(
        get_text("welcome", user_id, total_stars_sold=get_setting("total_stars_sold") or 0),
        reply_markup=reply_markup
    )
    context.user_data['last_main_menu_id'] = message.message_id

# Показ меню покупки
async def show_buy_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    target_username = context.user_data.get('buy_username', '####')
    stars = context.user_data.get('buy_stars', '####')
    payment_method = context.user_data.get('payment_method', '####')
    
    price_usd = float(get_setting("stars_price_usd")) * (int(stars) / 50 if stars != '####' else 1)
    commission = 0
    if payment_method == '@CryptoBot':
        commission = float(get_setting("cryptobot_commission")) / 100
    elif payment_method == 'TON Wallet':
        commission = float(get_setting("ton_commission")) / 100
    elif payment_method == 'Card':
        commission = float(get_setting("card_commission")) / 100
    amount_usd = price_usd * (1 + commission) if stars != '####' else 0
    
    keyboard = [
        [InlineKeyboardButton(f"Имя: {target_username}", callback_data="set_username")],
        [InlineKeyboardButton(f"Кол-во звёзд: {stars}", callback_data="set_amount")],
        [InlineKeyboardButton(f"Оплата: {payment_method}", callback_data="set_payment_method")],
        [InlineKeyboardButton(f"Стоимость: ${amount_usd:.2f}", callback_data="noop")],
        [
            InlineKeyboardButton("Назад", callback_data="back_to_main"),
            InlineKeyboardButton("Оплатить", callback_data="confirm_payment")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    query = update.callback_query
    if query:
        await query.message.edit_text(
            get_text("buy_stars_prompt", user_id),
            reply_markup=reply_markup
        )
    else:
        message = await update.message.reply_text(
            get_text("buy_stars_prompt", user_id),
            reply_markup=reply_markup
        )
        context.user_data['buy_menu_id'] = message.message_id
    
    # Таймер 30 секунд
    context.job_queue.run_once(
        callback=lambda ctx: delete_buy_menu(ctx, user_id),
        when=30,
        data={'user_id': user_id}
    )

async def delete_buy_menu(context: ContextTypes.DEFAULT_TYPE, user_id):
    buy_menu_id = context.user_data.get('buy_menu_id')
    if buy_menu_id:
        try:
            await context.bot.delete_message(chat_id=user_id, message_id=buy_menu_id)
            context.user_data['buy_menu_id'] = None
        except:
            pass
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
        context.user_data['buy_username'] = '####'
        context.user_data['buy_stars'] = '####'
        context.user_data['payment_method'] = '####'
        await show_buy_menu(update, context)
        return BUY_STARS_USERNAME
    
    if query.data == "set_username":
        await query.message.reply_text(get_text("buy_username_prompt", user_id))
        context.user_data['state'] = 'buy_username'
        return BUY_STARS_USERNAME
    
    if query.data == "set_amount":
        await query.message.reply_text(get_text("buy_amount_prompt", user_id))
        context.user_data['state'] = 'buy_amount'
        return BUY_STARS_AMOUNT
    
    if query.data == "set_payment_method":
        card_enabled = get_setting("card_payment_enabled")
        keyboard = [
            [InlineKeyboardButton("Карта" if get_user_language(user_id) == 'ru' else "Card", callback_data="payment_card")],
            [InlineKeyboardButton("Крипта" if get_user_language(user_id) == 'ru' else "Crypto", callback_data="payment_crypto")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.reply_text(
            get_text("buy_payment_method_prompt", user_id),
            reply_markup=reply_markup
        )
        context.user_data['state'] = 'buy_payment_method'
        return BUY_STARS_PAYMENT_METHOD
    
    if query.data == "payment_card":
        if not get_setting("card_payment_enabled"):
            await query.message.reply_text(get_text("buy_card_disabled", user_id))
            await show_buy_menu(update, context)
            return BUY_STARS_USERNAME
        context.user_data['payment_method'] = 'Card'
        await show_buy_menu(update, context)
        return BUY_STARS_USERNAME
    
    if query.data == "payment_crypto":
        keyboard = [
            [InlineKeyboardButton("@CryptoBot", callback_data="payment_cryptobot")],
            [InlineKeyboardButton("TON Wallet", callback_data="payment_ton")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.reply_text(
            get_text("buy_crypto_method_prompt", user_id),
            reply_markup=reply_markup
        )
        context.user_data['state'] = 'buy_crypto_method'
        return BUY_STARS_PAYMENT_METHOD
    
    if query.data == "payment_cryptobot":
        context.user_data['payment_method'] = '@CryptoBot'
        await show_buy_menu(update, context)
        return BUY_STARS_USERNAME
    
    if query.data == "payment_ton":
        context.user_data['payment_method'] = 'TON Wallet'
        await show_buy_menu(update, context)
        return BUY_STARS_USERNAME
    
    if query.data == "back_to_main":
        buy_menu_id = context.user_data.get('buy_menu_id')
        if buy_menu_id:
            try:
                await context.bot.delete_message(chat_id=user_id, message_id=buy_menu_id)
            except:
                pass
        context.user_data['buy_username'] = None
        context.user_data['buy_stars'] = None
        context.user_data['payment_method'] = None
        context.user_data['buy_menu_id'] = None
        await show_main_menu(update, context)
        return ConversationHandler.END
    
    if query.data == "confirm_payment":
        target_username = context.user_data.get('buy_username')
        stars = context.user_data.get('buy_stars')
        payment_method = context.user_data.get('payment_method')
        
        if target_username == '####' or stars == '####' or payment_method == '####':
            await query.message.reply_text("Заполните все поля (Имя, Кол-во звёзд, Оплата).")
            await show_buy_menu(update, context)
            return BUY_STARS_USERNAME

        price_usd = float(get_setting("stars_price_usd")) * (int(stars) / 50)
        commission = 0
        if payment_method == '@CryptoBot':
            commission = float(get_setting("cryptobot_commission")) / 100
        elif payment_method == 'TON Wallet':
            commission = float(get_setting("ton_commission")) / 100
        elif payment_method == 'Card':
            commission = float(get_setting("card_commission")) / 100
        amount_usd = price_usd * (1 + commission)
        
        if payment_method == '@CryptoBot':
            invoice_id = await create_cryptobot_invoice(amount_usd, target_username, stars)
            if invoice_id:
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE users SET stars_bought = %s, username = %s, cryptobot_invoice_id = %s WHERE user_id = %s;",
                            (stars, target_username, invoice_id, user_id)
                        )
                        conn.commit()
                
                keyboard = [
                    [InlineKeyboardButton("Оплатить" if get_user_language(user_id) == 'ru' else "Pay", url=f"https://t.me/CryptoBot?start=pay{invoice_id}")],
                    [InlineKeyboardButton("Проверить" if get_user_language(user_id) == 'ru' else "Check", callback_data="check_payment")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.message.edit_text(
                    get_text("buy_cryptobot_prompt", user_id, amount_usd=amount_usd, stars=stars, username=target_username),
                    reply_markup=reply_markup
                )
            else:
                await query.message.reply_text("Ошибка при создании чека. Попробуйте позже.")
            return ConversationHandler.END
        
        elif payment_method == 'TON Wallet':
            ton_exchange_rate = float(get_setting("ton_exchange_rate"))
            amount_ton = amount_usd / ton_exchange_rate
            payment_info = await generate_ton_address(user_id)
            address = payment_info["address"]
            memo = payment_info["memo"]
            
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE users SET stars_bought = %s, username = %s, address = %s, memo = %s, amount_ton = %s WHERE user_id = %s;",
                        (stars, target_username, address, memo, amount_ton, user_id)
                    )
                    conn.commit()
            
            keyboard = [
                [InlineKeyboardButton("Оплатить" if get_user_language(user_id) == 'ru' else "Pay", url=f"https://ton.space/transfer?to={address}&amount={amount_ton}&memo={memo}")],
                [InlineKeyboardButton("Проверить" if get_user_language(user_id) == 'ru' else "Check", callback_data="check_payment")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.message.edit_text(
                get_text("buy_ton_prompt", user_id, amount_ton=amount_ton, stars=stars, address=address, memo=memo, username=target_username),
                reply_markup=reply_markup
            )
            return ConversationHandler.END
        
        elif payment_method == 'Card':
            await query.message.reply_text(get_text("buy_card_disabled", user_id))
            await show_buy_menu(update, context)
            return BUY_STARS_USERNAME
    
    if query.data == "check_payment":
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT username, stars_bought, address, memo, amount_ton, cryptobot_invoice_id FROM users WHERE user_id = %s;", (user_id,))
                result = cur.fetchone()
                if not result:
                    await query.message.reply_text("Нет активных заказов.")
                    return ConversationHandler.END
                target_username, stars, address, memo, amount_ton, invoice_id = result
        
        paid = False
        if invoice_id:
            paid = await check_cryptobot_payment(invoice_id)
        elif address and memo and amount_ton:
            paid = await check_ton_payment(address, memo, amount_ton)
        
        if paid:
            if await issue_stars_api(target_username, stars):
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE users SET stars_bought = 0, address = NULL, memo = NULL, amount_ton = NULL, cryptobot_invoice_id = NULL WHERE user_id = %s;",
                            (user_id,)
                        )
                        total_stars_sold = int(get_setting("total_stars_sold") or 0) + stars
                        total_profit_usd = float(get_setting("total_profit_usd") or 0) + (stars / 50 * float(get_setting("stars_price_usd")))
                        total_profit_ton = float(get_setting("total_profit_ton") or 0) + (amount_ton if amount_ton else 0)
                        update_setting("total_stars_sold", total_stars_sold)
                        update_setting("total_profit_usd", total_profit_usd)
                        update_setting("total_profit_ton", total_profit_ton)
                        conn.commit()
                await query.message.reply_text(
                    get_text("buy_success", user_id, username=target_username, stars=stars)
                )
        else:
            await query.message.reply_text("Оплата не подтверждена. Попробуйте позже.")
        return ConversationHandler.END
    
    if query.data == "profile":
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
            [InlineKeyboardButton(get_text("set_commissions_btn", user_id), callback_data="set_commissions")],
            [InlineKeyboardButton(get_text("set_review_channel_btn", user_id), callback_data="set_review_channel")],
            [InlineKeyboardButton(get_text("set_card_payment_btn", user_id), callback_data="set_card_payment")],
            [InlineKeyboardButton(get_text("stats_btn", user_id), callback_data="stats")],
            [InlineKeyboardButton(get_text("reset_profit_btn", user_id), callback_data="reset_profit")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.reply_text("Админ-панель", reply_markup=reply_markup)
    
    elif query.data == "edit_text" and is_admin(user_id):
        await query.message.reply_text(
            get_text("edit_text_prompt", user_id)
        )
        context.user_data['state'] = 'edit_text'
        return EDIT_TEXT
    
    elif query.data == "set_price" and is_admin(user_id):
        await query.message.reply_text(
            get_text("set_price_prompt", user_id)
        )
        context.user_data['state'] = 'set_price'
        return SET_PRICE
    
    elif query.data == "set_percent" and is_admin(user_id):
        await query.message.reply_text(
            get_text("set_percent_prompt", user_id)
        )
        context.user_data['state'] = 'set_percent'
        return SET_PERCENT
    
    elif query.data == "set_commissions" and is_admin(user_id):
        await query.message.reply_text(
            get_text("set_commissions_prompt", user_id)
        )
        context.user_data['state'] = 'set_commissions'
        return SET_COMMISSIONS
    
    elif query.data == "set_card_payment" and is_admin(user_id):
        await query.message.reply_text(
            get_text("set_card_payment_prompt", user_id)
        )
        context.user_data['state'] = 'set_card_payment'
        return SET_COMMISSIONS
    
    elif query.data == "set_review_channel" and is_admin(user_id):
        await query.message.reply_text(
            get_text("set_review_channel_prompt", user_id)
        )
        context.user_data['state'] = 'set_review_channel'
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

# Обработка текстового ввода
async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    state = context.user_data.get('state')
    text = update.message.text.strip()
    
    if state == 'buy_username':
        if text.startswith('@'):
            await update.message.reply_text(get_text("buy_invalid_username", user_id))
            return BUY_STARS_USERNAME
        context.user_data['buy_username'] = text
        try:
            await update.message.delete()
        except:
            pass
        await show_buy_menu(update, context)
        return BUY_STARS_USERNAME
    
    if state == 'buy_amount':
        try:
            stars = int(text)
            if stars <= 0:
                await update.message.reply_text(get_text("buy_invalid_amount", user_id))
                return BUY_STARS_AMOUNT
            context.user_data['buy_stars'] = stars
            await update.message.delete()
            await show_buy_menu(update, context)
            return BUY_STARS_USERNAME
        except ValueError:
            await update.message.reply_text(get_text("buy_invalid_amount", user_id))
            return BUY_STARS_AMOUNT
    
    if not is_admin(user_id):
        await update.message.reply_text(get_text("access_denied", user_id))
        return ConversationHandler.END
    
    if state == 'edit_text':
        try:
            key, value = text.split(":", 1)
            key = key.strip()
            value = value.strip()
            if not key.endswith(("_ru", "_en")):
                await update.message.reply_text(get_text("invalid_text_key", user_id))
                return EDIT_TEXT
            update_text(key, value)
            log_admin_action(user_id, f"Edited text {key}")
            await update.message.reply_text(get_text("text_updated", user_id, key=key))
        except ValueError:
            await update.message.reply_text(get_text("text_format", user_id))
        return ConversationHandler.END
    
    elif state == 'set_price':
        try:
            price_usd, stars = text.split(":")
            price_usd = float(price_usd.strip())
            stars = int(stars.strip())
            if price_usd <= 0 or stars <= 0:
                await update.message.reply_text(get_text("invalid_price", user_id))
                return SET_PRICE
            update_setting("stars_price_usd", price_usd)
            update_setting("stars_per_purchase", stars)
            log_admin_action(user_id, f"Set price: {price_usd} USD for {stars} stars")
            await update.message.reply_text(get_text("price_set", user_id, price_usd=price_usd, stars=stars))
        except ValueError:
            await update.message.reply_text(get_text("price_format", user_id))
        return ConversationHandler.END
    
    elif state == 'set_percent':
        try:
            ref_bonus, profit = text.split(":")
            ref_bonus = float(ref_bonus.strip())
            profit = float(profit.strip())
            if not (0 <= ref_bonus <= 100 and 10 <= profit <= 50):
                await update.message.reply_text(get_text("invalid_percent", user_id))
                return SET_PERCENT
            update_setting("ref_bonus_percent", ref_bonus)
            update_setting("profit_percent", profit)
            log_admin_action(user_id, f"Set percentages: ref_bonus {ref_bonus}%, profit {profit}%")
            await update.message.reply_text(get_text("percent_set", user_id, ref_bonus=ref_bonus, profit=profit))
        except ValueError:
            await update.message.reply_text(get_text("percent_format", user_id))
        return ConversationHandler.END
    
    elif state == 'set_commissions':
        try:
            cryptobot, ton, card = text.split(":")
            cryptobot = float(cryptobot.strip())
            ton = float(ton.strip())
            card = float(card.strip())
            if not (0 <= cryptobot <= 100 and 0 <= ton <= 100 and 0 <= card <= 100):
                await update.message.reply_text(get_text("invalid_commissions", user_id))
                return SET_COMMISSIONS
            update_setting("cryptobot_commission", cryptobot)
            update_setting("ton_commission", ton)
            update_setting("card_commission", card)
            log_admin_action(user_id, f"Set commissions: cryptobot {cryptobot}%, ton {ton}%, card {card}%")
            await update.message.reply_text(get_text("commissions_set", user_id, cryptobot=cryptobot, ton=ton, card=card))
        except ValueError:
            await update.message.reply_text(get_text("commissions_format", user_id))
        return ConversationHandler.END
    
    elif state == 'set_card_payment':
        try:
            enabled = text.strip().lower()
            if enabled not in ('true', 'false'):
                await update.message.reply_text("Формат: enabled (true/false)")
                return SET_COMMISSIONS
            update_setting("card_payment_enabled", enabled)
            log_admin_action(user_id, f"Set card payment: {enabled}")
            await update.message.reply_text(
                get_text("card_payment_set", user_id, status="включена" if enabled == 'true' else "выключена" if get_user_language(user_id) == 'ru' else "enabled" if enabled == 'true' else "disabled")
            )
        except ValueError:
            await update.message.reply_text("Формат: enabled (true/false)")
        return ConversationHandler.END
    
    elif state == 'set_review_channel':
        try:
            channel = text.strip()
            if not channel.startswith("@"):
                await update.message.reply_text(get_text("invalid_channel", user_id))
                return SET_REVIEW_CHANNEL
            update_setting("review_channel", channel)
            log_admin_action(user_id, f"Set review channel: {channel}")
            await update.message.reply_text(get_text("channel_set", user_id, channel=channel))
        except ValueError:
            await update.message.reply_text(get_text("channel_format", user_id))
        return ConversationHandler.END
    
    return ConversationHandler.END

# HTTP health check и заглушки
async def health_check(request):
    return web.Response(text="OK")

async def root_handler(request):
    return web.Response(text="Stars Bot is running")

async def favicon_handler(request):
    return web.Response(status=204)

async def start_health_server():
    app = web.Application()
    app.add_routes([
        web.get('/health', health_check),
        web.get('/', root_handler),
        web.get('/favicon.ico', favicon_handler)
    ])
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"Health check server started on port {port}")

# Отмена ConversationHandler
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(get_text("cancel", update.effective_user.id))
    buy_menu_id = context.user_data.get('buy_menu_id')
    if buy_menu_id:
        try:
            await context.bot.delete_message(chat_id=update.effective_user.id, message_id=buy_menu_id)
        except:
            pass
    context.user_data['buy_username'] = None
    context.user_data['buy_stars'] = None
    context.user_data['payment_method'] = None
    context.user_data['buy_menu_id'] = None
    await show_main_menu(update, context)
    return ConversationHandler.END

# Основная функция
async def main():
    try:
        init_db()
        application = Application.builder().token(BOT_TOKEN).build()
        conv_handler = ConversationHandler(
            entry_points=[
                CallbackQueryHandler(button, pattern="^(edit_text|set_price|set_percent|set_commissions|set_card_payment|set_review_channel|lang_.*|buy_stars|set_username|set_amount|set_payment_method|payment_card|payment_crypto|payment_cryptobot|payment_ton|check_payment|back_to_main|confirm_payment)$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
            ],
            states={
                EDIT_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)],
                SET_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)],
                SET_PERCENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)],
                SET_COMMISSIONS: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)],
                SET_REVIEW_CHANNEL: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)],
                CHOOSE_LANGUAGE: [CallbackQueryHandler(button)],
                BUY_STARS_USERNAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input), CallbackQueryHandler(button)],
                BUY_STARS_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input), CallbackQueryHandler(button)],
                BUY_STARS_PAYMENT_METHOD: [CallbackQueryHandler(button)]
            },
            fallbacks=[CommandHandler("cancel", cancel)],
            per_message=False
        )
        application.add_handler(CommandHandler("start", start))
        application.add_handler(conv_handler)
        application.job_queue.run_repeating(payment_checker, interval=60)
        application.job_queue.run_repeating(update_ton_price, interval=300)
        asyncio.create_task(start_health_server())
        
        await application.bot.delete_webhook(drop_pending_updates=True)
        logger.info("Webhook deleted, starting polling")
        
        await application.initialize()
        await application.start()
        await application.updater.start_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
            poll_interval=1
        )
        while True:
            await asyncio.sleep(3600)
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
