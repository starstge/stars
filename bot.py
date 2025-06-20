import os
import time
import json
import logging
import asyncio
import aiohttp
import psycopg2
import uuid
from urllib.parse import urlparse
from dotenv import load_dotenv
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
BOT_TOKEN = os.getenv("BOT_TOKEN", "7579031437:AAGTnqJUHlZeDk7GuUyFljBnW9NbxPIJZyE")
TON_API_KEY = os.getenv("TON_API_KEY")
POSTGRES_URL = os.getenv("POSTGRES_URL") or os.getenv("DATABASE_URL", "postgresql://stars_bot_user:R3Exnj2LEASCsuY0NImBP44nkMbSORKH@dpg-d19qr395pdvs73a3ao2g-a.oregon-postgres.render.com/stars_bot")
SPLIT_API_TOKEN = os.getenv("SPLIT_API_TOKEN")
OWNER_WALLET = os.getenv("OWNER_WALLET")
CRYPTOBOT_API_TOKEN = os.getenv("CRYPTOBOT_API_TOKEN", "417243:AAOX6otWrre07SY0GHWtVXKWrLZxyvFNttV")
MARKUP_PERCENTAGE = float(os.getenv("MARKUP_PERCENTAGE", 10))

# Состояния для ConversationHandler
EDIT_TEXT, SET_PRICE, SET_PERCENT, SET_COMMISSIONS, SET_REVIEW_CHANNEL, SET_CARD_PAYMENT, CHOOSE_LANGUAGE, BUY_STARS_USERNAME, BUY_STARS_AMOUNT, BUY_STARS_PAYMENT_METHOD, SET_MARKUP, ADD_ADMIN, REMOVE_ADMIN = range(13)

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
        raise e

# Инициализация базы данных
def init_db():
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
                CREATE TABLE IF NOT EXISTS bot_instances (
                    instance_id TEXT PRIMARY KEY,
                    last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
                    ('buy_crypto_method_prompt_ru', '💸 Выбери криптовалюту:'),
                    ('buy_crypto_method_prompt_en', '💸 Choose cryptocurrency:'),
                    ('buy_cryptobot_prompt_ru', '💸 Оплати {amount_usd:.2f} USDT через @CryptoBot за {stars} звёзд для @{username}.'),
                    ('buy_cryptobot_prompt_en', '💸 Pay {amount_usd:.2f} USDT via @CryptoBot for {stars} stars to @{username}.'),
                    ('buy_card_prompt_ru', '💳 Оплати {amount_usd:.2f} USD картой за {stars} звёзд для @{username}.'),
                    ('buy_card_prompt_en', '💳 Pay {amount_usd:.2f} USD by card for {stars} stars to @{username}.'),
                    ('buy_ton_prompt_ru', '💸 Оплати {amount_ton:.6f} TON через TON Wallet за {stars} звёзд для @{username}.\n📍 Адрес: {address}\n📝 Memo: {memo}'),
                    ('buy_ton_prompt_en', '💸 Pay {amount_ton:.6f} TON via TON Wallet for {stars} stars to @{username}.\n📍 Address: {address}\n📝 Memo: {memo}'),
                    ('buy_success_ru', '🎉 Оплата прошла! @{username} получил {stars} звёзд!'),
                    ('buy_success_en', '🎉 Payment successful! @{username} received {stars} stars!'),
                    ('buy_invalid_username_ru', '⚠️ Неверный username. Введи без @.'),
                    ('buy_invalid_username_en', '⚠️ Invalid username. Enter without @.'),
                    ('buy_invalid_amount_ru', '⚠️ Количество звёзд должно быть не менее {min_stars}.'),
                    ('buy_invalid_amount_en', '⚠️ Number of stars must be at least {min_stars}.'),
                    ('buy_card_disabled_ru', '⚠️ Оплата картой сейчас недоступна.'),
                    ('buy_card_disabled_en', '⚠️ Card payment is currently unavailable.'),
                    ('buy_error_fill_fields_ru', '⚠️ Заполни все поля: username, количество звёзд, способ оплаты.'),
                    ('buy_error_fill_fields_en', '⚠️ Fill all fields: username, number of stars, payment method.'),
                    ('buy_error_cryptobot_ru', '⚠️ Ошибка создания счёта в @CryptoBot. Попробуй позже.'),
                    ('buy_error_cryptobot_en', '⚠️ Failed to create invoice in @CryptoBot. Try again later.'),
                    ('ref_info_ru', '💰 Реферальный бонус: {ref_bonus_ton:.6f} TON\n🔗 Твоя ссылка: t.me/{bot_username}?start=ref_{user_id}\n👥 Рефералов: {ref_count}'),
                    ('ref_info_en', '💰 Referral bonus: {ref_bonus_ton:.6f} TON\n🔗 Your link: t.me/{bot_username}?start=ref_{user_id}\n👥 Referrals: {ref_count}'),
                    ('tech_support_ru', '📞 Нужна помощь? Пиши в поддержку: {support_channel}'),
                    ('tech_support_en', '📞 Need help? Contact support: {support_channel}'),
                    ('reviews_ru', '⭐️ Оставь отзыв: {review_channel}'),
                    ('reviews_en', '⭐️ Share feedback: {review_channel}'),
                    ('choose_language_ru', '🌐 Выбери язык:'),
                    ('choose_language_en', '🌐 Choose language:'),
                    ('profile_ru', '👤 Профиль: @{username}\n🌟 Куплено звёзд: {stars_bought}\n💰 Бонус: {ref_bonus_ton:.6f} TON'),
                    ('profile_en', '👤 Profile: @{username}\n🌟 Stars bought: {stars_bought}\n💰 Bonus: {ref_bonus_ton:.6f} TON'),
                    ('top_referrals_ru', '🏆 Топ-5 рефералов:'),
                    ('top_referrals_en', '🏆 Top-5 referrers:'),
                    ('top_purchases_ru', '🏆 Топ-5 покупок:'),
                    ('top_purchases_en', '🏆 Top-5 purchases:'),
                    ('no_referrals_ru', '😔 Рефералов пока нет.'),
                    ('no_referrals_en', '😔 No referrers yet.'),
                    ('no_purchases_ru', '😔 Покупок пока нет.'),
                    ('no_purchases_en', '😔 No purchases yet.'),
                    ('buy_stars_btn_ru', '🌟 Купить звёзды'),
                    ('buy_stars_btn_en', '🌟 Buy stars'),
                    ('profile_btn_ru', '👤 Профиль'),
                    ('profile_btn_en', '👤 Profile'),
                    ('referrals_btn_ru', '🔗 Рефералы'),
                    ('referrals_btn_en', '🔗 Referrals'),
                    ('tech_support_btn_ru', '📞 Поддержка'),
                    ('tech_support_btn_en', '📞 Support'),
                    ('reviews_btn_ru', '⭐️ Отзывы'),
                    ('reviews_btn_en', '⭐️ Reviews'),
                    ('admin_panel_btn_ru', '⚙️ Админ-панель'),
                    ('admin_panel_btn_en', '⚙️ Admin panel'),
                    ('back_btn_ru', '◀️ Назад'),
                    ('back_btn_en', '◀️ Back'),
                    ('cancel_btn_ru', '❌ Отмена'),
                    ('cancel_btn_en', '❌ Cancel'),
                    ('edit_text_btn_ru', '✏️ Тексты'),
                    ('edit_text_btn_en', '✏️ Texts'),
                    ('set_price_btn_ru', '💵 Цена'),
                    ('set_price_btn_en', '💵 Price'),
                    ('set_percent_btn_ru', '📊 Проценты'),
                    ('set_percent_btn_en', '📊 Percentages'),
                    ('set_commissions_btn_ru', '💸 Комиссии'),
                    ('set_commissions_btn_en', '💸 Commissions'),
                    ('set_review_channel_btn_ru', '📢 Канал отзывов'),
                    ('set_review_channel_btn_en', '📢 Review channel'),
                    ('set_card_payment_btn_ru', '💳 Оплата картой'),
                    ('set_card_payment_btn_en', '💳 Card payment'),
                    ('set_markup_btn_ru', '📈 Наценка'),
                    ('set_markup_btn_en', '📈 Markup'),
                    ('stats_btn_ru', '📊 Статистика'),
                    ('stats_btn_en', '📊 Statistics'),
                    ('manage_admins_btn_ru', '👑 Управление админами'),
                    ('manage_admins_btn_en', '👑 Manage admins'),
                    ('add_admin_btn_ru', '➕ Добавить админа'),
                    ('add_admin_btn_en', '➕ Add admin'),
                    ('remove_admin_btn_ru', '➖ Удалить админа'),
                    ('remove_admin_btn_en', '➖ Remove admin'),
                    ('edit_text_prompt_ru', '✏️ Введи: key:value\nПример: welcome_ru:Новый текст'),
                    ('edit_text_prompt_en', '✏️ Enter: key:value\nExample: welcome_en:New text'),
                    ('set_price_prompt_ru', '💵 Введи: price_usd:stars\nПример: 0.81:50'),
                    ('set_price_prompt_en', '💵 Enter: price_usd:stars\nExample: 0.81:50'),
                    ('set_percent_prompt_ru', '📊 Введи: ref_bonus:profit\nПример: 30:20'),
                    ('set_percent_prompt_en', '📊 Enter: ref_bonus:profit\nExample: 30:20'),
                    ('set_commissions_prompt_ru', '💸 Введи: cryptobot:ton:card\nПример: 25:20:30'),
                    ('set_commissions_prompt_en', '💸 Enter: cryptobot:ton:card\nExample: 25:20:30'),
                    ('set_review_channel_prompt_ru', '📢 Введи: @channel\nПример: @sacoectasy'),
                    ('set_review_channel_prompt_en', '📢 Enter: @channel\nExample: @sacoectasy'),
                    ('set_card_payment_prompt_ru', '💳 Введи: true/false\nПример: true'),
                    ('set_card_payment_prompt_en', '💳 Enter: true/false\nExample: true'),
                    ('set_markup_prompt_ru', '📈 Введи: markup_percentage\nПример: 10'),
                    ('set_markup_prompt_en', '📈 Enter: markup_percentage\nExample: 10'),
                    ('add_admin_prompt_ru', '👑 Введи ID пользователя для добавления в админы:'),
                    ('add_admin_prompt_en', '👑 Enter user ID to add as admin:'),
                    ('remove_admin_prompt_ru', '👑 Введи ID пользователя для удаления из админов:'),
                    ('remove_admin_prompt_en', '👑 Enter user ID to remove from admins:'),
                    ('access_denied_ru', '🚫 Доступ запрещён.'),
                    ('access_denied_en', '🚫 Access denied.'),
                    ('invalid_text_key_ru', '⚠️ Неверный ключ. Используй _ru или _en.'),
                    ('invalid_text_key_en', '⚠️ Invalid key. Use _ru or _en.'),
                    ('text_updated_ru', '✅ Текст обновлён: {key}'),
                    ('text_updated_en', '✅ Text updated: {key}'),
                    ('text_format_ru', '⚠️ Формат: key:value'),
                    ('text_format_en', '⚠️ Format: key:value'),
                    ('invalid_price_ru', '⚠️ Неверная цена или количество звёзд.'),
                    ('invalid_price_en', '⚠️ Invalid price or stars amount.'),
                    ('price_set_ru', '✅ Установлено: ${price_usd} за {stars} звёзд'),
                    ('price_set_en', '✅ Set: ${price_usd} for {stars} stars'),
                    ('price_format_ru', '⚠️ Формат: price_usd:stars'),
                    ('price_format_en', '⚠️ Format: price_usd:stars'),
                    ('invalid_percent_ru', '⚠️ Проценты: ref_bonus 0-100, profit 10-50.'),
                    ('invalid_percent_en', '⚠️ Percentages: ref_bonus 0-100, profit 10-50.'),
                    ('percent_set_ru', '✅ Установлено: бонус {ref_bonus}%, прибыль {profit}%'),
                    ('percent_set_en', '✅ Set: bonus {ref_bonus}%, profit {profit}%'),
                    ('percent_format_ru', '⚠️ Формат: ref_bonus:profit'),
                    ('percent_format_en', '⚠️ Format: ref_bonus:profit'),
                    ('invalid_commissions_ru', '⚠️ Комиссии: cryptobot 0-100, ton 0-100, card 0-100.'),
                    ('invalid_commissions_en', '⚠️ Commissions: cryptobot 0-100, ton 0-100, card 0-100.'),
                    ('commissions_set_ru', '✅ Установлено: CryptoBot {cryptobot}%, TON {ton}%, Card {card}%'),
                    ('commissions_set_en', '✅ Set: CryptoBot {cryptobot}%, TON {ton}%, Card {card}%'),
                    ('commissions_format_ru', '⚠️ Формат: cryptobot:ton:card'),
                    ('commissions_format_en', '⚠️ Format: cryptobot:ton:card'),
                    ('invalid_markup_ru', '⚠️ Наценка должна быть от 0 до 100.'),
                    ('invalid_markup_en', '⚠️ Markup must be between 0 and 100.'),
                    ('markup_set_ru', '✅ Наценка: {markup}%'),
                    ('markup_set_en', '✅ Markup: {markup}%'),
                    ('markup_format_ru', '⚠️ Формат: markup_percentage'),
                    ('markup_format_en', '⚠️ Format: markup_percentage'),
                    ('invalid_channel_ru', '⚠️ Неверный канал. Используй @channel.'),
                    ('invalid_channel_en', '⚠️ Invalid channel. Use @channel.'),
                    ('channel_set_ru', '✅ Канал: {channel}'),
                    ('channel_set_en', '✅ Channel: {channel}'),
                    ('channel_format_ru', '⚠️ Формат: @channel'),
                    ('channel_format_en', '⚠️ Format: @channel'),
                    ('stats_ru', '📊 Статистика:\n🌟 Продано: {total_stars_sold} звёзд\n💵 Прибыль: ${total_profit_usd:.2f}\n💰 TON: {total_profit_ton:.6f}'),
                    ('stats_en', '📊 Statistics:\n🌟 Sold: {total_stars_sold} stars\n💵 Profit: ${total_profit_usd:.2f}\n💰 TON: {total_profit_ton:.6f}'),
                    ('reset_profit_ru', '✅ Прибыль сброшена.'),
                    ('reset_profit_en', '✅ Profit reset.'),
                    ('cancel_ru', '❌ Операция отменена.'),
                    ('cancel_en', '❌ Operation cancelled.'),
                    ('card_payment_set_ru', '✅ Оплата картой: {status}'),
                    ('card_payment_set_en', '✅ Card payment: {status}'),
                    ('admin_added_ru', '✅ Пользователь {user_id} добавлен в админы.'),
                    ('admin_added_en', '✅ User {user_id} added as admin.'),
                    ('admin_removed_ru', '✅ Пользователь {user_id} удалён из админов.'),
                    ('admin_removed_en', '✅ User {user_id} removed from admins.'),
                    ('invalid_admin_id_ru', '⚠️ Неверный ID пользователя.'),
                    ('invalid_admin_id_en', '⚠️ Invalid user ID.'),
                    ('admin_already_exists_ru', '⚠️ Пользователь {user_id} уже админ.'),
                    ('admin_already_exists_en', '⚠️ User {user_id} is already an admin.'),
                    ('admin_not_found_ru', '⚠️ Пользователь {user_id} не является админом.'),
                    ('admin_not_found_en', '⚠️ User {user_id} is not an admin.')
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
                return float(result[0]) if key in ('ref_bonus_percent', 'profit_percent', 'stars_price_usd', 'ton_exchange_rate', 'cryptobot_commission', 'ton_commission', 'card_commission', 'min_stars_purchase', 'markup_percentage', 'total_stars_sold', 'total_profit_usd', 'total_profit_ton') else result[0]
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
async def is_admin(user_id):
    admin_ids = get_setting("admin_ids") or [6956377285]
    return user_id in admin_ids

# Логирование действий админа
def log_admin_action(admin_id, action):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO admin_log (user_id, action) VALUES (%s, %s);", (admin_id, action))
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
                "INSERT INTO users (user_id, language) VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET language = %s;",
                (user_id, language, language)
            )
            conn.commit()

# Получение числового значения прибыли
def get_profit_usd(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0

def get_profit_ton(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0

# Обновление курса TON через CoinGecko
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
                        wait_time = 2 ** attempt
                        logger.warning(f"Rate limit hit, retrying in {wait_time} seconds...")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(f"Failed to update TON price: {response.status}")
                        return
            except Exception as e:
                logger.error(f"Error updating TON price: {e}")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
        logger.error("Failed to update TON price after 3 attempts")

# Создание инвойса через @CryptoBot
async def create_cryptobot_invoice(user_id, amount_usd, currency, target_username, stars):
    api_url = "https://pay.crypt.bot/api/v3/invoices"
    headers = {
        "Authorization": f"Bearer {CRYPTOBOT_API_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "amount": float(amount_usd),
        "currency": currency,
        "description": f"Purchase {stars} Telegram Stars for @{target_username}",
        "metadata": {"user_id": str(user_id), "stars": stars, "username": target_username}
    }
    async with aiohttp.ClientSession() as session:
        for attempt in range(3):
            try:
                async with session.post(api_url, headers=headers, json=payload) as response:
                    response_text = await response.text()
                    logger.info(f"CryptoBot API response: {response.status} - {response_text}")
                    if response.status == 200:
                        data = await response.json()
                        if data.get("ok"):
                            invoice_id = data.get("result", {}).get("invoice_id")
                            payment_url = data.get("result", {}).get("pay_url")
                            logger.info(f"Invoice created: ID {invoice_id}, URL {payment_url}")
                            return {"invoice_id": invoice_id, "pay_url": payment_url}
                        else:
                            logger.error(f"CryptoBot API error: {data.get('error')}")
                            return None
                    elif response.status == 429:
                        wait_time = 2 ** attempt * 5
                        logger.warning(f"CryptoBot rate limit hit, retrying in {wait_time} seconds...")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(f"CryptoBot API request failed: {response.status} - {response_text}")
                        return None
            except Exception as e:
                logger.error(f"Error creating CryptoBot invoice (attempt {attempt+1}): {e}")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt * 5)
        logger.error("Failed to create CryptoBot invoice after 3 attempts")
        return None

# Проверка оплаты через @CryptoBot
async def check_cryptobot_payment(invoice_id):
    api_url = f"https://pay.crypt.bot/api/v3/invoices?invoice_ids={invoice_id}"
    headers = {
        "Authorization": f"Bearer {CRYPTOBOT_API_TOKEN}"
    }
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(api_url, headers=headers) as response:
                response_text = await response.text()
                logger.info(f"Check CryptoBot response: {response.status} - {response_text}")
                if response.status == 200:
                    data = await response.json()
                    if data.get("ok"):
                        invoice = data.get("result", {}).get("items", [{}])[0]
                        return invoice.get("status") == "paid"
                    else:
                        logger.error(f"Failed to check CryptoBot invoice: {data.get('error_message', 'Unknown error')}")
                        return False
                else:
                    logger.error(f"CryptoBot API request failed: {response.status} - {response_text}")
                    return False
        except Exception as e:
            logger.error(f"Error checking CryptoBot payment: {str(e)}")
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
        try:
            async with session.post(api_url, headers=headers, json=payload) as response:
                response_text = await response.text()
                logger.info(f"Split.tg API response: {response.status} - {response_text}")
                if response.status == 200:
                    data = await response.json()
                    if data.get("ok"):
                        logger.info(f"Stars issued: @{username}, {stars}")
                        return True
                    else:
                        logger.error(f"Split.tg API error: {data.get('error_message')}")
                        return False
                else:
                    logger.error(f"Split.tg request failed: {response.status} - {response_text}")
                    return False
        except Exception as e:
            logger.error(f"Error issuing stars via Split.tg: {str(e)}")
            return False

# Генерация TON-адреса
async def generate_ton_address(user_id):
    return {"address": OWNER_WALLET, "memo": f"order_{user_id}_{int(time.time())}"}

# Проверка оплаты TON
async def check_ton_payment(address, memo, amount_ton):
    headers = {"Authorization": f"Bearer {TON_API_KEY}"}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"https://tonapi.io/v2/transactions?address={address}", headers=headers) as response:
                if response.status == 200:
                    transactions = await response.json()
                    for tx in transactions.get("transactions", []):
                        if tx.get("memo") == memo and float(tx.get("amount", 0)) / 1e9 >= amount_ton:
                            return True
                logger.error(f"TON API request failed: {response.status}")
                return False
        except Exception as e:
            logger.error(f"Error checking TON payment: {str(e)}")
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
                            base_price_usd = float(get_setting("stars_price_usd") or 0.81) * (stars / 50)
                            markup = float(get_setting("markup_percentage") or MARKUP_PERCENTAGE)
                            total_profit_usd = float(get_setting("total_profit_usd") or 0) + (base_price_usd * (markup / 100))
                            total_profit_ton = float(get_setting("total_profit_ton") or 0) + (float(amount_ton) if amount_ton else 0)
                            update_setting("total_stars_sold", total_stars_sold)
                            update_setting("total_profit_usd", total_profit_usd)
                            update_setting("total_profit_ton", total_profit_ton)
                            conn.commit()
                    await context.bot.send_message(
                        user_id=user_id,
                        text=get_text("buy_success", user_id=user_id, username=username, stars=stars)
                    )
                    with get_db_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute("SELECT referrer_id FROM users WHERE user_id = %s;", (user_id,))
                            result = cur.fetchone()
                            referrer_id = result[0] if result else None
                    if referrer_id:
                        ref_bonus_percent = float(get_setting("ref_bonus_percent") or 10) / 100
                        ref_bonus_ton = (amount_ton if amount_ton else (stars / 50 * float(get_setting("stars_price_usd")) * (1 + markup / 100))) * ref_bonus_percent
                        with get_db_connection() as conn:
                            with conn.cursor() as cur:
                                cur.execute(
                                    "UPDATE users SET ref_bonus_ton = ref_bonus_ton + %s, bonus_history = bonus_history || %s WHERE user_id = %s;",
                                    (ref_bonus_ton, json.dumps({"amount": ref_bonus_ton, "timestamp": time.time()}), referrer_id)
                                )
                                conn.commit()
                        await context.bot.send_message(
                            referrer_id,
                            get_text("ref_info", referrer_id, ref_bonus_ton=ref_bonus_ton, bot_username=context.bot.name.lstrip("@"), user_id=referrer_id, ref_count=len(get_user_data(referrer_id, "referrals")))
                        )
        await asyncio.sleep(30)

# Получение пользовательских данных
def get_user_data(user_id, field):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT {field} FROM users WHERE user_id = %s;", (user_id,))
            result = cur.fetchone()
            if result:
                if field in ("referrals", "bonus_history"):
                    return json.loads(result[0])
                return result[0]
    return None

# Добавление администратора
async def add_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await is_admin(user_id):
        await update.message.reply_text(get_text("access_denied", user_id))
        return ConversationHandler.END
    
    keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    message = await update.message.reply_text(
        get_text("add_admin_prompt", user_id),
        reply_markup=reply_markup
    )
    context.user_data['input_prompt_id'] = message.message_id
    context.user_data['input_state'] = 'add_admin'
    return ADD_ADMIN

# Удаление администратора
async def remove_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await is_admin(user_id):
        await update.message.reply_text(get_text("access_denied", user_id))
        return ConversationHandler.END
    
    keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    message = await update.message.reply_text(
        get_text("remove_admin_prompt", user_id),
        reply_markup=reply_markup
    )
    context.user_data['input_prompt_id'] = message.message_id
    context.user_data['input_state'] = 'remove_admin'
    return REMOVE_ADMIN

# Команда /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or f"user_{user_id}"
    ref_id = context.args[0].split("ref_")[-1] if context.args and "ref_" in context.args[0] else None
    
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
            [InlineKeyboardButton("Русский", callback_data="lang_ru"),
             InlineKeyboardButton("English", callback_data="lang_en")],
            [InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await update.message.reply_text(
            get_text("choose_language", user_id),
            reply_markup=reply_markup
        )
        context.user_data['input_prompt_id'] = message.message_id
        context.job_queue.run_once(
            callback=lambda x: delete_input_prompt(context, user_id),
            when=10,
            data={'user_id': user_id}
        )
        return CHOOSE_LANGUAGE
    
    await show_main_menu(update, context)
    return ConversationHandler.END

# Показ главного меню
async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    last_main_menu_id = context.user_data.get('last_main_menu_id')
    if last_main_menu_id:
        try:
            await context.bot.delete_message(chat_id=user_id, message_id=last_main_menu_id)
        except Exception:
            pass
    
    keyboard = [
        [InlineKeyboardButton(get_text("buy_stars_btn", user_id), callback_data="buy_stars")],
        [
            InlineKeyboardButton(get_text("profile_btn", user_id), callback_data="profile"),
            InlineKeyboardButton(get_text("referrals_btn", user_id), callback_data="referrals")
        ],
        [
            InlineKeyboardButton(get_text("tech_support_btn", user_id), callback_data="tech_support"),
            InlineKeyboardButton(get_text("reviews_btn", user_id), callback_data="reviews")
        ]
    ]
    if await is_admin(user_id):
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
    
    base_price_usd = float(get_setting("stars_price_usd") or 0.81) * (int(stars) / 50 if stars != '####' else 1)
    markup = float(get_setting("markup_percentage") or MARKUP_PERCENTAGE)
    price_usd = base_price_usd * (1 + markup / 100)
    commission = 0
    if payment_method == '@CryptoBot':
        commission = float(get_setting("cryptobot_commission") or 25) / 100
    elif payment_method == 'TON Wallet':
        commission = float(get_setting("ton_commission") or 20) / 100
    elif payment_method == 'Card':
        commission = float(get_setting("card_commission") or 30) / 100
    amount_usd = price_usd * (1 + commission) if stars != '####' else 0
    
    keyboard = [
        [InlineKeyboardButton(f"👤 @{target_username}", callback_data="set_username")],
        [InlineKeyboardButton(f"🌟 {stars}", callback_data="set_amount")],
        [InlineKeyboardButton(f"💸 {payment_method}", callback_data="set_payment_method")],
        [InlineKeyboardButton(f"💰 ${amount_usd:.2f}", callback_data="noop")],
        [
            InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel"),
            InlineKeyboardButton("✅ Оплатить", callback_data="confirm_payment")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    buy_menu_id = context.user_data.get('buy_menu_id')
    if buy_menu_id:
        try:
            await context.bot.delete_message(chat_id=user_id, message_id=buy_menu_id)
        except Exception:
            pass
    
    input_prompt_id = context.user_data.get('input_prompt_id')
    if input_prompt_id:
        try:
            await context.bot.delete_message(chat_id=user_id, message_id=input_prompt_id)
        except Exception:
            pass
        context.user_data['input_prompt_id'] = None
    
    message = await (update.message or update.callback_query.message).reply_text(
        get_text("buy_stars_prompt", user_id),
        reply_markup=reply_markup
    )
    context.user_data['buy_menu_id'] = message.message_id
    
    context.job_queue.run_once(
        callback=lambda x: delete_buy_menu(context, user_id),
        when=30,
        data={'user_id': user_id}
    )

# Удаление меню покупки
async def delete_buy_menu(context: ContextTypes.DEFAULT_TYPE, user_id):
    buy_menu_id = context.user_data.get('buy_menu_id')
    if buy_menu_id:
        try:
            await context.bot.delete_message(chat_id=user_id, message_id=buy_menu_id)
            context.user_data['buy_menu_id'] = None
        except Exception:
            pass

# Удаление временных сообщений
async def delete_input_prompt(context: ContextTypes.DEFAULT_TYPE, user_id):
    input_prompt_id = context.user_data.get('input_prompt_id')
    if input_prompt_id:
        try:
            await context.bot.delete_message(chat_id=user_id, message_id=input_prompt_id)
            context.user_data['input_prompt_id'] = None
        except Exception:
            pass

# Обработка кнопок
async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    username = query.from_user.username or f"user_{user_id}"
    
    await query.answer()
    
    last_main_menu_id = context.user_data.get('last_main_menu_id')
    if last_main_menu_id:
        try:
            await context.bot.delete_message(chat_id=user_id, message_id=last_main_menu_id)
        except Exception:
            pass
        context.user_data['last_main_menu_id'] = None
    
    if query.data.startswith("lang_"):
        language = query.data.split("_")[1]
        update_user_language(user_id, language)
        await show_main_menu(update, context)
        return ConversationHandler.END
    
    if query.data == "buy_stars":
        context.user_data['buy_username'] = '####'
        context.user_data['buy_stars'] = '####'
        context.user_data['payment_method'] = '####'
        context.user_data['buy_menu_id'] = None
        context.user_data['input_prompt_id'] = None
        await show_buy_menu(update, context)
        return BUY_STARS_USERNAME
    
    elif query.data == "set_username":
        buy_menu_id = context.user_data.get('buy_menu_id')
        if buy_menu_id:
            try:
                await context.bot.delete_message(chat_id=user_id, message_id=buy_menu_id)
            except Exception:
                pass
            context.user_data['buy_menu_id'] = None
        
        keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await query.message.reply_text(
            get_text("buy_username_prompt", user_id),
            reply_markup=reply_markup
        )
        context.user_data['input_prompt_id'] = message.message_id
        context.user_data['input_state'] = 'buy_username'
        return BUY_STARS_USERNAME
    
    elif query.data == "set_amount":
        buy_menu_id = context.user_data.get('buy_menu_id')
        if buy_menu_id:
            try:
                await context.bot.delete_message(chat_id=user_id, message_id=buy_menu_id)
            except Exception:
                pass
            context.user_data['buy_menu_id'] = None
        
        keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await query.message.reply_text(
            get_text("buy_amount_prompt", user_id, min_stars=get_setting("min_stars_purchase") or 10),
            reply_markup=reply_markup
        )
        
        context.user_data['input_prompt_id'] = message.message_id
        context.user_data['input_state'] = 'buy_amount'
        return BUY_STARS_AMOUNT
    
    elif query.data == "set_payment_method":
        buy_menu_id = context.user_data.get('buy_menu_id')
        if buy_menu_id:
            try:
                await context.bot.delete_message(chat_id=user_id, message_id=buy_menu_id)
            except Exception:
                pass
            context.user_data['buy_menu_id'] = None
        
        keyboard = [
            [InlineKeyboardButton("💳 Карта", callback_data="payment_card")],
            [InlineKeyboardButton("💸 Криптовалюта", callback_data="payment_crypto")],
            [InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await query.message.reply_text(
            get_text("buy_payment_method_prompt", user_id=user_id),
            reply_markup=reply_markup
        )
        
        context.user_data['input_prompt_id'] = message.message_id
        context.user_data['input_state'] = 'buy_payment_method'
        return BUY_STARS_PAYMENT_METHOD
    
    elif query.data == "payment_card":
        if not get_setting("card_payment_enabled"):
            input_prompt_id = context.user_data.get('input_prompt_id')
            if input_prompt_id:
                try:
                    await context.bot.delete_message(chat_id=user_id, message_id=input_prompt_id)
                except Exception:
                    pass
                context.user_data['input_prompt_id'] = None
            message = await query.message.reply_text(get_text("buy_card_disabled", user_id))
            context.user_data['input_prompt_id'] = message.message_id
            
            context.job_queue.run_once(
                callback=lambda x: delete_input_prompt(context, user_id),
                when=5,
                data={'user_id': user_id}
            )
            await show_buy_menu(update, context)
            return BUY_STARS_USERNAME
        context.user_data['payment_method'] = 'Card'
        await show_buy_menu(update, context)
        return BUY_STARS_USERNAME
    
    elif query.data == "payment_crypto":
        input_prompt_id = context.user_data.get('input_prompt_id')
        if input_prompt_id:
            try:
                await context.bot.delete_message(chat_id=user_id, message_id=input_prompt_id)
            except Exception:
                pass
            context.user_data['input_prompt_id'] = None
        keyboard = [
            [InlineKeyboardButton("@CryptoBot", callback_data="payment_cryptobot")],
            [InlineKeyboardButton("TON Wallet", callback_data="payment_ton")],
            [InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await query.message.reply_text(
            get_text("buy_crypto_method_prompt", user_id),
            reply_markup=reply_markup
        )
        context.user_data['input_prompt_id'] = message.message_id
        context.user_data['input_state'] = 'buy_payment_method'
        return BUY_STARS_PAYMENT_METHOD
    
    elif query.data == "payment_cryptobot":
        input_prompt_id = context.user_data.get('input_prompt_id')
        if input_prompt_id:
            try:
                await context.bot.delete_message(chat_id=user_id, message_id=input_prompt_id)
            except Exception:
                pass
            context.user_data['input_prompt_id'] = None
        context.user_data['payment_method'] = '@CryptoBot'
        await show_buy_menu(update, context)
        return BUY_STARS_USERNAME
    
    elif query.data == "payment_ton":
        input_prompt_id = context.user_data.get('input_prompt_id')
        if input_prompt_id:
            try:
                await context.bot.delete_message(chat_id=user_id, message_id=input_prompt_id)
            except Exception:
                pass
            context.user_data['input_prompt_id'] = None
        context.user_data['payment_method'] = 'TON Wallet'
        await show_buy_menu(update, context)
        return BUY_STARS_USERNAME
    
    elif query.data == "cancel":
        input_prompt_id = context.user_data.get('input_prompt_id')
        if input_prompt_id:
            try:
                await context.bot.delete_message(chat_id=user_id, message_id=input_prompt_id)
            except Exception:
                pass
        buy_menu_id = context.user_data.get('buy_menu_id')
        if buy_menu_id:
            try:
                await context.bot.delete_message(chat_id=user_id, message_id=buy_menu_id)
            except Exception:
                pass
        context.user_data['buy_username'] = None
        context.user_data['buy_stars'] = None
        context.user_data['payment_method'] = None
        context.user_data['buy_menu_id'] = None
        context.user_data['input_prompt_id'] = None
        context.user_data['input_state'] = None
        message = await query.message.reply_text(get_text("cancel", user_id))
        context.user_data['input_prompt_id'] = message.message_id
        context.job_queue.run_once(
            callback=lambda x: delete_input_prompt(context, user_id),
            when=5,
            data={'user_id': user_id}
        )
        await show_main_menu(update, context)
        return ConversationHandler.END
    
    elif query.data == "confirm_payment":
        buy_menu_id = context.user_data.get('buy_menu_id')
        if buy_menu_id:
            try:
                await context.bot.delete_message(chat_id=user_id, message_id=buy_menu_id)
            except Exception:
                pass
            context.user_data['buy_menu_id'] = None
        
        target_username = context.user_data.get('buy_username')
        stars = context.user_data.get('buy_stars')
        payment_method = context.user_data.get('payment_method')
        
        if target_username == '####' or stars == '####' or payment_method == '####':
            message = await query.message.reply_text(get_text("buy_error_fill_fields", user_id))
            context.user_data['input_prompt_id'] = message.message_id
            context.job_queue.run_once(
                callback=lambda x: delete_input_prompt(context, user_id),
                when=5,
                data={'user_id': user_id}
            )
            await show_buy_menu(update, context)
            return BUY_STARS_USERNAME
        
        base_price_usd = float(get_setting("stars_price_usd") or 0.81) * (int(stars) / 50)
        markup = float(get_setting("markup_percentage") or MARKUP_PERCENTAGE)
        price_usd = base_price_usd * (1 + markup / 100)
        commission = 0
        if payment_method == '@CryptoBot':
            commission = float(get_setting("cryptobot_commission") or 25) / 100
        elif payment_method == 'TON Wallet':
            commission = float(get_setting("ton_commission") or 20) / 100
        elif payment_method == 'Card':
            commission = float(get_setting("card_commission") or 30) / 100
        amount_usd = price_usd * (1 + commission)
        
        if payment_method == '@CryptoBot' or payment_method == 'Card':
            currency = 'USD' if payment_method == 'Card' else 'USDT'
            invoice_info = await create_cryptobot_invoice(user_id, amount_usd, currency, target_username, stars)
            if invoice_info:
                invoice_id = invoice_info['invoice_id']
                payment_url = invoice_info['pay_url']
                
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE users SET stars_bought = %s, username = %s, cryptobot_invoice_id = %s WHERE user_id = %s;",
                            (stars, target_username, invoice_id, user_id)
                        )
                        conn.commit()
                
                keyboard = [
                    [InlineKeyboardButton("✅ Оплатить", url=payment_url)],
                    [InlineKeyboardButton("🔄 Проверить", callback_data="check_payment")],
                    [InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                message = await query.message.reply_text(
                    get_text(f"buy_{'card' if payment_method == 'Card' else 'cryptobot'}_prompt", user_id, amount_usd=amount_usd, stars=stars, username=target_username),
                    reply_markup=reply_markup
                )
                context.user_data['input_prompt_id'] = message.message_id
            else:
                message = await query.message.reply_text(get_text("buy_error_cryptobot", user_id))
                context.user_data['input_prompt_id'] = message.message_id
                context.job_queue.run_once(
                    callback=lambda x: delete_input_prompt(context, user_id),
                    when=5,
                    data={'user_id': user_id}
                )
                await show_main_menu(update, context)
            return ConversationHandler.END
        
        elif payment_method == 'TON Wallet':
            ton_exchange_rate = float(get_setting("ton_exchange_rate") or 2.93)
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
                    [InlineKeyboardButton("✅ Оплатить", url=f"https://ton.app/wallet/pay?address={address}&amount={amount_ton}&memo={memo}")],
                    [InlineKeyboardButton("🔄 Проверить", callback_data="check_payment")],
                    [InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                message = await query.message.reply_text(
                    get_text("buy_ton_prompt", user_id, amount_ton=amount_ton, stars=stars, address=address, memo=memo, username=target_username),
                    reply_markup=reply_markup
                )
                context.user_data['input_prompt_id'] = message.message_id
            return ConversationHandler.END
    
    elif query.data == "check_payment":
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT username, stars_bought, address, memo, amount_ton, cryptobot_invoice_id FROM users WHERE user_id = %s;",
                    (user_id,)
                )
                result = cur.fetchone()
                if not result:
                    message = await query.message.reply_text("⚠️ Нет активных заказов.")
                    context.user_data['input_prompt_id'] = message.message_id
                    context.job_queue.run_once(
                        callback=lambda x: delete_input_prompt(context, user_id),
                        when=5,
                        data={'user_id': user_id}
                    )
                    await show_main_menu(update, context)
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
                        base_price_usd = float(get_setting("stars_price_usd") or 0.81) * (stars / 50)
                        markup = float(get_setting("markup_percentage") or MARKUP_PERCENTAGE)
                        total_profit_usd = float(get_setting("total_profit_usd") or 0) + (base_price_usd * (markup / 100))
                        total_profit_ton = float(get_setting("total_profit_ton") or 0) + (float(amount_ton) if amount_ton else 0)
                        update_setting("total_stars_sold", total_stars_sold)
                        update_setting("total_profit_usd", total_profit_usd)
                        update_setting("total_profit_ton", total_profit_ton)
                        conn.commit()
                message = await query.message.reply_text(
                    get_text("buy_success", user_id, username=target_username, stars=stars)
                )
                context.user_data['input_prompt_id'] = message.message_id
                context.job_queue.run_once(
                    callback=lambda x: delete_input_prompt(context, user_id),
                    when=5,
                    data={'user_id': user_id}
                )
            else:
                message = await query.message.reply_text("❌ Не удалось выдать звёзды. Обратитесь в поддержку.")
                context.user_data['input_prompt_id'] = message.message_id
                context.job_queue.run_once(
                    callback=lambda x: delete_input_prompt(context, user_id),
                    when=5,
                    data={'user_id': user_id}
                )
            await show_main_menu(update, context)
        else:
            message = await query.message.reply_text("⚠️ Оплата не подтверждена. Попробуй снова.")
            context.user_data['input_prompt_id'] = message.message_id
            context.job_queue.run_once(
                callback=lambda x: delete_input_prompt(context, user_id),
                when=5,
                data={'user_id': user_id}
            )
        return ConversationHandler.END
    
    elif query.data == "profile":
        stars_bought = get_user_data(user_id, "stars_bought") or 0
        ref_bonus_ton = get_user_data(user_id, "ref_bonus_ton") or 0
        keyboard = [
            [InlineKeyboardButton(get_text("back_btn", user_id), callback_data="back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await query.message.reply_text(
            get_text("profile", user_id, username=username, stars_bought=stars_bought, ref_bonus_ton=ref_bonus_ton),
            reply_markup=reply_markup
        )
        context.user_data['input_prompt_id'] = message.message_id
        context.job_queue.run_once(
            callback=lambda x: delete_input_prompt(context, user_id),
            when=5,
            data={'user_id': user_id}
        )
    
    elif query.data == "referrals":
        ref_bonus_ton = get_user_data(user_id, "ref_bonus_ton") or 0
        referrals = get_user_data(user_id, "referrals") or []
        keyboard = [
            [InlineKeyboardButton("🏆 Топ-5 рефералов", callback_data="top_referrals")],
            [InlineKeyboardButton("🏆 Топ-5 покупок", callback_data="top_purchases")],
            [InlineKeyboardButton(get_text("back_btn", user_id), callback_data="back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await query.message.reply_text(
            get_text("ref_info", user_id, ref_bonus_ton=ref_bonus_ton, bot_username=context.bot.name.lstrip("@"), user_id=user_id, ref_count=len(referrals)),
            reply_markup=reply_markup
        )
        context.user_data['input_prompt_id'] = message.message_id
        context.job_queue.run_once(
            callback=lambda x: delete_input_prompt(context, user_id),
            when=5,
            data={'user_id': user_id}
        )
    
    elif query.data == "top_referrals":
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT username, jsonb_array_length(referrals) as ref_count
                    FROM users
                    WHERE jsonb_array_length(referrals) > 0
                    ORDER BY ref_count DESC
                    LIMIT 5;
                """)
                top_referrals = cur.fetchall()
        
        text = get_text("top_referrals", user_id) + "\n"
        if top_referrals:
            for i, (username, ref_count) in enumerate(top_referrals, 1):
                text += f"{i}. @{username}: {ref_count} рефералов\n"
        else:
            text += get_text("no_referrals", user_id)
        keyboard = [
            [InlineKeyboardButton(get_text("back_btn", user_id), callback_data="back")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await query.message.reply_text(
            text,
            reply_markup=reply_markup
        )
        context.user_data['input_prompt_id'] = message.message_id
        context.job_queue.run_once(
            callback=lambda x: delete_input_prompt(context, user_id),
            when=5,
            data={'user_id': user_id}
        )
    
    elif query.data == "top_purchases":
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT username, stars_bought
                    FROM users
                    WHERE stars_bought > 0
                    ORDER BY stars_bought DESC
                    LIMIT 5;
                """)
                top_purchases = cur.fetchall()
        
        text = get_text("top_purchases", user_id) + "\n"
        if top_purchases:
            for i, (username, stars_bought) in enumerate(top_purchases, 1):
                text += f"{i}. @{username}: {stars_bought} звёзд\n"
        else:
            text += get_text("no_purchases", user_id)
        keyboard = [
            [InlineKeyboardButton(get_text("back_btn", user_id), callback_data="back")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await query.message.reply_text(
            text,
            reply_markup=reply_markup
        )
        context.user_data['input_prompt_id'] = message.message_id
        context.job_queue.run_once(
            callback=lambda x: delete_input_prompt(context, user_id),
            when=5,
            data={'user_id': user_id}
        )
    
    elif query.data == "tech_support":
        support_channel = get_setting("support_channel") or "@support_channel"
        keyboard = [
            [InlineKeyboardButton(get_text("back_btn", user_id), callback_data="back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await query.message.reply_text(
            get_text("tech_support", user_id, support_channel=support_channel),
            reply_markup=reply_markup
        )
        context.user_data['input_prompt_id'] = message.message_id
        context.job_queue.run_once(
            callback=lambda x: delete_input_prompt(context, user_id),
            when=5,
            data={'user_id': user_id}
        )
    
    elif query.data == "reviews":
        review_channel = get_setting("review_channel") or "@sacoectasy"
        keyboard = [
            [InlineKeyboardButton(get_text("back_btn", user_id), callback_data="back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await query.message.reply_text(
            get_text("reviews", user_id, review_channel=review_channel),
            reply_markup=reply_markup
        )
        context.user_data['input_prompt_id'] = message.message_id
        context.job_queue.run_once(
            callback=lambda x: delete_input_prompt(context, user_id),
            when=5,
            data={'user_id': user_id}
        )
    
    elif query.data == "back":
        input_prompt_id = context.user_data.get('input_prompt_id')
        if input_prompt_id:
            try:
                await context.bot.delete_message(chat_id=user_id, message_id=input_prompt_id)
            except Exception:
                pass
            context.user_data['input_prompt_id'] = None
        await show_main_menu(update, context)
        return ConversationHandler.END
    
    elif query.data == "admin_panel" and await is_admin(user_id):
        keyboard = [
            [InlineKeyboardButton(get_text("edit_text_btn", user_id), callback_data="edit_text"),
             InlineKeyboardButton(get_text("set_price_btn", user_id), callback_data="set_price")],
            [InlineKeyboardButton(get_text("set_percent_btn", user_id), callback_data="set_percent"),
             InlineKeyboardButton(get_text("set_commissions_btn", user_id), callback_data="set_commissions")],
            [InlineKeyboardButton(get_text("set_review_channel_btn", user_id), callback_data="set_review_channel"),
             InlineKeyboardButton(get_text("set_card_payment_btn", user_id), callback_data="set_card_payment")],
            [InlineKeyboardButton(get_text("set_markup_btn", user_id), callback_data="set_markup"),
             InlineKeyboardButton(get_text("stats_btn", user_id), callback_data="stats")],
            [InlineKeyboardButton(get_text("manage_admins_btn", user_id), callback_data="manage_admins")],
            [InlineKeyboardButton(get_text("back_btn", user_id), callback_data="back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await query.message.reply_text("⚙️ Админ-панель", reply_markup=reply_markup)
        context.user_data['input_prompt_id'] = message.message_id
        context.job_queue.run_once(
            callback=lambda x: delete_input_prompt(context, user_id),
            when=5,
            data={'user_id': user_id}
        )
    
    elif query.data == "manage_admins" and await is_admin(user_id):
        keyboard = [
            [InlineKeyboardButton(get_text("add_admin_btn", user_id), callback_data="add_admin"),
             InlineKeyboardButton(get_text("remove_admin_btn", user_id), callback_data="remove_admin")],
            [InlineKeyboardButton(get_text("back_btn", user_id), callback_data="back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await query.message.reply_text("👑 Управление админами", reply_markup=reply_markup)
        context.user_data['input_prompt_id'] = message.message_id
        context.job_queue.run_once(
            callback=lambda x: delete_input_prompt(context, user_id),
            when=5,
            data={'user_id': user_id}
        )
    
    elif query.data == "add_admin" and await is_admin(user_id):
        keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await query.message.reply_text(
            get_text("add_admin_prompt", user_id),
            reply_markup=reply_markup
        )
        context.user_data['input_prompt_id'] = message.message_id
        context.user_data['input_state'] = 'add_admin'
        return ADD_ADMIN
    
    elif query.data == "remove_admin" and await is_admin(user_id):
        keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await query.message.reply_text(
            get_text("remove_admin_prompt", user_id),
            reply_markup=reply_markup
        )
        context.user_data['input_prompt_id'] = message.message_id
        context.user_data['input_state'] = 'remove_admin'
        return REMOVE_ADMIN
    
    elif query.data == "edit_text" and await is_admin(user_id):
        keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await query.message.reply_text(
            get_text("edit_text_prompt", user_id),
            reply_markup=reply_markup
        )
        context.user_data['input_prompt_id'] = message.message_id
        context.user_data['input_state'] = 'edit_text'
        return EDIT_TEXT
    
    elif query.data == "set_price" and await is_admin(user_id):
        keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await query.message.reply_text(
            get_text("set_price_prompt", user_id),
            reply_markup=reply_markup
        )
        context.user_data['input_prompt_id'] = message.message_id
        context.user_data['input_state'] = 'set_price'
        return SET_PRICE
    
    elif query.data == "set_percent" and await is_admin(user_id):
        keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await query.message.reply_text(
            get_text("set_percent_prompt", user_id),
            reply_markup=reply_markup
        )
        context.user_data['input_prompt_id'] = message.message_id
        context.user_data['input_state'] = 'set_percent'
        return SET_PERCENT
    
    elif query.data == "set_commissions" and await is_admin(user_id):
        keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await query.message.reply_text(
            get_text("set_commissions_prompt", user_id),
            reply_markup=reply_markup
        )
        context.user_data['input_prompt_id'] = message.message_id
        context.user_data['input_state'] = 'set_commissions'
        return SET_COMMISSIONS
    
    elif query.data == "set_review_channel" and await is_admin(user_id):
        keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await query.message.reply_text(
            get_text("set_review_channel_prompt", user_id),
            reply_markup=reply_markup
        )
        context.user_data['input_prompt_id'] = message.message_id
        context.user_data['input_state'] = 'set_review_channel'
        return SET_REVIEW_CHANNEL
    
    elif query.data == "set_card_payment" and await is_admin(user_id):
        keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await query.message.reply_text(
            get_text("set_card_payment_prompt", user_id),
            reply_markup=reply_markup
        )
        context.user_data['input_prompt_id'] = message.message_id
        context.user_data['input_state'] = 'set_card_payment'
        return SET_CARD_PAYMENT
    
    elif query.data == "set_markup" and await is_admin(user_id):
        keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await query.message.reply_text(
            get_text("set_markup_prompt", user_id),
            reply_markup=reply_markup
        )
        context.user_data['input_prompt_id'] = message.message_id
        context.user_data['input_state'] = 'set_markup'
        return SET_MARKUP
    
    elif query.data == "stats" and await is_admin(user_id):
        total_stars_sold = get_setting("total_stars_sold") or 0
        total_profit_usd = float(get_setting("total_profit_usd") or 0)
        total_profit_ton = float(get_setting("total_profit_ton") or 0)
        keyboard = [
            [InlineKeyboardButton(get_text("back_btn", user_id), callback_data="back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await query.message.reply_text(
            get_text("stats", user_id, total_stars_sold=total_stars_sold, total_profit_usd=total_profit_usd, total_profit_ton=total_profit_ton),
            reply_markup=reply_markup
        )
        context.user_data['input_prompt_id'] = message.message_id
        context.job_queue.run_once(
            callback=lambda x: delete_input_prompt(context, user_id),
            when=5,
            data={'user_id': user_id}
        )
    
    elif query.data == "reset_profit" and await is_admin(user_id):
        update_setting("total_profit_usd", "0")
        update_setting("total_profit_ton", "0")
        log_admin_action(user_id, "Reset profit")
        keyboard = [
            [InlineKeyboardButton(get_text("back_btn", user_id), callback_data="back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await query.message.reply_text(
            get_text("reset_profit", user_id),
            reply_markup=reply_markup
        )
        context.user_data['input_prompt_id'] = message.message_id
        context.job_queue.run_once(
            callback=lambda x: delete_input_prompt(context, user_id),
            when=5,
            data={'user_id': user_id}
        )

# Обработка текстового ввода
async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    state = context.user_data.get('input_state')
    text = update.message.text.strip()
    
    input_prompt_id = context.user_data.get('input_prompt_id')
    if input_prompt_id:
        try:
            await context.bot.delete_message(chat_id=user_id, message_id=input_prompt_id)
        except Exception:
            pass
        context.user_data['input_prompt_id'] = None
    
    try:
        await update.message.delete()
    except Exception:
        pass
    
    if state == 'buy_username':
        if text.startswith('@'):
            keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            message = await update.message.reply_text(
                get_text("buy_invalid_username", user_id),
                reply_markup=reply_markup
            )
            context.user_data['input_prompt_id'] = message.message_id
            context.job_queue.run_once(
                callback=lambda x: delete_input_prompt(context, user_id),
                when=5,
                data={'user_id': user_id}
            )
            return BUY_STARS_USERNAME
        context.user_data['buy_username'] = text
        await show_buy_menu(update, context)
        return BUY_STARS_USERNAME

    elif state == 'buy_amount':
        try:
            stars = int(text)
            min_stars = int(get_setting("min_stars_purchase") or 10)
            if stars < min_stars:
                keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                message = await update.message.reply_text(
                    get_text("buy_invalid_amount", user_id, min_stars=min_stars),
                    reply_markup=reply_markup
                )
                context.user_data['input_prompt_id'] = message.message_id
                context.job_queue.run_once(
                    callback=lambda x: delete_input_prompt(context, user_id),
                    when=5,
                    data={'user_id': user_id}
                )
                return BUY_STARS_AMOUNT
            context.user_data['buy_stars'] = stars
            await show_buy_menu(update, context)
            return BUY_STARS_USERNAME
        except ValueError:
            keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            message = await update.message.reply_text(
                get_text("buy_invalid_amount", user_id, min_stars=get_setting("min_stars_purchase") or 10),
                reply_markup=reply_markup
            )
            context.user_data['input_prompt_id'] = message.message_id
            context.job_queue.run_once(
                callback=lambda x: delete_input_prompt(context, user_id),
                when=5,
                data={'user_id': user_id}
            )
            return BUY_STARS_AMOUNT

    elif state == 'edit_text' and await is_admin(user_id):
        if ':' not in text or len(text.split(':', 1)) != 2:
            keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            message = await update.message.reply_text(
                get_text("text_format", user_id),
                reply_markup=reply_markup
            )
            context.user_data['input_prompt_id'] = message.message_id
            return EDIT_TEXT
        key, value = text.split(':', 1)
        key = key.strip()
        value = value.strip()
        if not key.endswith(('_ru', '_en')):
            keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            message = await update.message.reply_text(
                get_text("invalid_text_key", user_id),
                reply_markup=reply_markup
            )
            context.user_data['input_prompt_id'] = message.message_id
            return EDIT_TEXT
        update_text(key, value)
        log_admin_action(user_id, f"Edited text: {key}")
        message = await update.message.reply_text(
            get_text("text_updated", user_id, key=key)
        )
        context.user_data['input_prompt_id'] = message.message_id
        context.job_queue.run_once(
            callback=lambda x: delete_input_prompt(context, user_id),
            when=5,
            data={'user_id': user_id}
        )
        await show_main_menu(update, context)
        return ConversationHandler.END

    elif state == 'set_price' and await is_admin(user_id):
        if ':' not in text or len(text.split(':', 1)) != 2:
            keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            message = await update.message.reply_text(
                get_text("price_format", user_id),
                reply_markup=reply_markup
            )
            context.user_data['input_prompt_id'] = message.message_id
            return SET_PRICE
        try:
            price_usd, stars = map(float, text.split(':', 1))
            if price_usd <= 0 or stars <= 0:
                raise ValueError
            update_setting("stars_price_usd", price_usd)
            update_setting("stars_per_purchase", stars)
            log_admin_action(user_id, f"Set price: ${price_usd} for {stars} stars")
            message = await update.message.reply_text(
                get_text("price_set", user_id, price_usd=price_usd, stars=stars)
            )
            context.user_data['input_prompt_id'] = message.message_id
            context.job_queue.run_once(
                callback=lambda x: delete_input_prompt(context, user_id),
                when=5,
                data={'user_id': user_id}
            )
            await show_main_menu(update, context)
            return ConversationHandler.END
        except ValueError:
            keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            message = await update.message.reply_text(
                get_text("invalid_price", user_id),
                reply_markup=reply_markup
            )
            context.user_data['input_prompt_id'] = message.message_id
            return SET_PRICE

    elif state == 'set_percent' and await is_admin(user_id):
        if ':' not in text or len(text.split(':', 1)) != 2:
            keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            message = await update.message.reply_text(
                get_text("percent_format", user_id),
                reply_markup=reply_markup
            )
            context.user_data['input_prompt_id'] = message.message_id
            return SET_PERCENT
        try:
            ref_bonus, profit = map(float, text.split(':', 1))
            if not (0 <= ref_bonus <= 100 and 10 <= profit <= 50):
                keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                message = await update.message.reply_text(
                    get_text("invalid_percent", user_id),
                    reply_markup=reply_markup
                )
                context.user_data['input_prompt_id'] = message.message_id
                return SET_PERCENT
            update_setting("ref_bonus_percent", ref_bonus)
            update_setting("profit_percent", profit)
            log_admin_action(user_id, f"Set percentages: ref_bonus {ref_bonus}%, profit {profit}%")
            message = await update.message.reply_text(
                get_text("percent_set", user_id, ref_bonus=ref_bonus, profit=profit)
            )
            context.user_data['input_prompt_id'] = message.message_id
            context.job_queue.run_once(
                callback=lambda x: delete_input_prompt(context, user_id),
                when=5,
                data={'user_id': user_id}
            )
            await show_main_menu(update, context)
            return ConversationHandler.END
        except ValueError:
            keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            message = await update.message.reply_text(
                get_text("invalid_percent", user_id),
                reply_markup=reply_markup
            )
            context.user_data['input_prompt_id'] = message.message_id
            return SET_PERCENT

    elif state == 'set_commissions' and await is_admin(user_id):
        if text.count(':') != 2:
            keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            message = await update.message.reply_text(
                get_text("commissions_format", user_id),
                reply_markup=reply_markup
            )
            context.user_data['input_prompt_id'] = message.message_id
            return SET_COMMISSIONS
        try:
            cryptobot, ton, card = map(float, text.split(':'))
            if not all(0 <= x <= 100 for x in [cryptobot, ton, card]):
                keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                message = await update.message.reply_text(
                    get_text("invalid_commissions", user_id),
                    reply_markup=reply_markup
                )
                context.user_data['input_prompt_id'] = message.message_id
                return SET_COMMISSIONS
            update_setting("cryptobot_commission", cryptobot)
            update_setting("ton_commission", ton)
            update_setting("card_commission", card)
            log_admin_action(user_id, f"Set commissions: CryptoBot {cryptobot}%, TON {ton}%, Card {card}%")
            message = await update.message.reply_text(
                get_text("commissions_set", user_id, cryptobot=cryptobot, ton=ton, card=card)
            )
            context.user_data['input_prompt_id'] = message.message_id
            context.job_queue.run_once(
                callback=lambda x: delete_input_prompt(context, user_id),
                when=5,
                data={'user_id': user_id}
            )
            await show_main_menu(update, context)
            return ConversationHandler.END
        except ValueError:
            keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            message = await update.message.reply_text(
                get_text("invalid_commissions", user_id),
                reply_markup=reply_markup
            )
            context.user_data['input_prompt_id'] = message.message_id
            return SET_COMMISSIONS

    elif state == 'set_review_channel' and await is_admin(user_id):
        if not text.startswith('@'):
            keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            message = await update.message.reply_text(
                get_text("invalid_channel", user_id),
                reply_markup=reply_markup
            )
            context.user_data['input_prompt_id'] = message.message_id
            return SET_REVIEW_CHANNEL
        update_setting("review_channel", text)
        log_admin_action(user_id, f"Set review channel: {text}")
        message = await update.message.reply_text(
            get_text("channel_set", user_id, channel=text)
        )
        context.user_data['input_prompt_id'] = message.message_id
        context.job_queue.run_once(
            callback=lambda x: delete_input_prompt(context, user_id),
            when=5,
            data={'user_id': user_id}
        )
        await show_main_menu(update, context)
        return ConversationHandler.END

        elif state == 'set_card_payment':
            if not await is_admin(user_id):
                await update.message.reply_text(get_text("access_denied", user_id))
                return ConversationHandler.END
            if text.lower() not in ('true', 'false'):
                keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                message = await update.message.reply_text(
                    get_text("set_card_payment_prompt", user_id),
                    reply_markup=reply_markup
                )
                context.user_data['input_prompt_id'] = message.message_id
                return SET_CARD_PAYMENT
            update_setting("card_payment_enabled", text.lower())
            log_admin_action(user_id, f"Set card payment: {text.lower()}")
            message = await update.message.reply_text(
                get_text("card_payment_set", user_id, status=text.lower())
            )
            context.user_data['input_prompt_id'] = message.message_id
            context.job_queue.run_once(
                callback=lambda x: delete_input_prompt(context, user_id),
                when=5,
                data={'user_id': user_id}
            )
            await show_main_menu(update, context)
            return ConversationHandler.END

    elif state == 'set_markup' and await is_admin(user_id):
    try:
        markup = float(text)
        if not 0 <= markup <= 100:
            keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            message = await update.message.reply_text(
                get_text("invalid_markup", user_id),
                reply_markup=reply_markup
            )
            context.user_data['input_prompt_id'] = message.message_id
            return SET_MARKUP
        update_setting("markup_percentage", markup)
        log_admin_action(user_id, f"Set markup: {markup}%")
        message = await update.message.reply_text(
            get_text("markup_set", user_id, markup=markup)
        )
        context.user_data['input_prompt_id'] = message.message_id
        context.job_queue.run_once(
            callback=lambda x: delete_input_prompt(context, user_id),
            when=5,
            data={'user_id': user_id}
        )
        await show_main_menu(update, context)
        return ConversationHandler.END
    except ValueError:
        keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await update.message.reply_text(
            get_text("markup_format", user_id),
            reply_markup=reply_markup
        )
        context.user_data['input_prompt_id'] = message.message_id
        return SET_MARKUP

    elif state == 'add_admin' and await is_admin(user_id):
        try:
            admin_id = int(text)
            admin_ids = get_setting("admin_ids") or [6956377285]
            if admin_id in admin_ids:
                keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                message = await update.message.reply_text(
                    get_text("admin_already_exists", user_id, user_id=admin_id),
                    reply_markup=reply_markup
                )
                context.user_data['input_prompt_id'] = message.message_id
                return ADD_ADMIN
            admin_ids.append(admin_id)
            update_setting("admin_ids", admin_ids)
            log_admin_action(user_id, f"Added admin: {admin_id}")
            message = await update.message.reply_text(
                get_text("admin_added", user_id, user_id=admin_id)
            )
            context.user_data['input_prompt_id'] = message.message_id
            context.job_queue.run_once(
                callback=lambda x: delete_input_prompt(context, user_id),
                when=5,
                data={'user_id': user_id}
            )
            await show_main_menu(update, context)
            return ConversationHandler.END
        except ValueError:
            keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            message = await update.message.reply_text(
                get_text("invalid_admin_id", user_id),
                reply_markup=reply_markup
            )
            context.user_data['input_prompt_id'] = message.message_id
            return ADD_ADMIN

    elif state == 'remove_admin' and await is_admin(user_id):
        try:
            admin_id = int(text)
            admin_ids = get_setting("admin_ids") or [6956377285]
            if admin_id not in admin_ids:
                keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                message = await update.message.reply_text(
                    get_text("admin_not_found", user_id, user_id=admin_id),
                    reply_markup=reply_markup
                )
                context.user_data['input_prompt_id'] = message.message_id
                return REMOVE_ADMIN
            admin_ids.remove(admin_id)
            update_setting("admin_ids", admin_ids)
            log_admin_action(user_id, f"Removed admin: {admin_id}")
            message = await update.message.reply_text(
                get_text("admin_removed", user_id, user_id=admin_id)
            )
            context.user_data['input_prompt_id'] = message.message_id
            context.job_queue.run_once(
                callback=lambda x: delete_input_prompt(context, user_id),
                when=5,
                data={'user_id': user_id}
            )
            await show_main_menu(update, context)
            return ConversationHandler.END
        except ValueError:
            keyboard = [[InlineKeyboardButton(get_text("cancel_btn", user_id), callback_data="cancel")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            message = await update.message.reply_text(
                get_text("invalid_admin_id", user_id),
                reply_markup=reply_markup
            )
            context.user_data['input_prompt_id'] = message.message_id
            return REMOVE_ADMIN

# Обработчик ошибок
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Update {update} caused error {context.error}")
    if update and update.effective_user:
        user_id = update.effective_user.id
        await context.bot.send_message(
            chat_id=user_id,
            text=get_text("tech_support", user_id, support_channel=get_setting("support_channel") or "@support_channel")
        )

# Главная функция
def main():
    application = Application.builder().token(BOT_TOKEN).build()

    init_db()

    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            CommandHandler("addadmin", cmd_add_admin),
            CommandHandler("removeadmin", cmd_remove_admin),
            CallbackQueryHandler(button)
        ],
        states={
            CHOOSE_LANGUAGE: [CallbackQueryHandler(button)],
            BUY_STARS_USERNAME: [
                CallbackQueryHandler(button),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
            ],
            BUY_STARS_AMOUNT: [
                CallbackQueryHandler(button),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
            ],
            BUY_STARS_PAYMENT_METHOD: [
                CallbackQueryHandler(button),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
            ],
            EDIT_TEXT: [
                CallbackQueryHandler(button),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
            ],
            SET_PRICE: [
                CallbackQueryHandler(button),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
            ],
            SET_PERCENT: [
                CallbackQueryHandler(button),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
            ],
            SET_COMMISSIONS: [
                CallbackQueryHandler(button),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
            ],
            SET_REVIEW_CHANNEL: [
                CallbackQueryHandler(button),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
            ],
            SET_CARD_PAYMENT: [
                CallbackQueryHandler(button),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
            ],
            SET_MARKUP: [
                CallbackQueryHandler(button),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
            ],
            ADD_ADMIN: [
                CallbackQueryHandler(button),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
            ],
            REMOVE_ADMIN: [
                CallbackQueryHandler(button),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input)
            ]
        },
        fallbacks=[CommandHandler("start", start)]
    )

    application.add_handler(conv_handler)
    application.add_error_handler(error_handler)

    application.job_queue.run_repeating(update_ton_price, interval=3600, first=10)
    application.job_queue.run_repeating(payment_checker, interval=30, first=10)

    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
