import os
import time
import json
import logging
import asyncio
import aiohttp
import psycopg2
from uuid import uuid4
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
from undetected_chromedriver import Chrome, ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv


load_dotenv("/etc/secrets/.env")  

BOT_TOKEN = os.getenv("BOT_TOKEN")
POSTGRES_URL = os.getenv("POSTGRES_URL")
TONAPI_KEY = os.getenv("TONAPI_KEY")
OWNER_WALLET = os.getenv("OWNER_WALLET")
SPLIT_TG_WALLET = os.getenv("SPLIT_TG_WALLET")
# Логирование
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Состояния для ConversationHandler
EDIT_TEXT, SET_PRICE, SET_PERCENT, SET_REVIEW_CHANNEL = range(4)

def get_db_connection():
    return psycopg2.connect(POSTGRES_URL)

# Инициализация базы данных
def init_db():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Таблица настроек
            cur.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
            """)
            # Таблица пользователей
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
                    amount_ton FLOAT
                );
            """)
            # Таблица логов админов
            cur.execute("""
                CREATE TABLE IF NOT EXISTS admin_log (
                    id SERIAL PRIMARY KEY,
                    admin_id BIGINT,
                    action TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            # Таблица текстов
            cur.execute("""
                CREATE TABLE IF NOT EXISTS texts (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
            """)
            # Инициализация настроек
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
                    ('ton_exchange_rate', '8'),
                    ('review_channel', '@sacoectasy')
                ON CONFLICT (key) DO NOTHING;
            """)
            # Инициализация текстов
            cur.execute("""
                INSERT INTO texts (key, value)
                VALUES
                    ('welcome', 'Добро пожаловать! Купите Telegram Stars за TON.\nЗвезд продано: {total_stars_sold}'),
                    ('buy_prompt', 'Оплатите {amount_ton:.6f} TON для {stars} звезд через TON Space.\nАдрес: {address}\nMemo: {memo}'),
                    ('buy_success', 'Оплата прошла! Вы получили {stars} звезд.'),
                    ('ref_info', 'Ваш реф. бонус: {ref_bonus_ton:.6f} TON\nРеф. ссылка: t.me/{bot_username}?start=ref_{user_id}'),
                    ('tech_support', 'Свяжитесь с техподдержкой: {support_channel}'),
                    ('reviews', 'Оставьте отзыв: {review_channel}')
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
def get_text(key, **kwargs):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM texts WHERE key = %s;", (key,))
            result = cur.fetchone()
            if result:
                return result[0].format(**kwargs)
    return "Текст не найден."

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

# Selenium: Инициализация драйвера
def init_selenium_driver():
    chrome_options = ChromeOptions()
    if SELENIUM_HEADLESS:
        chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    driver = Chrome(options=chrome_options)
    return driver

# Selenium: Выдача звезд через https://split.tg/premium (по Pastebin)
async def issue_stars_selenium(username, stars):
    driver = init_selenium_driver()
    try:
        logger.info(f"Выдача {stars} звезд для @{username} через Selenium")
        driver.get("https://split.tg/premium")
        
        # Ожидание загрузки страницы
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Подключение кошелька
        connect_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Connect Wallet')]"))
        )
        connect_button.click()
        
        # Ввод username
        username_field = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//input[@placeholder='Enter Telegram username']"))
        )
        username_field.send_keys(username)
        
        # Ввод количества звезд
        stars_field = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//input[@placeholder='Enter stars amount']"))
        )
        stars_field.send_keys(str(stars))
        
        # Подтверждение транзакции
        confirm_button = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Confirm')]"))
        )
        confirm_button.click()
        
        # Ожидание результата
        success_message = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(text(), 'Stars issued successfully')]"))
        )
        logger.info(f"Звезды выданы: @{username}, {stars}")
        return True
    except Exception as e:
        logger.error(f"Ошибка Selenium: {e}")
        return False
    finally:
        driver.quit()

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
                if await issue_stars_selenium(username, stars):
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
                        get_text("buy_success", stars=stars)
                    )
                    # Реферальный бонус
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
                            f"Ваш реф. бонус: +{ref_bonus_ton:.6f} TON от @{username}"
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
            conn.commit()
    
    keyboard = [
        [InlineKeyboardButton("Купить звезды", callback_data="buy_stars")],
        [InlineKeyboardButton("Профиль", callback_data="profile")],
        [InlineKeyboardButton("Рефералы", callback_data="referrals")],
        [InlineKeyboardButton("Техподдержка", callback_data="tech_support")],
        [InlineKeyboardButton("Отзывы", callback_data="reviews")],
        [InlineKeyboardButton("Админ-панель", callback_data="admin_panel")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        get_text("welcome", total_stars_sold=get_setting("total_stars_sold") or 0),
        reply_markup=reply_markup
    )

# Обработка кнопок
async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    username = query.from_user.username or f"user_{user_id}"
    
    await query.answer()
    
    if query.data == "buy_stars":
        stars = int(get_setting("stars_per_purchase"))
        price_usd = float(get_setting("stars_price_usd"))
        ton_exchange_rate = float(get_setting("ton_exchange_rate"))
        amount_ton = (price_usd / ton_exchange_rate)
        
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
            get_text("buy_prompt", amount_ton=amount_ton, stars=stars, address=address, memo=memo)
        )
    
    elif query.data == "profile":
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT stars_bought, ref_bonus_ton FROM users WHERE user_id = %s;", (user_id,))
                result = cur.fetchone()
                stars_bought, ref_bonus_ton = result if result else (0, 0)
        
        await query.message.reply_text(
            f"Профиль @{username}:\nЗвезд куплено: {stars_bought}\nРеф. бонус: {ref_bonus_ton:.6f} TON"
        )
    
    elif query.data == "referrals":
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT ref_bonus_ton, referrals FROM users WHERE user_id = %s;", (user_id,))
                result = cur.fetchone()
                ref_bonus_ton, referrals = result if result else (0, [])
        
        await query.message.reply_text(
            get_text(
                "ref_info",
                ref_bonus_ton=ref_bonus_ton,
                bot_username=context.bot.name.lstrip("@"),
                user_id=user_id
            )
        )
    
    elif query.data == "tech_support":
        support_channel = get_setting("review_channel") or "@sacoectasy"
        await query.message.reply_text(
            get_text("tech_support", support_channel=support_channel)
        )
    
    elif query.data == "reviews":
        review_channel = get_setting("review_channel") or "@sacoectasy"
        await query.message.reply_text(
            get_text("reviews", review_channel=review_channel)
        )
    
    elif query.data == "admin_panel" and is_admin(user_id):
        keyboard = [
            [InlineKeyboardButton("Изменить текст", callback_data="edit_text")],
            [InlineKeyboardButton("Установить цену", callback_data="set_price")],
            [InlineKeyboardButton("Установить проценты", callback_data="set_percent")],
            [InlineKeyboardButton("Установить канал отзывов", callback_data="set_review_channel")],
            [InlineKeyboardButton("Статистика", callback_data="stats")],
            [InlineKeyboardButton("Сбросить выручку", callback_data="reset_profit")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.reply_text("Админ-панель:", reply_markup=reply_markup)
    
    elif query.data == "edit_text" and is_admin(user_id):
        await query.message.reply_text(
            "Введите ключ текста (welcome, buy_prompt, buy_success, ref_info, tech_support, reviews) и новый текст через двоеточие, например:\nwelcome:Новый текст приветствия"
        )
        return EDIT_TEXT
    
    elif query.data == "set_price" and is_admin(user_id):
        await query.message.reply_text(
            "Введите цену за 50 звезд в USD и количество звезд, например:\n0.972:50"
        )
        return SET_PRICE
    
    elif query.data == "set_percent" and is_admin(user_id):
        await query.message.reply_text(
            "Введите реф. бонус (%) и процент прибыли (%), например:\n30:20"
        )
        return SET_PERCENT
    
    elif query.data == "set_review_channel" and is_admin(user_id):
        await query.message.reply_text(
            "Введите Telegram-канал для отзывов, например:\n@MyReviewChannel"
        )
        return SET_REVIEW_CHANNEL
    
    elif query.data == "stats" and is_admin(user_id):
        total_stars_sold = get_setting("total_stars_sold") or 0
        total_profit_usd = get_setting("total_profit_usd") or 0
        total_profit_ton = get_setting("total_profit_ton") or 0
        await query.message.reply_text(
            f"Статистика:\nЗвезд продано: {total_stars_sold}\nПрибыль USD: {total_profit_usd:.2f}\nПрибыль TON: {total_profit_ton:.6f}"
        )
    
    elif query.data == "reset_profit" and is_admin(user_id):
        update_setting("total_profit_usd", 0)
        update_setting("total_profit_ton", 0)
        log_admin_action(user_id, "Сброс выручки")
        await query.message.reply_text("Выручка сброшена.")

# Обработка редактирования текста
async def edit_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Доступ запрещен.")
        return ConversationHandler.END
    
    try:
        key, value = update.message.text.split(":", 1)
        key = key.strip()
        value = value.strip()
        if key not in ("welcome", "buy_prompt", "buy_success", "ref_info", "tech_support", "reviews"):
            await update.message.reply_text("Неверный ключ. Используйте: welcome, buy_prompt, buy_success, ref_info, tech_support, reviews")
            return EDIT_TEXT
        update_text(key, value)
        log_admin_action(update.effective_user.id, f"Изменен текст {key}")
        await update.message.reply_text(f"Текст {key} обновлен.")
    except ValueError:
        await update.message.reply_text("Формат: ключ:новый текст")
    return ConversationHandler.END

# Обработка установки цены
async def set_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Доступ запрещен.")
        return ConversationHandler.END
    
    try:
        price_usd, stars = update.message.text.split(":")
        price_usd = float(price_usd.strip())
        stars = int(stars.strip())
        if price_usd <= 0 or stars <= 0:
            await update.message.reply_text("Цена и количество звезд должны быть положительными.")
            return SET_PRICE
        update_setting("stars_price_usd", price_usd)
        update_setting("stars_per_purchase", stars)
        log_admin_action(update.effective_user.id, f"Установлена цена: {price_usd} USD за {stars} звезд")
        await update.message.reply_text(f"Цена установлена: {price_usd} USD за {stars} звезд.")
    except ValueError:
        await update.message.reply_text("Формат: цена:количество_звезд")
    return ConversationHandler.END

# Обработка установки процентов
async def set_percent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Доступ запрещен.")
        return ConversationHandler.END
    
    try:
        ref_bonus, profit = update.message.text.split(":")
        ref_bonus = float(ref_bonus.strip())
        profit = float(profit.strip())
        if not (0 <= ref_bonus <= 100 and 10 <= profit <= 50):
            await update.message.reply_text("Реф. бонус: 0–100%, прибыль: 10–50%")
            return SET_PERCENT
        update_setting("ref_bonus_percent", ref_bonus)
        update_setting("profit_percent", profit)
        log_admin_action(update.effective_user.id, f"Установлены проценты: реф. бонус {ref_bonus}%, прибыль {profit}%")
        await update.message.reply_text(f"Проценты установлены: реф. бонус {ref_bonus}%, прибыль {profit}%")
    except ValueError:
        await update.message.reply_text("Формат: реф_бонус:прибыль")
    return ConversationHandler.END

# Обработка установки канала отзывов
async def set_review_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Доступ запрещен.")
        return ConversationHandler.END
    
    try:
        channel = update.message.text.strip()
        if not channel.startswith("@"):
            await update.message.reply_text("Канал должен начинаться с @")
            return SET_REVIEW_CHANNEL
        update_setting("review_channel", channel)
        log_admin_action(update.effective_user.id, f"Установлен канал отзывов: {channel}")
        await update.message.reply_text(f"Канал отзывов установлен: {channel}")
    except ValueError:
        await update.message.reply_text("Формат: @ChannelName")
    return ConversationHandler.END

# Отмена ConversationHandler
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Действие отменено.")
    return ConversationHandler.END

# Основная функция
async def main():
    init_db()
    application = Application.builder().token(BOT_TOKEN).build()
    
    conv_handler = ConversationHandler(
    entry_points=[
        CallbackQueryHandler(button, pattern="edit_text$"),
        CallbackQueryHandler(button, pattern="set_price$"),
        CallbackQueryHandler(button, pattern="set_percent$"),
        CallbackQueryHandler(button, pattern="set_review_channel$"),
    ],
    states={
        EDIT_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_text)],
        SET_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_price)],
        SET_PERCENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_percent)],
        SET_REVIEW_CHANNEL: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_review_channel)],
    },
    fallbacks=[CommandHandler("cancel", cancel)],
    per_message=True,
    )
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button))
    application.add_handler(conv_handler)
    
    application.job_queue.run_repeating(payment_checker, interval=60)
    
    await application.run_polling()

if __name__ == "__main__":
    asyncio.run(main())
