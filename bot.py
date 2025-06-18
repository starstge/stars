# Бот для покупки Telegram Stars с оплатой TON
# Требования: реферальная система, админ-панель, статистика, отправка TON на кошелек
# Зависимости: python-telegram-bot, requests, motor, python-dotenv
# Развертывание: Render с MongoDB
# Запуск: python bot.py

import os
from datetime import datetime
import requests
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from motor.motor_asyncio import AsyncIOMotorClient
import logging

# Настройка логирования
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# Загрузка .env
load_dotenv()

# Константы
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = 8028944732  # @sacoectasy
OWNER_WALLET = os.getenv("OWNER_WALLET")
TONAPI_KEY = os.getenv("TONAPI_KEY")
MONGODB_URI = os.getenv("MONGODB_URI")
STARS_PER_PURCHASE = 50
TON_PRICE_USD = 0.972  # Цена за 50 звезд в USD
PROFIT_PERCENT_TON = 20  # Начальная наценка 20%
REF_BONUS_PERCENT = 30  # Реферальный бонус 30%
TON_EXCHANGE_RATE = 8  # Курс: 1 TON = $8 (заглушка)

# Инициализация MongoDB
client = AsyncIOMotorClient(MONGODB_URI)
db = client["stars_bot"]
settings_collection = db["settings"]
users_collection = db["users"]
admin_log_collection = db["admin_log"]

# Инициализация базы данных
async def init_db():
    settings = await settings_collection.find_one({"_id": "config"})
    if not settings:
        await settings_collection.insert_one({
            "_id": "config",
            "admins": [ADMIN_ID],
            "ref_bonus_percent": REF_BONUS_PERCENT,
            "profit_percent": {"ton": PROFIT_PERCENT_TON},
            "total_stars_sold": 0,
            "total_profit_usd": 0,
            "total_profit_ton": 0
        })

# Заглушка для Telegram Wallet (генерация адреса)
def generate_ton_address(user_id, timestamp):
    # TODO: Заменить на реальный вызов Telegram Wallet API
    logger.info(f"Генерация адреса для user_id={user_id}, timestamp={timestamp}")
    return f"EQB_FAKE_ADDRESS_{user_id}_{timestamp}"

# Проверка оплаты через tonapi.io
def check_ton_payment(address, amount_ton, memo):
    headers = {"Authorization": f"Bearer {TONAPI_KEY}"}
    try:
        response = requests.get(f"https://tonapi.io/v2/transactions?address={address}", headers=headers)
        if response.status_code != 200:
            logger.error(f"Ошибка tonapi.io: {response.status_code} {response.text}")
            return False
        transactions = response.json().get("transactions", [])
        for tx in transactions:
            tx_amount = tx.get("amount", 0) / 1e9  # НаноTON
            tx_comment = tx.get("comment", "")
            if tx_amount >= amount_ton and tx_comment == memo:
                logger.info(f"Оплата найдена: {tx_amount} TON, Memo={memo}")
                return True
        return False
    except Exception as e:
        logger.error(f"Ошибка проверки оплаты: {e}")
        return False

# Заглушка для отправки TON
def send_ton_to_owner(amount_ton):
    # TODO: Заменить на реальный перевод через tonapi.io или TON SDK
    logger.info(f"Отправка {amount_ton} TON на кошелек {OWNER_WALLET}")
    return True

# Заглушка для Split.gg
def issue_stars(user_id, username, stars):
    # TODO: Заменить на реальную выдачу звезд через Split.gg
    logger.info(f"Выдача {stars} звезд пользователю @{username} (ID: {user_id})")
    return True

# Логирование действий админа
async def log_admin_action(admin_id, action):
    await admin_log_collection.insert_one({
        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "admin_id": admin_id,
        "action": action
    })

# Главное меню
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or f"User{user_id}"
    settings = await settings_collection.find_one({"_id": "config"})

    # Инициализация пользователя
    user = await users_collection.find_one({"user_id": user_id})
    if not user:
        ref_id = context.args[0][4:] if context.args and context.args[0].startswith("ref_") else None
        if ref_id:
            ref_exists = await users_collection.find_one({"user_id": int(ref_id)})
            ref_id = ref_id if ref_exists else None
        await users_collection.insert_one({
            "user_id": user_id,
            "username": username,
            "stars_bought": 0,
            "ref_bonus_ton": 0,
            "referrals": [],
            "bonus_history": [],
            "referrer_id": ref_id
        })

    keyboard = [
        [
            InlineKeyboardButton("🛒 Купить звезды", callback_data="buy_stars"),
            InlineKeyboardButton("👤 Профиль", callback_data="profile")
        ],
        [
            InlineKeyboardButton("ℹ️ Инфо", callback_data="info"),
            InlineKeyboardButton("🛠 Техподдержка", callback_data="support")
        ],
        [
            InlineKeyboardButton("💬 Отзывы", callback_data="reviews"),
            InlineKeyboardButton("📈 Рефералы", callback_data="referrals")
        ]
    ]
    if user_id in settings["admins"]:
        keyboard.append([InlineKeyboardButton("🔧 Админ-панель", callback_data="admin_panel")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        f"Добро пожаловать! Это бот для покупки Telegram Stars.\n"
        f"Всего продано: {settings['total_stars_sold']} звезд\n"
        f"Выберите действие:",
        reply_markup=reply_markup
    )

# Обработка кнопок
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    settings = await settings_collection.find_one({"_id": "config"})
    user_id = query.from_user.id
    username = query.from_user.username or f"User{user_id}"
    callback_data = query.data

    if callback_data == "buy_stars":
        ton_amount = TON_PRICE_USD / TON_EXCHANGE_RATE
        timestamp = int(datetime.now().timestamp())
        address = generate_ton_address(user_id, timestamp)
        memo = f"order_{user_id}_{timestamp}"
        keyboard = [[InlineKeyboardButton("Назад", callback_data="back_to_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"Оплатите {ton_amount:.4f} TON за {STARS_PER_PURCHASE} звезд:\n"
            f"Адрес: {address}\n"
            f"Memo: {memo}\n"
            f"После оплаты звезды будут выданы автоматически.",
            reply_markup=reply_markup
        )
        context.user_data.setdefault(user_id, {})["pending_payment"] = {
            "address": address,
            "amount_ton": ton_amount,
            "memo": memo,
            "stars": STARS_PER_PURCHASE
        }

    elif callback_data == "profile":
        user = await users_collection.find_one({"user_id": user_id})
        keyboard = [[InlineKeyboardButton("Назад", callback_data="back_to_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"👤 Профиль\n"
            f"Username: @{username}\n"
            f"Куплено звезд: {user['stars_bought']}\n"
            f"Реферальный бонус: {user['ref_bonus_ton']:.4f} TON",
            reply_markup=reply_markup
        )

    elif callback_data == "info":
        keyboard = [[InlineKeyboardButton("Назад", callback_data="back_to_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"ℹ️ Информация\n"
            f"Бот для покупки Telegram Stars.\n"
            f"Цена: ${TON_PRICE_USD} ({TON_PRICE_USD/TON_EXCHANGE_RATE:.4f} TON) за {STARS_PER_PURCHASE} звезд.\n"
            f"Оплата: TON через Telegram Wallet.\n"
            f"Техподдержка: @sacoectasy",
            reply_markup=reply_markup
        )

    elif callback_data == "support":
        keyboard = [[InlineKeyboardButton("Назад", callback_data="back_to_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"🛠 Техподдержка\n"
            f"Свяжитесь с @sacoectasy для помощи.",
            reply_markup=reply_markup
        )

    elif callback_data == "reviews":
        keyboard = [[InlineKeyboardButton("Назад", callback_data="back_to_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"💬 Отзывы\n"
            f"Пока отзывов нет. Напишите @sacoectasy, чтобы оставить отзыв!",
            reply_markup=reply_markup
        )

    elif callback_data == "referrals":
        user = await users_collection.find_one({"user_id": user_id})
        ref_count = len([r for r in user["referrals"] if r["stars_bought"] > 0])
        ref_details = "\n".join(
            [f"- @{r['username']}: {r['stars_bought']} звезд, бонус {r['bonus_ton']:.4f} TON"
             for r in user["referrals"]]
        ) if user["referrals"] else "Нет покупок."
        bonus_history = "\n".join(
            [f"- {h['date']}: @{h['ref_user']} купил {h['stars']} звезд, бонус {h['bonus_ton']:.4f} TON"
             for h in user["bonus_history"]]
        ) if user["bonus_history"] else "Нет начислений."
        keyboard = [
            [InlineKeyboardButton("Ваша реферальная ссылка", switch_inline_query=f"ref_{user_id}")],
            [InlineKeyboardButton("Запросить вывод", callback_data="request_withdrawal")],
            [InlineKeyboardButton("Назад", callback_data="back_to_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"📈 Рефералы\n"
            f"Приглашено: {ref_count} пользователей\n"
            f"Ваш бонус: {user['ref_bonus_ton']:.4f} TON\n"
            f"Детали:\n{ref_details}\n\n"
            f"История бонусов:\n{bonus_history}\n"
            f"Для вывода обратитесь к: @sacoectasy",
            reply_markup=reply_markup
        )

    elif callback_data == "request_withdrawal":
        user = await users_collection.find_one({"user_id": user_id})
        if user["ref_bonus_ton"] > 0:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=f"Пользователь @{username} запрашивает вывод {user['ref_bonus_ton']:.4f} TON (ID: {user_id})"
            )
            await query.edit_message_text(
                "Запрос на вывод отправлен @sacoectasy. Ожидайте ответа!",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data="back_to_menu")]])
            )
        else:
            await query.edit_message_text(
                "У вас нет бонусов для вывода.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data="back_to_menu")]])
            )

    elif callback_data == "admin_panel":
        if user_id not in settings["admins"]:
            return
        keyboard = [
            [InlineKeyboardButton("Изменить % реф. бонусов", callback_data="change_ref_bonus")],
            [InlineKeyboardButton("Изменить % прибыли", callback_data="change_profit")],
            [InlineKeyboardButton("Изменить проданные звезды", callback_data="change_total_stars")],
            [InlineKeyboardButton("Изменить звезды пользователя", callback_data="change_user_stars")],
            [InlineKeyboardButton("Изменить реф. бонусы", callback_data="change_user_rewards")],
            [InlineKeyboardButton("Показать статистику", callback_data="show_stats")],
            [InlineKeyboardButton("Переглянуть лог действий", callback_data="show_log")],
            [InlineKeyboardButton("Назад", callback_data="back_to_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"🔧 Админ-панель\n"
            f"Админы: {len(settings['admins'])}\n"
            f"% реф. бонусов: {settings['ref_bonus_percent']}\n"
            f"% прибыли (TON): {settings['profit_percent']['ton']}\n"
            reply_markup=reply_markup
        )

    elif callback_data == "back_to_menu":
        await start(query, context)

    # Админские функции
    elif callback_data == "change_ref_bonus":
        context.user_data["admin_action"] = "change_ref_bonus"
        await query.edit_message_text(
            "Введите новый % реф. бонусов (0–100):",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Отмена", callback_data="back_to_admin_panel")]])
        )

    elif callback_data == "change_profit":
        context.user_data["admin_action"] = "change_profit"
        await query.edit_message_text(
            "Введите новый % прибыли для TON (10–50):",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Отмена", callback_data="back_to_admin_panel")]])
        )

    elif callback_data == "change_total_stars":
        context.user_data["admin_action"] = "change_total_stars"
        await query.edit_message_text(
            "Введите новое количество проданных звезд:",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Отмена", callback_data="back_to_admin_panel")]])
        )

    elif callback_data == "change_user_stars":
        context.user_data["admin_action"] = "change_user_stars"
        await query.edit_message_text(
            "Введите ID пользователя и количество звезд (например, 123456789 100):",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Отмена", callback_data="back_to_admin_panel")]])
        )

    elif callback_data == "change_user_rewards":
        context.user_data["admin_action"] = "change_user_rewards"
        await query.edit_message_text(
            "Введите ID пользователя и сумму бонусов в TON (например, 123456789 0.1):",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Отмена", callback_data="back_to_admin_panel")]])
        )

    elif callback_data == "show_stats":
        history = []
        async for user in users_collection.find():
            for h in user["bonus_history"]:
                history.append(
                    f"- {h['date']}: @{user['username']} купил {h['stars']} звезд, бонус {h['bonus_ton']:.4f} TON"
                )
        history_text = "\n".join(history) or "Нет начислений."
        keyboard = [[InlineKeyboardButton("Назад", callback_data="back_to_admin_panel")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"📊 Статистика\n"
            f"Продано звезд: {settings['total_stars_sold']}\n"
            f"Прибыль: ${settings['total_profit_usd']:.2f} / {settings['total_profit_ton']:.2f} TON\n"
            f"История бонусов:\n{history_text}",
            reply_markup=reply_markup
        )

    elif callback_data == "show_log":
        logs = []
        async for log in admin_log_collection.find():
            logs.append(f"- {log['date']}: Админ {log['admin_id']} {log['action']}")
        logs_text = "\n".join(logs) or "Нет действий."
        keyboard = [[InlineKeyboardButton("Назад", callback_data="back_to_admin_panel")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"📋 Лог действий\n{logs_text}",
            reply_markup=reply_markup
        )

    elif callback_data == "back_to_admin_panel":
        query.data = "admin_panel"
        await button_handler(update, context)

# Обработка текстовых сообщений для админских изменений
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    settings = await settings_collection.find_one({"_id": "config"})
    if user_id not in settings["admins"]:
        return
    action = context.user_data.get("admin_action")
    if not action:
        return

    text = update.message.text.strip()
    try:
        if action == "change_ref_bonus":
            percent = float(text)
            if 0 <= percent <= 100:
                await settings_collection.update_one(
                    {"_id": "config"},
                    {"$set": {"ref_bonus_percent": percent}}
                )
                await log_admin_action(user_id, f"Изменил % реф. бонусов на {percent}%")
                await update.message.reply_text("Процент реферальных бонусов обновлен!")
            else:
                await update.message.reply_text("Введите значение от 0 до 100%.")

        elif action == "change_profit":
            percent = float(text)
            if 10 <= percent <= 50:
                await settings_collection.update_one(
                    {"_id": "config"},
                    {"$set": {"profit_percent.ton": percent}}
                )
                await log_admin_action(user_id, f"Изменил % прибыли (TON) на {percent}%")
                await update.message.reply_text("Процент прибыли обновлен!")
            else:
                await update.message.reply_text("Введите значение от 10 до 50%.")

        elif action == "change_total_stars":
            stars = int(text)
            if stars >= 0:
                await settings_collection.update_one(
                    {"_id": "config"},
                    {"$set": {"total_stars_sold": stars}}
                )
                await log_admin_action(user_id, f"Изменил общее количество проданных звезд на {stars}")
                await update.message.reply_text("Количество проданных звезд обновлено!")
            else:
                await update.message.reply_text("Введите положительное число.")

        elif action == "change_user_stars":
            user_id_str, stars = text.split()
            stars = int(stars)
            user = await users_collection.find_one({"user_id": int(user_id_str)})
            if user and stars >= 0:
                await users_collection.update_one(
                    {"user_id": int(user_id_str)},
                    {"$set": {"stars_bought": stars}}
                )
                await log_admin_action(user_id, f"Изменил звезды для пользователя {user_id_str} на {stars}")
                await update.message.reply_text("Звезды пользователя обновлены!")
            else:
                await update.message.reply_text("Неверный ID пользователя или количество звезд.")

        elif action == "change_user_rewards":
            user_id_str, ton = text.split()
            ton = float(ton)
            user = await users_collection.find_one({"user_id": int(user_id_str)})
            if user and ton >= 0:
                await users_collection.update_one(
                    {"user_id": int(user_id_str)},
                    {"$set": {"ref_bonus_ton": ton}}
                )
                await log_admin_action(user_id, f"Изменил реф. бонусы для пользователя {user_id_str} на {ton} TON")
                await update.message.reply_text("Бонусы пользователя обновлены!")
            else:
                await update.message.reply_text("Неверный ID пользователя или сумма TON.")

        context.user_data.pop("admin_action", None)
    except ValueError:
        await update.message.reply_text("Неверный формат ввода. Попробуйте снова.")

# Команды для управления админами
async def add_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_ID:
        return
    try:
        new_admin_id = int(context.args[0])
        settings = await settings_collection.find_one({"_id": "config"})
        if new_admin_id not in settings["admins"]:
            await settings_collection.update_one(
                {"_id": "config"},
                {"$push": {"admins": new_admin_id}}
            )
            await log_admin_action(user_id, f"Добавил админа {new_admin_id}")
            await update.message.reply_text(f"Админ {new_admin_id} добавлен.")
        else:
            await update.message.reply_text("Этот пользователь уже админ.")
    except (IndexError, ValueError):
        await update.message.reply_text("Введите ID пользователя: /add_admin <ID>")

async def remove_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_ID:
        return
    try:
        admin_id = int(context.args[0])
        settings = await settings_collection.find_one({"_id": "config"})
        if admin_id in settings["admins"] and admin_id != ADMIN_ID:
            await settings_collection.update_one(
                {"_id": "config"},
                {"$pull": {"admins": admin_id}}
            )
            await log_admin_action(user_id, f"Удалил админа {admin_id}")
            await update.message.reply_text(f"Админ {admin_id} удален.")
        else:
            await update.message.reply_text("Нельзя удалить главного админа или несуществующего админа.")
    except (IndexError, ValueError):
        await update.message.reply_text("Введите ID пользователя: /remove_admin <ID>")

# Проверка ожидающих оплат
async def check_pending_payments(context: ContextTypes.DEFAULT_TYPE):
    async for user in users_collection.find():
        user_id = user["user_id"]
        if user_id in context.user_data and "pending_payment" in context.user_data[user_id]:
            payment = context.user_data[user_id]["pending_payment"]
            if check_ton_payment(payment["address"], payment["amount_ton"], payment["memo"]):
                stars = payment["stars"]
                amount_ton = payment["amount_ton"]
                if send_ton_to_owner(amount_ton):
                    if issue_stars(user_id, user["username"], stars):
                        await users_collection.update_one(
                            {"user_id": user_id},
                            {"$inc": {"stars_bought": stars}}
                        )
                        settings = await settings_collection.find_one({"_id": "config"})
                        profit_usd = (stars / STARS_PER_PURCHASE) * 0.162
                        profit_ton = profit_usd / TON_EXCHANGE_RATE
                        await settings_collection.update_one(
                            {"_id": "config"},
                            {
                                "$inc": {
                                    "total_stars_sold": stars,
                                    "total_profit_usd": profit_usd,
                                    "total_profit_ton": profit_ton
                                }
                            }
                        )
                        if user["referrer_id"]:
                            ref_user = await users_collection.find_one({"user_id": int(user["referrer_id"])})
                            if ref_user:
                                ref_bonus_usd = profit_usd * (settings["ref_bonus_percent"] / 100)
                                ref_bonus_ton = ref_bonus_usd / TON_EXCHANGE_RATE
                                await users_collection.update_one(
                                    {"user_id": int(user["referrer_id"])},
                                    {
                                        "$inc": {"ref_bonus_ton": ref_bonus_ton},
                                        "$push": {
                                            "referrals": {
                                                "user_id": str(user_id),
                                                "username": user["username"],
                                                "stars_bought": stars,
                                                "bonus_ton": ref_bonus_ton
                                            },
                                            "bonus_history": {
                                                "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                                "ref_user": user["username"],
                                                "stars": stars,
                                                "bonus_ton": ref_bonus_ton
                                            }
                                        }
                                    }
                                )
                        await context.bot.send_message(
                            chat_id=user_id,
                            text=f"Оплата подтверждена! Вы получили {stars} звезд."
                        )
                        context.user_data[user_id].pop("pending_payment")
                    else:
                        await context.bot.send_message(
                            chat_id=ADMIN_ID,
                            text=f"Ошибка выдачи звезд для пользователя @{user['username']} (ID: {user_id})"
                        )

# Основная функция
async def main():
    await init_db()
    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("add_admin", add_admin))
    application.add_handler(CommandHandler("remove_admin", remove_admin))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    # Периодическая проверка оплат (каждые 30 секунд)
    application.job_queue.run_repeating(check_pending_payments, interval=30)

    await application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
