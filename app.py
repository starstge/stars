import logging
import os
import pytz
from datetime import datetime, timedelta
from flask import Flask, render_template, request, redirect, url_for, flash, session
import asyncpg
from asyncpg.pool import Pool
import bcrypt

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "your-secret-key")  # Set in .env
POSTGRES_URL = os.getenv("POSTGRES_URL")
ADMIN_PASSWORD_HASH = os.getenv("ADMIN_PASSWORD_HASH").encode('utf-8')  # Bcrypt hash

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database pool
db_pool = None

async def ensure_db_pool():
    """Инициализация или повторная инициализация пула соединений с базой данных."""
    global db_pool
    try:
        if db_pool is None or db_pool._closed:
            db_pool = await asyncpg.create_pool(
                POSTGRES_URL,
                min_size=1,
                max_size=10,
                max_inactive_connection_lifetime=300
            )
            logger.info("Пул соединений с базой данных инициализирован")
        return db_pool
    except Exception as e:
        logger.error(f"Ошибка инициализации пула базы данных: {e}", exc_info=True)
        raise

@app.route("/login", methods=["GET", "POST"])
async def login():
    """Обработка входа администратора."""
    if request.method == "POST":
        user_id = request.form.get("user_id")
        password = request.form.get("password")
        logger.debug(f"Попытка входа: user_id={user_id}, password=***")
        try:
            user_id = int(user_id)
            async with (await ensure_db_pool()) as conn:
                is_admin = await conn.fetchval("SELECT is_admin FROM users WHERE user_id = $1", user_id)
                logger.debug(f"Статус администратора для user_id={user_id}: is_admin={is_admin}")
                if is_admin and bcrypt.checkpw(password.encode('utf-8'), ADMIN_PASSWORD_HASH):
                    session["user_id"] = user_id
                    session["is_admin"] = True
                    logger.info(f"Успешный вход: user_id={user_id}")
                    flash("Вход выполнен успешно!", "success")
                    return redirect(url_for("transactions"))
                else:
                    logger.warning(f"Неудачный вход: user_id={user_id}, неверный пароль или не администратор")
                    flash("Неверный ID пользователя или пароль.", "error")
        except ValueError:
            logger.error(f"Ошибка: ID пользователя должен быть числом, получено: {user_id}")
            flash("ID пользователя должен быть числом.", "error")
        except Exception as e:
            logger.error(f"Ошибка при входе: {e}", exc_info=True)
            flash(f"Ошибка при входе: {str(e)}", "error")
    return render_template("login.html")

@app.route("/logout")
def logout():
    """Обработка выхода администратора."""
    session.pop("user_id", None)
    session.pop("is_admin", None)
    flash("Вы вышли из системы.", "success")
    logger.info("Пользователь вышел из системы")
    return redirect(url_for("login"))

@app.route("/", methods=["GET", "POST"])
async def transactions():
    """Отображение транзакций с поиском и фильтрацией."""
    if not session.get("is_admin"):
        logger.warning("Попытка доступа к транзакциям без авторизации")
        return redirect(url_for("login"))

    user_id = request.args.get("user_id", "")
    date_from = request.args.get("date_from", "")
    date_to = request.args.get("date_to", "")
    stars_min = request.args.get("stars_min", "")
    stars_max = request.args.get("stars_max", "")
    recipient = request.args.get("recipient", "")
    page = int(request.args.get("page", 1))
    per_page = 10

    query = "SELECT id, user_id, recipient_username, stars_amount, price_ton, purchase_time FROM transactions WHERE 1=1"
    params = []
    param_count = 1

    if user_id:
        try:
            query += f" AND user_id = ${param_count}"
            params.append(int(user_id))
            param_count += 1
        except ValueError:
            flash("ID пользователя должен быть числом.", "error")
            logger.error(f"Неверный формат user_id: {user_id}")

    if date_from:
        try:
            query += f" AND purchase_time >= ${param_count}"
            params.append(datetime.strptime(date_from, "%Y-%m-%d"))
            param_count += 1
        except ValueError:
            flash("Неверный формат начальной даты (гггг-мм-дд).", "error")
            logger.error(f"Неверный формат date_from: {date_from}")

    if date_to:
        try:
            query += f" AND purchase_time <= ${param_count}"
            params.append(datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1))
            param_count += 1
        except ValueError:
            flash("Неверный формат конечной даты (гггг-мм-дд).", "error")
            logger.error(f"Неверный формат date_to: {date_to}")

    if stars_min:
        try:
            query += f" AND stars_amount >= ${param_count}"
            params.append(int(stars_min))
            param_count += 1
        except ValueError:
            flash("Минимальное количество звезд должно быть числом.", "error")
            logger.error(f"Неверный формат stars_min: {stars_min}")

    if stars_max:
        try:
            query += f" AND stars_amount <= ${param_count}"
            params.append(int(stars_max))
            param_count += 1
        except ValueError:
            flash("Максимальное количество звезд должно быть числом.", "error")
            logger.error(f"Неверный формат stars_max: {stars_max}")

    if recipient:
        query += f" AND recipient_username ILIKE ${param_count}"
        params.append(f"%{recipient}%")
        param_count += 1

    query += " ORDER BY purchase_time DESC"
    query += f" LIMIT ${param_count} OFFSET ${param_count + 1}"
    params.extend([per_page, (page - 1) * per_page])

    try:
        async with (await ensure_db_pool()) as conn:
            transactions = await conn.fetch(query, *params)
            total = await conn.fetchval("SELECT COUNT(*) FROM transactions WHERE 1=1" + query.split("WHERE 1=1")[1].split("ORDER BY")[0], *params[:-2])
            total_pages = (total + per_page - 1) // per_page

            # Конвертация purchase_time в EEST
            eest = pytz.timezone("Europe/Tallinn")
            transactions = [
                {
                    "id": t["id"],
                    "user_id": t["user_id"],
                    "recipient_username": t["recipient_username"],
                    "stars_amount": t["stars_amount"],
                    "price_ton": t["price_ton"],
                    "purchase_time": t["purchase_time"].astimezone(eest).strftime("%Y-%m-%d %H:%M:%S EEST")
                }
                for t in transactions
            ]

        logger.info(f"Отображено {len(transactions)} транзакций, страница {page} из {total_pages}")
        return render_template(
            "transactions.html",
            transactions=transactions,
            page=page,
            total_pages=total_pages,
            user_id=user_id,
            date_from=date_from,
            date_to=date_to,
            stars_min=stars_min,
            stars_max=stars_max,
            recipient=recipient
        )
    except Exception as e:
        logger.error(f"Ошибка при загрузке транзакций: {e}", exc_info=True)
        flash(f"Ошибка при загрузке транзакций: {str(e)}", "error")
        return render_template("transactions.html", transactions=[], page=1, total_pages=1)

if __name__ == "__main__":
    import asyncio
    app.run(debug=True)
