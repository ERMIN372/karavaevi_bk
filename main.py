import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import aiosqlite
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import (CallbackQuery, InlineKeyboardButton,
                           InlineKeyboardMarkup, ReplyKeyboardMarkup,
                           ReplyKeyboardRemove)
from aiogram.utils import executor
from dotenv import load_dotenv


load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0"))
TECH_CHAT_ID = int(os.getenv("TECH_CHAT_ID", "0"))
ADMINS = {
    int(user_id)
    for user_id in os.getenv("ADMINS", "").split(",")
    if user_id.strip().isdigit()
}

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

DB_PATH = os.getenv("DB_PATH", "bot.db")


class DirectorStates(StatesGroup):
    date = State()
    time_from = State()
    time_to = State()
    note = State()
    confirm = State()


class WorkerStates(StatesGroup):
    date = State()
    time_from = State()
    time_to = State()
    shop = State()
    note = State()
    confirm = State()


async def init_db(conn: aiosqlite.Connection) -> None:
    await conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            role TEXT NOT NULL,
            phone TEXT,
            shop TEXT
        );
        CREATE TABLE IF NOT EXISTS shops (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind TEXT NOT NULL,
            date TEXT NOT NULL,
            time_from TEXT NOT NULL,
            time_to TEXT NOT NULL,
            shop_id INTEGER NOT NULL,
            note TEXT,
            author_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(shop_id) REFERENCES shops(id),
            FOREIGN KEY(author_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind TEXT NOT NULL,
            payload TEXT,
            created_at TEXT NOT NULL
        );
        """
    )
    await conn.commit()

    async with conn.execute("SELECT COUNT(*) FROM shops") as cursor:
        row = await cursor.fetchone()
        if row and row[0] == 0:
            await conn.executemany(
                "INSERT INTO shops(name) VALUES (?)",
                [(f"Лавка №{i}",) for i in range(1, 11)],
            )
            await conn.commit()
            logging.info("Создан справочник лавок")


async def ensure_user(ctx: types.User) -> None:
    role = "director" if ctx.id in ADMINS else "worker"
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            "INSERT INTO users(id, role) VALUES(?, ?) ON CONFLICT(id) DO UPDATE SET role=excluded.role",
            (ctx.id, role),
        )
        await conn.commit()


def build_start_keyboard() -> ReplyKeyboardMarkup:
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add("Я директор лавки")
    keyboard.add("Я сотрудник лавки")
    return keyboard


async def start_menu(message: types.Message) -> None:
    text = "👋 Привет! Это бот подработок сети «Братья Караваевы»."
    text += "\nВыберите нужный режим:"
    await message.answer(text, reply_markup=build_start_keyboard())


def format_mention(entity: Any) -> str:
    full_name = (
        getattr(entity, "full_name", None)
        or getattr(entity, "title", None)
        or getattr(entity, "username", None)
    )
    if not full_name:
        first_name = getattr(entity, "first_name", None)
        last_name = getattr(entity, "last_name", None)
        full_name = " ".join(filter(None, [first_name, last_name])) or "Пользователь"
    return f'<a href="tg://user?id={entity.id}">{full_name}</a>'


async def guard_rate_limit(user_id: int) -> bool:
    threshold = datetime.utcnow() - timedelta(hours=24)
    async with aiosqlite.connect(DB_PATH) as conn:
        async with conn.execute(
            """
            SELECT COUNT(*) FROM requests
            WHERE author_id = ?
              AND created_at >= ?
              AND status IN ('open', 'assigned', 'picked')
            """,
            (user_id, threshold.isoformat()),
        ) as cursor:
            row = await cursor.fetchone()
            return (row or (0,))[0] < 3


def validate_timeslot(date_text: str, time_from_text: str, time_to_text: str) -> Optional[str]:
    try:
        date_obj = datetime.strptime(date_text, "%Y-%m-%d").date()
    except ValueError:
        return "Дата должна быть в формате ГГГГ-ММ-ДД."

    today = datetime.utcnow().date()
    if date_obj < today:
        return "Дата не может быть в прошлом."

    try:
        from_parts = datetime.strptime(time_from_text, "%H:%M").time()
        to_parts = datetime.strptime(time_to_text, "%H:%M").time()
    except ValueError:
        return "Время укажите в формате ЧЧ:ММ."

    if from_parts >= to_parts:
        return "Время начала должно быть раньше окончания."

    for check_time in (from_parts, to_parts):
        if check_time.minute % 15 != 0:
            return "Используйте шаг 15 минут."

    delta = datetime.combine(date_obj, to_parts) - datetime.combine(date_obj, from_parts)
    if delta > timedelta(hours=12):
        return "Длительность не может превышать 12 часов."

    return None


async def send_tech(message: str) -> None:
    if TECH_CHAT_ID == 0:
        logging.error("TECH_CHAT_ID не настроен: %s", message)
        return
    try:
        await bot.send_message(TECH_CHAT_ID, message)
    except Exception as exc:  # noqa: BLE001
        logging.exception("Не удалось отправить сообщение в тех-чат: %s", exc)


async def fetch_shops() -> Dict[int, str]:
    async with aiosqlite.connect(DB_PATH) as conn:
        async with conn.execute("SELECT id, name FROM shops ORDER BY id") as cursor:
            return {row[0]: row[1] for row in await cursor.fetchall()}


def render_channel_post(record: Dict[str, Any]) -> str:
    if record["kind"] == "director":
        title = "🔔 Заявка от директора лавки"
        note = record.get("note") or "Без комментариев"
        return (
            f"{title}\n"
            f"Лавка: {record['shop_name']}\n"
            f"Дата: {record['date']}\n"
            f"Смена: {record['time_from']}–{record['time_to']}\n"
            f"Комментарий: {note}\n"
            "Нажмите «Откликнуться», чтобы связаться с директором."
        )
    title = "💼 Сотрудник ищет смену"
    note = record.get("note") or "Без комментариев"
    return (
        f"{title}\n"
        f"Лавка: {record['shop_name']}\n"
        f"Дата: {record['date']}\n"
        f"Смена: {record['time_from']}–{record['time_to']}\n"
        f"Пожелания: {note}\n"
        "Нажмите «Пригласить», чтобы связаться с сотрудником."
    )


async def record_event(kind: str, payload: str) -> None:
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            "INSERT INTO events(kind, payload, created_at) VALUES(?, ?, ?)",
            (kind, payload, datetime.utcnow().isoformat()),
        )
        await conn.commit()


async def create_request(data: Dict[str, Any]) -> int:
    async with aiosqlite.connect(DB_PATH) as conn:
        cursor = await conn.execute(
            """
            INSERT INTO requests(kind, date, time_from, time_to, shop_id, note, author_id, status, created_at)
            VALUES(?, ?, ?, ?, ?, ?, ?, 'open', ?)
            """,
            (
                data["kind"],
                data["date"],
                data["time_from"],
                data["time_to"],
                data["shop_id"],
                data.get("note"),
                data["author_id"],
                datetime.utcnow().isoformat(),
            ),
        )
        await conn.commit()
        request_id = cursor.lastrowid
    await record_event("request_created", f"{{'id': {request_id}, 'kind': '{data['kind']}'}}")
    return request_id


async def fetch_request(request_id: int) -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as conn:
        async with conn.execute(
            """
            SELECT r.id, r.kind, r.date, r.time_from, r.time_to, r.shop_id, r.note,
                   r.author_id, r.status, s.name as shop_name
            FROM requests r
            JOIN shops s ON s.id = r.shop_id
            WHERE r.id = ?
            """,
            (request_id,),
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            columns = [
                "id",
                "kind",
                "date",
                "time_from",
                "time_to",
                "shop_id",
                "note",
                "author_id",
                "status",
                "shop_name",
            ]
            return dict(zip(columns, row))


async def update_request_status(request_id: int, status: str) -> None:
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            "UPDATE requests SET status = ? WHERE id = ?",
            (status, request_id),
        )
        await conn.commit()


async def on_callback_pick(call: CallbackQuery) -> None:
    try:
        _, request_id_text = call.data.split(":", 1)
        request_id = int(request_id_text)
    except (ValueError, AttributeError):
        await call.answer("Некорректный формат заявки.", show_alert=True)
        return

    try:
        record = await fetch_request(request_id)
        if not record:
            await call.answer("Заявка не найдена или была удалена.", show_alert=True)
            return

        if record["status"] != "open":
            await call.answer("Эта заявка уже закрыта.", show_alert=True)
            return

        picker = call.from_user
        if record["author_id"] == picker.id:
            await call.answer("Нельзя откликаться на собственную заявку.", show_alert=True)
            return

        author_chat = await bot.get_chat(record["author_id"])
        author_mention = format_mention(author_chat)
        picker_mention = format_mention(picker)

        if record["kind"] == "director":
            new_status = "assigned"
            director_message = (
                "✅ Сотрудник откликнулся на вашу заявку!\n"
                f"Контакт: {picker_mention}"
            )
            worker_message = (
                "🎉 Вы откликнулись на смену!\n"
                f"Свяжитесь с директором: {author_mention}"
            )
        else:
            new_status = "picked"
            director_message = (
                "🎯 Вы пригласили сотрудника на смену!\n"
                f"Контакт: {picker_mention}"
            )
            worker_message = (
                "✅ Директор пригласил вас на смену!\n"
                f"Свяжитесь с директором: {author_mention}"
            )

        await update_request_status(request_id, new_status)
        await record_event("request_status", f"{{'id': {request_id}, 'status': '{new_status}'}}")

        try:
            await bot.send_message(
                record["author_id"], director_message, disable_web_page_preview=True
            )
        except Exception as exc:  # noqa: BLE001
            logging.exception("Не удалось уведомить автора заявки %s", record["author_id"])
            await send_tech(
                f"Не удалось уведомить автора заявки {record['author_id']}: {exc}"
            )

        try:
            await bot.send_message(
                picker.id, worker_message, disable_web_page_preview=True
            )
        except Exception as exc:  # noqa: BLE001
            logging.exception("Не удалось уведомить участника %s", picker.id)
            await send_tech(f"Не удалось уведомить пользователя {picker.id}: {exc}")

        logging.info("Заявка %s изменила статус на %s", request_id, new_status)
        await call.answer("Контакты отправлены в личные сообщения.")
    except Exception as exc:  # noqa: BLE001
        logging.exception("Ошибка при обработке отклика на заявку %s", call.data)
        await call.answer("Что-то пошло не так. Мы уже разбираемся.", show_alert=True)
        await send_tech(f"Ошибка при обработке отклика: {exc}")


async def handle_post_publication(
    chat_id: int,
    author: types.User,
    state: FSMContext,
    kind: str,
) -> None:
    data = await state.get_data()
    shop_id = data.get("shop_id")
    shops = await fetch_shops()
    if shop_id not in shops:
        await bot.send_message(chat_id, "Не удалось определить лавку. Попробуйте начать заново.")
        await state.finish()
        return
    shop_name = shops[shop_id]
    payload = {
        "kind": kind,
        "date": data.get("date"),
        "time_from": data.get("time_from"),
        "time_to": data.get("time_to"),
        "shop_id": shop_id,
        "note": data.get("note"),
        "author_id": author.id,
        "shop_name": shop_name,
    }

    allowed = await guard_rate_limit(author.id)
    if not allowed:
        await bot.send_message(chat_id, "Лимит заявок на сегодня достигнут.")
        await state.finish()
        return

    request_id = await create_request(payload)
    payload["id"] = request_id
    text = render_channel_post(payload)
    button_text = "Откликнуться" if kind == "director" else "Пригласить"
    markup = InlineKeyboardMarkup().add(
        InlineKeyboardButton(button_text, callback_data=f"pick:{request_id}")
    )
    await bot.send_message(CHANNEL_ID, text, reply_markup=markup)
    await bot.send_message(
        chat_id,
        "Готово! Заявка опубликована в канале.",
        reply_markup=build_start_keyboard(),
    )
    logging.info(
        "Пользователь %s опубликовал заявку %s типа %s",
        author.id,
        request_id,
        kind,
    )
    await state.finish()


def run_director_flow(dispatcher: Dispatcher) -> None:
    @dispatcher.message_handler(lambda m: m.text == "Я директор лавки")
    async def director_entry(message: types.Message) -> None:
        await ensure_user(message.from_user)
        if message.from_user.id not in ADMINS:
            await message.answer(
                "Доступ только для директоров. Если считаете, что это ошибка — обратитесь в поддержку."
            )
            return
        await message.answer(
            "Укажите дату смены в формате ГГГГ-ММ-ДД:",
            reply_markup=ReplyKeyboardRemove(),
        )
        await DirectorStates.date.set()

    @dispatcher.message_handler(state=DirectorStates.date)
    async def director_date(message: types.Message, state: FSMContext) -> None:
        await state.update_data(date=message.text.strip())
        await message.answer("Укажите время начала смены (ЧЧ:ММ):")
        await DirectorStates.next()

    @dispatcher.message_handler(state=DirectorStates.time_from)
    async def director_time_from(message: types.Message, state: FSMContext) -> None:
        await state.update_data(time_from=message.text.strip())
        await message.answer("Укажите время окончания смены (ЧЧ:ММ):")
        await DirectorStates.next()

    @dispatcher.message_handler(state=DirectorStates.time_to)
    async def director_time_to(message: types.Message, state: FSMContext) -> None:
        await state.update_data(time_to=message.text.strip())
        data = await state.get_data()
        error = validate_timeslot(data.get("date", ""), data.get("time_from", ""), data.get("time_to", ""))
        if error:
            await message.answer(error + " Попробуйте снова. Укажите дату в формате ГГГГ-ММ-ДД.")
            await DirectorStates.date.set()
            return
        shops = await fetch_shops()
        keyboard = InlineKeyboardMarkup(row_width=2)
        for shop_id, shop_name in shops.items():
            keyboard.insert(
                InlineKeyboardButton(shop_name, callback_data=f"director_shop:{shop_id}")
            )
        await message.answer("Выберите лавку:", reply_markup=keyboard)

    @dispatcher.callback_query_handler(lambda c: c.data.startswith("director_shop:"), state=DirectorStates.time_to)
    async def director_shop_choice(call: CallbackQuery, state: FSMContext) -> None:
        shop_id = int(call.data.split(":", 1)[1])
        shops = await fetch_shops()
        if shop_id not in shops:
            await call.answer("Такой лавки нет.", show_alert=True)
            return
        await call.answer()
        await state.update_data(shop_id=shop_id)
        await call.message.edit_text("Добавьте комментарий (можно телефон). Если не нужно, напишите «Без комментариев».")
        await DirectorStates.next()

    @dispatcher.message_handler(state=DirectorStates.note)
    async def director_note(message: types.Message, state: FSMContext) -> None:
        await state.update_data(note=message.text.strip())
        data = await state.get_data()
        shops = await fetch_shops()
        summary = (
            "Проверьте заявку:\n"
            f"Дата: {data['date']}\n"
            f"Смена: {data['time_from']}–{data['time_to']}\n"
            f"Лавка: {shops.get(data['shop_id'], 'Не выбрана')}\n"
            f"Комментарий: {data['note']}"
        )
        keyboard = InlineKeyboardMarkup().add(
            InlineKeyboardButton("Опубликовать", callback_data="director_confirm"),
            InlineKeyboardButton("Отмена", callback_data="director_cancel"),
        )
        await message.answer(summary, reply_markup=keyboard)
        await DirectorStates.confirm.set()

    @dispatcher.callback_query_handler(lambda c: c.data == "director_cancel", state=DirectorStates.confirm)
    async def director_cancel(call: CallbackQuery, state: FSMContext) -> None:
        await call.answer("Заявка отменена")
        await state.finish()
        await call.message.edit_text("Заявка отменена. Возвращайтесь, когда будете готовы.")

    @dispatcher.callback_query_handler(lambda c: c.data == "director_confirm", state=DirectorStates.confirm)
    async def director_confirm(call: CallbackQuery, state: FSMContext) -> None:
        await call.answer()
        await call.message.edit_text("Публикуем заявку...")
        await handle_post_publication(call.message.chat.id, call.from_user, state, "director")


def run_worker_flow(dispatcher: Dispatcher) -> None:
    @dispatcher.message_handler(lambda m: m.text == "Я сотрудник лавки")
    async def worker_entry(message: types.Message) -> None:
        await ensure_user(message.from_user)
        await message.answer(
            "Укажите дату, когда готовы выйти, в формате ГГГГ-ММ-ДД:",
            reply_markup=ReplyKeyboardRemove(),
        )
        await WorkerStates.date.set()

    @dispatcher.message_handler(state=WorkerStates.date)
    async def worker_date(message: types.Message, state: FSMContext) -> None:
        await state.update_data(date=message.text.strip())
        await message.answer("Укажите время начала смены (ЧЧ:ММ):")
        await WorkerStates.next()

    @dispatcher.message_handler(state=WorkerStates.time_from)
    async def worker_time_from(message: types.Message, state: FSMContext) -> None:
        await state.update_data(time_from=message.text.strip())
        await message.answer("Укажите время окончания смены (ЧЧ:ММ):")
        await WorkerStates.next()

    @dispatcher.message_handler(state=WorkerStates.time_to)
    async def worker_time_to(message: types.Message, state: FSMContext) -> None:
        await state.update_data(time_to=message.text.strip())
        data = await state.get_data()
        error = validate_timeslot(data.get("date", ""), data.get("time_from", ""), data.get("time_to", ""))
        if error:
            await message.answer(error + " Попробуйте снова. Укажите дату в формате ГГГГ-ММ-ДД.")
            await WorkerStates.date.set()
            return
        shops = await fetch_shops()
        keyboard = InlineKeyboardMarkup(row_width=2)
        for shop_id, shop_name in shops.items():
            keyboard.insert(InlineKeyboardButton(shop_name, callback_data=f"worker_shop:{shop_id}"))
        await message.answer("Выберите желаемую лавку:", reply_markup=keyboard)

    @dispatcher.callback_query_handler(lambda c: c.data.startswith("worker_shop:"), state=WorkerStates.time_to)
    async def worker_shop_choice(call: CallbackQuery, state: FSMContext) -> None:
        shop_id = int(call.data.split(":", 1)[1])
        shops = await fetch_shops()
        if shop_id not in shops:
            await call.answer("Такой лавки нет.", show_alert=True)
            return
        await call.answer()
        await state.update_data(shop_id=shop_id)
        await call.message.edit_text("Расскажите, на какую роль готовы выйти и оставьте комментарий (можно телефон).")
        await WorkerStates.next()

    @dispatcher.message_handler(state=WorkerStates.note)
    async def worker_note(message: types.Message, state: FSMContext) -> None:
        await state.update_data(note=message.text.strip())
        data = await state.get_data()
        shops = await fetch_shops()
        summary = (
            "Проверьте заявку:\n"
            f"Дата: {data['date']}\n"
            f"Смена: {data['time_from']}–{data['time_to']}\n"
            f"Лавка: {shops.get(data['shop_id'], 'Не выбрана')}\n"
            f"Пожелания: {data['note']}"
        )
        keyboard = InlineKeyboardMarkup().add(
            InlineKeyboardButton("Опубликовать", callback_data="worker_confirm"),
            InlineKeyboardButton("Отмена", callback_data="worker_cancel"),
        )
        await message.answer(summary, reply_markup=keyboard)
        await WorkerStates.confirm.set()

    @dispatcher.callback_query_handler(lambda c: c.data == "worker_cancel", state=WorkerStates.confirm)
    async def worker_cancel(call: CallbackQuery, state: FSMContext) -> None:
        await call.answer("Заявка отменена")
        await state.finish()
        await call.message.edit_text("Заявка отменена. Возвращайтесь, когда будете готовы.")

    @dispatcher.callback_query_handler(lambda c: c.data == "worker_confirm", state=WorkerStates.confirm)
    async def worker_confirm(call: CallbackQuery, state: FSMContext) -> None:
        await call.answer()
        await call.message.edit_text("Публикуем заявку...")
        await handle_post_publication(call.message.chat.id, call.from_user, state, "worker")


@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message) -> None:
    await ensure_user(message.from_user)
    await start_menu(message)


@dp.errors_handler()
async def on_error(update: types.Update, error: Exception) -> bool:
    logging.exception("Ошибка при обработке апдейта: %s", error)
    await send_tech(f"Ошибка: {error}")
    return True


async def on_startup(_: Dispatcher) -> None:
    async with aiosqlite.connect(DB_PATH) as conn:
        await init_db(conn)
    logging.info("Бот запущен и готов к работе")


def register_handlers() -> None:
    run_director_flow(dp)
    run_worker_flow(dp)
    dp.register_callback_query_handler(on_callback_pick, lambda c: c.data and c.data.startswith("pick:"))


def main() -> None:
    register_handlers()
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)


if __name__ == "__main__":
    main()


# TODO: Модерировать посты перед каналом.
# TODO: Авто-закрытие open-карточек по истечении смены.
# TODO: Экспорт отчетов в CSV.
