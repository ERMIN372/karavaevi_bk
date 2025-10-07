import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import (CallbackQuery, InlineKeyboardButton,
                           InlineKeyboardMarkup, ReplyKeyboardMarkup,
                           ReplyKeyboardRemove)
from aiogram.utils import executor
from dotenv import load_dotenv

from zoneinfo import ZoneInfo

import storage


load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0"))
TECH_CHAT_ID = int(os.getenv("TECH_CHAT_ID", "0"))
TIMEZONE = ZoneInfo(os.getenv("TIMEZONE", "Europe/Moscow"))
ADMINS = {
    int(user_id)
    for user_id in os.getenv("ADMINS", "").split(",")
    if user_id.strip().isdigit()
}
RATE_LIMIT_PER_DAY = storage.RATE_LIMIT_PER_DAY

WEBAPP_HOST = os.getenv("WEBAPP_HOST", "0.0.0.0")
WEBAPP_PORT = int(os.getenv("WEBAPP_PORT", "5000"))
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")

if not BOT_TOKEN:
    raise RuntimeError(
        "BOT_TOKEN is required. Please set it in Replit Secrets.\n"
        "Also required: GOOGLE_SERVICE_ACCOUNT_JSON_BASE64, GOOGLE_SPREADSHEET_ID, "
        "CHANNEL_ID, TECH_CHAT_ID, ADMINS"
    )

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
fsm_storage = MemoryStorage()
dp = Dispatcher(bot, storage=fsm_storage)


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
    note = State()
    confirm = State()


async def ensure_user(ctx: types.User) -> str:
    role = "director" if ctx.id in ADMINS else "worker"
    await storage.gs_ensure_user(
        {
            "id": ctx.id,
            "role": role,
            "username": ctx.username,
            "first_name": ctx.first_name,
            "last_name": ctx.last_name,
        }
    )
    return role


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
    allowed = await storage.guard_rate_limit_gs(user_id)
    if not allowed:
        logging.info("User %s hit rate limit %s", user_id, RATE_LIMIT_PER_DAY)
    return allowed


def validate_timeslot(date_text: str, time_from_text: str, time_to_text: str) -> Optional[str]:
    try:
        date_obj = datetime.strptime(date_text, "%Y-%m-%d").date()
    except ValueError:
        return "Дата должна быть в формате ГГГГ-ММ-ДД."

    today = datetime.now(TIMEZONE).date()
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
    return storage.get_shops()


def render_channel_post(record: Dict[str, Any]) -> str:
    shop_name = record.get("shop_name") or "Любая лавка"
    if record["kind"] == "director":
        title = "🔔 Заявка от директора лавки"
        note = record.get("note") or "Без комментариев"
        return (
            f"{title}\n"
            f"Лавка: {shop_name}\n"
            f"Дата: {record['date']}\n"
            f"Смена: {record['time_from']}–{record['time_to']}\n"
            f"Комментарий: {note}\n"
            "Нажмите «Откликнуться», чтобы связаться с директором."
        )
    title = "💼 Сотрудник ищет смену"
    note = record.get("note") or "Без комментариев"
    return (
        f"{title}\n"
        f"Лавка: {shop_name}\n"
        f"Дата: {record['date']}\n"
        f"Смена: {record['time_from']}–{record['time_to']}\n"
        f"Пожелания: {note}\n"
        "Нажмите «Пригласить», чтобы связаться с сотрудником."
    )


async def on_callback_pick(call: CallbackQuery) -> None:
    try:
        _, request_id_text = call.data.split(":", 1)
        request_id = int(request_id_text)
    except (ValueError, AttributeError):
        await call.answer("Некорректный формат заявки.", show_alert=True)
        return

    try:
        record = await storage.gs_find_request(request_id)
        if not record:
            await call.answer("Заявка не найдена или была удалена.", show_alert=True)
            return

        if str(record.get("status")) != "open":
            await call.answer("Карточка уже закрыта.", show_alert=True)
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

        channel_message_id = call.message.message_id if call.message else None
        await storage.gs_update_request_status(request_id, new_status, channel_message_id)

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
    shop_name = data.get("shop_name")
    shops = await fetch_shops()
    if shop_id is not None:
        if shop_id not in shops:
            await bot.send_message(
                chat_id,
                "Не удалось определить лавку. Попробуйте начать заново.",
            )
            await state.finish()
            return
        shop_name = shops[shop_id]
    elif not shop_name:
        shop_name = "Любая лавка"
    payload = {
        "kind": kind,
        "date": data.get("date"),
        "time_from": data.get("time_from"),
        "time_to": data.get("time_to"),
        "shop_id": shop_id,
        "note": data.get("note"),
        "author_id": author.id,
        "shop_name": shop_name,
        "status": "open",
    }

    allowed = await guard_rate_limit(author.id)
    if not allowed:
        await bot.send_message(chat_id, "Лимит заявок на сегодня достигнут.")
        await state.finish()
        return

    now_iso = datetime.now(timezone.utc).isoformat()
    payload["created_at"] = now_iso
    payload["updated_at"] = now_iso
    request_id, _ = await storage.gs_append_request(payload)
    payload["id"] = request_id
    text = render_channel_post(payload)
    button_text = "Откликнуться" if kind == "director" else "Пригласить"
    markup = InlineKeyboardMarkup().add(
        InlineKeyboardButton(button_text, callback_data=f"pick:{request_id}")
    )
    channel_message = await bot.send_message(CHANNEL_ID, text, reply_markup=markup)
    try:
        await storage.gs_update_request_status(
            request_id, "open", channel_message_id=channel_message.message_id
        )
    except Exception as exc:  # noqa: BLE001
        logging.exception("Failed to update channel message id for request %s", request_id)
        await send_tech(f"Не удалось сохранить ссылку на пост {request_id}: {exc}")
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
        if not shops:
            await message.answer(
                "Список лавок пуст. Обратитесь к администратору для настройки справочника.",
                reply_markup=build_start_keyboard(),
            )
            await state.finish()
            return
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
        await state.update_data(shop_id=shop_id, shop_name=shops[shop_id])
        await call.message.edit_text("Добавьте комментарий (можно телефон). Если не нужно, напишите «Без комментариев».")
        await DirectorStates.next()

    @dispatcher.message_handler(state=DirectorStates.note)
    async def director_note(message: types.Message, state: FSMContext) -> None:
        await state.update_data(note=message.text.strip())
        data = await state.get_data()
        shops = await fetch_shops()
        shop_name = data.get("shop_name") or shops.get(data.get("shop_id"), "Не выбрана")
        summary = (
            "Проверьте заявку:\n"
            f"Дата: {data['date']}\n"
            f"Смена: {data['time_from']}–{data['time_to']}\n"
            f"Лавка: {shop_name}\n"
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
        keyboard.add(InlineKeyboardButton("Любая лавка", callback_data="worker_shop:any"))
        await message.answer("Выберите желаемую лавку:", reply_markup=keyboard)

    @dispatcher.callback_query_handler(lambda c: c.data.startswith("worker_shop:"), state=WorkerStates.time_to)
    async def worker_shop_choice(call: CallbackQuery, state: FSMContext) -> None:
        _, raw_id = call.data.split(":", 1)
        if raw_id == "any":
            await call.answer()
            await state.update_data(shop_id=None, shop_name="Любая лавка")
        else:
            try:
                shop_id = int(raw_id)
            except ValueError:
                await call.answer("Такой лавки нет.", show_alert=True)
                return
            shops = await fetch_shops()
            if shop_id not in shops:
                await call.answer("Такой лавки нет.", show_alert=True)
                return
            await call.answer()
            await state.update_data(shop_id=shop_id, shop_name=shops[shop_id])
        await call.message.edit_text("Расскажите, на какую роль готовы выйти и оставьте комментарий (можно телефон).")
        await WorkerStates.next()

    @dispatcher.message_handler(state=WorkerStates.note)
    async def worker_note(message: types.Message, state: FSMContext) -> None:
        await state.update_data(note=message.text.strip())
        data = await state.get_data()
        shops = await fetch_shops()
        shop_name = data.get("shop_name") or shops.get(data.get("shop_id"), "Любая лавка")
        summary = (
            "Проверьте заявку:\n"
            f"Дата: {data['date']}\n"
            f"Смена: {data['time_from']}–{data['time_to']}\n"
            f"Лавка: {shop_name}\n"
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
    shops = storage.get_shops()
    logging.info("Бот запущен и готов к работе. Доступно лавок: %s", len(shops))
    if WEBHOOK_URL:
        webhook_url = WEBHOOK_URL + WEBHOOK_PATH
        await bot.set_webhook(webhook_url, drop_pending_updates=True)
        logging.info("Webhook установлен на %s", webhook_url)
        info = await bot.get_webhook_info()
        logging.info(
            "Webhook состояние: pending=%s, ip=%s, last_error_date=%s, last_error_message=%s",
            info.pending_update_count,
            info.ip_address,
            info.last_error_date,
            info.last_error_message,
        )
    else:
        logging.warning("WEBHOOK_URL не установлен, используется polling режим")


async def on_shutdown(_: Dispatcher) -> None:
    if WEBHOOK_URL:
        await bot.delete_webhook()
        logging.info("Webhook удален")


def register_handlers() -> None:
    run_director_flow(dp)
    run_worker_flow(dp)
    dp.register_callback_query_handler(on_callback_pick, lambda c: c.data and c.data.startswith("pick:"))


async def _handle_health(_: web.Request) -> web.Response:
    """Простая проверка доступности вебхука."""
    try:
        info = await bot.get_webhook_info()
        payload: Dict[str, Any] = {
            "status": "ok",
            "webhook_url": info.url,
            "has_custom_certificate": info.has_custom_certificate,
            "pending_update_count": info.pending_update_count,
            "ip_address": info.ip_address,
            "last_error_date": info.last_error_date,
            "last_error_message": info.last_error_message,
            "max_connections": info.max_connections,
        }
    except Exception as exc:  # noqa: BLE001
        logging.exception("Не удалось получить состояние вебхука")
        payload = {"status": "error", "detail": str(exc)}
    return web.json_response(payload)


async def _handle_root(_: web.Request) -> web.Response:
    logging.info("Получен запрос к debug root")
    return web.json_response({
        "status": "running",
        "timestamp": datetime.now(TIMEZONE).isoformat(),
        "webhook_path": WEBHOOK_PATH,
    })


def build_web_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", _handle_root)
    app.router.add_get("/healthz", _handle_health)
    return app


def main() -> None:
    register_handlers()
    if WEBHOOK_URL:
        app = build_web_app()
        executor.start_webhook(
            dispatcher=dp,
            webhook_path=WEBHOOK_PATH,
            skip_updates=True,
            on_startup=on_startup,
            on_shutdown=on_shutdown,
            host=WEBAPP_HOST,
            port=WEBAPP_PORT,
            web_app=app,
        )
    else:
        logging.info("Запуск в polling режиме (для разработки)")
        executor.start_polling(dp, skip_updates=True, on_startup=on_startup)


if __name__ == "__main__":
    main()


# TODO: Модерировать посты перед каналом.
# TODO: Авто-закрытие open-карточек по истечении смены.
# TODO: Экспорт отчетов в CSV.
