import asyncio
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import (CallbackQuery, ContentType,
                           InlineKeyboardButton, InlineKeyboardMarkup,
                           KeyboardButton, ReplyKeyboardMarkup,
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
DATE_WINDOW_DAYS = max(0, int(os.getenv("DATE_WINDOW_DAYS", "90")))
ADMINS = {
    int(user_id)
    for user_id in os.getenv("ADMINS", "").split(",")
    if user_id.strip().isdigit()
}

WEBAPP_HOST = os.getenv("WEBAPP_HOST", "0.0.0.0")
WEBAPP_PORT = int(os.getenv("WEBAPP_PORT", "5000"))
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")


def now_in_timezone() -> datetime:
    """Return the current datetime converted to the configured timezone."""

    return datetime.now(timezone.utc).astimezone(TIMEZONE)

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
    time_range = State()
    shop = State()
    note = State()
    confirm = State()


class WorkerStates(StatesGroup):
    metro = State()
    metro_search = State()
    shop = State()
    date = State()
    time_range = State()
    note = State()
    confirm = State()


class RegistrationStates(StatesGroup):
    waiting_contact = State()


DATE_PLACEHOLDER = "например: 09.10 или “завтра”"
DATE_PROMPT_MESSAGE = "Выбери дату или введи вручную: 09.10, “завтра”, “суббота”."
DATE_PARSE_ERROR_MESSAGE = "Не понял дату. Введи как 09.10 или нажми кнопку ниже."
DATE_CONFIRMATION_TEMPLATE = "Дата: {date_human} (ISO: {date_iso})"
DATE_BUTTON_TODAY = "Сегодня"
DATE_BUTTON_TOMORROW = "Завтра"
BACK_COMMAND = "Назад"
TIME_PROMPT_MESSAGE = (
    "Время смены. Формат 09:00–18:00. Шаг 15 минут.\n"
    "Примеры: 09:00–13:30, 12:15–16:45."
)
TIME_PLACEHOLDER = "например: 09:00–13:30"
TIME_CONFIRMATION_TEMPLATE = "Время смены: {time_from}–{time_to}"
INLINE_DATE_DAYS = 10

DIRECTOR_BUTTON_TEXT = "🧑‍💼 Директор лавки"
WORKER_BUTTON_TEXT = "👨‍🍳 Хочу подработать"

METRO_PROMPT_MESSAGE = (
    "Выбери метро поблизости или напиши название.\n"
    "Можно: «Проспект Мира», «ВДНХ», «Китай-город»."
)
METRO_SEARCH_PROMPT = (
    "Напиши название станции метро.\n"
    "Можно: «Проспект Мира», «ВДНХ», «Китай-город»."
)
METRO_NO_SHOPS_TEMPLATE = "Рядом с «{station}» лавок не нашли. Попробуй другую станцию."
METRO_SHOPS_TITLE_TEMPLATE = "Лавки у «{station}». Выбери место:"

METRO_SEARCH_BUTTON_TEXT = "🔎 Поиск по названию"
METRO_BACK_BUTTON_TEXT = "⬅️ Назад"
METRO_ALL_BUTTON_TEXT = "📋 Все станции"
SHOP_BACK_BUTTON_TEXT = "⬅️ К метро"
SHOP_RESET_BUTTON_TEXT = "🔁 Сбросить выбор"

STATIONS_PER_PAGE = 10
SHOPS_PER_PAGE = 10
MAX_SEARCH_RESULTS = 50

SHOPS_REFRESH_INTERVAL_SECONDS = 15 * 60
shops_refresh_task: Optional[asyncio.Task] = None

WEEKDAY_SHORT_LABELS = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
WEEKDAY_FULL_NAMES = [
    "Понедельник",
    "Вторник",
    "Среда",
    "Четверг",
    "Пятница",
    "Суббота",
    "Воскресенье",
]
MONTH_GENITIVE = {
    1: "января",
    2: "февраля",
    3: "марта",
    4: "апреля",
    5: "мая",
    6: "июня",
    7: "июля",
    8: "августа",
    9: "сентября",
    10: "октября",
    11: "ноября",
    12: "декабря",
}

NATURAL_DAY_OFFSETS = {
    "сегодня": 0,
    "segodnya": 0,
    "завтра": 1,
    "zavtra": 1,
    "послезавтра": 2,
    "poslezavtra": 2,
}

WEEKDAY_ALIASES = {
    "пн": 0,
    "пон": 0,
    "понедельник": 0,
    "вт": 1,
    "вторник": 1,
    "ср": 2,
    "среда": 2,
    "чт": 3,
    "четверг": 3,
    "пт": 4,
    "пятница": 4,
    "сб": 5,
    "суб": 5,
    "суббота": 5,
    "вс": 6,
    "воск": 6,
    "воскресенье": 6,
}


def build_date_reply_keyboard() -> ReplyKeyboardMarkup:
    keyboard = ReplyKeyboardMarkup(
        resize_keyboard=True,
        input_field_placeholder=DATE_PLACEHOLDER,
    )
    keyboard.row(DATE_BUTTON_TODAY, DATE_BUTTON_TOMORROW)
    return keyboard


def build_back_keyboard() -> ReplyKeyboardMarkup:
    keyboard = ReplyKeyboardMarkup(
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder=TIME_PLACEHOLDER,
    )
    keyboard.add(BACK_COMMAND)
    return keyboard


def build_inline_date_keyboard(base_date: date) -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup(row_width=3)
    for offset in range(INLINE_DATE_DAYS):
        candidate = base_date + timedelta(days=offset)
        if candidate > base_date + timedelta(days=DATE_WINDOW_DAYS):
            break
        label = f"{WEEKDAY_SHORT_LABELS[candidate.weekday()]} {candidate.day:02d}"
        markup.insert(
            InlineKeyboardButton(
                label,
                callback_data=f"pick_date:{candidate.isoformat()}",
            )
        )
    return markup


def format_human_date(value: date) -> str:
    weekday_name = WEEKDAY_FULL_NAMES[value.weekday()]
    month_name = MONTH_GENITIVE[value.month]
    return f"{weekday_name}, {value.day:02d} {month_name} {value.year}"


def _normalize_text(value: str) -> str:
    text = value.strip().lower()
    text = text.replace("\u2013", "-")
    text = text.replace("\u2014", "-")
    text = text.replace("\u2012", "-")
    text = text.replace("\u2010", "-")
    text = text.replace(",", ".")
    text = text.replace("\xa0", " ")
    text = text.replace("ё", "е")
    text = text.replace("“", "")
    text = text.replace("”", "")
    text = text.replace('"', "")
    text = text.replace("'", "")
    text = re.sub(r"\s+", "", text)
    return text


def parse_user_date_input(raw_value: str, *, today: date, max_days: int) -> date:
    if not raw_value:
        raise ValueError("empty")
    normalized = _normalize_text(raw_value)
    if not normalized:
        raise ValueError("empty")

    try:
        candidate = date.fromisoformat(normalized)
    except ValueError:
        candidate = None

    if candidate is None:
        if normalized in NATURAL_DAY_OFFSETS:
            candidate = today + timedelta(days=NATURAL_DAY_OFFSETS[normalized])
        elif normalized in WEEKDAY_ALIASES:
            target_weekday = WEEKDAY_ALIASES[normalized]
            days_ahead = (target_weekday - today.weekday()) % 7
            if days_ahead == 0:
                days_ahead = 7
            candidate = today + timedelta(days=days_ahead)
        else:
            parts = tuple(normalized.split(".")) if "." in normalized else ()
            candidate = _parse_numeric_date(parts, today)

    if candidate is None:
        raise ValueError("unparsed")

    if candidate < today:
        raise ValueError("past")

    if candidate > today + timedelta(days=max_days):
        raise ValueError("too_far")

    return candidate


def _parse_numeric_date(parts: Tuple[str, ...], today: date) -> Optional[date]:
    if not parts:
        return None
    if len(parts) == 2 and all(part.isdigit() for part in parts):
        day = int(parts[0])
        month = int(parts[1])
        try:
            candidate = date(today.year, month, day)
        except ValueError:
            return None
        if candidate < today:
            try:
                candidate = date(today.year + 1, month, day)
            except ValueError:
                return None
        return candidate
    if (
        len(parts) == 3
        and all(part.isdigit() for part in parts)
        and len(parts[2]) in (2, 4)
    ):
        day = int(parts[0])
        month = int(parts[1])
        year = int(parts[2])
        if year < 100:
            year += 2000 if year < 70 else 1900
        try:
            return date(year, month, day)
        except ValueError:
            return None
    return None


TIME_RANGE_PATTERN = re.compile(
    r"^(\d{1,2})(?::?(\d{0,2}))?-(\d{1,2})(?::?(\d{0,2}))?$"
)


def _normalize_station_search_text(value: str) -> str:
    text = value.strip().lower()
    text = text.replace("ё", "е")
    text = re.sub(r"[\s\-–—_]", "", text)
    text = text.replace("«", "").replace("»", "")
    text = text.replace("(", "").replace(")", "")
    return text


def _compute_page_bounds(length: int, page: int, per_page: int) -> Tuple[int, int, int, int]:
    total_pages = max(1, (length + per_page - 1) // per_page) if length else 1
    page = max(0, min(page, total_pages - 1))
    start = page * per_page
    end = min(start + per_page, length)
    return page, start, end, total_pages


def build_metro_keyboard(
    stations: List[str], page: int, mode: str
) -> Tuple[InlineKeyboardMarkup, int, int]:
    page, start, end, total_pages = _compute_page_bounds(len(stations), page, STATIONS_PER_PAGE)
    markup = InlineKeyboardMarkup(row_width=1)
    for index in range(start, end):
        station = stations[index]
        markup.add(InlineKeyboardButton(station, callback_data=f"wm_pick:{index}"))
    nav_buttons: List[InlineKeyboardButton] = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"wm_page:{page - 1}"))
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"wm_page:{page + 1}"))
    if nav_buttons:
        markup.row(*nav_buttons)
    markup.row(InlineKeyboardButton(METRO_SEARCH_BUTTON_TEXT, callback_data="wm_search"))
    if mode == "search":
        markup.row(InlineKeyboardButton(METRO_ALL_BUTTON_TEXT, callback_data="wm_all"))
    markup.row(InlineKeyboardButton(METRO_BACK_BUTTON_TEXT, callback_data="wm_back"))
    return markup, page, total_pages


def build_shop_keyboard(
    shops: List[Dict[str, Any]], page: int
) -> Tuple[InlineKeyboardMarkup, int, int]:
    page, start, end, total_pages = _compute_page_bounds(len(shops), page, SHOPS_PER_PAGE)
    markup = InlineKeyboardMarkup(row_width=1)
    for index in range(start, end):
        entry = shops[index]
        button_text = f"🏪 {entry['name']} · {entry['distance']} м"
        markup.add(InlineKeyboardButton(button_text, callback_data=f"wshop_pick:{index}"))
    nav_buttons: List[InlineKeyboardButton] = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"wshop_page:{page - 1}"))
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"wshop_page:{page + 1}"))
    if nav_buttons:
        markup.row(*nav_buttons)
    markup.row(InlineKeyboardButton(SHOP_BACK_BUTTON_TEXT, callback_data="wshop_back"))
    markup.row(InlineKeyboardButton(SHOP_RESET_BUTTON_TEXT, callback_data="wshop_reset"))
    return markup, page, total_pages


def parse_time_range(raw_value: str) -> Optional[Tuple[str, str]]:
    if not raw_value:
        return None
    normalized = raw_value.strip().lower()
    normalized = normalized.replace(" ", "")
    normalized = normalized.replace("—", "-")
    normalized = normalized.replace("–", "-")
    normalized = normalized.replace("−", "-")
    normalized = normalized.replace("..", ".")
    normalized = normalized.replace(",", ":")
    normalized = normalized.replace(".", ":")
    match = TIME_RANGE_PATTERN.match(normalized)
    if not match:
        return None
    start_hour, start_minute, end_hour, end_minute = match.groups()
    start = _normalize_time_component(start_hour, start_minute)
    end = _normalize_time_component(end_hour, end_minute)
    if not start or not end:
        return None
    return start, end


def _normalize_time_component(hour_text: str, minute_text: Optional[str]) -> Optional[str]:
    hour = int(hour_text)
    if not 0 <= hour <= 23:
        return None
    if minute_text:
        if len(minute_text) == 1:
            minute = int(minute_text) * 10
        else:
            minute = int(minute_text)
    else:
        minute = 0
    if not 0 <= minute < 60:
        return None
    return f"{hour:02d}:{minute:02d}"


def resolve_flow(state_name: Optional[str]) -> Optional[str]:
    if not state_name:
        return None
    if state_name.startswith(f"{DirectorStates.__name__}:"):
        return "director"
    if state_name.startswith(f"{WorkerStates.__name__}:"):
        return "worker"
    return None


async def start_date_step(message: types.Message, state: FSMContext, flow: str) -> None:
    state_cls = DirectorStates if flow == "director" else WorkerStates
    await state.set_state(state_cls.date.state)
    await state.set_data({})
    await message.answer(
        DATE_PROMPT_MESSAGE,
        reply_markup=build_date_reply_keyboard(),
    )
    await send_inline_date_choices(message)


async def send_inline_date_choices(message: types.Message) -> None:
    today_local = now_in_timezone().date()
    markup = build_inline_date_keyboard(today_local)
    if not markup.inline_keyboard:
        await message.answer("Нет доступных дат в заданном окне.")
        return
    await message.answer("Выберите дату из списка ниже:", reply_markup=markup)


async def prompt_time_range(message: types.Message, state: FSMContext, flow: str) -> None:
    state_cls = DirectorStates if flow == "director" else WorkerStates
    await state.set_state(state_cls.time_range.state)
    await message.answer(TIME_PROMPT_MESSAGE, reply_markup=build_back_keyboard())


async def apply_date_selection(
    message: types.Message, state: FSMContext, flow: str, selected_date: date
) -> None:
    date_iso = selected_date.isoformat()
    date_human = format_human_date(selected_date)
    await state.update_data(date=date_iso, date_human=date_human)
    await message.answer(
        DATE_CONFIRMATION_TEMPLATE.format(date_human=date_human, date_iso=date_iso),
        reply_markup=ReplyKeyboardRemove(),
    )
    await prompt_time_range(message, state, flow)


async def process_date_message(message: types.Message, state: FSMContext) -> None:
    state_name = await state.get_state()
    flow = resolve_flow(state_name)
    if not flow:
        logging.error(
            "Не удалось определить поток для обработки даты. state=%s user=%s",
            state_name,
            message.from_user.id if message.from_user else "unknown",
        )
        await message.answer(
            DATE_PROMPT_MESSAGE,
            reply_markup=build_date_reply_keyboard(),
        )
        await send_inline_date_choices(message)
        return
    today_local = now_in_timezone().date()
    try:
        parsed_date = parse_user_date_input(
            message.text or "",
            today=today_local,
            max_days=DATE_WINDOW_DAYS,
        )
    except ValueError as exc:
        logging.info(
            "Не удалось распознать дату '%s' от пользователя %s: %s",
            message.text,
            message.from_user.id if message.from_user else "unknown",
            exc,
        )
        await message.answer(DATE_PARSE_ERROR_MESSAGE)
        await message.answer(
            DATE_PROMPT_MESSAGE,
            reply_markup=build_date_reply_keyboard(),
        )
        return
    await apply_date_selection(message, state, flow, parsed_date)


async def process_date_callback(call: CallbackQuery, state: FSMContext, iso_value: str) -> None:
    state_name = await state.get_state()
    flow = resolve_flow(state_name)
    if not flow:
        logging.error(
            "Не удалось определить поток для callback-даты. state=%s user=%s",
            state_name,
            call.from_user.id if call.from_user else "unknown",
        )
        await call.answer("Ошибка состояния", show_alert=True)
        return
    today_local = now_in_timezone().date()
    try:
        parsed_date = parse_user_date_input(
            iso_value,
            today=today_local,
            max_days=DATE_WINDOW_DAYS,
        )
    except ValueError as exc:
        logging.info(
            "Не удалось распознать дату '%s' из callback от пользователя %s: %s",
            iso_value,
            call.from_user.id if call.from_user else "unknown",
            exc,
        )
        await call.answer(DATE_PARSE_ERROR_MESSAGE, show_alert=True)
        return
    await call.answer()
    try:
        await call.message.edit_reply_markup()
    except Exception:  # noqa: BLE001
        logging.debug("Не удалось скрыть inline-клавиатуру после выбора даты")
    await apply_date_selection(call.message, state, flow, parsed_date)


async def handle_back_to_date(message: types.Message, state: FSMContext) -> None:
    state_name = await state.get_state()
    flow = resolve_flow(state_name)
    if not flow:
        await message.answer("Возвращаюсь в начало меню.")
        await start_menu(message)
        return
    existing_data = await state.get_data()
    await start_date_step(message, state, flow)
    preserved_keys = {
        key: existing_data[key]
        for key in ("shop_id", "shop_name", "chosen_metro", "chosen_metro_dist_m", "note")
        if key in existing_data
    }
    if preserved_keys:
        await state.update_data(**preserved_keys)


async def on_pick_date_selection(call: CallbackQuery, state: FSMContext) -> None:
    try:
        _, iso_value = call.data.split(":", 1)
    except (AttributeError, ValueError):
        await call.answer("Некорректная дата", show_alert=True)
        return
    await process_date_callback(call, state, iso_value)


async def ensure_user(ctx: types.User, phone_number: Optional[str] = None) -> str:
    role = "director" if ctx.id in ADMINS else "worker"
    username = (ctx.username or "").lstrip("@")
    await storage.gs_ensure_user(
        {
            "id": ctx.id,
            "role": role,
            "username": username,
            "phone_number": phone_number,
            "first_name": ctx.first_name,
            "last_name": ctx.last_name,
        }
    )
    return role


def build_start_keyboard() -> ReplyKeyboardMarkup:
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add(DIRECTOR_BUTTON_TEXT)
    keyboard.add(WORKER_BUTTON_TEXT)
    return keyboard


def build_contact_keyboard() -> ReplyKeyboardMarkup:
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    keyboard.add(KeyboardButton("Отправить контакт ☎️", request_contact=True))
    return keyboard


async def ensure_contact_exists(message: types.Message) -> bool:
    user_record = await storage.gs_get_user(message.from_user.id)
    raw_phone = (user_record or {}).get("phone_number") if user_record else ""
    if isinstance(raw_phone, str):
        phone_number = raw_phone.strip()
    elif raw_phone:
        phone_number = str(raw_phone).strip()
    else:
        phone_number = ""
    await ensure_user(message.from_user, phone_number=phone_number or None)
    if phone_number:
        return True
    await message.answer(
        "Для регистрации поделитесь, пожалуйста, своим контактом.",
        reply_markup=build_contact_keyboard(),
    )
    await RegistrationStates.waiting_contact.set()
    return False


async def start_menu(message: types.Message) -> None:
    text = (
        "👋 Привет! Это бот подработок сети «Братья Караваевы».\n"
        "Здесь можно:\n"
        "— 👨‍🍳 Оставить заявку, если ты хочешь подработать в другой лавке.\n"
        "— 🧑‍💼 Найти сотрудника на смену, если ты директор лавки.\n\n"
        "Выбери, кто ты:"
    )
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


def format_contact_details(
    user_data: Optional[Dict[str, Any]], entity: Any
) -> str:
    phone_number = (user_data or {}).get("phone_number", "").strip()
    username = (user_data or {}).get("username") or getattr(entity, "username", None)
    contact_parts = []
    if phone_number:
        contact_parts.append(phone_number)
    if username:
        username = username.lstrip("@")
        contact_parts.append(f"@{username}")
    if contact_parts:
        return " ".join(contact_parts)
    return format_mention(entity)


def validate_timeslot(date_text: str, time_from_text: str, time_to_text: str) -> Optional[str]:
    try:
        date_obj = datetime.strptime(date_text, "%Y-%m-%d").date()
    except ValueError:
        return "Дата должна быть в формате ГГГГ-ММ-ДД."

    now_local = now_in_timezone()
    today = now_local.date()
    if date_obj < today:
        return "Дата не может быть в прошлом."

    try:
        from_parts = datetime.strptime(time_from_text, "%H:%M").time()
        to_parts = datetime.strptime(time_to_text, "%H:%M").time()
    except ValueError:
        return "Время укажите в формате ЧЧ:ММ."

    if from_parts >= to_parts:
        return "Время начала должно быть раньше окончания."

    if date_obj == today and from_parts < now_local.time():
        return "Время начала не может быть в прошлом."

    for check_time in (from_parts, to_parts):
        if check_time.minute % 15 != 0:
            return "Используйте шаг 15 минут."

    delta = datetime.combine(date_obj, to_parts) - datetime.combine(date_obj, from_parts)
    if delta < timedelta(hours=1):
        return "Интервал должен быть не менее 1 часа."
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


async def fetch_shops() -> Dict[int, storage.ShopRecord]:
    return storage.get_shops()


async def periodic_shops_refresh() -> None:
    while True:
        try:
            await asyncio.sleep(SHOPS_REFRESH_INTERVAL_SECONDS)
            await storage.refresh_shops_cache()
            shops = storage.get_shops()
            logging.info(
                "Кеш лавок обновлён автоматически. Доступно лавок: %s", len(shops)
            )
        except asyncio.CancelledError:
            break
        except Exception as exc:  # noqa: BLE001
            logging.exception("Не удалось автоматически обновить справочник лавок: %s", exc)


def render_channel_post(record: Dict[str, Any]) -> str:
    shop_name = record.get("shop_name") or "Любая лавка"
    if record["kind"] == "director":
        title = "🔔 Заявка на подработку от директора лавки"
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
    station = record.get("chosen_metro") or ""
    distance = record.get("chosen_metro_dist_m")
    lines = [
        title,
        f"Лавка: {shop_name}",
    ]
    if station:
        distance_text = f"{distance} м" if distance is not None else ""
        if distance_text:
            lines.append(f"Метро: {station} · {distance_text}")
        else:
            lines.append(f"Метро: {station}")
    lines.extend(
        [
            f"Дата: {record['date']}",
            f"Смена: {record['time_from']}–{record['time_to']}",
            f"Пожелания: {note}",
            "Нажмите «Пригласить», чтобы связаться с сотрудником.",
        ]
    )
    return "\n".join(lines)


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

        status = str(record.get("status") or "").strip().lower()
        if status != "open":
            await call.answer("Карточка уже закрыта. Спасибо, что откликнулись", show_alert=True)
            return

        picker = call.from_user
        if record["author_id"] == picker.id:
            await call.answer("Нельзя откликаться на собственную заявку.", show_alert=True)
            return

        author_chat = await bot.get_chat(record["author_id"])
        picker_user_data = await storage.gs_get_user(picker.id)
        author_user_data = await storage.gs_get_user(record["author_id"])
        picker_contact = format_contact_details(picker_user_data, picker)
        author_contact = format_contact_details(author_user_data, author_chat)

        if record["kind"] == "director":
            new_status = "assigned"
            message_for_author = (
                "✅ Сотрудник откликнулся на вашу заявку!\n"
                f"Контакт: {picker_contact}"
            )
            message_for_picker = (
                "🎉 Вы откликнулись на смену!\n"
                f"Свяжитесь с директором: {author_contact}"
            )
        else:
            new_status = "picked"
            message_for_author = (
                "✅ Директор пригласил вас на смену!\n"
                f"Свяжитесь с директором: {picker_contact}"
            )
            message_for_picker = (
                "🎉 Вы пригласили сотрудника на смену!\n"
                f"Контакт: {author_contact}"
            )

        channel_message_id = call.message.message_id if call.message else None
        await storage.gs_update_request_status(request_id, new_status, channel_message_id)

        try:
            await bot.send_message(
                record["author_id"], message_for_author, disable_web_page_preview=True
            )
        except Exception as exc:  # noqa: BLE001
            logging.exception("Не удалось уведомить автора заявки %s", record["author_id"])
            await send_tech(
                f"Не удалось уведомить автора заявки {record['author_id']}: {exc}"
            )

        try:
            await bot.send_message(
                picker.id, message_for_picker, disable_web_page_preview=True
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
        shop_name = shops[shop_id].name
    elif not shop_name:
        shop_name = "Любая лавка"
    payload = {
        "kind": kind,
        "date": data.get("date"),
        "time_from": data.get("time_from"),
        "time_to": data.get("time_to"),
        "shop_id": shop_id,
        "chosen_metro": data.get("chosen_metro"),
        "chosen_metro_dist_m": data.get("chosen_metro_dist_m"),
        "note": data.get("note"),
        "author_id": author.id,
        "shop_name": shop_name,
        "status": "open",
    }

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
        "Готово! Заявка опубликована в канале: @karavaevi_bk.",
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
    @dispatcher.message_handler(lambda m: m.text == DIRECTOR_BUTTON_TEXT)
    async def director_entry(message: types.Message, state: FSMContext) -> None:
        if not await ensure_contact_exists(message):
            return
        await state.finish()
        await start_date_step(message, state, "director")

    @dispatcher.message_handler(state=DirectorStates.date)
    async def director_date(message: types.Message, state: FSMContext) -> None:
        await process_date_message(message, state)

    @dispatcher.message_handler(state=DirectorStates.time_range)
    async def director_time_range(message: types.Message, state: FSMContext) -> None:
        if (message.text or "").strip().lower() == BACK_COMMAND.lower():
            await handle_back_to_date(message, state)
            return
        parsed_range = parse_time_range(message.text or "")
        if not parsed_range:
            await message.answer(
                "Не понял время.\n" + TIME_PROMPT_MESSAGE,
                reply_markup=build_back_keyboard(),
            )
            return
        time_from, time_to = parsed_range
        data = await state.get_data()
        error = validate_timeslot(
            data.get("date", ""),
            time_from,
            time_to,
        )
        if error:
            await message.answer(
                f"{error}\n{TIME_PROMPT_MESSAGE}",
                reply_markup=build_back_keyboard(),
            )
            return
        await state.update_data(time_from=time_from, time_to=time_to)
        await message.answer(
            TIME_CONFIRMATION_TEMPLATE.format(time_from=time_from, time_to=time_to),
            reply_markup=ReplyKeyboardRemove(),
        )
        shops = await fetch_shops()
        if not shops:
            await message.answer(
                "Список лавок пуст. Обратитесь к администратору для настройки справочника.",
                reply_markup=build_start_keyboard(),
            )
            await state.finish()
            return
        keyboard = InlineKeyboardMarkup(row_width=2)
        sorted_shops = sorted(shops.values(), key=lambda record: record.name.lower())
        for shop in sorted_shops:
            keyboard.insert(
                InlineKeyboardButton(shop.name, callback_data=f"director_shop:{shop.id}")
            )
        await state.set_state(DirectorStates.shop.state)
        await message.answer("Выберите вашу лавку:", reply_markup=keyboard)

    @dispatcher.callback_query_handler(
        lambda c: c.data.startswith("director_shop:"), state=DirectorStates.shop
    )
    async def director_shop_choice(call: CallbackQuery, state: FSMContext) -> None:
        shop_id = int(call.data.split(":", 1)[1])
        shops = await fetch_shops()
        if shop_id not in shops:
            await call.answer("Такой лавки нет.", show_alert=True)
            return
        await call.answer()
        await state.update_data(shop_id=shop_id, shop_name=shops[shop_id].name)
        await call.message.edit_text("Добавьте комментарий(например: какая должность вам требуется в лавку). Если не нужно, напишите «Без комментариев».")
        await DirectorStates.note.set()

    @dispatcher.message_handler(state=DirectorStates.note)
    async def director_note(message: types.Message, state: FSMContext) -> None:
        if (message.text or "").strip().lower() == BACK_COMMAND.lower():
            await handle_back_to_date(message, state)
            return
        await state.update_data(note=message.text.strip())
        data = await state.get_data()
        shops = await fetch_shops()
        selected_shop = shops.get(data.get("shop_id")) if data.get("shop_id") is not None else None
        shop_name = data.get("shop_name") or (selected_shop.name if selected_shop else "Не выбрана")
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
        await start_menu(call.message)

    @dispatcher.callback_query_handler(lambda c: c.data == "director_confirm", state=DirectorStates.confirm)
    async def director_confirm(call: CallbackQuery, state: FSMContext) -> None:
        await call.answer()
        await call.message.edit_text("Публикуем заявку...")
        await handle_post_publication(call.message.chat.id, call.from_user, state, "director")


def run_worker_flow(dispatcher: Dispatcher) -> None:
    async def set_metro_context(
        state: FSMContext,
        *,
        mode: str,
        stations: List[str],
        page: int = 0,
        query: str = "",
    ) -> Dict[str, Any]:
        context = {
            "mode": mode,
            "stations": list(stations),
            "page": page,
            "query": query,
        }
        await state.update_data(worker_metro=context)
        return context

    async def get_metro_context(state: FSMContext) -> Dict[str, Any]:
        data = await state.get_data()
        context = data.get("worker_metro")
        if context and isinstance(context.get("stations"), list) and context["stations"]:
            return context
        stations = list(storage.get_station_names())
        if not stations:
            return {}
        return await set_metro_context(state, mode="list", stations=stations, page=0)

    async def present_metro_menu(
        target_message: types.Message,
        state: FSMContext,
        context: Dict[str, Any],
        *,
        via_edit: bool,
    ) -> None:
        stations = context.get("stations") or []
        if not stations:
            await state.finish()
            await target_message.answer(
                "Список станций пуст. Обратитесь к администратору.",
                reply_markup=build_start_keyboard(),
            )
            return
        markup, actual_page, _ = build_metro_keyboard(
            stations, context.get("page", 0), context.get("mode", "list")
        )
        context["page"] = actual_page
        await state.update_data(worker_metro=context)
        if context.get("mode") == "search" and context.get("query"):
            text = f"Результаты поиска по «{context['query']}».\n{METRO_PROMPT_MESSAGE}"
        else:
            text = METRO_PROMPT_MESSAGE
        if via_edit:
            try:
                await target_message.edit_text(text, reply_markup=markup)
                return
            except Exception:  # noqa: BLE001
                logging.debug("Не удалось обновить список метро, отправляем новое сообщение")
        await target_message.answer(text, reply_markup=markup)

    async def set_shop_context(
        state: FSMContext, station: str, shops: List[Dict[str, Any]], page: int = 0
    ) -> Dict[str, Any]:
        context = {
            "station": station,
            "shops": shops,
            "page": page,
        }
        await state.update_data(worker_shop=context)
        return context

    async def get_shop_context(state: FSMContext) -> Dict[str, Any]:
        data = await state.get_data()
        context = data.get("worker_shop")
        return context or {}

    async def present_shop_menu(
        target_message: types.Message,
        state: FSMContext,
        context: Dict[str, Any],
        *,
        via_edit: bool,
    ) -> None:
        shops_list = context.get("shops") or []
        station = context.get("station", "")
        if not shops_list:
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton(SHOP_BACK_BUTTON_TEXT, callback_data="wshop_back"))
            text = METRO_NO_SHOPS_TEMPLATE.format(station=station) if station else METRO_PROMPT_MESSAGE
            if via_edit:
                try:
                    await target_message.edit_text(text, reply_markup=markup)
                    return
                except Exception:  # noqa: BLE001
                    logging.debug("Не удалось показать сообщение о пустом списке лавок")
            await target_message.answer(text, reply_markup=markup)
            return
        markup, actual_page, _ = build_shop_keyboard(shops_list, context.get("page", 0))
        context["page"] = actual_page
        await state.update_data(worker_shop=context)
        text = METRO_SHOPS_TITLE_TEMPLATE.format(station=station)
        if via_edit:
            try:
                await target_message.edit_text(text, reply_markup=markup)
                return
            except Exception:  # noqa: BLE001
                logging.debug("Не удалось обновить список лавок, отправляем новое сообщение")
        await target_message.answer(text, reply_markup=markup)

    @dispatcher.message_handler(lambda m: m.text == WORKER_BUTTON_TEXT)
    async def worker_entry(message: types.Message, state: FSMContext) -> None:
        if not await ensure_contact_exists(message):
            return
        await state.finish()
        stations = list(storage.get_station_names())
        if not stations:
            await message.answer(
                "Список станций пуст. Обратитесь к администратору.",
                reply_markup=build_start_keyboard(),
            )
            return
        await state.set_state(WorkerStates.metro.state)
        context = await set_metro_context(state, mode="list", stations=stations, page=0)
        await present_metro_menu(message, state, context, via_edit=False)

    @dispatcher.callback_query_handler(
        lambda c: c.data and c.data.startswith("wm_"),
        state=[WorkerStates.metro, WorkerStates.metro_search],
    )
    async def worker_metro_callbacks(call: CallbackQuery, state: FSMContext) -> None:
        data = call.data or ""
        if data.startswith("wm_pick:"):
            context = await get_metro_context(state)
            stations = context.get("stations") or []
            try:
                index = int(data.split(":", 1)[1])
            except ValueError:
                await call.answer("Некорректный выбор", show_alert=True)
                return
            if index < 0 or index >= len(stations):
                await call.answer("Станция не найдена", show_alert=True)
                return
            station = stations[index]
            logging.info("Пользователь %s выбрал метро «%s»", call.from_user.id, station)
            await call.answer()
            locations = storage.get_station_shops(station)
            if not locations:
                await state.set_state(WorkerStates.metro.state)
                context["page"] = context.get("page", 0)
                await state.update_data(worker_metro=context)
                empty_context = {"station": station, "shops": [], "page": 0}
                await present_shop_menu(call.message, state, empty_context, via_edit=True)
                return
            shops = [
                {"id": location.shop_id, "name": location.shop_name, "distance": location.distance_m}
                for location in locations
            ]
            await set_shop_context(state, station, shops, page=0)
            await state.update_data(chosen_metro=station, chosen_metro_dist_m=None)
            await state.set_state(WorkerStates.shop.state)
            await present_shop_menu(call.message, state, await get_shop_context(state), via_edit=True)
            return
        if data.startswith("wm_page:"):
            context = await get_metro_context(state)
            if not context:
                await call.answer("Станции недоступны", show_alert=True)
                return
            try:
                requested_page = int(data.split(":", 1)[1])
            except ValueError:
                await call.answer("Некорректная страница", show_alert=True)
                return
            context["page"] = requested_page
            await state.update_data(worker_metro=context)
            await call.answer()
            await present_metro_menu(call.message, state, context, via_edit=True)
            return
        if data == "wm_search":
            await call.answer()
            await state.set_state(WorkerStates.metro_search.state)
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton(METRO_ALL_BUTTON_TEXT, callback_data="wm_all"))
            markup.add(InlineKeyboardButton(METRO_BACK_BUTTON_TEXT, callback_data="wm_back"))
            try:
                await call.message.edit_text(METRO_SEARCH_PROMPT, reply_markup=markup)
            except Exception:  # noqa: BLE001
                await call.message.answer(METRO_SEARCH_PROMPT, reply_markup=markup)
            return
        if data == "wm_all":
            await call.answer()
            stations = list(storage.get_station_names())
            if not stations:
                await state.finish()
                await call.message.edit_text(
                    "Список станций пуст. Обратитесь к администратору.",
                    reply_markup=None,
                )
                await start_menu(call.message)
                return
            await state.set_state(WorkerStates.metro.state)
            context = await set_metro_context(state, mode="list", stations=stations, page=0)
            await present_metro_menu(call.message, state, context, via_edit=True)
            return
        if data == "wm_back":
            await call.answer()
            await state.finish()
            try:
                await call.message.edit_reply_markup()
            except Exception:  # noqa: BLE001
                pass
            await start_menu(call.message)
            return
        await call.answer("Действие недоступно", show_alert=True)

    @dispatcher.message_handler(state=WorkerStates.metro_search)
    async def worker_metro_search_input(message: types.Message, state: FSMContext) -> None:
        query = (message.text or "").strip()
        if not query:
            await message.answer("Введи название станции метро.")
            return
        stations_all = list(storage.get_station_names())
        if not stations_all:
            await message.answer(
                "Список станций пуст. Обратитесь к администратору.",
                reply_markup=build_start_keyboard(),
            )
            await state.finish()
            return
        normalized_query = _normalize_station_search_text(query)
        if not normalized_query:
            await message.answer("Введи название станции метро.")
            return
        matches: List[str] = []
        for station in stations_all:
            if normalized_query in _normalize_station_search_text(station):
                matches.append(station)
                if len(matches) >= MAX_SEARCH_RESULTS:
                    break
        if not matches:
            await message.answer(
                f"Станций по запросу «{query}» не нашли. Попробуй другое название."
            )
            return
        await state.set_state(WorkerStates.metro.state)
        context = await set_metro_context(state, mode="search", stations=matches, page=0, query=query)
        await present_metro_menu(message, state, context, via_edit=False)

    @dispatcher.callback_query_handler(
        lambda c: c.data and c.data.startswith("wshop_page:"),
        state=WorkerStates.shop,
    )
    async def worker_shop_page(call: CallbackQuery, state: FSMContext) -> None:
        context = await get_shop_context(state)
        if not context:
            await call.answer("Список лавок недоступен", show_alert=True)
            return
        try:
            requested_page = int(call.data.split(":", 1)[1])
        except ValueError:
            await call.answer("Некорректная страница", show_alert=True)
            return
        context["page"] = requested_page
        await state.update_data(worker_shop=context)
        await call.answer()
        await present_shop_menu(call.message, state, context, via_edit=True)

    @dispatcher.callback_query_handler(
        lambda c: c.data and c.data.startswith("wshop_pick:"),
        state=WorkerStates.shop,
    )
    async def worker_shop_pick(call: CallbackQuery, state: FSMContext) -> None:
        context = await get_shop_context(state)
        shops_list = context.get("shops") or []
        station = context.get("station") or ""
        try:
            index = int(call.data.split(":", 1)[1])
        except ValueError:
            await call.answer("Некорректная лавка", show_alert=True)
            return
        if index < 0 or index >= len(shops_list):
            await call.answer("Лавка не найдена", show_alert=True)
            return
        entry = shops_list[index]
        await call.answer()
        preserve_data = {
            "shop_id": entry["id"],
            "shop_name": entry["name"],
            "chosen_metro": station,
            "chosen_metro_dist_m": entry["distance"],
        }
        logging.info(
            "Пользователь %s выбрал лавку %s у метро «%s» (%s м)",
            call.from_user.id,
            entry["id"],
            station,
            entry["distance"],
        )
        try:
            await call.message.edit_text(
                f"Выбрана лавка «{entry['name']}» у метро «{station}»."
            )
        except Exception:  # noqa: BLE001
            pass
        await start_date_step(call.message, state, "worker")
        await state.update_data(**preserve_data)

    @dispatcher.callback_query_handler(
        lambda c: c.data == "wshop_back",
        state=[WorkerStates.metro, WorkerStates.shop],
    )
    async def worker_shop_back(call: CallbackQuery, state: FSMContext) -> None:
        await call.answer()
        await state.set_state(WorkerStates.metro.state)
        context = await get_metro_context(state)
        if not context:
            stations = list(storage.get_station_names())
            if not stations:
                await state.finish()
                await call.message.edit_text(
                    "Список станций пуст. Обратитесь к администратору.",
                    reply_markup=None,
                )
                await start_menu(call.message)
                return
            context = await set_metro_context(state, mode="list", stations=stations, page=0)
        await present_metro_menu(call.message, state, context, via_edit=True)

    @dispatcher.callback_query_handler(
        lambda c: c.data == "wshop_reset",
        state=[WorkerStates.metro, WorkerStates.shop],
    )
    async def worker_shop_reset(call: CallbackQuery, state: FSMContext) -> None:
        await call.answer()
        await state.finish()
        try:
            await call.message.edit_reply_markup()
        except Exception:  # noqa: BLE001
            pass
        await start_menu(call.message)

    @dispatcher.message_handler(state=WorkerStates.date)
    async def worker_date(message: types.Message, state: FSMContext) -> None:
        await process_date_message(message, state)

    @dispatcher.message_handler(state=WorkerStates.time_range)
    async def worker_time_range(message: types.Message, state: FSMContext) -> None:
        if (message.text or "").strip().lower() == BACK_COMMAND.lower():
            await handle_back_to_date(message, state)
            return
        parsed_range = parse_time_range(message.text or "")
        if not parsed_range:
            await message.answer(
                "Не понял время.\n" + TIME_PROMPT_MESSAGE,
                reply_markup=build_back_keyboard(),
            )
            return
        time_from, time_to = parsed_range
        data = await state.get_data()
        error = validate_timeslot(
            data.get("date", ""),
            time_from,
            time_to,
        )
        if error:
            await message.answer(
                f"{error}\n{TIME_PROMPT_MESSAGE}",
                reply_markup=build_back_keyboard(),
            )
            return
        await state.update_data(time_from=time_from, time_to=time_to)
        await message.answer(
            TIME_CONFIRMATION_TEMPLATE.format(time_from=time_from, time_to=time_to),
            reply_markup=ReplyKeyboardRemove(),
        )
        await state.set_state(WorkerStates.note.state)
        await message.answer(
            "Расскажите, на какую должность готовы выйти и оставьте комментарий."
        )

    @dispatcher.message_handler(state=WorkerStates.note)
    async def worker_note(message: types.Message, state: FSMContext) -> None:
        if (message.text or "").strip().lower() == BACK_COMMAND.lower():
            await handle_back_to_date(message, state)
            return
        await state.update_data(note=message.text.strip())
        data = await state.get_data()
        shops = await fetch_shops()
        selected_shop = shops.get(data.get("shop_id")) if data.get("shop_id") is not None else None
        shop_name = data.get("shop_name") or (selected_shop.name if selected_shop else "Любая лавка")
        station = data.get("chosen_metro") or "Не выбрано"
        distance = data.get("chosen_metro_dist_m")
        metro_line = (
            f"Метро: {station} · {distance} м" if distance is not None else f"Метро: {station}"
        )
        summary = (
            "Проверьте заявку:\n"
            f"Дата: {data['date']}\n"
            f"Смена: {data['time_from']}–{data['time_to']}\n"
            f"Лавка: {shop_name}\n"
            f"{metro_line}\n"
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
        await start_menu(call.message)

    @dispatcher.callback_query_handler(lambda c: c.data == "worker_confirm", state=WorkerStates.confirm)
    async def worker_confirm(call: CallbackQuery, state: FSMContext) -> None:
        await call.answer()
        await call.message.edit_text("Публикуем заявку...")
        await handle_post_publication(call.message.chat.id, call.from_user, state, "worker")

@dp.message_handler(commands=["refresh_shops"], state="*")
async def cmd_refresh_shops(message: types.Message, state: FSMContext) -> None:
    if not message.from_user or message.from_user.id not in ADMINS:
        await message.reply("Недостаточно прав для обновления справочника.")
        return
    await message.reply("Обновляем справочник лавок...")
    try:
        await storage.refresh_shops_cache()
    except Exception as exc:  # noqa: BLE001
        logging.exception("Не удалось обновить справочник лавок по команде /refresh_shops: %s", exc)
        await message.answer("Не удалось обновить справочник лавок. Проверьте логи.")
        return
    shops = storage.get_shops()
    updated_at = storage.get_shops_updated_at()
    if updated_at:
        local_time = updated_at.astimezone(TIMEZONE).strftime("%d.%m %H:%M")
        await message.answer(
            f"Справочник лавок обновлён. Доступно {len(shops)} лавок. Обновлено: {local_time}."
        )
    else:
        await message.answer(f"Справочник лавок обновлён. Доступно {len(shops)} лавок.")


@dp.message_handler(commands=["start"], state="*")
async def cmd_start(message: types.Message, state: FSMContext) -> None:
    # Сбрасываем любые активные состояния, чтобы полностью перезапустить сценарий.
    await state.finish()
    if not await ensure_contact_exists(message):
        return
    await start_menu(message)


@dp.message_handler(content_types=ContentType.CONTACT, state=RegistrationStates.waiting_contact)
async def registration_contact(message: types.Message, state: FSMContext) -> None:
    contact = message.contact
    if not contact or contact.user_id != message.from_user.id:
        await message.answer(
            "Пожалуйста, отправьте контакт, используя кнопку «Отправить контакт ☎️».",
            reply_markup=build_contact_keyboard(),
        )
        return
    await ensure_user(message.from_user, phone_number=contact.phone_number)
    await state.finish()
    await message.answer(
        "Спасибо! Контакт сохранён.", reply_markup=ReplyKeyboardRemove()
    )
    await start_menu(message)


@dp.message_handler(state=RegistrationStates.waiting_contact)
async def registration_waiting(message: types.Message) -> None:
    await message.answer(
        "Чтобы продолжить, поделитесь, пожалуйста, своим контактом.",
        reply_markup=build_contact_keyboard(),
    )


@dp.errors_handler()
async def on_error(update: types.Update, error: Exception) -> bool:
    logging.exception("Ошибка при обработке апдейта: %s", error)
    await send_tech(f"Ошибка: {error}")
    return True


async def on_startup(_: Dispatcher) -> None:
    shops = storage.get_shops()
    logging.info("Бот запущен и готов к работе. Доступно лавок: %s", len(shops))
    global shops_refresh_task
    if shops_refresh_task is None:
        shops_refresh_task = asyncio.create_task(periodic_shops_refresh())
        logging.info("Запущено автоматическое обновление справочника лавок каждые %s секунд", SHOPS_REFRESH_INTERVAL_SECONDS)
    if WEBHOOK_URL:
        webhook_url = WEBHOOK_URL + WEBHOOK_PATH
        await bot.set_webhook(webhook_url, drop_pending_updates=True)
        logging.info("Webhook установлен на %s", webhook_url)
    else:
        logging.warning("WEBHOOK_URL не установлен, используется polling режим")


async def on_shutdown(_: Dispatcher) -> None:
    global shops_refresh_task
    if shops_refresh_task:
        shops_refresh_task.cancel()
        try:
            await shops_refresh_task
        except asyncio.CancelledError:
            pass
        shops_refresh_task = None
    if WEBHOOK_URL:
        await bot.delete_webhook()
        logging.info("Webhook удален")


def register_handlers() -> None:
    run_director_flow(dp)
    run_worker_flow(dp)
    dp.register_callback_query_handler(
        on_pick_date_selection,
        lambda c: c.data and c.data.startswith("pick_date:"),
        state=[DirectorStates.date, WorkerStates.date],
    )
    dp.register_message_handler(
        handle_back_to_date,
        lambda m: (m.text or "").strip().lower() == BACK_COMMAND.lower(),
        state=[
            DirectorStates.time_range,
            DirectorStates.shop,
            DirectorStates.note,
            DirectorStates.confirm,
            WorkerStates.time_range,
            WorkerStates.shop,
            WorkerStates.note,
            WorkerStates.confirm,
        ],
    )
    dp.register_callback_query_handler(on_callback_pick, lambda c: c.data and c.data.startswith("pick:"))


def main() -> None:
    register_handlers()
    if WEBHOOK_URL:
        executor.start_webhook(
            dispatcher=dp,
            webhook_path=WEBHOOK_PATH,
            skip_updates=True,
            on_startup=on_startup,
            on_shutdown=on_shutdown,
            host=WEBAPP_HOST,
            port=WEBAPP_PORT,
        )
    else:
        logging.info("Запуск в polling режиме (для разработки)")
        executor.start_polling(dp, skip_updates=True, on_startup=on_startup)


if __name__ == "__main__":
    main()


# TODO: Модерировать посты перед каналом.
# TODO: Авто-закрытие open-карточек по истечении смены.
# TODO: Экспорт отчетов в CSV.
