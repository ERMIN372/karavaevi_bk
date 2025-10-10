import asyncio
import html
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

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
    position = State()
    note = State()
    confirm = State()


class WorkerStates(StatesGroup):
    area = State()
    metro = State()
    metro_search = State()
    shop = State()
    date = State()
    time_range = State()
    position = State()
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

AREA_PROMPT_MESSAGE = "Выбери район. Потом подберём метро и лавку рядом."
STATION_PROMPT_TEMPLATE = "Станции в «{area_name}». Выбери метро:"
STATION_EMPTY_TEMPLATE = "В «{area_name}» сейчас нет вариантов. Выбери другой район."
STATION_SEARCH_PROMPT = (
    "Напиши название станции метро.\n"
    "Можно: «Проспект Мира», «ВДНХ», «Китай-город»."
)
STATION_SEARCH_RESULTS_TEMPLATE = "Результаты поиска по «{query}». Выбери метро:"
SHOP_EMPTY_TEMPLATE = "Рядом со станцией «{station}» сейчас пусто. Выбери другую станцию."
SHOP_LIST_TITLE_TEMPLATE = "Лавки у «{station}». Выбери место:"

STATION_SEARCH_BUTTON_TEXT = "🔎 Поиск по названию"
STATION_BACK_BUTTON_TEXT = "⬅️ Районы"
STATION_RESET_BUTTON_TEXT = "📋 Список станций"
SHOP_BACK_BUTTON_TEXT = "⬅️ Станции"
SHOP_RESET_BUTTON_TEXT = "🔁 Сбросить выбор"

STATIONS_PER_PAGE = 10
SHOPS_PER_PAGE = 10
MAX_SEARCH_RESULTS = 50

MAX_REQUEST_SLOTS_DEFAULT = 5
LIMIT_REACHED_MESSAGE = (
    "Эта заявка уже набрала 5 из 5.\n"
    "Попробуй другую — рядом есть ещё хорошие варианты ✨"
)
EXPIRED_REQUEST_MESSAGE = (
    "Упс, заявка уже закрыта. Загляни в свежие — они ждут тебя 🙌"
)
DISABLED_CALLBACK_DATA = "noop"

REQUEST_LOCKS: Dict[int, asyncio.Lock] = {}
REQUEST_LOCKS_MAP_LOCK = asyncio.Lock()

SHOPS_REFRESH_INTERVAL_SECONDS = 15 * 60
shops_refresh_task: Optional[asyncio.Task] = None
REQUESTS_CLEANUP_INTERVAL_SECONDS = 60
requests_cleanup_task: Optional[asyncio.Task] = None

POSITION_BUTTON_OPTIONS: Tuple[str, ...] = (
    "Кассир",
    "Бариста",
    "Повар",
    "Повар-универсал",
    "РТЗ",
    "Уборщик",
)
POSITION_PROMPTS: Dict[str, str] = {
    "director": "Укажите требуемую должность. Можно выбрать кнопку или ввести свою.",
    "worker": "Укажите желаемую должность. Можно выбрать кнопку или ввести свою.",
}
POSITION_CUSTOM_BUTTON = "Другая…"
POSITION_BACK_BUTTON = "⬅️ Назад"
POSITION_CUSTOM_PROMPT = (
    "Введите должность текстом. От 2 до 30 символов. Допустимы буквы, пробел и дефис."
)
POSITION_ALLOWED_PATTERN = re.compile(r"^[A-Za-zА-Яа-яЁё\s-]+$")
POSITION_MIN_LENGTH = 2
POSITION_MAX_LENGTH = 30
POSITION_SYNONYMS: Dict[str, str] = {
    "повар универсал": "Повар-универсал",
    "повар-универсал": "Повар-универсал",
    "универсал": "Повар-универсал",
    "ртз": "РТЗ",
}

WEEKDAY_SHORT_LABELS = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
WEEKDAY_COMPACT_NAMES = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]
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


def build_position_keyboard(flow: str) -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup(row_width=3)
    for index, label in enumerate(POSITION_BUTTON_OPTIONS):
        markup.insert(
            InlineKeyboardButton(
                label,
                callback_data=f"{flow}_position:{index}",
            )
        )
    markup.row(
        InlineKeyboardButton(
            POSITION_CUSTOM_BUTTON, callback_data=f"{flow}_position:custom"
        ),
        InlineKeyboardButton(
            POSITION_BACK_BUTTON, callback_data=f"{flow}_position:back"
        ),
    )
    return markup


def _canonicalize_position_key(value: str) -> str:
    text = value.strip().lower()
    text = text.replace("ё", "е")
    text = text.replace("–", "-")
    text = text.replace("—", "-")
    text = text.replace("−", "-")
    text = re.sub(r"\s*-[\s-]*", "-", text)
    text = re.sub(r"\s+", " ", text)
    return text


def _capitalize_position_text(value: str) -> str:
    words = []
    for word in value.split(" "):
        parts = []
        for part in word.split("-"):
            if not part:
                parts.append(part)
            else:
                parts.append(part[0].upper() + part[1:].lower())
        words.append("-".join(parts))
    return " ".join(words)


def normalize_position_input(raw_value: str) -> Tuple[Optional[str], Optional[str]]:
    if raw_value is None:
        return None, "Пожалуйста, укажите должность."
    candidate = raw_value.strip()
    candidate = re.sub(r"\s+", " ", candidate)
    candidate = candidate.replace("–", "-")
    candidate = candidate.replace("—", "-")
    candidate = candidate.replace("−", "-")
    candidate = re.sub(r"\s*-[\s-]*", "-", candidate)
    candidate = re.sub(r"\s+", " ", candidate)
    if not candidate:
        return None, "Пожалуйста, укажите должность."
    length = len(candidate)
    if length < POSITION_MIN_LENGTH:
        return None, "Название должности слишком короткое. Минимум 2 символа."
    if length > POSITION_MAX_LENGTH:
        return None, "Название должности слишком длинное. Сократите до 30 символов."
    if not POSITION_ALLOWED_PATTERN.match(candidate):
        return None, "Можно использовать только буквы, пробел и дефис. Попробуйте ещё раз."
    normalized_key = _canonicalize_position_key(candidate)
    if normalized_key in POSITION_SYNONYMS:
        return POSITION_SYNONYMS[normalized_key], None
    return _capitalize_position_text(candidate), None


def build_position_prompt(
    flow: str,
    *,
    current: Optional[str] = None,
    reminder: Optional[str] = None,
) -> str:
    base = POSITION_PROMPTS.get(flow, POSITION_PROMPTS["worker"])
    parts = []
    if reminder:
        parts.append(reminder)
    parts.append(base)
    if current:
        parts.append(f"Сейчас выбрано: {current}")
    return "\n\n".join(parts)


async def present_position_step(
    target_message: types.Message,
    state: FSMContext,
    flow: str,
    *,
    via_edit: bool,
    reminder: Optional[str] = None,
) -> None:
    state_cls = DirectorStates if flow == "director" else WorkerStates
    data = await state.get_data()
    current = data.get("position") if isinstance(data, dict) else None
    prompt_text = build_position_prompt(flow, current=current, reminder=reminder)
    markup = build_position_keyboard(flow)
    prompt_message: Optional[types.Message] = None
    if via_edit:
        try:
            await target_message.edit_text(prompt_text, reply_markup=markup)
            prompt_message = target_message
        except Exception:  # noqa: BLE001
            logging.debug(
                "Не удалось обновить сообщение с выбором должности, отправляем новое"
            )
    if prompt_message is None:
        prompt_message = await target_message.answer(prompt_text, reply_markup=markup)
    await state.set_state(state_cls.position.state)
    prompt_context = {
        "message_id": prompt_message.message_id,
    }
    if prompt_message.chat:
        prompt_context["chat_id"] = prompt_message.chat.id
    await state.update_data(position_prompt=prompt_context)


async def proceed_to_note_step(
    flow: str,
    source_message: types.Message,
    state: FSMContext,
    *,
    via_edit: bool,
) -> None:
    state_cls = DirectorStates if flow == "director" else WorkerStates
    position_value = None
    prompt_context: Dict[str, Any] = {}
    data = await state.get_data()
    if isinstance(data, dict):
        position_value = data.get("position")
        context_value = data.get("position_prompt")
        if isinstance(context_value, dict):
            prompt_context = context_value
    display_value = position_value or "должность выбрана"
    edited = False
    if via_edit:
        try:
            await source_message.edit_text(f"Должность: {display_value}")
            edited = True
        except Exception:  # noqa: BLE001
            logging.debug("Не удалось обновить сообщение с выбранной должностью напрямую")
    if not edited and prompt_context:
        chat_id = prompt_context.get("chat_id")
        message_id = prompt_context.get("message_id")
        if chat_id and message_id:
            try:
                await bot.edit_message_text(
                    f"Должность: {display_value}", chat_id, message_id
                )
                edited = True
            except Exception:  # noqa: BLE001
                try:
                    await bot.edit_message_reply_markup(chat_id, message_id, reply_markup=None)
                    edited = True
                except Exception:  # noqa: BLE001
                    logging.debug("Не удалось скрыть клавиатуру выбора должности")
    await state.set_state(state_cls.note.state)
    if flow == "director":
        prompt = (
            "Добавьте комментарий к заявке. Если ничего не нужно, отправьте «—». "
            "Чтобы вернуться к выбору должности, напишите «Назад»."
        )
    else:
        prompt = (
            "Оставьте пожелания по смене. Если нечего добавить, отправьте «—». "
            "Чтобы вернуться к выбору должности, напишите «Назад»."
        )
    await source_message.answer(prompt, reply_markup=ReplyKeyboardRemove())


def format_human_date(value: date) -> str:
    weekday_name = WEEKDAY_FULL_NAMES[value.weekday()]
    month_name = MONTH_GENITIVE[value.month]
    return f"{weekday_name}, {value.day:02d} {month_name} {value.year}"


def format_compact_date_text(raw_value: str) -> str:
    if not raw_value:
        return "—"
    try:
        parsed = datetime.strptime(raw_value, "%Y-%m-%d").date()
    except ValueError:
        return raw_value
    weekday = WEEKDAY_COMPACT_NAMES[parsed.weekday()]
    return f"{weekday}, {parsed.strftime('%d.%m')}"


def build_channel_message_url(
    message_id: Optional[int], *, chat_username: Optional[str] = None
) -> Optional[str]:
    if not message_id:
        return None
    if chat_username:
        username = chat_username.lstrip("@")
        if username:
            return f"https://t.me/{username}/{message_id}"
    if not CHANNEL_ID:
        return None
    identifier = str(abs(CHANNEL_ID))
    if identifier.startswith("100"):
        identifier = identifier[3:]
    return f"https://t.me/c/{identifier}/{message_id}"


def build_request_summary_line(
    *,
    shop_name: str,
    date_human: str,
    time_from: str,
    time_to: str,
    station: str,
) -> str:
    station_part = f" • {station}" if station else ""
    return (
        f"<b>{shop_name}</b> • {date_human} • {time_from}-{time_to}{station_part}"
    )


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


def build_area_keyboard(areas: List[storage.AreaSummary]) -> InlineKeyboardMarkup:
    markup = InlineKeyboardMarkup(row_width=1)
    for area in areas:
        emoji = f"{area.emoji} " if area.emoji else ""
        button_text = f"{emoji}{area.title} ({area.shop_count})"
        markup.add(InlineKeyboardButton(button_text, callback_data=f"warea:{area.area_id}"))
    return markup


def build_station_keyboard(
    stations: List[storage.StationSummary], page: int, *, show_reset: bool
) -> Tuple[InlineKeyboardMarkup, int, int]:
    page, start, end, total_pages = _compute_page_bounds(len(stations), page, STATIONS_PER_PAGE)
    markup = InlineKeyboardMarkup(row_width=1)
    for index in range(start, end):
        station = stations[index]
        button_text = f"{station.name} (лавок: {station.shop_count})"
        markup.add(InlineKeyboardButton(button_text, callback_data=f"wstation_pick:{index}"))
    nav_buttons: List[InlineKeyboardButton] = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"wstation_page:{page - 1}"))
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"wstation_page:{page + 1}"))
    if nav_buttons:
        markup.row(*nav_buttons)
    markup.row(InlineKeyboardButton(STATION_SEARCH_BUTTON_TEXT, callback_data="wstation_search"))
    if show_reset:
        markup.row(InlineKeyboardButton(STATION_RESET_BUTTON_TEXT, callback_data="wstation_reset"))
    markup.row(InlineKeyboardButton(STATION_BACK_BUTTON_TEXT, callback_data="wstation_back_area"))
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
        for key in (
            "shop_id",
            "shop_name",
            "chosen_metro",
            "chosen_metro_dist_m",
            "note",
            "position",
        )
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


async def get_request_lock(request_id: int) -> asyncio.Lock:
    async with REQUEST_LOCKS_MAP_LOCK:
        lock = REQUEST_LOCKS.get(request_id)
        if lock is None:
            lock = asyncio.Lock()
            REQUEST_LOCKS[request_id] = lock
    return lock


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


def get_request_slots(record: Dict[str, Any]) -> Tuple[int, int]:
    try:
        max_slots = int(record.get("max_slots") or MAX_REQUEST_SLOTS_DEFAULT)
    except (TypeError, ValueError):
        max_slots = MAX_REQUEST_SLOTS_DEFAULT
    if max_slots <= 0:
        max_slots = MAX_REQUEST_SLOTS_DEFAULT
    if max_slots > MAX_REQUEST_SLOTS_DEFAULT:
        max_slots = MAX_REQUEST_SLOTS_DEFAULT
    try:
        filled = int(record.get("filled_slots") or 0)
    except (TypeError, ValueError):
        filled = 0
    filled = max(0, min(filled, max_slots))
    return filled, max_slots


def build_request_markup(record: Dict[str, Any]) -> InlineKeyboardMarkup:
    filled_slots, max_slots = get_request_slots(record)
    markup = InlineKeyboardMarkup()
    if filled_slots >= max_slots:
        button_text = f"Мест нет (занято {filled_slots}/{max_slots})"
        markup.add(InlineKeyboardButton(button_text, callback_data=DISABLED_CALLBACK_DATA))
        return markup
    request_id = record.get("id")
    button_text = "Откликнуться" if record.get("kind") == "director" else "Пригласить"
    markup.add(InlineKeyboardButton(button_text, callback_data=f"pick:{request_id}"))
    return markup


def render_channel_post(record: Dict[str, Any]) -> str:
    shop_name = record.get("shop_name") or "Любая лавка"
    filled_slots, max_slots = get_request_slots(record)
    if record["kind"] == "director":
        title = "🔔 Заявка на подработку от директора лавки"
        position_value = (record.get("position") or "").strip() or "—"
        note = (record.get("note") or "").strip() or "—"
        lines = [
            title,
            f"Лавка: {shop_name}",
            f"Дата: {record['date']}",
            f"Смена: {record['time_from']}–{record['time_to']}",
            f"Должность: {position_value}",
            f"Комментарий: {note}",
            "Нажмите «Откликнуться», чтобы связаться с директором.",
        ]
    else:
        title = "💼 Сотрудник ищет смену"
        position_value = (record.get("position") or "").strip() or "—"
        note = (record.get("note") or "").strip() or "—"
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
                f"Желаемая должность: {position_value}",
                f"Пожелания: {note}",
                "Нажмите «Пригласить», чтобы связаться с сотрудником.",
            ]
        )
    lines.append("")
    lines.append(f"Статус: занято {filled_slots}/{max_slots}")
    return "\n".join(lines)


async def cleanup_expired_requests() -> None:
    try:
        records = await storage.gs_list_requests()
    except Exception as exc:  # noqa: BLE001
        logging.exception("Не удалось получить список заявок для очистки: %s", exc)
        return

    now_local = now_in_timezone()
    for record in records:
        request_id = record.get("id")
        if not request_id:
            continue
        status = str(record.get("status") or "").strip().lower()
        if status in {"expired", "cancelled"}:
            continue
        end_dt_iso = record.get("end_dt_iso") or ""
        if not end_dt_iso:
            continue
        try:
            end_dt = datetime.fromisoformat(end_dt_iso)
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=TIMEZONE)
        except ValueError:
            logging.warning(
                "Не удалось распарсить время окончания смены для заявки %s: %s",
                request_id,
                end_dt_iso,
            )
            continue
        if now_local <= end_dt + timedelta(minutes=1):
            continue

        lock = await get_request_lock(request_id)
        async with lock:
            latest = await storage.gs_find_request(request_id)
            if not latest:
                continue
            latest_status = str(latest.get("status") or "").strip().lower()
            if latest_status in {"expired", "cancelled"}:
                continue
            latest_end_iso = latest.get("end_dt_iso") or end_dt_iso
            try:
                latest_end = datetime.fromisoformat(latest_end_iso)
                if latest_end.tzinfo is None:
                    latest_end = latest_end.replace(tzinfo=TIMEZONE)
            except ValueError:
                latest_end = end_dt
            if now_local <= latest_end + timedelta(minutes=1):
                continue

            message_id = latest.get("channel_message_id")
            if message_id:
                try:
                    await bot.delete_message(CHANNEL_ID, message_id)
                except Exception as exc:  # noqa: BLE001
                    logging.debug(
                        "Не удалось удалить сообщение заявки %s: %s",
                        request_id,
                        exc,
                    )
            try:
                await storage.gs_update_request_fields(
                    request_id,
                    {"status": "expired", "channel_message_id": ""},
                )
            except Exception as exc:  # noqa: BLE001
                logging.exception(
                    "Не удалось обновить статус заявки %s при очистке: %s",
                    request_id,
                    exc,
                )
                continue

            author_id = latest.get("author_id")
            if author_id:
                try:
                    station_raw = (latest.get("chosen_metro") or "").strip()
                    summary_line = build_request_summary_line(
                        shop_name=html.escape(latest.get("shop_name") or "Любая лавка"),
                        date_human=html.escape(
                            format_compact_date_text(latest.get("date") or "")
                        ),
                        time_from=html.escape(latest.get("time_from") or "—"),
                        time_to=html.escape(latest.get("time_to") or "—"),
                        station=html.escape(station_raw) if station_raw else "",
                    )
                    expire_message = (
                        f"⏱ Заявка №{html.escape(str(request_id))} завершена.\n"
                        f"{summary_line}"
                    )
                    await bot.send_message(
                        author_id,
                        expire_message,
                        disable_web_page_preview=True,
                    )
                except Exception as exc:  # noqa: BLE001
                    logging.debug(
                        "Не удалось уведомить автора заявки %s о завершении: %s",
                        request_id,
                        exc,
                    )


async def periodic_requests_cleanup() -> None:
    while True:
        try:
            await asyncio.sleep(REQUESTS_CLEANUP_INTERVAL_SECONDS)
            await cleanup_expired_requests()
        except asyncio.CancelledError:
            break
        except Exception as exc:  # noqa: BLE001
            logging.exception("Ошибка при очистке завершённых заявок: %s", exc)


async def on_callback_pick(call: CallbackQuery) -> None:
    try:
        _, request_id_text = call.data.split(":", 1)
        request_id = int(request_id_text)
    except (ValueError, AttributeError):
        await call.answer("Некорректный формат заявки.", show_alert=True)
        return

    lock = await get_request_lock(request_id)
    async with lock:
        try:
            record = await storage.gs_find_request(request_id)
            if not record:
                await call.answer(EXPIRED_REQUEST_MESSAGE, show_alert=True)
                return

            status = str(record.get("status") or "").strip().lower()
            if status in {"expired", "cancelled"}:
                await call.answer(EXPIRED_REQUEST_MESSAGE, show_alert=True)
                return

            picker = call.from_user
            if record.get("author_id") == picker.id:
                await call.answer("Нельзя откликаться на собственную заявку.", show_alert=True)
                return

            end_dt_iso = record.get("end_dt_iso")
            if end_dt_iso:
                try:
                    end_dt = datetime.fromisoformat(end_dt_iso)
                    if end_dt.tzinfo is None:
                        end_dt = end_dt.replace(tzinfo=TIMEZONE)
                    if now_in_timezone() > end_dt + timedelta(minutes=1):
                        await storage.gs_update_request_fields(request_id, {"status": "expired"})
                        await call.answer(EXPIRED_REQUEST_MESSAGE, show_alert=True)
                        return
                except ValueError:
                    logging.warning(
                        "Не удалось распарсить время окончания смены для заявки %s: %s",
                        request_id,
                        end_dt_iso,
                    )

            filled_slots, max_slots = get_request_slots(record)
            ids_key = "picked_ids" if record.get("kind") == "director" else "invited_ids"
            current_ids = list(record.get(ids_key) or [])
            if picker.id in current_ids:
                await call.answer("Вы уже откликались по этой заявке.", show_alert=True)
                return

            if filled_slots >= max_slots:
                await call.answer(LIMIT_REACHED_MESSAGE, show_alert=True)
                return

            current_ids.append(picker.id)
            new_filled = len(current_ids)
            new_status = "filled" if new_filled >= max_slots else "open"
            updates = {
                ids_key: current_ids,
                "filled_slots": new_filled,
                "status": new_status,
                "channel_message_id": call.message.message_id if call.message else record.get("channel_message_id"),
            }
            await storage.gs_update_request_fields(request_id, updates)
            updated_record = await storage.gs_find_request(request_id)
            if not updated_record:
                await call.answer(EXPIRED_REQUEST_MESSAGE, show_alert=True)
                return

            updated_filled, updated_max = get_request_slots(updated_record)
            if updated_filled > updated_max:
                await call.answer(LIMIT_REACHED_MESSAGE, show_alert=True)
                return

            author_chat = await bot.get_chat(updated_record["author_id"])
            picker_user_data = await storage.gs_get_user(picker.id)
            author_user_data = await storage.gs_get_user(updated_record["author_id"])
            picker_contact = html.escape(format_contact_details(picker_user_data, picker))
            author_contact = html.escape(
                format_contact_details(author_user_data, author_chat)
            )

            station_raw = (updated_record.get("chosen_metro") or "").strip()
            summary_line = build_request_summary_line(
                shop_name=html.escape(updated_record.get("shop_name") or "Любая лавка"),
                date_human=html.escape(
                    format_compact_date_text(updated_record.get("date") or "")
                ),
                time_from=html.escape(updated_record.get("time_from") or "—"),
                time_to=html.escape(updated_record.get("time_to") or "—"),
                station=html.escape(station_raw) if station_raw else "",
            )
            request_id_text = html.escape(str(request_id))

            if updated_record.get("kind") == "director":
                message_for_author = (
                    f"✅ Сотрудник откликнулся на вашу заявку №{request_id_text}\n"
                    f"{summary_line}\n"
                    f"Контакт сотрудника: {picker_contact}"
                )
                message_for_picker = (
                    f"🎉 Вы откликнулись на смену по заявке №{request_id_text}\n"
                    f"{summary_line}\n"
                    f"Свяжитесь с директором: {author_contact}"
                )
            else:
                message_for_picker = (
                    f"✅ Вы пригласили по заявке №{request_id_text}\n"
                    f"{summary_line}\n"
                    f"Контакт сотрудника: {author_contact}"
                )
                message_for_author = (
                    f"🎉 Директор пригласил вас на смену по заявке №{request_id_text}\n"
                    f"{summary_line}\n"
                    f"Контакт директора: {picker_contact}"
                )

            try:
                await bot.send_message(
                    updated_record["author_id"],
                    message_for_author,
                    disable_web_page_preview=True,
                )
            except Exception as exc:  # noqa: BLE001
                logging.exception(
                    "Не удалось уведомить автора заявки %s", updated_record["author_id"]
                )
                await send_tech(
                    f"Не удалось уведомить автора заявки {updated_record['author_id']}: {exc}"
                )

            try:
                await bot.send_message(
                    picker.id, message_for_picker, disable_web_page_preview=True
                )
            except Exception as exc:  # noqa: BLE001
                logging.exception("Не удалось уведомить участника %s", picker.id)
                await send_tech(f"Не удалось уведомить пользователя {picker.id}: {exc}")

            updated_text = render_channel_post(updated_record)
            updated_markup = build_request_markup(updated_record)
            target_message_id = (
                call.message.message_id
                if call.message and call.message.message_id
                else updated_record.get("channel_message_id")
            )
            target_chat_id = (
                call.message.chat.id
                if call.message and call.message.chat
                else CHANNEL_ID
            )
            if target_message_id:
                try:
                    await bot.edit_message_text(
                        updated_text,
                        target_chat_id,
                        target_message_id,
                        reply_markup=updated_markup,
                        disable_web_page_preview=True,
                    )
                except Exception as exc:  # noqa: BLE001
                    logging.exception(
                        "Не удалось обновить сообщение заявки %s: %s",
                        request_id,
                        exc,
                    )
            logging.info(
                "Пользователь %s обновил заявку %s (%s/%s)",
                picker.id,
                request_id,
                updated_filled,
                updated_max,
            )
            await call.answer("Контакты отправлены в личные сообщения.")
        except Exception as exc:  # noqa: BLE001
            logging.exception("Ошибка при обработке отклика на заявку %s", call.data)
            await call.answer("Что-то пошло не так. Мы уже разбираемся.", show_alert=True)
            await send_tech(f"Ошибка при обработке отклика: {exc}")


async def on_disabled_callback(call: CallbackQuery) -> None:
    await call.answer(LIMIT_REACHED_MESSAGE, show_alert=True)


async def handle_post_publication(
    chat_id: int,
    author: types.User,
    state: FSMContext,
    kind: str,
) -> None:
    data = await state.get_data()
    shop_id = data.get("shop_id")
    shop_name = data.get("shop_name")
    position_value = (data.get("position") or "").strip()
    shops = await fetch_shops()
    if not position_value:
        state_cls = DirectorStates if kind == "director" else WorkerStates
        await state.set_state(state_cls.position.state)
        prompt_text = build_position_prompt(kind, reminder="Это обязательное поле.")
        markup = build_position_keyboard(kind)
        sent = await bot.send_message(chat_id, prompt_text, reply_markup=markup)
        await state.update_data(
            position_prompt={"chat_id": sent.chat.id if sent.chat else chat_id, "message_id": sent.message_id}
        )
        return
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
        "position": position_value,
        "note": data.get("note"),
        "author_id": author.id,
        "shop_name": shop_name,
        "status": "open",
        "max_slots": MAX_REQUEST_SLOTS_DEFAULT,
        "picked_ids": [],
        "invited_ids": [],
        "filled_slots": 0,
    }

    end_dt_iso = ""
    date_raw = data.get("date")
    time_to_raw = data.get("time_to")
    if date_raw and time_to_raw:
        try:
            date_obj = datetime.strptime(date_raw, "%Y-%m-%d").date()
            time_to_obj = datetime.strptime(time_to_raw, "%H:%M").time()
            end_dt = datetime.combine(date_obj, time_to_obj, tzinfo=TIMEZONE)
            end_dt_iso = end_dt.isoformat()
        except ValueError:
            logging.warning(
                "Не удалось вычислить окончание смены для заявки: %s %s",
                date_raw,
                time_to_raw,
            )
    payload["end_dt_iso"] = end_dt_iso

    now_iso = datetime.now(timezone.utc).isoformat()
    payload["created_at"] = now_iso
    payload["updated_at"] = now_iso
    request_id, _ = await storage.gs_append_request(payload)
    payload["id"] = request_id
    text = render_channel_post(payload)
    markup = build_request_markup(payload)
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


async def present_director_shop_menu(
    target_message: types.Message, state: FSMContext, *, via_edit: bool
) -> bool:
    shops = await fetch_shops()
    if not shops:
        await state.finish()
        text = (
            "Список лавок пуст. Обратитесь к администратору для настройки справочника."
        )
        markup = build_start_keyboard()
        if via_edit:
            try:
                await target_message.edit_text(text, reply_markup=markup)
                return False
            except Exception:  # noqa: BLE001
                logging.debug(
                    "Не удалось обновить список лавок для директора, отправляем новое сообщение"
                )
        await target_message.answer(text, reply_markup=markup)
        return False
    keyboard = InlineKeyboardMarkup(row_width=2)
    sorted_shops = sorted(shops.values(), key=lambda record: record.name.lower())
    for shop in sorted_shops:
        keyboard.insert(
            InlineKeyboardButton(shop.name, callback_data=f"director_shop:{shop.id}")
        )
    await state.set_state(DirectorStates.shop.state)
    if via_edit:
        try:
            await target_message.edit_text("Выберите вашу лавку:", reply_markup=keyboard)
            return True
        except Exception:  # noqa: BLE001
            logging.debug("Не удалось обновить список лавок, отправляем новое сообщение")
    await target_message.answer("Выберите вашу лавку:", reply_markup=keyboard)
    return True


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
        if not await present_director_shop_menu(message, state, via_edit=False):
            return

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
        await present_position_step(call.message, state, "director", via_edit=True)

    @dispatcher.callback_query_handler(
        lambda c: c.data and c.data.startswith("director_position:"),
        state=DirectorStates.position,
    )
    async def director_position_choice(call: CallbackQuery, state: FSMContext) -> None:
        try:
            _, action = (call.data or "").split(":", 1)
        except ValueError:
            await call.answer("Некорректный выбор", show_alert=True)
            return
        if action == "back":
            await call.answer()
            await present_director_shop_menu(call.message, state, via_edit=True)
            return
        if action == "custom":
            await call.answer()
            await call.message.answer(POSITION_CUSTOM_PROMPT)
            return
        try:
            index = int(action)
        except ValueError:
            await call.answer("Некорректный выбор", show_alert=True)
            return
        if index < 0 or index >= len(POSITION_BUTTON_OPTIONS):
            await call.answer("Некорректный выбор", show_alert=True)
            return
        position_value = POSITION_BUTTON_OPTIONS[index]
        await state.update_data(position=position_value)
        await call.answer("Готово")
        await proceed_to_note_step("director", call.message, state, via_edit=True)

    @dispatcher.message_handler(state=DirectorStates.position)
    async def director_position_input(message: types.Message, state: FSMContext) -> None:
        text = (message.text or "").strip()
        if not text:
            await message.answer(POSITION_CUSTOM_PROMPT)
            return
        if text.lower() == BACK_COMMAND.lower():
            await present_director_shop_menu(message, state, via_edit=False)
            return
        normalized, error = normalize_position_input(text)
        if error:
            await message.answer(f"{error}\n{POSITION_CUSTOM_PROMPT}")
            return
        await state.update_data(position=normalized)
        await proceed_to_note_step("director", message, state, via_edit=False)

    @dispatcher.message_handler(state=DirectorStates.note)
    async def director_note(message: types.Message, state: FSMContext) -> None:
        if (message.text or "").strip().lower() == BACK_COMMAND.lower():
            await present_position_step(message, state, "director", via_edit=False)
            return
        note_value = (message.text or "").strip()
        await state.update_data(note=note_value)
        data = await state.get_data()
        position_value = (data.get("position") or "").strip()
        if not position_value:
            await message.answer("Сначала укажите должность.")
            await present_position_step(
                message,
                state,
                "director",
                via_edit=False,
                reminder="Это обязательное поле.",
            )
            return
        shops = await fetch_shops()
        selected_shop = shops.get(data.get("shop_id")) if data.get("shop_id") is not None else None
        shop_name = data.get("shop_name") or (selected_shop.name if selected_shop else "Не выбрана")
        note_display = note_value if note_value else "—"
        summary = (
            "Проверьте заявку:\n"
            f"Дата: {data['date']}\n"
            f"Смена: {data['time_from']}–{data['time_to']}\n"
            f"Лавка: {shop_name}\n"
            f"Должность: {position_value}\n"
            f"Комментарий: {note_display}"
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
        data = await state.get_data()
        position_value = (data.get("position") or "").strip()
        if not position_value:
            await call.answer("Укажите должность", show_alert=True)
            await present_position_step(
                call.message,
                state,
                "director",
                via_edit=True,
                reminder="Это обязательное поле.",
            )
            return
        await call.answer()
        await call.message.edit_text("Публикуем заявку...")
        await handle_post_publication(call.message.chat.id, call.from_user, state, "director")


def run_worker_flow(dispatcher: Dispatcher) -> None:
    async def present_area_menu(
        target_message: types.Message, state: FSMContext, *, via_edit: bool
    ) -> None:
        areas = [area for area in storage.get_area_summaries() if area.shop_count > 0]
        if not areas:
            await state.finish()
            text = "Список районов пуст. Обратитесь к администратору."
            markup = build_start_keyboard()
            if via_edit:
                try:
                    await target_message.edit_text(text, reply_markup=markup)
                    return
                except Exception:  # noqa: BLE001
                    logging.debug("Не удалось обновить сообщение с районами, отправляем новое")
            await target_message.answer(text, reply_markup=markup)
            return
        markup = build_area_keyboard(list(areas))
        if via_edit:
            try:
                await target_message.edit_text(AREA_PROMPT_MESSAGE, reply_markup=markup)
                return
            except Exception:  # noqa: BLE001
                logging.debug("Не удалось обновить список районов, отправляем новое сообщение")
        await target_message.answer(AREA_PROMPT_MESSAGE, reply_markup=markup)

    async def set_station_context(
        state: FSMContext,
        area_summary: storage.AreaSummary,
        *,
        mode: str = "list",
        stations: Optional[Iterable[storage.StationSummary]] = None,
        page: int = 0,
        query: str = "",
    ) -> Dict[str, Any]:
        context = {
            "area_id": area_summary.area_id,
            "area_name": area_summary.area_name,
            "area_title": area_summary.title,
            "mode": mode,
            "stations": list(stations if stations is not None else area_summary.stations),
            "page": page,
            "query": query,
        }
        await state.update_data(worker_station=context)
        return context

    async def get_station_context(state: FSMContext) -> Dict[str, Any]:
        data = await state.get_data()
        context = data.get("worker_station")
        if not isinstance(context, dict):
            return {}
        stations = context.get("stations")
        if isinstance(stations, tuple):
            context["stations"] = list(stations)
        return context

    async def present_station_menu(
        target_message: types.Message,
        state: FSMContext,
        context: Dict[str, Any],
        *,
        via_edit: bool,
    ) -> None:
        stations = context.get("stations") or []
        area_name = context.get("area_name") or context.get("area_title") or ""
        if not stations:
            text = STATION_EMPTY_TEMPLATE.format(area_name=area_name or "выбранном районе")
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton(STATION_BACK_BUTTON_TEXT, callback_data="wstation_back_area"))
            if via_edit:
                try:
                    await target_message.edit_text(text, reply_markup=markup)
                    return
                except Exception:  # noqa: BLE001
                    logging.debug("Не удалось показать сообщение об отсутствии станций")
            await target_message.answer(text, reply_markup=markup)
            return
        markup, actual_page, _ = build_station_keyboard(
            stations, context.get("page", 0), show_reset=context.get("mode") == "search"
        )
        context["page"] = actual_page
        await state.update_data(worker_station=context)
        if context.get("mode") == "search" and context.get("query"):
            text = STATION_SEARCH_RESULTS_TEMPLATE.format(query=context["query"])
        else:
            text = STATION_PROMPT_TEMPLATE.format(area_name=area_name)
        if via_edit:
            try:
                await target_message.edit_text(text, reply_markup=markup)
                return
            except Exception:  # noqa: BLE001
                logging.debug("Не удалось обновить список станций, отправляем новое сообщение")
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
        markup, actual_page, _ = build_shop_keyboard(shops_list, context.get("page", 0))
        context["page"] = actual_page
        await state.update_data(worker_shop=context)
        text = (
            SHOP_LIST_TITLE_TEMPLATE.format(station=station)
            if shops_list
            else SHOP_EMPTY_TEMPLATE.format(station=station)
        )
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
        await state.set_state(WorkerStates.area.state)
        await present_area_menu(message, state, via_edit=False)

    @dispatcher.callback_query_handler(
        lambda c: c.data and c.data.startswith("warea:"),
        state=[WorkerStates.area, WorkerStates.metro, WorkerStates.shop],
    )
    async def worker_area_choice(call: CallbackQuery, state: FSMContext) -> None:
        try:
            _, area_id = (call.data or "").split(":", 1)
        except ValueError:
            await call.answer("Район недоступен", show_alert=True)
            return
        summary = storage.get_area_summary(area_id)
        if summary is None:
            await call.answer("Район недоступен", show_alert=True)
            await state.set_state(WorkerStates.area.state)
            await present_area_menu(call.message, state, via_edit=True)
            return
        await call.answer()
        await state.set_state(WorkerStates.metro.state)
        await state.update_data(
            shop_id=None,
            shop_name=None,
            chosen_metro=None,
            chosen_metro_dist_m=None,
        )
        context = await set_station_context(state, summary, mode="list", page=0)
        await present_station_menu(call.message, state, context, via_edit=True)

    @dispatcher.callback_query_handler(
        lambda c: c.data and c.data.startswith("wstation_page:"),
        state=WorkerStates.metro,
    )
    async def worker_station_page(call: CallbackQuery, state: FSMContext) -> None:
        context = await get_station_context(state)
        stations = context.get("stations") or []
        if not stations:
            await call.answer("Станции недоступны", show_alert=True)
            return
        try:
            requested_page = int((call.data or "").split(":", 1)[1])
        except (ValueError, IndexError):
            await call.answer("Некорректная страница", show_alert=True)
            return
        context["page"] = requested_page
        await state.update_data(worker_station=context)
        await call.answer()
        await present_station_menu(call.message, state, context, via_edit=True)

    @dispatcher.callback_query_handler(
        lambda c: c.data and c.data.startswith("wstation_pick:"),
        state=WorkerStates.metro,
    )
    async def worker_station_pick(call: CallbackQuery, state: FSMContext) -> None:
        context = await get_station_context(state)
        stations = context.get("stations") or []
        try:
            index = int((call.data or "").split(":", 1)[1])
        except (ValueError, IndexError):
            await call.answer("Некорректный выбор", show_alert=True)
            return
        if index < 0 or index >= len(stations):
            await call.answer("Станция не найдена", show_alert=True)
            return
        summary = stations[index]
        await call.answer()
        locations = storage.get_station_shops(summary.name)
        shops = [
            {"id": location.shop_id, "name": location.shop_name, "distance": location.distance_m}
            for location in locations
        ]
        await state.update_data(chosen_metro=summary.name, chosen_metro_dist_m=None)
        shop_context = await set_shop_context(state, summary.name, shops, page=0)
        await state.set_state(WorkerStates.shop.state)
        await present_shop_menu(call.message, state, shop_context, via_edit=True)

    @dispatcher.callback_query_handler(
        lambda c: c.data == "wstation_search",
        state=WorkerStates.metro,
    )
    async def worker_station_search(call: CallbackQuery, state: FSMContext) -> None:
        await call.answer()
        await state.set_state(WorkerStates.metro_search.state)
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton(STATION_RESET_BUTTON_TEXT, callback_data="wstation_reset"))
        markup.add(InlineKeyboardButton(STATION_BACK_BUTTON_TEXT, callback_data="wstation_back_area"))
        try:
            await call.message.edit_text(STATION_SEARCH_PROMPT, reply_markup=markup)
        except Exception:  # noqa: BLE001
            await call.message.answer(STATION_SEARCH_PROMPT, reply_markup=markup)

    @dispatcher.callback_query_handler(
        lambda c: c.data == "wstation_reset",
        state=[WorkerStates.metro, WorkerStates.metro_search],
    )
    async def worker_station_reset(call: CallbackQuery, state: FSMContext) -> None:
        context = await get_station_context(state)
        area_id = context.get("area_id")
        if not area_id:
            await call.answer("Сначала выбери район", show_alert=True)
            return
        summary = storage.get_area_summary(area_id)
        if summary is None:
            await call.answer("Район недоступен", show_alert=True)
            await state.set_state(WorkerStates.area.state)
            await present_area_menu(call.message, state, via_edit=True)
            return
        await state.set_state(WorkerStates.metro.state)
        await call.answer()
        context = await set_station_context(state, summary, mode="list", page=0)
        await present_station_menu(call.message, state, context, via_edit=True)

    @dispatcher.callback_query_handler(
        lambda c: c.data == "wstation_back_area",
        state=[WorkerStates.area, WorkerStates.metro, WorkerStates.metro_search, WorkerStates.shop],
    )
    async def worker_station_back_area(call: CallbackQuery, state: FSMContext) -> None:
        await call.answer()
        await state.set_state(WorkerStates.area.state)
        await state.set_data({})
        await present_area_menu(call.message, state, via_edit=True)

    @dispatcher.message_handler(state=WorkerStates.metro_search)
    async def worker_metro_search_input(message: types.Message, state: FSMContext) -> None:
        query = (message.text or "").strip()
        if not query:
            await message.answer("Введи название станции метро.")
            return
        if query.lower() == BACK_COMMAND.lower():
            await state.set_state(WorkerStates.metro.state)
            context = await get_station_context(state)
            if not context:
                await state.set_state(WorkerStates.area.state)
                await present_area_menu(message, state, via_edit=False)
                return
            await present_station_menu(message, state, context, via_edit=False)
            return
        context = await get_station_context(state)
        area_id = context.get("area_id")
        if not area_id:
            await message.answer("Сначала выбери район.")
            await state.set_state(WorkerStates.area.state)
            await present_area_menu(message, state, via_edit=False)
            return
        results = storage.search_stations(query, limit=MAX_SEARCH_RESULTS)
        if not results:
            await message.answer(
                f"Станций по запросу «{query}» не нашли. Попробуй другое название."
            )
            return
        summary = storage.get_area_summary(area_id)
        if summary is None:
            await message.answer("Район недоступен, выбери заново.")
            await state.set_state(WorkerStates.area.state)
            await present_area_menu(message, state, via_edit=False)
            return
        await state.set_state(WorkerStates.metro.state)
        context = await set_station_context(
            state,
            summary,
            mode="search",
            stations=results,
            page=0,
            query=query,
        )
        await present_station_menu(message, state, context, via_edit=False)

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
            requested_page = int((call.data or "").split(":", 1)[1])
        except (ValueError, IndexError):
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
            index = int((call.data or "").split(":", 1)[1])
        except (ValueError, IndexError):
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
        context = await get_station_context(state)
        if not context:
            await state.set_state(WorkerStates.area.state)
            await present_area_menu(call.message, state, via_edit=True)
            return
        await present_station_menu(call.message, state, context, via_edit=True)

    @dispatcher.callback_query_handler(
        lambda c: c.data == "wshop_reset",
        state=[WorkerStates.metro, WorkerStates.shop],
    )
    async def worker_shop_reset(call: CallbackQuery, state: FSMContext) -> None:
        await call.answer()
        await state.set_state(WorkerStates.area.state)
        await state.set_data({})
        await present_area_menu(call.message, state, via_edit=True)

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
        await present_position_step(message, state, "worker", via_edit=False)

    @dispatcher.callback_query_handler(
        lambda c: c.data and c.data.startswith("worker_position:"),
        state=WorkerStates.position,
    )
    async def worker_position_choice(call: CallbackQuery, state: FSMContext) -> None:
        try:
            _, action = (call.data or "").split(":", 1)
        except ValueError:
            await call.answer("Некорректный выбор", show_alert=True)
            return
        if action == "back":
            await call.answer()
            try:
                await call.message.edit_reply_markup()
            except Exception:  # noqa: BLE001
                pass
            await prompt_time_range(call.message, state, "worker")
            return
        if action == "custom":
            await call.answer()
            await call.message.answer(POSITION_CUSTOM_PROMPT)
            return
        try:
            index = int(action)
        except ValueError:
            await call.answer("Некорректный выбор", show_alert=True)
            return
        if index < 0 or index >= len(POSITION_BUTTON_OPTIONS):
            await call.answer("Некорректный выбор", show_alert=True)
            return
        position_value = POSITION_BUTTON_OPTIONS[index]
        await state.update_data(position=position_value)
        await call.answer("Готово")
        await proceed_to_note_step("worker", call.message, state, via_edit=True)

    @dispatcher.message_handler(state=WorkerStates.position)
    async def worker_position_input(message: types.Message, state: FSMContext) -> None:
        text = (message.text or "").strip()
        if not text:
            await message.answer(POSITION_CUSTOM_PROMPT)
            return
        if text.lower() == BACK_COMMAND.lower():
            await prompt_time_range(message, state, "worker")
            return
        normalized, error = normalize_position_input(text)
        if error:
            await message.answer(f"{error}\n{POSITION_CUSTOM_PROMPT}")
            return
        await state.update_data(position=normalized)
        await proceed_to_note_step("worker", message, state, via_edit=False)

    @dispatcher.message_handler(state=WorkerStates.note)
    async def worker_note(message: types.Message, state: FSMContext) -> None:
        if (message.text or "").strip().lower() == BACK_COMMAND.lower():
            await present_position_step(message, state, "worker", via_edit=False)
            return
        await state.update_data(note=(message.text or "").strip())
        data = await state.get_data()
        position_value = (data.get("position") or "").strip()
        if not position_value:
            await message.answer("Сначала выберите должность.")
            await present_position_step(
                message,
                state,
                "worker",
                via_edit=False,
                reminder="Это обязательное поле.",
            )
            return
        note_value = (data.get("note") or "").strip()
        shops = await fetch_shops()
        selected_shop = shops.get(data.get("shop_id")) if data.get("shop_id") is not None else None
        shop_name = data.get("shop_name") or (selected_shop.name if selected_shop else "Любая лавка")
        station = data.get("chosen_metro") or "Не выбрано"
        distance = data.get("chosen_metro_dist_m")
        metro_line = (
            f"Метро: {station} · {distance} м" if distance is not None else f"Метро: {station}"
        )
        note_display = note_value if note_value else "—"
        summary = (
            "Проверьте заявку:\n"
            f"Дата: {data['date']}\n"
            f"Смена: {data['time_from']}–{data['time_to']}\n"
            f"Лавка: {shop_name}\n"
            f"{metro_line}\n"
            f"Желаемая должность: {position_value}\n"
            f"Пожелания: {note_display}"
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
        data = await state.get_data()
        position_value = (data.get("position") or "").strip()
        if not position_value:
            await call.answer("Укажите должность", show_alert=True)
            await present_position_step(
                call.message,
                state,
                "worker",
                via_edit=True,
                reminder="Это обязательное поле.",
            )
            return
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
    global requests_cleanup_task
    if requests_cleanup_task is None:
        requests_cleanup_task = asyncio.create_task(periodic_requests_cleanup())
        logging.info(
            "Запущена автоматическая очистка заявок каждые %s секунд",
            REQUESTS_CLEANUP_INTERVAL_SECONDS,
        )
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
    global requests_cleanup_task
    if requests_cleanup_task:
        requests_cleanup_task.cancel()
        try:
            await requests_cleanup_task
        except asyncio.CancelledError:
            pass
        requests_cleanup_task = None
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
    dp.register_callback_query_handler(
        on_disabled_callback, lambda c: c.data == DISABLED_CALLBACK_DATA
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
