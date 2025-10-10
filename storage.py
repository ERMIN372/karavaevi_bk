"""Google Sheets storage helpers for the Караваевы бот."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import RLock
from typing import Any, Dict, Iterable, List, Optional, Tuple

import gspread
from gspread.utils import rowcol_to_a1
from google.oauth2.service_account import Credentials


LOGGER = logging.getLogger(__name__)

SERVICE_ACCOUNT_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_BASE64")
SPREADSHEET_ID = os.getenv("GOOGLE_SPREADSHEET_ID")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

REQUESTS_SHEET = "Requests"
USERS_SHEET = "Users"
SHOPS_SHEET = "Shops"

REQUESTS_HEADERS = [
    "id",
    "kind",
    "date",
    "time_from",
    "time_to",
    "shop_id",
    "shop_name",
    "chosen_metro",
    "chosen_metro_dist_m",
    "note",
    "author_id",
    "status",
    "created_at",
    "updated_at",
    "channel_message_id",
]

USERS_HEADERS = [
    "id",
    "role",
    "username",
    "phone_number",
    "first_name",
    "last_name",
    "updated_at",
]

SHOPS_HEADERS = [
    "id",
    "name",
    "is_active",
    "metro_1",
    "dist_1_m",
    "metro_2",
    "dist_2_m",
    "metro_3",
    "dist_3_m",
]


@dataclass(frozen=True)
class ShopMetro:
    name: str
    distance_m: int


@dataclass(frozen=True)
class ShopRecord:
    id: int
    name: str
    metros: Tuple[ShopMetro, ...]
    is_active: bool = True


@dataclass(frozen=True)
class ShopLocation:
    shop_id: int
    shop_name: str
    distance_m: int


_client = None
_spreadsheet = None
_requests_ws = None
_users_ws = None
_shops_ws = None
SHOPS_CACHE: Dict[int, ShopRecord] = {}
METRO_CACHE: Dict[str, Tuple[ShopLocation, ...]] = {}
STATIONS_CACHE: Tuple[str, ...] = ()
SHOPS_CACHE_UPDATED_AT: Optional[datetime] = None
_SHOPS_LOCK = RLock()


def _decode_service_account() -> Dict[str, Any]:
    if not SERVICE_ACCOUNT_B64:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON_BASE64 is required")
    try:
        decoded = base64.b64decode(SERVICE_ACCOUNT_B64)
        return json.loads(decoded)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Failed to decode GOOGLE_SERVICE_ACCOUNT_JSON_BASE64") from exc


def _ensure_initialized() -> None:
    global _client, _spreadsheet, _requests_ws, _users_ws, _shops_ws

    if _client is not None:
        return
    
    if not SPREADSHEET_ID:
        raise RuntimeError("GOOGLE_SPREADSHEET_ID is required")
    
    _credentials_info = _decode_service_account()
    _credentials = Credentials.from_service_account_info(_credentials_info, scopes=SCOPES)
    _client = gspread.authorize(_credentials)
    _spreadsheet = _client.open_by_key(SPREADSHEET_ID)
    
    _requests_ws = _get_or_create_worksheet(REQUESTS_SHEET)
    _ensure_headers(_requests_ws, REQUESTS_HEADERS)
    
    _users_ws = _get_or_create_worksheet(USERS_SHEET)
    _ensure_headers(_users_ws, USERS_HEADERS)
    
    _shops_ws = _get_or_create_worksheet(SHOPS_SHEET)
    _ensure_headers(_shops_ws, SHOPS_HEADERS)

    _load_shops_cache()


def _get_or_create_worksheet(title: str) -> gspread.Worksheet:
    if _spreadsheet is None:
        raise RuntimeError("Spreadsheet not initialized")
    try:
        return _spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        LOGGER.info("Worksheet %s not found, creating", title)
        return _spreadsheet.add_worksheet(title=title, rows=1000, cols=26)


def _ensure_headers(ws: gspread.Worksheet, headers: Iterable[str]) -> None:
    headers = list(headers)
    current = ws.row_values(1)
    if current[: len(headers)] != headers:
        ws.update("A1", [headers])


def _column_letter(index: int) -> str:
    index = int(index)
    if index < 1:
        raise ValueError("Column index must be >= 1")
    result = ""
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        result = chr(65 + remainder) + result
    return result


REQUESTS_COLUMNS = {name: idx + 1 for idx, name in enumerate(REQUESTS_HEADERS)}
USERS_COLUMNS = {name: idx + 1 for idx, name in enumerate(USERS_HEADERS)}


def _parse_bool(value: Any) -> bool:
    text = str(value or "1").strip().lower()
    if not text:
        return True
    return text not in {"0", "false", "нет", "no"}


def _parse_distance(value: Any, *, row_number: int, column: str, shop_name: str) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        distance = int(float(value))
    else:
        text = str(value).strip()
        if not text:
            return None
        normalized = text.lower().replace("м", "").replace("\u00a0", " ").strip()
        normalized = normalized.replace(",", ".")
        try:
            distance = float(normalized)
        except ValueError:
            LOGGER.warning(
                "Не удалось распарсить расстояние '%s' для лавки '%s' (строка %s, колонка %s)",
                text,
                shop_name,
                row_number,
                column,
            )
            return None
        distance = int(distance)
    if distance < 0:
        LOGGER.warning(
            "Отрицательное расстояние %s для лавки '%s' (строка %s, колонка %s) пропущено",
            distance,
            shop_name,
            row_number,
            column,
        )
        return None
    return distance


def _load_shops_cache() -> None:
    global SHOPS_CACHE, METRO_CACHE, STATIONS_CACHE, SHOPS_CACHE_UPDATED_AT

    if _shops_ws is None:
        raise RuntimeError("Shops worksheet not initialized")

    values = _shops_ws.get_all_records(expected_headers=SHOPS_HEADERS)
    shops: Dict[int, ShopRecord] = {}
    metro_map: Dict[str, Dict[int, int]] = {}

    for idx, row in enumerate(values, start=2):
        raw_id = row.get("id")
        name = (row.get("name") or "").strip()
        if not name:
            continue
        try:
            shop_id = int(raw_id) if raw_id not in (None, "") else idx - 1
        except (TypeError, ValueError):
            LOGGER.warning(
                "Некорректный идентификатор лавки '%s' в строке %s. Будет использован порядковый номер.",
                raw_id,
                idx,
            )
            shop_id = idx - 1

        is_active = _parse_bool(row.get("is_active"))
        metros: List[ShopMetro] = []
        for suffix in ("1", "2", "3"):
            station = (row.get(f"metro_{suffix}") or "").strip()
            distance_value = row.get(f"dist_{suffix}_m")
            if not station:
                continue
            distance = _parse_distance(
                distance_value,
                row_number=idx,
                column=f"dist_{suffix}_m",
                shop_name=name,
            )
            if distance is None:
                continue
            metros.append(ShopMetro(name=station, distance_m=distance))
            metro_map.setdefault(station, {})
            current = metro_map[station].get(shop_id)
            if current is None or distance < current:
                metro_map[station][shop_id] = distance

        record = ShopRecord(id=shop_id, name=name, metros=tuple(metros), is_active=is_active)
        shops[shop_id] = record

    if not shops:
        LOGGER.warning("Shops sheet is empty – no options will be available")

    metro_cache: Dict[str, Tuple[ShopLocation, ...]] = {}
    for station, per_shop in metro_map.items():
        locations = [
            ShopLocation(shop_id=s_id, shop_name=shops[s_id].name, distance_m=dist)
            for s_id, dist in per_shop.items()
            if shops.get(s_id) and shops[s_id].is_active
        ]
        if not locations:
            continue
        locations.sort(key=lambda item: (item.distance_m, item.shop_name.lower()))
        metro_cache[station] = tuple(locations)

    stations = tuple(sorted(metro_cache.keys(), key=lambda value: value.lower()))

    with _SHOPS_LOCK:
        SHOPS_CACHE = shops
        METRO_CACHE = metro_cache
        STATIONS_CACHE = stations
        SHOPS_CACHE_UPDATED_AT = datetime.now(timezone.utc)


def get_shops() -> Dict[int, ShopRecord]:
    """Return cached shops mapping."""

    _ensure_initialized()
    with _SHOPS_LOCK:
        return {
            shop_id: record
            for shop_id, record in SHOPS_CACHE.items()
            if record.is_active
        }


def get_shop_name(shop_id: Optional[int]) -> Optional[str]:
    if shop_id is None:
        return None
    _ensure_initialized()
    with _SHOPS_LOCK:
        record = SHOPS_CACHE.get(shop_id)
        return record.name if record else None


def get_station_names() -> Tuple[str, ...]:
    _ensure_initialized()
    with _SHOPS_LOCK:
        return STATIONS_CACHE


def get_station_shops(station: str) -> Tuple[ShopLocation, ...]:
    _ensure_initialized()
    with _SHOPS_LOCK:
        return METRO_CACHE.get(station, tuple())


def get_shops_updated_at() -> Optional[datetime]:
    _ensure_initialized()
    with _SHOPS_LOCK:
        return SHOPS_CACHE_UPDATED_AT


def _refresh_shops_cache_sync() -> None:
    _ensure_initialized()
    _load_shops_cache()


async def refresh_shops_cache() -> None:
    await asyncio.to_thread(_refresh_shops_cache_sync)


def _next_request_id(ids: Iterable[str]) -> Tuple[int, int]:
    max_id = 0
    count = 0
    for count, value in enumerate(ids, start=1):
        try:
            current_id = int(value)
        except (TypeError, ValueError):
            continue
        max_id = max(max_id, current_id)
    return max_id + 1, count + 1


def _append_request_sync(payload: Dict[str, Any]) -> Tuple[int, int]:
    _ensure_initialized()
    now = datetime.now(timezone.utc).isoformat()
    col_values = _requests_ws.col_values(REQUESTS_COLUMNS["id"])[1:]
    request_id, row_number = _next_request_id(col_values)
    row = [
        str(request_id),
        payload.get("kind", ""),
        payload.get("date", ""),
        payload.get("time_from", ""),
        payload.get("time_to", ""),
        str(payload.get("shop_id") or ""),
        payload.get("shop_name", ""),
        payload.get("chosen_metro", ""),
        str(payload.get("chosen_metro_dist_m") or ""),
        payload.get("note", ""),
        str(payload.get("author_id") or ""),
        payload.get("status", "open"),
        payload.get("created_at", now),
        payload.get("updated_at", now),
        str(payload.get("channel_message_id") or ""),
    ]
    _requests_ws.append_row(row, value_input_option="USER_ENTERED")
    LOGGER.info("Appended request %s to Google Sheets", request_id)
    return request_id, row_number + 1


async def gs_append_request(payload: Dict[str, Any]) -> Tuple[int, int]:
    return await asyncio.to_thread(_append_request_sync, payload)


def _update_request_status_sync(
    request_id: int, status: str, channel_message_id: Optional[int]
) -> None:
    _ensure_initialized()
    try:
        cell = _requests_ws.find(
            str(request_id), in_column=REQUESTS_COLUMNS["id"]
        )
    except gspread.exceptions.CellNotFound:
        raise KeyError(f"Request {request_id} not found")

    updates = []
    status_cell = rowcol_to_a1(cell.row, REQUESTS_COLUMNS["status"])
    updates.append(
        {
            "range": status_cell,
            "values": [[status]],
        }
    )
    updated_at_cell = rowcol_to_a1(cell.row, REQUESTS_COLUMNS["updated_at"])
    updates.append(
        {
            "range": updated_at_cell,
            "values": [[datetime.now(timezone.utc).isoformat()]],
        }
    )
    channel_cell = rowcol_to_a1(cell.row, REQUESTS_COLUMNS["channel_message_id"])
    updates.append(
        {
            "range": channel_cell,
            "values": [["" if channel_message_id is None else str(channel_message_id)]],
        }
    )
    _requests_ws.batch_update(updates)
    LOGGER.info("Updated request %s status to %s", request_id, status)


async def gs_update_request_status(
    request_id: int, status: str, channel_message_id: Optional[int] = None
) -> None:
    await asyncio.to_thread(_update_request_status_sync, request_id, status, channel_message_id)


def _find_request_sync(request_id: int) -> Optional[Dict[str, Any]]:
    _ensure_initialized()
    try:
        cell = _requests_ws.find(
            str(request_id), in_column=REQUESTS_COLUMNS["id"]
        )
    except gspread.exceptions.CellNotFound:
        return None
    row_values = _requests_ws.row_values(cell.row)
    data: Dict[str, Any] = {}
    for index, header in enumerate(REQUESTS_HEADERS):
        data[header] = row_values[index] if index < len(row_values) else ""
    if data.get("id"):
        data["id"] = int(data["id"])
    if data.get("shop_id"):
        try:
            data["shop_id"] = int(data["shop_id"])
        except ValueError:
            data["shop_id"] = None
    else:
        data["shop_id"] = None
    if data.get("chosen_metro_dist_m"):
        try:
            data["chosen_metro_dist_m"] = int(float(data["chosen_metro_dist_m"]))
        except ValueError:
            data["chosen_metro_dist_m"] = None
    else:
        data["chosen_metro_dist_m"] = None
    if data.get("author_id"):
        data["author_id"] = int(data["author_id"])
    if data.get("channel_message_id"):
        try:
            data["channel_message_id"] = int(data["channel_message_id"])
        except ValueError:
            data["channel_message_id"] = None
    if not data.get("shop_name") and data.get("shop_id"):
        record = SHOPS_CACHE.get(data["shop_id"])
        if record:
            data["shop_name"] = record.name
    return data


async def gs_find_request(request_id: int) -> Optional[Dict[str, Any]]:
    return await asyncio.to_thread(_find_request_sync, request_id)


def _ensure_user_sync(user: Dict[str, Any]) -> None:
    _ensure_initialized()
    if not user or "id" not in user:
        raise ValueError("User payload must include id")
    user_id = str(user.get("id"))
    try:
        cell = _users_ws.find(user_id)
    except gspread.exceptions.CellNotFound:
        cell = None
    row_index = cell.row if cell else len(_users_ws.get_all_values()) + 1
    existing_row: Dict[str, Any] = {}
    if cell:
        current_values = _users_ws.row_values(row_index)
        existing_row = {
            header: current_values[idx] if idx < len(current_values) else ""
            for idx, header in enumerate(USERS_HEADERS)
        }
    payload = {
        "id": user_id,
        "role": user.get("role", "worker"),
        "username": user.get("username") or existing_row.get("username") or "",
        "phone_number": user.get("phone_number")
        or existing_row.get("phone_number")
        or "",
        "first_name": user.get("first_name") or "",
        "last_name": user.get("last_name") or "",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    row_values = [payload.get(header, "") for header in USERS_HEADERS]
    end_column = _column_letter(len(USERS_HEADERS))
    target_range = f"A{row_index}:{end_column}{row_index}"
    _users_ws.batch_update([
        {
            "range": target_range,
            "values": [row_values],
        }
    ])
    LOGGER.debug("Ensured user %s in sheet", user_id)


async def gs_ensure_user(user: Dict[str, Any]) -> None:
    await asyncio.to_thread(_ensure_user_sync, user)


def _get_user_sync(user_id: int) -> Optional[Dict[str, Any]]:
    _ensure_initialized()
    try:
        cell = _users_ws.find(str(user_id))
    except gspread.exceptions.CellNotFound:
        return None
    if cell is None:
        return None
    row_values = _users_ws.row_values(cell.row)
    data: Dict[str, Any] = {}
    for index, header in enumerate(USERS_HEADERS):
        data[header] = row_values[index] if index < len(row_values) else ""
    if data.get("id"):
        try:
            data["id"] = int(data["id"])
        except ValueError:
            data["id"] = user_id
    if data.get("phone_number"):
        data["phone_number"] = str(data["phone_number"]).strip()
    if data.get("username"):
        data["username"] = str(data["username"]).strip()
    return data


async def gs_get_user(user_id: int) -> Optional[Dict[str, Any]]:
    return await asyncio.to_thread(_get_user_sync, user_id)


__all__ = [
    "gs_append_request",
    "gs_update_request_status",
    "gs_find_request",
    "gs_ensure_user",
    "gs_get_user",
    "get_shops",
    "get_shop_name",
]

