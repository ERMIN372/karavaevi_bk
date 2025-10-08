"""Google Sheets storage helpers for the Караваевы бот."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional, Tuple

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
    "first_name",
    "last_name",
    "updated_at",
]

SHOPS_HEADERS = [
    "id",
    "name",
    "is_active",
]


_client = None
_spreadsheet = None
_requests_ws = None
_users_ws = None
_shops_ws = None
SHOPS_CACHE: Dict[int, str] = {}


def _decode_service_account() -> Dict[str, Any]:
    if not SERVICE_ACCOUNT_B64:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON_BASE64 is required")
    try:
        decoded = base64.b64decode(SERVICE_ACCOUNT_B64)
        return json.loads(decoded)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Failed to decode GOOGLE_SERVICE_ACCOUNT_JSON_BASE64") from exc


def _ensure_initialized() -> None:
    global _client, _spreadsheet, _requests_ws, _users_ws, _shops_ws, SHOPS_CACHE
    
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
    
    SHOPS_CACHE = _load_shops_cache()


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


def _load_shops_cache() -> Dict[int, str]:
    if _shops_ws is None:
        raise RuntimeError("Shops worksheet not initialized")
    values = _shops_ws.get_all_records(expected_headers=SHOPS_HEADERS)
    cache: Dict[int, str] = {}
    for idx, row in enumerate(values, start=2):
        raw_id = row.get("id") or ""
        try:
            shop_id = int(raw_id)
        except (TypeError, ValueError):
            shop_id = idx - 1
        name = (row.get("name") or "").strip()
        active = str(row.get("is_active") or "1").strip()
        if not name:
            continue
        if active.lower() in {"0", "false", "нет"}:
            continue
        cache[shop_id] = name
    if not cache:
        LOGGER.warning("Shops sheet is empty – no options will be available")
    return cache


def get_shops() -> Dict[int, str]:
    """Return cached shops mapping."""
    _ensure_initialized()
    return dict(SHOPS_CACHE)


def get_shop_name(shop_id: Optional[int]) -> Optional[str]:
    if shop_id is None:
        return None
    return SHOPS_CACHE.get(shop_id)


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
        cell = _requests_ws.find(str(request_id))
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
    if channel_message_id is not None:
        channel_cell = rowcol_to_a1(cell.row, REQUESTS_COLUMNS["channel_message_id"])
        updates.append(
            {
                "range": channel_cell,
                "values": [[str(channel_message_id)]],
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
        cell = _requests_ws.find(str(request_id))
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
    if data.get("author_id"):
        data["author_id"] = int(data["author_id"])
    if data.get("channel_message_id"):
        try:
            data["channel_message_id"] = int(data["channel_message_id"])
        except ValueError:
            data["channel_message_id"] = None
    if not data.get("shop_name") and data.get("shop_id"):
        data["shop_name"] = SHOPS_CACHE.get(data["shop_id"])
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
    payload = {
        "id": user_id,
        "role": user.get("role", "worker"),
        "username": user.get("username") or "",
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


__all__ = [
    "gs_append_request",
    "gs_update_request_status",
    "gs_find_request",
    "gs_ensure_user",
    "get_shops",
    "get_shop_name",
]

