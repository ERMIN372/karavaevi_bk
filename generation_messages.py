"""Utilities for building user-facing messages for media generation queues."""

from __future__ import annotations

from typing import Dict

# Mapping between normalized asset kinds and the noun that should be used in the
# message. The values are written in lowercase so that the resulting string fits
# seamlessly into the final sentence.
GENERATION_ASSET_LABELS: Dict[str, str] = {
    "video": "видео",
    "sora": "видео",
    "vid": "видео",
    "photo": "фото",
    "image": "фото",
    "img": "фото",
}


def format_generation_asset_label(asset_kind: str) -> str:
    """Return a human-friendly noun for the requested generation asset."""

    normalized = (asset_kind or "").strip().lower()
    return GENERATION_ASSET_LABELS.get(normalized, "результат")


def build_generation_queue_message(queue_position: int, asset_kind: str) -> str:
    """Compose a queue notification message for the requested asset kind."""

    label = format_generation_asset_label(asset_kind)
    return (
        "🧾 Заказ принят. "
        f"Место в очереди: {queue_position}. "
        f"Пришлём {label} сюда."
    )

