"""Unit tests for the generation queue message helpers."""

import unittest

from generation_messages import build_generation_queue_message


class GenerationQueueMessageTests(unittest.TestCase):
    def test_photo_queue_message_mentions_photo(self) -> None:
        message = build_generation_queue_message(1, "photo")
        self.assertIn("фото", message)
        self.assertNotIn("видео", message)

    def test_video_queue_message_mentions_video(self) -> None:
        message = build_generation_queue_message(3, "video")
        self.assertIn("видео", message)

    def test_unknown_kind_falls_back_to_generic_word(self) -> None:
        message = build_generation_queue_message(2, "document")
        self.assertIn("результат", message)


if __name__ == "__main__":  # pragma: no cover - convenience entry point
    unittest.main()

