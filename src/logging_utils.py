from __future__ import annotations

import logging
from pathlib import Path

from src.config import get_settings


def configure_logging() -> None:
    settings = get_settings()
    log_path = Path(settings.log_file_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger()
    if root_logger.handlers:
        root_logger.setLevel(settings.log_level)
        return

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    root_logger.setLevel(settings.log_level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(stream_handler)
