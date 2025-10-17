from __future__ import annotations

import sys

import loguru
from loguru import logger

from .settings import ProjectSettings


def patching(record: loguru.Record):
    pass


def configure(settings: ProjectSettings, /, purge_existing_settings: bool = True):
    global logger

    configurations = {
        "handlers": [
            {
                "sink": sys.stdout,
                "serialize": settings.is_production,
                "colorize": not settings.is_production,
                "level": settings.logging.log_level,
            },
        ],
        "extra": {},
    }
    if purge_existing_settings:
        logger.remove(0)

    logger = logger.patch(patching)
    logger.configure(**configurations)
