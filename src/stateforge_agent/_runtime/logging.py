"""Configure process logging from validated Agent settings."""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime

from stateforge_agent._config.models import LoggingConfiguration


class _JsonFormatter(logging.Formatter):
    """Format one log record as a compact JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        """Serialize a log record without exposing arbitrary internals."""
        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(
                record.created,
                tz=UTC,
            ).isoformat(),
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info is not None:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def configure_logging(configuration: LoggingConfiguration) -> None:
    """Replace process handlers with the configured stderr handler."""
    handler = logging.StreamHandler(sys.stderr)
    if configuration.format == "json":
        handler.setFormatter(_JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S%z",
            )
        )

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(configuration.level.upper())
