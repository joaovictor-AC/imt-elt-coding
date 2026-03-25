import logging
import json
from datetime import datetime, timezone


class JSONFormatter(logging.Formatter):
    """Custom formatter that outputs logs as JSON."""

    def format(self, record):
        """Convert a log record to JSON."""
        # dictionary with log details
        log_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "module": record.module,
            "function": record.funcName,
            "message": record.getMessage(),
        }

        if record.exc_info:
            log_data["exception"] = str(record.exc_info[1])

        return json.dumps(log_data)


def get_logger(name):
    """Get a logger with JSON formatting."""
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler()

        handler.setFormatter(JSONFormatter())

        logger.addHandler(handler)

        logger.setLevel(logging.INFO)

    return logger
