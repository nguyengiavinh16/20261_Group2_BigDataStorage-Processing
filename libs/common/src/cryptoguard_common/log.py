"""JSON-lines logging to stdout (collected by Loki/kubectl logs)."""

import json
import logging
import sys

from cryptoguard_common.timeutil import now_us, us_to_iso


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": us_to_iso(now_us()),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        extra = getattr(record, "extra_fields", None)
        if extra:
            payload.update(extra)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(level)
        logger.propagate = False
    return logger


def log_kv(logger: logging.Logger, level: int, msg: str, **fields) -> None:
    """logger.info with structured fields: log_kv(log, logging.INFO, 'reconnect', exchange='binance')."""
    logger.log(level, msg, extra={"extra_fields": fields})
