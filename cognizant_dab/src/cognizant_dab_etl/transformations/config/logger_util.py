"""
Lightweight logging utility for the ETL pipeline.

Provides a singleton-style logger that can be imported and reused across
all pipeline modules.  Every public method wraps the call in a try/except
so that a logging failure never breaks the pipeline itself.
"""

import logging
import sys


class PipelineLogger:
    """
    Thread-safe singleton logger wrapper.

    Usage::

        from transformations.config.logger_util import PipelineLogger

        logger = PipelineLogger(__name__)
        logger.info("Ingestion started")
    """

    _instances = {}

    def __new__(cls, name: str = "ETL_Pipeline"):
        # One logger instance per name – avoids duplicate handler pollution
        if name not in cls._instances:
            instance = super().__new__(cls)
            instance._init_logger(name)
            cls._instances[name] = instance
        return cls._instances[name]

    def _init_logger(self, name: str) -> None:
        """Configure the underlying Python logger exactly once."""
        self._logger = logging.getLogger(name)
        self._logger.setLevel(logging.INFO)

        # Prevent duplicate handlers on re-instantiation
        if not self._logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(logging.INFO)
            formatter = logging.Formatter(
                fmt="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            handler.setFormatter(formatter)
            self._logger.addHandler(handler)

    # ------------------------------------------------------------------
    # Public helper methods – each guarded so logging never raises
    # ------------------------------------------------------------------

    def info(self, message: str) -> None:
        try:
            self._logger.info(message)
        except Exception:
            pass  # logging must never break the pipeline

    def warning(self, message: str) -> None:
        try:
            self._logger.warning(message)
        except Exception:
            pass

    def error(self, message: str) -> None:
        try:
            self._logger.error(message)
        except Exception:
            pass

    def log_exception(self, message: str, exc: Exception) -> None:
        """Log an error message together with the exception traceback."""
        try:
            self._logger.error(f"{message}: {exc}", exc_info=True)
        except Exception:
            pass

    def log_with_context(self, level: str, message: str, context: dict = None) -> None:
        """Log a message with an optional structured context dictionary."""
        try:
            context_str = ""
            if context:
                context_str = " | " + " | ".join(f"{k}={v}" for k, v in context.items())
            full_message = f"{message}{context_str}"
            level_upper = level.upper()
            if level_upper == "INFO":
                self._logger.info(full_message)
            elif level_upper == "WARNING":
                self._logger.warning(full_message)
            elif level_upper == "ERROR":
                self._logger.error(full_message)
            else:
                self._logger.info(full_message)
        except Exception:
            pass
