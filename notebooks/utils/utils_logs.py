import logging
import os
from pathlib import Path
from typing import Literal
import inspect

LOG_FOLDER_PATH = "/logs"
LOG_FILE_NAME = "app.log"
LOGLEVEL = Literal["debug", "info", "warning", "error", "critical"]

def log_message(
    level: LOGLEVEL,
    msg_log: str,
    file_log: bool = False,
    console_log: bool = True,
    debug_mode: bool = False
) -> None:
    """
    Log a message with dynamic configuration per call.

    This function logs a message at the specified severity level and
    dynamically configures logging handlers on each invocation. Depending
    on the provided flags, the message can be sent to the console, a log
    file, or both.

    When `debug_mode` is enabled, the logger and handlers are set to DEBUG
    level and the caller's file name and line number are appended to the
    log message.

    Parameters:
        `level` (LOGLEVEL): Log severity level ("debug", "info", "warning", "error", "critical").
        `msg_log` (str): The message to be logged.
        `file_log` (bool): If True, log the message to a file located at LOG_FOLDER_PATH / LOG_FILE_NAME.
        `console_log` (bool): If True, log the message to the console.
        `debug_mode` (bool): If True, enable DEBUG level logging and append caller file and line information to the message.

    Returns:
        None
    """
    if file_log or console_log:
        level = level.lower()
        logger = logging.getLogger("app_logger")
        # --- Update logger level
        logger.setLevel(logging.DEBUG if debug_mode else logging.INFO)

        # === REMOVE existing handlers (important!)
        logger.handlers.clear()
        formatter = logging.Formatter(
            "%(asctime)s - [%(levelname)s] - %(message)s"
        )

        # === Console log
        if console_log:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        # === File log
        if file_log:
            Path(LOG_FOLDER_PATH).mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(os.path.join(LOG_FOLDER_PATH, LOG_FILE_NAME))
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        # --- Update handler levels every call
        for handler in logger.handlers:
            handler.setLevel(logging.DEBUG if debug_mode else logging.INFO)

        # --- Add caller info manually if debug_mode
        if debug_mode:
            frame = inspect.stack()[1]  # caller of log_message
            caller_file = os.path.basename(frame.filename)
            caller_line = frame.lineno
            msg_log = f"{msg_log} - {caller_file}:{caller_line}"

        # --- Dispatch log
        if level == "debug":
            logger.debug(msg_log)
        elif level == "info":
            logger.info(msg_log)
        elif level == "warning":
            logger.warning(msg_log)
        elif level == "error":
            logger.error(msg_log)
        elif level == "critical":
            logger.critical(msg_log)
        else:
            logger.warning(f"Invalid log level '{level}', defaulting to INFO.")
            logger.info(msg_log)
    #else: # => no logging: warning


