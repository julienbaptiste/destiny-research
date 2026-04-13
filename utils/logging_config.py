"""
utils/logging_config.py
-----------------------
Centralized logging setup for all Destiny Research CLI scripts.

Design decisions:
- Single StreamHandler on stderr: keeps stdout clean for any piped output,
  and ensures errors/warnings are naturally separated from data output.
- Fixed format with timestamp: critical for long-running batch jobs (normalization,
  reconstruction) where knowing wall-clock start/end time matters.
- levelname padded to 8 chars: aligns log lines regardless of level (INFO vs WARNING).
- No file handler here: scripts are interactive/batch, not daemons. If file logging
  is needed later (e.g. scheduled jobs), extend via add_file_handler().
- logging.captureWarnings(True): routes Python warnings (DeprecationWarning etc.)
  through the logging system instead of printing to stderr independently.

Usage:
    # In any CLI script's if __name__ == "__main__" block:
    from utils.logging_config import setup_logging
    setup_logging()                          # default INFO level
    setup_logging(level=logging.DEBUG)       # verbose mode

    # In any module (never call setup_logging() from a library module):
    import logging
    log = logging.getLogger(__name__)
    log.info("Processing %s", filename)
"""

from __future__ import annotations

import logging
import sys


# Log format shared across all scripts.
# %(asctime)s    — wall-clock time (HH:MM:SS) so batch start/end is visible
# %(levelname)-8s — level left-justified in 8 chars (INFO    / WARNING / ERROR   )
# %(message)s    — the actual log message
_LOG_FORMAT = "%(asctime)s  %(levelname)-8s %(message)s"
_DATE_FORMAT = "%H:%M:%S"


def setup_logging(level: int = logging.INFO) -> None:
    """
    Configure the root logger for a CLI script.

    Should be called exactly once, at the top of the if __name__ == "__main__"
    block. Never call from library/module code — only from entry points.

    Args:
        level: Logging level (e.g. logging.INFO, logging.DEBUG).
               Defaults to INFO which shows all operational messages.
    """
    # StreamHandler on stderr: stdout stays clean for any downstream piping.
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT))

    root = logging.getLogger()
    root.setLevel(level)

    # Avoid duplicate handlers if setup_logging() is accidentally called twice
    # (e.g. during interactive notebook use or test imports).
    if not root.handlers:
        root.addHandler(handler)

    # Route Python warnings (DeprecationWarning, UserWarning, etc.) through
    # the logging system so they appear with the same format and timestamp.
    logging.captureWarnings(True)