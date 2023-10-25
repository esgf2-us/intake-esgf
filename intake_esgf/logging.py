import logging
from pathlib import Path
from typing import Union


def setup_logging(local_cache: Union[Path, None] = None) -> logging.Logger:
    """Setup the location and logging for this package."""

    # Where will the log be written?
    if local_cache is None:
        local_cache = Path.home()
    log_file = local_cache / "esgf.log"
    if not log_file.is_file():
        log_file.touch()

    # We need a named logger to avoid other packages that use the root logger
    logger = logging.getLogger("intake-esgf")

    # Now setup the file into which we log relevant information
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(
        logging.Formatter(
            "\x1b[36;20m%(asctime)s \x1b[36;32m%(funcName)s\033[0m %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    file_handler.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)

    # This is probably wrong, but when I log into my logger it logs to parent also
    logger.parent.handlers = []
    return logger
