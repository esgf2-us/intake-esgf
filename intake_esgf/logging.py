"""Functions which control how logging takes place."""
import logging
from pathlib import Path
from typing import Union


def setup_logging(
    local_cache: Union[Path, None] = None, filename: Union[str, None] = None
) -> logging.Logger:
    """Setup the location and logging for this package."""

    # Where will the log be written?
    if local_cache is None:
        local_cache = Path.home() / ".esgf"
    log_file = local_cache / ("esgf.log" if filename is None else filename)
    if not log_file.is_file():
        log_file.touch()

    # We need a named logger to avoid other packages that use the root logger
    logger = logging.getLogger("intake-esgf")
    if not logger.handlers:
        # Now setup the file into which we log relevant information
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(
            logging.Formatter(
                "\x1b[36;20m%(asctime)s \033[0m%(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        file_handler.setLevel(logging.INFO)
        logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)

    # This is probably wrong, but when I log from my logger it logs from parent also
    logger.parent.handlers = []
    return logger
