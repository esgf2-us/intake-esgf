import io
import logging

NAME = "intake-esgf"


class Logger:
    """
    A logger that writes all log messages sent through it to a stream in addition
    to logging them using standard Python logging.
    """

    def __init__(self):
        self.stream = io.StringIO()
        self.formatter = logging.Formatter(
            fmt="\033[36m%(asctime)s\033[0m %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self.level = logging.DEBUG
        self.logger = logging.getLogger(NAME)

    def log(self, level: int, msg: str, *args, **kwargs) -> None:
        self.logger.log(level, msg, *args, **kwargs)
        if level > self.level:
            record = logging.makeLogRecord(
                {
                    "msg": msg,
                    "level": level,
                    "name": NAME,
                    "args": args,
                }
            )
            self.stream.write(f"{self.formatter.format(record)}\n")

    def debug(self, msg: str, *args, **kwargs) -> None:
        self.log(logging.DEBUG, msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs) -> None:
        self.log(logging.INFO, msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs) -> None:
        self.log(logging.WARNING, msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs) -> None:
        self.log(logging.ERROR, msg, *args, **kwargs)

    def critical(self, msg: str, *args, **kwargs) -> None:
        self.log(logging.CRITICAL, msg, *args, **kwargs)

    def read(self) -> str:
        """Read all log messages."""
        try:
            return self.stream.getvalue()
        except AttributeError:
            # This happens if `self.stream` is a file handler.
            self.stream.seek(0)
            return self.stream.read()
