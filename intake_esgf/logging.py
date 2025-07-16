import io
import logging

NAME = "intake-esgf"
"""The name of the intake-esgf logger."""


class Logger:
    """
    A logger that writes all log messages sent through it to a stream in addition
    to logging them using standard Python logging.

    Atributes
    ---------
    stream: io.TextIOBase
        A stream that the captured log messages will be written to.
    formatter: logging.Formatter
        A formatter for log messages written to the stream.
    level: int
        The minimum log level for messages to be written to the stream.
    logger: logging.Logger
        The standard Python logger that log messages will be forwarded to.

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
        """
        Log 'msg % args' with the integer severity 'level'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.log(level, "We have a %s", "mysterious problem", exc_info=True)
        """
        self.logger.log(level, msg, *args, **kwargs)
        if level >= self.level:
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
        """
        Log 'msg % args' with severity 'DEBUG'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.debug("Houston, we have a %s", "thorny problem", exc_info=True)
        """
        self.log(logging.DEBUG, msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs) -> None:
        """
        Log 'msg % args' with severity 'INFO'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.info("Houston, we have a %s", "notable problem", exc_info=True)
        """
        self.log(logging.INFO, msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs) -> None:
        """
        Log 'msg % args' with severity 'WARNING'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.warning("Houston, we have a %s", "bit of a problem", exc_info=True)
        """
        self.log(logging.WARNING, msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs) -> None:
        """
        Log 'msg % args' with severity 'ERROR'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.error("Houston, we have a %s", "major problem", exc_info=True)
        """
        self.log(logging.ERROR, msg, *args, **kwargs)

    def critical(self, msg: str, *args, **kwargs) -> None:
        """
        Log 'msg % args' with severity 'CRITICAL'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.critical("Houston, we have a %s", "major disaster", exc_info=True)
        """
        self.log(logging.CRITICAL, msg, *args, **kwargs)

    def read(self) -> str:
        """
        Read all log messages from the stream.
        """
        try:
            return self.stream.getvalue()
        except AttributeError:
            # This happens if `self.stream` is a file handler.
            self.stream.seek(0)
            return self.stream.read()
