import logging

import intake_esgf.logging


def test_logger(caplog):
    """Test the `intake_esgf.logging.Logger` basic functionality."""
    logger = intake_esgf.logging.Logger()

    # Set the log level
    logger.level = logging.DEBUG
    caplog.set_level(logging.DEBUG)

    # Test logging at all levels.
    levels = (
        "debug",
        "info",
        "warning",
        "error",
        "critical",
    )
    for level in levels:
        getattr(logger, level)("Example message at level %s", level)

    # Test reading the captured messages from the stream.
    messages = logger.read()
    assert len(messages.rstrip("\n").split("\n")) == len(levels)

    # Test that the messages were written to the standard Python logger.
    assert len(caplog.records) == len(levels)


def test_log_level(caplog):
    """Test that setting the log level works."""
    logger = intake_esgf.logging.Logger()
    logger.level = logging.WARNING
    caplog.set_level(logging.DEBUG)
    logger.debug("This is a debug message")
    logger.warning("This is a warning message")
    assert len(logger.read().rstrip("\n").split("\n")) == 1
    assert len(caplog.records) == 2


def test_log_to_file(tmp_path):
    """Test that a file can be used to save the captured log message."""
    log_file = tmp_path / "log.txt"

    logger = intake_esgf.logging.Logger()
    msg = "This is a log message"
    with log_file.open(mode="a+", encoding="utf-8") as fp:
        logger.stream = fp
        logger.info(msg)
        assert msg in logger.read()

    assert msg in log_file.read_text(encoding="utf-8")


def test_logger_consistency():
    """Test that the catalog and indices are created with the capturing logger."""
    cat = intake_esgf.ESGFCatalog()
    assert isinstance(cat.logger, intake_esgf.logging.Logger)
    assert len(cat.indices)
    for ind in cat.indices:
        assert ind.logger == cat.logger
