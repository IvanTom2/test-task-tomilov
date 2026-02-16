import logging


def setup_logger(
    name: str,
    level: int = logging.INFO,
) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger


logger = setup_logger("task-1")
