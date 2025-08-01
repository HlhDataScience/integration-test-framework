import logging
from pathlib import Path
import sys

def setup_basic_logging(log_file_path: str)-> None:
    """
    Small utility function to set up logging using the python module logging.
    :param log_file_path: The path in which the log will be saved.
    :return: None
    """

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file_path),
            logging.StreamHandler(sys.stdout)
        ]
    )
def setup_custom_logging(log_file_path: str | Path, logger_name: str) -> logging.Logger:
    """
    Sets up application-specific logging.
    :param logger_name: the custom graph_logger name.
    :param log_file_path: Path to the log file.
    :return: Configured custom_logger object.
    """
    logger = logging.getLogger(name=logger_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False  # VERY IMPORTANT: prevents double logs if root graph_logger is also logging

    # Avoid adding handlers multiple times if setup is called more than once
    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] - %(message)s", "%Y-%m-%d %H:%M:%S")

        file_handler = logging.FileHandler(f"{log_file_path}/{logger_name}")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger
