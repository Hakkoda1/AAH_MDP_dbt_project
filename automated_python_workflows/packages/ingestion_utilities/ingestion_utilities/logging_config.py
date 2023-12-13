import os
import logging
from logging.handlers import TimedRotatingFileHandler
import datetime

def configure_logging(log_path: str):
    # Get the base log directory from LOG_PATH
    base_log_dir = os.path.dirname(log_path)

    # Include today's date in the log directory name
    today = datetime.date.today().isoformat()
    log_dir = os.path.join(base_log_dir, today)

    # Create the log directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)

    # Create a logger and set its level to the lowest level you want to capture
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Create a formatter for the log messages
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')

    # Create a StreamHandler for console output and set its level
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # Set the level for console output
    console_handler.setFormatter(formatter)

    # Create a TimedRotatingFileHandler for the log file
    log_file_name = os.path.basename(log_path)
    file_handler = TimedRotatingFileHandler(os.path.join(log_dir, log_file_name), when="midnight", interval=1, backupCount=7)
    file_handler.setLevel(logging.DEBUG)  # Set the level for the log file
    file_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)