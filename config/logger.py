import os
import logging
from pathlib import Path
from datetime import datetime

# Create 'logs' directory if it doesn't exist
ROOT_DIR = Path(__file__).resolve().parent.parent
LOGS_DIR = ROOT_DIR / 'logs'
LOGS_DIR.mkdir(exist_ok=True)

# Logging settings
LOG_LEVEL = logging.INFO #DEBUG
LOG_FORMAT = '%(asctime)s - [%(levelname)s] - %(message)s' #- [%(filename)s:%(funcName)s:%(lineno)d]
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
MAX_LOG_FILES = 5 # max log files we want to keep


def cleanup_logs() -> None:
    try:
        log_files = [f for f in LOGS_DIR.glob("*.log")] # get .log files
        log_files.sort(key=lambda x: os.path.getmtime(x), reverse=True) # newest first

        for old_log in log_files[MAX_LOG_FILES-1:]:
            old_log.unlink() # delete log
    except Exception as e:
        error_msg = f"Error during log cleanup. Error: {e}"
        print(error_msg)

def setup_logger() -> logging.Logger:
    cleanup_logs() # clean up old logs before creating new one
    log_file = LOGS_DIR / f'{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    logging.basicConfig(
        level=LOG_LEVEL
        , format=LOG_FORMAT
        , datefmt=DATE_FORMAT
        , handlers=[
            logging.FileHandler(str(log_file)) # save .log file
            , logging.StreamHandler() # output to the console
        ]
    )

    return logging.getLogger()
