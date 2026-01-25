import logging
import os
from logging.handlers import RotatingFileHandler
import sys

# Define log dir relative to project root (assuming this file is in src/utils/)
# root is ../../
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
LOG_FILE = os.path.join(LOG_DIR, 'siaptce.log')

def setup_logger(name: str = "SIAP", log_to_file: bool = True) -> logging.Logger:
    """
    Configures and returns a logger instance.
    - Writes to logs/siaptce.log (Rotating: 5MB, 3 backups)
    - Writes to Console (StreamHandler)
    """
    # Ensure log directory exists
    if log_to_file and not os.path.exists(LOG_DIR):
        try:
            os.makedirs(LOG_DIR, exist_ok=True)
        except Exception:
            pass # Fail silently if permission issue, will fallback to console

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Avoid duplicate handlers if setup is called multiple times
    if logger.handlers:
        return logger

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # 1. Console Handler (Standard Output -> STDERR to avoid data pipe pollution)
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 2. File Handler (Rotating)
    if log_to_file:
        try:
            file_handler = RotatingFileHandler(
                LOG_FILE, 
                maxBytes=5*1024*1024, # 5 MB
                backupCount=3,
                encoding='utf-8'
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            # Fallback output if file logging fails
            print(f"[WARN] Failed to setup file logging: {e}")

    return logger
