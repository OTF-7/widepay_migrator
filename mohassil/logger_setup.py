import logging
import os
from datetime import datetime as dt

def setup_logger(migration_name=None):
    """
    Set up and configure the logger.
    Args:
        migration_name (str, optional): Name of the migration for specific log file.
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logs directory if it doesn't exist
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    # Create a timestamp for the log filename
    timestamp = dt.now().strftime('%Y%m%d_%H%M%S')
    
    # Create log filename based on migration name if provided
    if migration_name:
        log_filename = f"{migration_name}_{timestamp}.log"
    else:
        log_filename = f"migration_{timestamp}.log"
    
    log_path = os.path.join(log_dir, log_filename)
    
    # Configure logger
    logger = logging.getLogger(migration_name or 'migration')
    logger.setLevel(logging.INFO)
    
    # Create file handler
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(file_handler)
    
    return logger