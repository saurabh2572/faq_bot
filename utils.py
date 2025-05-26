"""
Utility functions for the Copilot UI application.

This module provides helper functions for:
- File management (audio file deletion)
- Logging configuration and setup
- System-wide utilities

All functions include error handling and logging capabilities.
"""

import os
import logging
import sys
from typing import Optional


def delete_audio_file(audio_file_path: str) -> bool:
    """
    Delete a temporary audio file from the filesystem.

    Args:
        audio_file_path (str): Path to the audio file to be deleted

    Returns:
        bool: True if deletion was successful, False otherwise

    Raises:
        OSError: If file deletion fails due to permissions or system errors
    """
    try:
        if not audio_file_path:
            raise ValueError("Audio file path cannot be empty")

        if os.path.exists(audio_file_path):
            os.remove(audio_file_path)
            logging.info(f"File '{audio_file_path}' deleted successfully")
            return True
        else:
            logging.warning(f"File not found for deletion: '{audio_file_path}'")
            return False

    except OSError as e:
        logging.error(f"Error deleting file '{audio_file_path}': {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error during file deletion: {str(e)}")
        return False


def setup_logger(name: str = "copilot_ui") -> Optional[logging.Logger]:
    """
    Configure and return a logger with console output.

    Sets up a logger with consistent formatting and console output handling.
    Clears any existing handlers to prevent duplicate logging.

    Args:
        name (str): Name of the logger instance. Defaults to "copilot_ui"

    Returns:
        Optional[logging.Logger]: Configured logger instance or None if setup fails

    Example:
        >>> logger = setup_logger("my_module")
        >>> logger.info("This is a log message")
        2024-05-21 10:30:45 | my_module    | INFO     | This is a log message
    """
    try:
        # Validate input
        if not isinstance(name, str):
            raise ValueError("Logger name must be a string")

        # Create logger
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        
        # Remove any existing handlers to prevent duplicates
        if logger.handlers:
            logger.handlers.clear()
        
        # Create console handler with stdout stream
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        
        # Create detailed formatter
        formatter = logging.Formatter(
            fmt='%(asctime)s | %(name)-12s | %(levelname)-8s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(console_handler)
        
        return logger

    except Exception as e:
        # Log to system logger in case of setup failure
        logging.error(f"Failed to setup logger '{name}': {str(e)}")
        return None