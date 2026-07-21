import logging
from pathlib import Path


def configure_logging(path, file_name, active_profile):
    """Configure Kebab Storm logging once at application startup."""
    log_directory = Path(path)
    log_directory.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    log_path = str((log_directory / file_name).resolve())
    if not any(getattr(handler, '_kebab_storm_log_path', None) == log_path for handler in root_logger.handlers):
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(formatter)
        file_handler._kebab_storm_log_path = log_path
        root_logger.addHandler(file_handler)

    if active_profile in {'local', 'dev'} and not any(
            getattr(handler, '_kebab_storm_console', False) for handler in root_logger.handlers):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler._kebab_storm_console = True
        root_logger.addHandler(console_handler)
