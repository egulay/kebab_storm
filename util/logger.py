import logging
import os

from conf.settings import ACTIVE_PROFILE


def get_logger(name, path, file_name):
    if not os.path.exists(path):
        os.makedirs(path)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler(f'{path}/{file_name}')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if str(ACTIVE_PROFILE) == 'local' or str(ACTIVE_PROFILE) == 'dev':
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger
