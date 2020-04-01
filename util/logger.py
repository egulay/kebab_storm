import logging
import os

def get_logger(name, path, file_name, active_profile):
    if not os.path.exists(path):
        os.makedirs(path)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler(f'{path}/{file_name}')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if str(active_profile) == 'local' or str(active_profile) == 'dev':
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger
