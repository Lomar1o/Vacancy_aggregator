import os
import logging
import logging.config


def write_logs(cls, level='INFO'):
    """
    Функция для настройки логгирования.

    Принимает:
    класс,
    необязательный параметр уровень логгирования (по умолчанию INFO).

    Определяет наименование директории, в которой расположен файл с классом,
    и создает в этой директории файл для записи.
    Читает конфигурации и файла с настройками,
    создает логгер.

    """
    parent_path = os.path.abspath(os.path.dirname(__file__))
    log_conf_path = os.path.join(parent_path, 'log.config')
    if not os.path.exists('logs'):
        os.makedirs('logs')
    logging.config.fileConfig(fname=log_conf_path,
                              defaults={'logfilename': f'logs/{cls.__name__}.log'},
                              disable_existing_loggers=False)
    logger = logging.getLogger(cls.__name__)
    logger.setLevel(level)
    return logger
