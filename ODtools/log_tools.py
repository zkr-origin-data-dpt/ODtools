import os
import logging
import time
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler

log_level = {
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}


def base_logger(log_name: str = 'default', file_path: str = './', mode: str = 'DEBUG',
                cmd_output: bool = False, backup_count: int = 3) -> logging:
    """
    log eg: abc.log.2018-04-01
    :param log_name: log name
    :param file_path: log path
    :param mode: log mode
    :param cmd_output: terminal print log
    :param backup_count: save the number of log files
    :return: logger
    """

    logger = logging.getLogger(log_name)
    logger.setLevel(log_level[mode])
    formatter = logging.Formatter('%(asctime)s @zkr@ %(pathname)s @zkr@ %(lineno)d @zkr@ %(name)s @zkr@ %(levelname)s @zkr@ %(message)s')

    handlers = set()

    if not os.path.exists(file_path):
        os.makedirs(file_path)
    log_name = os.path.join(file_path, log_name)
    fh = RotatingFileHandler(filename='{name}.log'.format(name=log_name),
                             maxBytes=1024*1024*50, backupCount=5,
                             encoding="utf-8",delay=False)

    fh.setFormatter(formatter)
    handlers.add(fh)

    if mode == 'DEBUG' or cmd_output:
        ch = logging.StreamHandler()
        ch.setLevel(log_level[mode])
        ch.setFormatter(formatter)
        handlers.add(ch)

    for handler in handlers:
        logger.addHandler(handler)

    return logger


if __name__ == '__main__':
    a = base_logger("test")
    while True:
        a.info("test.1")
