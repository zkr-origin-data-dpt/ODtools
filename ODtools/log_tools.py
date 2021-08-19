import logging
import os
import time
from logging.handlers import RotatingFileHandler

import colorlog as colorlog

log_level = {
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}

log_colors_config = {
    'DEBUG': 'white',  # cyan white
    'INFO': 'green',
    'WARNING': 'yellow',
    'ERROR': 'red',
    'CRITICAL': 'bold_red',
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

    import socket
    try:
        host_name = socket.gethostname()
    except Exception as e:
        host_name = "未知"
    try:
        ip = socket.gethostbyname(host_name)
    except Exception as e:
        ip = "未知服务器ip"
    logger = logging.getLogger(log_name)
    logger.setLevel(log_level[mode])
    formatter = logging.Formatter(
        '%(asctime)s @zkr@ %(pathname)s @zkr@ %(lineno)d @zkr@ %(name)s @zkr@ %(levelname)s @zkr@ ' + ip + ' @zkr@ ' + host_name + ' @zkr@ %(message)s')
    #  这里进行判断，如果logger.handlers列表为空，则添加，否则，直接去写日志

    if not logger.handlers:
        handlers = set()
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        log_name = os.path.join(file_path, log_name)
        fh = RotatingFileHandler(filename='{name}.log'.format(name=log_name),
                                 maxBytes=1024 * 1024 * 50, backupCount=5,
                                 encoding="utf-8", delay=False)

        fh.setFormatter(formatter)
        handlers.add(fh)
        #  这里进行判断，如果logger.handlers列表为空，则添加，否则，直接去写日志
        if mode == 'DEBUG' or cmd_output:
            ch = logging.StreamHandler()
            ch.setLevel(log_level[mode])
            console_formatter = colorlog.ColoredFormatter(
                fmt='%(log_color)s[%(asctime)s.%(msecs)03d] %(filename)s -> %(funcName)s line:%(lineno)d [%(levelname)s] : %(message)s',
                datefmt='%Y-%m-%d  %H:%M:%S',
                log_colors=log_colors_config
            )
            ch.setFormatter(console_formatter)
            handlers.add(ch)

        for handler in handlers:
            logger.addHandler(handler)

    return logger


if __name__ == '__main__':

    a = base_logger(log_name="integertor--{}".format("1"),
                    file_path="../testMethod",
                    cmd_output=True)
    while True:
        a.info("test.1")
        time.sleep(5)
