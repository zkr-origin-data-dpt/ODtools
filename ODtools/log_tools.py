import os
import logging
import time
from logging.handlers import TimedRotatingFileHandler

log_level = {
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}


class SafeRotatingFileHandler(TimedRotatingFileHandler):
    def __init__(self, filename, when='h', interval=1, backup_count=0, encoding=None, delay=False, utc=False):
        TimedRotatingFileHandler.__init__(self, filename, when, interval, backup_count, encoding, delay, utc)

    """
    Override doRollover
    lines commanded by "##" is changed by cc
    """

    def doRollover(self):
        """
        do a rollover; in this case, a date/time stamp is appended to the filename
        when the rollover happens.  However, you want the file to be named for the
        start of the interval, not the current time.  If there is a backup count,
        then we have to get a list of matching filenames, sort them and remove
        the one with the oldest suffix.

        Override,   1. if dfn not exist then do rename
                    2. _open with "a" model
        """
        if self.stream:
            self.stream.close()
            self.stream = None
        # get the time that this sequence started at and make it a TimeTuple
        current_time = int(time.time())
        dst_now = time.localtime(current_time)[-1]
        t = self.rolloverAt - self.interval
        if self.utc:
            time_tuple = time.gmtime(t)
        else:
            time_tuple = time.localtime(t)
            dst_then = time_tuple[-1]
            if dst_now != dst_then:
                if dst_now:
                    addend = 3600
                else:
                    addend = -3600
                time_tuple = time.localtime(t + addend)
        dfn = self.baseFilename + "." + time.strftime(self.suffix, time_tuple)
        # if os.path.exists(dfn):
        #     os.remove(dfn)
        # Issue 18940: A file may not have been created if delay is True.
        #     if os.path.exists(self.baseFilename):
        if not os.path.exists(dfn) and os.path.exists(self.baseFilename):
            os.rename(self.baseFilename, dfn)
        if self.backupCount > 0:
            for s in self.getFilesToDelete():
                os.remove(s)
        if not self.delay:
            self.mode = "a"
            self.stream = self._open()
        new_rollover_at = self.computeRollover(current_time)
        while new_rollover_at <= current_time:
            new_rollover_at = new_rollover_at + self.interval
        # If DST changes and midnight or weekly rollover, adjust for this.
        if (self.when == 'MIDNIGHT' or self.when.startswith('W')) and not self.utc:
            dst_at_rollover = time.localtime(new_rollover_at)[-1]
            if dst_now != dst_at_rollover:
                if not dst_now:  # DST kicks in before next rollover, so we need to deduct an hour
                    addend = -3600
                else:  # DST bows out before next rollover, so we need to add an hour
                    addend = 3600
                new_rollover_at += addend
        self.rolloverAt = new_rollover_at


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
    formatter = logging.Formatter('%(asctime)s - %(pathname)s - %(lineno)d - %(name)s - %(levelname)s: %(message)s')

    if not os.path.exists(file_path):
        os.makedirs(file_path)
    log_name = os.path.join(file_path, log_name)
    fh = SafeRotatingFileHandler(filename='{name}.log'.format(name=log_name),
                                 delay=False,
                                 when="MIDNIGHT",
                                 encoding='utf-8',
                                 backup_count=backup_count)

    fh.setFormatter(formatter)
    logger.addHandler(fh)

    if mode == 'DEBUG' or cmd_output:
        ch = logging.StreamHandler()
        ch.setLevel(log_level[mode])
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger


if __name__ == '__main__':
    pass
