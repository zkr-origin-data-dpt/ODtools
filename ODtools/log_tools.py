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
    def __init__(self, filename, when='h', interval=1, backupCount=0, encoding=None, delay=False, utc=False):
        TimedRotatingFileHandler.__init__(self, filename, when, interval, backupCount, encoding, delay, utc)

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
        currentTime = int(time.time())
        dstNow = time.localtime(currentTime)[-1]
        t = self.rolloverAt - self.interval
        if self.utc:
            timeTuple = time.gmtime(t)
        else:
            timeTuple = time.localtime(t)
            dstThen = timeTuple[-1]
            if dstNow != dstThen:
                if dstNow:
                    addend = 3600
                else:
                    addend = -3600
                timeTuple = time.localtime(t + addend)
        dfn = self.baseFilename + "." + time.strftime(self.suffix, timeTuple)
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
        newRolloverAt = self.computeRollover(currentTime)
        while newRolloverAt <= currentTime:
            newRolloverAt = newRolloverAt + self.interval
        # If DST changes and midnight or weekly rollover, adjust for this.
        if (self.when == 'MIDNIGHT' or self.when.startswith('W')) and not self.utc:
            dstAtRollover = time.localtime(newRolloverAt)[-1]
            if dstNow != dstAtRollover:
                if not dstNow:  # DST kicks in before next rollover, so we need to deduct an hour
                    addend = -3600
                else:  # DST bows out before next rollover, so we need to add an hour
                    addend = 3600
                newRolloverAt += addend
        self.rolloverAt = newRolloverAt


def base_logger(log_name: str, file_path: str = './', mode: str = 'DEBUG',
                cmd_output: bool = False, backupCount: int = 3) -> logging:
    """
    日志demo,后缀是 如: abc.log.2018-04-01
    :param log_name: 日志文件的名字.
    :param file_path: 您可以在哪里放置日志文件。请注意它是一个路径，而不是文件.
    :param mode: 你想看到的日志级别。默认信息级别 DEBUG.
    :param cmd_output: 是否将日志输出到您的屏幕。DEBUG调试级别默认输出它.
    :param backupCount: 保存日志文件个数.
    :return: 日志记录器准备完毕.
    """

    logger = logging.getLogger(log_name)
    logger.setLevel(log_level[mode])

    formatter = logging.Formatter('%(asctime)s %(pathname)s --的%(lineno)d行--%(levelname)s: *%(name)s* %(message)s')
    #  这里进行判断，如果logger.handlers列表为空，则添加，否则，直接去写日志
    if os.name != 'nt':
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        log_name = os.path.join(file_path, log_name)
        fh = SafeRotatingFileHandler(filename='{name}.log'.format(name=log_name), when="MIDNIGHT",
                                     backupCount=backupCount, delay=False, encoding='utf-8')
        """
        filename 日志文件名,when 时间间隔的单位,backupCount 保留文件个数，delay 是否开启 OutSteam缓存，
        True 表示开启缓存，OutStream输出到缓存，待缓存区满后，刷新缓存区，并输出缓存数据到文件。
        False表示不缓存，OutStream直接输出到文件
        """
        fh.setFormatter(formatter)
        if not logger.handlers:
            logger.addHandler(fh)

    # 终端打印日志
    if mode == 'DEBUG' or cmd_output:
        ch = logging.StreamHandler()
        ch.setLevel(log_level[mode])
        ch.setFormatter(formatter)
        # 日志文件系统。windows系统开发不写日志
        if not logger.handlers:
            logger.addHandler(ch)

    return logger


if __name__ == '__main__':
    pass
