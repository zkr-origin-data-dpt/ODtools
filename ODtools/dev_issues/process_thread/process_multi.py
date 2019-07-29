# -*- coding: utf-8 -*-
# @Email: jqian_bo@163.com
# @Author: JingQian Bo
# @Create Time: 2019/7/29-11:23 AM

import os
import time
from demo import run
from multiprocessing import Pool
from process_futures import control_process


def multi():
    t = lambda: time.time()
    start = t()
    print('Parent process %s.' % os.getpid())
    p = Pool(4)
    for i in range(5):
        p.apply_async(run, args=())
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')
    print(t() - start)


def future():
    t = lambda: time.time()
    start = t()
    result = control_process(list_=[1, 2, 3, 4, 5], func=run)
    print(t() - start)


if __name__ == '__main__':
    multi()
    future()
