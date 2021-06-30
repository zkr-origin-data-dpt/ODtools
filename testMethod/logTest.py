#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@File  : logTest.py
@Author: longfei 
@Date  : 2021/6/29 2:54 下午
@Desc  : 
'''
import time
import sys
sys.path.append("../")
from ODtools.log_tools import base_logger

a = base_logger(log_name="integertor--{}".format("1"),
                    file_path="./", mode="DEBUG",
                    cmd_output=True)
for i in range(100):
    a.debug("test.1")
    time.sleep(2)
