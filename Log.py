#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: Log.py
@time: 2018/12/17 17:04
"""
import logging
import datetime


def get_logger(name):
    logger = logging.getLogger(name)

    ch = logging.StreamHandler()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(threadName)s- %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)

    fh = logging.FileHandler(''.join([datetime.datetime.now().strftime('%Y%m%d'), '.log']))
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(threadName)s- %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)

    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger


if __name__ == '__main__':
    pass