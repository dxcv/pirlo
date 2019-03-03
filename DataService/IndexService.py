#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: IndexService.py
@time: 2019/2/13 15:07
"""
import os
import Log

import pandas as pd

class IndexService(object):

    logger = Log.get_logger(__name__)

    root = 'D:\\data'
    def __init__(self):
        pass

    @classmethod
    def getIndexRet(cls, index_code, start ,end):
        dir = os.path.join(cls.root, 'h5data', 'index', 'daybar', 'byIndex')
        f = os.path.join(dir, index_code + '.csv')
        try:
            df = pd.read_csv(f, index_col=['Date'], parse_dates=True)
            ret = df['Close'] / df['PreClose'] - 1
            return ret.loc[start: end]
        except Exception as e:
            cls.logger.error(e)
            cls.logger.info('%s %s %s got IndexRet Wrong' % (index_code, start, end))

    @classmethod
    def getIndexComponentWeight(cls, tradingday, index_compoenent_weight):
        pass


if __name__ == '__main__':
    df = IndexService.getIndexRet('399001', '20100101', '20100130')
    print(df)