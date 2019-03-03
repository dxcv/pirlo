#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: FactorService.py
@time: 2018/12/19 14:05
"""
import os
import Log
import pandas as pd


class FactorService(object):

    factor_root = 'G:\\data\\factor'
    logger = Log.get_logger(__name__)

    def __init__(self):
        pass

    @classmethod
    def get_factor(cls, factor, start, end, type):
        """获取各种归一化好的因子值"""
        df = pd.DataFrame()

        f = os.path.join(cls.factor_root, type, factor + '.csv')

        try:
            df = pd.read_csv(f, index_col=0, parse_dates=True)
            df = df[start: end]
        except Exception as e:
            msg = '%s %s-%s got Wrong' % (factor, start, end)
            cls.logger.error(e)
            cls.logger.error(msg)
        return df

if __name__ == '__main__':
    df = FactorService.get_factor('marketCap', '20180101', '20180110', 'mad-normal')
    print(df.head())