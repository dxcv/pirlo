#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: StaticDataService.py
@time: 2018/12/17 16:56
"""
import os
import Log
import pandas as pd

class StaticDataService(object):

    logger = Log.get_logger(__name__)

    static_root = 'D:\\data\\static'
    def __init__(self):
        pass

    @classmethod
    def getDailyDerivativeByDay(cls, tradingday, type):
        dir = os.path.join(cls.static_root, 'stock', 'DailyDerivative', 'byDay', 'jydb')
        f = os.path.join(dir, tradingday + '.xls')
        try:
            df = pd.read_excel(f)
            return df[type]
        except Exception as e:
            cls.logger.error(e)
            cls.logger.info('%s got DailyDerivative Wrong' % tradingday)

    @classmethod
    def getDailyPerformanceByDay(cls, tradingday, type):
        dir = os.path.join(cls.static_root, 'stock', 'DailyPerformance', 'jydb', type)
        f = os.path.join(dir, tradingday + '.csv')
        try:
            df = pd.read_csv(f, index_col=0)
            return df[type]
        except Exception as e:
            cls.logger.error(e)
            cls.logger.info('%s got DailyDerivative Wrong' % tradingday)

    @classmethod
    def getFinancialReportByDay(cls, tradingday, type):
        pass

    @classmethod
    def getFinancialDerivativeByDay(cls, tradingday, type):
        dir = os.path.join(self.static_root, 'stock', 'FinancialDerivative', 'byDay', 'jydb')
        f = os.path.join(dir, tradingday + '.xls')
        try:
            df = pd.read_excel(f)
            return df[type]
        except Exception as e:
            cls.logger.error(e)
            cls.logger.info('%s got FinancialDerivative Wrong' % tradingday)


if __name__ == '__main__':

    df = StaticDataService.getDailyPerformanceByDay('20131017','Ret')
    print(df.head())