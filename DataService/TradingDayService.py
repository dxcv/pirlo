#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: TradingDayService.py
@time: 2019/1/5 16:27
"""
import Log
from DataSource.JydbSource import jydbSource

class TradingDayService(object):

    tradingDay = jydbSource.get_tradingday('19900101', '20211231')
    logger = Log.get_logger(__name__)
    def __init__(self):
        pass
        # self.tradingDay = jydbSource.get_tradingday('19900101', '20211231')
        # self.logger = Log.get_logger(__name__)

    @classmethod
    def getTradingDay(cls, start ,end):
        """
        返回start和end之间的tradingday
        :param start:  '20150101'
        :param end: '20151231'
        :return: ['20150101', '20150102']
        """
        return [x for x in cls.tradingDay if (x >= start) and (x <= end)]

    @classmethod
    def getRelativeTradingDay(cls, tradingday, shift):
        """
        返回tradingday + n的tradingday
        :param tradingday: '20150101'
        :param shift: 2
        :return: '20150101'
        """
        try:
            x = next(x for x in cls.tradingDay if x >= tradingday)
            i = cls.tradingDay.index(x)
            return cls.tradingDay[i+shift]
        except ValueError:
            cls.logger.info('%s %d relativeTradingday查询失败' % (tradingday, shift))
            return None


if __name__ == '__main__':
    pass