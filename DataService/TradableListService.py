#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: TradableListService.py
@time: 2019/1/5 16:29
"""
from DataSource.JydbSource import jydbSource
from DataSource.FileSource import file_source

import Log
class TradableListSerivce(object):

    logger = Log.get_logger(__name__)
    source = jydbSource
    fsource = file_source

    tradingdays = []
    index_code = []
    universe = {}

    index_component = {}

    def __init__(self):
        pass
        # self.source = jydbSource

    @classmethod
    def setTradingDay(cls, start, end):
        cls.tradingdays = cls.source.get_tradingday(start, end)
        cls.logger.info('开始加载universe')
        for t in cls.tradingdays:
            df = file_source.get_universe(t)
            if not df.empty:
                cls.universe[t] = cls.fsource.get_universe(t)

        cls.logger.info('加载universe结束')

    @classmethod
    def setIndexComponent(cls, index_code):
        if index_code != 'all':
            cls.index_code.append(index_code)
            cls.logger.info('开始加载行业')
            for t in cls.tradingdays:
                for code in cls.index_code:
                    df = cls.fsource.get_index_component(t, code)
                    if not df.empty:
                        cls.index_component[t] = {}
                        cls.index_component[t][code] = df
            cls.logger.info('行业加载完毕')

    @classmethod
    def getIndexComponent(cls, tradingday, index):
        try:
            df = cls.index_component[tradingday][index]
        except KeyError as e:
            df = cls.fsource.get_index_component(tradingday, index)
            cls.index_component[tradingday] = {}
            cls.index_component[tradingday][index] = df
        return df['Symbol'].tolist()

    @classmethod
    def getTradableStock(cls, tradingday):
        try:
            universe = cls.universe[tradingday]
        except KeyError as e:
            universe = cls.source.get_universe(tradingday)
            cls.universe[tradingday] = universe
        tradable = universe[(universe['ListDate']<=tradingday) & (universe['DelistDate']>tradingday) & (universe['isTradable'] == 1)]['Symbol'].tolist()
        return tradable

    @classmethod
    def getStopStock(cls, tradingday):
        try:
            universe = cls.universe[tradingday]
        except KeyError as e:
            universe = cls.source.get_universe(tradingday)
            cls.universe[tradingday] = universe
        stopStock = universe[(universe['ListDate']<=tradingday) & (universe['DelistDate']>tradingday) & (universe['isTradable'] == 0)]['Symbol'].tolist()
        return stopStock

    @classmethod
    def getDelistedStock(cls, tradingday):
        try:
            universe = cls.universe[tradingday]
        except KeyError as e:
            universe = cls.source.get_universe(tradingday)
            cls.universe[tradingday] = universe
        delisted = universe[universe['DelistDate'] <= tradingday]['Symbol'].tolist()
        return delisted

    @classmethod
    def getIpoStock(cls, start ,end):
        try:
            universe = cls.universe[end]
        except KeyError as e:
            universe = cls.source.get_universe(end)
            cls.universe[end] = universe
        newStock = universe[(universe['ListDate']>=start) & (universe['DelistDate'] > end)]['Symbol'].tolist()
        return newStock

    @classmethod
    def getStStock(cls, tradingday):
        try:
            universe = cls.universe[tradingday]
        except KeyError as e:
            universe = cls.source.get_universe(tradingday)
            cls.universe[tradingday] = universe
        stStock = universe[universe['ChineseName'].map(lambda x: 'ST' in x) & (universe['DelistDate'] > tradingday)]['Symbol'].tolist()
        return stStock

    @classmethod
    def getUniverse(cls, tradingday):
        try:
            universe = cls.universe[tradingday]
        except KeyError as e:
            universe = cls.source.get_universe(tradingday)
            cls.universe[tradingday] = universe
        df = universe[(universe['ListDate']<=tradingday)]['Symbol'].tolist()
        return df

    @classmethod
    def getNonDelisted(cls, tradingday):
        try:
            universe = cls.universe[tradingday]
        except KeyError as e:
            universe = cls.source.get_universe(tradingday)
            cls.universe[tradingday] = universe
        nondelisted = universe[(universe['ListDate']<=tradingday) & (universe['DelistDate'] > tradingday)]['Symbol'].tolist()
        return nondelisted

if __name__ == '__main__':
    pass