#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: IndustryFactorService.py
@time: 2019/1/17 9:30
"""
import Log

from DataSource.FileSource import file_source
from DataSource.JydbSource import jydbSource


class IndustryFactorService(object):

    source = jydbSource
    fsource = file_source
    industryCache = {}
    logger = Log.get_logger(__name__)
    @classmethod
    def get_industry_factor(cls, tradingday):
        """
        返回industry factor矩阵字典
        :param start: '20150101'
        :param end: '20150101'
        :return: {'20150101' : pd.DataFrame}
        """
        try:
            df = cls.industryCache[tradingday]
        except KeyError as e:
            # cls.logger.error(e)
            df = file_source.get_industry_factor(tradingday)
            if not df.empty:
                cls.industryCache[tradingday] = df
        return df

if __name__ == '__main__':
    pass