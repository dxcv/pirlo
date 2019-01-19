#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: IndustryFactorService.py
@time: 2019/1/17 9:30
"""
from DataSource.FileSource import file_source
from DataSource.JydbSource import jydbSource


class IndustryFactorService(object):

    source = jydbSource
    fsource = file_source

    @classmethod
    def get_industry_factor(cls, start, end):
        """
        返回industry factor矩阵字典
        :param start: '20150101'
        :param end: '20150101'
        :return: {'20150101' : pd.DataFrame}
        """
        result = {}
        tradingdays = jydbSource.get_tradingday(start, end)
        for t in tradingdays:
            df = file_source.get_industry_factor(t)
            if not df.empty:
                result[t] = df
        return result

if __name__ == '__main__':
    pass