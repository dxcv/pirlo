#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: TNDataService.py
@time: 2019/1/5 16:29
"""
from DataSource.FileSource import file_source

class TNDataSerivce(object):

    source = file_source

    def __init__(self):
        pass

    @classmethod
    def getTNData(cls, code_list, start, end, type):
        df = cls.source.getTNData(start, end, type)
        valid_symbol = [s for s in code_list if s in df.columns]
        return df[valid_symbol]

if __name__ == '__main__':
    pass