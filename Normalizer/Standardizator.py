#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: Standardizator.py
@time: 2018/12/18 10:33
"""


class TrivialStandardlizer(object):

    def do(self, data):
        return data

class NormalStandardlizer(object):

    def do(self, data):
        mean = data.mean(axis=1)
        std = data.std(axis=1)

        df = data.sub(mean, axis=0).div(std, axis=0)

        return df

if __name__ == '__main__':
    import pandas as pd
    import numpy as np
    x = pd.DataFrame(data=[[5,np.NaN,3],[4, 5, 10]])
    x.index = pd.to_datetime(['20180101', '20180102'])
    # x = pd.read_csv('D:\\data\\factor\\marketCap\\marketCap.csv', index_col=0, parse_dates=True)
    # x = x['20180101': '20180501']
    standardlier = NormalStandardlizer()
    df = standardlier.do(x)
    print(df)