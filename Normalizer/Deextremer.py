#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: Deextremer.py
@time: 2018/12/18 10:30
"""
import numpy as np
import pandas as pd

class TrivialDeextremer(object):

    def __init__(self):
        pass

    def do(self, data):
        return data

class ThreeSigmaDeextremer(object):

    def __init__(self, n=3):
        self.n = n

    def do(self, data):
        """
        每行的数如果超过该行3倍的sigma则拉回u+3*sigma
        :param data: pd.DataFrame
        :return: pd.DataFrame
        """

        df = data.apply(self.operation, axis=1)
        return df

    def operation(self, x):
        mean = np.mean(x)
        std = np.std(x)
        upper_bound = mean + self.n * std
        lower_bound = mean - self.n * std
        y = x.apply(lambda z : upper_bound if z>upper_bound else (lower_bound if z < lower_bound else z))
        return y

class MADDeextremer(object):

    def __init__(self, n=3):
        self.n = n

    def do(self, data):
        """
        中位数去极值法
        :param data: pd.DataFrame
        :return: pd.DataFrame
        """
        df = data.apply(self.operation, axis=1)
        return df


    def operation(self, x):
        mad = (x - np.mean(x)).abs().mean()
        upper_bound = x.median() + self.n * mad
        lower_bound = x.median() - self.n * mad
        y = x.apply(lambda z: upper_bound if z > upper_bound else (lower_bound if z < lower_bound else z))
        return y

if __name__ == '__main__':
    import pandas as pd

    x = pd.DataFrame(data=[[5,2,3],[4, 5, 10]])
    x.index = pd.to_datetime(['20180101', '20180102'])
    # x = pd.read_csv('D:\\data\\factor\\marketCap\\marketCap.csv', index_col=0, parse_dates=True)
    # x = x['20180101': '20180501']
    # extremer = ThreeSigmaDeextremer()
    extremer = MADDeextremer()
    df = extremer.do(x)
    print(df)