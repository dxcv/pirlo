#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: PortfolioOptimizer.py
@time: 2019/2/20 16:24
"""
import cvxpy as cvx
import numpy as np

class PortfolioOptimizer(object):

    def __init__(self, start, end, base_index):
        self.start = start
        self.end = end
        self.base_index = base_index

    def run(self):
        self.compute()

    def compute(self):
        omega = cvx.Variable(4)
        basic_weight = np.array([1,2,3,4,5]) / 15
        dev = 0.1
        # dev = np.array([0.01, 0.01, 0.01, 0.01, 0.01])
        beta = np.random.randn(5, 1)
        X = np.random.randn(4, 5)
        obj = cvx.Maximize(omega * X * beta)
        constraints = [cvx.sum(omega) == 1,
                       omega >= 0,
                       cvx.norm1(omega-basic_weight)<=0.1]

        problem = cvx.Problem(obj, constraints)
        result = problem.solve()
        print(result)
        print(omega.value)


if __name__ == '__main__':
    po = PortfolioOptimizer('20130101', '20180101', '000300')
    po.run()