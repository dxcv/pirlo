#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: ICTestor.py
@time: 2018/12/18 19:58
"""


class ICTestor(object):

    def __init__(self, factor, start, end):
        self.factor = factor
        self.start = start
        self.end = end

        self.ret = None
        self.factor_data = None
        self.IC = None
        self.rankIc = None

        self.factor_ret = []
    def test(self):
        self.load()
        self.compute()
        self.report()

    def load(self):
        self.load_factor()
        self.load_ret()

    def load_factor(self):
        df = self.factor_service.get_factor(self.factor_name, self.start, self.end)
        self.factor_data = df

    def load_ret(self):
        df = self.dataservice.getDailyReturn(self.start, self.end)
        self.ret = df

    def compute(self):
        for idx, row in self.factor_data.iterrows():
            factor = row.dropna()
            factor_value = factor.values
            ret = self.ret.loc[idx][factor.keys()].values / 100
            t, r_square_adj, factor_ret = self.regress(ret, factor_value)

            self.factor_ret.append(factor_ret)
            print('t:%5.2f, r_square_adj:%8.5f, factor_ret:%8.5f' % (t, r_square_adj, factor_ret))

    def regress(self, y, X):
        X = sm.add_constant(X)
        results = sm.OLS(y, X).fit()
        r_square_adj = results.rsquared_adj
        factor_ret = results.params[1]
        t = results.tvalues[1]
        return t, r_square_adj, factor_ret

    def report(self):
        tradingday = self.dataservice.getTradingDay(self.start, self.end)
        t = [datetime.datetime.strptime(s, '%Y%m%d').strftime('%Y-%m-%d') for s in tradingday]
        df = pd.DataFrame(index=t, data={'ret': self.ret, 'factor_ret': self.factor_ret})
        df['ret'].shift(1).corr(df[factor_ret])

if __name__ == '__main__':
    pass