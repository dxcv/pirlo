#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: SingleNeutralizer.py
@time: 2018/12/24 13:19
"""
import os

import pandas as pd
import numpy as np
import statsmodels.api as sm

import datetime

# from DataService import TradableListService
from DataService.FactorService import FactorService


import plotly
import plotly.graph_objs as go

from DataService.TradingDayService import TradingDayService
from DataService.TradableListService import TradableListSerivce

import Log


class SingleNeutralizer(object):

    def __init__(self, factor_name, start, end, neutralizer_name):
        self.factor_name = factor_name
        self.start = start
        self.end = end
        self.neutralizer_name = neutralizer_name

        self.exclude_ipo = False

        self.neutralizer_data = None
        self.factor_data = None

        self.residual = pd.DataFrame()
        self.beta = []
        self.r_square_adj = []
        self.t = []

        self.logger = Log.get_logger(__name__)
        # self.dataservice = DataService()
        # self.static_dataservice = StaticDataService()
        # self.factor_service = FactorService()

    def run(self):
        self.test()

    def test(self):
        self.load()
        self.compute()
        self.store()
        self.report()

    def load(self):
        self.load_factor()
        # self.load_ret()

    def load_factor(self):
        df = FactorService.get_factor(self.factor_name, self.start, self.end, 'mad-normal')
        # df = self.factor_service.get_factor(self.factor_name, self.start, self.end, 'mad-normal')
        self.factor_data = df
        neutralizer_data = FactorService.get_factor(self.neutralizer_name, self.start, self.end, 'mad-normal')
        # marketCap = self.factor_service.get_factor('MarketCap', self.start, self.end, 'mad-normal' )
        self.neutralizer_data = neutralizer_data

    def load_ret(self):
        pass

    def compute(self):
        for idx, row in self.factor_data.iterrows():
            today = idx.strftime('%Y%m%d')
            print(today)
            factor = row.dropna()
            tradable = TradableListSerivce.getTradableStock(today)
            neutralizer_data = self.neutralizer_data.loc[idx].dropna()


            if self.exclude_ipo:
                ipo_stock = TradableListSerivce.getIpoStock(TradingDayService.getRelativeTradingDay(today, -60), today)
            else:
                ipo_stock = []

            valid_symbol = [s for s in tradable if (s in neutralizer_data.keys()) and (s in factor.keys()) and (s not in ipo_stock)]

            neutralizer_data = neutralizer_data[valid_symbol].values
            factor_value = factor[valid_symbol].values

            if factor[valid_symbol].isna().all():
                beta, residual, r_square_adj, t = None, None, None, None
            else:
                beta, residual, r_square_adj, t = self.regress(factor_value, neutralizer_data)
            x = pd.Series(index=valid_symbol, data=residual, name=idx)
            self.residual = self.residual.append(x)
            self.beta.append(beta)
            self.r_square_adj.append(r_square_adj)
            self.t.append(t)

    def regress(self, y, X):
        X = sm.add_constant(X)
        results = sm.OLS(y, X).fit()

        r_square_adj = results.rsquared_adj
        beta = results.params[1]
        t = results.tvalues[1]
        residual =  y - np.dot(X, results.params)

        return beta, residual, r_square_adj, t

    def report(self):
        tradingday = TradingDayService.getTradingDay(self.start, self.end)

        t = [datetime.datetime.strptime(s, '%Y%m%d').strftime('%Y-%m-%d') for s in tradingday]
        d = pd.DataFrame(
            data={'Date': t, 't': self.t, 'beta': self.beta, 'r_square_adj': self.r_square_adj})

        trace0 = go.Bar(
            x=d['Date'],
            y=d['t'],
            name='t序列'
        )

        trace1 = go.Bar(
            x=d['Date'],
            y=d['r_square_adj'],
            name='R2序列'
        )

        trace2 = go.Bar(
            x=d['Date'],
            y=d['beta'],
            name='回归系数'
        )

        less2ratio = len(d[d['t'].abs() > 2]) / len(d)

        fig = plotly.tools.make_subplots(rows=2, cols=2, subplot_titles=('t序列', 'R2序列', '回归系数', ''))

        fig.append_trace(trace0, 1, 1)
        fig.append_trace(trace1, 1, 2)
        fig.append_trace(trace2, 2, 1)

        fig['layout'].update(title='%s-%s %s %s 中性化分析' % (self.start, self.end, self.factor_name, self.neutralizer_name))

        if not os.path.exists('report\\' + self.neutralizer_name + 'Neutralized'):
            os.mkdir('report\\' + self.neutralizer_name + 'Neutralized')
        plotly.offline.plot(fig, filename="report\\" + self.neutralizer_name + 'Neutralized'+ "\\" + self.factor_name + ('-exclude_ipo' if self.exclude_ipo else '') + '.html', auto_open=True)

        print('|t|>2 比例 %5.2f' % less2ratio)
        print('R2平均值 %5.2f%%' % (d['r_square_adj'].mean() * 100))
        print('回归系数均值: %5.2f%%' % (d['beta'].mean() * 100))

    def store(self):
        if not self.residual.empty:
            root = os.path.join('D:\\data\\factor', self.neutralizer_name + 'Neutralized')
            if not os.path.exists(root):
                os.mkdir(root)
            f = os.path.join(root, self.factor_name + '.csv')
            if not os.path.exists(f):
                with open(f, 'w+'):
                    pass
                df = self.residual
                df.to_csv(f, header=True)
                return None
            else:
                try:
                    df = pd.read_csv(f, index_col=0, parse_dates=True)
                    d = self.residual[~self.residual.index.isin(df.index)]
                    if not d.empty:
                        df = df.append(d)
                        df.sort_index(inplace=True)
                    df.to_csv(f)
                except Exception as e:
                    self.logger.error(e)
                    self.logger.info('%s %s Neutralizer store wrong' % (self.factor_name, self.neutralizer_name))


if __name__ == '__main__':
    neutralizer = SingleNeutralizer('up_attack', '20160720', '20160731', 'Turnover')
    neutralizer.run()
