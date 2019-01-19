#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: RegressTestor.py
@time: 2018/12/18 19:53
"""
import os

import statsmodels.api as sm
import pandas as pd
import plotly
import plotly.graph_objs as go

import datetime

from DataService import TradableListService, TNDataService, FactorService
from DataService.TradingDayService import TradingDayService


class RegressTestor(object):

    def __init__(self, factor_name, factor_type, start, end, universe_code):
        self.factor_name = factor_name

        self.factor_type = factor_type
        self.start = start
        self.end = end

        self.exclude_ipo = True
        self.exclude_st = True
        self.exclude_limit = True

        self.ret = None
        self.factor_data = None
        self.t = []
        self.r_square_adj = []
        self.factor_ret = []
        self.universe_code = universe_code

    def run(self):
        self.test()

    def test(self):
        self.load()
        self.compute()
        self.report()

    def load(self):
        self.load_factor()
        self.load_ret()

    def load_factor(self):
        df = FactorService.FactorService.get_factor(self.factor_name, self.start, self.end, self.factor_type)
        self.factor_data = df

    def load_ret(self):
        tradingDay = TradingDayService.getTradingDay(self.start, self.end)
        end_day = TradingDayService.getRelativeTradingDay(tradingDay[-1], 1)
        universe = TradableListService.TradableListSerivce.getUniverse(end_day)
        df = TNDataService.TNDataSerivce.getTNData(universe, self.start, end_day, 'Ret')
        self.ret = df

    def compute(self):
        for idx, row in self.factor_data.iterrows():
            today = idx.strftime('%Y%m%d')
            print(today)
            next_day = TradingDayService.getRelativeTradingDay(today, 1)
            factor = row.dropna()
            tradable = TradableListService.TradableListSerivce.getTradableStock(today)
            ret = self.ret.loc[pd.to_datetime(next_day)].dropna() / 100

            if self.exclude_limit:
                ret = ret[ret.abs() < 0.095]

            if self.exclude_ipo:
                ipo_stock = TradableListService.TradableListSerivce.getIpoStock(TradingDayService.getRelativeTradingDay(today, -60), today)
            else:
                ipo_stock = []

            if self.exclude_st:
                st_stock = TradableListService.TradableListSerivce.getStStock(today)
            else:
                st_stock = []

            if self.universe_code == 'all':
                universe = tradable
            else:
                universe = TradableListService.TradableListSerivce.getIndexComponent(today, self.universe_code)

            valid_symbol = [s for s in tradable if (s in universe) and (s in ret.keys()) and (s in factor.keys()) and (s not in ipo_stock) and (s not in st_stock)]

            if ret[valid_symbol].isna().all():
                t, r_square_adj, factor_ret = None, None, None
            else:
                ret = ret[valid_symbol].values
                factor_value = factor[valid_symbol].values

                t, r_square_adj, factor_ret = self.regress(ret, factor_value)

            self.t.append(t)
            self.r_square_adj.append(r_square_adj)
            self.factor_ret.append(factor_ret)
            if t:
                print('t:%5.2f, r_square_adj:%8.5f, factor_ret:%8.5f' % (t, r_square_adj, factor_ret))
            else:
                print('t:%s, r_square_adj:%s, factor_ret:%s' % (t, r_square_adj, factor_ret))

    def regress(self, y, X):
        X = sm.add_constant(X)
        results = sm.OLS(y, X).fit()
        r_square_adj = results.rsquared_adj
        factor_ret = results.params[1]
        t = results.tvalues[1]
        return t, r_square_adj, factor_ret

    def report(self):
        tradingday = TradingDayService.getTradingDay(self.start, self.end)

        t = [datetime.datetime.strptime(s, '%Y%m%d').strftime('%Y-%m-%d') for s in tradingday]
        d = pd.DataFrame(data={'Date': t, 't': self.t, 'factor_ret': self.factor_ret, 'r_square_adj': self.r_square_adj})

        trace0 = go.Bar(
            x = d['Date'],
            y = d['t'],
            name = 't序列'
        )

        trace1 = go.Bar(
            x = d['Date'],
            y = d['r_square_adj'],
            name = 'R2序列'
        )

        trace2 = go.Scatter(
            x = d['Date'],
            y = (d['factor_ret'] + 1).cumprod(),
            name = '因子收益率'
        )
        less2ratio = len(d[d['t'].abs() > 2]) / len(d)

        fig = plotly.tools.make_subplots(rows=2, cols=2, subplot_titles=('t序列', 'R2序列', '因子收益率', ''))

        fig.append_trace(trace0, 1, 1)
        fig.append_trace(trace1, 1, 2)
        fig.append_trace(trace2, 2, 1)


        fig['layout'].update(title='%s-%s %s 因子分析' % (self.start, self.end, self.factor_name))

        store_dir = 'report\\RegressTest\\' + self.universe_code + "\\"
        if not os.path.exists(store_dir):
            os.mkdir(store_dir)

        plotly.offline.plot(fig, filename=store_dir + self.factor_name + ('-excludelimit-' if self.exclude_limit else '-') +('-excludeipo-' if self.exclude_ipo else '-') + ('-excludest-' if self.exclude_st else '-') + self.factor_type +'.html', auto_open=True)

        print('|t|>2 比例 %5.2f' % less2ratio)
        print('R2平均值 %5.2f%%' % (d['r_square_adj'].mean()*100))



if __name__ == '__main__':
    testor = RegressTestor('Volume', 'marketCapNeutralized', '20181201', '20181231')

    testor.test()