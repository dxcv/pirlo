#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: RegressTestor.py
@time: 2018/12/18 19:53
"""
import os
import Log

import statsmodels.api as sm
import pandas as pd
import numpy as np
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

        self.logger = Log.get_logger(__name__)

    def run(self):
        self.test()

    def test(self):
        self.load()
        self.compute()
        self.store()
        self.report()

    def load(self):
        self.load_factor()
        self.load_ret()

    def load_factor(self):
        df = FactorService.FactorService.get_factor(self.factor_name, self.start, self.end, self.factor_type)
        self.factor_data = df

    def load_ret(self):
        self.tradingDay = TradingDayService.getTradingDay(self.start, self.end)
        end_day = TradingDayService.getRelativeTradingDay(self.tradingDay[-1], 1)
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
        if X.size==0:
            return None, None, None
        if np.max(X) == np.min(X):
            return None, None, None
        X = sm.add_constant(X)
        results = sm.OLS(y, X).fit()
        r_square_adj = results.rsquared_adj
        factor_ret = results.params[1]
        t = results.tvalues[1]
        return t, r_square_adj, factor_ret

    def store(self):
        root = 'D:\\data\\factorRet'
        if not os.path.exists(root):
            os.mkdir(root)
        root = os.path.join(root, self.universe_code)
        if not os.path.exists(root):
            os.mkdir(root)
        root = os.path.join(root, ('ExcludeLimit' if self.exclude_limit else '') + ('ExcludeSt' if self.exclude_st else '') + ('ExcludeIpo' if self.exclude_ipo else ''))
        if not os.path.exists(root):
            os.mkdir(root)
        data = pd.DataFrame(data={'factor_ret': self.factor_ret, 't': self.t, 'r_square_adj': self.r_square_adj}, index=pd.to_datetime(self.tradingDay))
        if not data.empty:
            f = os.path.join(root, self.factor_name + '.csv')
            if not os.path.exists(f):
                with open(f, 'w+'):
                    pass
                data.to_csv(f, header=True)
                return None
            else:
                try:
                    df = pd.read_csv(f, index_col=0, parse_dates=True)
                    d = data[~data.index.isin(df.index)]
                    if not d.empty:
                        df = df.append(d)
                        df.sort_index(inplace=True)
                    df.to_csv(f)
                except Exception as e:
                    self.logger.error(e)
                    self.logger.info('%s factor_ret store wrong' % self.factor_name)

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


        fig['layout'].update(title='%s-%s %s 因子均值:%5.2f 因子分析 |t|>2 比例 %5.2f R2平均值 %5.2f%%' % (self.start, self.end, self.factor_name, d['factor_ret'].mean()*100, less2ratio, d['r_square_adj'].mean()*100))

        store_dir = 'report\\RegressTest\\' + self.universe_code + "\\"
        if not os.path.exists(store_dir):
            os.mkdir(store_dir)

        plotly.offline.plot(fig, filename=store_dir + self.factor_name + ('-excludelimit-' if self.exclude_limit else '-') +('-excludeipo-' if self.exclude_ipo else '-') + ('-excludest-' if self.exclude_st else '-') + self.factor_type +'.html', auto_open=False)

        print('|t|>2 比例 %5.2f' % less2ratio)
        print('R2平均值 %5.2f%%' % (d['r_square_adj'].mean()*100))



if __name__ == '__main__':
    testor = RegressTestor('orderNum', 'TurnoverNeutralized', '20130101', '20130201')

    testor.test()