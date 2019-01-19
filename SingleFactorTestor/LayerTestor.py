#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: LayerTestor.py
@time: 2018/12/18 19:59
"""
import os

import datetime
import pandas as pd
import numpy as np

from DataService import TradableListService, TNDataService, FactorService, StaticDataService
from DataService.TradingDayService import TradingDayService

import plotly
import plotly.graph_objs as go

class LayerTestor(object):

    def __init__(self, factor, factor_type, start, end, universe_code, layer_num=5,):
        self.factor_name = factor
        self.start = start
        self.end = end
        self.factor_type = factor_type

        self.market_weight = False
        self.exclude_ipo = True
        self.exclude_st = True
        self.exclude_limit = True
        self.ret = None
        self.factor_data = None

        self.layer_value = None

        self.layer_num = layer_num
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
        df = TNDataService.TNDataSerivce.getTNData(universe, self.start, end_day, 'Ret') / 100
        self.ret = df

    def compute(self):
        tradingday = TradingDayService.getTradingDay(self.start, self.end)
        layer_value = np.zeros((len(tradingday), self.layer_num))

        for idx, today in enumerate(tradingday):
            print(today)
            next_day = TradingDayService.getRelativeTradingDay(today, 1)
            factor = self.factor_data.loc[today].dropna()
            ret = self.ret.loc[next_day].dropna()
            if self.exclude_limit:
                ret = ret[ret.abs() < 0.095]
            tradableList_today = TradableListService.TradableListSerivce.getTradableStock(today)
            if self.exclude_ipo:
                ipo_stock = TradableListService.TradableListSerivce.getIpoStock(TradingDayService.getRelativeTradingDay(today, -60), today)
            else:
                ipo_stock = []

            if self.exclude_st:
                st_stock = TradableListService.TradableListSerivce.getStStock(today)
            else:
                st_stock = []

            if self.universe_code == 'all':
                universe = tradableList_today
            else:
                universe = TradableListService.TradableListSerivce.getIndexComponent(today, self.universe_code)

            valid_symbol = [s for s in factor.keys() if (s in universe) and (s in ret.keys()) and (s in tradableList_today) and (s not in ipo_stock) and (s not in st_stock)]

            factor = factor[valid_symbol]
            ret = ret[valid_symbol]

            df = pd.DataFrame(data={'factor': factor, 'ret': ret})

            if self.market_weight:
                marketCap = StaticDataService.StaticDataService.getDailyPerformanceByDay(today, 'MarketCap')
                marketCap = marketCap[valid_symbol]
                df['marketCap'] = marketCap.loc[today]

            df.sort_values(by='factor', ascending=False, inplace=True)

            data_num = len(df)

            num_per_layer = np.floor(data_num / self.layer_num)
            s = 0
            for i in range(self.layer_num):
                if i == self.layer_num - 1:
                    if self.market_weight:
                        layer_value[idx, i] = np.average(df['ret'].iloc[s:].values, weights=df['marketCap'].iloc[s:].values)
                    else:
                        layer_value[idx, i] = np.average(df['ret'].iloc[s:].values)
                else:
                    if self.market_weight:
                        layer_value[idx, i] = np.average(df['ret'].iloc[s:int(s+num_per_layer)].values, weights=df['marketCap'].iloc[s:int(s+num_per_layer)].values)
                    else:
                        layer_value[idx, i] = np.average(df['ret'].iloc[s:int(s+num_per_layer)].values)
                s = int(s + num_per_layer)

        self.layer_value = pd.DataFrame(layer_value, columns=['layer' + str(s+1) for s in range(self.layer_num)])
        self.layer_value.index = pd.to_datetime(tradingday)
        # self.layer_value = (self.layer_value + 1).cumprod()

    def report(self):
        data = []

        tradingday = TradingDayService.getTradingDay(self.start, self.end)
        tradingday = TradingDayService.getTradingDay(TradingDayService.getRelativeTradingDay(tradingday[0], 1), TradingDayService.getRelativeTradingDay(tradingday[-1], 1))
        t = [datetime.datetime.strptime(s, '%Y%m%d').strftime('%Y-%m-%d') for s in tradingday]

        for col in self.layer_value:
            data.append(go.Scatter(
                x = t,
                y = (self.layer_value[col] + 1).cumprod(),
                name = col
            ))


        fig = plotly.tools.make_subplots(rows=1, cols=2, subplot_titles=('分层', '多空'))

        for trace in data:
            fig.append_trace(trace, 1, 1)

        trace2 = go.Scatter(
            x = t,
            # y = self.layer_value['layer2'] - self.layer_value['layer4'],
            y = ((self.layer_value['layer1'] - self.layer_value['layer'+str(self.layer_num)]) + 1).cumprod(),
            name = '多空组合'
        )


        fig.append_trace(trace2, 1, 2)


        fig['layout'].update(title='%s-%s %s 因子分层分析' % (self.start, self.end, self.factor_name))
        store_dir = 'report\\LayerTest\\' + self.universe_code + "\\"
        if not os.path.exists(store_dir):
            os.mkdir(store_dir)

        plotly.offline.plot(fig, filename=store_dir + self.factor_name + ('-excludelimit-' if self.exclude_limit else '-')  + ('-excludeipo-' if self.exclude_ipo else '-') + ('-excludest-' if self.exclude_st else '-') + ('marketweight-' if self.market_weight else 'equalweight-') + self.factor_type + '-' + str(self.layer_num) + '-layer.html', auto_open=True)

if __name__ == '__main__':
    testor = LayerTestor('up_attack', 'mad-normal', '20190102', '20190115', 'all')
    testor.run()
