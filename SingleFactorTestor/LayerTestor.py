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
from DataService.IndexService import IndexService
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
            ret_today = self.ret.loc[today].dropna()

            if self.exclude_limit:
                ret_today = ret_today[ret_today.abs() < 0.095]

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

            valid_symbol = [s for s in factor.keys() if (s in universe) and (s in ret.keys()) and (s in ret_today.keys()) and (s in tradableList_today) and (s not in ipo_stock) and (s not in st_stock)]

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
        tradingday_next = TradingDayService.getTradingDay(TradingDayService.getRelativeTradingDay(tradingday[0], 1), TradingDayService.getRelativeTradingDay(tradingday[-1], 1))

        self.layer_value.index = pd.to_datetime(tradingday_next)
        # self.layer_value = (self.layer_value + 1).cumprod()

    def report(self):
        data = []

        tradingday = TradingDayService.getTradingDay(self.start, self.end)
        tradingday = TradingDayService.getTradingDay(TradingDayService.getRelativeTradingDay(tradingday[0], 1), TradingDayService.getRelativeTradingDay(tradingday[-1], 1))
        start_day = tradingday[0]
        end_day = tradingday[-1]
        dapan = IndexService.getIndexRet('000001', start_day, end_day)
        hs300 = IndexService.getIndexRet('000300', start_day, end_day)
        sz50 = IndexService.getIndexRet('000016', start_day, end_day)
        zz500 = IndexService.getIndexRet('000905', start_day, end_day)
        zz1000 = IndexService.getIndexRet('000852', start_day, end_day)
        cyb = IndexService.getIndexRet('399006', start_day, end_day)


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

        relative = []

        trace2 = go.Scatter(
            x = t,
            # y = self.layer_value['layer2'] - self.layer_value['layer4'],
            y = ((self.layer_value['layer1'] - self.layer_value['layer'+str(self.layer_num)]) + 1).cumprod(),
            name = '多空组合'
        )
        relative.append(trace2)
        trace3 = go.Scatter(
            x = t,
            y = ((self.layer_value['layer1'] - dapan) + 1).cumprod(),
            name = 'layer1相对大盘'
        )
        relative.append(trace3)
        trace4 = go.Scatter(
            x = t,
            y = ((self.layer_value['layer1'] - cyb) + 1).cumprod(),
            name = 'layer1相对创业板'
        )
        relative.append(trace4)
        trace5 = go.Scatter(
            x = t,
            y = ((self.layer_value['layer1'] - hs300) + 1).cumprod(),
            name = 'layer1相对沪深300'
        )
        relative.append(trace5)
        trace6 = go.Scatter(
            x = t,
            y = ((self.layer_value['layer1'] - zz500) + 1).cumprod(),
            name = 'layer1相对中证500'
        )
        relative.append(trace6)
        trace7 = go.Scatter(
            x = t,
            y = ((self.layer_value['layer1'] - zz1000) + 1).cumprod(),
            name = 'layer1相对中证1000'
        )
        relative.append(trace7)
        trace8 = go.Scatter(
            x = t,
            y = ((self.layer_value['layer1'] - sz50) + 1).cumprod(),
            name = 'layer1相对上证50'
        )
        relative.append(trace8)

        layerRelative = []

        if self.universe_code == 'all':
            relative_index = dapan
            layerRelative = self.getLayerRelative(relative_index, t, '大盘')
        if self.universe_code == '000852':
            relative_index = zz1000
            layerRelative = self.getLayerRelative(relative_index, t, '中证1000')
        if self.universe_code == '000905':
            relative_index = zz500
            layerRelative = self.getLayerRelative(relative_index, t, '中证500')
        if self.universe_code == '000300':
            relative_index = hs300
            layerRelative = self.getLayerRelative(relative_index, t, '沪深300')
        if self.universe_code == '000016':
            relative_index = sz50
            layerRelative = self.getLayerRelative(relative_index, t, '上证50')
        if self.universe_code == '399006':
            relative_index = cyb
            layerRelative = self.getLayerRelative(relative_index, t, '创业板')

        relative.extend(layerRelative)
        for trace in relative:
            fig.append_trace(trace, 1, 2)

        fig['layout'].update(title='%s-%s %s 因子分层分析' % (self.start, self.end, self.factor_name))

        store_dir = 'report\\LayerTest\\' + self.universe_code + "\\"
        if not os.path.exists(store_dir):
            os.mkdir(store_dir)

        plotly.offline.plot(fig, filename=store_dir + self.factor_name + ('-excludelimit-' if self.exclude_limit else '-')  + ('-excludeipo-' if self.exclude_ipo else '-') + ('-excludest-' if self.exclude_st else '-') + ('marketweight-' if self.market_weight else 'equalweight-') + self.factor_type + '-' + str(self.layer_num) + '-layer.html', auto_open=True)

    def getLayerRelative(self, relative_index, t, index_name):
        data = []

        trace9 = go.Scatter(
            x=t,
            y=((self.layer_value['layer2'] - relative_index) + 1).cumprod(),
            name='layer2相对' + index_name
        )
        trace10 = go.Scatter(
            x=t,
            y=((self.layer_value['layer3'] - relative_index) + 1).cumprod(),
            name='layer3相对' + index_name
        )
        trace11 = go.Scatter(
            x=t,
            y=((self.layer_value['layer4'] - relative_index) + 1).cumprod(),
            name='layer4相对' + index_name
        )
        trace12 = go.Scatter(
            x=t,
            y=((self.layer_value['layer5'] - relative_index) + 1).cumprod(),
            name='layer5相对' + index_name
        )
        data.append(trace9)
        data.append(trace10)
        data.append(trace11)
        data.append(trace12)
        return data


if __name__ == '__main__':
    testor = LayerTestor('Turnover', 'mad-normal', '20150805', '20150820', '399006')
    testor.run()
