#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: TurnoverRateEstimator.py
@time: 2019/2/19 13:38
"""
import pandas as pd
import numpy as np
from DataService.FactorService import FactorService
from DataService.TNDataService import TNDataSerivce
from DataService.TradingDayService import TradingDayService
from DataService.TradableListService import TradableListSerivce

import plotly
import plotly.graph_objs as go

import datetime

import os

class TurnoverRateEstimator(object):

    def __init__(self, factor_name, factor_type, start, end, unierse):
        self.exclude_st = True
        self.exclude_ipo = True
        self.exclude_limit = True
        self.factor_name = factor_name
        self.factor_type = factor_type
        self.start = start
        self.end = end
        self.universe_code = unierse

        self.factor_data = None
        self.ret = None
        self.layer_num = 5

    def run(self):
        self.read()
        self.compute()
        self.report()

    def compute(self):
        tradingday = TradingDayService.getTradingDay(self.start, self.end)
        result = {}
        for idx, today in enumerate(tradingday):
            print(today)
            next_day = TradingDayService.getRelativeTradingDay(today, 1)
            factor = self.factor_data.loc[today].dropna()
            ret = self.ret.loc[next_day].dropna()
            ret_today = self.ret.loc[today].dropna()

            if self.exclude_limit:
                ret_today = ret_today[ret_today.abs() < 0.095]

            tradableList_today = TradableListSerivce.getTradableStock(today)

            if self.exclude_ipo:
                ipo_stock = TradableListSerivce.getIpoStock(TradingDayService.getRelativeTradingDay(today, -60), today)
            else:
                ipo_stock = []

            if self.exclude_st:
                st_stock = TradableListSerivce.getStStock(today)
            else:
                st_stock = []

            if self.universe_code == 'all':
                universe = tradableList_today
            else:
                universe = TradableListSerivce.getIndexComponent(today, self.universe_code)

            valid_symbol = [s for s in factor.keys() if (s in universe) and (s in ret.keys()) and (s in ret_today.keys()) and (s in tradableList_today) and (s not in ipo_stock) and (s not in st_stock)]

            factor = factor[valid_symbol]

            df = pd.DataFrame(data={'factor': factor})

            df.sort_values(by='factor', ascending=False, inplace=True)

            data_num = len(df)

            num_per_layer = np.floor(data_num / self.layer_num)
            s = 0


            for i in range(self.layer_num):
                if i == self.layer_num-1:
                    stocks = df['factor'].iloc[s: ].index
                else:
                    stocks = df['factor'].iloc[s: int(s+num_per_layer)].index

                s = int(s+num_per_layer)
                if i == 0:
                    result[today] = {}
                result[today]['layer'+str(i+1)] = pd.Series({symbol: 1 for symbol in stocks})
                # s = int(s + num_per_layer)

        yesterday = tradingday[0]

        layer_turnoverRate = np.zeros((len(tradingday)-1, self.layer_num))
        for idx, today in enumerate(tradingday):
            if idx == 0:
                continue
            else:
                for i in range(self.layer_num):
                    y = result[yesterday]['layer'+str(i+1)]
                    x = result[today]['layer'+str(i+1)]
                    union_stocks = x.index.tolist()
                    y_stock = y.index.tolist()
                    union_stocks.extend(y_stock)

                    x = x.reindex(union_stocks, fill_value=0)
                    y = y.reindex(union_stocks, fill_value=0)
                    x = x / len(x) if len(x) !=0 else x
                    y = y / len(y) if len(y) !=0 else y
                    z = (x-y).abs().sum()
                    layer_turnoverRate[idx-1, i] = z
                yesterday = today

        self.layer_turnoverRate = pd.DataFrame(layer_turnoverRate, columns=['layer'+str(s+1) for s in range(self.layer_num)])
        # tradingday_next = TradingDayService.getTradingDay(TradingDayService.getRelativeTradingDay(tradingday[0], 1), TradingDayService.getRelativeTradingDay(tradingday[-1], 1))
        self.layer_turnoverRate.index = pd.to_datetime(tradingday[1:])

    def read(self):
        df = FactorService.get_factor(self.factor_name, self.start, self.end, self.factor_type)
        self.factor_data = df
        tradingDay = TradingDayService.getTradingDay(self.start, self.end)
        end_day = TradingDayService.getRelativeTradingDay(tradingDay[-1], 1)
        universe = TradableListSerivce.getUniverse(end_day)
        df = TNDataSerivce.getTNData(universe, self.start, end_day, 'Ret') / 100
        self.ret = df

    def report(self):
        tradingday = TradingDayService.getTradingDay(self.start, self.end)
        t = [datetime.datetime.strptime(s, '%Y%m%d').strftime('%Y-%m-%d') for s in tradingday[1:]]
        data = []
        for col in self.layer_turnoverRate:
            data.append(go.Scatter(
                x = t,
                y = self.layer_turnoverRate[col],
                name = col
            ))
        x = ''.join([col + ':' + str(self.layer_turnoverRate[col].mean()) + ' ' for col  in self.layer_turnoverRate])
        layout = dict(title=('换手率估计 ' + x))
        fig = dict(data=data, layout=layout)

        store_dir = 'G:\\report\\estimator\\TurnoverRateEstimator\\' + self.universe_code + "\\"
        if not os.path.exists(store_dir):
            os.mkdir(store_dir)

        plotly.offline.plot(fig, filename=store_dir + self.factor_name + ('-excludelimit-' if self.exclude_limit else '-')  + ('-excludeipo-' if self.exclude_ipo else '-') + ('-excludest-' if self.exclude_st else '-') + self.factor_type + '-' + str(self.layer_num) + '-layer.html', auto_open=True)

if __name__ == '__main__':
    estimator = TurnoverRateEstimator('tradeNum', 'mad-normal', '20170322', '20170327', '399006')
    estimator.run()