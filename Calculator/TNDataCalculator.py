#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: TNDataCalculator.py
@time: 2018/12/17 14:34
"""
import os

import pandas as pd
import numpy as np
import Log
from DataService import TradableListService, TNDataService

from Normalizer.Deextremer import ThreeSigmaDeextremer, MADDeextremer, TrivialDeextremer
from Normalizer.Standardizator import NormalStandardlizer, TrivialStandardlizer


class TNDataCalculator(object):

    def __init__(self, type, start, end, logged, deextremer='mad', standardlizer='normal'):
        self.type = type
        self.start = start
        self.end = end
        self.logged = logged
        if deextremer or standardlizer:
            self.normalized = True
        else:
            self.normalized = False
        self.deextremer = None
        self.standardizer = None
        self.normalizedname = ''
        self.init_normalizer(deextremer, standardlizer)
        # self.dataservice = DataService()
        # self.static_dataservice = StaticDataService()
        self.logger = Log.get_logger(__name__)

    def init_normalizer(self, deextremer, standardlizer):
        if deextremer == '3sigma':
            self.deextremer = ThreeSigmaDeextremer()
        if deextremer == 'mad':
            self.deextremer = MADDeextremer()
        if deextremer == 'trivial':
            self.deextremer = TrivialDeextremer()
        if standardlizer == 'normal':
            self.standardizer = NormalStandardlizer()
        if standardlizer == 'trivial':
            self.standardizer = TrivialStandardlizer()
        self.normalizedname = ''.join([deextremer, '-', standardlizer])

    def run(self):
        universe = TradableListService.TradableListSerivce.getUniverse(self.end)
        # universe = ['600000.sh', '000002.sz', '300003.sz']
        df = TNDataService.TNDataSerivce.getTNData(universe, self.start, self.end, self.type)
        if self.logged:
            df[df==0] = np.NaN
            df = np.log(df)
        if self.normalized:
            df = self.normalize(df)
        self.store(df)

    def normalize(self, data):
        if self.deextremer:
            data = self.deextremer.do(data)
        if self.standardizer:
            data = self.standardizer.do(data)
        return data

    def store(self, data):
        dir = os.path.join('G:\\data\\factor', self.normalizedname)
        if not os.path.exists(dir):
            os.makedirs(dir)
        file = os.path.join(dir, self.type + ('Log' if self.logged else '') + '.csv')

        if not os.path.exists(file):
            with open(file, 'w+') as f:
                pass
            df = pd.DataFrame()
        else:
            try:
                df = pd.read_csv(file, index_col=0, parse_dates=True)
            except Exception as e:
                self.logger.error(e)
                self.logger.info('打开%s出错' % file)
                return
        d = data[~data.index.isin(df.index)]
        if not d.empty:
            df = df.append(d)
            df.sort_index(inplace=True)
        new_symbol = [s for s in data.keys().tolist() if s not in df.keys().tolist()]
        for symbol in new_symbol:
            df[symbol] = data[symbol]
        df.to_csv(file, header=True)


if __name__ == '__main__':
    calculator = TNDataCalculator('TurnoverRate', '19911219', '20181115')
    calculator.run()