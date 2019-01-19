#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: test.py
@time: 2019/1/6 14:01
"""
from DataService.TNDataService import TNDataSerivce

from DataService.TradableListService import TradableListSerivce
if __name__ == '__main__':
    universe = TradableListSerivce.getUniverse('19990201')
    print(universe)
    # universe = ['600000.sh', '000002.sz']
    # df = TNDataSerivce.getMarketCap(universe, '20181001', '20181101')
    df = TNDataSerivce.getTNData(universe, '19990101', '19990201', 'MarketCap')
    print(df)
    x = [s for s in df.columns if s not in universe]
    print(x)