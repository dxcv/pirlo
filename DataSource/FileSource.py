#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: FileSource.py
@time: 2019/1/6 13:29
"""
import os

import pandas as pd

import Log
class FileSource(object):

    def __init__(self):
        self.static_root = "D:\\data\\static"
        self.logger = Log.get_logger(__name__)

    def get_industry_factor(self, tradingday):
        pass

    def get_index_component(self, tradingday, index):
        index_root = os.path.join(self.static_root, 'index', 'indexweight', tradingday)
        if not os.path.exists(index_root):
            self.logger.error('%s %s got index component wrong' % (index, tradingday))
            return pd.DataFrame()
        else:
            f = os.path.join(index_root, index + '.csv')
            try:
                df = pd.read_csv(f)
                return df
            except Exception as e:
                self.logger.error(e)
                self.logger.info('%s %s got index component wrong' % (index, tradingday))
                return pd.DataFrame()

    def getTNData(self, start, end ,type):
        tndata_root = os.path.join(self.static_root, 'stock', 'TNData')

        basedir = os.listdir(tndata_root)
        for d in basedir:
            tmpdir = os.path.join(tndata_root, d)
            tmpfiles = os.listdir(tmpdir)
            tmp_file_name = [os.path.splitext(x)[0] for x in tmpfiles]
            if type in tmp_file_name:
                f = os.path.join(tndata_root, d, type + '.csv')
                break
        if f is None:
            self.logger.error('%s %s-%s got TNData Wrong, has no data')
            return pd.DataFrame()
        # dir = os.path.join(self.static_root, 'stock', 'TNData', 'jydb')
        # f = os.path.join(dir, type + '.csv')
        try:
            df = pd.read_csv(f, index_col=0, parse_dates=True)
            data = df[start: end].dropna(axis=1, how='all')
            return data
        except Exception as e:
            self.logger.error(e)
            self.logger.info('%s %s-%s got TNData wrong' % (type, start, end))
            return pd.DataFrame()

    def get_universe(self, t):
        dir = os.path.join(self.static_root, 'stock', 'universe', 'jydb')
        f = os.path.join(dir, t + '.csv')
        try:
            df = pd.read_csv(f, index_col=False)
            df['ListDate'] = df['ListDate'].map(lambda x : str(x))
            df['DelistDate'] = df['DelistDate'].map(lambda x :str(x))
            return df
        except Exception as e:
            self.logger.error(e)
            self.logger.info('%s got universe Wrong' % t)
            return pd.DataFrame()

file_source = FileSource()
if __name__ == '__main__':
    # df = file_source.getMarketCap('000001.sz', '20180101', '20180301')
    # df = file_source.getTNData('20130101', '20130201', 'Ret')
    df = file_source.get_index_component('20010102', '000300')
    print(df)