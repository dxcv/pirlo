#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: TushareSource.py
@time: 2018/12/17 14:46
"""
import tushare as ts
import pandas as pd

import Log


class TushareSource(object):

    def __init__(self):
        self.token = '75315937e4585518e01ac1ef804ba6e8a296af1d831ea67378b16591'
        ts.set_token(self.token)
        self.logger = Log.get_logger(__name__)
        self.pro = ts.pro_api()

    def get_tradingday(self, start, end):
        """
        返回交易日序列
        :param start: '20150101'
        :param end: '20150130'
        :return: tradingDay: ['20150101', '20150101']
        """
        df = self.pro.query('trade_cal', start_date=start, end_date=end)
        tradingdays = [str(s) for s in df[df['is_open'] == 1]['cal_date'].values]
        return tradingdays

    def get_universe(self, tradingDay):
        """
        返回在交易日之前的全部证券基础信息
        :param tradingDay: '20150101'
        :return: df : pd.DataFrame
        """

        listed_stock = self.pro.query('stock_basic', exchange_id='', list_status='L',
                                      fields='ts_code,name,list_date,delist_date,list_status')
        listed_stock['delist_date'] = '29991231'
        delisted_stock = self.pro.query('stock_basic', exchange_id='', list_status='D',
                                        fields='ts_code,name,list_date,delist_date,list_status')
        data = listed_stock.append(delisted_stock)

        data['Type'] = 'EQA'
        data = data[data['list_date'] <= tradingDay]
        data = data.loc[data['ts_code'].map(lambda x: x[0: 6].isdigit())]
        df = pd.DataFrame(
            {'Symbol': data['ts_code'].map(lambda x: x.lower()), 'ChineseName': data['name'],
             'Type': data['Type'].transform(lambda x: str(x)),
             'ListDate': data['list_date'].transform(lambda x: str(x)),
             'DelistDate': data['delist_date']})

        return df

    def get_tradableItem(self, tradingDay):
        """
        返回交易日证券Item列表是否可交易用一列标识
        :param tradingDay: '20150101'
        :return: pd.DataFrame
        """
        listed_stock = self.pro.query('stock_basic', exchange_id='', list_status='L',
                                      fields='ts_code,name,list_date,delist_date,list_status')
        listed_stock['delist_date'] = '29991231'

        suspend = self.pro.suspend(suspend_date=tradingDay)
        # suspend stock
        suspend_stock = listed_stock[listed_stock['ts_code'].isin(suspend['ts_code'])]
        suspend_stock['isTradable'] = 0
        # tradable stock
        tradable_stock = listed_stock[listed_stock['ts_code'].isin(suspend['ts_code']).map(lambda x: not x)]
        tradable_stock['isTradable'] = 1
        # delisted_stock
        delisted_stock = self.pro.query('stock_basic', exchange_id='', list_status='D',
                                        fields='ts_code,name,list_date,delist_date,list_status')
        delisted_stock['isTradable'] = 0

        data = tradable_stock.append(delisted_stock).append(suspend_stock)
        data['Type'] = 'EQA'
        data = data[data['list_date'] <= tradingDay]

        df = pd.DataFrame(
            {'Symbol': data['ts_code'].map(lambda x: x.lower()), 'ChineseName': data['name'],
             'Type': data['Type'].transform(lambda x: str(x)), 'isTradable': data['isTradable']})
        return df

    def get_tradableList(self, tradingDay):
        """
        返回交易日可交易证券列表
        :param tradingDay: '20150101’
        :return: ['600000.sh', '000001.sz']
        """
        data = self.pro.query('stock_basic', exchange_id='', list_status='L',
                              fields='ts_code,name,list_date,delist_date,list_status')
        data['delist_date'] = '29991231'
        data = data[(data['delist_date'] >= tradingDay) & (data['list_date'] <= tradingDay)]

        suspend = self.pro.suspend(suspend_date=tradingDay)

        stockId = data[data['ts_code'].isin(suspend['ts_code']).map(lambda x: not x)]['ts_code'].map(
            lambda x: x.lower()).values.tolist()

        return stockId

    def getDividend(self, start, end):
        """
        获得除权除息数据
        :param start: '20150101'
        :param end: '20160101'
        :return: pd.DataFrame
        """

        tradingDay = self.get_tradingday(start, end)
        df = pd.concat([self.pro.adj_factor(ts_code='', trade_date=t) for t in tradingDay])
        df['AdjustingFactor'] = None
        df['AdjustingConst'] = None

        data = pd.DataFrame(
            {'Symbol': df['ts_code'].transform(lambda x: x.lower()), 'ExdiviDate': df['trade_date'],
             'AdjustingFactor': df['AdjustingFactor'], 'AdjustingConst': df['AdjustingConst'],
             'RatioAdjustingFactor': df['adj_factor']})

        return data

    def getDayBar(self, symbol, start, end):
        """
        获得一只股票的日线数据
        :param symbol: '600000.sh'
        :param start: '20150101'
        :param end: '20160101'
        :return: pd.DataFrame
        """
        try:
            data = self.pro.query('daily', ts_code=symbol.upper(), start_date=start, end_date=end)
            df = pd.DataFrame(
                {'Date': data['trade_date'], 'Open': data['open'], 'Close': data['close'], 'High': data['high'],
                 'Low': data['low'], 'Volume': data['vol'] * 100, 'Turnover': data['amount'] * 1000,
                 'PreClose': data['pre_close']})
            return df
        except Exception as e:
            self.logger.error(e)
            self.logger.error('%s %s-%s day bar occurs error during downloading' % (symbol, start, end))
            return pd.DataFrame()

    def getInnerdayKline(self, symbol, tradingday):
        pass

    def getDayBarByDay(self, tradingDay):
        """
        获得一天的所有股票的日线数据
        :param tradingDay: '20150101'
        :return:
        """
        try:
            data = self.pro.query('daily', trade_date=tradingDay)
        except Exception as e:
            self.logger.error(e)
            self.logger.error('%s day bar occurs error during downloading' % tradingDay)
        df = pd.DataFrame(
            {
                'Symbol': data['ts_code'].map(lambda x: x.lower()),
                'Open': data['open'],
                'Close': data['close'],
                'High': data['high'],
                'Low': data['low'],
                'Volume': data['vol'] * 100,
                'Turnover': data['amount'] * 1000,
                'PreClose': data['pre_close']
            }
        )
        return df

    def getIndexUniverse(self, tradingDay):
        """
        返回交易日之前的指数基本信息
        :param tradingDay: '20150101'
        :return: pd.DataFrame()
                     'Symbol', 'Name', 'Market', 'Publisher', 'Category', 'ListDate'
        """
        # 上交所指数
        # sse = self.pro.index_basic(market='SSE')
        # 深交所指数
        # szse = self.pro.index_basic(market='SZSE')
        # MSCI指数
        # msci = self.pro.index_basic(market='MSCI')
        # 申万指数
        sw = self.pro.index_basic(market='SW')
        # 中金所指数
        # cicc = self.pro.index_basic(market='CICC')
        # 国证指数
        # cni = self.pro.index_basic(market='CNI')
        # 中证指数
        # csi = self.pro.index_basic(market='CSI')

        df = sw
        # df = pd.concat([sse, szse, msci, sw, cicc, cni, csi])
        # df = self.pro.query('index_basic', market='SW', fields='ts_code,name,market,list_date,exp_date')
        df = df[df['list_date'] <= tradingDay]
        df.rename(columns={'ts_code': 'Symbol', 'name': 'Name', 'publisher': 'Publisher', 'category': 'Category',
                           'list_date': 'ListDate'}, inplace=True)

        df = df[(df['Publisher'] == '申万研究') & (df['Category'] == '一级行业指数')]

        df['Symbol'] = df['Symbol'].map(lambda x: x[0:6])

        del df['base_date']
        del df['base_point']
        del df['Category']
        del df['market']

        df = df.append({'Symbol': '000001', 'Name': '上证指数', 'ListDate': '19901219', 'Publisher': '上海证券交易所'},
                       ignore_index=True)
        df = df.append({'Symbol': '000016', 'Name': '上证50', 'ListDate': '20040102', 'Publisher': '上海证券交易所'},
                       ignore_index=True)
        df = df.append({'Symbol': '000300', 'Name': '沪深300', 'ListDate': '20050408', 'Publisher': '中证指数'},
                       ignore_index=True)
        df = df.append({'Symbol': '399001', 'Name': '深圳成指', 'ListDate': '19950123', 'Publisher': '深圳证券交易所'},
                       ignore_index=True)
        df = df.append({'Symbol': '399005', 'Name': '中小板指', 'ListDate': '20060124', 'Publisher': '深圳证券交易所'},
                       ignore_index=True)
        df = df.append({'Symbol': '399006', 'Name': '创业板指', 'ListDate': '20100601', 'Publisher': '上海证券交易所'},
                       ignore_index=True)
        df = df.append({'Symbol': '000905', 'Name': '中证500', 'ListDate': '20070115', 'Publisher': '中证指数'},
                       ignore_index=True)
        df = df.append({'Symbol': '000906', 'Name': '中证800', 'ListDate': '20070115', 'Publisher': '中证指数'},
                       ignore_index=True)
        df = df.append({'Symbol': '000852', 'Name': '中证1000', 'ListDate': '20141017', 'Publisher': '中证指数'},
                       ignore_index=True)

        return df

    def getIndexDayBar(self, symbol, start, end):
        """
        返回指数日线数据
        :param symbol: '399001'
        :param start: '20180901'
        :param end: '20180914'
        :return:
        """
        try:
            sh_index = ['000001', '000016', '000300']
            if symbol in sh_index:
                symbol = symbol + '.SH'
            if symbol.startswith('399'):
                symbol = symbol + '.SZ'

            df = self.pro.index_daily(ts_code=symbol.upper())
        except Exception as e:
            self.logger.error(e)
            self.logger.error('%s %s-%s index daybar interrupts during downloading' % (symbol, start, end))
        df = df[(df['trade_date'] >= start) & (df['trade_date'] <= end)]
        data = df[['trade_date', 'open', 'close', 'high', 'low', 'vol', 'amount', 'pre_close']]
        data.rename(columns={'trade_date': 'Date', 'open': 'Open', 'close': 'Close', 'high': 'High', 'low': 'Low',
                             'vol': 'Volume', 'amount': 'Turnover', 'pre_close': 'PreClose'}, inplace=True)
        data['Volume'] = data['Volume'] * 100
        data['Turnover'] = data['Turnover'] * 1000
        return data

    def getIndexWeight(self, symbol, tradingday):
        try:
            sh_index = ['000001', '000016', '000300']
            if symbol in sh_index:
                symbol = symbol + '.SH'
            if symbol.startswith('399'):
                symbol = symbol + '.SZ'

            df = self.pro.index_weight(index_code=symbol.upper(), trade_date=tradingday)
            df.rename(columns={'con_code': 'Symbol', 'weight': 'Weight'}, inplace=True)
            del df['index_code']
            del df['trade_date']
            return df
        except Exception as e:
            self.logger.error(e)
            self.logger.error('%s %s index weight occur error during downloading' % (index, tradingday))

    def getBalanceSheet(self, start, end, symbol):
        try:
            data = self.pro.balancesheet(ts_code=symbol, start_date=start, end_date=end)
            del data['ts_code'], data['ann_date'], data['comp_type'], data['report_type']
            df = data
            return df
        except Exception as e:
            self.logger.error(e)
            self.logger.error('%s %s-%s balanceSheet occurs error during downloading' % (symbol, start, end))
            return pd.DataFrame()

    def getIncomeSheet(self, start, end, symbol):
        try:
            data = self.pro.income(ts_code=symbol, start_date=start, end_date=end)
            del data['ts_code'], data['ann_date'], data['comp_type'], data['report_type']
            df = data
            return df
        except Exception as e:
            self.logger.error(e)
            self.logger.error('%s %s-%s balanceSheet occurs error during downloading' % (symbol, start, end))
            return pd.DataFrame()

    def getCashFlow(self, start, end, symbol):
        try:
            data = self.pro.cashflow(ts_code=symbol, start_date=start, end_date=end)
            del data['ts_code'], data['ann_date'], data['comp_type'], data['report_type']
            df = data
            return df
        except Exception as e:
            self.logger.error(e)
            self.logger.error('%s %s-%s balanceSheet occurs error during downloading' % (symbol, start, end))
            return pd.DataFrame()


if __name__ == '__main__':
    ts = TushareSource()

    df = ts.get_tradableList('20180102')
    print(df)