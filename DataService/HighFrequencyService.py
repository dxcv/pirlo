#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: HighFrequencyService.py
@time: 2019/1/8 10:08
"""
import os
import datetime

import h5py

import pandas as pd

class HighFrequencyService(object):

    high_freqency_root = 'Y:\Data'

    @classmethod
    def to_datetime(cls, time, tradingDay):
        ms = time % 1000
        time = time // 1000

        second = time % 100
        time = time // 100

        minute = time % 100
        time = time // 100
        hour = time % 100
        dt_str = tradingDay + ' ' + ''.join([str(hour), ':', str(minute), ':', str(second), ':', str(ms)])
        return datetime.datetime.strptime(dt_str, '%Y%m%d %H:%M:%S:%f')


    @classmethod
    def read_transaction(cls, symbol, tradingday):
        """
        read transaction data
        :param symbol: '600000.sh' str
        :param tradingday: '20170104' str
        :return: pd.DataFrame index-pd.Datetime
        """
        with h5py.File(os.path.join(cls.high_freqency_root, 'h5data', 'stock', 'Transaction', ''.join([tradingday, '.h5'])), 'r') as f:
            time = f[symbol]['Time']
            orderKind = f[symbol]['OrderKind']
            functionCode = f[symbol]['FunctionCode']
            bsflag = f[symbol]['BSFlag']
            price = f[symbol]['Price']
            volume = f[symbol]['Volume']
            ask_order = f[symbol]['AskOrder']
            bid_order = f[symbol]['BidOrder']

            transaction = pd.DataFrame(
                {'Time': time, 'OrderKind': orderKind, 'FunctionCode': functionCode, 'Bsflag': bsflag, 'Price': price, 'Volume': volume,
                 'AskOrder': ask_order, 'BidOrder': bid_order})
            transaction['Time'] = transaction['Time'].map(lambda x: cls.to_datetime(x, tradingday))
            transaction.set_index('Time', inplace=True)

        return transaction

    @classmethod
    def read_order(cls, symbol, tradingday):
        """
        read order data
        :param symbol: '600000.sh' str
        :param tradingday: '20170104' str
        :return: pd.DataFrame index-pd.DateTime
        """
        with h5py.File(os.path.join(cls.high_freqency_root, 'h5data', 'stock', 'order', ''.join([tradingday, '.h5'])), 'r') as f:
            time = f[symbol]['Time']
            order_number = f[symbol]['OrderNumber']
            orderKind = f[symbol]['OrderKind']
            function_code = f[symbol]['FunctionCode']
            price = f[symbol]['Price']
            volume = f[symbol]['Volume']

            order = pd.DataFrame({'Time': time, 'OrderKind': orderKind, 'OrderNumber': order_number, 'Price': price, 'Volume': volume, 'FunctionCode': function_code})
            order['Time'] = order['Time'].map(lambda x: cls.to_datetime(x, tradingday))
            order.set_index('Time', inplace=True)
        return order

    @classmethod
    def read_orderqueue(cls, symbol, tradingday):
        """
        read orderqueue data
        :param symbol: '600000.sh' str
        :param tradingday: '20170104' str
        :return: list [orderqueueitem]
                 orderqueueitem {} dict
        """
        orderqueue = []
        with h5py.File(os.path.join(cls.high_freqency_root, 'h5data', 'stock', 'orderqueue', ''.join([tradingday, '.h5'])), 'r') as f:
            time = f[symbol]['Time'][:]
            side = f[symbol]['Side'][:]
            price = f[symbol]['Price'][:]
            orderItems = f[symbol]['OrderItems'][:]
            abItems = f[symbol]['ABItems'][:]
            abVolume = f[symbol]['ABVolume'][:]

            for i in range(len(time)):
                orderqueueItem = {'Time': time[i], 'Side': side[i], 'Price': price[i], 'OrderItems': orderItems[i], 'ABItems': abItems[i], 'ABVolume': abVolume[i,:]}
                orderqueue.append(orderqueueItem)
        return orderqueue

    @classmethod
    def read_tick(cls, symbol, tradingday):
        """
        read tick data
        :param symbol: '600000.sh' str
        :param tradingday: '20170104' str
        :return: pd.DataFrame index-pd.DateTime
        """
        with h5py.File(os.path.join(cls.high_freqency_root, 'h5data', 'stock', 'tick', ''.join([tradingday, '.h5'])), 'r') as f:
            time = f[symbol]['Time']
            price = f[symbol]['Price']
            volume = f[symbol]['Volume']
            turnover = f[symbol]['Turnover']
            matchItem = f[symbol]['MatchItem']
            bsflag = f[symbol]['BSFlag']
            accVolume = f[symbol]['AccVolume']
            accTurnover = f[symbol]['AccTurnover']
            askAvgPrice = f[symbol]['AskAvgPrice']
            bidAvgPrice = f[symbol]['BidAvgPrice']
            totalAskVolume = f[symbol]['TotalAskVolume']
            totalBidVolume = f[symbol]['TotalBidVolume']
            open_p = f[symbol]['Open']
            high = f[symbol]['High']
            low = f[symbol]['Low']
            preClose = f[symbol]['PreClose']

            tick = pd.DataFrame(
                {'Time': time, 'Price': price, 'Volume': volume, 'Turnover': turnover, 'MatchItem': matchItem,
                 'BSFlag': bsflag, 'AccVolume': accVolume, 'AccTurnover': accTurnover, 'AskAvgPrice': askAvgPrice,
                 'BidAvgPrice': bidAvgPrice, 'TotalAskVolume': totalAskVolume, 'TotalBidVolume': totalBidVolume,
                 'Open': open_p, 'High': high, 'Low': low, 'PreClose': preClose})

            for i in range(10):
                tick['BidPrice' + str(i + 1)] = f[symbol]['BidPrice10'][:][:, i]
                tick['AskPrice' + str(i + 1)] = f[symbol]['AskPrice10'][:][:, i]
                tick['BidVolume' + str(i + 1)] = f[symbol]['BidVolume10'][:][:, i]
                tick['AskVolume' + str(i + 1)] = f[symbol]['AskVolume10'][:][:, i]

            tick['Time'] = tick['Time'].map(lambda x: cls.to_datetime(x, tradingday))
            tick.set_index('Time', inplace=True)
        return tick

if __name__ == '__main__':
    pass