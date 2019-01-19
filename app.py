#!/usr/bin/env python
# encoding: utf-8
"""
Usage:
    app.py Testor <type> <factor> <factorType> <start> <end> <universe>
    app.py TNCalculator <factor> <start> <end>
    app.py Neutralizer <factor> <start> <end> <neutralizer>

Options:
    -h --help show this screen
"""
from docopt import docopt

from DataService import TradableListService
from Neutralizer.SingleNeutralizer import SingleNeutralizer
from SingleFactorTestor.LayerTestor import LayerTestor

"""
@author: zhangmeng
@contact: arws@qq.com
@file: app.py
@time: 2018/12/17 10:33
"""

from Calculator.TNDataCalculator import TNDataCalculator

from SingleFactorTestor.RegressTestor import RegressTestor


class TNCalculator(object):

    def __init__(self, factor, start, end):
        self.runner = None
        self.initialize(factor,start, end)

    def initialize(self, factor, start, end):
        self.runner = TNDataCalculator(factor, start, end)

    def run(self):
        self.runner.run()


class Testor(object):
    def __init__(self, type, factor, factor_type, start, end, universe):
        self.runner = None
        self.initialize(type, factor, factor_type, start, end, universe)

    def initialize(self, type, factor, factor_type, start, end, universe):
        if type == 'RegressTestor':
            self.runner = RegressTestor(factor, factor_type, start, end, universe)

        if type == 'LayerTestor':
            self.runner = LayerTestor(factor, factor_type, start, end, universe)

    def run(self):
        self.runner.run()


class Neutralizer(object):
    def __init__(self, factor, start, end, neutralizer):
        self.runner = None
        self.initialize(factor, start, end, neutralizer)

    def initialize(self, factor, start, end, neutralizer):
        self.runner = SingleNeutralizer(factor, start, end, neutralizer)

    def run(self):
        self.runner.run()


if __name__ == '__main__':
    arg = docopt(__doc__)
    if arg['TNCalculator']:
        factor = arg['<factor>']
        start = arg['<start>']
        end = arg['<end>']
        TradableListService.TradableListSerivce.setTradingDay(start, end)
        app = TNCalculator(factor, start, end)
        app.run()

    if arg['Testor']:
        type = arg['<type>']
        start = arg['<start>']
        end = arg['<end>']
        factor = arg['<factor>']
        factor_type =arg['<factorType>']
        universe = arg['<universe>']

        TradableListService.TradableListSerivce.setTradingDay(start, end)
        TradableListService.TradableListSerivce.setIndexComponent(universe)

        app = Testor(type, factor, factor_type, start, end, universe)
        app.run()

    if arg['Neutralizer']:
        factor = arg['<factor>']
        start = arg['<start>']
        end = arg['<end>']
        neutralizer = arg['<neutralizer>']
        TradableListService.TradableListSerivce.setTradingDay(start, end)
        app = Neutralizer(factor, start, end, neutralizer)
        app.run()