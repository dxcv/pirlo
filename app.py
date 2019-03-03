#!/usr/bin/env python
# encoding: utf-8
"""
Usage:
    app.py Testor <type> <factor> <factorType> <start> <end> <universe>
    app.py TNCalculator <factor> <start> <end> <logged>
    app.py Neutralizer <factor> <start> <end> <neutralizer> <Industrized>
    app.py Estimator <type> <factor> <factorType> <start> <end> <universe>

Options:
    -h --help show this screen
"""
from docopt import docopt

from DataService import TradableListService
from Estimator.TurnoverRateEstimator import TurnoverRateEstimator
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

class Estimator(object):

    def __init__(self, estimator_type, factor_name, factor_type, start, end, universe):
        self.runner = None
        self.initilize(estimator_type, factor_name, factor_type, start, end, universe)

    def initilize(self, estimator_type, factor_name, factor_type, start, end, universe):
        if estimator_type == 'TurnoverRate':
            self.runner = TurnoverRateEstimator(factor_name, factor_type, start, end, universe)

    def run(self):
        self.runner.run()


class TNCalculator(object):

    def __init__(self, factor, start, end, logged):
        self.runner = None
        self.initialize(factor,start, end, logged)

    def initialize(self, factor, start, end, logged):
        self.runner = TNDataCalculator(factor, start, end, logged)

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
    def __init__(self, factor, start, end, neutralizer, Industrized):
        self.runner = None
        self.initialize(factor, start, end, neutralizer, Industrized)

    def initialize(self, factor, start, end, neutralizer, Industrized):
        self.runner = SingleNeutralizer(factor, start, end, neutralizer, Industrized)

    def run(self):
        self.runner.run()


if __name__ == '__main__':
    arg = docopt(__doc__)
    if arg['Estimator']:
        estimator_type = arg['<type>']
        factor_name = arg['<factor>']
        factor_type = arg['<factorType>']
        start = arg['<start>']
        end = arg['<end>']
        universe = arg['<universe>']
        TradableListService.TradableListSerivce.setTradingDay(start, end)
        app = Estimator(estimator_type, factor_name, factor_type, start, end, universe)
        app.run()

    if arg['TNCalculator']:
        factor = arg['<factor>']
        start = arg['<start>']
        end = arg['<end>']
        logged = True if arg['<logged>'] == 'Log' else False
        TradableListService.TradableListSerivce.setTradingDay(start, end)
        app = TNCalculator(factor, start, end, logged)
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
        Industrized = True if arg['<Industrized>'] == 'True' else False
        TradableListService.TradableListSerivce.setTradingDay(start, end)
        app = Neutralizer(factor, start, end, neutralizer, Industrized)
        app.run()