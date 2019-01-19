#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: JydbSource.py
@time: 2018/12/17 14:46
"""
from configparser import ConfigParser
import pymssql
import pandas as pd
import sys

import Log
from numpy import NaN

import JydbConst

sys.path.append('..')


class JydbSource(object):

    def __init__(self):
        self.server = ""
        self.user = ""
        self.password = ""
        self.database = ""
        self.initialize()
        self.conn = None
        self.logger = Log.get_logger(__name__)

    def get_connection(self):
        for i in range(3):
            try:
                self.conn = pymssql.connect(self.server, self.user, self.password, self.database)
                return self.conn
            except pymssql.OperationalError as e:
                self.logger.error(e)

    def get_tradingday(self, start, end):
        """
        返回交易日序列
        :param start: '20150101'
        :param end: '20150130'
        :return: tradingDay: ['20150101', '20150101']
        """
        tradingDay = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                proc = 'spu_GetTradingDate'
                cursor.callproc(proc, (start, end))
                for row in cursor:
                    tradingDay.append(row['TradingDate'])

        return tradingDay

    def initialize(self):
        cfg = ConfigParser()
        cfg.read('config.ini')
        self.server = cfg.get('Jydb', 'server')
        self.user = cfg.get('Jydb', 'user')
        self.password = cfg.get('Jydb', 'password')
        self.database = cfg.get('Jydb', 'database')

    def get_tradableList(self, tradingDay):
        """
        返回交易日可交易证券列表
        :param tradingDay: '20150101’
        :return: ['600000.sh', '000001.sz']
        """

        stockId = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                proc = 'spu_GetTradableStock'
                cursor.callproc(proc, (tradingDay,))
                for row in cursor:
                    if row['sType'] != 'EQA' or row['isTradable'] == 0:
                        continue
                    stockId.append(row['stockId'])
        return stockId

    def get_tradableItem(self, tradingDay):
        """
        返回交易日可交易证券Item列表
        :param tradingDay: '20150101'
        :return: pd.DataFrame
        """
        stockId = []
        stockname = []
        sType = []
        isTradable = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                proc = 'spu_GetTradableSecurity'
                cursor.callproc(proc, (tradingDay,))
                for row in cursor:
                    if row['sType'] == 'BDC' or row['sType'] == 'FTR':
                        continue
                    stockId.append(row['stockId'])
                    stockname.append(row['stockName'].encode('latin-1').decode('gbk'))
                    sType.append(row['sType'])
                    isTradable.append(row['isTradable'])

        df = pd.DataFrame(
            {'Symbol': stockId, 'ChineseName': stockname, 'Type': sType, 'isTradable': isTradable})
        return df

    def get_universe(self, tradingDay):
        """
        返回在交易日之前的全部证券基础信息
        :param tradingDay: '20150101'
        :return: df : pd.DataFrame
                    Symbol ChineseName Type ListDate DelistDate
                    Type - 'EQA'
        """
        stockId = []
        stockname = []
        sType = []
        listDate = []
        delistDate = []
        isTradable = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                proc = 'spu_GetTradableStock'
                cursor.callproc(proc, (tradingDay,))
                for row in cursor:
                    if row['sType'] != 'EQA':
                        continue
                    stockId.append(row['stockId'])
                    # stockname.append(row['stockName'].encode('latin-1').decode('gbk'))
                    stockname.append(row['stockName'])
                    sType.append(row['sType'])
                    listDate.append(row['listDate'])
                    delistDate.append(row['delistDate'])
                    isTradable.append(row['isTradable'])
        df = pd.DataFrame(
            {'Symbol': stockId, 'ChineseName': stockname, 'Type': sType, 'ListDate': listDate,
             'DelistDate': delistDate, 'isTradable': isTradable})
        return df

    def getDayBarByDay(self, tradingday):
        """
        获得日线数据
        :param tradingDay: '20150101'
        :return:
        """

        symbol = []
        preClose = []
        open_p = []
        close = []
        high = []
        low = []
        volume = []
        turnover = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'SELECT CASE SecuMarket when 83 then SecuCode+\'.sh\' when 90 then SecuCode+\'.sz\' End as Symbol,TradingDay, PrevClosePrice, OpenPrice, HighPrice, LowPrice, ClosePrice, TurnoverVolume, TurnoverValue FROM QT_DailyQuote t1 LEFT JOIN SecuMain t2 ON t1.InnerCode = t2.InnerCode where SecuMarket in (83,90) and SecuCategory = 1 and ListedState = 1 and TradingDay = \'%s\'' % tradingday
                cursor.execute(stmt)
                # 查询结果字段
                # -------------------------------------------
                # 'TradingDay'       '20150101'
                # 'PrevClosePrice'    12.60
                # 'OpenPrice'    12.60
                # 'HighPrice'    12.60
                # 'LowPrice'    12.60
                # 'ClosePrice'    12.60
                # 'TurnoverVolume'    31323053
                # 'TurnoverValue'    398614966
                for row in cursor:
                    symbol.append(row['Symbol'])
                    preClose.append(row['PrevClosePrice'])
                    open_p.append(row['OpenPrice'])
                    close.append(row['ClosePrice'])
                    high.append(row['HighPrice'])
                    low.append(row['LowPrice'])
                    volume.append(row['TurnoverVolume'])
                    turnover.append(row['TurnoverValue'])
        df = pd.DataFrame(
            {'Symbol': symbol, 'Open': open_p, 'Close': close, 'High': high,
             'Low': low, 'Volume': volume, 'Turnover': turnover, 'PreClose': preClose})
        return df

    def getDayBar(self, symbol, start, end):
        """
        获得日线数据
        :param symbol: '600000.sh'
        :param start: '20150101'
        :param end: '20160101'
        :return: pd.DataFrame
        """
        innerCode = self.get_stockInnerCode(symbol)

        tradingDay = []
        preClose = []
        open_p = []
        close = []
        high = []
        low = []
        volume = []
        turnover = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'SELECT TradingDay, PrevClosePrice, OpenPrice, HighPrice, LowPrice, ClosePrice, TurnoverVolume, TurnoverValue FROM QT_DailyQuote WHERE InnerCode = %d AND TradingDay>=\'%s\' AND TradingDay <=\'%s\' ORDER BY TradingDay' % (
                    innerCode, start, end)
                cursor.execute(stmt)
                # 查询结果字段
                # -------------------------------------------
                # 'TradingDay'       '20150101'
                # 'PrevClosePrice'    12.60
                # 'OpenPrice'    12.60
                # 'HighPrice'    12.60
                # 'LowPrice'    12.60
                # 'ClosePrice'    12.60
                # 'TurnoverVolume'    31323053
                # 'TurnoverValue'    398614966
                for row in cursor:
                    tradingDay.append(row['TradingDay'].strftime('%Y%m%d'))
                    preClose.append(row['PrevClosePrice'])
                    open_p.append(row['OpenPrice'])
                    close.append(row['ClosePrice'])
                    high.append(row['HighPrice'])
                    low.append(row['LowPrice'])
                    volume.append(row['TurnoverVolume'])
                    turnover.append(row['TurnoverValue'])
        df = pd.DataFrame(
            {'Date': tradingDay, 'Open': open_p, 'Close': close, 'High': high,
             'Low': low, 'Volume': volume, 'Turnover': turnover, 'PreClose': preClose})
        return df

    def get_stockInnerCode(self, symbol):
        """
        根据交易所代码返回inner_code序列
        :param symbol: '000001.sz'
        :return: innerCode: 3
        """
        result = None
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'SELECT InnerCode, SecuCode FROM vwu_SecuMain WHERE SecuCode = \'%s\' AND SecuMarket in (83, 90) AND SecuCategory=1' % symbol[: 6]

                cursor.execute(stmt)
                # 查询结果字段
                # -------------------------------------------
                # 'secuCode'       000001
                # 'InnerCode'    'sz'/'sh'
                for row in cursor:
                    result = row['InnerCode']
        return result

    def getDividend(self, start, end):
        """
        获得除权除息数据
        :param start: '20150101'
        :param end: '20160101'
        :return: pd.DataFrame
        """
        tradingDay = pd.DataFrame({'ExdiviDate': self.get_tradingday('19900101', end)})
        symbol = []
        exdividate = []
        adjustingFactor = []
        adjustingConst = []
        ratioAdjustingFactor = []

        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'select Case t2.SecuMarket WHEN 83 then t2.SecuCode + \'.sh\' WHEN 90 then t2.SecuCode + \'.sz\' end as Symbol, t1.ExdiviDate, t1.AdjustingFactor, t1.AdjustingConst, t1.RatioAdjustingFactor from QT_AdjustingFactor t1 left join SecuMain t2 on t1.InnerCode = t2.InnerCode where t1.ExDiviDate >= \'%s\' and t1.ExDiviDate <= \'%s\' and t2.SecuCategory = 1' % (
                    '19900101', end)
                cursor.execute(stmt)
                # 查询结果字段
                # -------------------------------------------
                # 'Symbol'       '600000.sh'
                # 'ExdiviDate'    '20150101'
                # 'AdjustingFactor'    1.0
                # 'AdjustConst'    1.0
                # 'RatioAdjustingFactor'    1.0
                for row in cursor:
                    symbol.append(row['Symbol'])
                    exdividate.append(row['ExdiviDate'].strftime('%Y%m%d'))
                    adjustingFactor.append(float(row['AdjustingFactor']))
                    adjustingConst.append(float(row['AdjustingConst']))
                    ratioAdjustingFactor.append(
                        float(row['RatioAdjustingFactor']) if row['RatioAdjustingFactor'] is not None else None)

        df = pd.DataFrame(
            {'Symbol': symbol, 'ExdiviDate': exdividate, 'AdjustingFactor': adjustingFactor,
             'AdjustingConst': adjustingConst,
             'RatioAdjustingFactor': ratioAdjustingFactor})

        result = pd.DataFrame(columns=['Symbol', 'ExdiviDate', 'Stock', 'Cash', 'RatioAdjustingFactor'])
        symbols = pd.unique(df['Symbol'])
        for s in symbols:
            data = pd.merge(tradingDay, df[df['Symbol'] == s], how='left', on=['ExdiviDate'])
            data.fillna(method='ffill', inplace=True)
            data.fillna(method='bfill', inplace=True)
            data['Stock'] = data['AdjustingFactor'] / data['AdjustingFactor'].shift(1) - 1
            data['Cash'] = data['AdjustingConst'].diff() / data['AdjustingFactor'].shift(1)
            data.drop([0], inplace=True)
            data = data.loc[(data['ExdiviDate'] >= start) & (data['ExdiviDate'] <= end)]
            result = result.append(data, ignore_index=True)
        result = result[['Symbol', 'ExdiviDate', 'Stock', 'Cash', 'RatioAdjustingFactor']]
        # result['Stock'] = result['AdjustingFactor'] / result['AdjustingFactor'].shift(1) - 1
        # result['Cash'] = result['AdjustingConst'].diff() / result['AdjustingFactor'].shift(1)
        # del result['AdjustingFactor'], result['AdjustingConst']
        return result

    def getIndexUniverse(self, tradingDay):
        """
        返回交易日之前的指数基本信息
        :param tradingDay: '20150101'
        :return: pd.DataFrame()
                     'Symbol', 'Name', 'Market', 'Publisher', 'Category', 'ListDate'
        """
        symbol = []
        name = []
        publisher = []
        list_date = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'select t2.InnerCode, t2.SecuCode as Symbol, t2.SecuAbbr as Name , t1.PubOrgName as Publisher, t1.PubDate as ListDate from LC_IndexBasicInfo t1 LEFT JOIN SecuMain t2 on t1.IndexCode = t2.InnerCode where t2.InnerCode in (1, 46, 3145, 4978, 4982, 39144, 11089, 7542, 1055, 5375, 5376, 5377, 5378, 5379, 5382, 5385, 5386, 5387, 5388, 5389, 5390, 5391, 5392, 5394, 5395, 5397, 32616, 32617, 32618, 32619, 32620, 32621, 32622, 32623, 32624, 32625, 32626)'
                cursor.execute(stmt)
                # 查询结果字段
                # ----------------------------------
                # 'InnerCode' 15
                # 'Symbol' 000005
                # 'Name' 上证50指数
                # 'Market' 83
                # 'Publisher' 上海证券交易所
                # 'ListDate' 发布日期
                for row in cursor:
                    symbol.append(row['Symbol'])
                    name.append(row['Name'])
                    # name.append(row['Name'].encode('latin-1').decode('gbk'))
                    # publisher.append(row['Publisher'].encode('latin-1').decode('gbk'))
                    publisher.append(row['Publisher'])
                    list_date.append(row['ListDate'].strftime('%Y%m%d'))

        df = pd.DataFrame({'Symbol': symbol, 'Name': name, 'Publisher': publisher, 'ListDate': list_date})
        return df

    def getIndexDayBar(self, symbol, start, end):
        """
        返回指数日线数据
        :param symbol: '399001'
        :param start: '20180901'
        :param end: '20180914'
        :return:
        """
        date = []
        open_p = []
        close_p = []
        high = []
        low = []
        pre_close = []
        volume = []
        turnvoer = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'select InnerCode from SecuMain where SecuCategory = 4 and SecuMarket in (83, 90) and SecuCode = \'%s\'' % symbol
                cursor.execute(stmt)
                for row in cursor:
                    innerCode = row['InnerCode']
                stmt = 'select TradingDay as Date, OpenPrice as OpenP, HighPrice as High, LowPrice as Low, ClosePrice as CloseP, TurnoverVolume as Volume, TurnoverValue as Turnover, PrevClosePrice as PreClose from QT_IndexQuote where Innercode = %s and TradingDay >= \'%s\' and TradingDay <= \'%s\' order by TradingDay' % (
                    innerCode, start, end)
                cursor.execute(stmt)
                for row in cursor:
                    date.append(row['Date'].strftime('%Y%m%d'))
                    open_p.append(row['OpenP'])
                    close_p.append(row['CloseP'])
                    high.append(row['High'])
                    low.append(row['Low'])
                    pre_close.append(row['PreClose'])
                    volume.append(row['Volume'])
                    turnvoer.append(row['Turnover'])
        data = pd.DataFrame(
            {'Date': date, 'Open': open_p, 'Close': close_p, 'High': high, 'Low': low, 'PreClose': pre_close,
             'Volume': volume, 'Turnover': turnvoer})
        return data

    def getIndexWeight(self, index, tradingday):
        symbol = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'select InnerCode from SecuMain where SecuCategory = 4 and SecuMarket in (83, 90) and SecuCode = \'%s\'' % index
                cursor.execute(stmt)
                for row in cursor:
                    innerCode = row['InnerCode']
                stmt = 'select case t2.SecuMarket when 83 then t2.SecuCode+\'.sh\' when 90 then t2.SecuCode+\'.sz\' end as Symbol from LC_IndexComponent t1 LEFT JOIN SecuMain t2 on t1.SecuInnerCode = t2.InnerCode where IndexInnerCode = %s and InDate <= \'%s\' and (OutDate > \'%s\' Or OutDate is null)' % (
                    innerCode, tradingday, tradingday)
                cursor.execute(stmt)
                for row in cursor:
                    symbol.append(row['Symbol'])
        data = pd.DataFrame({'Symbol': symbol})
        data['Weight'] = None
        return data

    def getBalanceSheet(self, start, end, symbol):
        """
        获得日线数据
        :param symbol: '600000.sh'
        :param start: '20150101'
        :param end: '20160101'
        :return: pd.DataFrame
        """
        companyCode = self.get_stockCompanyCode(symbol)
        df = pd.DataFrame()

        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'Select t1.InfoPublDate, t1.EndDate, CashEquivalents,ClientDeposit,TradingAssets,BillReceivable,DividendReceivable,BillAccReceivable,InterestReceivable,AccountReceivable,ContractualAssets,OtherReceivable,AdvancePayment,Inventories,BearerBiologicalAssets,DeferredExpense,HoldAndFSAssets,NonCurrentAssetIn1Year,OtherCurrentAssets,CAExceptionalItems,CAAdjustmentItems,TotalCurrentAssets,DebtInvestment,OthDebtInvestment,HoldToMaturityInvestments,HoldForSaleAssets,OthEquityInstrument,OthNonCurFinAssets,InvestmentProperty,LongtermEquityInvest,LongtermReceivableAccount,FixedAssets,ConstructionMaterials,ConstruInProcess,FixedAssetsLiquidation,BiologicalAssets,OilGasAssets,IntangibleAssets,SeatCosts,DevelopmentExpenditure,GoodWill,LongDeferredExpense,DeferredTaxAssets,OtherNonCurrentAssets,NCAExceptionalItems,NCAAdjustmentItems,TotalNonCurrentAssets,LoanAndAccountReceivables,SettlementProvi,ClientProvi,DepositInInterbank,RMetal,LendCapital,DerivativeAssets,BoughtSellbackAssets,LoanAndAdvance,InsuranceReceivables,ReceivableSubrogationFee,ReinsuranceReceivables,ReceivableUnearnedR,ReceivableClaimsR,ReceivableLifeR,ReceivableLTHealthR,InsurerImpawnLoan,FixedDeposit,RefundableDeposit,RefundableCapitalDeposit,IndependenceAccountAssets,OtherAssets,AExceptionalItems,AAdjustmentItems,TotalAssets,ShortTermLoan,ImpawnedLoan,TradingLiability,NotesPayable,AccountsPayable,NotAccountsPayable,ContractLiability,STBondsPayable,AdvanceReceipts,SalariesPayable,DividendPayable,TaxsPayable,InterestPayable,OtherPayable,AccruedExpense,DeferredProceeds,HoldAndFSLi,NonCurrentLiabilityIn1Year,OtherCurrentLiability,CLExceptionalItems,CLAdjustmentItems,TotalCurrentLiability,LongtermLoan,BondsPayable,LPreferStock,LPerpetualDebt,LongtermAccountPayable,LongSalariesPay,SpecificAccountPayable,EstimateLiability,DeferredTaxLiability,LongDeferIncome,OtherNonCurrentLiability,NCLExceptionalItems,NCLAdjustmentItems,TotalNonCurrentLiability,BorrowingFromCentralBank,DepositOfInterbank,BorrowingCapital,DerivativeLiability,SoldBuybackSecuProceeds,Deposit,ProxySecuProceeds,SubIssueSecuProceeds,DepositsReceived,AdvanceInsurance,CommissionPayable,ReinsurancePayables,CompensationPayable,PolicyDividendPayable,InsurerDepositInvestment,UnearnedPremiumReserve,OutstandingClaimReserve,LifeInsuranceReserve,LTHealthInsuranceLR,IndependenceLiability,OtherLiability,LExceptionalItems,LAdjustmentItems,TotalLiability,PaidInCapital,OtherEquityinstruments,EPreferStock,EPerpetualDebt,CapitalReserveFund,SurplusReserveFund,RetainedProfit,TreasuryStock,OtherCompositeIncome,OrdinaryRiskReserveFund,TradeRiskRSRVFd,ForeignCurrencyReportConvDiff,UncertainedInvestmentLoss,OtherReserves,SpecificReserves,SEExceptionalItems,SEAdjustmentItems,SEWithoutMI,MinorityInterests,OtherItemsEffectingSE,TotalShareholderEquity,LEExceptionalItems,LEAdjustmentItems,TotalLiabilityAndEquity from LC_BalanceSheetAll t1 right join (select MIN(InfoPublDate) as InfoPublDate, EndDate from LC_BalanceSheetAll where CompanyCode = %s group by EndDate) t2 on t1.EndDate = t2.EndDate where t1.CompanyCode = %s and t1.IfMerged = 1 and t1.IfAdjusted = 2 and t1.EndDate >= \'%s\' and t2.EndDate <= \'%s\'' % (
                    companyCode, companyCode, start, end)
                cursor.execute(stmt)
                # 查询结果字段
                # -------------------------------------------
                # 'InfoPublDate'       '20150101'
                # 'EndDate'    '20150101'
                # 'TotalOperatingRevenue'    12.60
                for row in cursor:
                    df = df.append(row, ignore_index=True)
        if not df.empty:
            df = df[['InfoPublDate', 'EndDate', 'CashEquivalents', 'ClientDeposit', 'TradingAssets', 'BillReceivable',
                     'DividendReceivable', 'BillAccReceivable', 'InterestReceivable', 'AccountReceivable',
                     'ContractualAssets', 'OtherReceivable', 'AdvancePayment', 'Inventories', 'BearerBiologicalAssets',
                     'DeferredExpense', 'HoldAndFSAssets', 'NonCurrentAssetIn1Year', 'OtherCurrentAssets',
                     'CAExceptionalItems', 'CAAdjustmentItems', 'TotalCurrentAssets', 'DebtInvestment',
                     'OthDebtInvestment', 'HoldToMaturityInvestments', 'HoldForSaleAssets', 'OthEquityInstrument',
                     'OthNonCurFinAssets', 'InvestmentProperty', 'LongtermEquityInvest', 'LongtermReceivableAccount',
                     'FixedAssets', 'ConstructionMaterials', 'ConstruInProcess', 'FixedAssetsLiquidation',
                     'BiologicalAssets', 'OilGasAssets', 'IntangibleAssets', 'SeatCosts', 'DevelopmentExpenditure',
                     'GoodWill', 'LongDeferredExpense', 'DeferredTaxAssets', 'OtherNonCurrentAssets',
                     'NCAExceptionalItems', 'NCAAdjustmentItems', 'TotalNonCurrentAssets', 'LoanAndAccountReceivables',
                     'SettlementProvi', 'ClientProvi', 'DepositInInterbank', 'RMetal', 'LendCapital',
                     'DerivativeAssets', 'BoughtSellbackAssets', 'LoanAndAdvance', 'InsuranceReceivables',
                     'ReceivableSubrogationFee', 'ReinsuranceReceivables', 'ReceivableUnearnedR', 'ReceivableClaimsR',
                     'ReceivableLifeR', 'ReceivableLTHealthR', 'InsurerImpawnLoan', 'FixedDeposit', 'RefundableDeposit',
                     'RefundableCapitalDeposit', 'IndependenceAccountAssets', 'OtherAssets', 'AExceptionalItems',
                     'AAdjustmentItems', 'TotalAssets', 'ShortTermLoan', 'ImpawnedLoan', 'TradingLiability',
                     'NotesPayable', 'AccountsPayable', 'NotAccountsPayable', 'ContractLiability', 'STBondsPayable',
                     'AdvanceReceipts', 'SalariesPayable', 'DividendPayable', 'TaxsPayable', 'InterestPayable',
                     'OtherPayable', 'AccruedExpense', 'DeferredProceeds', 'HoldAndFSLi', 'NonCurrentLiabilityIn1Year',
                     'OtherCurrentLiability', 'CLExceptionalItems', 'CLAdjustmentItems', 'TotalCurrentLiability',
                     'LongtermLoan', 'BondsPayable', 'LPreferStock', 'LPerpetualDebt', 'LongtermAccountPayable',
                     'LongSalariesPay', 'SpecificAccountPayable', 'EstimateLiability', 'DeferredTaxLiability',
                     'LongDeferIncome', 'OtherNonCurrentLiability', 'NCLExceptionalItems', 'NCLAdjustmentItems',
                     'TotalNonCurrentLiability', 'BorrowingFromCentralBank', 'DepositOfInterbank', 'BorrowingCapital',
                     'DerivativeLiability', 'SoldBuybackSecuProceeds', 'Deposit', 'ProxySecuProceeds',
                     'SubIssueSecuProceeds', 'DepositsReceived', 'AdvanceInsurance', 'CommissionPayable',
                     'ReinsurancePayables', 'CompensationPayable', 'PolicyDividendPayable', 'InsurerDepositInvestment',
                     'UnearnedPremiumReserve', 'OutstandingClaimReserve', 'LifeInsuranceReserve', 'LTHealthInsuranceLR',
                     'IndependenceLiability', 'OtherLiability', 'LExceptionalItems', 'LAdjustmentItems',
                     'TotalLiability', 'PaidInCapital', 'OtherEquityinstruments', 'EPreferStock', 'EPerpetualDebt',
                     'CapitalReserveFund', 'SurplusReserveFund', 'RetainedProfit', 'TreasuryStock',
                     'OtherCompositeIncome', 'OrdinaryRiskReserveFund', 'TradeRiskRSRVFd',
                     'ForeignCurrencyReportConvDiff', 'UncertainedInvestmentLoss', 'OtherReserves', 'SpecificReserves',
                     'SEExceptionalItems', 'SEAdjustmentItems', 'SEWithoutMI', 'MinorityInterests',
                     'OtherItemsEffectingSE', 'TotalShareholderEquity', 'LEExceptionalItems', 'LEAdjustmentItems',
                     'TotalLiabilityAndEquity']]
            df['InfoPublDate'] = df['InfoPublDate'].map(lambda x: x.strftime('%Y%m%d'))
            df.index = df['EndDate']
            df['EndDate'] = df['EndDate'].map(lambda x: x.strftime('%Y%m%d'))
            df.rename(columns=JydbConst.BALANCE_SHEET_MAP, inplace=True)
            df.index.set_names('', inplace=True)
            df = df.astype('float64')
        return df

    def getIncomeSheet(self, start, end, symbol):
        """
        获得日线数据
        :param symbol: '600000.sh'
        :param start: '20150101'
        :param end: '20160101'
        :return: pd.DataFrame
        """
        companyCode = self.get_stockCompanyCode(symbol)
        df = pd.DataFrame()

        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'select t1.InfoPublDate, t1.EndDate, TotalOperatingRevenue,OperatingRevenue,NetInterestIncome,InterestIncome,InterestExpense,NetCommissionIncome,CommissionIncome,CommissionExpense,NetProxySecuIncome,NetSubIssueSecuIncome,NetTrustIncome,PremiumsEarned,PremiumsIncome,ReinsuranceIncome,Reinsurance,UnearnedPremiumReserve,OtherOperatingRevenue,SpecialItemsOR,AdjustmentItemsOR,TotalOperatingCost,OperatingPayout,RefundedPremiums,CompensationExpense,AmortizationExpense,PremiumReserve,AmortizationPremiumReserve,PolicyDividendPayout,ReinsuranceCost,OperatingAndAdminExpense,AmortizationReinsuranceCost,InsuranceCommissionExpense,OtherOperatingCost,OperatingCost,OperatingTaxSurcharges,OperatingExpense,AdministrationExpense,FinancialExpense,InterestFinExp,InterestIncomeFin,RAndD,CreditImpairmentL,AssetImpairmentLoss,SpecialItemsTOC,AdjustmentItemsTOC,OtherNetRevenue,FairValueChangeIncome,InvestIncome,InvestIncomeAssociates,NetOpenHedgeIncome,ExchangeIncome,AssetDealIncome,OtherRevenue,OtherItemsEffectingOP,AdjustedItemsEffectingOP,OperatingProfit,NonoperatingIncome,NonoperatingExpense,NonCurrentAssetssDealLoss,OtherItemsEffectingTP,AdjustedItemsEffectingTP,TotalProfit,IncomeTaxCost,UncertainedInvestmentLosses,OtherItemsEffectingNP,AdjustedItemsEffectingNP,NetProfit,OperSustNetP,DisconOperNetP,NPParentCompanyOwners,MinorityProfit,OtherItemsEffectingNPP,AdjustedItemsEffectingNPP,OtherCompositeIncome,OCIParentCompanyOwners,OCIMinorityOwners,OCINotInIncomeStatement,OCIInIncomeStatement,AdjustedItemsEffectingCI,TotalCompositeIncome,CIParentCompanyOwners,CIMinorityOwners,AdjustedItemsEffectingPCI,BasicEPS,DilutedEPS from LC_IncomeStatementAll t1 right join (select MIN(InfoPublDate) as InfoPublDate, EndDate from LC_IncomeStatementAll where CompanyCode = %s group by EndDate) t2 on t1.EndDate = t2.EndDate where t1.CompanyCode = %s and t1.IfMerged = 1 and t1.IfAdjusted = 2 and t1.EndDate >= \'%s\' and t2.EndDate <= \'%s\'' % (
                    companyCode, companyCode, start, end)
                cursor.execute(stmt)
                # 查询结果字段
                # -------------------------------------------
                # 'InfoPublDate'       '20150101'
                # 'EndDate'    '20150101'
                # 'TotalOperatingRevenue'    12.60
                for row in cursor:
                    df = df.append(row, ignore_index=True)
        if not df.empty:
            df = df[['InfoPublDate', 'EndDate', 'TotalOperatingRevenue', 'OperatingRevenue', 'NetInterestIncome',
                     'InterestIncome', 'InterestExpense', 'NetCommissionIncome', 'CommissionIncome',
                     'CommissionExpense', 'NetProxySecuIncome', 'NetSubIssueSecuIncome', 'NetTrustIncome',
                     'PremiumsEarned', 'PremiumsIncome', 'ReinsuranceIncome', 'Reinsurance', 'UnearnedPremiumReserve',
                     'OtherOperatingRevenue', 'SpecialItemsOR', 'AdjustmentItemsOR', 'TotalOperatingCost',
                     'OperatingPayout', 'RefundedPremiums', 'CompensationExpense', 'AmortizationExpense',
                     'PremiumReserve', 'AmortizationPremiumReserve', 'PolicyDividendPayout', 'ReinsuranceCost',
                     'OperatingAndAdminExpense', 'AmortizationReinsuranceCost', 'InsuranceCommissionExpense',
                     'OtherOperatingCost', 'OperatingCost', 'OperatingTaxSurcharges', 'OperatingExpense',
                     'AdministrationExpense', 'FinancialExpense', 'InterestFinExp', 'InterestIncomeFin', 'RAndD',
                     'CreditImpairmentL', 'AssetImpairmentLoss', 'SpecialItemsTOC', 'AdjustmentItemsTOC',
                     'OtherNetRevenue', 'FairValueChangeIncome', 'InvestIncome', 'InvestIncomeAssociates',
                     'NetOpenHedgeIncome', 'ExchangeIncome', 'AssetDealIncome', 'OtherRevenue', 'OtherItemsEffectingOP',
                     'AdjustedItemsEffectingOP', 'OperatingProfit', 'NonoperatingIncome', 'NonoperatingExpense',
                     'NonCurrentAssetssDealLoss', 'OtherItemsEffectingTP', 'AdjustedItemsEffectingTP', 'TotalProfit',
                     'IncomeTaxCost', 'UncertainedInvestmentLosses', 'OtherItemsEffectingNP',
                     'AdjustedItemsEffectingNP', 'NetProfit', 'OperSustNetP', 'DisconOperNetP', 'NPParentCompanyOwners',
                     'MinorityProfit', 'OtherItemsEffectingNPP', 'AdjustedItemsEffectingNPP', 'OtherCompositeIncome',
                     'OCIParentCompanyOwners', 'OCIMinorityOwners', 'OCINotInIncomeStatement', 'OCIInIncomeStatement',
                     'AdjustedItemsEffectingCI', 'TotalCompositeIncome', 'CIParentCompanyOwners', 'CIMinorityOwners',
                     'AdjustedItemsEffectingPCI', 'BasicEPS', 'DilutedEPS']]
            df.index = pd.to_datetime(df['EndDate'])
            df['InfoPublDate'] = df['InfoPublDate'].map(lambda x: x.strftime('%Y%m%d'))
            df['EndDate'] = df['EndDate'].map(lambda x: x.strftime('%Y%m%d'))
            df.rename(columns=JydbConst.INCOME_NAME_MAP, inplace=True)
            df.index.set_names('', inplace=True)
            df = df.astype('float64')

        return df

    def getCashFlow(self, start, end, symbol):
        """
        获得日线数据
        :param symbol: '600000.sh'
        :param start: '20150101'
        :param end: '20160101'
        :return: pd.DataFrame
        """
        companyCode = self.get_stockCompanyCode(symbol)
        df = pd.DataFrame()

        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'Select t1.InfoPublDate as InfoPublDate, t2.EndDate as EndDate, GoodsSaleServiceRenderCash,TaxLevyRefund,NetDepositIncrease,NetBorrowingFromCentralBank,NetBorrowingFromFinanceCo,InterestAndCommissionCashIn,NetDealTradingAssets,NetBuyBack,NetOriginalInsuranceCash,NetReinsuranceCash,NetInsurerDepositInvestment,OtherCashInRelatedOperate,SpecialItemsOCIF,AdjustmentItemsOCIF,SubtotalOperateCashInflow,GoodsServicesCashPaid,StaffBehalfPaid,AllTaxesPaid,NetLoanAndAdvanceIncrease,NetDepositInCBAndIB,NetLendCapital,CommissionCashPaid,OriginalCompensationPaid,NetCashForReinsurance,PolicyDividendCashPaid,OtherOperateCashPaid,SpecialItemsOCOF,AdjustmentItemsOCOF,SubtotalOperateCashOutflow,AdjustmentItemsNOCF,NetOperateCashFlow,InvestWithdrawalCash,Investproceeds,FixIntanOtherAssetDispoCash,NetCashDealSubCompany,OtherCashFromInvestAct,SpecialItemsICIF,AdjustmentItemsICIF,SubtotalInvestCashInflow,FixIntanOtherAssetAcquiCash,InvestCashPaid,NetCashFromSubCompany,ImpawnedLoanNetIncrease,OtherCashToInvestAct,SpecialItemsICOF,AdjustmentItemsICOF,SubtotalInvestCashOutflow,AdjustmentItemsNICF,NetInvestCashFlow,CashFromInvest,CashFromMinoSInvestSub,CashFromBondsIssue,CashFromBorrowing,OtherFinanceActCash,SpecialItemsFCIF,AdjustmentItemsFCIF,SubtotalFinanceCashInflow,BorrowingRepayment,DividendInterestPayment,ProceedsFromSubToMinoS,OtherFinanceActPayment,SpecialItemsFCOF,AdjustmentItemsFCOF,SubtotalFinanceCashOutflow,AdjustmentItemsNFCF,NetFinanceCashFlow,ExchanRateChangeEffect,OtherItemsEffectingCE,AdjustmentItemsCE,CashEquivalentIncrease,BeginPeriodCash,OtherItemsEffectingCEI,AdjustmentItemsCEI,EndPeriodCashEquivalent from LC_CashFlowStatementAll t1 right join (select MIN(InfoPublDate) as InfoPublDate, EndDate from LC_CashFlowStatementAll where CompanyCode = %s group by EndDate) t2 on t1.EndDate = t2.EndDate where t1.CompanyCode = %s and t1.IfMerged = 1 and t1.IfAdjusted = 2 and t1.EndDate >= \'%s\' and t2.EndDate <= \'%s\'' % (
                    companyCode, companyCode, start, end)
                cursor.execute(stmt)
                # 查询结果字段
                # -------------------------------------------
                # 'InfoPublDate'       '20150101'
                # 'EndDate'    '20150101'
                # 'TotalOperatingRevenue'    12.60
                for row in cursor:
                    df = df.append(row, ignore_index=True)
        if not df.empty:
            df = df[['InfoPublDate', 'EndDate', 'GoodsSaleServiceRenderCash', 'TaxLevyRefund', 'NetDepositIncrease',
                     'NetBorrowingFromCentralBank', 'NetBorrowingFromFinanceCo', 'InterestAndCommissionCashIn',
                     'NetDealTradingAssets', 'NetBuyBack', 'NetOriginalInsuranceCash', 'NetReinsuranceCash',
                     'NetInsurerDepositInvestment', 'OtherCashInRelatedOperate', 'SpecialItemsOCIF',
                     'AdjustmentItemsOCIF', 'SubtotalOperateCashInflow', 'GoodsServicesCashPaid', 'StaffBehalfPaid',
                     'AllTaxesPaid', 'NetLoanAndAdvanceIncrease', 'NetDepositInCBAndIB', 'NetLendCapital',
                     'CommissionCashPaid', 'OriginalCompensationPaid', 'NetCashForReinsurance',
                     'PolicyDividendCashPaid', 'OtherOperateCashPaid', 'SpecialItemsOCOF', 'AdjustmentItemsOCOF',
                     'SubtotalOperateCashOutflow', 'AdjustmentItemsNOCF', 'NetOperateCashFlow', 'InvestWithdrawalCash',
                     'Investproceeds', 'FixIntanOtherAssetDispoCash', 'NetCashDealSubCompany', 'OtherCashFromInvestAct',
                     'SpecialItemsICIF', 'AdjustmentItemsICIF', 'SubtotalInvestCashInflow',
                     'FixIntanOtherAssetAcquiCash', 'InvestCashPaid', 'NetCashFromSubCompany',
                     'ImpawnedLoanNetIncrease', 'OtherCashToInvestAct', 'SpecialItemsICOF', 'AdjustmentItemsICOF',
                     'SubtotalInvestCashOutflow', 'AdjustmentItemsNICF', 'NetInvestCashFlow', 'CashFromInvest',
                     'CashFromMinoSInvestSub', 'CashFromBondsIssue', 'CashFromBorrowing', 'OtherFinanceActCash',
                     'SpecialItemsFCIF', 'AdjustmentItemsFCIF', 'SubtotalFinanceCashInflow', 'BorrowingRepayment',
                     'DividendInterestPayment', 'ProceedsFromSubToMinoS', 'OtherFinanceActPayment', 'SpecialItemsFCOF',
                     'AdjustmentItemsFCOF', 'SubtotalFinanceCashOutflow', 'AdjustmentItemsNFCF', 'NetFinanceCashFlow',
                     'ExchanRateChangeEffect', 'OtherItemsEffectingCE', 'AdjustmentItemsCE', 'CashEquivalentIncrease',
                     'BeginPeriodCash', 'OtherItemsEffectingCEI', 'AdjustmentItemsCEI', 'EndPeriodCashEquivalent']]
            df.index = pd.to_datetime(df['EndDate'])
            df['InfoPublDate'] = df['InfoPublDate'].map(lambda x: x.strftime('%Y%m%d'))
            df['EndDate'] = df['EndDate'].map(lambda x: x.strftime('%Y%m%d'))
            df.rename(columns=JydbConst.CASH_FLOW_MAP, inplace=True)
            df.index.set_names('', inplace=True)
            df = df.astype('float64')

        return df

    def get_stockCompanyCode(self, symbol):
        """
        根据交易所代码返回company_code序列
        :param symbol: '000001.sz'
        :return: innerCode: 3
        """
        result = None
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'SELECT InnerCode, CompanyCode FROM vwu_SecuMain WHERE SecuCode = \'%s\' AND SecuMarket in (83, 90) AND SecuCategory=1' % symbol[
                                                                                                                                                 : 6]

                cursor.execute(stmt)
                # 查询结果字段
                # -------------------------------------------
                # 'secuCode'       000001
                # 'InnerCode'    'sz'/'sh'
                for row in cursor:
                    result = row['CompanyCode']
        return result

    def getCapitalStock(self, start, end, symbol):
        companyCode = self.get_stockCompanyCode(symbol)
        tradingDay = pd.DataFrame({'EndDate': self.get_tradingday(start, end)})

        totalshares = []
        ashares = []
        afloats = []
        restrictedAShares = []
        endDate = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'select EndDate, TotalShares, Ashares, AFloats, RestrictedAShares  from LC_ShareStru where CompanyCode = %s and EndDate >= \'%s\' and EndDate <= \'%s\' ' % (
                    companyCode, start, end)
                cursor.execute(stmt)
                # 查询结果字段
                # -------------------------------------------
                # 'Symbol'       '600000.sh'
                # 'ExdiviDate'    '20150101'
                # 'AdjustingFactor'    1.0
                # 'AdjustConst'    1.0
                # 'RatioAdjustingFactor'    1.0
                for row in cursor:
                    endDate.append(row['EndDate'].strftime('%Y%m%d'))
                    totalshares.append(float(row['TotalShares']) if row['TotalShares'] else NaN)
                    ashares.append(float(row['Ashares']) if row['Ashares'] else NaN)
                    restrictedAShares.append(float(row['RestrictedAShares']) if row['RestrictedAShares'] else NaN)
                    afloats.append(float(row['AFloats']) if row['AFloats'] else NaN)

        df = pd.DataFrame(
            {'EndDate': endDate, '总股本': totalshares, '流通A股': afloats, '限制性流通A股': restrictedAShares,
             'A股股本': ashares})

        data = pd.merge(tradingDay, df, how='left', on=['EndDate'])
        data.index = pd.to_datetime(data['EndDate'])
        data.index.name = ''
        # data = data.reindex(pd.to_datetime(data['EndDate']))
        data.fillna(method='ffill', inplace=True)
        data.fillna(method='bfill', inplace=True)
        data = data[['总股本', 'A股股本', '流通A股', '限制性流通A股']]
        data['自由流通股'] = NaN
        return data

    def getFinancialDerivative(self, start, end, symbol):
        """
        获得日线数据
        :param symbol: '600000.sh'
        :param start: '20150101'
        :param end: '20160101'
        :return: pd.DataFrame
        """
        companyCode = self.get_stockCompanyCode(symbol)

        df = pd.DataFrame()
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'IF OBJECT_ID(\'tempdb..#tmpData\') IS NOT NULL\n  DROP TABLE #tmpData'
                cursor.execute(stmt)
                stmt = 'IF OBJECT_ID(\'tempdb..#tmpDate\') IS NOT NULL\n DROP TABLE #tmpDate'
                cursor.execute(stmt)

                stmt = 'select EndDate, ROE, ROA, ROIC, GrossIncomeRatio as \'销售毛利率\' ,OperatingProfitRatio as \'营业利润率\',OperatingExpenseRate as \'销售费用率\', NPToTOR as \'净利润率\' ,EBITToTOR as \'EBIT利润率\', EBITDA/ (EBIT / nullif(EBITToTOR, 0)) AS \'EBITDA利润率\',  NetProfitCut / (EBIT / nullif(EBITToTOR, 0)) as \'扣非净利率\' into #tmpData from LC_MainIndexNew where CompanyCode = %s and EndDate >= \'%s\' and EndDate <= \'%s\' ' % (companyCode, start, end)
                cursor.execute(stmt)

                stmt = 'select InfoPublDate, EndDate into #tmpDate from LC_MainDataNew where CompanyCode = %s and Mark = 2' % companyCode
                cursor.execute(stmt)

                stmt = 'select t1.*, t2.InfoPublDate as \'发布日\' from #tmpData t1 left JOIN #tmpDate t2 on t1.EndDate=t2.EndDate'
                cursor.execute(stmt)
                for row in cursor:
                    df = df.append(row, ignore_index=True)
        if not df.empty:
            df.index = df['EndDate']
            df['报告日'] = df['EndDate'].map(lambda x: x.strftime('%Y%m%d') if not pd.isnull(x) else None)
            df['发布日'] = df['发布日'].map(lambda x: x.strftime('%Y%m%d') if not pd.isnull(x) else None)
            del df['EndDate']
            df.index.rename('', inplace=True)
        df = df.astype('float64')
        return df

    def getDailyDerivative(self, start, end, symbol):
        """
        获得日线数据
        :param symbol: '600000.sh'
        :param start: '20150101'
        :param end: '20160101'
        :return: pd.DataFrame
        """
        # companyCode = self.get_stockCompanyCode(symbol)
        innerCode = self.get_stockInnerCode(symbol)
        tradingDay = pd.to_datetime(self.get_tradingday(start, end))

        # df = pd.DataFrame()
        endDate = []
        turnover = []
        mv = []
        nego_mv = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'select TradingDay as EndDate, TurnoverRate, TotalMV, NegotiableMV from QT_Performance where InnerCode = %s and TradingDay >= \'%s\' and TradingDay <= \'%s\'' % (innerCode, start, end)
                cursor.execute(stmt)
                # 查询结果字段
                # -------------------------------------------
                # 'InfoPublDate'       '20150101'
                # 'EndDate'    '20150101'
                # 'TotalOperatingRevenue'    12.60
                for row in cursor:
                    endDate.append(row['EndDate'])
                    turnover.append(row['TurnoverRate'])
                    mv.append(row['TotalMV'])
                    nego_mv.append(row['NegotiableMV'])
                    # df = df.append(row, ignore_index=True)
        df = pd.DataFrame(index=endDate, data={'换手率': turnover, '总市值': mv, '流通市值': nego_mv})
        if not df.empty:
            df = df.reindex(tradingDay)
            df.index.set_names('', inplace=True)

        tradingDay = []
        pe = []
        pettm = []
        pb = []
        ps = []
        psttm = []
        dividend = []
        ew = []
        ewn = []
        with self.get_connection() as conn:
            with conn.cursor(as_dict=True) as cursor:
                stmt = 'select TradingDay, PELYR as PE, PE as PETTM, PB, PS, PSTTM, DividendRatioLYR,  EnterpriseValueW, EnterpriseValueN from LC_DIndicesForValuation where InnerCode = %s and TradingDay >= \'%s\' and TradingDay <= \'%s\'' % (innerCode, start, end)
                cursor.execute(stmt)

                for row in cursor:
                    tradingDay.append(row['TradingDay'])
                    pe.append(row['PE'])
                    pettm.append(row['PETTM'])
                    pb.append(row['PB'])
                    ps.append(row['PS'])
                    psttm.append(row['PSTTM'])
                    dividend.append(row['DividendRatioLYR'])
                    ew.append(row['EnterpriseValueW'])
                    ewn.append(row['EnterpriseValueN'])
        df1 = pd.DataFrame(index=tradingDay, data={'PE': pe, 'PETTM': pettm, 'PB': pb, 'PS': ps, 'PSTTM': psttm, '股息率': dividend, '企业价值': ew, '企业价值不含货币': ewn})
        if not df1.empty:
            df1 = df1.reindex(tradingDay)
            df1.index.set_names('', inplace=True)
        df = pd.concat([df, df1], axis=1)
        df.fillna(method='ffill', inplace=True)
        df = df.astype('float64')
        return df


jydbSource = JydbSource()

if __name__ == '__main__':
    pass