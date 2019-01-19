#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: JydbConst.py
@time: 2018/12/17 17:05
"""
INCOME_NAME_MAP = {
    'InfoPublDate': '发布日',
    'EndDate': '报告日',
    'TotalOperatingRevenue': '营业总收入',
    'OperatingRevenue': '营业收入',
    'NetInterestIncome': '利息净收入',
    'InterestIncome': '利息收入',
    'InterestExpense': '利息支出',
    'NetCommissionIncome': '手续费及佣金净收入',
    'CommissionIncome': '手续费及佣金收入',
    'CommissionExpense': '手续费及佣金支出',
    'NetProxySecuIncome': '代理买卖证券业务净收入',
    'NetSubIssueSecuIncome': '证券承销业务净收入',
    'NetTrustIncome': '受托客户资产管理业务净收入',
    'PremiumsEarned': '已赚保费',
    'PremiumsIncome': '保险业务收入',
    'ReinsuranceIncome': '分保费收入',
    'Reinsurance': '分出保费',
    'UnearnedPremiumReserve': '提取未到期责任准备金',
    'OtherOperatingRevenue': '其他营业收入',
    'SpecialItemsOR': '营业总收入特殊项目',
    'AdjustmentItemsOR': '营业收入调整项目',
    'TotalOperatingCost': '营业总成本',
    'OperatingPayout': '营业支出',
    'RefundedPremiums': '退保金',
    'CompensationExpense': '赔付支出',
    'AmortizationExpense': '摊回赔付支出',
    'PremiumReserve': '提取保险责任准备金',
    'AmortizationPremiumReserve': '摊回保险责任准备金',
    'PolicyDividendPayout': '保单红利支出',
    'ReinsuranceCost': '分保费用',
    'OperatingAndAdminExpense': '业务及管理费',
    'AmortizationReinsuranceCost': '摊回分保费用',
    'InsuranceCommissionExpense': '保险手续费及佣金支出',
    'OtherOperatingCost': '其他营业成本',
    'OperatingCost': '营业成本',
    'OperatingTaxSurcharges': '营业税金及附加',
    'OperatingExpense': '销售费用',
    'AdministrationExpense': '管理费用',
    'FinancialExpense': '财务费用',
    'InterestFinExp': '利息费用(财务费用)',
    'InterestIncomeFin': '利息收入(财务费用)',
    'RAndD': '研发费用',
    'CreditImpairmentL': '信用减值损失',
    'AssetImpairmentLoss': '资产减值损失',
    'SpecialItemsTOC': '营业总成本特殊项目',
    'AdjustmentItemsTOC': '营业总成本调整项目',
    'OtherNetRevenue': '非经营性净收益',
    'FairValueChangeIncome': '公允价值变动净收益',
    'InvestIncome': '投资净收益',
    'InvestIncomeAssociates': '对联营合营企业的投资收益',
    'NetOpenHedgeIncome': '净敞口套期收益',
    'ExchangeIncome': '汇兑收益',
    'AssetDealIncome': '资产处置收益',
    'OtherRevenue': '其他收益',
    'OtherItemsEffectingOP': '非经营性净收益特殊项目',
    'AdjustedItemsEffectingOP': '非经营性净收益调整项目',
    'OperatingProfit': '营业利润',
    'NonoperatingIncome': '营业外收入',
    'NonoperatingExpense': '营业外支出',
    'NonCurrentAssetssDealLoss': '非流动资产处置净损失',
    'OtherItemsEffectingTP': '影响利润总额的其他科目',
    'AdjustedItemsEffectingTP': '影响利润总额的调整项目',
    'TotalProfit': '利润总额',
    'IncomeTaxCost': '所得税费用',
    'UncertainedInvestmentLosses': '未确认的投资损失',
    'OtherItemsEffectingNP': '影响净利润的其他科目',
    'AdjustedItemsEffectingNP': '影响净利润的调整项目',
    'NetProfit': '净利润',
    'OperSustNetP': '持续经营净利润',
    'DisconOperNetP': '终止经营净利润',
    'NPParentCompanyOwners': '归属于母公司所有者的净利润',
    'MinorityProfit': '少数股东损益',
    'OtherItemsEffectingNPP': '影响母公司净利润的特殊项目',
    'AdjustedItemsEffectingNPP': '影响母公司净利润的调整项目',
    'OtherCompositeIncome': '其他综合收益',
    'OCIParentCompanyOwners': '归属于母公司所有者的其他综合收益总额',
    'OCIMinorityOwners': '归属于少数股东的其他综合收益总额',
    'OCINotInIncomeStatement': '以后不能重分类进损益表的其他综合收益',
    'OCIInIncomeStatement': '以后能重分类进损益表的其他综合收益',
    'AdjustedItemsEffectingCI': '影响综合收益总额的调整项目',
    'TotalCompositeIncome': '综合收益总额',
    'CIParentCompanyOwners': '归属于母公司所有者的综合收益总额',
    'CIMinorityOwners': '归属于少数股东的综合收益总额',
    'AdjustedItemsEffectingPCI': '影响母公司综合收益总额的调整项目',
    'BasicEPS': '基本每股收益',
    'DilutedEPS': '稀释每股收益',

}

BALANCE_SHEET_MAP = {
    'InfoPublDate': '发布日',
    'EndDate': '报告日',
    'CashEquivalents': '货币资金',
    'ClientDeposit': '客户资金存款',
    'TradingAssets': '交易性金融资产',
    'BillReceivable': '应收票据',
    'DividendReceivable': '应收股利',
    'BillAccReceivable': '应收票据及应收账款',
    'InterestReceivable': '应收利息',
    'AccountReceivable': '应收账款',
    'ContractualAssets': '合同资产',
    'OtherReceivable': '其他应收款',
    'AdvancePayment': '预付款项',
    'Inventories': '存货',
    'BearerBiologicalAssets': '消耗性生物资产',
    'DeferredExpense': '待摊费用',
    'HoldAndFSAssets': '划分为持有待售的资产',
    'NonCurrentAssetIn1Year': '一年内到期的非流动资产',
    'OtherCurrentAssets': '其他流动资产',
    'CAExceptionalItems': '流动资产特殊项目',
    'CAAdjustmentItems': '流动资产调整项目',
    'TotalCurrentAssets': '流动资产合计',
    'DebtInvestment': '债权投资',
    'OthDebtInvestment': '其他债权投资',
    'HoldToMaturityInvestments': '持有至到期投资',
    'HoldForSaleAssets': '可供出售金融资产',
    'OthEquityInstrument': '其他权益工具投资',
    'OthNonCurFinAssets': '其他非流动金融资产',
    'InvestmentProperty': '投资性房地产',
    'LongtermEquityInvest': '长期股权投资',
    'LongtermReceivableAccount': '长期应收款',
    'FixedAssets': '固定资产',
    'ConstructionMaterials': '工程物资',
    'ConstruInProcess': '在建工程',
    'FixedAssetsLiquidation': '固定资产清理',
    'BiologicalAssets': '生产性生物资产',
    'OilGasAssets': '油气资产',
    'IntangibleAssets': '无形资产',
    'SeatCosts': '交易席位费',
    'DevelopmentExpenditure': '开发支出',
    'GoodWill': '商誉',
    'LongDeferredExpense': '长期待摊费用',
    'DeferredTaxAssets': '递延所得税资产',
    'OtherNonCurrentAssets': '其他非流动资产',
    'NCAExceptionalItems': '非流动资产特殊项目',
    'NCAAdjustmentItems': '非流动资产调整项目',
    'TotalNonCurrentAssets': '非流动资产合计',
    'LoanAndAccountReceivables': '投资-贷款及应收款项(应收款项类投资)',
    'SettlementProvi': '结算备付金',
    'ClientProvi': '客户备付金',
    'DepositInInterbank': '存放同业款项',
    'RMetal': '贵金属',
    'LendCapital': '拆出资金',
    'DerivativeAssets': '衍生金融资产',
    'BoughtSellbackAssets': '买入返售金融资产',
    'LoanAndAdvance': '发放贷款和垫款',
    'InsuranceReceivables': '应收保费',
    'ReceivableSubrogationFee': '应收代位追偿款',
    'ReinsuranceReceivables': '应收分保账款',
    'ReceivableUnearnedR': '应收分保未到期责任准备金',
    'ReceivableClaimsR': '应收分保未决赔款准备金',
    'ReceivableLifeR': '应收分保寿险责任准备金',
    'ReceivableLTHealthR': '应收分保长期健康险责任准备金',
    'InsurerImpawnLoan': '保户质押贷款',
    'FixedDeposit': '定期存款',
    'RefundableDeposit': '存出保证金',
    'RefundableCapitalDeposit': '存出资本保证金',
    'IndependenceAccountAssets': '独立账户资产',
    'OtherAssets': '其他资产',
    'AExceptionalItems': '资产特殊项目',
    'AAdjustmentItems': '资产调整项目',
    'TotalAssets': '资产总计',
    'ShortTermLoan': '短期借款',
    'ImpawnedLoan': '质押借款',
    'TradingLiability': '交易性金融负债',
    'NotesPayable': '应付票据',
    'AccountsPayable': '应付账款',
    'NotAccountsPayable': '应付票据及应付账款',
    'ContractLiability': '合同负债',
    'STBondsPayable': '应付短期债券',
    'AdvanceReceipts': '预收款项',
    'SalariesPayable': '应付职工薪酬',
    'DividendPayable': '应付股利',
    'TaxsPayable': '应交税费',
    'InterestPayable': '应付利息',
    'OtherPayable': '其他应付款',
    'AccruedExpense': '预提费用',
    'DeferredProceeds': '递延收益',
    'HoldAndFSLi': '划分为持有待售的负债',
    'NonCurrentLiabilityIn1Year': '一年内到期的非流动负债',
    'OtherCurrentLiability': '其他流动负债',
    'CLExceptionalItems': '流动负债特殊项目',
    'CLAdjustmentItems': '流动负债调整项目',
    'TotalCurrentLiability': '流动负债合计',
    'LongtermLoan': '长期借款',
    'BondsPayable': '应付债券',
    'LPreferStock': '负债-优先股',
    'LPerpetualDebt': '负债-永续债',
    'LongtermAccountPayable': '长期应付款',
    'LongSalariesPay': '长期应付职工薪酬',
    'SpecificAccountPayable': '专项应付款',
    'EstimateLiability': '预计负债',
    'DeferredTaxLiability': '递延所得税负债',
    'LongDeferIncome': '长期递延收益',
    'OtherNonCurrentLiability': '其他非流动负债',
    'NCLExceptionalItems': '非流动负债特殊项目',
    'NCLAdjustmentItems': '非流动负债调整项目',
    'TotalNonCurrentLiability': '非流动负债合计',
    'BorrowingFromCentralBank': '向中央银行借款',
    'DepositOfInterbank': '同业及其他金融机构存放款项',
    'BorrowingCapital': '拆入资金',
    'DerivativeLiability': '衍生金融负债',
    'SoldBuybackSecuProceeds': '卖出回购金融资产款',
    'Deposit': '吸收存款',
    'ProxySecuProceeds': '代理买卖证券款',
    'SubIssueSecuProceeds': '代理承销证券款',
    'DepositsReceived': '存入保证金',
    'AdvanceInsurance': '预收保费',
    'CommissionPayable': '应付手续费及佣金',
    'ReinsurancePayables': '应付分保账款',
    'CompensationPayable': '应付赔付款',
    'PolicyDividendPayable': '应付保单红利',
    'InsurerDepositInvestment': '保户储金及投资款',
    'UnearnedPremiumReserve': '未到期责任准备金',
    'OutstandingClaimReserve': '未决赔款准备金',
    'LifeInsuranceReserve': '寿险责任准备金',
    'LTHealthInsuranceLR': '长期健康险责任准备金',
    'IndependenceLiability': '独立账户负债',
    'OtherLiability': '其他负债',
    'LExceptionalItems': '负债特殊项目',
    'LAdjustmentItems': '负债调整项目',
    'TotalLiability': '负债合计',
    'PaidInCapital': '实收资本(或股本)',
    'OtherEquityinstruments': '其他权益工具',
    'EPreferStock': '权益-优先股',
    'EPerpetualDebt': '权益-永续债',
    'CapitalReserveFund': '资本公积',
    'SurplusReserveFund': '盈余公积',
    'RetainedProfit': '未分配利润',
    'TreasuryStock': '库存股',
    'OtherCompositeIncome': '其他综合收益',
    'OrdinaryRiskReserveFund': '一般风险准备',
    'TradeRiskRSRVFd': '交易风险准备',
    'ForeignCurrencyReportConvDiff': '外币报表折算差额',
    'UncertainedInvestmentLoss': '未确认投资损失',
    'OtherReserves': '其他储备(公允价值变动储备)',
    'SpecificReserves': '专项储备',
    'SEExceptionalItems': '归属母公司所有者权益特殊项目',
    'SEAdjustmentItems': '归属母公司所有者权益调整项目',
    'SEWithoutMI': '归属母公司股东权益合计',
    'MinorityInterests': '少数股东权益',
    'OtherItemsEffectingSE': '所有者权益调整项目',
    'TotalShareholderEquity': '所有者权益(或股东权益)合计',
    'LEExceptionalItems': '负债和权益特殊项目',
    'LEAdjustmentItems': '负债和权益调整项目',
    'TotalLiabilityAndEquity': '负债和所有者权益(或股东权益)总计',

}

CASH_FLOW_MAP = {
    'InfoPublDate': '发布日',
    'EndDate': '报告日',
    'GoodsSaleServiceRenderCash': '销售商品、提供劳务收到的现金',
    'TaxLevyRefund': '收到的税费返还',
    'NetDepositIncrease': '客户存款和同业存放款项净增加额',
    'NetBorrowingFromCentralBank': '向中央银行借款净增加额',
    'NetBorrowingFromFinanceCo': '向其他金融机构拆入资金净增加额',
    'InterestAndCommissionCashIn': '收取利息、手续费及佣金的现金',
    'NetDealTradingAssets': '处置交易性金融资产净增加额',
    'NetBuyBack': '回购业务资金净增加额',
    'NetOriginalInsuranceCash': '收到原保险合同保费取得的现金',
    'NetReinsuranceCash': '收到再保业务现金净额',
    'NetInsurerDepositInvestment': '保户储金及投资款净增加额',
    'OtherCashInRelatedOperate': '收到其他与经营活动有关的现金',
    'SpecialItemsOCIF': '经营活动现金流入特殊项目',
    'AdjustmentItemsOCIF': '经营活动现金流入调整项目',
    'SubtotalOperateCashInflow': '经营活动现金流入小计',
    'GoodsServicesCashPaid': '购买商品、接受劳务支付的现金',
    'StaffBehalfPaid': '支付给职工以及为职工支付的现金',
    'AllTaxesPaid': '支付的各项税费',
    'NetLoanAndAdvanceIncrease': '客户贷款及垫款净增加额',
    'NetDepositInCBAndIB': '存放中央银行和同业款项净增加额',
    'NetLendCapital': '拆出资金净增加额',
    'CommissionCashPaid': '支付手续费及佣金的现金',
    'OriginalCompensationPaid': '支付原保险合同赔付款项的现金',
    'NetCashForReinsurance': '支付再保业务现金净额',
    'PolicyDividendCashPaid': '支付保单红利的现金',
    'OtherOperateCashPaid': '支付其他与经营活动有关的现金',
    'SpecialItemsOCOF': '经营活动现金流出特殊项目',
    'AdjustmentItemsOCOF': '经营活动现金流出调整项目',
    'SubtotalOperateCashOutflow': '经营活动现金流出小计',
    'AdjustmentItemsNOCF': '经营活动现金流量净额调整项目',
    'NetOperateCashFlow': '经营活动产生的现金流量净额',
    'InvestWithdrawalCash': '收回投资收到的现金',
    'Investproceeds': '取得投资收益收到的现金',
    'FixIntanOtherAssetDispoCash': '处置固定资产、无形资产和其他长期资产收回的现金净额',
    'NetCashDealSubCompany': '处置子公司及其他营业单位收到的现金净额',
    'OtherCashFromInvestAct': '收到其他与投资活动有关的现金',
    'SpecialItemsICIF': '投资活动现金流入特殊项目',
    'AdjustmentItemsICIF': '投资活动现金流入调整项目',
    'SubtotalInvestCashInflow': '投资活动现金流入小计',
    'FixIntanOtherAssetAcquiCash': '购建固定资产、无形资产和其他长期资产支付的现金',
    'InvestCashPaid': '投资支付的现金',
    'NetCashFromSubCompany': '取得子公司及其他营业单位支付的现金净额',
    'ImpawnedLoanNetIncrease': '质押贷款净增加额',
    'OtherCashToInvestAct': '支付其他与投资活动有关的现金',
    'SpecialItemsICOF': '投资活动现金流出特殊项目',
    'AdjustmentItemsICOF': '投资活动现金流出调整项目',
    'SubtotalInvestCashOutflow': '投资活动现金流出小计',
    'AdjustmentItemsNICF': '投资活动现金流量净额调整项目',
    'NetInvestCashFlow': '投资活动产生的现金流量净额',
    'CashFromInvest': '吸收投资收到的现金',
    'CashFromMinoSInvestSub': '子公司吸收少数股东投资收到的现金',
    'CashFromBondsIssue': '发行债券收到的现金',
    'CashFromBorrowing': '取得借款收到的现金',
    'OtherFinanceActCash': '收到其他与筹资活动有关的现金',
    'SpecialItemsFCIF': '筹资活动现金流入特殊项目',
    'AdjustmentItemsFCIF': '筹资活动现金流入调整项目',
    'SubtotalFinanceCashInflow': '筹资活动现金流入小计',
    'BorrowingRepayment': '偿还债务支付的现金',
    'DividendInterestPayment': '分配股利、利润或偿付利息支付的现金',
    'ProceedsFromSubToMinoS': '子公司支付给少数股东的股利、利润或偿付的利息',
    'OtherFinanceActPayment': '支付其他与筹资活动有关的现金',
    'SpecialItemsFCOF': '筹资活动现金流出特殊项目',
    'AdjustmentItemsFCOF': '筹资活动现金流出调整项目',
    'SubtotalFinanceCashOutflow': '筹资活动现金流出小计',
    'AdjustmentItemsNFCF': '筹资活动流量现金净额调整项目',
    'NetFinanceCashFlow': '筹资活动产生的现金流量净额',
    'ExchanRateChangeEffect': '汇率变动对现金及现金等价物的影响',
    'OtherItemsEffectingCE': '影响现金及现金等价物的其他科目',
    'AdjustmentItemsCE': '影响现金及现金等价物的调整项目',
    'CashEquivalentIncrease': '现金及现金等价物净增加额',
    'BeginPeriodCash': '期初现金及现金等价物余额',
    'OtherItemsEffectingCEI': '现金及现金等价物净增加额的特殊项目',
    'AdjustmentItemsCEI': '现金及现金等价物净增加额的调整项目',
    'EndPeriodCashEquivalent': '期末现金及现金等价物余额',

}

if __name__ == '__main__':
    pass