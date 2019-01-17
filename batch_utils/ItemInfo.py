import pandas as pd


# Core Items
Item_lst = []
Item_lst.append({'Item': '18150', 'Table': 'WSPITSupp', 'Name': 'Earn_from_c_Op_st'})
Item_lst.append({'Item': '3501', 'Table': 'WSPITFinVal', 'Name': 'CommonEquity_st'})
Item_lst.append({'Item': '3995', 'Table': 'WSPITFinVal', 'Name': 'TotEquity_st'})
Item_lst.append({'Item': '2999', 'Table': 'WSPITFinVal', 'Name': 'TotAsset_st'})
Item_lst.append({'Item': '1551', 'Table': 'WSPITFinVal', 'Name': 'NetIncome_bExIt'})
Item_lst.append({'Item': '1001', 'Table': 'WSPITFinVal', 'Name': 'NetSales_Revenue'})
Item_lst.append({'Item': '1100', 'Table': 'WSPITFinVal', 'Name': 'GrossIncome'})
Item_lst.append({'Item': '1250', 'Table': 'WSPITFinVal', 'Name': 'OperatingIncome'})
Item_lst.append({'Item': '1540', 'Table': 'WSPITFinVal', 'Name': 'NetOperatingIncome'})  # Operating Leverage
Item_lst.append({'Item': '8346', 'Table': 'WSPITRatios', 'Name': 'Eff_Tax'})
Item_lst.append({'Item': '2005', 'Table': 'WSPITFinVal', 'Name': 'Cash_STInv'})
Item_lst.append({'Item': '3255', 'Table': 'WSPITFinVal', 'Name': 'Total_Debt'})
Item_lst.append({'Item': '4860', 'Table': 'WSPITFinVal', 'Name': 'NetCF_Operating'})
Item_lst.append({'Item': '4601', 'Table': 'WSPITFinVal', 'Name': 'CapEx'})
Item_lst.append({'Item': '3251', 'Table': 'WSPITFinVal', 'Name': 'LT_Debt'})
Item_lst.append({'Item': '1501', 'Table': 'WSPITFinVal', 'Name': 'MinorityInterest'})
# Additional
Item_lst.append({'Item': '1051', 'Table': 'WSPITFinVal', 'Name': 'COGS_exDep'})
Item_lst.append({'Item': '18100', 'Table': 'WSPITSupp', 'Name': 'EnterpriseValue'})
Item_lst.append({'Item': '18190', 'Table': 'WSPITSupp', 'Name': 'Ebit'})  # Not Used
Item_lst.append({'Item': '4051', 'Table': 'WSPITFinVal', 'Name': 'Dep_n_Amort'})
Item_lst.append({'Item': '2051', 'Table': 'WSPITFinVal', 'Name': 'AccountsReceiv'})
Item_lst.append({'Item': '4551', 'Table': 'WSPITFinVal', 'Name': 'CashDivPaid_tot'})
Item_lst.append({'Item': '1251', 'Table': 'WSPITFinVal', 'Name': 'InterestExpense'})
Item_lst.append({'Item': '2501', 'Table': 'WSPITFinVal', 'Name': 'PPE_net'})
Item_lst.append({'Item': '2256', 'Table': 'WSPITFinVal', 'Name': 'Invest_AssoComp'})
Item_lst.append({'Item': '1706', 'Table': 'WSPITFinVal', 'Name': 'NetIncome_EPS'})
# "Operating Leverage" = (Rev - COGS) / NI
Item_lst = pd.DataFrame(Item_lst).set_index('Name')


# Ratio Items
RatioItem_lst = []
RatioItem_lst.append({'Item': '8316', 'Table': 'WSPITRatios', 'Name': 'OPM_ws'})
RatioItem_lst.append({'Item': '8366', 'Table': 'WSPITRatios', 'Name': 'NM_ws'})
RatioItem_lst.append({'Item': '8301', 'Table': 'WSPITRatios', 'Name': 'ROE_ws'})
RatioItem_lst.append({'Item': '8326', 'Table': 'WSPITRatios', 'Name': 'ROA_ws'})
RatioItem_lst.append({'Item': '8306', 'Table': 'WSPITRatios', 'Name': 'GPM_ws'})
RatioItem_lst.append({'Item': '8381', 'Table': 'WSPITRatios', 'Name': 'CFE_ws'})  # Only Yearly
RatioItem_lst.append({'Item': '8376', 'Table': 'WSPITRatios', 'Name': 'ROIC_ws'})  # Only Yearly
RatioItem_lst.append({'Item': '8631', 'Table': 'WSPITRatios', 'Name': 'REVg_l1yr_ws'})
RatioItem_lst.append({'Item': '8636', 'Table': 'WSPITRatios', 'Name': 'NIg_l1yr_ws'})
RatioItem_lst.append({'Item': '8226', 'Table': 'WSPITRatios', 'Name': 'LTDtCE_ws'})
RatioItem_lst.append({'Item': '8256', 'Table': 'WSPITRatios', 'Name': 'Payout_ws'})
RatioItem_lst = pd.DataFrame(RatioItem_lst).set_index('Name')

# CurrentData Items
CurrItem_lst = []
CurrItem_lst.append({'Item': '9102', 'Table': 'WSPITCmpIssFData',
                     'Name': 'PE_curr'})
CurrItem_lst.append({'Item': '9302', 'Table': 'WSPITCmpIssFData',
                     'Name': 'PB_curr'})
CurrItem_lst.append({'Item': '9402', 'Table': 'WSPITCmpIssFData',
                     'Name': 'DY_curr'})
CurrItem_lst.append({'Item': '9602', 'Table': 'WSPITCmpIssFData',
                     'Name': 'PC_curr'})
CurrItem_lst.append({'Item': '9202', 'Table': 'WSPITCmpIssFData',
                     'Name': 'EY_curr'})
CurrItem_lst.append({'Item': '8372', 'Table': 'WSPITCmpIssFData',
                     'Name': 'ROE_curr'})
CurrItem_lst.append({'Item': '9502', 'Table': 'WSPITCmpIssFData',
                     'Name': 'Payout_curr'})
CurrItem_lst.append({'Item': '5302', 'Table': 'WSPITCmpIssFData',
                     'Name': 'Sh_Out_curr'}) # Table has 0 with NULL EndDate (check)
CurrItem_lst.append({'Item': '8005', 'Table': 'WSPITCmpIssFData',
                     'Name': 'MkCap_curr'}) # Table has 0 with NULL EndDate (check)
CurrItem_lst.append({'Item': '9802', 'Table': 'WSPITCmpIssFData',
                     'Name': 'Beta'}) # Table has 0 with NULL EndDate (check)
CurrItem_lst = pd.DataFrame(CurrItem_lst).set_index('Name')


# IBES Items
IBESItem_lst = []
IBESItem_lst.append({'TableMeasure': 8, 'TableAct': 'IBESActL1', 'TableEst': 'IBESEstL1',
                     'Name': 'EPS_US', 'delay_type': 'dd', 'delay_per': 50, 'var_Name': 'EPS'})
IBESItem_lst.append({'TableMeasure': 8, 'TableAct': 'IBGSActL1', 'TableEst': 'IBGSEstL1',
                     'Name': 'EPS_exUS', 'delay_type': 'dd', 'delay_per': 50, 'var_Name': 'EPS'})

IBESItem_lst.append({'TableMeasure': 5, 'TableAct': 'IBESActL2', 'TableEst': 'IBESEstL2',
                     'Name': 'DPS_US', 'delay_type': 'dd', 'delay_per': 50, 'var_Name': 'DPS'})
IBESItem_lst.append({'TableMeasure': 5, 'TableAct': 'IBGSActL2', 'TableEst': 'IBGSEstL2',
                     'Name': 'DPS_exUS', 'delay_type': 'dd', 'delay_per': 50, 'var_Name': 'DPS'})

IBESItem_lst.append({'TableMeasure': 29, 'TableAct': 'IBESActL3', 'TableEst': 'IBESEstL3',
                     'Name': 'ROE_US', 'delay_type': 'dd', 'delay_per': 50, 'var_Name': 'ROE'})
IBESItem_lst.append({'TableMeasure': 29, 'TableAct': 'IBGSActL3', 'TableEst': 'IBGSEstL3',
                     'Name': 'ROE_exUS', 'delay_type': 'dd', 'delay_per': 50, 'var_Name': 'ROE'})
IBESItem_lst = pd.DataFrame(IBESItem_lst).set_index('Name')
