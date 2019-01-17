# -*- coding: utf-8 -*-

# Python 3.6
# author: EJ Jang
# date: 2018.06.14

# Options - Batch or Backfill (bkfil: False, True)
add_lback_kwarg = dict(add_lback_yr=10, add_lback_qtr=24)
fileName = 'Value'
mapping = 'worldscope'
# option = 'batch'
# freq = 'W'
import sys
option = sys.argv[1]
freq = sys.argv[2]
print('# Starting Factor - {}_{} ({})'.format(mapping, fileName, freq))

import numpy as np
import pandas as pd
import datetime as dt
import time
import re
from functools import reduce

from batch_utils.utils_dateSeq import batch_sequence
from batch_utils.utils_mapping import get_Mapping, getUnique_TMSRS_CD
from batch_utils.utils_mapping_orig import get_Mapping_orig
from batch_utils.ItemInfo import CurrItem_lst, Item_lst
from batch_utils.common import chunker, chunker_count, add_mapped_tick
from batch_utils.common import firstCheck_duplicates, secondCheck_columns
from batch_utils.common import check_mapping_df, save_batch
from batch_utils.common import batch_monitor_msg, batch_finish_msg

from batch_utils import WS_currVal, WS_retrieve_custom, WS_resample
from batch_utils import simple_add, simple_mult, simple_div, simple_subtract
from batch_utils import align_add, align_mult, align_div, align_subtract
from batch_utils import substitute_Value

# Date Sequence to be made by this batch
bkfil, rtvStart, seq_DT = batch_sequence(option, freq)

# Getting the Universe in TMSRS_CD ~ Code Map
allSec = getUnique_TMSRS_CD()
codeMap = get_Mapping_orig(mapping)
trim_codeMap = codeMap[codeMap['TMSRS_CD'].isin(allSec)].copy()
trim_codeMap_uniq = trim_codeMap['Code'].unique()

print(trim_codeMap.iloc[:2])
print('\n>>> Total Mapping Securities #: {}'.format(trim_codeMap.shape[0]))
print('>>> Total Performing Securities #: {}'.format(trim_codeMap_uniq.shape[0]))

# Checking Level of Duplicates in codeMap
chk_codeMap = check_mapping_df(trim_codeMap)

DF_Value = pd.DataFrame()

# Transform Security List into Batch Chunks
tot_n = chunker_count(trim_codeMap_uniq, 50)
Code_lst_tot = chunker(trim_codeMap_uniq.tolist(), 50)

# Bring needed items
pe_item = CurrItem_lst.loc['PE_curr']
pb_item = CurrItem_lst.loc['PB_curr']
dy_item = CurrItem_lst.loc['DY_curr']
pc_item = CurrItem_lst.loc['PC_curr']
roe_item = CurrItem_lst.loc['ROE_curr']
payout_item = CurrItem_lst.loc['Payout_curr']
mktcap_item = CurrItem_lst.loc['MkCap_curr']
hist_item = Item_lst.loc[['NetSales_Revenue', 'NetCF_Operating', 'CapEx']]

# Unused because Debugged in WS_currVal
# def MkCap_debug(df):
#     # Debugging zero trails in data, glitch for 
#     df = df.copy()
#     df.sort_values('BASE_DT', inplace=True)
#     loc_z = (df['Value_']==0).astype(int)
#     if loc_z.sum() >= 1:
#         _srs = loc_z.cumsum()
#         _srs_d = _srs.diff()

#         _srs_d = _srs_d.values
#         _srs_d[np.isnan(_srs_d)] = 0
#         str_seq = ''.join(_srs_d.astype(int).astype(str))
#         _srch = re.search('1+$', str_seq)
#         if _srch is not None:
#             i_ = _srch.start()
#             return df.iloc[:i_]
#         else:
#             df.loc[df['Value_']==0, 'Value_'] = np.nan
#             df['Value_'].fillna(method='ffill', limit=3, inplace=True)
#             return df.reset_index(drop=True)
#     else:
#         return df.reset_index(drop=True)

st_time = time.time()
for i, Code_lst in enumerate(Code_lst_tot):
    # Long-term Debt
    pe_DF = WS_currVal(
        seq_DT, pe_item['Item'], pe_item['Table'], Code_lst, Name=pe_item.name)
    pb_DF = WS_currVal(
        seq_DT, pb_item['Item'], pb_item['Table'], Code_lst, Name=pb_item.name)
    dy_DF = WS_currVal(
        seq_DT, dy_item['Item'], dy_item['Table'], Code_lst, Name=dy_item.name)
    pc_DF = WS_currVal(
        seq_DT, pc_item['Item'], pc_item['Table'], Code_lst, Name=pc_item.name)
    roe_DF = WS_currVal(
        seq_DT, roe_item['Item'], roe_item['Table'], Code_lst, Name=roe_item.name)
    payout_DF = WS_currVal(
        seq_DT, payout_item['Item'], payout_item['Table'], Code_lst,
        Name=payout_item.name)
    mktcap_DF = WS_currVal(
        seq_DT, mktcap_item['Item'], mktcap_item['Table'], Code_lst,
        Name=mktcap_item.name)
    mktcap_DF = mktcap_DF[mktcap_DF['Value_'] != 0]
    # mktcap_DF = mktcap_DF.groupby('Code').apply(MkCap_debug)
    # mktcap_DF.index = mktcap_DF.index.droplevel(level=1)
    # mktcap_DF.index.rename(None, inplace=True)
    # mktcap_DF.reset_index(drop=True, inplace=True)

    hist_DF_yr, hist_DF_qtr = WS_retrieve_custom(
        hist_item['Item'], hist_item['Table'].iloc[0], Code_lst, 'sum', bkfil, **add_lback_kwarg)

    # Year
    fill_cols = ['FiscalPrd', 'CalPrdEndDate', 'Value_', 'FILLyr']
    hist_yr_samp = WS_resample(seq_DT, hist_DF_yr, fill_cols=fill_cols)
    # Yr-P2Sales
    ps_DF_yr = pd.merge(
        mktcap_DF.rename(columns={'Value_': 'mktcap'})[['BASE_DT', 'Code', 'mktcap']],
        hist_yr_samp[hist_yr_samp['Item'] == '1001'].rename(
            columns={'Value_': 'sales'})[['BASE_DT', 'Code', 'sales']],
        on=['BASE_DT', 'Code']
    )
    ps_DF_yr['Value_'], ps_DF_yr['freq'] = ps_DF_yr['mktcap'] / ps_DF_yr['sales'], 'Y'
    # Yr-P2FCF
    pfcf_DF_yr = align_subtract(
        hist_yr_samp[hist_yr_samp['Item'] == '4860'][['BASE_DT', 'Code', 'FiscalPrd', 'Value_']],
        hist_yr_samp[hist_yr_samp['Item'] == '4601'][['BASE_DT', 'Code', 'FiscalPrd', 'Value_']]
    )
    pfcf_DF_yr = pd.merge(
        mktcap_DF.rename(columns={'Value_': 'mktcap'})[['BASE_DT', 'Code', 'mktcap']],
        pfcf_DF_yr.rename(columns={'Value_0': 'CFO', 'Value_1': 'Capex', 'Value_': 'FCF'}),
        on=['BASE_DT', 'Code']
    )
    pfcf_DF_yr['Value_'], pfcf_DF_yr['freq'] = pfcf_DF_yr['mktcap'] / pfcf_DF_yr['FCF'], 'Y'
    

    # Qtr
    fill_cols = ['FiscalPrd', 'FiscalPrd2', 'CalPrdEndDate', 'Value_', 'FILLyr']
    hist_qtr_samp = WS_resample(seq_DT, hist_DF_qtr, fill_cols=fill_cols)
    # Qtr-P2Sales
    ps_DF_qtr = pd.merge(
        mktcap_DF.rename(columns={'Value_': 'mktcap'})[['BASE_DT', 'Code', 'mktcap']],
        hist_qtr_samp[hist_qtr_samp['Item'] == '1001'].rename(
            columns={'Value_': 'sales'})[['BASE_DT', 'Code', 'sales']],
        on=['BASE_DT', 'Code']
    )
    ps_DF_qtr['Value_'], ps_DF_qtr['freq'] = ps_DF_qtr['mktcap'] / ps_DF_qtr['sales'], 'Q'
    # Qtr-P2FCF
    pfcf_DF_qtr = align_subtract(
        hist_qtr_samp[hist_qtr_samp['Item'] == '4860'][['BASE_DT', 'Code', 'FiscalPrd', 'Value_']],
        hist_qtr_samp[hist_qtr_samp['Item'] == '4601'][['BASE_DT', 'Code', 'FiscalPrd', 'Value_']]
    )
    pfcf_DF_qtr = pd.merge(
        mktcap_DF.rename(columns={'Value_': 'mktcap'})[['BASE_DT', 'Code', 'mktcap']],
        pfcf_DF_qtr.rename(columns={'Value_0': 'CFO', 'Value_1': 'Capex', 'Value_': 'FCF'}),
        on=['BASE_DT', 'Code']
    )
    pfcf_DF_qtr['Value_'], pfcf_DF_qtr['freq'] = pfcf_DF_qtr['mktcap'] / pfcf_DF_qtr['FCF'], 'Q'

    # <<P2Sales>>
    ps_DF = substitute_Value(ps_DF_yr, ps_DF_qtr).copy()
    ps_DF = ps_DF[['BASE_DT', 'Code', 'freq', 'Value_']]
    ps_DF['Item'], ps_DF['Name'] = np.nan, 'PS_curr'
    ps_DF = ps_DF[(~ps_DF['Value_'].isin([np.Inf, -np.Inf])) &
                  ps_DF['Value_'].notnull()]
    pfcf_DF = substitute_Value(pfcf_DF_yr, pfcf_DF_qtr).copy()
    pfcf_DF = pfcf_DF[['BASE_DT', 'Code', 'freq', 'Value_']]
    pfcf_DF['Item'], pfcf_DF['Name'] = np.nan, 'PFCF_curr'
    pfcf_DF = pfcf_DF[(~pfcf_DF['Value_'].isin([np.Inf, -np.Inf])) &
                      pfcf_DF['Value_'].notnull()]

    TMP = reduce(lambda x, y: pd.DataFrame.append(x, y, sort=True),
                 [pe_DF, pb_DF, dy_DF, pc_DF, roe_DF, payout_DF, mktcap_DF])
    TMP['freq'] = 'curr'
    DF_Value = reduce(lambda x, y: pd.DataFrame.append(x, y, sort=True),
                      [DF_Value, TMP, ps_DF, pfcf_DF])
    
    batch_monitor_msg(i, tot_n, st_time)
batch_finish_msg(tot_n, st_time)

#*------------
firstCheck_duplicates(DF_Value, add_cols=['Name'])
#*------------
# Map Code -> TMSRS_CD
# Should customize columns by needed ones.
# MUST HAVE 'BASE_DT', 'TMSRS_CD', 'Code', 'Value_', 'RGN_TP_CD', 'freq'
cols = ['BASE_DT', 'TMSRS_CD', 'Code', 'Value_', 'RGN_TP_CD','freq', 'Name']
DF_Value = DF_Value.groupby('Name').apply(lambda x: add_mapped_tick(x, trim_codeMap))
DF_Value.index = DF_Value.index.droplevel(level=1)
DF_Value.reset_index(drop=True, inplace=True)
DF_Value = DF_Value[cols]

DF_Value.rename(columns={'Name': 'StyleName'}, inplace=True)
DF_Value['ref'] = None
#*------------
secondCheck_columns(DF_Value)
#*------------

# Save!
save_batch(bkfil, DF_Value, mapping, fileName)

from batch_utils.fill_monitor import print_fillReport
out = print_fillReport(bkfil, freq, DF_Value)