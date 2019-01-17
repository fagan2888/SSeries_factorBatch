# -*- coding: utf-8 -*-

# Python 3.6
# author: EJ Jang
# date: 2018.06.14

# Options - Batch or Backfill (bkfil: False, True)
add_lback_kwarg = dict(add_lback_yr=10, add_lback_qtr=24)
fileName = 'ROIC'
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
from batch_utils.ItemInfo import Item_lst
from batch_utils.common import chunker, chunker_count, add_mapped_tick
from batch_utils.common import firstCheck_duplicates, secondCheck_columns
from batch_utils.common import check_mapping_df, save_batch
from batch_utils.common import batch_monitor_msg, batch_finish_msg

from batch_utils import WS_retrieve_custom, WS_year, WS_qtr_avg, WS_resample
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

DF_roic_yr, DF_roic_qtr = pd.DataFrame(), pd.DataFrame()

# Transform Security List into Batch Chunks
tot_n = chunker_count(trim_codeMap_uniq, 50)
Code_lst_tot = chunker(trim_codeMap_uniq.tolist(), 50)

# Bring needed items
oi_item = Item_lst.loc['OperatingIncome']
tax_item = Item_lst.loc['Eff_Tax']
aa_item = Item_lst.loc['TotAsset_st']
cs_item = Item_lst.loc['Cash_STInv']
pp_item = Item_lst.loc['PPE_net']
in_item = Item_lst.loc['Invest_AssoComp']

from scipy import stats
def f(x):
    d = {}
    d['Value_'] = x['Value_'].sum()
    d['FiscalPrd'] = (stats.mode(x['FiscalPrd'].astype(int),
                                 nan_policy='omit')[0][0])
    d['FiscalPrd'] = d['FiscalPrd'].astype(int).astype(str)
    return pd.Series(d, index=list(d.keys()))

def agg_add(*args):
    c_yr_samp = reduce(lambda x, y: pd.DataFrame.append(x, y, sort=False), args)
    tmp = c_yr_samp.groupby(['Code', 'BASE_DT'], as_index=False)['Item'].agg(
        {'CNT': len, 'filt': lambda x: (x == '2005').sum()})
    tmp = tmp[(tmp['filt'] == 1) & (tmp['CNT'] >= 2)]
    c_yr_samp = pd.merge(c_yr_samp, tmp[['Code', 'BASE_DT']], on=['Code', 'BASE_DT'])
    c_yr_samp_ = c_yr_samp.groupby(['Code', 'BASE_DT']).apply(f)
    c_yr_samp_.reset_index(inplace=True)
    return c_yr_samp_

st_time = time.time()
for i, Code_lst in enumerate(Code_lst_tot):
    # Operating Income
    oi_yr, oi_qtr = WS_retrieve_custom(
        oi_item['Item'], oi_item['Table'], Code_lst, 'sum', bkfil, **add_lback_kwarg)
    # Total Asset
    aa_yr, aa_qtr = WS_retrieve_custom(
        aa_item['Item'], aa_item['Table'], Code_lst, 'avg', bkfil, **add_lback_kwarg)
    # Short-term Cash
    cs_yr, cs_qtr = WS_retrieve_custom(
        cs_item['Item'], cs_item['Table'], Code_lst, 'avg', bkfil, **add_lback_kwarg)
    # PPE
    pp_yr, pp_qtr = WS_retrieve_custom(
        pp_item['Item'], pp_item['Table'], Code_lst, 'avg', bkfil, **add_lback_kwarg)
    # Invest AssoComp
    in_yr, in_qtr = WS_retrieve_custom(
        in_item['Item'], in_item['Table'], Code_lst, 'avg', bkfil, **add_lback_kwarg)
    # Effective Tax
    tax_yr = WS_year(tax_item['Item'], tax_item['Table'], Code_lst, bkfil)
    tax_qtr = WS_qtr_avg(tax_item['Item'], tax_item['Table'], Code_lst, bkfil, maxMonths=13)
    tax_yr['Value_'] = 1 - tax_yr['Value_'] / 100
    tax_qtr['Value_'] = 1 - tax_qtr['Value_'] / 100

    # Year
    fill_cols = ['FiscalPrd', 'CalPrdEndDate', 'Value_', 'FILLyr']
    oi_yr_samp = WS_resample(seq_DT, oi_yr, fill_cols=fill_cols)
    aa_yr_samp = WS_resample(seq_DT, aa_yr, fill_cols=fill_cols)
    cs_yr_samp = WS_resample(seq_DT, cs_yr, fill_cols=fill_cols)
    pp_yr_samp = WS_resample(seq_DT, pp_yr, fill_cols=fill_cols)
    in_yr_samp = WS_resample(seq_DT, in_yr, fill_cols=fill_cols)
    tax_yr_samp = WS_resample(seq_DT, tax_yr, fill_cols=fill_cols)

    c_yr_samp = agg_add(cs_yr_samp, pp_yr_samp, in_yr_samp)
    c_yr_samp = pd.merge(c_yr_samp, cs_yr_samp[
        ['Code', 'BASE_DT', 'FiscalPrd', 'CalPrdEndDate'
         ]].drop_duplicates(), on=['Code', 'BASE_DT', 'FiscalPrd'], how='left')

    ro_yr_samp = simple_mult(oi_yr_samp, tax_yr_samp).rename(columns={'FiscalPrd_0': 'FiscalPrd'})
    ic_yr_samp = simple_subtract(aa_yr_samp, c_yr_samp).rename(columns={'FiscalPrd_0': 'FiscalPrd'})
    ic_yr_samp = ic_yr_samp[ic_yr_samp['Value_'] > 0]

    roic_yr_samp = align_div(ro_yr_samp, ic_yr_samp)
    roic_yr_samp['Value_'] = roic_yr_samp['Value_'] * 100

    DF_roic_yr = DF_roic_yr.append(roic_yr_samp, sort=False)
    

    # Quarter
    fill_cols = ['FiscalPrd', 'FiscalPrd2', 'CalPrdEndDate', 'Value_', 'FILLyr']
    oi_qtr_samp = WS_resample(seq_DT, oi_qtr, fill_cols=fill_cols)
    aa_qtr_samp = WS_resample(seq_DT, aa_qtr, fill_cols=fill_cols)
    cs_qtr_samp = WS_resample(seq_DT, cs_qtr, fill_cols=fill_cols)
    pp_qtr_samp = WS_resample(seq_DT, pp_qtr, fill_cols=fill_cols)
    in_qtr_samp = WS_resample(seq_DT, in_qtr, fill_cols=fill_cols)
    tax_qtr_samp = WS_resample(seq_DT, tax_qtr, fill_cols=fill_cols)

    c_qtr_samp = agg_add(cs_qtr_samp, pp_qtr_samp, in_qtr_samp)
    c_qtr_samp = pd.merge(c_qtr_samp, cs_qtr_samp[
        ['Code', 'BASE_DT', 'FiscalPrd', 'CalPrdEndDate'
         ]].drop_duplicates(), on=['Code', 'BASE_DT', 'FiscalPrd'], how='left')

    ro_qtr_samp = simple_mult(oi_qtr_samp, tax_qtr_samp).rename(columns={'FiscalPrd_0': 'FiscalPrd'})
    ic_qtr_samp = simple_subtract(aa_qtr_samp, c_qtr_samp).rename(columns={'FiscalPrd_0': 'FiscalPrd'})
    ic_qtr_samp = ic_qtr_samp[ic_qtr_samp['Value_'] > 0]

    roic_qtr_samp = align_div(ro_qtr_samp, ic_qtr_samp)
    roic_qtr_samp['Value_'] = roic_qtr_samp['Value_'] * 100

    DF_roic_qtr = DF_roic_qtr.append(roic_qtr_samp, sort=False)
    
    batch_monitor_msg(i, tot_n, st_time)
batch_finish_msg(tot_n, st_time)

DF_roic_tot = substitute_Value(DF_roic_yr, DF_roic_qtr)
#*------------
firstCheck_duplicates(DF_roic_tot)
#*------------
# Map Code -> TMSRS_CD
# Should customize columns by needed ones.
# MUST HAVE 'BASE_DT', 'TMSRS_CD', 'Code', 'Value_', 'RGN_TP_CD', 'freq'
cols = ['BASE_DT', 'TMSRS_CD', 'Code', 'Value_yr', 'Value_qtr', 'Value_',
        'RGN_TP_CD', 'freq', 'ref']
DF_roic_tot = add_mapped_tick(DF_roic_tot, trim_codeMap)[cols]
DF_roic_tot['StyleName'] = 'ROIC'
#*------------
secondCheck_columns(DF_roic_tot)
#*------------
DF = DF_roic_tot

# Save!
save_batch(bkfil, DF, mapping, fileName)

from batch_utils.fill_monitor import print_fillReport
out = print_fillReport(bkfil, freq, DF)