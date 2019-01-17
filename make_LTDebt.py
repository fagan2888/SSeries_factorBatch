# -*- coding: utf-8 -*-

# Python 3.6
# author: EJ Jang
# date: 2018.06.14

# Options - Batch or Backfill (bkfil: False, True)
add_lback_kwarg = dict(add_lback_yr=10, add_lback_qtr=24)
fileName = 'LTDebt'
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

from batch_utils.utils_dateSeq import batch_sequence
from batch_utils.utils_mapping import get_Mapping, getUnique_TMSRS_CD
from batch_utils.utils_mapping_orig import get_Mapping_orig
from batch_utils.ItemInfo import Item_lst
from batch_utils.common import chunker, chunker_count, add_mapped_tick
from batch_utils.common import firstCheck_duplicates, secondCheck_columns
from batch_utils.common import check_mapping_df, save_batch
from batch_utils.common import batch_monitor_msg, batch_finish_msg

from batch_utils import WS_retrieve_custom, WS_resample
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

DF_de_yr, DF_de_qtr = pd.DataFrame(), pd.DataFrame()

# Transform Security List into Batch Chunks
tot_n = chunker_count(trim_codeMap_uniq, 50)
Code_lst_tot = chunker(trim_codeMap_uniq.tolist(), 50)

# Bring needed items
d_item = Item_lst.loc['LT_Debt']
e_item = Item_lst.loc['TotEquity_st']

st_time = time.time()
for i, Code_lst in enumerate(Code_lst_tot):
    # Long-term Debt
    d_yr, d_qtr = WS_retrieve_custom(
        d_item['Item'], d_item['Table'], Code_lst, 'avg', bkfil, **add_lback_kwarg)
    # Total Equity
    e_yr, e_qtr = WS_retrieve_custom(
        e_item['Item'], e_item['Table'], Code_lst, 'avg', bkfil, **add_lback_kwarg)

    # Year
    fill_cols = ['FiscalPrd', 'CalPrdEndDate', 'Value_', 'FILLyr']
    d_yr_samp = WS_resample(seq_DT, d_yr, fill_cols=fill_cols)
    e_yr_samp = WS_resample(seq_DT, e_yr, fill_cols=fill_cols)

    cols = ['BASE_DT', 'Code', 'FiscalPrd', 'Value_']
    de_yr_samp = align_div(d_yr_samp, e_yr_samp)
    de_yr_samp = de_yr_samp[de_yr_samp['Value_'] >= 0]

    DF_de_yr = DF_de_yr.append(de_yr_samp, sort=False)
    

    # Quarter
    fill_cols = ['FiscalPrd', 'FiscalPrd2', 'CalPrdEndDate', 'Value_', 'FILLyr']
    d_qtr_samp = WS_resample(seq_DT, d_qtr, fill_cols=fill_cols)
    e_qtr_samp = WS_resample(seq_DT, e_qtr, fill_cols=fill_cols)

    cols = ['BASE_DT', 'Code', 'FiscalPrd', 'Value_']
    de_qtr_samp = align_div(d_qtr_samp, e_qtr_samp)
    de_qtr_samp = de_qtr_samp[de_qtr_samp['Value_'] >= 0]

    DF_de_qtr = DF_de_qtr.append(de_qtr_samp, sort=False)
    
    batch_monitor_msg(i, tot_n, st_time)
batch_finish_msg(tot_n, st_time)

DF_de_tot = substitute_Value(DF_de_yr, DF_de_qtr)
#*------------
firstCheck_duplicates(DF_de_tot)
#*------------
# Map Code -> TMSRS_CD
# Should customize columns by needed ones.
# MUST HAVE 'BASE_DT', 'TMSRS_CD', 'Code', 'Value_', 'RGN_TP_CD', 'freq'
cols = ['BASE_DT', 'TMSRS_CD', 'Code', 'Value_yr', 'Value_qtr', 'Value_',
        'RGN_TP_CD', 'freq', 'ref']
DF_de_tot = add_mapped_tick(DF_de_tot, trim_codeMap)[cols]
DF_de_tot['StyleName'] = 'LTDtE'
#*------------
secondCheck_columns(DF_de_tot)
#*------------
DF = DF_de_tot

# Save!
save_batch(bkfil, DF, mapping, fileName)

from batch_utils.fill_monitor import print_fillReport
out = print_fillReport(bkfil, freq, DF)