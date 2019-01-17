# -*- coding: utf-8 -*-

# Python 3.6
# author: EJ Jang
# date: 2018.06.14

# Options - Batch or Backfill (bkfil: False, True)
add_lback_kwarg = dict(add_lback_yr=70, add_lback_qtr=84)
fileName = '5yrRel_CFO2EV'
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

from batch_utils.utils_dateSeq import batch_sequence, cust_sequence
from batch_utils.utils_mapping import get_Mapping, getUnique_TMSRS_CD
from batch_utils.utils_mapping_orig import get_Mapping_orig
from batch_utils.ItemInfo import Item_lst
from batch_utils.common import chunker, chunker_count, add_mapped_tick
from batch_utils.common import firstCheck_duplicates, secondCheck_columns
from batch_utils.common import check_mapping_df, save_batch
from batch_utils.common import batch_monitor_msg, batch_finish_msg

from batch_utils import WS_retrieve_custom, WS_resample
from batch_utils.WS.WS_5yrRel_temp import *
from batch_utils import simple_add, simple_mult, simple_div, simple_subtract
from batch_utils import align_add, align_mult, align_div, align_subtract
from batch_utils import substitute_Value

# Date Sequence to be made by this batch
bkfil, rtvStart, seq_DT = batch_sequence(option, freq)
seq_DT_long = cust_sequence(rtvStart, freq=freq) # CUSTOM!!

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

DF_cfo2ev_yr, DF_cfo2ev_qtr = pd.DataFrame(), pd.DataFrame()

# Transform Security List into Batch Chunks
tot_n = chunker_count(trim_codeMap_uniq, 50)
Code_lst_tot = chunker(trim_codeMap_uniq.tolist(), 50)

# Bring needed items
cf_item = Item_lst.loc['NetCF_Operating']
ev_item = Item_lst.loc['EnterpriseValue']

def trimDateSeq(seq_DT, DF):
    return DF[DF['BASE_DT'].isin(seq_DT)]

st_time = time.time()
for i, Code_lst in enumerate(Code_lst_tot):
    # Operating CashFlow
    cf_yr, cf_qtr = WS_retrieve_custom(
        cf_item['Item'], cf_item['Table'], Code_lst, 'sum', bkfil, **add_lback_kwarg)
    # Enterprise Value
    ev_yr, ev_qtr = WS_retrieve_custom(
        ev_item['Item'], ev_item['Table'], Code_lst, 'avg', bkfil, **add_lback_kwarg)

    # <<Year>>
    fill_cols = ['FiscalPrd', 'CalPrdEndDate', 'Value_', 'FILLyr']
    cf_yr_samp = WS_resample(seq_DT_long, cf_yr, fill_cols=fill_cols)
    ev_yr_samp = WS_resample(seq_DT_long, ev_yr, fill_cols=fill_cols)
    
    cfo2ev_yr_samp = align_div(cf_yr_samp, ev_yr_samp)
    cfo2ev_yr_samp = Conv_Historical_Val2_speed(cfo2ev_yr_samp, back=5)

    # <<Qtr>>
    fill_cols = ['FiscalPrd', 'FiscalPrd2', 'CalPrdEndDate', 'Value_', 'FILLyr']
    cf_qtr_samp = WS_resample(seq_DT_long, cf_qtr, fill_cols=fill_cols)
    ev_qtr_samp = WS_resample(seq_DT_long, ev_qtr, fill_cols=fill_cols)

    cfo2ev_qtr_samp = align_div(cf_qtr_samp, ev_qtr_samp)
    cfo2ev_qtr_samp = Conv_Historical_Val2_speed(cfo2ev_qtr_samp, back=20)

    # <<< Trim Date >>>
    (cfo2ev_yr_samp, cfo2ev_qtr_samp) = (
        trimDateSeq(seq_DT, DF) for DF in
        [cfo2ev_yr_samp, cfo2ev_qtr_samp])

    DF_cfo2ev_qtr = DF_cfo2ev_qtr.append(cfo2ev_qtr_samp, sort=False)
    DF_cfo2ev_yr = DF_cfo2ev_yr.append(cfo2ev_yr_samp, sort=False)
    
    batch_monitor_msg(i, tot_n, st_time)
batch_finish_msg(tot_n, st_time)

DF_cfo2ev_tot = substitute_Value(DF_cfo2ev_yr, DF_cfo2ev_qtr, val_col='zValue_')
#*------------
firstCheck_duplicates(DF_cfo2ev_tot)
#*------------
# Map Code -> TMSRS_CD
# Should customize columns by needed ones.
# MUST HAVE 'BASE_DT', 'TMSRS_CD', 'Code', 'Value_', 'RGN_TP_CD', 'freq'
cols = ['BASE_DT', 'TMSRS_CD', 'Code', 'Value_yr', 'Value_qtr', 'Value_',
        'RGN_TP_CD', 'freq', 'ref']
DF_cfo2ev_tot = add_mapped_tick(DF_cfo2ev_tot, trim_codeMap)[cols]
DF_cfo2ev_tot['StyleName'] = '5YRel_CFO2EV'
#*------------
secondCheck_columns(DF_cfo2ev_tot)
#*------------
DF = DF_cfo2ev_tot

# Save!
save_batch(bkfil, DF, mapping, fileName)

from batch_utils.fill_monitor import print_fillReport
out = print_fillReport(bkfil, freq, DF)