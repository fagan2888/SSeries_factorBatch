# -*- coding: utf-8 -*-

# Python 3.6
# author: EJ Jang
# date: 2018.06.14

# Options - Batch or Backfill (bkfil: False, True)
add_lback_kwarg = dict(add_lback_yr=10, add_lback_qtr=24)
fileName = 'CAcqR'
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

DF_CAcqR_yr, DF_CAcqR_qtr = pd.DataFrame(), pd.DataFrame()

# Transform Security List into Batch Chunks
tot_n = chunker_count(trim_codeMap_uniq, 50)
Code_lst_tot = chunker(trim_codeMap_uniq.tolist(), 50)

# Bring needed items
cf_item = Item_lst.loc['NetCF_Operating']
dv_item = Item_lst.loc['CashDivPaid_tot']
cx_item = Item_lst.loc['CapEx']

st_time = time.time()
for i, Code_lst in enumerate(Code_lst_tot):
    # Operating CashFlow
    cf_yr, cf_qtr = WS_retrieve_custom(
        cf_item['Item'], cf_item['Table'], Code_lst, 'sum', bkfil, **add_lback_kwarg)
    # Cash Dividend
    dv_yr, dv_qtr = WS_retrieve_custom(
        dv_item['Item'], dv_item['Table'], Code_lst, 'sum', bkfil, **add_lback_kwarg)
    # Capex
    cx_yr, cx_qtr = WS_retrieve_custom(
        cx_item['Item'], cx_item['Table'], Code_lst, 'sum', bkfil, **add_lback_kwarg)

    # Year
    fill_cols = ['FiscalPrd', 'CalPrdEndDate', 'Value_', 'FILLyr']
    cf_yr_samp = WS_resample(seq_DT, cf_yr, fill_cols=fill_cols)
    dv_yr_samp = WS_resample(seq_DT, dv_yr, fill_cols=fill_cols)
    cx_yr_samp = WS_resample(seq_DT, cx_yr, fill_cols=fill_cols)

    cols = ['BASE_DT', 'Code', 'FiscalPrd', 'Value_']
    cc_yr_samp = align_add(cf_yr_samp, dv_yr_samp)
    cacqr_yr_samp = align_div(cc_yr_samp[cols], cx_yr_samp)

    DF_CAcqR_yr = DF_CAcqR_yr.append(cacqr_yr_samp,sort=False)
    

    # Quarter
    fill_cols = ['FiscalPrd', 'FiscalPrd2', 'CalPrdEndDate', 'Value_', 'FILLyr']
    cf_qtr_samp = WS_resample(seq_DT, cf_qtr, fill_cols=fill_cols)
    dv_qtr_samp = WS_resample(seq_DT, dv_qtr, fill_cols=fill_cols)
    cx_qtr_samp = WS_resample(seq_DT, cx_qtr, fill_cols=fill_cols)
    
    cols = ['BASE_DT', 'Code', 'FiscalPrd', 'Value_']
    cc_qtr_samp = align_add(cf_qtr_samp, dv_qtr_samp)
    cacqr_qtr_samp = align_div(cc_qtr_samp[cols], cx_qtr_samp)

    DF_CAcqR_qtr = DF_CAcqR_qtr.append(cacqr_qtr_samp, sort=False)
    
    batch_monitor_msg(i, tot_n, st_time)
batch_finish_msg(tot_n, st_time)

DF_CAcqR_tot = substitute_Value(DF_CAcqR_yr, DF_CAcqR_qtr)
#*------------
firstCheck_duplicates(DF_CAcqR_tot)
#*------------
# Map Code -> TMSRS_CD
# Should customize columns by needed ones.
# MUST HAVE 'BASE_DT', 'TMSRS_CD', 'Code', 'Value_', 'RGN_TP_CD', 'freq'
cols = ['BASE_DT', 'TMSRS_CD', 'Code', 'Value_yr', 'Value_qtr', 'Value_',
        'RGN_TP_CD', 'freq', 'ref']
DF_CAcqR_tot = add_mapped_tick(DF_CAcqR_tot, trim_codeMap)[cols]
DF_CAcqR_tot['StyleName'] = 'CAcqR'
#*------------
secondCheck_columns(DF_CAcqR_tot)
#*------------
DF = DF_CAcqR_tot

# Save!
save_batch(bkfil, DF, mapping, fileName)

from batch_utils.fill_monitor import print_fillReport
out = print_fillReport(bkfil, freq, DF)