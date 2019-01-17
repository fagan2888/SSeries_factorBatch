# -*- coding: utf-8 -*-

# Python 3.6
# author: EJ Jang
# date: 2018.06.14

# Options - Batch or Backfill (bkfil: False, True)
add_lback_kwarg = dict(add_lback_yr=10, add_lback_qtr=24)
fileName = 'Accruals'
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

DF_accruals_yr, DF_accruals_qtr = pd.DataFrame(), pd.DataFrame()
DF_accrualsS_yr, DF_accrualsS_qtr = pd.DataFrame(), pd.DataFrame()

# Transform Security List into Batch Chunks
tot_n = chunker_count(trim_codeMap_uniq, 50)
Code_lst_tot = chunker(trim_codeMap_uniq.tolist(), 50)

# Bring needed items
cf_item  = Item_lst.loc['NetCF_Operating']
ni_item  = Item_lst.loc['NetIncome_bExIt']
rev_item = Item_lst.loc['NetSales_Revenue']
aa_item  = Item_lst.loc['TotAsset_st']

st_time = time.time()
for i, Code_lst in enumerate(Code_lst_tot):
    # Cashflow
    cf_yr, cf_qtr = WS_retrieve_custom(
        cf_item['Item'], cf_item['Table'], Code_lst, 'sum', bkfil, **add_lback_kwarg)
    # NetIncome
    ni_yr, ni_qtr = WS_retrieve_custom(
        ni_item['Item'], ni_item['Table'], Code_lst, 'sum', bkfil, **add_lback_kwarg)
    # Revenue
    rev_yr, rev_qtr = WS_retrieve_custom(
        rev_item['Item'], rev_item['Table'], Code_lst, 'sum', bkfil, **add_lback_kwarg)
    # Total Asset
    aa_yr, aa_qtr = WS_retrieve_custom(
        aa_item['Item'], aa_item['Table'], Code_lst, 'avg', bkfil, **add_lback_kwarg)

    # Year
    fill_cols = ['FiscalPrd', 'CalPrdEndDate', 'Value_', 'FILLyr']
    cf_yr_samp = WS_resample(seq_DT, cf_yr, fill_cols=fill_cols)
    ni_yr_samp = WS_resample(seq_DT, ni_yr, fill_cols=fill_cols)
    rev_yr_samp = WS_resample(seq_DT, rev_yr, fill_cols=fill_cols)
    aa_yr_samp = WS_resample(seq_DT, aa_yr, fill_cols=fill_cols)

    cols = ['BASE_DT', 'Code', 'FiscalPrd', 'Value_']
    _acc_yr_samp = align_subtract(cf_yr_samp, ni_yr_samp)
    accruals_yr_samp = align_div(_acc_yr_samp[cols], aa_yr_samp)
    accrualsS_yr_samp = align_div(_acc_yr_samp[cols], rev_yr_samp)

    DF_accruals_yr = DF_accruals_yr.append(accruals_yr_samp, sort=False)
    DF_accrualsS_yr = DF_accrualsS_yr.append(accrualsS_yr_samp, sort=False)


    # Quarter
    fill_cols = ['FiscalPrd', 'FiscalPrd2', 'CalPrdEndDate', 'Value_', 'FILLyr']
    cf_qtr_samp = WS_resample(seq_DT, cf_qtr, fill_cols=fill_cols)
    ni_qtr_samp = WS_resample(seq_DT, ni_qtr, fill_cols=fill_cols)
    rev_qtr_samp = WS_resample(seq_DT, rev_qtr, fill_cols=fill_cols)
    aa_qtr_samp = WS_resample(seq_DT, aa_qtr, fill_cols=fill_cols)

    cols = ['BASE_DT', 'Code', 'FiscalPrd', 'Value_']
    _acc_qtr_samp = align_subtract(cf_qtr_samp, ni_qtr_samp)
    accruals_qtr_samp = align_div(_acc_qtr_samp[cols], aa_qtr_samp)
    accrualsS_qtr_samp = align_div(_acc_qtr_samp[cols], rev_qtr_samp)

    DF_accruals_qtr = DF_accruals_qtr.append(accruals_qtr_samp, sort=False)
    DF_accrualsS_qtr = DF_accrualsS_qtr.append(accrualsS_qtr_samp, sort=False)

    batch_monitor_msg(i, tot_n, st_time)
batch_finish_msg(tot_n, st_time)

DF_accruals_tot = substitute_Value(DF_accruals_yr, DF_accruals_qtr)
DF_accrualsS_tot = substitute_Value(DF_accrualsS_yr, DF_accrualsS_qtr)
#*------------
firstCheck_duplicates(DF_accruals_tot, DF_accrualsS_tot)
#*------------
# Map Code -> TMSRS_CD
# Should customize columns by needed ones.
# MUST HAVE 'BASE_DT', 'TMSRS_CD', 'Code', 'Value_', 'RGN_TP_CD', 'freq'
cols = ['BASE_DT', 'TMSRS_CD', 'Code', 'Value_yr', 'Value_qtr', 'Value_',
        'RGN_TP_CD', 'freq', 'ref']
DF_accruals_tot = add_mapped_tick(DF_accruals_tot, trim_codeMap)[cols]
DF_accrualsS_tot = add_mapped_tick(DF_accrualsS_tot, trim_codeMap)[cols]

DF_accruals_tot['StyleName'] = 'Accruals'
DF_accrualsS_tot['StyleName'] = 'Accruals.S'
#*------------
secondCheck_columns(DF_accruals_tot, DF_accrualsS_tot)
#*------------
DF = DF_accruals_tot.append(DF_accrualsS_tot)

# Save!
save_batch(bkfil, DF, mapping, fileName)

from batch_utils.fill_monitor import print_fillReport
out = print_fillReport(bkfil, freq, DF)
