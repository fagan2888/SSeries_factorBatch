# -*- coding: utf-8 -*-

# Python 3.6
# author: EJ Jang
# date: 2018.06.14

# Options - Batch or Backfill (bkfil: False, True)
add_lback_kwarg = dict(add_lback_yr=10, add_lback_qtr=24)
fileName = 'EVEbitda'
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

DF_evebitda_yr, DF_evebitda_qtr = pd.DataFrame(), pd.DataFrame()
DF_cfoev_yr, DF_cfoev_qtr = pd.DataFrame(), pd.DataFrame()
DF_fcfev_yr, DF_fcfev_qtr = pd.DataFrame(), pd.DataFrame()

# Transform Security List into Batch Chunks
tot_n = chunker_count(trim_codeMap_uniq, 50)
Code_lst_tot = chunker(trim_codeMap_uniq.tolist(), 50)

# Bring needed items
ev_item = Item_lst.loc['EnterpriseValue']
eb_item = Item_lst.loc['OperatingIncome']
da_item = Item_lst.loc['Dep_n_Amort']
cf_item = Item_lst.loc['NetCF_Operating']
cx_item = Item_lst.loc['CapEx']

st_time = time.time()
for i, Code_lst in enumerate(Code_lst_tot):
    # Operating CashFlow
    cf_yr, cf_qtr = WS_retrieve_custom(
        cf_item['Item'], cf_item['Table'], Code_lst, 'sum', bkfil, **add_lback_kwarg)
    # Enterprise Value
    ev_yr, ev_qtr = WS_retrieve_custom(
        ev_item['Item'], ev_item['Table'], Code_lst, 'avg', bkfil, **add_lback_kwarg)
    # Operating Income
    eb_yr, eb_qtr = WS_retrieve_custom(
        eb_item['Item'], eb_item['Table'], Code_lst, 'sum', bkfil, **add_lback_kwarg)
    # Depreciation & Amortization
    da_yr, da_qtr = WS_retrieve_custom(
        da_item['Item'], da_item['Table'], Code_lst, 'sum', bkfil, **add_lback_kwarg)
    # Capex
    cx_yr, cx_qtr = WS_retrieve_custom(
        cx_item['Item'], cx_item['Table'], Code_lst, 'sum', bkfil, **add_lback_kwarg)

    # Year
    fill_cols = ['FiscalPrd', 'CalPrdEndDate', 'Value_', 'FILLyr']
    cf_yr_samp = WS_resample(seq_DT, cf_yr, fill_cols=fill_cols)
    ev_yr_samp = WS_resample(seq_DT, ev_yr, fill_cols=fill_cols)
    eb_yr_samp = WS_resample(seq_DT, eb_yr, fill_cols=fill_cols)
    da_yr_samp = WS_resample(seq_DT, da_yr, fill_cols=fill_cols)
    cx_yr_samp = WS_resample(seq_DT, cx_yr, fill_cols=fill_cols)

    eba_yr_samp = align_add(eb_yr_samp, da_yr_samp)
    fcf_yr_samp = align_subtract(cf_yr_samp, cx_yr_samp)

    evebitda_yr_samp = align_div(ev_yr_samp, eba_yr_samp)
    cfoev_yr_samp = align_div(cf_yr_samp, ev_yr_samp)
    fcfev_yr_samp = align_div(fcf_yr_samp, ev_yr_samp)

    DF_evebitda_yr = DF_evebitda_yr.append(evebitda_yr_samp, sort=False)
    DF_cfoev_yr = DF_cfoev_yr.append(cfoev_yr_samp, sort=False)
    DF_fcfev_yr = DF_fcfev_yr.append(fcfev_yr_samp, sort=False)
    

    # Quarter
    fill_cols = ['FiscalPrd', 'FiscalPrd2', 'CalPrdEndDate', 'Value_', 'FILLyr']
    cf_qtr_samp = WS_resample(seq_DT, cf_qtr, fill_cols=fill_cols)
    ev_qtr_samp = WS_resample(seq_DT, ev_qtr, fill_cols=fill_cols)
    eb_qtr_samp = WS_resample(seq_DT, eb_qtr, fill_cols=fill_cols)
    da_qtr_samp = WS_resample(seq_DT, da_qtr, fill_cols=fill_cols)
    cx_qtr_samp = WS_resample(seq_DT, cx_qtr, fill_cols=fill_cols)

    eba_qtr_samp = align_add(eb_qtr_samp, da_qtr_samp)
    fcf_qtr_samp = align_subtract(cf_qtr_samp, cx_qtr_samp)

    evebitda_qtr_samp = align_div(ev_qtr_samp, eba_qtr_samp)
    cfoev_qtr_samp = align_div(cf_qtr_samp, ev_qtr_samp)
    fcfev_qtr_samp = align_div(fcf_qtr_samp, ev_qtr_samp)

    DF_evebitda_qtr = DF_evebitda_qtr.append(evebitda_qtr_samp, sort=False)
    DF_cfoev_qtr = DF_cfoev_qtr.append(cfoev_qtr_samp, sort=False)
    DF_fcfev_qtr = DF_fcfev_qtr.append(fcfev_qtr_samp, sort=False)
    
    batch_monitor_msg(i, tot_n, st_time)
batch_finish_msg(tot_n, st_time)

DF_evebitda_tot = substitute_Value(DF_evebitda_yr, DF_evebitda_qtr)
DF_cfoev_tot = substitute_Value(DF_cfoev_yr, DF_cfoev_qtr)
DF_fcfev_tot = substitute_Value(DF_fcfev_yr, DF_fcfev_qtr)
#*------------
firstCheck_duplicates(DF_evebitda_tot, DF_cfoev_tot, DF_fcfev_tot)
#*------------
# Map Code -> TMSRS_CD
# Should customize columns by needed ones.
# MUST HAVE 'BASE_DT', 'TMSRS_CD', 'Code', 'Value_', 'RGN_TP_CD', 'freq'
cols = ['BASE_DT', 'TMSRS_CD', 'Code', 'Value_yr', 'Value_qtr', 'Value_',
        'RGN_TP_CD', 'freq', 'ref']
DF_evebitda_tot = add_mapped_tick(DF_evebitda_tot, trim_codeMap)[cols]
DF_cfoev_tot = add_mapped_tick(DF_cfoev_tot, trim_codeMap)[cols]
DF_fcfev_tot = add_mapped_tick(DF_fcfev_tot, trim_codeMap)[cols]
DF_evebitda_tot['StyleName'] = 'EV2EBITDA'
DF_cfoev_tot['StyleName'] = 'CFO2EV'
DF_fcfev_tot['StyleName'] = 'FCF2EV'
#*------------
secondCheck_columns(DF_evebitda_tot, DF_cfoev_tot, DF_fcfev_tot)
#*------------
DF = DF_evebitda_tot.append(DF_cfoev_tot, sort=False).append(
    DF_fcfev_tot, sort=False)

# Save!
save_batch(bkfil, DF, mapping, fileName)

from batch_utils.fill_monitor import print_fillReport
out = print_fillReport(bkfil, freq, DF)