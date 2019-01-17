# -*- coding: utf-8 -*-

# Python 3.6
# author: EJ Jang
# date: 2018.06.14

# Options - Batch or Backfill (bkfil: False, True)
add_lback_kwarg = dict(add_lback_yr=34, add_lback_qtr=48)
fileName = 'Margin'
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
from batch_utils import get_HistAvg, get_HistChgAvg
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

DF_nm_yr, DF_nm_qtr = pd.DataFrame(), pd.DataFrame()
DF_nm_yr_chg, DF_nm_qtr_chg = pd.DataFrame(), pd.DataFrame()
DF_opm_yr, DF_opm_qtr = pd.DataFrame(), pd.DataFrame()
DF_opm_yr_chg, DF_opm_qtr_chg = pd.DataFrame(), pd.DataFrame()
DF_ebitdam_yr, DF_ebitdam_qtr = pd.DataFrame(), pd.DataFrame()
DF_ebitdam_yr_chg, DF_ebitdam_qtr_chg = pd.DataFrame(), pd.DataFrame()

# Transform Security List into Batch Chunks
tot_n = chunker_count(trim_codeMap_uniq, 50)
Code_lst_tot = chunker(trim_codeMap_uniq.tolist(), 50)

# Bring needed items
ni_item = Item_lst.loc['NetIncome_bExIt']
oi_item = Item_lst.loc['OperatingIncome']
da_item = Item_lst.loc['Dep_n_Amort']
rev_item = Item_lst.loc['NetSales_Revenue']

def trimDateSeq(seq_DT, DF):
    return DF[DF['BASE_DT'].isin(seq_DT)]

st_time = time.time()
for i, Code_lst in enumerate(Code_lst_tot):
    # Net Income
    a_yr, a_qtr = WS_retrieve_custom(
        ni_item['Item'], ni_item['Table'], Code_lst, 'sum', bkfil, **add_lback_kwarg)
    # Operating Income
    a1_yr, a1_qtr = WS_retrieve_custom(
        oi_item['Item'], oi_item['Table'], Code_lst, 'sum', bkfil, **add_lback_kwarg)
    # Dep & Amort
    a2_yr, a2_qtr = WS_retrieve_custom(
        da_item['Item'], da_item['Table'], Code_lst, 'sum', bkfil, **add_lback_kwarg)
    # Revenue
    b_yr, b_qtr = WS_retrieve_custom(
        rev_item['Item'], rev_item['Table'], Code_lst, 'sum', bkfil, **add_lback_kwarg)

    # <<Year>>
    fill_cols = ['FiscalPrd', 'CalPrdEndDate', 'Value_', 'FILLyr']
    a_yr_samp = WS_resample(seq_DT_long, a_yr, fill_cols=fill_cols)
    a1_yr_samp = WS_resample(seq_DT_long, a1_yr, fill_cols=fill_cols)
    a2_yr_samp = WS_resample(seq_DT_long, a2_yr, fill_cols=fill_cols)
    b_yr_samp = WS_resample(seq_DT_long, b_yr, fill_cols=fill_cols)

    # Margin
    nm_yr_samp = align_div(a_yr_samp, b_yr_samp)
    nm_yr_samp['Value_'] = 100 * nm_yr_samp['Value_']

    ebitda_yr_samp = align_add(a1_yr_samp, a2_yr_samp)
    tmp_col = ['BASE_DT', 'Code', 'FiscalPrd', 'Value_']
    ebitdam_yr_samp = align_div(ebitda_yr_samp[tmp_col], b_yr_samp)
    ebitdam_yr_samp['Value_'] = 100 * ebitdam_yr_samp['Value_']

    opm_yr_samp = align_div(a1_yr_samp, b_yr_samp)
    opm_yr_samp['Value_'] = 100 * opm_yr_samp['Value_']

    # Margin Chg
    nm_yr_chg_samp = get_HistChgAvg(nm_yr_samp, 2, 1, buffer=2 / 3)
    opm_yr_chg_samp = get_HistChgAvg(opm_yr_samp, 2, 1, buffer=2 / 3)
    ebitdam_yr_chg_samp = get_HistChgAvg(ebitdam_yr_samp, 2, 1, buffer=2 / 3)
    

    # <<Quarter>>
    fill_cols = ['FiscalPrd', 'FiscalPrd2', 'CalPrdEndDate', 'Value_', 'FILLyr']
    a_qtr_samp = WS_resample(seq_DT_long, a_qtr, fill_cols=fill_cols)
    a1_qtr_samp = WS_resample(seq_DT_long, a1_qtr, fill_cols=fill_cols)
    a2_qtr_samp = WS_resample(seq_DT_long, a2_qtr, fill_cols=fill_cols)
    b_qtr_samp = WS_resample(seq_DT_long, b_qtr, fill_cols=fill_cols)

    # Margin
    nm_qtr_samp = align_div(a_qtr_samp, b_qtr_samp)
    nm_qtr_samp['Value_'] = 100 * nm_qtr_samp['Value_']

    ebitda_qtr_samp = align_add(a1_qtr_samp, a2_qtr_samp)
    tmp_col = ['BASE_DT', 'Code', 'FiscalPrd', 'Value_']
    ebitdam_qtr_samp = align_div(ebitda_qtr_samp[tmp_col], b_qtr_samp)
    ebitdam_qtr_samp['Value_'] = 100 * ebitdam_qtr_samp['Value_']

    opm_qtr_samp = align_div(a1_qtr_samp, b_qtr_samp)
    opm_qtr_samp['Value_'] = 100 * opm_qtr_samp['Value_']

    # Margin Chg
    nm_qtr_chg_samp = get_HistChgAvg(nm_qtr_samp, 5, 4, buffer=2 / 3)
    opm_qtr_chg_samp = get_HistChgAvg(opm_qtr_samp, 5, 4, buffer=2 / 3)
    ebitdam_qtr_chg_samp = get_HistChgAvg(ebitdam_qtr_samp, 5, 4, buffer=2 / 3)

    # <<Trim Date>>
    (
        nm_yr_samp, nm_qtr_samp, nm_yr_chg_samp, nm_qtr_chg_samp,
        opm_yr_samp, opm_qtr_samp, opm_yr_chg_samp, opm_qtr_chg_samp,
        ebitdam_yr_samp, ebitdam_qtr_samp, ebitdam_yr_chg_samp, ebitdam_qtr_chg_samp
    ) = (
        trimDateSeq(seq_DT, DF) for DF in
        [nm_yr_samp, nm_qtr_samp, nm_yr_chg_samp, nm_qtr_chg_samp,
         opm_yr_samp, opm_qtr_samp, opm_yr_chg_samp, opm_qtr_chg_samp,
         ebitdam_yr_samp, ebitdam_qtr_samp, ebitdam_yr_chg_samp, ebitdam_qtr_chg_samp]
    )
    
    # <<Appending>>
    DF_nm_yr = DF_nm_yr.append(nm_yr_samp, sort=False)
    DF_opm_yr = DF_opm_yr.append(opm_yr_samp, sort=False)
    DF_ebitdam_yr = DF_ebitdam_yr.append(ebitdam_yr_samp, sort=False)

    DF_nm_yr_chg = DF_nm_yr_chg.append(nm_yr_chg_samp, sort=False)
    DF_opm_yr_chg = DF_opm_yr_chg.append(opm_yr_chg_samp, sort=False)
    DF_ebitdam_yr_chg = DF_ebitdam_yr_chg.append(ebitdam_yr_chg_samp, sort=False)

    DF_nm_qtr = DF_nm_qtr.append(nm_qtr_samp, sort=False)
    DF_opm_qtr = DF_opm_qtr.append(opm_qtr_samp, sort=False)
    DF_ebitdam_qtr = DF_ebitdam_qtr.append(ebitdam_qtr_samp, sort=False)

    DF_nm_qtr_chg = DF_nm_qtr_chg.append(nm_qtr_chg_samp, sort=False)
    DF_opm_qtr_chg = DF_opm_qtr_chg.append(opm_qtr_chg_samp, sort=False)
    DF_ebitdam_qtr_chg = DF_ebitdam_qtr_chg.append(ebitdam_qtr_chg_samp, sort=False)
    
    batch_monitor_msg(i, tot_n, st_time)
batch_finish_msg(tot_n, st_time)

DF_nm_tot = substitute_Value(DF_nm_yr, DF_nm_qtr)
DF_opm_tot = substitute_Value(DF_opm_yr, DF_opm_qtr)
DF_ebitdam_tot = substitute_Value(DF_ebitdam_yr, DF_ebitdam_qtr)
DF_nm_chg_tot = substitute_Value(DF_nm_yr_chg, DF_nm_qtr_chg)
DF_opm_chg_tot = substitute_Value(DF_opm_yr_chg, DF_opm_qtr_chg)
DF_ebitdam_chg_tot = substitute_Value(DF_ebitdam_yr_chg, DF_ebitdam_qtr_chg)
#*------------
firstCheck_duplicates(DF_nm_tot, DF_opm_tot, DF_ebitdam_tot,
                      DF_nm_chg_tot, DF_opm_chg_tot, DF_ebitdam_chg_tot)
#*------------
# Map Code -> TMSRS_CD
# Should customize columns by needed ones.
# MUST HAVE 'BASE_DT', 'TMSRS_CD', 'Code', 'Value_', 'RGN_TP_CD', 'freq'
cols = ['BASE_DT', 'TMSRS_CD', 'Code', 'Value_yr', 'Value_qtr', 'Value_',
        'RGN_TP_CD', 'freq', 'ref']
DF_nm_tot = add_mapped_tick(DF_nm_tot, trim_codeMap)[cols]
DF_opm_tot = add_mapped_tick(DF_opm_tot, trim_codeMap)[cols]
DF_ebitdam_tot = add_mapped_tick(DF_ebitdam_tot, trim_codeMap)[cols]
DF_nm_chg_tot = add_mapped_tick(DF_nm_chg_tot, trim_codeMap)[cols]
DF_opm_chg_tot = add_mapped_tick(DF_opm_chg_tot, trim_codeMap)[cols]
DF_ebitdam_chg_tot = add_mapped_tick(DF_ebitdam_chg_tot, trim_codeMap)[cols]

DF_nm_tot['StyleName'] = 'NM'
DF_opm_tot['StyleName'] = 'OPM'
DF_ebitdam_tot['StyleName'] = 'EBITDAM'
DF_nm_chg_tot['StyleName'] = 'NM_l2yrAvg_chg'
DF_opm_chg_tot['StyleName'] = 'OPM_l2yrAvg_chg'
DF_ebitdam_chg_tot['StyleName'] = 'EBITDAM_l2yrAvg_chg'
#*------------
secondCheck_columns(DF_nm_tot, DF_opm_tot, DF_ebitdam_tot,
                    DF_nm_chg_tot, DF_opm_chg_tot, DF_ebitdam_chg_tot)
#*------------
DF = reduce(lambda x, y: pd.DataFrame.append(x, y, sort=False),
            [DF_nm_tot, DF_opm_tot, DF_ebitdam_tot,
             DF_nm_chg_tot, DF_opm_chg_tot, DF_ebitdam_chg_tot])

# Save!
save_batch(bkfil, DF, mapping, fileName)

from batch_utils.fill_monitor import print_fillReport
out = print_fillReport(bkfil, freq, DF)