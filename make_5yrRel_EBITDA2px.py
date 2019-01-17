# -*- coding: utf-8 -*-

# Python 3.6
# author: EJ Jang
# date: 2018.06.14

# Options - Batch or Backfill (bkfil: False, True)
add_lback_kwarg = dict(add_lback_yr=70, add_lback_qtr=84)
fileName = '5yrRel_EBITDA2px'
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
from batch_utils.ItemInfo import Item_lst, CurrItem_lst
from batch_utils.common import chunker, chunker_count, add_mapped_tick
from batch_utils.common import firstCheck_duplicates, secondCheck_columns
from batch_utils.common import check_mapping_df, save_batch
from batch_utils.common import batch_monitor_msg, batch_finish_msg

from batch_utils import WS_retrieve_custom, WS_currVal, WS_resample
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

DF_ebitda2p_yr, DF_ebitda2p_qtr = pd.DataFrame(), pd.DataFrame()

# Transform Security List into Batch Chunks
tot_n = chunker_count(trim_codeMap_uniq, 50)
Code_lst_tot = chunker(trim_codeMap_uniq.tolist(), 50)

# Bring needed items
eb_item = Item_lst.loc['OperatingIncome']
da_item = Item_lst.loc['Dep_n_Amort']
mc_item = CurrItem_lst.loc['MkCap_curr']

def trimDateSeq(seq_DT, DF):
    return DF[DF['BASE_DT'].isin(seq_DT)]

st_time = time.time()
for i, Code_lst in enumerate(Code_lst_tot):
    # Operating Income & Dep_n_Amort
    eb_yr, eb_qtr = WS_retrieve_custom(
        eb_item['Item'], eb_item['Table'], Code_lst, 'sum', bkfil, **add_lback_kwarg)
    da_yr, da_qtr = WS_retrieve_custom(
        da_item['Item'], da_item['Table'], Code_lst, 'sum', bkfil, **add_lback_kwarg)
    # Price
    mc_samp = WS_currVal(seq_DT_long, Item=mc_item['Item'], Table=mc_item['Table'],
                         Name=mc_item.name, Code=Code_lst)
    mc_samp = find_n_mod_error(mc_samp)

    # <<Year>>
    fill_cols = ['FiscalPrd', 'CalPrdEndDate', 'Value_', 'FILLyr']
    eb_yr_samp = WS_resample(seq_DT_long, eb_yr, fill_cols=fill_cols)
    da_yr_samp = WS_resample(seq_DT_long, da_yr, fill_cols=fill_cols)
    eba_yr_samp = align_add(eb_yr_samp, da_yr_samp)
    ebitda2p_yr_samp = simple_div(eba_yr_samp, mc_samp)

    ebitda2p_yr_samp = Conv_Historical_Val3(ebitda2p_yr_samp, freq=freq, bkfil=bkfil)    

    # <<Qtr>>
    fill_cols = ['FiscalPrd', 'FiscalPrd2', 'CalPrdEndDate', 'Value_', 'FILLyr']
    eb_qtr_samp = WS_resample(seq_DT_long, eb_qtr, fill_cols=fill_cols)
    da_qtr_samp = WS_resample(seq_DT_long, da_qtr, fill_cols=fill_cols)
    eba_qtr_samp = align_add(eb_qtr_samp, da_qtr_samp)
    ebitda2p_qtr_samp = simple_div(eba_qtr_samp, mc_samp)

    ebitda2p_qtr_samp = Conv_Historical_Val3(ebitda2p_qtr_samp, freq=freq, bkfil=bkfil)

    # <<< Trim Date >>>
    (ebitda2p_yr_samp, ebitda2p_qtr_samp) = (
        trimDateSeq(seq_DT, DF) for DF in
        [ebitda2p_yr_samp, ebitda2p_qtr_samp])

    DF_ebitda2p_qtr = DF_ebitda2p_qtr.append(ebitda2p_qtr_samp, sort=False)
    DF_ebitda2p_yr = DF_ebitda2p_yr.append(ebitda2p_yr_samp, sort=False)
    
    batch_monitor_msg(i, tot_n, st_time)
batch_finish_msg(tot_n, st_time)

DF_ebitda2p_tot = substitute_Value(DF_ebitda2p_yr, DF_ebitda2p_qtr, val_col='zValue_')
#*------------
firstCheck_duplicates(DF_ebitda2p_tot)
#*------------
# Map Code -> TMSRS_CD
# Should customize columns by needed ones.
# MUST HAVE 'BASE_DT', 'TMSRS_CD', 'Code', 'Value_', 'RGN_TP_CD', 'freq'
cols = ['BASE_DT', 'TMSRS_CD', 'Code', 'Value_yr', 'Value_qtr', 'Value_',
        'RGN_TP_CD', 'freq', 'ref']
DF_ebitda2p_tot = add_mapped_tick(DF_ebitda2p_tot, trim_codeMap)[cols]
DF_ebitda2p_tot['StyleName'] = '5YRel_EBITDA2P'
#*------------
secondCheck_columns(DF_ebitda2p_tot)
#*------------
DF = DF_ebitda2p_tot

# Save!
save_batch(bkfil, DF, mapping, fileName)

from batch_utils.fill_monitor import print_fillReport
out = print_fillReport(bkfil, freq, DF)