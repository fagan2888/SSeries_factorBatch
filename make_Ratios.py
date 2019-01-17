# -*- coding: utf-8 -*-

# Python 3.6
# author: EJ Jang
# date: 2018.06.14

# Options - Batch or Backfill (bkfil: False, True)
add_lback_kwarg = dict(add_lback_yr=10, add_lback_qtr=24)
fileName = 'Ratios'
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
from batch_utils.ItemInfo import RatioItem_lst
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

DF_ROE_qtr, DF_ROE_yr = pd.DataFrame(), pd.DataFrame()
DF_ROA_qtr, DF_ROA_yr = pd.DataFrame(), pd.DataFrame()
DF_OPM_qtr, DF_OPM_yr = pd.DataFrame(), pd.DataFrame()
DF_NM_qtr, DF_NM_yr = pd.DataFrame(), pd.DataFrame()
DF_GPM_qtr, DF_GPM_yr = pd.DataFrame(), pd.DataFrame()
DF_RevGr_qtr, DF_RevGr_yr = pd.DataFrame(), pd.DataFrame()
DF_NIGr_qtr, DF_NIGr_yr = pd.DataFrame(), pd.DataFrame()
DF_DtE_qtr, DF_DtE_yr = pd.DataFrame(), pd.DataFrame()

# Transform Security List into Batch Chunks
tot_n = chunker_count(trim_codeMap_uniq, 50)
Code_lst_tot = chunker(trim_codeMap_uniq.tolist(), 50)

# Bring needed items
re_item = RatioItem_lst.loc['ROE_ws']
ra_item = RatioItem_lst.loc['ROA_ws']
om_item = RatioItem_lst.loc['OPM_ws']
nm_item = RatioItem_lst.loc['NM_ws']
gm_item = RatioItem_lst.loc['GPM_ws']
rv1_item = RatioItem_lst.loc['REVg_l1yr_ws']
ni1_item = RatioItem_lst.loc['NIg_l1yr_ws']
de_item = RatioItem_lst.loc['LTDtCE_ws']

st_time = time.time()
for i, Code_lst in enumerate(Code_lst_tot):
    # ROE
    roe_yr, roe_qtr = WS_retrieve_custom(
        re_item['Item'], re_item['Table'], Code_lst, None, bkfil, **add_lback_kwarg)
    # ROA
    roa_yr, roa_qtr = WS_retrieve_custom(
        ra_item['Item'], ra_item['Table'], Code_lst, None, bkfil, **add_lback_kwarg)
    # OPM
    opm_yr, opm_qtr = WS_retrieve_custom(
        om_item['Item'], om_item['Table'], Code_lst, None, bkfil, **add_lback_kwarg)
    # NM
    nm_yr, nm_qtr = WS_retrieve_custom(
        nm_item['Item'], nm_item['Table'], Code_lst, None, bkfil, **add_lback_kwarg)
    # GPM
    gpm_yr, gpm_qtr = WS_retrieve_custom(
        gm_item['Item'], gm_item['Table'], Code_lst, None, bkfil, **add_lback_kwarg)
    # REVg_l1yr_ws
    REVgr_yr, REVgr_qtr = WS_retrieve_custom(
        rv1_item['Item'], rv1_item['Table'], Code_lst, None, bkfil, **add_lback_kwarg)
    # NIg_l1yr_ws
    NIgr_yr, NIgr_qtr = WS_retrieve_custom(
        ni1_item['Item'], ni1_item['Table'], Code_lst, None, bkfil, **add_lback_kwarg)
    # LTDtCE_ws
    DtE_yr, DtE_qtr = WS_retrieve_custom(
        de_item['Item'], de_item['Table'], Code_lst, None, bkfil, **add_lback_kwarg)

    # Year
    fill_cols = ['FiscalPrd', 'CalPrdEndDate', 'Value_', 'FILLyr']
    roe_yr_samp = WS_resample(seq_DT, roe_yr, fill_cols=fill_cols)
    roa_yr_samp = WS_resample(seq_DT, roa_yr, fill_cols=fill_cols)
    opm_yr_samp = WS_resample(seq_DT, opm_yr, fill_cols=fill_cols)
    nm_yr_samp = WS_resample(seq_DT, nm_yr, fill_cols=fill_cols)
    gpm_yr_samp = WS_resample(seq_DT, gpm_yr, fill_cols=fill_cols)
    REVgr_yr_samp = WS_resample(seq_DT, REVgr_yr, fill_cols=fill_cols)
    NIgr_yr_samp = WS_resample(seq_DT, NIgr_yr, fill_cols=fill_cols)
    DtE_yr_samp = WS_resample(seq_DT, DtE_yr, fill_cols=fill_cols)

    DF_ROE_yr = DF_ROE_yr.append(roe_yr_samp, sort=False)
    DF_ROA_yr = DF_ROA_yr.append(roa_yr_samp, sort=False)
    DF_OPM_yr = DF_OPM_yr.append(opm_yr_samp, sort=False)
    DF_NM_yr = DF_NM_yr.append(nm_yr_samp, sort=False)
    DF_GPM_yr = DF_GPM_yr.append(gpm_yr_samp, sort=False)
    DF_RevGr_yr = DF_RevGr_yr.append(REVgr_yr_samp, sort=False)
    DF_NIGr_yr = DF_NIGr_yr.append(NIgr_yr_samp, sort=False)
    DF_DtE_yr = DF_DtE_yr.append(DtE_yr_samp, sort=False)
    

    # Quarter
    fill_cols = ['FiscalPrd', 'FiscalPrd2', 'CalPrdEndDate', 'Value_', 'FILLyr']
    roe_qtr_samp = WS_resample(seq_DT, roe_qtr, fill_cols=fill_cols)
    roa_qtr_samp = WS_resample(seq_DT, roa_qtr, fill_cols=fill_cols)
    opm_qtr_samp = WS_resample(seq_DT, opm_qtr, fill_cols=fill_cols)
    nm_qtr_samp = WS_resample(seq_DT, nm_qtr, fill_cols=fill_cols)
    gpm_qtr_samp = WS_resample(seq_DT, gpm_qtr, fill_cols=fill_cols)
    REVgr_qtr_samp = WS_resample(seq_DT, REVgr_qtr, fill_cols=fill_cols)
    NIgr_qtr_samp = WS_resample(seq_DT, NIgr_qtr, fill_cols=fill_cols)
    DtE_qtr_samp = WS_resample(seq_DT, DtE_qtr, fill_cols=fill_cols)

    DF_ROE_qtr = DF_ROE_qtr.append(roe_qtr_samp, sort=False)
    DF_ROA_qtr = DF_ROA_qtr.append(roa_qtr_samp, sort=False)
    DF_OPM_qtr = DF_OPM_qtr.append(opm_qtr_samp, sort=False)
    DF_NM_qtr = DF_NM_qtr.append(nm_qtr_samp, sort=False)
    DF_GPM_qtr = DF_GPM_qtr.append(gpm_qtr_samp, sort=False)
    DF_RevGr_qtr = DF_RevGr_qtr.append(REVgr_qtr_samp, sort=False)
    DF_NIGr_qtr = DF_NIGr_qtr.append(NIgr_qtr_samp, sort=False)
    DF_DtE_qtr = DF_DtE_qtr.append(DtE_qtr_samp, sort=False)
    
    batch_monitor_msg(i, tot_n, st_time)
batch_finish_msg(tot_n, st_time)

DF_ROE_tot = substitute_Value(DF_ROE_yr, DF_ROE_qtr)
DF_ROA_tot = substitute_Value(DF_ROA_yr, DF_ROA_qtr)
DF_OPM_tot = substitute_Value(DF_OPM_yr, DF_OPM_qtr)
DF_NM_tot = substitute_Value(DF_NM_yr, DF_NM_qtr)
DF_GPM_tot = substitute_Value(DF_GPM_yr, DF_GPM_qtr)
DF_RevGr_tot = substitute_Value(DF_RevGr_yr, DF_RevGr_qtr)
DF_NIGr_tot = substitute_Value(DF_NIGr_yr, DF_NIGr_qtr)
DF_DtE_tot = substitute_Value(DF_DtE_yr, DF_DtE_qtr)
#*------------
firstCheck_duplicates(DF_ROE_tot, DF_ROA_tot, DF_OPM_tot, DF_NM_tot,
                      DF_GPM_tot, DF_RevGr_tot, DF_NIGr_tot, DF_DtE_tot)
#*------------
# Map Code -> TMSRS_CD
# Should customize columns by needed ones.
# MUST HAVE 'BASE_DT', 'TMSRS_CD', 'Code', 'Value_', 'RGN_TP_CD', 'freq'
cols = ['BASE_DT', 'TMSRS_CD', 'Code', 'Value_yr', 'Value_qtr', 'Value_',
        'RGN_TP_CD', 'freq', 'ref']
DF_ROE_tot = add_mapped_tick(DF_ROE_tot, trim_codeMap)[cols]
DF_ROA_tot = add_mapped_tick(DF_ROA_tot, trim_codeMap)[cols]
DF_OPM_tot = add_mapped_tick(DF_OPM_tot, trim_codeMap)[cols]
DF_NM_tot = add_mapped_tick(DF_NM_tot, trim_codeMap)[cols]
DF_GPM_tot = add_mapped_tick(DF_GPM_tot, trim_codeMap)[cols]
DF_RevGr_tot = add_mapped_tick(DF_RevGr_tot, trim_codeMap)[cols]
DF_NIGr_tot = add_mapped_tick(DF_NIGr_tot, trim_codeMap)[cols]
DF_DtE_tot = add_mapped_tick(DF_DtE_tot, trim_codeMap)[cols]

DF_ROE_tot['StyleName'] = RatioItem_lst.loc['ROE_ws'].name
DF_ROA_tot['StyleName'] = RatioItem_lst.loc['ROA_ws'].name
DF_OPM_tot['StyleName'] = RatioItem_lst.loc['OPM_ws'].name
DF_NM_tot['StyleName'] = RatioItem_lst.loc['NM_ws'].name
DF_GPM_tot['StyleName'] = RatioItem_lst.loc['GPM_ws'].name
DF_RevGr_tot['StyleName'] = RatioItem_lst.loc['REVg_l1yr_ws'].name
DF_NIGr_tot['StyleName'] = RatioItem_lst.loc['NIg_l1yr_ws'].name
DF_DtE_tot['StyleName'] = RatioItem_lst.loc['LTDtCE_ws'].name
#*------------
secondCheck_columns(DF_ROE_tot, DF_ROA_tot, DF_OPM_tot, DF_NM_tot,
                    DF_GPM_tot, DF_RevGr_tot, DF_NIGr_tot, DF_DtE_tot)
#*------------
DF = reduce(lambda x, y: pd.DataFrame.append(x, y, sort=False),
            [DF_ROE_tot, DF_ROA_tot, DF_OPM_tot, DF_NM_tot,
             DF_GPM_tot, DF_RevGr_tot, DF_NIGr_tot, DF_DtE_tot])
DF.reset_index(drop=True, inplace=True)

# Save!
save_batch(bkfil, DF, mapping, fileName)

from batch_utils.fill_monitor import print_fillReport
out = print_fillReport(bkfil, freq, DF)