# -*- coding: utf-8 -*-

# Python 3.6
# author: EJ Jang
# date: 2018.06.14

# Options - Batch or Backfill (bkfil: False, True)
fileName = 'ErnRev'
mapping = 'IBES'
# option = 'backfill'
# freq = 'M'
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
from batch_utils.ItemInfo import IBESItem_lst
from batch_utils.common import chunker, chunker_count, chg_to_type, add_mapped_tick
from batch_utils.common import firstCheck_duplicates, secondCheck_columns, chk_dateFormat
from batch_utils.common import check_mapping_df, save_batch, monthdelta
from batch_utils.common import batch_monitor_msg, batch_finish_msg
from batch_utils import IBES_year_fy1est
from batch_utils import IBES_year2, IBES_year_Ratio2, IBES_resample

# Date Sequence to be made by this batch
bkfil, _, seq_DT = batch_sequence(option, freq, rtvDays=400)
_tmp = dt.datetime.strptime(seq_DT.min(), '%Y%m%d').date()
_ovrd_startDT = dt.date(monthdelta(_tmp, -1).year, monthdelta(_tmp, -1).month, 1)
_ovrd_startDT = _ovrd_startDT.strftime('%Y%m%d')

# Needs to Find Monthly since IBES summary comes in month-freq
_, rtvStart, seq_DT_calc = batch_sequence(
    option, 'M', batch_n=8, rtvDays=400, ovrd_startDT=_ovrd_startDT)
dt_map = pd.DataFrame(
    {'EstDate': seq_DT_calc, 'EstDate_prev': seq_DT_calc.shift(3)}
).dropna()

# Getting the Universe in TMSRS_CD ~ Code Map
allSec = getUnique_TMSRS_CD()
codeMap = get_Mapping_orig(mapping)
trim_codeMap = codeMap[codeMap['TMSRS_CD'].isin(allSec)].copy()

print(trim_codeMap.iloc[:2])
print('\n>>> Total Mapping Securities #: {}'.format(trim_codeMap.shape[0]))

# Checking Level of Duplicates in codeMap
chk_codeMap = check_mapping_df(trim_codeMap)

DF_IBES_EPS = pd.DataFrame()

# <<NA>>
# Transform Security List into Batch Chunks
trim_codeMap_uniq = trim_codeMap.loc[trim_codeMap['RGN_TP_CD'] == '1', 'Code'].unique()
print('>>> Total NA Performing Securities #: {}'.format(trim_codeMap_uniq.shape[0]))
tot_n = chunker_count(trim_codeMap_uniq, 50)
Code_lst_tot = chunker(trim_codeMap_uniq.tolist(), 50)

st_time = time.time()
for i, Code_lst in enumerate(Code_lst_tot):

    var_Name = IBESItem_lst.loc['EPS_US']['var_Name']
    DF_EPS_ = IBES_year_fy1est(Code=Code_lst, St_Dt=rtvStart,
                               **dict(IBESItem_lst.loc['EPS_US']))
    DF_EPS_['RGN_TP_CD'] = 1

    DF_tmp = DF_EPS_.drop(['Measure', 'PerType', 'Mean1', 'Mean2',
                           'Mean3', 'PxDate0', 'Sh0'], axis=1)
    cols = ['maxUseDate', 'Px0', 'FY0', 'ActValue0', 'FY1', 'Median1', 'FY2', 'Median2',
            'FY3', 'Median3', 'a0', 'a1', 'a1_', 'a2', 'EPS_fy1']
    DF_raw_samp = IBES_resample(seq_DT_calc, DF_tmp, fill_cols=cols, drop_maxCol=False,
                                base_Col='EstDate')

    DF_raw_samp_ = pd.merge(DF_raw_samp, dt_map, on='EstDate', how='inner')
    DF_prev = DF_raw_samp[
        ['RGN_TP_CD', 'Code', 'EstDate', 'Median1', 'Median2', 'Median3']
        ].rename(
        columns={'EstDate': 'EstDate_prev', 'Median1': 'Med1_prev',
                 'Median2': 'Med2_prev', 'Median3': 'Med3_prev'})
    DF = pd.merge(DF_raw_samp_, DF_prev,
                  on=['RGN_TP_CD', 'Code', 'EstDate_prev'], how='inner')

    est_cols = ['Med1_prev', 'Med2_prev', 'Med3_prev']
    wgt_cols = ['a0', 'a1', 'a2']

    est = DF[est_cols].values
    wgt = DF[wgt_cols].values
    out = np.nansum(est * wgt, axis=1)
    cnt = np.isnan(DF[est_cols].values)

    loc = cnt.sum(1) > 1
    out[loc] = np.nan

    loc = cnt.T[-1] & (wgt.T[-1] < 0.15) & (cnt.sum(1) <= 1)
    out[loc] = out[loc] / wgt.T[:-1].sum(0)[loc]

    DF['EPS_fy1_prev'] = out
    DF['EPS_chg_pct'] = 100 * ((DF['EPS_fy1'] - DF['EPS_fy1_prev']) / 
                               DF['EPS_fy1_prev'].abs())
    DF.loc[DF['EPS_fy1_prev'] == 0, 'EPS_chg_pct'] = 0
    DF.loc[DF['EPS_chg_pct'].abs() > 200, 'EPS_chg_pct'] = DF.loc[
        DF['EPS_chg_pct'].abs() > 200, 'EPS_chg_pct'].apply(np.sign) * 200
    
    DF.drop(['Med1_prev', 'Med2_prev', 'Med3_prev'], axis=1, inplace=True)
    DF = IBES_resample(
        seq_DT, DF, fill_cols=cols + ['EstDate_prev', 'EPS_fy1_prev', 'EPS_chg_pct'],
        base_Col='EstDate')

    DF_IBES_EPS = DF_IBES_EPS.append(DF)
    
    batch_monitor_msg(i, tot_n, st_time)
batch_finish_msg(tot_n, st_time)

# <<Global>>
# Transform Security List into Batch Chunks
trim_codeMap_uniq = trim_codeMap.loc[trim_codeMap['RGN_TP_CD'] == '3', 'Code'].unique()
print('>>> Total Global Performing Securities #: {}'.format(trim_codeMap_uniq.shape[0]))
tot_n = chunker_count(trim_codeMap_uniq, 50)
Code_lst_tot = chunker(trim_codeMap_uniq.tolist(), 50)

st_time = time.time()
for i, Code_lst in enumerate(Code_lst_tot):

    var_Name = IBESItem_lst.loc['EPS_exUS']['var_Name']
    DF_EPS_ = IBES_year_fy1est(Code=Code_lst, St_Dt=rtvStart,
                               **dict(IBESItem_lst.loc['EPS_exUS']))
    DF_EPS_['RGN_TP_CD'] = 3

    DF_tmp = DF_EPS_.drop(['Measure', 'PerType', 'Mean1', 'Mean2',
                           'Mean3', 'PxDate0', 'Sh0'], axis=1)
    cols = ['maxUseDate', 'Px0', 'FY0', 'ActValue0', 'FY1', 'Median1', 'FY2', 'Median2',
            'FY3', 'Median3', 'a0', 'a1', 'a1_', 'a2', 'EPS_fy1']
    DF_raw_samp = IBES_resample(seq_DT_calc, DF_tmp, fill_cols=cols, drop_maxCol=False,
                                base_Col='EstDate')

    DF_raw_samp_ = pd.merge(DF_raw_samp, dt_map, on='EstDate', how='inner')
    DF_prev = DF_raw_samp[
        ['RGN_TP_CD', 'Code', 'EstDate', 'Median1', 'Median2', 'Median3']
        ].rename(
        columns={'EstDate': 'EstDate_prev', 'Median1': 'Med1_prev',
                 'Median2': 'Med2_prev', 'Median3': 'Med3_prev'})
    DF = pd.merge(DF_raw_samp_, DF_prev,
                  on=['RGN_TP_CD', 'Code', 'EstDate_prev'], how='inner')

    est_cols = ['Med1_prev', 'Med2_prev', 'Med3_prev']
    wgt_cols = ['a0', 'a1', 'a2']

    est = DF[est_cols].values
    wgt = DF[wgt_cols].values
    out = np.nansum(est * wgt, axis=1)
    cnt = np.isnan(DF[est_cols].values)

    loc = cnt.sum(1) > 1
    out[loc] = np.nan

    loc = cnt.T[-1] & (wgt.T[-1] < 0.15) & (cnt.sum(1) <= 1)
    out[loc] = out[loc] / wgt.T[:-1].sum(0)[loc]

    DF['EPS_fy1_prev'] = out
    DF['EPS_chg_pct'] = 100 * ((DF['EPS_fy1'] - DF['EPS_fy1_prev']) / 
                               DF['EPS_fy1_prev'].abs())
    DF.loc[DF['EPS_fy1_prev'] == 0, 'EPS_chg_pct'] = 0
    DF.loc[DF['EPS_chg_pct'].abs() > 200, 'EPS_chg_pct'] = DF.loc[
        DF['EPS_chg_pct'].abs() > 200, 'EPS_chg_pct'].apply(np.sign) * 200
    
    DF.drop(['Med1_prev', 'Med2_prev', 'Med3_prev'], axis=1, inplace=True)
    DF = IBES_resample(
        seq_DT, DF, fill_cols=cols + ['EstDate_prev', 'EPS_fy1_prev', 'EPS_chg_pct'],
        base_Col='EstDate')

    DF_IBES_EPS = DF_IBES_EPS.append(DF)
    
    batch_monitor_msg(i, tot_n, st_time)
batch_finish_msg(tot_n, st_time)

DF_IBES_EPS.rename(columns={'EstDate': 'BASE_DT'}, inplace=True)
DF_IBES_EPS = DF_IBES_EPS.sort_values(
    ['Code', 'RGN_TP_CD', 'BASE_DT']).reset_index(drop=True)

#*------------
firstCheck_duplicates(DF_IBES_EPS, add_cols=['RGN_TP_CD'])
#*------------

DF_IBES_EPS = chg_to_type(DF_IBES_EPS, chg_col=['Code', 'BASE_DT', 'RGN_TP_CD'])

DF_IBES_EPS = add_mapped_tick(
    DF_IBES_EPS, trim_codeMap, on=['Code', 'RGN_TP_CD'])

cols = ['BASE_DT', 'TMSRS_CD', 'Code', 'RGN_TP_CD', 'FY0', 'Px0',
        'ActValue0', 'EPS_fy1', 'EPS_fy1_prev', 'EPS_chg_pct']
DF_IBES_EPS.dropna(subset=['EPS_chg_pct'], axis=0, inplace=True)
DF_IBES_EPS = DF_IBES_EPS[cols]

cols = ['BASE_DT', 'TMSRS_CD', 'Code', 'RGN_TP_CD',
        'EPS_fy1', 'EPS_fy1_prev', 'EPS_chg_pct']
DF_final = DF_IBES_EPS[cols].copy()
DF_final['freq'], DF_final['ref'] = 'M', None
DF_final.rename(columns={'EPS_chg_pct': 'Value_'}, inplace=True)
DF_final['StyleName'] = 'EPSRev3M'

#*------------
secondCheck_columns(DF_final)
#*------------

save_batch(bkfil, DF_final, mapping, fileName + '3M')

from batch_utils.fill_monitor import print_fillReport
out = print_fillReport(bkfil, freq, DF_final)