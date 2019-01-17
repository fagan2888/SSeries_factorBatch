# -*- coding: utf-8 -*-

# Python 3.6
# author: EJ Jang
# date: 2018.06.14

# Options - Batch or Backfill (bkfil: False, True)
fileName = 'refIBES_EDP'
mapping = 'IBES'
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
from batch_utils.ItemInfo import IBESItem_lst
from batch_utils.common import chunker, chunker_count, chg_to_type, add_mapped_tick
from batch_utils.common import firstCheck_duplicates, secondCheck_columns
from batch_utils.common import check_mapping_df, save_batch
from batch_utils.common import batch_monitor_msg, batch_finish_msg
from batch_utils import IBES_year, IBES_year_Ratio, IBES_resample

# Date Sequence to be made by this batch
bkfil, rtvStart, seq_DT = batch_sequence(option, freq, rtvDays=60)

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
    DF_EPS_ = IBES_year(Code=Code_lst, St_Dt=rtvStart,
                        **dict(IBESItem_lst.loc['EPS_US']))
    DF_EPS_['RGN_TP_CD'] = 1

    DF_tmp = DF_EPS_.drop(['Measure', 'PerType', 'Mean1', 'Mean2',
                           'Mean3', 'PxDate0', 'Sh0'], axis=1)
    cols = ['Px0', 'FY0', 'ActValue0', 'FY1', 'Median1', 'FY2', 'Median2',
            'FY3', 'Median3', 'EPS_fy0', 'EPS_fy1', 'EPS_fy2', 'EPSg_2yr',
            'EPSchg_2yr', 'Ratio_f12m']
    DF_raw_samp = IBES_resample(seq_DT, DF_tmp, fill_cols=cols, base_Col='EstDate')
    DF_raw_samp.rename(columns={'Ratio_f12m': 'PE_f12m', 'EstDate': 'BASE_DT'},
                       inplace=True)
    DF_IBES_EPS = DF_IBES_EPS.append(DF_raw_samp)

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
    DF_EPS_ = IBES_year(Code=Code_lst, St_Dt=rtvStart,
                        **dict(IBESItem_lst.loc['EPS_exUS']))
    DF_EPS_['RGN_TP_CD'] = 3

    DF_tmp = DF_EPS_.drop(['Measure', 'PerType', 'Mean1', 'Mean2',
                           'Mean3', 'PxDate0', 'Sh0'], axis=1)
    cols = ['Px0', 'FY0', 'ActValue0', 'FY1', 'Median1', 'FY2', 'Median2',
            'FY3', 'Median3', 'EPS_fy0', 'EPS_fy1', 'EPS_fy2', 'EPSg_2yr',
            'EPSchg_2yr', 'Ratio_f12m']
    DF_raw_samp = IBES_resample(seq_DT, DF_tmp, fill_cols=cols, base_Col='EstDate')
    DF_raw_samp.rename(columns={'Ratio_f12m': 'PE_f12m', 'EstDate': 'BASE_DT'},
                       inplace=True)
    DF_IBES_EPS = DF_IBES_EPS.append(DF_raw_samp)

    batch_monitor_msg(i, tot_n, st_time)
batch_finish_msg(tot_n, st_time)

DF_IBES_EPS.sort_values(['RGN_TP_CD', 'Code', 'BASE_DT'], inplace=True)
DF_IBES_EPS.reset_index(drop=True, inplace=True)
DF_IBES_EPS = chg_to_type(
    DF_IBES_EPS, chg_col=['BASE_DT', 'Code', 'RGN_TP_CD'], type_=str)

#*------------
firstCheck_duplicates(DF_IBES_EPS, add_cols=['RGN_TP_CD'])
#*------------

DF_IBES_EPS = add_mapped_tick(
    DF_IBES_EPS, trim_codeMap, on=['Code', 'RGN_TP_CD'])

cols = ['BASE_DT', 'TMSRS_CD', 'Code', 'RGN_TP_CD', 'FY0', 'Px0',
        'ActValue0', 'FY1', 'Median1', 'FY2', 'Median2',
        'FY3', 'Median3', 'EPS_fy0', 'EPS_fy1', 'EPS_fy2',
        'EPSg_2yr', 'EPSchg_2yr', 'PE_f12m']
DF_IBES_EPS = DF_IBES_EPS.loc[DF_IBES_EPS['Median1'].notnull(), cols]
DF_IBES_EPS['StyleName'] = 'IBES_EPS'

DF_IBES_EPS.sort_values(['TMSRS_CD', 'RGN_TP_CD', 'BASE_DT'], inplace=True)
DF_IBES_EPS.drop_duplicates(['TMSRS_CD', 'BASE_DT'], keep='first', inplace=True)

DF_IBES_DPS = pd.DataFrame()

# <<NA>>
# Transform Security List into Batch Chunks
trim_codeMap_uniq = trim_codeMap.loc[trim_codeMap['RGN_TP_CD'] == '1', 'Code'].unique()
print('>>> Total NA Performing Securities #: {}'.format(trim_codeMap_uniq.shape[0]))
tot_n = chunker_count(trim_codeMap_uniq, 50)
Code_lst_tot = chunker(trim_codeMap_uniq.tolist(), 50)

st_time = time.time()
for i, Code_lst in enumerate(Code_lst_tot):
    var_Name = IBESItem_lst.loc['DPS_US']['var_Name']
    DF_DPS_ = IBES_year(Code=Code_lst, St_Dt=rtvStart,
                                 **dict(IBESItem_lst.loc['DPS_US']))
    DF_DPS_['RGN_TP_CD'] = 1

    DF_tmp = DF_DPS_.drop(['Measure', 'PerType', 'Mean1', 'Mean2',
                           'Mean3', 'PxDate0', 'Sh0'], axis=1)
    cols = ['Px0', 'FY0', 'ActValue0', 'FY1', 'Median1', 'FY2', 'Median2',
            'FY3', 'Median3', 'DPS_fy0', 'DPS_fy1', 'DPS_fy2', 'DPSg_2yr',
            'DPSchg_2yr', 'Ratio_f12m']
    DF_raw_samp = IBES_resample(seq_DT, DF_tmp, fill_cols=cols, base_Col='EstDate')
    DF_raw_samp['Ratio_f12m'] = 100 / DF_raw_samp['Ratio_f12m']
    DF_raw_samp['Ratio_f12m'] = DF_raw_samp.apply(
        lambda x: 0 if (x['DPS_fy1'] == 0) & pd.isnull(x['Ratio_f12m'])
        else x['Ratio_f12m'], axis=1)
    DF_raw_samp.rename(columns={'Ratio_f12m': 'DY_f12m', 'EstDate': 'BASE_DT'},
                       inplace=True)
    DF_IBES_DPS = DF_IBES_DPS.append(DF_raw_samp)

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
    var_Name = IBESItem_lst.loc['DPS_exUS']['var_Name']
    DF_DPS_ = IBES_year(Code=Code_lst, St_Dt=rtvStart,
                        **dict(IBESItem_lst.loc['DPS_exUS']))
    DF_DPS_['RGN_TP_CD'] = 3

    DF_tmp = DF_DPS_.drop(['Measure', 'PerType', 'Mean1', 'Mean2',
                           'Mean3', 'PxDate0', 'Sh0'], axis=1)
    cols = ['Px0', 'FY0', 'ActValue0', 'FY1', 'Median1', 'FY2', 'Median2',
            'FY3', 'Median3', 'DPS_fy0', 'DPS_fy1', 'DPS_fy2', 'DPSg_2yr',
            'DPSchg_2yr', 'Ratio_f12m']
    DF_raw_samp = IBES_resample(seq_DT, DF_tmp, fill_cols=cols, base_Col='EstDate')
    DF_raw_samp['Ratio_f12m'] = 100 / DF_raw_samp['Ratio_f12m']
    DF_raw_samp['Ratio_f12m'] = DF_raw_samp.apply(
        lambda x: 0 if (x['DPS_fy1'] == 0) & pd.isnull(x['Ratio_f12m'])
        else x['Ratio_f12m'], axis=1)
    DF_raw_samp.rename(columns={'Ratio_f12m': 'DY_f12m', 'EstDate': 'BASE_DT'},
                       inplace=True)
    DF_IBES_DPS = DF_IBES_DPS.append(DF_raw_samp)

    batch_monitor_msg(i, tot_n, st_time)
batch_finish_msg(tot_n, st_time)

DF_IBES_DPS.sort_values(['RGN_TP_CD', 'Code', 'BASE_DT'], inplace=True)
DF_IBES_DPS.reset_index(drop=True, inplace=True)

DF_IBES_DPS = chg_to_type(DF_IBES_DPS, chg_col=['BASE_DT', 'Code', 'RGN_TP_CD'])

#*------------
firstCheck_duplicates(DF_IBES_DPS, add_cols=['RGN_TP_CD'])
#*------------

DF_IBES_DPS = add_mapped_tick(
    DF_IBES_DPS, trim_codeMap, on=['Code', 'RGN_TP_CD'])

cols = ['BASE_DT', 'TMSRS_CD', 'Code', 'RGN_TP_CD', 'FY0', 'Px0',
        'ActValue0', 'FY1', 'Median1', 'FY2', 'Median2',
        'FY3', 'Median3', 'DPS_fy0', 'DPS_fy1', 'DPS_fy2',
        'DPSg_2yr', 'DPSchg_2yr', 'DY_f12m']
DF_IBES_DPS = DF_IBES_DPS.loc[DF_IBES_DPS['Median1'].notnull(), cols]
DF_IBES_DPS['StyleName'] = 'IBES_DPS'

DF_IBES_DPS.sort_values(['TMSRS_CD', 'RGN_TP_CD', 'BASE_DT'], inplace=True)
DF_IBES_DPS.drop_duplicates(['TMSRS_CD', 'BASE_DT'], keep='first', inplace=True)

# CREATE FY0, 1, 2yr Payout-------
def my_merge(DF0, DF1, col0, col1):
    DF0_ = DF0.loc[DF0[col0].notnull(), ['BASE_DT', 'TMSRS_CD', col0]]
    DF1_ = DF1.loc[DF1[col1].notnull(), ['BASE_DT', 'TMSRS_CD', col1]]
    DF_out = pd.merge(DF0_, DF1_, on=['BASE_DT', 'TMSRS_CD'])
    return DF_out

TMP0 = my_merge(DF_IBES_DPS, DF_IBES_EPS, 'DPS_fy0', 'EPS_fy0')
TMP0['Payout_fy0'] = TMP0.apply(
    lambda x: (100 * x['DPS_fy0'] / x['EPS_fy0']) if x['EPS_fy0'] > 0 else np.nan,
    axis=1)
TMP0.loc[TMP0['Payout_fy0'] > 100, 'Payout_fy0'] = 100

TMP1 = my_merge(DF_IBES_DPS, DF_IBES_EPS, 'DPS_fy1', 'EPS_fy1')
TMP1['Payout_fy1'] = TMP1.apply(
    lambda x: (100 * x['DPS_fy1'] / x['EPS_fy1']) if x['EPS_fy1'] > 0 else np.nan,
    axis=1)
TMP1.loc[TMP1['Payout_fy1'] > 100, 'Payout_fy1'] = 100

TMP2 = my_merge(DF_IBES_DPS, DF_IBES_EPS, 'DPS_fy2', 'EPS_fy2')
TMP2['Payout_fy2'] = TMP2.apply(
    lambda x: (100 * x['DPS_fy2'] / x['EPS_fy2']) if x['EPS_fy2'] > 0 else np.nan,
    axis=1)
TMP2.loc[TMP2['Payout_fy2'] > 100, 'Payout_fy2'] = 100

def my_merge(A, B):
    return pd.merge(A, B, on=['BASE_DT', 'TMSRS_CD'], how='outer')
DF_IBES_Payout = reduce(my_merge, [TMP0, TMP1, TMP2])

# CREATE Expected2YR Payout AVERAGE-------
def get_value_col(DF, col):
    return DF.loc[DF[col].notnull(),
                  ['BASE_DT', 'TMSRS_CD', col]
                 ].rename(columns={col: 'Value_'})
TMP = reduce(
    lambda x, y: pd.DataFrame.append(x, y, sort=False),
    [get_value_col(TMP0, 'Payout_fy0'),
     get_value_col(TMP1, 'Payout_fy1'),
     get_value_col(TMP2, 'Payout_fy2')])

tmp_ = TMP.groupby(
    ['BASE_DT', 'TMSRS_CD'], as_index=False)['Value_'].count()
TMP = TMP.merge(
    tmp_.rename(columns={'Value_': 'CNT'}), on=['BASE_DT', 'TMSRS_CD'])

TMP_ = TMP[TMP['CNT'] >= 2]
Payout_mean = TMP_.groupby(['BASE_DT', 'TMSRS_CD'], as_index=False)['Value_'].mean()
Payout_mean.rename(columns={'Value_': 'Payout_avg'}, inplace=True)
# END AVERAGE calculation-------

DF_IBES_Payout = pd.merge(DF_IBES_Payout, Payout_mean, on=['BASE_DT', 'TMSRS_CD'], how='left')
DF_IBES_Payout['StyleName'] = 'IBES_Payout'

save_batch(bkfil, DF_IBES_EPS, mapping, fileName[:-4] + '_EPS')
save_batch(bkfil, DF_IBES_DPS, mapping, fileName[:-4] + '_DPS')
save_batch(bkfil, DF_IBES_Payout, mapping, fileName[:-4] + '_Payout')

from batch_utils.fill_monitor import print_fillReport
out = print_fillReport(bkfil, freq, DF_IBES_EPS)
out = print_fillReport(bkfil, freq, DF_IBES_DPS)
out = print_fillReport(bkfil, freq, DF_IBES_Payout, chk_col='Payout_fy1')