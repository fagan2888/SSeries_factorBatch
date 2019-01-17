# -*- coding: utf-8 -*-

# Python 3.6
# author: EJ Jang
# date: 2018.06.14

# Options - Batch or Backfill (bkfil: False, True)
fileName = 'refIBES_ROE'
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

DF_IBES_ROE = pd.DataFrame()

# <<NA>>
# Transform Security List into Batch Chunks
trim_codeMap_uniq = trim_codeMap.loc[trim_codeMap['RGN_TP_CD'] == '1', 'Code'].unique()
print('>>> Total NA Performing Securities #: {}'.format(trim_codeMap_uniq.shape[0]))
tot_n = chunker_count(trim_codeMap_uniq, 50)
Code_lst_tot = chunker(trim_codeMap_uniq.tolist(), 50)


def my_merge(df1, df2):
    res = pd.merge(df1, df2, on=['Code', 'RGN_TP_CD', 'BASE_DT'], how='outer')
    return res

st_time = time.time()
for i, Code_lst in enumerate(Code_lst_tot):

    var_Name = IBESItem_lst.loc['ROE_US']['var_Name']
    DF_ROE_ = IBES_year_Ratio(Code=Code_lst, St_Dt=rtvStart,
                              **dict(IBESItem_lst.loc['ROE_US']))
    DF_ROE_['RGN_TP_CD'] = 1
    # ActValue0, ROE_fy0
    DF_tmp = DF_ROE_.loc[DF_ROE_['ActValue0'].notnull(),
                         ['Code', 'RGN_TP_CD', 'EstDate', 'maxUseDate',
                          'FY0', 'ActValue0', 'ROE_fy0']].copy()
    DF_tmp.rename(columns={'EstDate': 'BASE_DT'}, inplace=True)
    DF_raw0 = IBES_resample(seq_DT, DF_tmp, ['FY0', 'ActValue0', 'ROE_fy0'])
    # Px0, Median1, Median2, ROE_fy1
    DF_tmp = DF_ROE_.loc[DF_ROE_['Median1'].notnull(),
                         ['Code', 'RGN_TP_CD', 'EstDate', 'maxUseDate',
                          'Px0', 'FY1', 'Median1', 'FY2', 'Median2', 'ROE_fy1']].copy()
    DF_tmp.rename(columns={'EstDate': 'BASE_DT'}, inplace=True)
    DF_raw1 = IBES_resample(seq_DT, DF_tmp, ['Px0', 'FY1', 'Median1', 'FY2', 'Median2', 'ROE_fy1'])
    # Median3, ROE_fy2
    DF_tmp = DF_ROE_.loc[DF_ROE_['Median1'].notnull(),
                         ['Code', 'RGN_TP_CD', 'EstDate', 'maxUseDate',
                          'FY3', 'Median3', 'ROE_fy2']].copy()
    DF_tmp.rename(columns={'EstDate': 'BASE_DT'}, inplace=True)
    DF_raw2 = IBES_resample(seq_DT, DF_tmp, ['FY3', 'Median3', 'ROE_fy2'])

    DF_raw_samp = reduce(my_merge, [DF_raw0, DF_raw1, DF_raw2])

    DF_IBES_ROE = DF_IBES_ROE.append(DF_raw_samp, sort=False)
    
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

    var_Name = IBESItem_lst.loc['ROE_exUS']['var_Name']
    DF_ROE_ = IBES_year_Ratio(Code=Code_lst, St_Dt=rtvStart,
                              **dict(IBESItem_lst.loc['ROE_exUS']))
    DF_ROE_['RGN_TP_CD'] = 3
    # ActValue0, ROE_fy0
    DF_tmp = DF_ROE_.loc[DF_ROE_['ActValue0'].notnull(),
                         ['Code', 'RGN_TP_CD', 'EstDate', 'maxUseDate',
                          'FY0', 'ActValue0', 'ROE_fy0']].copy()
    DF_tmp.rename(columns={'EstDate': 'BASE_DT'}, inplace=True)
    DF_raw0 = IBES_resample(seq_DT, DF_tmp, ['FY0', 'ActValue0', 'ROE_fy0'])
    # Px0, Median1, Median2, ROE_fy1
    DF_tmp = DF_ROE_.loc[DF_ROE_['Median1'].notnull(),
                         ['Code', 'RGN_TP_CD', 'EstDate', 'maxUseDate',
                          'Px0', 'FY1', 'Median1', 'FY2', 'Median2', 'ROE_fy1']].copy()
    DF_tmp.rename(columns={'EstDate': 'BASE_DT'}, inplace=True)
    DF_raw1 = IBES_resample(seq_DT, DF_tmp, ['Px0', 'FY1', 'Median1', 'FY2', 'Median2', 'ROE_fy1'])
    # Median3, ROE_fy2
    DF_tmp = DF_ROE_.loc[DF_ROE_['Median1'].notnull(),
                         ['Code', 'RGN_TP_CD', 'EstDate', 'maxUseDate',
                          'FY3', 'Median3', 'ROE_fy2']].copy()
    DF_tmp.rename(columns={'EstDate': 'BASE_DT'}, inplace=True)
    DF_raw2 = IBES_resample(seq_DT, DF_tmp, ['FY3', 'Median3', 'ROE_fy2'])
    
    DF_raw_samp = reduce(my_merge, [DF_raw0, DF_raw1, DF_raw2])

    DF_IBES_ROE = DF_IBES_ROE.append(DF_raw_samp, sort=False)

    batch_monitor_msg(i, tot_n, st_time)
batch_finish_msg(tot_n, st_time)

DF_IBES_ROE.sort_values(['RGN_TP_CD', 'Code', 'BASE_DT'], inplace=True)
DF_IBES_ROE.reset_index(drop=True, inplace=True)
DF_IBES_ROE = chg_to_type(
    DF_IBES_ROE, chg_col=['BASE_DT', 'Code', 'RGN_TP_CD'], type_=str)

#*------------
firstCheck_duplicates(DF_IBES_ROE, add_cols=['RGN_TP_CD'])
#*------------
DF_IBES_ROE = add_mapped_tick(DF_IBES_ROE, trim_codeMap, on=['Code', 'RGN_TP_CD'])

cols = ['BASE_DT', 'TMSRS_CD', 'Code', 'RGN_TP_CD', 'FY0', 'Px0',
        'ActValue0', 'FY1', 'Median1', 'FY2', 'Median2',
        'FY3', 'Median3', 'ROE_fy0', 'ROE_fy1', 'ROE_fy2']
DF_IBES_ROE = DF_IBES_ROE[cols]

# CREATE Expected2YR ROE AVERAGE-------
A0 = DF_IBES_ROE.loc[DF_IBES_ROE['ROE_fy0'].notnull(),
                     ['BASE_DT', 'TMSRS_CD', 'Code', 'RGN_TP_CD', 'ROE_fy0']]
A0.rename(columns={'ROE_fy0': 'Value_'}, inplace=True)
A0['Value_'] = A0['Value_'].apply(lambda x: np.sign(x) * 100 if np.abs(x) > 100 else x)

A1 = DF_IBES_ROE.loc[DF_IBES_ROE['ROE_fy1'].notnull(),
                     ['BASE_DT', 'TMSRS_CD', 'Code', 'RGN_TP_CD', 'ROE_fy1']]
A1.rename(columns={'ROE_fy1': 'Value_'}, inplace=True)
A1['Value_'] = A1['Value_'].apply(lambda x: np.sign(x) * 100 if np.abs(x) > 100 else x)

A2 = DF_IBES_ROE.loc[DF_IBES_ROE['ROE_fy2'].notnull(),
                     ['BASE_DT', 'TMSRS_CD', 'Code', 'RGN_TP_CD', 'ROE_fy2']]
A2.rename(columns={'ROE_fy2': 'Value_'}, inplace=True)
A2['Value_'] = A2['Value_'].apply(lambda x: np.sign(x) * 100 if np.abs(x) > 100 else x)

A = reduce(lambda x, y: pd.DataFrame.append(x, y, sort=False), [A0, A1, A2])
A['CNT'] = A.groupby(['BASE_DT', 'TMSRS_CD'])['Value_'].transform(len)

A_ = A[A['CNT'] >= 2]
DF_final = A_.groupby(['BASE_DT', 'TMSRS_CD'], as_index=False)['Value_'].mean()
DF_final.rename(columns={'Value_': 'ROE_avg'}, inplace=True)
# END AVERAGE calculation-------

# Finalize Reference
DF = pd.merge(DF_IBES_ROE, DF_final, on=['BASE_DT', 'TMSRS_CD'])
DF['StyleName'] = 'IBES_ROE'
DF.sort_values(['TMSRS_CD', 'RGN_TP_CD', 'BASE_DT'], inplace=True)
DF.drop_duplicates(['TMSRS_CD', 'BASE_DT'], keep='first', inplace=True)
print(DF.iloc[:2])

save_batch(bkfil, DF, mapping, fileName)

from batch_utils.fill_monitor import print_fillReport
out = print_fillReport(bkfil, freq, DF)