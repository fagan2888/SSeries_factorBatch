# -*- coding: utf-8 -*-

# Python 3.6
# author: EJ Jang
# date: 2018.06.14

# Options - Batch or Backfill (bkfil: False, True)
add_lback_kwarg = dict(add_lback_yr=10, add_lback_qtr=24)
fileName = 'AxiomaMomentum'
mapping = 'sedol'

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

from batch_utils.utils_dateSeq import batch_sequence
from batch_utils.utils_mapping import get_Mapping, getUnique_TMSRS_CD
# from batch_utils.utils_mapping_orig import get_Mapping_orig
from batch_utils.ItemInfo import Item_lst
from batch_utils.common import chunker, chunker_count, add_mapped_tick
from batch_utils.common import firstCheck_duplicates, secondCheck_columns
from batch_utils.common import check_mapping_df, save_batch
from batch_utils.common import batch_monitor_msg, batch_finish_msg

from batch_utils.utils_axioma import AxiomaModel

# Date Sequence to be made by this batch
bkfil, rtvStart, seq_DT = batch_sequence(option, freq)

# Getting the Universe in TMSRS_CD ~ Code Map
allSec = getUnique_TMSRS_CD()
codeMap = get_Mapping(mapping)

trim_codeMap = codeMap[codeMap['TMSRS_CD'].isin(allSec)].copy()

#*------------ Debug Map
trim_codeMap.sort_values(['TMSRS_CD', 'startDT', 'endDT'], inplace=True)
trim_codeMap.drop_duplicates(subset=['TMSRS_CD', 'startDT'], keep='last', inplace=True)

cnt = trim_codeMap.groupby('TMSRS_CD', as_index=False)['Sedol'].count()
cnt.rename(columns={'Sedol': 'CNT'}, inplace=True)
trim_codeMap = pd.merge(trim_codeMap, cnt, on='TMSRS_CD')

debug_dt = trim_codeMap.groupby('TMSRS_CD')['endDT'].max().to_frame('endDT')
debug_dt['endDT_fix'] = '20790606'
trim_codeMap = pd.merge(trim_codeMap, debug_dt, on=['TMSRS_CD', 'endDT'], how='left')
trim_codeMap.loc[trim_codeMap['endDT_fix'].notnull(), 'endDT'] = trim_codeMap.loc[
    trim_codeMap['endDT_fix'].notnull(), 'endDT_fix']
trim_codeMap.drop('endDT_fix', axis=1, inplace=True)

trim_codeMap.sort_values(['TMSRS_CD', 'startDT', 'Sedol'], inplace=True)
trim_codeMap.reset_index(drop=True, inplace=True)

DEBUG_ = trim_codeMap[trim_codeMap['CNT'] > 1].copy()
DEBUG_.sort_values(['TMSRS_CD', 'startDT', 'Sedol'], inplace=True)
GOOD_ = trim_codeMap[trim_codeMap['CNT'] == 1].copy()
GOOD_.sort_values(['TMSRS_CD', 'startDT', 'Sedol'], inplace=True)

def my_func(df):
    _raw = df.values
    def min1date(x):    
        x_ = dt.datetime.strptime(x, '%Y%m%d').date() - dt.timedelta(days=1)
        return x_.strftime('%Y%m%d')

    for i in range(1, len(_raw)):
        dt_ = min1date(_raw[i, 0])
        if (dt_ > _raw[i - 1, 1]) | (dt_ < _raw[i - 1, 1]):
            _raw[i - 1, 1] = dt_
    return pd.DataFrame(_raw, index=df.index, columns=df.columns)

DEBUG_[['startDT', 'endDT']] = DEBUG_.groupby('TMSRS_CD')['startDT', 'endDT'].apply(my_func)

trim_codeMap = DEBUG_.append(GOOD_, sort=True)
trim_codeMap.sort_index(inplace=True)
#*------------ Debug Map

trim_codeMap_uniq = trim_codeMap['TMSRS_CD'].unique()

print(trim_codeMap.iloc[:2])
print('\n>>> Total Mapping Securities #: {}'.format(trim_codeMap.shape[0]))
print('>>> Total Performing Securities #: {}'.format(trim_codeMap_uniq.shape[0]))

# Checking Level of Duplicates in codeMap
chk_codeMap = check_mapping_df(trim_codeMap, mapCode_nm='Sedol')

DF_Momentum = pd.DataFrame()

tot_n = seq_DT.shape[0]
st_time = time.time()
for i, dt_ in enumerate(seq_DT):
    handle_ax = AxiomaModel(modelDate=dt_, offline=True, check_existing_csv=True)
    handle_ax.get_RM()

    DF = handle_ax.factor_exp[['ShortTermMomentum', 'MediumTermMomentum']].copy()

    DF.reset_index(drop=False, inplace=True)
    DF['BASE_DT'] = handle_ax.requestDate
    DF['Sedol'] = DF['SEDOL'].str[:6]

    DFmelt = DF.melt(
        id_vars=['BASE_DT', 'SEDOL', 'Sedol'],
        value_vars=['ShortTermMomentum', 'MediumTermMomentum'],
        var_name='StyleName', value_name='Value_')
    DFmelt.dropna(axis=0, subset=['SEDOL'], inplace=True)

    DF_ = add_mapped_tick(DFmelt, trim_codeMap, on=['Sedol'])
    DF_['freq'] = 'D'
    DF_.rename(columns={'mstrCtry': 'ref'}, inplace=True)
    DF_Momentum = DF_Momentum.append(DF_)

    batch_monitor_msg(i, tot_n, st_time, add_msg=dt_)
batch_finish_msg(tot_n, st_time)

DF_Momentum.rename(columns={'SEDOL': 'Code'}, inplace=True)
cols = ['BASE_DT', 'TMSRS_CD', 'Code', 'Sedol', 'RGN_TP_CD',
        'Value_', 'freq', 'ref', 'StyleName']
DF_Momentum = DF_Momentum[cols].copy()

# check1
DF_Momentum.loc[
    DF_Momentum.duplicated(subset=['BASE_DT', 'TMSRS_CD', 'StyleName']),
                           ['BASE_DT', 'TMSRS_CD', 'StyleName']]

DF_Momentum.drop_duplicates(subset=['BASE_DT', 'TMSRS_CD', 'StyleName'],
                            keep='first', inplace=True)

# check2 - Should be Empty
DF_Momentum.loc[
    DF_Momentum.duplicated(subset=['BASE_DT', 'TMSRS_CD', 'StyleName']),
                           ['BASE_DT', 'TMSRS_CD', 'StyleName']]

#*------------
firstCheck_duplicates(DF_Momentum, add_cols=['TMSRS_CD', 'StyleName'])
#*------------

#*------------
secondCheck_columns(DF_Momentum)
#*------------
DF = DF_Momentum

# Save!
save_batch(bkfil, DF, mapping, fileName)

from batch_utils.fill_monitor import print_fillReport
out = print_fillReport(bkfil, freq, DF)