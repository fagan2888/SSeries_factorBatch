# -*- coding: utf-8 -*-

# Python 3.6
# author: EJ Jang
# date: 2018.06.14

# Options - Batch or Backfill (bkfil: False, True)
fileName = 'Starmine'
mapping = 'starmine'

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
from batch_utils.utils_db_alch2 import connectDB
from batch_utils.ItemInfo import Item_lst
from batch_utils.common import chunker, chunker_count, add_mapped_tick
from batch_utils.common import firstCheck_duplicates, secondCheck_columns
from batch_utils.common import list2sqlstr, chg_to_type, check_mapping_df, save_batch
from batch_utils.common import batch_monitor_msg, batch_finish_msg

from batch_utils.utils_axioma import AxiomaModel

# Date Sequence to be made by this batch
bkfil, rtvStart, seq_DT = batch_sequence(option, freq, rtvDays=65)

# Getting the Universe in TMSRS_CD ~ Code Map
allSec = getUnique_TMSRS_CD()
codeMap = get_Mapping(mapping)

trim_codeMap = codeMap[codeMap['TMSRS_CD'].isin(allSec)].copy()
# trim_codeMap_uniq = trim_codeMap['TMSRS_CD'].unique()



#*------------ Debug Map
trim_codeMap.sort_values(['TMSRS_CD', 'startDT', 'endDT'], inplace=True)
trim_codeMap.drop_duplicates(subset=['TMSRS_CD', 'startDT'], keep='last', inplace=True)

cnt = trim_codeMap.groupby('TMSRS_CD', as_index=False)['Code'].count()
cnt.rename(columns={'Code': 'CNT'}, inplace=True)
trim_codeMap = pd.merge(trim_codeMap, cnt, on='TMSRS_CD')

debug_dt = trim_codeMap.groupby('TMSRS_CD')['endDT'].max().to_frame('endDT')
debug_dt['endDT_fix'] = '20790606'
trim_codeMap = pd.merge(trim_codeMap, debug_dt, on=['TMSRS_CD', 'endDT'], how='left')
trim_codeMap.loc[trim_codeMap['endDT_fix'].notnull(), 'endDT'] = trim_codeMap.loc[
    trim_codeMap['endDT_fix'].notnull(), 'endDT_fix']
trim_codeMap.drop('endDT_fix', axis=1, inplace=True)

trim_codeMap.sort_values(['TMSRS_CD', 'startDT', 'Code'], inplace=True)
trim_codeMap.reset_index(drop=True, inplace=True)

DEBUG_ = trim_codeMap[trim_codeMap['CNT'] > 1].copy()
DEBUG_.sort_values(['TMSRS_CD', 'startDT', 'Code'], inplace=True)
GOOD_ = trim_codeMap[trim_codeMap['CNT'] == 1].copy()
GOOD_.sort_values(['TMSRS_CD', 'startDT', 'Code'], inplace=True)

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

print(trim_codeMap.iloc[:2])
print('\n>>> Total Mapping Securities #: {}'.format(trim_codeMap.shape[0]))
# print('>>> Total Performing Securities #: {}'.format(trim_codeMap_uniq.shape[0]))

# Checking Level of Duplicates in codeMap
chk_codeMap = check_mapping_df(trim_codeMap)

DF_ARM = pd.DataFrame()

# <<NA>>
# Transform Security List into Batch Chunks
trim_codeMap_uniq = trim_codeMap.loc[trim_codeMap['PermRegion'] == '1', 'Code'].unique()
print('>>> Total NA Performing Securities #: {}'.format(trim_codeMap_uniq.shape[0]))
tot_n = chunker_count(trim_codeMap_uniq, 50)
Code_lst_tot = chunker(trim_codeMap_uniq.tolist(), 50)

st_time = time.time()
for i, Code_lst in enumerate(Code_lst_tot):
    Sql_S = """
        Select SecID as Code, Item, EstCurr,
               convert(varchar(8), StartDate, 112) as BASE_DT,
               convert(varchar(8), isnull(EndDate, GETDATE()), 112) as expire_DT,
               Value_
          from SM2DArmAAm with (nolock)
         where Item = 45
           and SecId in ({})
           and Value_ is not null
           and Value_ <> -99999
           and Value_ <> -9999
           and StartDate >= '{}'
    """.format(list2sqlstr(Code_lst), rtvStart)
    CONNECT_QAD = connectDB(ODBC_NAME="MSSQL_QAD")
    DF = pd.read_sql(Sql_S, CONNECT_QAD)
    CONNECT_QAD.close()

    DF = chg_to_type(DF, ['Code', 'Item', 'BASE_DT', 'expire_DT'], type_=str)
    DF['PermRegion'] = '1'

    DF_trim = pd.DataFrame()
    for code_ in Code_lst:
        B = DF[DF['Code']==code_].copy()
        B['BASE_DT'] = pd.to_datetime(B['BASE_DT'], format='%Y%m%d')
        if B.shape[0] > 0:
            A = pd.to_datetime(seq_DT, format='%Y%m%d').to_frame('BASE_DT')

            DF_ = pd.merge_asof(A, B, on='BASE_DT')
            DF_.dropna(subset=['Code'], inplace=True)
            DF_['BASE_DT'] = DF_['BASE_DT'].dt.strftime('%Y%m%d')
            # last update typically has same-day expire_DT (need to divide)
            lst_DF = DF_[DF_['BASE_DT'] == seq_DT.max()].copy()
            bef_DF = DF_[DF_['BASE_DT'] != seq_DT.max()].copy()
            bef_DF = bef_DF.query('BASE_DT <= expire_DT')

            DF_trim = DF_trim.append(bef_DF, sort=False).append(lst_DF, sort=False)
    
    DF_ARM = DF_ARM.append(DF_trim)
    
    batch_monitor_msg(i, tot_n, st_time)
batch_finish_msg(tot_n, st_time)

# <<Asia>>
# Transform Security List into Batch Chunks
trim_codeMap_uniq = trim_codeMap.loc[trim_codeMap['PermRegion'] == '2', 'Code'].unique()
print('>>> Total Asia Performing Securities #: {}'.format(trim_codeMap_uniq.shape[0]))
tot_n = chunker_count(trim_codeMap_uniq, 50)
Code_lst_tot = chunker(trim_codeMap_uniq.tolist(), 50)

st_time = time.time()
for i, Code_lst in enumerate(Code_lst_tot):
    Sql_S = """
        Select SecID as Code, Item, EstCurr,
               convert(varchar(8), StartDate, 112) as BASE_DT,
               convert(varchar(8), isnull(EndDate, GETDATE()), 112) as expire_DT,
               Value_
          from SM2DArmAAp with (nolock)
         where Item = 45
           and SecId in ({})
           and Value_ is not null
           and Value_ <> -99999
           and Value_ <> -9999
           and StartDate >= '{}'
    """.format(list2sqlstr(Code_lst), rtvStart)
    CONNECT_QAD = connectDB(ODBC_NAME="MSSQL_QAD")
    DF = pd.read_sql(Sql_S, CONNECT_QAD)
    CONNECT_QAD.close()

    DF = chg_to_type(DF, ['Code', 'Item', 'BASE_DT', 'expire_DT'], type_=str)
    DF['PermRegion'] = '2'

    DF_trim = pd.DataFrame()
    for code_ in Code_lst:
        B = DF[DF['Code']==code_].copy()
        B['BASE_DT'] = pd.to_datetime(B['BASE_DT'], format='%Y%m%d')
        if B.shape[0] > 0:
            A = pd.to_datetime(seq_DT, format='%Y%m%d').to_frame('BASE_DT')

            DF_ = pd.merge_asof(A, B, on='BASE_DT')
            DF_.dropna(subset=['Code'], inplace=True)
            DF_['BASE_DT'] = DF_['BASE_DT'].dt.strftime('%Y%m%d')
            # last update typically has same-day expire_DT (need to divide)
            lst_DF = DF_[DF_['BASE_DT'] == seq_DT.max()].copy()
            bef_DF = DF_[DF_['BASE_DT'] != seq_DT.max()].copy()
            bef_DF = bef_DF.query('BASE_DT <= expire_DT')

            DF_trim = DF_trim.append(bef_DF, sort=False).append(lst_DF, sort=False)
    
    DF_ARM = DF_ARM.append(DF_trim)
    
    batch_monitor_msg(i, tot_n, st_time)
batch_finish_msg(tot_n, st_time)

# <<Europe>>
# Transform Security List into Batch Chunks
trim_codeMap_uniq = trim_codeMap.loc[trim_codeMap['PermRegion'] == '3', 'Code'].unique()
print('>>> Total Europe Performing Securities #: {}'.format(trim_codeMap_uniq.shape[0]))
tot_n = chunker_count(trim_codeMap_uniq, 50)
Code_lst_tot = chunker(trim_codeMap_uniq.tolist(), 50)

st_time = time.time()
for i, Code_lst in enumerate(Code_lst_tot):
    Sql_S = """
        Select SecID as Code, Item, EstCurr,
               convert(varchar(8), StartDate, 112) as BASE_DT,
               convert(varchar(8), isnull(EndDate, GETDATE()), 112) as expire_DT,
               Value_
          from SM2DArmAEa with (nolock)
         where Item = 45
           and SecId in ({})
           and Value_ is not null
           and Value_ <> -99999
           and Value_ <> -9999
           and StartDate >= '{}'
    """.format(list2sqlstr(Code_lst), rtvStart)
    CONNECT_QAD = connectDB(ODBC_NAME="MSSQL_QAD")
    DF = pd.read_sql(Sql_S, CONNECT_QAD)
    CONNECT_QAD.close()

    DF = chg_to_type(DF, ['Code', 'Item', 'BASE_DT', 'expire_DT'], type_=str)
    DF['PermRegion'] = '3'

    DF_trim = pd.DataFrame()
    for code_ in Code_lst:
        B = DF[DF['Code']==code_].copy()
        B['BASE_DT'] = pd.to_datetime(B['BASE_DT'], format='%Y%m%d')
        if B.shape[0] > 0:
            A = pd.to_datetime(seq_DT, format='%Y%m%d').to_frame('BASE_DT')

            DF_ = pd.merge_asof(A, B, on='BASE_DT')
            DF_.dropna(subset=['Code'], inplace=True)
            DF_['BASE_DT'] = DF_['BASE_DT'].dt.strftime('%Y%m%d')
            # last update typically has same-day expire_DT (need to divide)
            lst_DF = DF_[DF_['BASE_DT'] == seq_DT.max()].copy()
            bef_DF = DF_[DF_['BASE_DT'] != seq_DT.max()].copy()
            bef_DF = bef_DF.query('BASE_DT <= expire_DT')

            DF_trim = DF_trim.append(bef_DF, sort=False).append(lst_DF, sort=False)
    
    DF_ARM = DF_ARM.append(DF_trim)
    
    batch_monitor_msg(i, tot_n, st_time)
batch_finish_msg(tot_n, st_time)

#*------------
firstCheck_duplicates(DF_ARM)
#*------------
cols = ['TMSRS_CD', 'Code', 'RGN_TP_CD', 'PermRegion', 'startDT', 'endDT']
DF_ARM = add_mapped_tick(DF_ARM, trim_codeMap[cols].copy(), on=['Code', 'PermRegion'])

cols = ['BASE_DT', 'TMSRS_CD', 'Code', 'Value_', 'PermRegion']
DF_ARM = DF_ARM.loc[DF_ARM['Value_'].notnull(), cols].rename(
    columns={'PermRegion': 'RGN_TP_CD'})
DF_ARM['StyleName'], DF_ARM['freq'] = 'ARM_starmine', 'D'
#*------------
secondCheck_columns(DF_ARM)
#*------------

DF_EQ = pd.DataFrame()

# <<NA>>
# Transform Security List into Batch Chunks
trim_codeMap_uniq = trim_codeMap.loc[trim_codeMap['PermRegion'] == '1', 'Code'].unique()
print('>>> Total NA Performing Securities #: {}'.format(trim_codeMap_uniq.shape[0]))
tot_n = chunker_count(trim_codeMap_uniq, 50)
Code_lst_tot = chunker(trim_codeMap_uniq.tolist(), 50)

st_time = time.time()
for i, Code_lst in enumerate(Code_lst_tot):
    SQL_S = """
        Select SecID as Code, Item, FncCurr,
               convert(varchar(8), StartDate, 112) as BASE_DT,
               convert(varchar(8), isnull(EndDate, GETDATE()), 112) as expire_DT,
               Value_
          from SM2DEqAm with (nolock)
         where Item = 118
           and SecId in ({})
           and FscPeriod = 19
           and ChgPeriod = 0
           and Value_ is not null
           and Value_ <> -99999
           and Value_ <> -9999
           and StartDate >= '{}'
    """.format(list2sqlstr(Code_lst), rtvStart)
    CONNECT_QAD = connectDB(ODBC_NAME="MSSQL_QAD")
    DF = pd.read_sql(SQL_S, CONNECT_QAD)
    CONNECT_QAD.close()

    DF = chg_to_type(DF, ['Code', 'Item', 'BASE_DT', 'expire_DT'], type_=str)
    DF['PermRegion'] = '1'

    DF_trim = pd.DataFrame()
    for code_ in Code_lst:
        B = DF[DF['Code']==code_].copy()
        B['BASE_DT'] = pd.to_datetime(B['BASE_DT'], format='%Y%m%d')
        if B.shape[0] > 0:
            A = pd.to_datetime(seq_DT, format='%Y%m%d').to_frame('BASE_DT')

            DF_ = pd.merge_asof(A, B, on='BASE_DT')
            DF_.dropna(subset=['Code'], inplace=True)
            DF_['BASE_DT'] = DF_['BASE_DT'].dt.strftime('%Y%m%d')
            # last update typically has same-day expire_DT (need to divide)
            lst_DF = DF_[DF_['BASE_DT'] == seq_DT.max()].copy()
            bef_DF = DF_[DF_['BASE_DT'] != seq_DT.max()].copy()
            bef_DF = bef_DF.query('BASE_DT <= expire_DT')

            DF_trim = DF_trim.append(bef_DF, sort=False).append(lst_DF, sort=False)

    DF_EQ = DF_EQ.append(DF_trim)
    
    batch_monitor_msg(i, tot_n, st_time)
batch_finish_msg(tot_n, st_time)

# <<Asia>>
# Transform Security List into Batch Chunks
trim_codeMap_uniq = trim_codeMap.loc[trim_codeMap['PermRegion'] == '2', 'Code'].unique()
print('>>> Total Asia Performing Securities #: {}'.format(trim_codeMap_uniq.shape[0]))
tot_n = chunker_count(trim_codeMap_uniq, 50)
Code_lst_tot = chunker(trim_codeMap_uniq.tolist(), 50)

st_time = time.time()
for i, Code_lst in enumerate(Code_lst_tot):
    SQL_S = """
        Select SecID as Code, Item, FncCurr,
               convert(varchar(8), StartDate, 112) as BASE_DT,
               convert(varchar(8), isnull(EndDate, GETDATE()), 112) as expire_DT,
               Value_
          from SM2DEqAp with (nolock)
         where Item = 118
           and SecId in ({})
           and FscPeriod = 19
           and ChgPeriod = 0
           and Value_ is not null
           and Value_ <> -99999
           and Value_ <> -9999
           and StartDate >= '{}'
    """.format(list2sqlstr(Code_lst), rtvStart)
    CONNECT_QAD = connectDB(ODBC_NAME="MSSQL_QAD")
    DF = pd.read_sql(SQL_S, CONNECT_QAD)
    CONNECT_QAD.close()

    DF = chg_to_type(DF, ['Code', 'Item', 'BASE_DT', 'expire_DT'], type_=str)
    DF['PermRegion'] = '2'

    DF_trim = pd.DataFrame()
    for code_ in Code_lst:
        B = DF[DF['Code']==code_].copy()
        B['BASE_DT'] = pd.to_datetime(B['BASE_DT'], format='%Y%m%d')
        if B.shape[0] > 0:
            A = pd.to_datetime(seq_DT, format='%Y%m%d').to_frame('BASE_DT')

            DF_ = pd.merge_asof(A, B, on='BASE_DT')
            DF_.dropna(subset=['Code'], inplace=True)
            DF_['BASE_DT'] = DF_['BASE_DT'].dt.strftime('%Y%m%d')
            # last update typically has same-day expire_DT (need to divide)
            lst_DF = DF_[DF_['BASE_DT'] == seq_DT.max()].copy()
            bef_DF = DF_[DF_['BASE_DT'] != seq_DT.max()].copy()
            bef_DF = bef_DF.query('BASE_DT <= expire_DT')

            DF_trim = DF_trim.append(bef_DF, sort=False).append(lst_DF, sort=False)

    DF_EQ = DF_EQ.append(DF_trim)
    
    batch_monitor_msg(i, tot_n, st_time)
batch_finish_msg(tot_n, st_time)

# <<Europe>>
# Transform Security List into Batch Chunks
trim_codeMap_uniq = trim_codeMap.loc[trim_codeMap['PermRegion'] == '3', 'Code'].unique()
print('>>> Total Europe Performing Securities #: {}'.format(trim_codeMap_uniq.shape[0]))
tot_n = chunker_count(trim_codeMap_uniq, 50)
Code_lst_tot = chunker(trim_codeMap_uniq.tolist(), 50)

st_time = time.time()
for i, Code_lst in enumerate(Code_lst_tot):
    SQL_S = """
        Select SecID as Code, Item, FncCurr,
               convert(varchar(8), StartDate, 112) as BASE_DT,
               convert(varchar(8), isnull(EndDate, GETDATE()), 112) as expire_DT,
               Value_
          from SM2DEqEa with (nolock)
         where Item = 118
           and SecId in ({})
           and FscPeriod = 19
           and ChgPeriod = 0
           and Value_ is not null
           and Value_ <> -99999
           and Value_ <> -9999
           and StartDate >= '{}'
    """.format(list2sqlstr(Code_lst), rtvStart)
    CONNECT_QAD = connectDB(ODBC_NAME="MSSQL_QAD")
    DF = pd.read_sql(SQL_S, CONNECT_QAD)
    CONNECT_QAD.close()

    DF = chg_to_type(DF, ['Code', 'Item', 'BASE_DT', 'expire_DT'], type_=str)
    DF['PermRegion'] = '3'

    DF_trim = pd.DataFrame()
    for code_ in Code_lst:
        B = DF[DF['Code']==code_].copy()
        B['BASE_DT'] = pd.to_datetime(B['BASE_DT'], format='%Y%m%d')
        if B.shape[0] > 0:
            A = pd.to_datetime(seq_DT, format='%Y%m%d').to_frame('BASE_DT')

            DF_ = pd.merge_asof(A, B, on='BASE_DT')
            DF_.dropna(subset=['Code'], inplace=True)
            DF_['BASE_DT'] = DF_['BASE_DT'].dt.strftime('%Y%m%d')
            # last update typically has same-day expire_DT (need to divide)
            lst_DF = DF_[DF_['BASE_DT'] == seq_DT.max()].copy()
            bef_DF = DF_[DF_['BASE_DT'] != seq_DT.max()].copy()
            bef_DF = bef_DF.query('BASE_DT <= expire_DT')

            DF_trim = DF_trim.append(bef_DF, sort=False).append(lst_DF, sort=False)

    DF_EQ = DF_EQ.append(DF_trim)
    
    batch_monitor_msg(i, tot_n, st_time)
batch_finish_msg(tot_n, st_time)

#*------------
firstCheck_duplicates(DF_EQ)
#*------------
cols = ['TMSRS_CD', 'Code', 'RGN_TP_CD', 'PermRegion', 'startDT', 'endDT']
DF_EQ = add_mapped_tick(DF_EQ, trim_codeMap[cols].copy(), on=['Code', 'PermRegion'])

cols = ['BASE_DT', 'TMSRS_CD', 'Code', 'Value_', 'PermRegion']
DF_EQ = DF_EQ.loc[DF_EQ['Value_'].notnull(), cols].rename(
    columns={'PermRegion': 'RGN_TP_CD'})
DF_EQ['StyleName'], DF_EQ['freq'] = 'EQ_starmine', 'D'
#*------------
secondCheck_columns(DF_EQ)
#*------------

DF = DF_ARM.append(DF_EQ, sort=False)

# Save!
save_batch(bkfil, DF, mapping, fileName)

from batch_utils.fill_monitor import print_fillReport
out = print_fillReport(bkfil, freq, DF)