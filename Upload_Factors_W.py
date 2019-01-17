# -*- coding: utf-8 -*-

# Python 3.6
# author: EJ Jang
# date: 2018.06.14

# option = 'backfill'
freq = 'W'
import sys
option = sys.argv[1]
# freq = sys.argv[2]
print('# Starting Upload Protocal for ("{}", "{}")\n'.format(option, freq))

import numpy as np
import pandas as pd
import datetime as dt
import time
import csv
import re
import os

from batch_utils.common import must, chg_to_type
from batch_utils.utils_dateSeq import batch_sequence
from batch_utils.utils_db_alch2 import connectDB
from batch_utils.utils_sql import create_table, update_table, check_exist
import batch_utils.utils_upload as up

# from scipy.stats import mode, kurtosis, skew
# print(np.mean(valadj_))
# print(np.median(valadj_))
# print(kurtosis(valadj_))
# print(skew(valadj_))

# ----------------------
# Date Sequence to be made by this batch
bkfil, rtvStart, seq_DT = batch_sequence(option, freq, rtvDays=60)
print('>>> Factor Dates to be Uploaded:')
if seq_DT.shape[0] > 8:
    print('    ' + ', '.join(seq_DT.iloc[:3]) + ' ... ' + ', '.join(seq_DT.iloc[-3:]))
else:
    print('    ' + ', '.join(seq_DT))
print('\n')

# ----------------------
_read_folder = 'save_{}'.format('total' if bkfil else 'batch')
print('>>> Checking Files to be uploaded from "{}" folder'.format(_read_folder))
_tgt_types = ['comb', 'starmine', 'sedol', 'worldscope']

upl_files = [f for f in os.listdir(_read_folder)
             if f.split('_')[0] in _tgt_types]
if 'IBES_ErnRev3M.p' in os.listdir(_read_folder):
    upl_files.append('IBES_ErnRev3M.p')
print('>>> Files to be Uploaded:')
for k in range(0, len(upl_files), 5):
    print('    ' + ' | '.join(upl_files[k: k + 3]))
print('\n')

# ----------------------
print('>>> Checking if "FactorBuild_adjSignal.csv" Exists:')
factor_adjMap = pd.read_csv(
    'batch_utils/key_files/FactorBuild_adjSignal.csv',
    index_col=0)
print(factor_adjMap.head())
print('\n')

# ----------------------
print('>>> Getting QAD Master Security Information for Reference Info:')
conn = connectDB('MSSQL_QAD')
secInf = up.get_refInformation(conn)
conn.close()
print('    Done.\n')

# ----------------------
server_ = 'MSSQL_DEV'
db_name = 'EJ_LIVE_FACTORS2'
print('>>> Checking if DB: {} exists:'.format(db_name))
conn = connectDB(server_)
if not check_exist(conn, db_name):
    while True:
        input_ = input('    Table does not exist! Proceed to create one?? (y/n)')
        if input_ in ['y', 'n']:
            break
        else:
            print('    Input should be y/n. Try Again.')
    if input_ == 'y':
        create_table(conn, db_name, up.typeStr, primary=up.primary)
    else:
        conn.close()
        exit()
else:
    print('    Exists. Checked.\n')
conn.close()

# ----------------------
# UPLOADING SCRIPT
def unloadedFactors_log(filename, StyleName, morethan_):
    today_ = dt.date.today().strftime('%Y%m%d')
    print('    !!! No data to update "{}"'.format(fctr))
    print('    !!! Current DB maxDate is "{}"'.format(morethan_))
    print('    !!! Leaving Log in "WARN_unloadedFactors_{}.log"'.format(morethan_))
    with open('WARN_unloadedFactors_{}.log'.format(morethan_), 'a+', newline='') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow([filename, StyleName, morethan_])

for file in upl_files:
    print(">>> Opening file: '{}'".format(file))
    df = pd.read_pickle('{}/{}'.format(_read_folder, file))
    fctr_lst = df['StyleName'].unique().tolist()
    for k in range(0, len(fctr_lst), 5):
        print('    # Factors List:')
        print('    # ' + ' | '.join(fctr_lst[k:k + 5]))

    fctr_lstMap = factor_adjMap.reindex(fctr_lst)
    assert fctr_lstMap['Transform'].notnull().all(), (
        "Factor missing in 'FactorBuild_adjSignal.csv'!")

    upload_order = fctr_lstMap['Transform'].unique().tolist()
    for adj_ in upload_order:
        # adj_ = 'r'
        list_ = fctr_lstMap.query("Transform=='{}'".format(adj_)).index.tolist()

        # COPY the upload sample into 'workDF'
        for j, fctr in enumerate(list_):
            print('    ({}) Processing {}:'.format(j, fctr))
            conn = connectDB(server_)

            workDF = df[df['StyleName']==fctr].copy()
            morethan_ = up.check_maxDate(conn, db_name=db_name, styleName=fctr)
            workDF = workDF[workDF['BASE_DT'] > morethan_]
            if workDF.shape[0] == 0:
                unloadedFactors_log(file, fctr, morethan_)
                conn.close()
                continue
            else:
                # setup before adjusting
                workDF.rename(columns={'RGN_TP_CD': 'CD_ref'}, inplace=True)
                if 'ref' not in workDF.columns.tolist():
                    workDF['ref'] = None

                # adjusting
                workDF, _ = up.adjScore_DFValue_(workDF, adj_, copy=True)
                workDF['Value_adj'] = workDF.groupby(
                    'BASE_DT')['Value_adj'].apply(up.winsor_medZ4)

                col_order = ['BASE_DT', 'StyleName', 'TMSRS_CD', 'Code', 'CD_ref',
                            'Value_', 'Value_adj', 'adj_op', 'freq', 'ref']
                workDF = workDF[col_order]

                if workDF['Value_adj'].isin([np.Inf, -np.Inf]).any():
                    raise AssertionError(
                        'Infinity Values (Value_adj) exist in {}_{}'.format(adj_, fctr))

                if workDF['Value_'].isin([np.Inf, -np.Inf]).any():
                    raise AssertionError(
                        'Infinity Values (Value_) exist in {}_{}'.format(adj_, fctr))

                workDF = pd.merge(workDF, secInf, on='TMSRS_CD', how='left')
                NOW_DT = dt.datetime.now()
                tmStr = NOW_DT.strftime("%Y-%m-%d %H:%M:%S.") + format(
                    int(round(NOW_DT.microsecond / 1000, 0)), "03d")
                workDF['REG_DTTM'] = tmStr
                workDF['REG_EMP_NMB'] = '2150416'
                workDF['LST_DTTM'] = tmStr
                workDF['LST_EMP_NMB'] = '2150416'
                workDF = workDF[up.typeStr.index]
                
                #*--- cleanse values to fit in sql table ---
                workDF['freq'] = workDF['freq'].str.replace('Norm', 'Nr')
                str_cols = ['Code', 'CD_ref', 'ref_CTRY', 'adj_op',
                            'ref_Name', 'ref_SEDOL', 'ref', 'freq']
                for col in str_cols:
                    workDF[col] = workDF[col].apply(
                        lambda x: x if isinstance(x, str) else 'NULL')
                float_cols = ['Value_', 'Value_adj']
                for col in float_cols:
                    workDF[col] = workDF[col].round(8)
                workDF['ref_Name'] = workDF['ref_Name'].apply(
                    lambda x: re.sub("'", " ", x))
                #*--- cleanse values to fit in sql table ---

                print('\n    - Uploading ({}) {}'.format(adj_, fctr))
                update_table(conn, db_name, workDF, up.typeStr, verbose=True)
                conn.close()
                continue