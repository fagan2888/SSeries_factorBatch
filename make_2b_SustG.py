# -*- coding: utf-8 -*-

# Python 3.6
# author: EJ Jang
# date: 2018.06.14

# Options - Batch or Backfill (bkfil: False, True)
add_lback_kwarg = dict(add_lback_yr=34, add_lback_qtr=48)
fileName = 'SustG'
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
from batch_utils.ItemInfo import Item_lst, RatioItem_lst, CurrItem_lst
from batch_utils.common import chunker, chunker_count, chg_to_type, add_mapped_tick
from batch_utils.common import firstCheck_duplicates, secondCheck_columns
from batch_utils.common import check_mapping_df, save_batch
from batch_utils.common import batch_monitor_msg, batch_finish_msg

from batch_utils import WS_retrieve_custom, WS_year, WS_qtr_currToHist, WS_resample
from batch_utils import simple_add, simple_mult, simple_div, simple_subtract
from batch_utils import align_add, align_mult, align_div, align_subtract
from batch_utils import substitute_Value
from batch_utils.WS.WS_histChg import get_HistAvg, get_HistChgAvg

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
DF_Payout_qtr, DF_Payout_yr = pd.DataFrame(), pd.DataFrame()
DF_epsg_qtr, DF_epsg_yr = pd.DataFrame(), pd.DataFrame()

# Transform Security List into Batch Chunks
tot_n = chunker_count(trim_codeMap_uniq, 50)
Code_lst_tot = chunker(trim_codeMap_uniq.tolist(), 50)

# Bring needed items
re_item = RatioItem_lst.loc['ROE_ws']
ni_item = Item_lst.loc['NetIncome_EPS']
po_item = RatioItem_lst.loc['Payout_ws']
poc_item = CurrItem_lst.loc['Payout_curr']

st_time = time.time()
for i, Code_lst in enumerate(Code_lst_tot):
    # Get Data
    roe_yr, roe_qtr = WS_retrieve_custom(
        re_item['Item'], re_item['Table'], Code_lst, None, bkfil, **add_lback_kwarg)
    eps_yr, eps_qtr = WS_retrieve_custom(
        ni_item['Item'], ni_item['Table'], Code_lst, 'sum', bkfil, **add_lback_kwarg)

    payout_yr = WS_year(po_item['Item'], po_item['Table'], Code_lst, bkfil, add_lback=34)
    payout_qtr = WS_qtr_currToHist(poc_item['Item'], poc_item['Table'], Code_lst, bkfil, add_lback=48)

    # Qtr
    roe_qtr_ = get_HistAvg(roe_qtr, k=8)
    payout_qtr_ = get_HistAvg(payout_qtr, k=8)
    epsg_qtr = get_HistChgAvg(eps_qtr, k=5, k2=4, growth=True)

    fill_cols = ['FiscalPrd', 'FiscalPrd2', 'CalPrdEndDate', 'Value_']
    roe_qtr_samp = WS_resample(seq_DT, roe_qtr_, fill_cols=fill_cols + ['FILLyr'])
    payout_qtr_samp = WS_resample(seq_DT, payout_qtr_, fill_cols=fill_cols)
    epsg_qtr_samp = WS_resample(seq_DT, epsg_qtr, fill_cols=fill_cols + ['Ratio'])

    DF_ROE_qtr = DF_ROE_qtr.append(roe_qtr_samp)
    DF_Payout_qtr = DF_Payout_qtr.append(payout_qtr_samp)
    DF_epsg_qtr = DF_epsg_qtr.append(epsg_qtr_samp)

    # Year
    roe_yr_ = get_HistAvg(roe_yr, k=2)
    payout_yr_ = get_HistAvg(payout_yr, k=2)
    epsg_yr = get_HistChgAvg(eps_yr, k=2, k2=1, growth=True)

    fill_cols = ['FiscalPrd', 'CalPrdEndDate', 'Value_']
    roe_yr_samp = WS_resample(seq_DT, roe_yr, fill_cols=fill_cols + ['FILLyr'])
    payout_yr_samp = WS_resample(seq_DT, payout_yr, fill_cols=fill_cols + ['FILLyr'])
    epsg_yr_samp = WS_resample(seq_DT, epsg_yr, fill_cols=fill_cols + ['Ratio'])

    DF_ROE_yr = DF_ROE_yr.append(roe_yr_samp, sort=False)
    DF_Payout_yr = DF_Payout_yr.append(payout_yr_samp, sort=False)
    DF_epsg_yr = DF_epsg_yr.append(epsg_yr_samp, sort=False)

    batch_monitor_msg(i, tot_n, st_time)
batch_finish_msg(tot_n, st_time)

DF_ROE_trail = substitute_Value(DF_ROE_yr, DF_ROE_qtr)
DF_Payout_trail = substitute_Value(DF_Payout_yr, DF_Payout_qtr)
DF_epsg_trail = substitute_Value(DF_epsg_yr, DF_epsg_qtr)

DF_ROE_trail = DF_ROE_trail[
    DF_ROE_trail['Value_'].notnull()
    ].sort_values(['Code', 'BASE_DT']).copy()
DF_ROE_trail.loc[
    DF_ROE_trail['Value_'].abs() > 100, 'Value_'] = DF_ROE_trail.loc[
    DF_ROE_trail['Value_'].abs() > 100, 'Value_'].apply(lambda x: 100 * np.sign(x))

DF_Payout_trail.loc[DF_Payout_trail['Value_'] < 0, 'Value_'] = np.nan
DF_Payout_trail = DF_Payout_trail[
    DF_Payout_trail['Value_'].notnull()
    ].sort_values(['Code', 'BASE_DT']).copy()
DF_Payout_trail.loc[DF_Payout_trail['Value_'] > 100, 'Value_'] = 100

DF_epsg_trail = DF_epsg_trail[
    DF_epsg_trail['Value_'].notnull()
    ].sort_values(['Code', 'BASE_DT']).copy()

DF_ROE_trail.reset_index(drop=True, inplace=True)
DF_Payout_trail.reset_index(drop=True, inplace=True)
DF_epsg_trail.reset_index(drop=True, inplace=True)

cols = ['BASE_DT', 'TMSRS_CD', 'Code', 'Value_yr', 'Value_qtr', 'Value_',
        'RGN_TP_CD', 'freq', 'ref']
DF_ROE_trail = add_mapped_tick(DF_ROE_trail, trim_codeMap)[cols]
DF_Payout_trail = add_mapped_tick(DF_Payout_trail, trim_codeMap)[cols]
DF_epsg_trail = add_mapped_tick(DF_epsg_trail, trim_codeMap)[cols]

DF_ROE_trail['StyleName'] = 'ROE_l2yr'
DF_Payout_trail['StyleName'] = 'Payout_l2yr'
DF_epsg_trail['StyleName'] = 'EPSg_l2yr'

#*------------
secondCheck_columns(DF_ROE_trail, DF_Payout_trail, DF_epsg_trail)
#*------------

DF = reduce(
    lambda x, y: pd.DataFrame.append(x, y, sort=False),
    [DF_ROE_trail, DF_Payout_trail])
DF.reset_index(drop=True, inplace=True)

# Save!
save_batch(bkfil, DF, mapping, fileName + '_ROEnPayout')

# Save!
save_batch(bkfil, DF_epsg_trail, mapping, fileName + '_EPSg')

from batch_utils.fill_monitor import print_fillReport
out = print_fillReport(bkfil, freq, DF)
out = print_fillReport(bkfil, freq, DF_epsg_trail)

# <<Sustainable Growth Script>>
dir_ = 'save_total' if bkfil else 'save_batch'
DF_IBES_Payout = pd.read_pickle('{}/IBES_refIBES_Payout.p'.format(dir_))
DF_IBES_ROE = pd.read_pickle('{}/IBES_refIBES_ROE.p'.format(dir_))

# Merge IBES with Worldscope for (normalized) Payout & ROE (comb. average)
TMP = DF_IBES_Payout[
    ['BASE_DT', 'TMSRS_CD', 'Payout_avg']
    ].rename(columns={'Payout_avg': 'Value_1'}).copy()
DF_Payout_norm = pd.merge(
    DF_Payout_trail, TMP, on=['BASE_DT', 'TMSRS_CD'], how='left')
DF_Payout_norm['Value_norm'] = DF_Payout_norm.apply(
    lambda x: (x['Value_'] + x['Value_1']) / 2 if pd.notnull(x['Value_1'])
    else x['Value_'], axis=1)

TMP = DF_IBES_ROE[
    ['BASE_DT', 'TMSRS_CD', 'ROE_avg']
    ].rename(columns={'ROE_avg': 'Value_1'}).copy()
DF_ROE_norm = pd.merge(
    DF_ROE_trail, TMP, on=['BASE_DT', 'TMSRS_CD'], how='left')
DF_ROE_norm['Value_norm'] = DF_ROE_norm.apply(
    lambda x: (x['Value_'] + x['Value_1']) / 2 if pd.notnull(x['Value_1'])
    else x['Value_'], axis=1)

# Calculate Sustainable Growth Factor
TMP0 = DF_ROE_norm[
    ['BASE_DT', 'TMSRS_CD', 'Value_norm']].rename(
    columns={'Value_norm': 'ROE_norm'})
TMP1 = DF_Payout_norm[
    ['BASE_DT', 'TMSRS_CD', 'Value_norm']].rename(
    columns={'Value_norm': 'Payout_norm'})

DF_SustG_tot = pd.merge(TMP0, TMP1, on=['BASE_DT', 'TMSRS_CD'])
DF_SustG_tot['Value_'] = DF_SustG_tot['ROE_norm'] * (
    1 - DF_SustG_tot['Payout_norm'] / 100)
DF_SustG_tot['StyleName'], DF_SustG_tot['freq'] = 'SustG', None
DF_SustG_tot['Code'], DF_SustG_tot['RGN_TP_CD'] = None, None
#*------------
secondCheck_columns(DF_SustG_tot, DF)
#*------------

# Save!
save_batch(bkfil, DF_SustG_tot, 'comb', fileName)
out = print_fillReport(bkfil, freq, DF_SustG_tot, chk_col='Value_')