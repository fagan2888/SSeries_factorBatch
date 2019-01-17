# -*- coding: utf-8 -*-

# Python 3.6
# author: EJ Jang
# date: 2018.06.14

# Options - Batch or Backfill (bkfil: False, True)
fileName = 'GARP'
mapping = 'comb'
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
from batch_utils.common import firstCheck_duplicates, secondCheck_columns
from batch_utils.common import check_mapping_df, save_batch

# Date Sequence to be made by this batch
bkfil, rtvStart, seq_DT = batch_sequence(option, freq)

dir_ = 'save_total' if bkfil else 'save_batch'
DF_IBES_EPS = pd.read_pickle('{}/IBES_refIBES_EPS.p'.format(dir_))
DF_IBES_EPS = DF_IBES_EPS[DF_IBES_EPS['Px0'] != 0]
DF_IBES_DPS = pd.read_pickle('{}/IBES_refIBES_DPS.p'.format(dir_))
DF_IBES_DPS = DF_IBES_DPS[DF_IBES_DPS['Px0'] != 0]
DF_IBES_ROE = pd.read_pickle('{}/IBES_refIBES_ROE.p'.format(dir_))
DF_SustG_tot = pd.read_pickle('{}/comb_SustG.p'.format(dir_))

# PEf12m
cols = ['BASE_DT', 'TMSRS_CD', 'Code', 'RGN_TP_CD', 'Px0', 'EPS_fy1', 'PE_f12m']
DF_PEf12m = DF_IBES_EPS[cols].copy()
DF_PEf12m['Value_'] = DF_PEf12m.apply(
    lambda x: x['Px0'] / x['EPS_fy1'] if x['EPS_fy1'] > 0 else np.nan, axis=1)

DF_PEf12m.dropna(subset=['Value_'], axis=0, inplace=True)
DF_PEf12m['freq'], DF_PEf12m['StyleName'] = 'M', 'PE_f12m'

# DYf12m
cols = ['BASE_DT', 'TMSRS_CD', 'Code', 'RGN_TP_CD', 'Px0', 'DPS_fy1', 'DY_f12m']
DF_DYf12m = DF_IBES_DPS[cols].copy()
DF_DYf12m['Value_'] = DF_DYf12m.apply(
    lambda x: 100 * x['DPS_fy1'] / x['Px0'] if x['DPS_fy1'] >= 0 else 0, axis=1)

DF_DYf12m.dropna(subset=['Value_'], axis=0, inplace=True)
DF_DYf12m['freq'], DF_DYf12m['StyleName'] = 'M', 'DY_f12m'

# EPS_fwd (Growth)
DF_EPS_fwd = DF_IBES_EPS[
    ['BASE_DT', 'TMSRS_CD', 'Code', 'RGN_TP_CD', 'EPS_fy0', 'EPS_fy1', 'EPS_fy2']
    ].dropna(subset=['EPS_fy0', 'EPS_fy1', 'EPS_fy2']).copy()

DF_EPS_fwd['FY1_0gr'] = DF_EPS_fwd.apply(
    lambda x: 100 * (x['EPS_fy1'] / x['EPS_fy0'] - 1)
    if (x['EPS_fy1'] > 0) & (x['EPS_fy0'] > 0) else np.nan, axis=1
)
DF_EPS_fwd['FY2_1gr'] = DF_EPS_fwd.apply(
    lambda x: 100 * (x['EPS_fy2'] / x['EPS_fy1'] - 1)
    if (x['EPS_fy2'] > 0) & (x['EPS_fy1'] > 0) else np.nan, axis=1
)
DF_EPS_fwd.loc[DF_EPS_fwd['FY1_0gr'].abs() > 100, 'FY1_0gr'] = DF_EPS_fwd.loc[
    DF_EPS_fwd['FY1_0gr'].abs() > 100, 'FY1_0gr'].apply(lambda x: 100 * np.sign(x))
DF_EPS_fwd.loc[DF_EPS_fwd['FY2_1gr'].abs() > 100, 'FY2_1gr'] = DF_EPS_fwd.loc[
    DF_EPS_fwd['FY2_1gr'].abs() > 100, 'FY2_1gr'].apply(lambda x: 100 * np.sign(x))
DF_EPS_fwd = DF_EPS_fwd[DF_EPS_fwd['FY1_0gr'].notnull() | DF_EPS_fwd['FY2_1gr'].notnull()]

DF_EPS_fwd['Value_'] = DF_EPS_fwd.apply(
    lambda x: np.nanmean([x['FY2_1gr'], x['FY1_0gr']]), axis=1)
DF_EPS_fwd.dropna(subset=['Value_'], inplace=True)
DF_EPS_fwd['freq'], DF_EPS_fwd['StyleName'] = 'M', 'EPSg_f2yr'

# ROE_fwd (Current)
DF_ROE_fwd = DF_IBES_ROE[
    ['BASE_DT', 'TMSRS_CD', 'Code', 'RGN_TP_CD', 'ROE_fy0',
    'ROE_fy1', 'ROE_fy2', 'ROE_avg']
    ].dropna(subset=['ROE_avg']).copy()
DF_ROE_fwd.rename(columns={'ROE_avg': 'Value_'}, inplace=True)
DF_ROE_fwd['freq'], DF_ROE_fwd['StyleName'] = 'M', 'ROEavg_f2yr'

# <<GARP Script>>
# EGP = 1 / PEG
A = DF_PEf12m[
    ['BASE_DT', 'TMSRS_CD', 'Code', 'RGN_TP_CD', 'Value_']
    ].rename(columns={'Value_': 'PE_f12m'})
B = DF_EPS_fwd[
    ['BASE_DT', 'TMSRS_CD', 'Code', 'RGN_TP_CD', 'Value_']
    ].rename(columns={'Value_': 'EPS_f2yrGR'})
DF_EGP_tot = pd.merge(A, B, on=['BASE_DT', 'TMSRS_CD', 'Code', 'RGN_TP_CD'])

DF_EGP_tot['Value_'] = DF_EGP_tot['EPS_f2yrGR'] / DF_EGP_tot['PE_f12m']
DF_EGP_tot['freq'], DF_EGP_tot['StyleName'] = 'M', 'EGP_f2yr'

# SGP = SustG / PE_f12m
A = DF_PEf12m[
    ['BASE_DT', 'TMSRS_CD', 'Code', 'RGN_TP_CD', 'Value_']
    ].rename(columns={'Value_': 'PE_f12m'})
B = DF_SustG_tot[
    ['BASE_DT', 'TMSRS_CD', 'Value_']
    ].rename(columns={'Value_': 'SustG'})
DF_SGP_tot = pd.merge(A, B, on=['BASE_DT', 'TMSRS_CD'])

DF_SGP_tot['Value_'] = DF_SGP_tot['SustG'] / DF_SGP_tot['PE_f12m']
DF_SGP_tot['freq'], DF_SGP_tot['StyleName'] = 'M', 'SGP_f2yr'

# Append All Factors
#*------------
firstCheck_duplicates(
    DF_PEf12m, DF_DYf12m, DF_EPS_fwd,
    DF_ROE_fwd, DF_EGP_tot, DF_SGP_tot,
    add_cols=['RGN_TP_CD'])
#*------------
cols = ['BASE_DT', 'TMSRS_CD', 'Code', 'RGN_TP_CD', 'Value_', 'freq', 'StyleName']
#*------------
secondCheck_columns(
    DF_PEf12m[cols], DF_DYf12m[cols], DF_EPS_fwd[cols],
    DF_ROE_fwd[cols], DF_EGP_tot[cols], DF_SGP_tot[cols])
#*------------
DF_GARP = reduce(
    lambda x, y: pd.DataFrame.append(x, y, sort=False),
    [DF_PEf12m[cols], DF_DYf12m[cols], DF_EPS_fwd[cols],
     DF_ROE_fwd[cols], DF_EGP_tot[cols], DF_SGP_tot[cols]])
DF_GARP.reset_index(drop=True, inplace=True)

# Save!
save_batch(bkfil, DF_GARP, mapping, fileName)

from batch_utils.fill_monitor import print_fillReport
out = print_fillReport(bkfil, freq, DF_GARP)