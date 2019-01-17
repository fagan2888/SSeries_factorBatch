import os
import numpy as np
import pandas as pd
# option = 'batch'
# freq = 'W'
import sys
option = sys.argv[1]
freq = sys.argv[2]

from batch_utils.utils_dateSeq import batch_sequence
bkfil, rtvStart, seq_DT = batch_sequence(option, freq)

tgt_folder = 'save_{}'.format('total' if bkfil else 'batch')
print(
    "# Checking Files in '{}' Folder".format(tgt_folder)
)

chk_files = [
    'IBES_refIBES_DPS.p',
    'IBES_refIBES_EPS.p',
    'IBES_refIBES_Payout.p',
    'IBES_refIBES_ROE.p',
    'worldscope_5yrRel_CFO2EV.p',
    'worldscope_5yrRel_CFO2px.p',
    'worldscope_5yrRel_EBITDA2px.p',
    'worldscope_Accruals.p',
    'worldscope_ARDays.p',
    'worldscope_CAcqR.p',
    'worldscope_EVEbitda.p',
    'worldscope_FCFROIC.p',
    'worldscope_GPOA.p',
    'worldscope_IntC.p',
    'worldscope_LTDebt.p',
    'worldscope_Margin.p',
    'worldscope_OpLev.p',
    'worldscope_Ratios.p',
    'worldscope_ROIC.p',
    'worldscope_SustG_EPSg.p',
    'worldscope_SustG_ROEnPayout.p',
    'worldscope_Value.p',
    'comb_SustG.p',
    'comb_GARP.pcc'
]

have_set = set(os.listdir(tgt_folder))
tgt_set = set(chk_files)
if have_set == tgt_set:
    print('ALL FILES EXIST! Good to Upload')
elif have_set.issubset(tgt_set):
    diff_ = list(tgt_set.difference(have_set))
    print('These are NOT FOUND!:')
    print('{}'.format(diff_))
else:
    diff_ = list(have_set.difference(tgt_set))
    print('Unnecessary files are FOUND! PLZ REMOVE:')
    print('{}'.format(diff_))