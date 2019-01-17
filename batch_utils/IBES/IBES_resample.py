import numpy as np
import pandas as pd


def IBES_resample(seq_DT, DT, fill_cols, drop_maxCol=True, **kwargs):
    if fill_cols is None:
        fill_cols = ['FY0', 'Px0', 'ActValue0', 'FY1', 'Median1', 'FY2',
                     'Median2', 'FY3', 'Median3',
                     'EPS_fy0', 'EPS_fy1', 'EPS_fy2', 'EPSg_2yr', 'Ratio_f12m']
    defaults = {'base_Col': 'BASE_DT', 'tick_Col': 'Code',
                'max_Col': 'maxUseDate', 'reg_Col': 'RGN_TP_CD'}
    defaults.update(kwargs)
    base_Col = defaults['base_Col']
    tick_Col = defaults['tick_Col']
    max_Col = defaults['max_Col']
    reg_Col = defaults['reg_Col']

    if DT.shape[0] > 0:
        tmp = DT[[reg_Col, tick_Col, base_Col, max_Col]].drop_duplicates().copy()
        tmp['dummy'] = 1
        A = pd.merge(pd.DataFrame({'BASE_DT0': seq_DT, 'dummy': 1}), tmp, on='dummy')
        A = A[(A['BASE_DT0'] >= A[base_Col]) &
              (A['BASE_DT0'] <= A['maxUseDate'])]
        tmp_ = A[[reg_Col, tick_Col, 'BASE_DT0']].drop_duplicates(keep='first')
        tmp_.rename(columns={'BASE_DT0': base_Col}, inplace=True)

        tmp1 = tmp_.append(
            DT[[reg_Col, tick_Col, base_Col]].drop_duplicates().copy())
        tmp1.drop_duplicates(
            subset=[reg_Col, tick_Col, base_Col], keep='first', inplace=True)
        tmp2 = pd.merge(tmp1, DT, on=[reg_Col, tick_Col, base_Col], how='left')
        tmp2.sort_values([reg_Col, tick_Col, base_Col], inplace=True)
        for col in fill_cols:
            tmp2[col] = tmp2.groupby([reg_Col, tick_Col])[col].fillna(method='ffill')
        tmp2.dropna(subset=fill_cols, axis=0, inplace=True)
        tmp3 = tmp2[tmp2[base_Col].isin(seq_DT)].copy()
        if drop_maxCol:
            tmp3.drop(max_Col, axis=1, inplace=True)
    else:
        tmp3 = pd.DataFrame(columns=DT.columns)
        if drop_maxCol:
            tmp3.drop(max_Col, axis=1, inplace=True)

    return tmp3.reset_index(drop=True)