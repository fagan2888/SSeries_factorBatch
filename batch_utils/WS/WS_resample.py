import pandas as pd
import numpy as np

def WS_resample(seq_DT, DT_fin, fill_cols=['FiscalPrd', 'FiscalPrd2',
                                           'CalPrdEndDate', 'Value_', 'FILLyr']):
    # DT_fin[['Item', 'Code', 'BASE_DT', 'FiscalPrd']] = (
    #     DT_fin[['Item', 'Code', 'BASE_DT', 'FiscalPrd']].astype(str)
    # )
    """
    'Item', 'Code', 'BASE_DT', 'FiscalPrd' must be objects
    """
    if DT_fin.shape[0] > 0:
        tmp = DT_fin.groupby(['Item', 'Code', 'FiscalPrd'], as_index=False
                             ).agg({'BASE_DT': min, 'maxUseDate': min})
        tmp['dummy'] = 1
        seq = pd.DataFrame({'BASE_DT0': seq_DT, 'dummy': 1})
        A = pd.merge(seq, tmp, on='dummy')
        A = A[(A['BASE_DT0'] >= A['BASE_DT']) &
              (A['BASE_DT0'] <= A['maxUseDate'])]
        tmp_ = A[['Item', 'Code', 'BASE_DT0']].drop_duplicates(keep='first')
        tmp_ = tmp_.sort_values(['Item', 'Code', 'BASE_DT0']).reset_index(drop=True)
        tmp_ = tmp_.rename(columns={'BASE_DT0': 'BASE_DT'})
        tmp_['FiscalPrd'] = None
        tmp_ = tmp_.append(
            DT_fin[['Item', 'Code', 'BASE_DT', 'FiscalPrd']
                   ].drop_duplicates(keep='first'))
        tmp_ = tmp_.drop_duplicates(['Item', 'Code', 'BASE_DT'], keep='last')
        tmp2 = pd.merge(tmp_, DT_fin,
                        on=['Item', 'Code', 'BASE_DT', 'FiscalPrd'], how='left')
        tmp2 = tmp2.sort_values(['Item', 'Code', 'BASE_DT']).reset_index(drop=True)

        for col in fill_cols:
            tmp2 = tmp2.sort_values(['Item', 'Code', 'BASE_DT'])
            tmp2[col] = tmp2.groupby(['Item', 'Code'])[col].fillna(method='ffill')
        tmp2 = tmp2.sort_values(['Item', 'Code', 'BASE_DT', 'FiscalPrd', 'CalPrdEndDate'])
        tmp2 = tmp2.drop_duplicates(['Item', 'Code', 'BASE_DT'], keep='last')

        tmp3 = tmp2[tmp2['BASE_DT'].isin(seq_DT)]
        if 'maxUseDate' not in fill_cols:
            DT_fin = tmp3.drop(
                'maxUseDate', axis=1).sort_values(['Item', 'Code', 'BASE_DT'])
            DT_fin = DT_fin.reset_index(drop=True)
        else:
            DT_fin = tmp3.sort_values.sort_values(['Item', 'Code', 'BASE_DT'])
            DT_fin = DT_fin.reset_index(drop=True)
    else:
        if 'maxUseDate' not in fill_cols:
            DT_fin = DT_fin.drop('maxUseDate', axis=1)

    return(DT_fin)