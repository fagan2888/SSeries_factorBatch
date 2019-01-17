import numpy as np
import pandas as pd
from functools import reduce

def get_HistAvg(DF, k=2, buffer=(2 / 3)):
    """
    gets 'k' consecutive mean of FiscalPrd Value_
    (Used in WS_SustG_2fin_SustG)
    """
    tmp = DF[['Item', 'Code', 'FiscalPrd']].drop_duplicates()
    tmp['dummy'] = 1
    tmp_dummy = DF.groupby(['Item', 'Code'], as_index=False)[
        'FiscalPrd'].min().rename(columns={'FiscalPrd': 'min_dummy'})
    tmp0 = pd.DataFrame({'dummy': 1, 'sub': list(range(k))})

    A_ = pd.merge(tmp, tmp0, on='dummy')
    A_['FiscalPrd_'] = (A_['FiscalPrd'].astype(int) - A_['sub']).astype(str)
    A_.drop(['dummy', 'sub'], axis=1, inplace=True)
    DF_ = pd.merge(DF, A_, on=['Item', 'Code', 'FiscalPrd'])
    DF_tmp = DF[['Item', 'Code', 'BASE_DT', 'FiscalPrd', 'Value_']
                ].rename(columns={'BASE_DT': 'BASE_DT_', 'FiscalPrd': 'FiscalPrd_',
                                  'Value_': 'prev_Val'})
    workDF = pd.merge(DF_, DF_tmp, on=['Item', 'Code', 'FiscalPrd_'])
    workDF = workDF[workDF['BASE_DT'] >= workDF['BASE_DT_']]
    workDF['dummy'] = workDF.groupby(['Item', 'Code', 'FiscalPrd_', 'BASE_DT']
                                     )['BASE_DT_'].transform(max)
    workDF_ = workDF[workDF['BASE_DT_'] == workDF['dummy']].copy()
    # workDF_['CNT'] = workDF_.groupby(['Item', 'Code', 'BASE_DT', 'FiscalPrd'])[
    #     'FiscalPrd_'].transform(len)
    colby = ['Item', 'Code', 'BASE_DT', 'FiscalPrd']
    tmp = workDF_.groupby(colby, as_index=False)[
        'FiscalPrd_'].count().rename(columns={'FiscalPrd_': 'CNT'})
    workDF_ = pd.merge(workDF_, tmp, on=colby)

    workDF_ = pd.merge(workDF_, tmp_dummy, on=['Item', 'Code'], how='left')
    workDF_ = workDF_[
        workDF_['FiscalPrd'].astype(int) >= workDF_['min_dummy'].astype(int) + k - 1]
    cols = ['Item', 'Code', 'BASE_DT', 'FiscalPrd', 'FiscalPrd2',
            'CalPrdEndDate', 'FILLyr', 'maxUseDate']
    call_Cols = np.intersect1d(workDF_.columns, cols).tolist()
    TMP = workDF_[workDF_['CNT'] >= round(k * buffer)]
    mean_DF = TMP.groupby(call_Cols, as_index=False)[
        'prev_Val'].mean().rename(columns={'prev_Val': 'Value_'})
    return mean_DF


def get_HistChgAvg(DF, k=2, k2=1, buffer=(2 / 3), growth=False):
    """
    gets 'k' consecutive mean of 'k2' FiscalPrd difference/return in Value_
    (Used in WS_f0_Margin, EPS growth[WS_SustG_2fin])
    """
    if any(DF.columns.isin(['Item'])):
        add_col = ['Item']
    else:
        add_col = []
    tmp = DF[add_col + ['Code', 'FiscalPrd']].drop_duplicates()
    tmp['dummy'] = 1
    tmp_dummy = DF.groupby(add_col + ['Code'], as_index=False)[
        'FiscalPrd'].min().rename(columns={'FiscalPrd': 'min_dummy'})
    tmp0 = pd.DataFrame({'dummy': 1, 'sub': list(range(k))})
    tmp0['sub2'] = k2

    A_ = pd.merge(tmp, tmp0, on='dummy')
    A_['FiscalPrd_1'] = (A_['FiscalPrd'].astype(int) - A_['sub']).astype(str)
    A_['FiscalPrd_0'] = (A_['FiscalPrd'].astype(int) -
                         A_['sub'] - A_['sub2']).astype(str)
    A_.drop(['dummy', 'sub', 'sub2'], axis=1, inplace=True)

    elim_cols = DF.columns[DF.columns.str.contains('^Value_')].tolist()
    WK1 = pd.merge(A_, DF.drop(elim_cols, axis=1),
                   on=(add_col + ['Code', 'FiscalPrd']))
    DF_tmp = DF[add_col + ['Code', 'BASE_DT', 'FiscalPrd', 'Value_']].rename(
        columns={'BASE_DT': 'BASE_DT_1', 'FiscalPrd': 'FiscalPrd_1',
                 'Value_': 'Value_1'})
    WK1 = pd.merge(WK1, DF_tmp, on=(add_col + ['Code', 'FiscalPrd_1']))
    WK1 = WK1[WK1['BASE_DT'] >= WK1['BASE_DT_1']]
    # WK1['dummy'] = WK1.groupby(['Item', 'Code', 'FiscalPrd_0', 'BASE_DT'])['BASE_DT_1'].transform(max)
    WK1 = WK1.sort_values('BASE_DT_1')
    WK1 = WK1.drop_duplicates(
        subset=(add_col + ['Code', 'FiscalPrd_0', 'BASE_DT']), keep='last')
    WK1.drop('BASE_DT_1', axis=1, inplace=True)
    # WK1 = WK1[WK1['BASE_DT_1']==WK1['dummy']]
    # WK1.drop(['BASE_DT_1', 'dummy'], axis=1, inplace=True)

    DF_tmp = DF[add_col + ['Code', 'BASE_DT', 'FiscalPrd', 'Value_']].rename(
        columns={'BASE_DT': 'BASE_DT_0', 'FiscalPrd': 'FiscalPrd_0',
                 'Value_': 'Value_0'})
    WK1 = pd.merge(WK1, DF_tmp, on=(add_col + ['Code', 'FiscalPrd_0']))
    WK1 = WK1[WK1['BASE_DT'] >= WK1['BASE_DT_0']]
    # WK1['dummy'] = WK1.groupby(['Item', 'Code', 'FiscalPrd_0', 'BASE_DT'])['BASE_DT_0'].transform(max)
    WK1 = WK1.sort_values('BASE_DT_0')
    WK1 = WK1.drop_duplicates(
        subset=(add_col + ['Code', 'FiscalPrd_0', 'BASE_DT']), keep='last')
    WK1.drop(['BASE_DT_0'], axis=1, inplace=True)
    # WK1 = WK1[WK1['BASE_DT_0']==WK1['dummy']]
    # WK1.drop(['BASE_DT_0', 'dummy'], axis=1, inplace=True)

    # - Count Different FiscalPrd for each date
    # WK1['CNT'] = WK1.groupby(['Item', 'Code', 'BASE_DT', 'FiscalPrd'])['FiscalPrd_0'].transform(len)
    colby = add_col + ['Code', 'BASE_DT', 'FiscalPrd']
    tmp = WK1.groupby(colby, as_index=False)[
        'FiscalPrd_0'].count().rename(columns={'FiscalPrd_0': 'CNT'})
    WK1 = pd.merge(WK1, tmp, on=colby)

    # - Operation for Value_
    WK1 = pd.merge(WK1, tmp_dummy, on=(add_col + ['Code']), how='left')
    WK1 = WK1[WK1['FiscalPrd'].astype(int) >= WK1['min_dummy'].astype(int) + k]
    if growth:
        WK1['Value_'] = WK1.apply(
            lambda x: 100 * (x['Value_1'] / x['Value_0'] - 1) if (x['Value_1'] >= 0) & (x['Value_0'] > 0) else np.nan, axis=1)
        WK1.loc[WK1['Value_'] > 100, 'Value_'] = 100
    else:
        WK1['Value_'] = WK1['Value_1'] - WK1['Value_0']
    # WK1['Value_'] = WK1.apply(
    #     lambda x: (np.nan if x['Value_1'] < 0 else x['Value_1']) -
    #     (np.nan if x['Value_0'] < 0 else x['Value_0']), axis=1)
    WK1['Value_'] = WK1['Value_'].apply(
        lambda x: np.sign(x) * 100 if np.abs(x) > 100 else x)
    WK1 = WK1[WK1['Value_'].notnull()]

    # - Additional Checkup: Count (non-NULL) Values for each date
    # WK1['CNT2'] = WK1.groupby(['Item', 'Code', 'BASE_DT', 'FiscalPrd'])['Value_'].transform(len)
    colby = add_col + ['Code', 'BASE_DT', 'FiscalPrd']
    tmp = WK1.groupby(colby, as_index=False)[
        'Value_'].count().rename(columns={'Value_': 'CNT2'})
    WK1 = pd.merge(WK1, tmp, on=colby)

    # - Aggregate (take mean) of consecutive prev. values for final Value_
    callCols = WK1.columns[WK1.columns.isin(
        add_col + ['Code', 'BASE_DT', 'FiscalPrd', 'FiscalPrd2',
                   'CalPrdEndDate', 'maxUseDate'])].tolist()
    DF_tmp = WK1[(WK1['CNT'] >= round(k * buffer)) &
                 (WK1['CNT2'] >= round(k * buffer))]
    mean_DF = DF_tmp.groupby(callCols, as_index=False)['Value_'].mean()

    dummy = DF[add_col + ['BASE_DT', 'Code', 'FiscalPrd'] + elim_cols].rename(
        columns={'Value_': 'Ratio'})
    mean_DF = pd.merge(
        mean_DF, dummy, on=(add_col + ['Code', 'BASE_DT', 'FiscalPrd']),
        how='left')

    return mean_DF