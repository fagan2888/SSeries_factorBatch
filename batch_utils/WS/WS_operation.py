import numpy as np
import pandas as pd
from functools import reduce


# Operations for Addition, Subtraction, Multiplication, Division for
# Accounting Values

__all__ = ['simple_add',
           'simple_mult',
           'simple_div',
           'simple_subtract',
           'align_add',
           'align_mult',
           'align_div',
           'align_subtract',
           'substitute_Value']


def _simpleOp_Samp(*args):
    Lst_samp = []
    for samp in args:
        Lst_samp.append(samp['Code'])
    intersec_ticks = reduce(np.intersect1d, tuple(Lst_samp))

    Lst_samp = []
    for samp in args:
        Lst_samp.append(samp[samp['Code'].isin(intersec_ticks)])

    col_n = len(Lst_samp)
    if all([Lst_samp[x].shape[x] > 0 for x in range(len(Lst_samp))]):
        tmp_lst = []
        for n, samp in enumerate(Lst_samp):
            if 'FiscalPrd' in samp.columns:
                cols = ['BASE_DT', 'Code', 'Value_', 'FiscalPrd']
                tmp = samp[cols]
                tmp = tmp.rename(columns={'FiscalPrd': 'FiscalPrd_{}'.format(n)})
            else:
                cols = ['BASE_DT', 'Code', 'Value_']
                tmp = samp[cols]
            tmp = tmp.rename(columns={'Value_': 'Value_{}'.format(n)})
            tmp_lst.append(tmp)

        def my_merge(df1, df2):
            res = pd.merge(df1, df2, on=['BASE_DT', 'Code'])
            return res

        tmp_AB = reduce(my_merge, tmp_lst)

    return col_n, tmp_AB


def simple_add(*args, allow_null=False):
    col_n, tmp_AB = _simpleOp_Samp(*args)
    tmp_AB['Value_'] = tmp_AB['Value_0']
    for n in range(1, col_n):
        tmp_AB['Value_'] += tmp_AB['Value_{}'.format(n)]
    if not allow_null:
        DT_fin = tmp_AB[tmp_AB['Value_'].notnull()]
    else:
        DT_fin = tmp_AB

    return DT_fin


def simple_mult(*args, allow_null=False):
    col_n, tmp_AB = _simpleOp_Samp(*args)
    tmp_AB['Value_'] = tmp_AB['Value_0']
    for n in range(1, col_n):
        tmp_AB['Value_'] *= tmp_AB['Value_{}'.format(n)]
    if not allow_null:
        DT_fin = tmp_AB[tmp_AB['Value_'].notnull()]
    else:
        DT_fin = tmp_AB

    return DT_fin


def simple_div(*args, setInf=np.nan, allow_null=False):
    col_n, tmp_AB = _simpleOp_Samp(*args)
    tmp_AB['Value_'] = tmp_AB['Value_0']
    for n in range(1, col_n):
        tmp_AB['Value_'] /= tmp_AB['Value_{}'.format(n)]

    tmp_AB.loc[abs(tmp_AB['Value_']) == np.inf, 'Value_'] = setInf
    if not allow_null:
        DT_fin = tmp_AB[tmp_AB['Value_'].notnull()]
    else:
        DT_fin = tmp_AB

    return DT_fin


def simple_subtract(*args, allow_null=False):
    col_n, tmp_AB = _simpleOp_Samp(*args)
    tmp_AB['Value_'] = tmp_AB['Value_0']
    for n in range(1, col_n):
        tmp_AB['Value_'] -= tmp_AB['Value_{}'.format(n)]
    if not allow_null:
        DT_fin = tmp_AB[tmp_AB['Value_'].notnull()]
    else:
        DT_fin = tmp_AB

    return DT_fin


def _alignOp_Samp(*args):
    """
    Get DataFrame ready to do
    operations(Add, Multiply, Divide, Subtract) for Creating Financial Ratios
    """
    Lst_samp = []
    for samp in args:
        Lst_samp.append(samp['Code'])
    intersec_ticks = reduce(np.intersect1d, tuple(Lst_samp))

    Lst_samp = []
    for samp in args:
        Lst_samp.append(samp[samp['Code'].isin(intersec_ticks)])

    if all([Lst_samp[x].shape[0] > 0 for x in range(len(Lst_samp))]):
        tmp_lst = []
        for n, samp in enumerate(Lst_samp):
            tmp = samp[['BASE_DT', 'Code', 'FiscalPrd']]
            tmp = tmp.rename(columns={'FiscalPrd': 'FiscalPrd_{}'.format(n)})
            tmp_lst.append(tmp)

        def my_merge(df1, df2):
            res = pd.merge(df1, df2, on=['BASE_DT', 'Code'])
            return res

        tmp_AB = reduce(my_merge, tmp_lst)
    else:
        cols = ['BASE_DT', 'Code']
        cols += ['FiscalPrd_{}'.format(n) for n in range(len(Lst_samp))]
        tmp_AB = pd.DataFrame(columns=cols)

    if tmp_AB.shape[0] > 0:
        tmp_AB_ = tmp_AB.melt(id_vars=['BASE_DT', 'Code'],
                              value_vars=['FiscalPrd_{}'.format(n) for n in range(len(Lst_samp))],
                              value_name='FiscalPrd')
        tmp_AB_['FiscalPrd'] = tmp_AB_['FiscalPrd'].astype(int)
        tmp_AB_ = tmp_AB_.groupby(['BASE_DT', 'Code'],
                                  as_index=False)['FiscalPrd'].min()
        tmp_AB_['FiscalPrd'] = tmp_AB_['FiscalPrd'].astype(str)

        Lst_samp_ = Lst_samp.copy()
        if all(['ref' in x.columns for x in Lst_samp_]):
            selec = 0
            cols = ['BASE_DT', 'Code', 'FiscalPrd', 'Value_', 'ref']

            def f(n):
                return {'Value_': 'Value_{}'.format(n), 'ref': 'ref_{}'.format(n)}
        else:
            selec = 1
            cols = ['BASE_DT', 'Code', 'FiscalPrd', 'Value_']

            def f(n):
                return {'Value_': 'Value_{}'.format(n)}

        for n, samp in enumerate(Lst_samp_):
            Lst_samp_[n] = samp[cols]
            Lst_samp_[n] = Lst_samp_[n].rename(columns=f(n))

        def my_merge(df1, df2):
            res = pd.merge(df1, df2, on=['Code', 'BASE_DT', 'FiscalPrd'], how='left')
            return res

        tmp_base = reduce(my_merge, [tmp_AB_] + Lst_samp_)

        op_cols = ['Value_{}'.format(n) for n in range(len(Lst_samp_))]
        if selec == 0:
            cols = ['ref_{}'.format(n) for n in range(len(Lst_samp_))]
            for col in cols:
                tmp_base = tmp_base.sort_values(['Code', 'BASE_DT'])
                tmp_base[col] = tmp_base.groupby('Code')[col].fillna(method='ffill')
            tmp_base['ref'] = tmp_base[cols[0]]
            for col in cols[1:]:
                tmp_base['ref'] += '|' + tmp_base[col]
            tmp_base = tmp_base.drop(cols, axis=1)

            for col in op_cols:
                tmp_base = tmp_base.sort_values(['Code', 'BASE_DT'])
                tmp_base[col] = tmp_base.groupby('Code')[col].fillna(method='ffill')
        else:
            for col in op_cols:
                tmp_base = tmp_base.sort_values(['Code', 'BASE_DT'])
                tmp_base[col] = tmp_base.groupby('Code')[col].fillna(method='ffill')
    else:
        cols = ['BASE_DT', 'Code', 'FiscalPrd']
        op_cols = ['Value_{}'.format(n) for n in range(len(Lst_samp))]
        cols += op_cols
        if all(['ref' in x.columns for x in Lst_samp]):
            cols += ['ref']
        tmp_base = pd.DataFrame(columns=cols)

    return op_cols, tmp_base


def align_add(*args, allow_null=False):
    cols, tmp_base = _alignOp_Samp(*args)
    tmp_base['Value_'] = tmp_base[cols[0]]
    for col in cols[1:]:
        tmp_base['Value_'] += tmp_base[col]
    if not allow_null:
        DT_fin = tmp_base[tmp_base['Value_'].notnull()]
    else:
        DT_fin = tmp_base

    return DT_fin


def align_mult(*args, allow_null=False):
    cols, tmp_base = _alignOp_Samp(*args)
    tmp_base['Value_'] = tmp_base[cols[0]]
    for col in cols[1:]:
        tmp_base['Value_'] *= tmp_base[col]
    if not allow_null:
        DT_fin = tmp_base[tmp_base['Value_'].notnull()]
    else:
        DT_fin = tmp_base

    return DT_fin


def align_div(*args, setInf=np.nan, allow_null=False):
    cols, tmp_base = _alignOp_Samp(*args)
    tmp_base['Value_'] = tmp_base[cols[0]]
    for col in cols[1:]:
        tmp_base['Value_'] /= tmp_base[col]

    tmp_base.loc[abs(tmp_base['Value_']) == np.inf, 'Value_'] = setInf
    if not allow_null:
        DT_fin = tmp_base[tmp_base['Value_'].notnull()]
    else:
        DT_fin = tmp_base

    return DT_fin


def align_subtract(*args, allow_null=False):
    cols, tmp_base = _alignOp_Samp(*args)
    tmp_base['Value_'] = tmp_base[cols[0]]
    for col in cols[1:]:
        tmp_base['Value_'] -= tmp_base[col]
    if not allow_null:
        DT_fin = tmp_base[tmp_base['Value_'].notnull()]
    else:
        DT_fin = tmp_base

    return DT_fin


def substitute_Value(DF_yr, DF_qtr, val_col='Value_'):
    """
    Preferrence for Quarter Data : Combines two DataFrame into one.
    """
    cols = ['BASE_DT', 'Code', val_col]
    if all(['ref' in DF_yr.columns, 'ref' in DF_qtr.columns]):
        cols += ['ref']
        DF_yr = DF_yr.rename(columns={'ref', 'ref_yr'})
        DF_qtr = DF_qtr.rename(columns={'ref', 'ref_qtr'})
        selec = 1
    else:
        selec = 0

    DF_yr = DF_yr.rename(columns={val_col: 'Value_yr'})
    DF_qtr = DF_qtr.rename(columns={val_col: 'Value_qtr'})
    cols_yr = [x if x != val_col else 'Value_yr' for x in cols]
    cols_qtr = [x if x != val_col else 'Value_qtr' for x in cols]
    cols_yr = [x if x != 'ref' else x + '_yr' for x in cols_yr]
    cols_qtr = [x if x != 'ref' else x + '_qtr' for x in cols_qtr]

    tmp_DF = pd.merge(DF_yr[cols_yr], DF_qtr[cols_qtr],
                      on=['BASE_DT', 'Code'], how='outer')
    # tmp_DF.assign(Value_=np.where(tmp_DF['Value_qtr'].isnull(),
    #                               tmp_DF['Value_yr'], tmp_DF['Value_qtr']))
    tmp_DF['Value_'], tmp_DF['freq'] = tmp_DF['Value_qtr'], 'Q'
    tmp_DF.loc[tmp_DF['Value_qtr'].isnull(), 'Value_'] = tmp_DF['Value_yr']
    tmp_DF.loc[tmp_DF['Value_qtr'].isnull(), 'freq'] = 'Y'

    if selec == 1:
        tmp_DF['ref'] = tmp_DF['ref_qtr']
        tmp_DF.loc[tmp_DF['freq'] == 'Y', 'ref'] = tmp_DF['ref_yr']
        tmp_DF.drop(['ref_yr', 'ref_qtr'], axis=1)
    elif selec == 0:
        tmp_DF['ref'] = None

    return tmp_DF