import numpy as np
import pandas as pd
import datetime as dt

#= Z-score for DateTime Period Range ================================
def rng_operator(subDF, back, buffer):
    """
    Find Mean, StDev for 'back' period range from BASE_DT
    """
    def f(x):
        out = TMP['Value_'][(TMP['curr'].astype(float) <= float(x)) &
                            (TMP['curr'].astype(float) >= float(x) - pd.DateOffset(years=back, days=2))]
        return out.agg({'CNT': len, 'Avg': np.mean, 'Std': np.std})

    TMP = subDF.copy()
    TMP['curr'] = TMP['BASE_DT'].apply(lambda x: dt.datetime.strptime(x, '%Y%m%d'))
    TMP = pd.concat([TMP, TMP['curr'].apply(f)], axis=1)
    TMP = TMP.drop('curr', axis=1)
    TMP = TMP[TMP['CNT'] >= int(back * 12 * buffer)]
    return TMP


def Conv_Historical_Val(subDF, group_col='Code', back_yr=5, buffer_mth=0.69):
    grouped = subDF.groupby(group_col)
    OUT = pd.DataFrame()
    for _, subDF_ in grouped:
        OUT_ = rng_operator(subDF_, back_yr, buffer_mth)
        OUT = OUT.append(OUT_)
        #print(i, Code)

    OUT['zValue_'] = (OUT['Value_'] - OUT['Avg']) / OUT['Std']
    OUT = OUT[~np.isclose(OUT['Std'], 0)]
    return OUT
#================================================================

#= Z-score for DateTime Period Range ================================
def rng_operator3(subDF, back, buffer, freq_, bkfil):
    """
    Find Mean, StDev for 'back' period range from BASE_DT
    """
    def f(x):
        out = TMP.loc[(TMP['curr'] <= x) &
                      (TMP['curr'] >= x - pd.DateOffset(years=back, days=2)), 'Value_']
        return out.agg({'CNT': len, 'Avg': np.mean, 'Std': np.std})

    TMP = subDF.copy()
    TMP['curr'] = TMP['BASE_DT'].apply(lambda x: dt.datetime.strptime(x, '%Y%m%d'))
    if not bkfil:
        TMP_ = TMP[TMP['BASE_DT'].isin(TMP['BASE_DT'].unique()[-10:])].copy()
    else:
        TMP_ = TMP.copy()
    TMP = pd.concat([TMP, TMP_['curr'].apply(f)], axis=1)
    TMP = TMP.drop('curr', axis=1)
    TMP = TMP[TMP['CNT'] >= int(back * freq_ * buffer)]
    return TMP


def Conv_Historical_Val3(subDF, group_col='Code', back_yr=5, buffer_mth=0.69, freq='M', bkfil=True):
    if freq == 'M':
        freq_ = 12
    elif freq == 'W':
        freq_ = 52
    elif freq == 'D':
        freq_ = 365
    else:
        raise ValueError('freq should be M, W, or D')

    grouped = subDF.groupby(group_col)
    OUT = pd.DataFrame()
    for _, subDF_ in grouped:
        OUT_ = rng_operator3(subDF_, back_yr, buffer_mth, freq_, bkfil)
        OUT = OUT.append(OUT_, sort=False)
        #print(i, Code)

    OUT['zValue_'] = (OUT['Value_'] - OUT['Avg']) / OUT['Std']
    OUT = OUT[~np.isclose(OUT['Std'], 0)]
    return OUT
#================================================================


#= Z-score for DateTime Period Range2 ================================
def rng_operator2(subDF_, back, buffer):
    """
    Find Mean, StDev for 'back' period range from FiscalPrd
    """
    def p(x, back=back):
        cols = ['BASE_DT', 'FiscalPrd', 'Value_']
        out = subDF.loc[(subDF['FiscalPrd'].astype(int) <= int(x)) &
                        (subDF['FiscalPrd'].astype(int) >= int(x) - back), cols]
        out.drop_duplicates(subset=['FiscalPrd'], keep='last', inplace=True)
        return out['Value_'].agg({'CNT': len, 'Avg': np.mean, 'Std': np.std})

    subDF = subDF_.copy()
    subDF.sort_values(['FiscalPrd', 'BASE_DT'], inplace=True)

    subDF = pd.concat([subDF, subDF['FiscalPrd'].apply(p)], axis=1)
    subDF = subDF[subDF['CNT'] >= int(back * buffer)]

    return subDF


def Conv_Historical_Val2(subDF, group_col='Code', back=5, buffer=0.8):
    grouped = subDF.groupby(group_col)
    OUT = pd.DataFrame()
    for _, subDF_ in grouped:
        OUT_ = rng_operator2(subDF_, back, buffer)
        OUT = OUT.append(OUT_)
    if OUT.shape[0] > 0:
        OUT['zValue_'] = (OUT['Value_'] - OUT['Avg']) / OUT['Std']
        OUT = OUT[~np.isclose(OUT['Std'], 0)]
    else:
        OUT = pd.DataFrame(columns=(subDF.columns.tolist() +
                                    ['CNT', 'Avg', 'Std', 'zValue_']))
    return OUT


def Conv_Historical_Val2_speed(subDF, group_col='Code', back=5, buffer=0.8):
    key_col = ['Code', 'BASE_DT']
    tmp = subDF.groupby(['Code', 'Value_'], as_index=False)['BASE_DT'].min()
    subDF_ = pd.merge(tmp[key_col], subDF, on=key_col, how='left')
    subDF_ = Conv_Historical_Val2(subDF_, group_col=group_col, back=back, buffer=buffer)
    subDF_ = pd.merge(subDF[key_col], subDF_, on=key_col, how='left')
    subDF_.sort_values(['Code', 'BASE_DT'], inplace=True)
    subDF_2 = subDF_.groupby('Code', as_index=False).fillna(method='ffill', axis=0).dropna(0)
    return subDF_2


def _find_n_mod_error(A):
    A = A.copy()
    if A.shape[0] < 7:
        return A
    else:
        tmp0 = (A['Value_'] / A['Value_'].shift(-1) - 1).abs().iloc[:-1]
        verif0 = (tmp0 - tmp0.mean()) / tmp0.std()
        verif0_ = verif0[verif0 > 1.5].index + 1
        tmp1 = (A['Value_'] / A['Value_'].shift(1) - 1).abs().iloc[1:]
        verif1 = (tmp1 - tmp1.mean()) / tmp1.std()
        verif1_ = verif1[verif1 > 1.5].index - 1

        err_idx = np.intersect1d(verif0_, verif1_)
        for err_loc in err_idx:
            A.loc[err_loc, 'Value_'] = (A.shift(-1).loc[err_loc, 'Value_'] + A.shift(1).loc[err_loc, 'Value_']) / 2
        return A


def find_n_mod_error(DF, group_col=['Code', 'Item']):
    DF = DF.sort_values(['Code', 'Item', 'BASE_DT']).reset_index(drop=True)
    grouped = DF.groupby(group_col)
    TMP = pd.DataFrame()
    for _, group in grouped:
        TMP = TMP.append(_find_n_mod_error(group))
    return TMP