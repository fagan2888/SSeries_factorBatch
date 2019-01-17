import pandas as pd
import datetime as dt


def chg_to_type(DF, chg_col=['BASE_DT', 'Code'], type_=str):
    if chg_col is not None:
        tmp = DF[chg_col].dtypes == type_
        chg_col_ = tmp[~tmp]
        if chg_col_.shape[0] > 0:
            for col in chg_col_.index:
                DF[col] = DF[col].astype(type_)
    return DF


def add_mapped_tick(DF, map, on=['Code']):
    DF_ = pd.merge(DF, map, on=on)
    DF_ = DF_[(DF_['BASE_DT'] >= DF_['startDT']) & (DF_['BASE_DT'] <= DF_['endDT'])]
    DF_ = DF_.drop(['startDT', 'endDT'], axis=1)
    return DF_


def chk_count(grp=['BASE_DT', 'StyleName'], *args):
    """
    Count observations by 'grp'
    """
    out_DF = []
    for DF in args:
        tmp = DF.groupby(grp)['TMSRS_CD'].size().reset_index(drop=False)
        tmp = tmp.pivot_table(values='TMSRS_CD', index=grp[0], columns=grp[1:])
        out_DF.append(tmp)

    out = pd.concat(out_DF, join='outer', axis=1)
    return out


# for Mapping old sectors to newly defined sector
_old = [40402010, 40402020, 40402030, 40402035, 40402040,
        40402045, 40402050, 40402060, 40402070, 40403010,
        40403020, 40403030, 40403040]
_new = [60101010, 60101020, 60101010, 60101030, 60101040,
        60101050, 60101060, 60101070, 60101080, 60102010,
        60102020, 60102030, 60102040]
REITS_traceback = {str(x):str(y) for x, y in zip(_old, _new)}
