
import pyodbc
import pandas as pd
import numpy as np

from .utils_db_alch2 import connectDB
from .common import chg_to_type
from .utils_sql import create_typeStr

# EJ_WRK_FACTORS3 (typeStr)
S0 = pd.Series({
    'BASE_DT': 'varchar(8)', 'StyleName': 'varchar(25)',
    'TMSRS_CD': 'varchar(10)',
    'Code': 'varchar(10)', 'CD_ref': 'varchar(1)',
    'Value_': 'float', 'Value_adj': 'float',
    'adj_op': 'varchar(10)', 'freq': 'varchar(6)', 'ref': 'varchar(10)',
    'ref_SEDOL': 'varchar(6)', 'ref_Name': 'varchar(50)', 'ref_CTRY': 'varchar(3)',
    'REG_DTTM': 'datetime', 'REG_EMP_NMB': 'varchar(10)',
    'LST_DTTM': 'datetime', 'LST_EMP_NMB': 'varchar(10)'
})
S1 = pd.Series({
    'BASE_DT': 1, 'StyleName': 1, 'TMSRS_CD': 1,
    'Code': 0, 'CD_ref': 0,
    'Value_': 1, 'Value_adj': 1,
    'adj_op': 1, 'freq': 0, 'ref': 0,
    'ref_SEDOL': 0, 'ref_Name': 0, 'ref_CTRY': 0,
    'REG_DTTM': 1, 'REG_EMP_NMB': 1,
    'LST_DTTM': 1, 'LST_EMP_NMB': 1
})
primary = ['BASE_DT', 'TMSRS_CD', 'StyleName']
typeStr = create_typeStr(S0, S1, primary)


def get_refInformation(conn):
    """
    gets Country, Name, and Sedol from QAD master-map
    for reference only information about uploaded factors
    Should NOT be used for accurate 'historical' record keeping
    """
    Sql_S = """
    Select a.VenCode as TMSRS_CD,
        convert(varchar(8), a.StartDate, 112) as startDT,
        convert(varchar(8), a.EndDate, 112) as endDT,
        b.Sedol as ref_SEDOL,
        b.Name as ref_Name,
        b.Country as ref_CTRY
    from SecMapX a with (nolock)
    left outer join SecMstrX b with (nolock)
        on a.SecCode = b.SecCode
    where a.VenType=7
    and b.Type_=1
    UNION
    Select a.VenCode as TMSRS_CD,
        convert(varchar(8), a.StartDate, 112) as startDT,
        convert(varchar(8), a.EndDate, 112) as endDT,
        b.Sedol as ref_SEDOL,
        b.Name as ref_Name,
        b.Country as ref_CTRY
    from GSecMapX a with (nolock)
    left outer join GSecMstrX b with (nolock)
        on a.SecCode = b.SecCode
    where a.VenType=7
    and b.Type_=10
    """
    conn = connectDB('MSSQL_QAD')

    secInf = pd.read_sql(Sql_S, conn)
    conn.close()

    secInf = chg_to_type(secInf, chg_col=secInf.columns.tolist(), type_=str)
    secInf.sort_values(['TMSRS_CD', 'startDT'], inplace=True)
    secInf.drop_duplicates(subset=['TMSRS_CD'], keep='last', inplace=True)
    secInf.drop(['startDT', 'endDT'], axis=1, inplace=True)
    return secInf



def check_maxDate(conn, db_name, styleName):
    """
    check maxDate if record exists. Otherwise, returns 19900000
    """
    c = conn.cursor()
    Sql_S = """
    Select top 1 * from {} with (nolock) where StyleName='{}'
    """.format(db_name, styleName)
    c.execute(Sql_S)
    out0 = c.fetchall()
    if len(out0) == 0:
        morethan_ = str('19900000')
    else:
        Sql_S = """
        Select max(BASE_DT) from {} with (nolock) where StyleName='{}'
        """.format(db_name, styleName)
        c.execute(Sql_S)
        morethan_ = str(c.fetchval())
    return morethan_


def winsor_medZ4(x):
    """
    attempted for being used on daily sample
    winsorize over z-score(by median) at 4
    """
    x_ = x.values
    me_ = np.median(x_)
    st_ = (((x_ - me_)**2).sum() / x_.shape[0]) ** (1/2)
    zsr_ = (x_ - me_) / st_

    x_[zsr_ > 4] = 4 * st_ + me_
    x_[zsr_ < -4] = -4 * st_ + me_
    return pd.Series(x_, index=x.index)


def adjScore_raw(arr, how=None):
    """
    adjusts raw data to score-analysis-ready dataset
    """
    arr = arr.astype(float)
    arr = np.ma.array(arr, mask=np.isnan(arr))

    def _how(out, how):
        if how is 'r':  # reciprocal
            out = 100 / out
            return out
        elif how is 'm':  # minus
            out = -1 * out
            return out
        elif how is 'l':  # log
            out = np.log(out)
            return out
        elif how == 'nl':  # log negative
            out = np.log(out) * (-1)
            return out
        elif how is 'c':  # copy
            return out
        else:
            return out

    return _how(arr, how)


def adjScore_DFValue_(DF, adj_, copy=False):
    if copy:
        DF = DF.copy()
    
    DF['adj_op'] = adj_
    
    if adj_ in ['l', 'nl']:
        except_ = DF['Value_'] <= 0
        debug = DF[except_]
        DF = DF[~except_] # greater than zero
    elif adj_ in ['r']:
        # elim zeros or zero-like
        except_ = np.isclose(DF['Value_'], 0)
        debug = DF[except_]
        DF = DF[~except_] # not zero
    else:
        debug = pd.DataFrame(columns=DF.columns)

    DF['Value_adj'] = adjScore_raw(DF['Value_'], how=adj_)
    debug['Value_adj'] = np.nan
    
    return DF, debug






# def get_msciInf_batch(dt_seq, verbose=False):
#     DF = pd.DataFrame({'BASE_DT': dt_seq, 'dummy': 1})
#     # For Insertion
#     S0 = pd.Series({'BASE_DT': 'varchar(8)', 'dummy': 'int'})
#     S1 = pd.Series({'BASE_DT': 1, 'dummy': 1})
#     primary = ['BASE_DT', 'dummy']
#     typeStr = create_typeStr(S0, S1, primary)
#     db_name = '#TEMP'

#     conn = connectDB('MSSQL_RDW')
#     create_table(conn, db_name, DF, typeStr, primary)
#     update_table(conn, db_name, DF, typeStr, verbose=verbose)

#     Sql_S = """
#     with DateMap as (
#         Select *
#           from (
#             Select a.BASE_DT, b.BASE_DT as MSCI_DT,
#                 row_number() over (partition by a.BASE_DT order by b.BASE_DT) as ro
#                 from #TEMP a
#                 left outer join ( Select distinct BASE_DT, 1 as dummy from MKT_MSCI_EQ_DM_MAIN) b
#                 on a.dummy = b.dummy
#                 and a.BASE_DT >= b.BASE_DT
#                 and b.BASE_DT >= DATEADD(D, -10, a.BASE_DT)
#         ) a
#          where ro = 1
#     )
#     Select dt.BASE_DT, inf.MSCI_TMSRS_CD as TMSRS_CD,
#            CTRY_SYMB_CD as CTRY
#       from DateMap dt
#       left outer join MKT_MSCI_EQ_DM_MAIN inf
#         on dt.MSCI_DT = inf.BASE_DT
#     """
#     refInfo = pd.read_sql(Sql_S, conn)
#     conn.close()
#     refInfo = refInfo.astype('str')
#     return refInfo


# def get_msciInf_all(dt_seq, verbose=False):
#     DF = pd.DataFrame({'BASE_DT': dt_seq, 'dummy': 1})
#     # For Insertion
#     S0 = pd.Series({'BASE_DT': 'varchar(8)', 'dummy': 'int'})
#     S1 = pd.Series({'BASE_DT': 1, 'dummy': 1})
#     primary = ['BASE_DT', 'dummy']
#     typeStr = create_typeStr(S0, S1, primary)
#     db_name = '#TEMP'

#     conn = connectDB('MSSQL_RSCH')
#     create_table(conn, db_name, DF, typeStr, primary)
#     update_table(conn, db_name, DF, typeStr, verbose=verbose)

#     Sql_S = """
#     with DateMap as (
#         Select *
#           from (
#             Select a.BASE_DT, convert(varchar(8), b.BASE_DT, 112) as MSCI_DT,
#                 row_number() over (partition by a.BASE_DT order by b.BASE_DT) as ro
#                 from #TEMP a
#                 left outer join ( Select distinct BASE_DT, 1 as dummy from RSCH_DS_DY) b
#                 on a.dummy = b.dummy
#                 and a.BASE_DT >= convert(varchar(8), b.BASE_DT, 112)
#                 and convert(varchar(8), b.BASE_DT, 112) >= DATEADD(D, -10, a.BASE_DT)
#         ) a
#          where ro = 1
#     )
#     Select dt.BASE_DT, inf.TMSRS_CD,
#            CTRY_SYMB_CD as CTRY, SUB_IDST_CD as GICS_CD, 
#       from DateMap dt
#       left outer join RSCH_DS_DY inf
#         on dt.MSCI_DT = convert(varchar(8), inf.BASE_DT, 112)
#     """
#     refInfo = pd.read_sql(Sql_S, conn)
#     conn.close()
#     refInfo = refInfo.astype('str')
#     return refInfo