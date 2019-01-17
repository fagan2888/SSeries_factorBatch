import numpy as np
import pandas as pd
import datetime as dt
import pyodbc
import os
from .utils_db_alch2 import connectDB
from .common import chg_to_type

sql_WS = """
SELECT a.*, b.Code,
       convert(varchar(8), b.ST_DT, 112) as startDT,
       convert(varchar(8), b.END_DT, 112) as endDT,
       1 as RGN_TP_CD
    from
    (Select distinct(TMSRS_CD)
        from RSCH_DS_DY with (nolock)
        where BASE_DT >= '19950101'
    ) a
    inner join
    (Select TMSRS_CD, isnull(WS_CMP_CD, WS_SEC_CD) as Code, ST_DT, END_DT
        from RSCH_WS_SEC_MST with (nolock)
        where ISNULL(END_DT, getdate()+10) >= '19950101'
        and TMSRS_CD <> '65273'
    ) b
    on a.TMSRS_CD=b.TMSRS_CD
"""

sql_IBES = ""
sql_DS = ""
sql_sedol = ""
sql_starmine = ""

mapVendor2Script = {
    'IBES': sql_IBES,
    'worldscope': sql_WS,
    'datastream': sql_DS,
    'sedol': sql_sedol,
    'starmine': sql_starmine
}

codeAlias = {
    'IBES': 'Code',
    'worldscope': 'Code',
    'datastream': 'Code',
    'sedol': 'Sedol',
    'starmine': 'Code'
}

customCols = {
    'IBES': ['RGN_TP_CD'],
    'worldscope': ['RGN_TP_CD'],
    'datastream': ['RGN_TP_CD'],
    'sedol': ['RGN_TP_CD'],
    'starmine': ['RGN_TP_CD', 'PermRegion']
}

dropNullCols = {
    'IBES': [],
    'worldscope': [],
    'datastream': [],
    'sedol': [],
    'starmine': ['PermRegion']
}


def _check_existing(vendor):
    today_ = dt.date.today().strftime('%Y%m%d')
    filename = '{}_{}.p'.format(vendor, today_)
    currFolder = [f for f in os.listdir() if ~os.path.isfile(f)]
    if 'save_map' not in currFolder:
        os.mkdir('save_map')

    files_lst = [f for f in os.listdir('save_map')]
    if len(files_lst) > 0:
        lst = pd.Series(files_lst)
        lst = lst[lst.str.match(vendor + '_\d{8}')].tolist()
        if len(lst) > 0:
            del_lst = [f for f in lst if int(f[-10:-2]) < int(today_)]
            if len(del_lst) > 0:
                for f in del_lst:
                    os.remove('save_map/' + f)
            if filename in lst:
                return pd.read_pickle('save_map/' + filename), filename
    return None, filename


def get_Mapping_orig(vendor, main='TMSRS_CD'):
    assert vendor in mapVendor2Script.keys(), (
        "'vendor' must be within {}".format(
            mapVendor2Script.keys())
    )
    MAP, filename = _check_existing(vendor)
    if MAP is None:
        if vendor == "IBES":
            MAP = run_custom_IBESmap()
            MAP = MAP.rename(columns={'IBES_TP_CD': 'RGN_TP_CD'})[
                ['TMSRS_CD', codeAlias[vendor], 'startDT', 'endDT'] +
                customCols[vendor]
            ]
            MAP.to_pickle('save_map/' + filename)
        else:
            Sql_S = mapVendor2Script[vendor]
            conn = connectDB('MSSQL_RSCH')
            MAP = pd.read_sql(Sql_S, conn)
            conn.close()

            # chg int columns to string
            if len(dropNullCols[vendor]) > 0:
                MAP.dropna(subset=dropNullCols[vendor], inplace=True)
                for col in dropNullCols[vendor]:
                    if MAP[col].dtype == np.float64:
                        MAP[col] = MAP[col].astype(int)

            MAP = chg_to_type(
                MAP,
                chg_col=MAP.columns.tolist(), type_=str)
            MAP.drop_duplicates(
                subset=['TMSRS_CD', 'RGN_TP_CD', 'startDT', 'endDT'],
                keep='first', inplace=True)
            MAP = MAP[
                ['TMSRS_CD', codeAlias[vendor], 'startDT', 'endDT'] +
                customCols[vendor]
            ]
            MAP.to_pickle('save_map/' + filename)

    return MAP


def run_custom_IBESmap():
    SQL_S = """
    Select a.*, b.Code, b.IBES_TP_CD,
        convert(varchar(8), b.ST_DT, 112) as startDT,
        convert(varchar(8), b.END_DT, 112) as endDT
    from (Select distinct(TMSRS_CD)
            from RSCH_DS_DY with (nolock)
            where DM_CD='1'
            and MSCI_STD_CD='1'
            and BASE_DT >= '19950101') a
    inner join
        (Select TMSRS_CD, IBES_CD as Code, IBES_TP_CD, ST_DT, END_DT
            from RSCH_IBES_SEC_MST with (nolock)
            where ISNULL(END_DT, getdate()+10) >= '19950101'
            and TMSRS_CD <> '65273' ) b
        on a.TMSRS_CD=b.TMSRS_CD
    """
    CONNECT_RSCH = connectDB(ODBC_NAME="MSSQL_RSCH")
    map_DT = pd.read_sql(SQL_S, CONNECT_RSCH)
    CONNECT_RSCH.close()
    map_DT = map_DT.sort_values(['TMSRS_CD', 'Code'])
    map_DT = map_DT.reset_index(drop=True)

    SQL_S = """
    Select b.VenCode as TMSRS_CD, a.VenCode as Code,
        3 as IBES_TP_CD,
        convert(varchar(8), a.StartDate, 112) as startDT,
        convert(varchar(8), a.EndDate, 112) as endDT,
        c.STicker as IBES_ticker, c.Name as IBES_name, c.Currency_ as IBES_fx,
        c.Country as IBES_ctry, c.ExchCtry as IBES_exchCtry,
        d.Sedol as MSCI_sedol, d.Name as MSCI_name, d.Country as MSCI_ctry
    from (
    Select *
    from GSecMapX with (nolock)
    where VenType=2 and Rank=1) a
    inner join GSecMapX b with (nolock)
        on b.VenType=7
    and a.SecCode=b.SecCode
    and b.Rank=1
    left outer join IBGSInfo3 c with (nolock)
        on a.VenCode=c.Code
    left outer join MSDsec d with (nolock)
        on b.VenCode=d.TsCode
    UNION
    Select b.VenCode as TMSRS_CD, a.VenCode as Code,
        1 as IBES_TP_CD,
        convert(varchar(8), a.StartDate, 112) as startDT,
        convert(varchar(8), a.EndDate, 112) as endDT,
        c.STicker as IBES_ticker, c.Name as IBES_name, c.Currency_ as IBES_fx,
        c.Country as IBES_ctry, c.ExchCtry as IBES_exchCtry,
        d.Sedol as MSCI_sedol, d.Name as MSCI_name, d.Country as MSCI_ctry
    from (
    Select *
    from SecMapX with (nolock)
    where VenType=2 and Rank=1) a
    inner join SecMapX b with (nolock)
        on b.VenType=7
    and a.SecCode=b.SecCode
    and b.Rank=1
    left outer join IBESInfo3 c with (nolock)
        on a.VenCode=c.Code
    left outer join MSDsec d with (nolock)
        on b.VenCode=d.TsCode
    """
    CONNECT_QAD = connectDB(ODBC_NAME="MSSQL_QAD")
    chk_map_DT = pd.read_sql(SQL_S, CONNECT_QAD)
    CONNECT_QAD.close()

    chk_map_DT = chg_to_type(
        chk_map_DT, chg_col=['TMSRS_CD', 'Code', 'IBES_TP_CD',
                             'startDT', 'endDT'], type_=str)

    ## Checking Phase ##
    ##########################################
    # Check where 'TMSRS_CD' has more than two Code
    T_check = map_DT.groupby('TMSRS_CD')[['Code', 'IBES_TP_CD']].apply(len)
    T_check_sec = T_check[T_check > 1].reset_index()
    map_DT_final = pd.DataFrame()
    T_unknown = []
    for a in T_check_sec['TMSRS_CD']:
        A = map_DT[map_DT['TMSRS_CD'] == a].copy()
        A.loc[A['endDT'].isnull(), 'endDT'] = '99991231'
        A_ = A[['startDT', 'endDT']].sort_values('startDT').values.astype(int)
        tmp = []
        for i in range(A_.shape[0] - 1):
            tmp.append(A_[i + 1][0] - A_[i][1])
        if all((np.array(tmp) == 1) | (np.array(tmp) > 1)):
            map_DT_final = map_DT_final.append(map_DT[map_DT['TMSRS_CD'] == a])
            print(a)
        else:
            T_unknown.append(a)
    map_DT = map_DT[~map_DT['TMSRS_CD'].isin(map_DT_final['TMSRS_CD'])]

    ##########################################
    # Check where 'Code' has more than two TMSRS_CD
    check = map_DT.groupby(['Code', 'IBES_TP_CD'])['TMSRS_CD'].size()
    check_sec = check[check > 1].reset_index()

    # Check where chk_map_DT has 1 outcome
    map_DT_rev = map_DT.copy()
    for a, b in zip(check_sec['Code'].tolist(), check_sec['IBES_TP_CD'].tolist()):
        A = chk_map_DT[(chk_map_DT['Code'] == a) & (chk_map_DT['IBES_TP_CD'] == b)]
        if A.shape[0] == 1:
            map_DT_rev = map_DT_rev[~((map_DT_rev['Code'] == a) & (map_DT_rev['IBES_TP_CD'] == b))]
            map_DT_rev = map_DT_rev.append(A, sort=False)
            print(a, b)

    check = map_DT_rev.groupby(['Code', 'IBES_TP_CD'])['TMSRS_CD'].size()
    check_sec = check[check > 1].reset_index()

    # Check where chk_map_DT has multiple outcome but non-overlapping period
    unknown = []
    for a, b in zip(check_sec['Code'].tolist(), check_sec['IBES_TP_CD'].tolist()):
        A = chk_map_DT[(chk_map_DT['Code'] == a) & (chk_map_DT['IBES_TP_CD'] == b)]
        A_ = A[['startDT', 'endDT']].sort_values('startDT').values.astype(int)
        tmp = []
        for i in range(A_.shape[0] - 1):
            tmp.append(A_[i + 1][0] - A_[i][1])
        if all((np.array(tmp) == 1) | (np.array(tmp) > 1)):
            map_DT_rev = map_DT_rev[~((map_DT_rev['Code'] == a) & (map_DT_rev['IBES_TP_CD'] == b))]
            map_DT_rev = map_DT_rev.append(A, sort=False)
            print(a, b)
        else:
            unknown.append([a, b])
    unknown = pd.DataFrame(unknown, columns=['Code', 'IBES_TP_CD'])

    return map_DT_rev