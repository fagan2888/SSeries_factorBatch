import pandas as pd
import numpy as np
import datetime as dt
import os
import csv

from .utils_sql import create_table, update_table, create_typeStr
from .utils_db_alch2 import connectDB


def print_fillReport(bkfil, freq, DF, chk_col='Code'):
    refInfo, filename = _check_existing(bkfil, freq)

    if refInfo is None:
        dt_seq = DF['BASE_DT'].unique().tolist()
        if bkfil:
            refInfo = get_msciCompareRef_all(dt_seq, verbose=False)
        else:
            refInfo = get_msciCompareRef_batch(dt_seq, verbose=False)
        refInfo.to_pickle('save_temp/' + filename)

    def my_func(df):
        df_ = pd.merge(refInfo, df, on=['BASE_DT', 'TMSRS_CD'], how='left')
        out = df_.groupby('BASE_DT')[chk_col].agg([len, lambda x: x[x.notnull()].count()])
        out.columns = ['msci_cnt', 'fctr_cnt']
        out['fill_pct'] = out['fctr_cnt'] / out['msci_cnt']
        return out

    out = DF.groupby('StyleName').apply(my_func)

    out2 = DF.groupby(['StyleName', 'BASE_DT'])['TMSRS_CD'].count()
    out2.name = 'tot_cnt'

    OUT_fin = pd.concat([out, out2], axis=1, sort=True)
    
    print('>>> Checking Filled Percentage to MSCI DM Universe:')
    print(OUT_fin)
    print('\n')
    _monitor_logging(bkfil, freq, OUT_fin)
    return OUT_fin


def _monitor_logging(bkfil, freq, DF):
    today_ = dt.date.today().strftime('%Y%m%d')
    type_ = 'full' if bkfil else 'batch'
    filename = '{}_{}_log_{}.log'.format(type_, freq, today_)
    currFolder = [f for f in os.listdir() if ~os.path.isfile(f)]
    if 'log' not in currFolder:
        os.mkdir('log')
    
    file_lst = [f for f in os.listdir('log') if os.path.isfile('log/{}'.format(f))]
    input_ = DF.reset_index().copy()
    if filename not in file_lst:
        with open('log/{}'.format(filename), 'w', newline='') as f:
            writer = csv.writer(f, delimiter=',')
            writer.writerow(input_.columns.tolist())
            writer.writerows(input_.values.tolist())
    else:
        with open('log/{}'.format(filename), 'a+', newline='') as f:
            writer = csv.writer(f, delimiter=',')
            writer.writerows(input_.values.tolist())


def _check_existing(bkfil, freq):
    today_ = dt.date.today().strftime('%Y%m%d')
    type_ = 'full' if bkfil else 'batch'
    filename = '{}_{}_msciInf_{}.p'.format(type_, freq, today_)
    currFolder = [f for f in os.listdir() if ~os.path.isfile(f)]
    if 'save_temp' not in currFolder:
        os.mkdir('save_temp')

    files_lst = [f for f in os.listdir('save_temp')]
    if len(files_lst) > 0:
        lst = pd.Series(files_lst)
        lst = lst[lst.str.match(type_ + '_' + freq + '_msciInf_\d{8}')].tolist()
        if len(lst) > 0:
            del_lst = [f for f in lst if int(f[-10:-2]) < int(today_)]
            if len(del_lst) > 0:
                for f in del_lst:
                    os.remove('save_temp/' + f)
            if filename in lst:
                return pd.read_pickle('save_temp/' + filename), filename
    return None, filename



def get_msciCompareRef_batch(dt_seq, verbose=False):
    DF = pd.DataFrame({'BASE_DT': dt_seq, 'dummy': 1})
    # For Insertion
    S0 = pd.Series({'BASE_DT': 'varchar(8)', 'dummy': 'int'})
    S1 = pd.Series({'BASE_DT': 1, 'dummy': 1})
    primary = ['BASE_DT', 'dummy']
    typeStr = create_typeStr(S0, S1, primary)
    db_name = '#TEMP'

    conn = connectDB('MSSQL_RDW')
    create_table(conn, db_name, typeStr, primary)
    update_table(conn, db_name, DF, typeStr, verbose=verbose)

    Sql_S = """
    with DateMap as (
        Select *
          from (
            Select a.BASE_DT, b.BASE_DT as MSCI_DT,
                row_number() over (partition by a.BASE_DT order by b.BASE_DT) as ro
                from #TEMP a
                left outer join ( Select distinct BASE_DT, 1 as dummy from MKT_MSCI_EQ_DM_MAIN) b
                on a.dummy = b.dummy
                and a.BASE_DT <= b.BASE_DT
                and b.BASE_DT <= DATEADD(D, 10, a.BASE_DT)
        ) a
         where ro = 1
    )
    Select dt.BASE_DT, inf.MSCI_TMSRS_CD as TMSRS_CD
      from DateMap dt
      left outer join MKT_MSCI_EQ_DM_MAIN inf
        on dt.MSCI_DT = inf.BASE_DT
     where inf.DM_YN = 1 and inf.FML_STD_YN = 1
    """
    refInfo = pd.read_sql(Sql_S, conn)
    conn.close()
    refInfo = refInfo.astype('str')
    return refInfo


def get_msciCompareRef_all(dt_seq, verbose=False):
    DF = pd.DataFrame({'BASE_DT': dt_seq, 'dummy': 1})
    # For Insertion
    S0 = pd.Series({'BASE_DT': 'varchar(8)', 'dummy': 'int'})
    S1 = pd.Series({'BASE_DT': 1, 'dummy': 1})
    primary = ['BASE_DT', 'dummy']
    typeStr = create_typeStr(S0, S1, primary)
    db_name = '#TEMP'

    conn = connectDB('MSSQL_RSCH')
    create_table(conn, db_name, typeStr, primary)
    update_table(conn, db_name, DF, typeStr, verbose=verbose)

    Sql_S = """
    with DateMap as (
        Select *
          from (
            Select a.BASE_DT, convert(varchar(8), b.BASE_DT, 112) as MSCI_DT,
                row_number() over (partition by a.BASE_DT order by b.BASE_DT) as ro
                from #TEMP a
                left outer join ( Select distinct BASE_DT, 1 as dummy from RSCH_DS_DY) b
                on a.dummy = b.dummy
                and a.BASE_DT <= convert(varchar(8), b.BASE_DT, 112)
                and convert(varchar(8), b.BASE_DT, 112) <= DATEADD(D, 10, a.BASE_DT)
        ) a
         where ro = 1
    )
    Select dt.BASE_DT, inf.TMSRS_CD
      from DateMap dt
      left outer join RSCH_DS_DY inf
        on dt.MSCI_DT = convert(varchar(8), inf.BASE_DT, 112)
     where inf.DM_CD = 1 and inf.MSCI_STD_CD = 1
    """
    refInfo = pd.read_sql(Sql_S, conn)
    conn.close()
    refInfo = refInfo.astype('str')
    return refInfo