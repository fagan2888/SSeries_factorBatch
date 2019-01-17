import pandas as pd
import numpy as np
from ..utils_db_alch2 import connectDB
from ..utils_sql import create_table, update_table, create_typeStr

from ..ItemInfo import Item_lst, RatioItem_lst, CurrItem_lst
from ..common import list2sqlstr, _conv2strCol, rm_backward, chunker

S0 = pd.Series({'marketdate': 'datetime'})
primary = 'marketdate'
typeStr = create_typeStr(S0, primary=primary)

def WS_currVal(seq_DT, Item='9102', Table='WSPITCmpIssFData',
               Code=['6751', '6809'], Name='PE_curr'):
    """
    This will continue last number if EndDate is NULL,
    Thus must "POST-PROCESS" as to how long number should continue
    for discontinued security.
    """

    if isinstance(seq_DT, list):
        cal_dt_ = pd.Series(seq_DT, name='marketdate').sort_values()
    else:
        cal_dt_ = seq_DT.rename('marketdate')

    conn = connectDB(ODBC_NAME="MSSQL_QAD")
    create_table(conn, '#Calendar', typeStr, primary=primary)
    update_table(conn, '#Calendar', cal_dt_, typeStr, verbose=False)

    Sql_S = """
    Select convert(varchar(8),mm.marketdate,112) as marketdate, dt.Code,
           dt.Item, convert(varchar(8), dt.StartDate,112) as StartDate,
           convert(varchar(8), dt.EndDate,112) as EndDate, dt.Value_
      from #Calendar mm
      left outer join {} dt with (nolock)
        on mm.marketdate >= dt.StartDate
       and mm.marketdate <= isnull(dt.EndDate, dateadd(d,2,GETDATE()))
     where dt.Code in ({})
       and dt.Value_ <> -1e+38
       and not (dt.Value_ = 0  and dt.EndDate is NULL)
       and dt.Item='{}'
    """.format(Table, list2sqlstr(Code), Item)
    DT = pd.read_sql(Sql_S, conn)
    conn.close()

    DT['Name'] = Name
    DT['marketdate'] = DT['marketdate'].astype(str)
    DT.rename(columns={'marketdate': 'BASE_DT'}, inplace=True)

    DT = _conv2strCol(DT)
    return DT[['BASE_DT', 'Code', 'Item', 'Value_', 'Name']]
