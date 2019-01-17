import numpy as np
import pandas as pd
import datetime as dt
import time
import re

from batch_utils.utils_db_alch2 import connectDB
from batch_utils.utils_sql import create_table, update_table, create_typeStr
from batch_utils.utils_dateSeq import batch_sequence
from batch_utils.common import list2sqlstr, _conv2strCol, rm_backward, chunker
from batch_utils import WS_currVal

bkfil, rtvStart, seq_DT = batch_sequence('backfill', 'M')

Table = 'WSPITCmpIssFData'
Code = ['6751']
Item = '9802' # beta
cal_dt_ = seq_DT.rename('marketdate')

S0 = pd.Series({'marketdate': 'datetime'})
primary = 'marketdate'
typeStr = create_typeStr(S0, primary=primary)

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

WS_currVal()