import pyodbc
import pandas as pd
import numpy as np
import re
import time

from .utils_db_alch2 import connectDB
from .common import chunker_count


def create_table(conn, db_name, typeStr, primary=None, scriptOnly=False):
    """
    conn: odbc connection
    db_name: str, 'name for table'
    DF: pd.DataFrame, table values
    typeStr: pd.Series (index - columns, values - sqlDataType)
    primary: key-columns
    chunk_size: upload row size
    """
    if primary is not None:
        primary = [primary] if isinstance(primary, str) else primary
    else:
        primary = []

    create_typeStr(typeStr, primary=primary) if isinstance(typeStr, pd.Series) else typeStr

    if not scriptOnly:
        c = conn.cursor()
    if len(primary) <= 1:
        Sql_S = "CREATE TABLE {} (\n".format(db_name)
        Sql_S += ',\n'.join(
            ['{} {} {} {}'.format(
                x, y,
                'PRIMARY KEY' if x in primary else '',
                'NOT NULL' if z == 1 else 'NULL')
             for x, y, z in zip(typeStr.index, typeStr['type'], typeStr['nulltype'])
             ])
        Sql_S += ')'
    else:
        Sql_S = "CREATE TABLE {} (\n".format(db_name)
        Sql_S += ',\n'.join(
            ['{} {} {}'.format(x, y, 'NOT NULL' if z == 1 else 'NULL')
             for x, y, z in zip(typeStr.index, typeStr['type'], typeStr['nulltype'])])
        Sql_S += '\nPRIMARY KEY ({})'.format(', '.join(primary))
        Sql_S += '\n)'

    if not scriptOnly:
        c.execute(Sql_S)
        conn.commit()
    else:
        return Sql_S


def chunker_pd(seq, size):
    return (seq.values[pos:pos + size] for pos in range(0, seq.shape[0], size))


def update_table(conn, db_name, DF, typeStr, chunk_size=800, verbose=True, scriptOnly=False):
    """
    conn: odbc connection
    db_name: str, 'name for table'
    DF: pd.DataFrame, table values
    typeStr: pd.Series (index - columns, values - sqlDataType)
    primary: key-columns
    chunk_size: upload row size
    """
    if not isinstance(DF, pd.Series):
        # for DF is DataFrame (order of columns)
        DF = DF[typeStr.index]
    tot_n = chunker_count(DF, chunk_size)
    DF_ = chunker_pd(DF, chunk_size)

    if verbose:
        st_time = time.time()
    if not scriptOnly:
        c = conn.cursor()
    for i, df in enumerate(DF_):
        Sql_S = "INSERT INTO {} VALUES".format(db_name)
        if not isinstance(DF, pd.Series):
            # when DF is DataFrame
            Sql_S += ','.join([re.sub("'NULL'", "NULL", str(tuple(elem))) for elem in df])
        else:
            # when DF is Series
            # Sql_S += '(' + re.sub("'NULL'", "NULL", '),('.join(df.astype(str))) + ')'
            Sql_S += "('"+ re.sub("'NULL'", "NULL", "'),('".join(df.astype(str))) + "')"
        if not scriptOnly:
            c.execute(Sql_S)
            conn.commit()
            if verbose:
                tmp_ = time.time() - st_time
                print('Uploading Values into "{}" Table: {:4.2f}% - {:02d}m{:02d}s'.format(
                    db_name, (i + 1) * 100 / tot_n,
                    int(tmp_ / 60), int(tmp_ % 60)), end='\r')
    if not scriptOnly:
        if verbose:
            tmp_ = time.time() - st_time
            print('Uploading Values into "{}" Table: 100% - {:02d}m{:02d}s'.format(
                db_name, int(tmp_ / 60), int(tmp_ % 60)))
    else:
        return Sql_S
    


def create_typeStr(S0, S1=None, primary=None):
    """
    S0, S1 must be pd.Series format.
    S0 - data types for SQL
    S1 - data null
    ex)
        S0 = pd.Series({'BASE_DT': 'varchar(8)', 'dummy': 'int'})
        S1 = pd.Series({'BASE_DT': 1, 'dummy': 1})
    """
    assert S0.dtypes.type == np.object_, 'S0 must be string type'
    if primary is not None:
        primary = [primary] if isinstance(primary, str) else primary
    else:
        primary = []
        print('warning: No Primary Key!')
    assert isinstance(primary, list), 'Check primary format'
    
    if S1 is None:
        S1 = pd.Series(np.zeros(S0.shape[0]).astype(int), index=S0.index)
        S1.loc[primary] = 1
    
    assert set(S0.index) == set(S1.index), 'S0 & S1 keys must equal!'
    assert S1.isin([0, 1]).all(), 'S1 (NULL/not NULL argument) must be in 0 or 1'

    if len(primary) > 0:
        check = (S1.loc[primary] == 1).all()
        assert check, 'All Primary Keys must be 1 for S1'
    
    typeStr = pd.concat([S0, S1], axis=1, sort=False, keys=['type', 'nulltype'])
    
    return typeStr


def check_exist(conn, db_name):
    """
    cols: TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
    """
    Sql_S = """
    SELECT * 
      FROM INFORMATION_SCHEMA.TABLES 
     WHERE TABLE_SCHEMA = 'dbo' 
       AND TABLE_NAME = '{}'
    """.format(db_name)
    c = conn.cursor()
    c.execute(Sql_S)
    out = c.fetchall()
    if len(out) == 1:
        return True
    else:
        return False