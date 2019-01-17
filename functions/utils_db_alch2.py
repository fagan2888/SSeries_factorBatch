#-*- coding: utf-8 -*-

# Python 3.6
# author: EJ Jang
# date: 2018.06.14

# References:
# HJ Woo

import pyodbc
import csv
from sqlalchemy import create_engine

CONN_INFO = [{'srv_nm': 'MSSQL_DEV', 'URL_DB': '192.10.10.29:1433/SIM_EQ',
              'driver': 'SQL+Server+Native+Client+10.0'},
             {'srv_nm': 'MSSQL_QAD', 'URL_DB': '192.10.10.53:1433/qai',
              'driver': 'SQL+Server+Native+Client+10.0'},
             {'srv_nm': 'MSSQL_RSCH', 'URL_DB': '192.10.10.36:1433/RSCH',
              'driver': 'SQL+Server+Native+Client+10.0'},
             {'srv_nm': 'MSSQL_Axioma', 'URL_DB': '192.10.10.132:1433/axiomadb_kic_2015r2',
              'driver': 'SQL+Server+Native+Client+10.0'},
             {'srv_nm': 'MSSQL_RDW', 'URL_DB': '192.10.10.192\dw/DW',
              'driver': 'SQL+Server+Native+Client+10.0'}]


def encrypt(msg):
    return msg.encode('utf-8').hex()


def decrypt(msg):
    dec = bytes.fromhex(msg).decode('utf-8')
    return dec


def connectDB(ODBC_NAME='MSSQL_RDW', uid=None, pwd=None, file_nm='c:/encrypt_DB2'):
    """
    DB server connection

    Args     :
        ODBC_NAME:  ODBC name to connect
        uid_: ID if explicit
        pwd_: password if explicit
        key: key for decrypting
        file_nm: decrypting file

        Returns  :
        DB connection
    """
    with open(file_nm) as f:
        reader = csv.reader(f)
        for line in reader:
            if line[0] == ODBC_NAME:
                break

    line[1] = decrypt(line[1]) if uid is None else uid
    line[2] = decrypt(line[2]) if pwd is None else pwd

    conStr = 'DSN=' + ODBC_NAME
    conStr += ';UID=' + line[1]
    conStr += ';PWD=' + line[2]

    return(pyodbc.connect(conStr))


def addDB(ODBC_NAME='test', uid='user1', pwd='pwd1', file_nm='c:/encrypt_DB2'):
    line = [ODBC_NAME, encrypt(uid), encrypt(pwd)]

    with open(file_nm, 'a+', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(line)


def safekeep_recreate(file_nm='c:/encrypt_DB2'):
    lines = [['MSSQL_REAL', 'kic_jangej', 'm5i3n2d0'],
             ['MSSQL_QAD', 'kic_001', 'front001'],
             ['MSSQL_RSCH', 'kic_jangej', 'm5i3n2d0'],
             ['MariaDB_REAL', 'ejjang', 'kic1211'],
             ['MSSQL_RDW', 'kic_jangej', 'm5i3n2d0'],
             ['MSSQL_Axioma', 'kic_eqaxm', '$equity0103'],
             ['MSSQL_DEV', 'kic_jangej', 'm5i3n2d0']]
    for line in lines:
        addDB(line[0], line[1], line[2], file_nm=file_nm)


def recreate(old_nm='c:/encrypt_DB', file_nm='c:/encrypt_DB2'):
    import base64
    from Crypto.Cipher import AES

    def decrypt_(msg_enc='5778140dce29fbac13f8d27884cc28b4', key='key_sample'):
        key_16 = (key + "################")[0:16]
        b_enc = base64.binascii.a2b_hex(msg_enc)

        cipher = AES.new(key_16, AES.MODE_ECB)
        dec = cipher.decrypt(b_enc).decode("utf-8")

        msg = dec[2:(2 + int(dec[:2]))]
        return(msg)

    file = open(old_nm, 'r')
    lines = file.readlines()
    file.close()

    newline = []
    for line in lines:
        words = list()
        words = line.replace('"', "").replace('\n', "").split(",")
        if words[0] != 'SERVER':
            words[1] = decrypt_(words[1], 'goodluck')
            words[2] = decrypt_(words[2], 'goodluck')
            newline.append(words)

    for line in newline:
        addDB(line[0], line[1], line[2], file_nm=file_nm)


def createEngine(ODBC_NAME="MSSQL_RDW", uid=None, pwd=None, file_nm="c:/encrypt_DB2"):
    """
    SQLAlchemy Engine

    Args     :
        ODBC_NAME:  ODBC name to connect
        uid_: ID if explicit
        pwd_: password if explicit
        key: key for decrypting
        file_nm: decrypting file

        Returns  :
        Engine
    """
    with open(file_nm) as f:
        reader = csv.reader(f)
        for line in reader:
            if line[0] == ODBC_NAME:
                break

    connInf = [x for x in CONN_INFO if x['srv_nm'] == ODBC_NAME][0]

    strEng = 'mssql://'
    strEng += (decrypt(line[1]) if uid is None else uid_) + ":"
    strEng += (decrypt(line[2]) if pwd is None else pwd_) + "@"
    strEng += connInf['URL_DB'] + "?driver="
    strEng += connInf['driver']

    return(create_engine(strEng))


###########################################################
if __name__ == "__main__":
    import os
    if not 'encrypt_DB2' in os.listdir('c:/'):
        recreate()

    cnxn = connectDB()
    cursor = cnxn.cursor()
    cursor.execute("select *from WRK_PMC_EQ_CRB where BASE_DT='20161223'")
    rows = cursor.fetchall()
    cnxn.close()

    aa = encrypt("org message")
    print(aa)

    dec_aa = decrypt(aa)
    print(dec_aa)

    ##################

    # from utils_db import *
    import pandas as pd

    sqlStr = ""
    sqlStr = sqlStr + " select *                  "
    sqlStr = sqlStr + " from WRK_PMC_EQ_CRB       "
    sqlStr = sqlStr + " where BASE_DT ='20161223' "
    sqlStr = sqlStr + " and   FND_CD  ='S8LH'     "

    cnxn = connectDB()
    data_df = pd.read_sql(sqlStr, cnxn)
    cnxn.close()
    print(data_df.head())

    sqlStr = ""
    sqlStr = sqlStr + " select *                  "
    sqlStr = sqlStr + " from WRK_PMC_EQ_CRB       "
    sqlStr = sqlStr + " where BASE_DT ='20161223' "
    sqlStr = sqlStr + " and   FND_CD  ='S8LH'     "

    eng = createEngine("MSSQL_RDW")
    data_df1 = pd.read_sql(sqlStr, eng)
    print(data_df1.head())
