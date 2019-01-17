import numpy as np
import pandas as pd
import datetime as dt
import pyodbc
from .utils_db_alch2 import connectDB
from .common import chg_to_type

sql_IBES = """
with FindCoreMapping as (
    Select a.SecCode, b.VenCode as TMSRS_CD, c.VenCode as Code,
           b.Rank as msciRank, c.Rank as ibesRank,
           3 as RGN_TP_CD, a.Name as mstrName, a.Country as mstrCtry,
           b.StartDate as st_t, b.EndDate as ed_t,
           c.StartDate as st_s, c.EndDate as ed_s,
           Case When b.StartDate <= c.StartDate Then c.StartDate
                Else b.StartDate End as StartDate,
           Case When b.EndDate <= c.EndDate Then b.EndDate
                Else c.EndDate End as EndDate
      from GSecMstrX a with (nolock)
     inner join GSecMapX b with (nolock)
        on a.SecCode=b.SecCode
      left outer join GSecMapX c with (nolock)
        on a.SecCode=c.SecCode
     where b.VenType=7
       and c.VenType=2
       and a.Type_=10
       and b.StartDate <= c.EndDate
       and c.StartDate <= b.EndDate
     UNION
    Select a.SecCode, b.VenCode as TMSRS_CD, c.VenCode as Code,
           b.Rank as msciRank, c.Rank as ibesRank,
           1 as RGN_TP_CD, a.Name as mstrName, a.Country as mstrCtry,
           b.StartDate as st_t, b.EndDate as ed_t,
           c.StartDate as st_s, c.EndDate as ed_s,
           Case When b.StartDate <= c.StartDate Then c.StartDate
                Else b.StartDate End as StartDate,
           Case When b.EndDate <= c.EndDate Then b.EndDate
                Else c.EndDate End as EndDate
      from SecMstrX a with (nolock)
     inner join SecMapX b with (nolock)
        on a.SecCode=b.SecCode
      left outer join SecMapX c with (nolock)
        on a.SecCode=c.SecCode
     where b.VenType=7
       and c.VenType=2
       and a.Type_=1
       and b.StartDate <= c.EndDate
       and c.StartDate <= b.EndDate
)
Select core.TMSRS_CD, core.Code, core.RGN_TP_CD, core.msciRank, core.ibesRank,
       convert(varchar(8), core.StartDate, 112) as startDT,
       convert(varchar(8), core.EndDate, 112) as endDT,
       core.mstrName, core.mstrCtry,
       msci.Name as msciName, msci.Country as msciCtry,
       ibes.Name as ibesName, ibes.Country as ibesCtry
  from FindCoreMapping core
  left outer join Msdsec msci with (nolock)
    on core.TMSRS_CD = msci.TsCode
  left outer join (
    Select *, 1 as RGN_TP_CD
      from IBESInfo3 with (nolock)
     UNION
    Select *, 3 as RGN_TP_CD
      from IBGSInfo3 with (nolock)
    ) ibes
    on core.Code = ibes.Code
   and core.RGN_TP_CD = ibes.RGN_TP_CD
 order by core.TMSRS_CD, core.RGN_TP_CD, core.msciRank, core.ibesRank, startDT, endDT
"""

sql_WS = """
with FindCoreMapping as (
    Select a.SecCode, b.VenCode as TMSRS_CD, c.VenCode as Code,
           b.Rank as msciRank, c.Rank as wsRank,
           3 as RGN_TP_CD, a.Name as mstrName, a.Country as mstrCtry,
           b.StartDate as st_t, b.EndDate as ed_t,
           c.StartDate as st_s, c.EndDate as ed_s,
           Case When b.StartDate <= c.StartDate Then c.StartDate
                Else b.StartDate End as StartDate,
           Case When b.EndDate <= c.EndDate Then b.EndDate
                Else c.EndDate End as EndDate
      from GSecMstrX a with (nolock)
     inner join GSecMapX b with (nolock)
        on a.SecCode=b.SecCode
      left outer join GSecMapX c with (nolock)
        on a.SecCode=c.SecCode
     where b.VenType=7
       and c.VenType=43
       and a.Type_=10
       and b.StartDate <= c.EndDate
       and c.StartDate <= b.EndDate
     UNION
    Select a.SecCode, b.VenCode as TMSRS_CD, c.VenCode as Code,
           b.Rank as msciRank, c.Rank as wsRank,
           1 as RGN_TP_CD, a.Name as mstrName, a.Country as mstrCtry,
           b.StartDate as st_t, b.EndDate as ed_t,
           c.StartDate as st_s, c.EndDate as ed_s,
           Case When b.StartDate <= c.StartDate Then c.StartDate
                Else b.StartDate End as StartDate,
           Case When b.EndDate <= c.EndDate Then b.EndDate
                Else c.EndDate End as EndDate
      from SecMstrX a with (nolock)
     inner join SecMapX b with (nolock)
        on a.SecCode=b.SecCode
      left outer join SecMapX c with (nolock)
        on a.SecCode=c.SecCode
     where b.VenType=7
       and c.VenType=43
       and a.Type_=1
       and b.StartDate <= c.EndDate
       and c.StartDate <= b.EndDate
)
Select core.TMSRS_CD, core.Code, core.RGN_TP_CD, core.msciRank, core.wsRank,
       convert(varchar(8), core.StartDate, 112) as startDT,
       convert(varchar(8), core.EndDate, 112) as endDT,
       core.mstrName, core.mstrCtry,
       msci.Name as msciName, msci.Country as msciCtry,
       ws.NameLong as wsName, ws.ISOCurrCode as wsCrncy
  from FindCoreMapping core
  left outer join Msdsec msci with (nolock)
    on core.TMSRS_CD = msci.TsCode
  left outer join WSPITInfo ws
    on core.Code = ws.Code
 order by core.TMSRS_CD, core.RGN_TP_CD, core.msciRank, core.wsRank, startDT, endDT
"""

sql_DS = """
with FindCoreMapping as (
    Select a.SecCode, b.VenCode as TMSRS_CD, c.VenCode as Code,
           b.Rank as msciRank, c.Rank as dsRank,
           3 as RGN_TP_CD, a.Name as mstrName, a.Country as mstrCtry,
           b.StartDate as st_t, b.EndDate as ed_t,
           c.StartDate as st_s, c.EndDate as ed_s,
           Case When b.StartDate <= c.StartDate Then c.StartDate
                Else b.StartDate End as StartDate,
           Case When b.EndDate <= c.EndDate Then b.EndDate
                Else c.EndDate End as EndDate
      from GSecMstrX a with (nolock)
     inner join GSecMapX b with (nolock)
        on a.SecCode=b.SecCode
      left outer join GSecMapX c with (nolock)
        on a.SecCode=c.SecCode
     where b.VenType=7
       and c.VenType=16
       and a.Type_=10
       and b.StartDate <= c.EndDate
       and c.StartDate <= b.EndDate
     UNION
    Select a.SecCode, b.VenCode as TMSRS_CD, c.VenCode as Code,
           b.Rank as msciRank, c.Rank as dsRank,
           1 as RGN_TP_CD, a.Name as mstrName, a.Country as mstrCtry,
           b.StartDate as st_t, b.EndDate as ed_t,
           c.StartDate as st_s, c.EndDate as ed_s,
           Case When b.StartDate <= c.StartDate Then c.StartDate
                Else b.StartDate End as StartDate,
           Case When b.EndDate <= c.EndDate Then b.EndDate
                Else c.EndDate End as EndDate
      from SecMstrX a with (nolock)
     inner join SecMapX b with (nolock)
        on a.SecCode=b.SecCode
      left outer join SecMapX c with (nolock)
        on a.SecCode=c.SecCode
     where b.VenType=7
       and c.VenType=16
       and a.Type_=1
       and b.StartDate <= c.EndDate
       and c.StartDate <= b.EndDate
)
Select core.TMSRS_CD, core.Code, core.RGN_TP_CD, core.msciRank, core.dsRank,
       convert(varchar(8), core.StartDate, 112) as startDT,
       convert(varchar(8), core.EndDate, 112) as endDT,
       core.mstrName, core.mstrCtry,
       msci.Name as msciName, msci.Country as msciCtry,
       ds.DsQtName as dsName, ds.Region as dsCrncy
  from FindCoreMapping core
  left outer join Msdsec msci with (nolock)
    on core.TMSRS_CD = msci.TsCode
  left outer join Ds2CtryQtInfo ds
    on core.Code = ds.InfoCode
 order by core.TMSRS_CD, core.RGN_TP_CD, core.msciRank, core.dsRank, startDT, endDT
"""

sql_sedol = """
with FindCoreMapping as (
    Select a.SecCode, b.Rank, b.VenCode as TMSRS_CD, c.Sedol,
           1 as RGN_TP_CD, a.Name as mstrName, a.Country as mstrCtry,
           b.StartDate as st_t, b.EndDate as ed_t,
           c.StartDate as st_s, c.EndDate as ed_s,
           Case When b.StartDate <= c.StartDate Then c.StartDate
                Else b.StartDate End as StartDate,
           Case When b.EndDate <= c.EndDate Then b.EndDate
                Else c.EndDate End as EndDate
      from SecMstrX a with (nolock)
     inner join SecMapX b with (nolock)
        on a.SecCode=b.SecCode
      left outer join SecSdlChgX c with (nolock)
        on a.SecCode=c.SecCode
     where b.VenType=7
       and a.Type_ = 1
       and b.StartDate <= c.EndDate
       and c.StartDate <= b.EndDate
     UNION
    Select a.SecCode, b.Rank, b.VenCode as TMSRS_CD, c.Sedol,
           3 as RGN_TP_CD, a.Name as mstrName, a.Country as mstrCtry,
           b.StartDate as st_t, b.EndDate as ed_t,
           c.StartDate as st_s, c.EndDate as ed_s,
           Case When b.StartDate <= c.StartDate Then c.StartDate
                Else b.StartDate End as StartDate,
           Case When b.EndDate <= c.EndDate Then b.EndDate
                Else c.EndDate End as EndDate
      from GSecMstrX a with (nolock)
     inner join GSecMapX b with (nolock)
        on a.SecCode=b.SecCode
      left outer join GSecSdlChg c with (nolock)
        on a.SecCode=c.SecCode
     where b.VenType=7
       and a.Type_ = 10
       and b.StartDate <= c.EndDate
       and c.StartDate <= b.EndDate
)
Select core.TMSRS_CD, core.Sedol, core.RGN_TP_CD, core.Rank,
       convert(varchar(8), core.StartDate, 112) as startDT,
       convert(varchar(8), core.EndDate, 112) as endDT,
       core.mstrName, core.mstrCtry,
       msci.Name as msciName, msci.Country as msciCtry
  from FindCoreMapping core
  left outer join Msdsec msci with (nolock)
    on core.TMSRS_CD = msci.TsCode
 order by core.TMSRS_CD, core.RGN_TP_CD, core.Rank, startDT, endDT
"""

sql_starmine = """
with FindCoreMapping as (
    Select a.SecCode, b.VenCode as TMSRS_CD, c.VenCode as Code,
           b.Rank as msciRank, c.Rank as smRank,
           3 as RGN_TP_CD, a.Name as mstrName, a.Country as mstrCtry,
           b.StartDate as st_t, b.EndDate as ed_t,
           c.StartDate as st_s, c.EndDate as ed_s,
           Case When b.StartDate <= c.StartDate Then c.StartDate
                Else b.StartDate End as StartDate,
           Case When b.EndDate <= c.EndDate Then b.EndDate
                Else c.EndDate End as EndDate
      from GSecMstrX a with (nolock)
     inner join GSecMapX b with (nolock)
        on a.SecCode=b.SecCode
      left outer join GSecMapX c with (nolock)
        on a.SecCode=c.SecCode
     where b.VenType=7
       and c.VenType=24
       and a.Type_=10
       and b.StartDate <= c.EndDate
       and c.StartDate <= b.EndDate
     UNION
    Select a.SecCode, b.VenCode as TMSRS_CD, c.VenCode as Code,
           b.Rank as msciRank, c.Rank as smRank,
           1 as RGN_TP_CD, a.Name as mstrName, a.Country as mstrCtry,
           b.StartDate as st_t, b.EndDate as ed_t,
           c.StartDate as st_s, c.EndDate as ed_s,
           Case When b.StartDate <= c.StartDate Then c.StartDate
                Else b.StartDate End as StartDate,
           Case When b.EndDate <= c.EndDate Then b.EndDate
                Else c.EndDate End as EndDate
      from SecMstrX a with (nolock)
     inner join SecMapX b with (nolock)
        on a.SecCode=b.SecCode
      left outer join SecMapX c with (nolock)
        on a.SecCode=c.SecCode
     where b.VenType=7
       and c.VenType=24
       and a.Type_=1
       and b.StartDate <= c.EndDate
       and c.StartDate <= b.EndDate
)
Select core.TMSRS_CD, core.Code, core.RGN_TP_CD, core.msciRank, core.smRank,
       convert(varchar(8), core.StartDate, 112) as startDT,
       convert(varchar(8), core.EndDate, 112) as endDT,
       sm.PermRegion,
       core.mstrName, core.mstrCtry,
       msci.Name as msciName, msci.Country as msciCtry,
       sm.CmpName as smName, sm.SecCtry as smCtry
  from FindCoreMapping core
  left outer join Msdsec msci with (nolock)
    on core.TMSRS_CD = msci.TsCode
  left outer join SM2DInfo sm with (nolock)
    on core.Code = sm.SecId
 order by core.TMSRS_CD, core.RGN_TP_CD, core.msciRank, core.smRank, startDT, endDT
"""

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


def get_Mapping(vendor, main='TMSRS_CD'):
    assert vendor in mapVendor2Script.keys(), (
        "'vendor' must be within {}".format(
            mapVendor2Script.keys())
    )
    Sql_S = mapVendor2Script[vendor]
    conn = connectDB('MSSQL_QAD')
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
        ['TMSRS_CD', codeAlias[vendor], 'startDT', 'endDT', 'mstrCtry'] +
        customCols[vendor]
    ].copy()

    return MAP


def getUnique_TMSRS_CD():
    Sql_S = """
    Select distinct TMSRS_CD
      from RSCH_DS_DY
    """
    conn = connectDB('MSSQL_RSCH')
    cursor = conn.cursor()
    cursor.execute(Sql_S)
    lst = cursor.fetchall()
    conn.close()
    return np.array([x[0] for x in lst])
