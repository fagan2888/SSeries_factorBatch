import pandas as pd
import numpy as np
from ..utils_db_alch2 import connectDB

from ..ItemInfo import Item_lst, RatioItem_lst, CurrItem_lst
from ..common import list2sqlstr, _conv2strCol, rm_backward


def WS_year(Item='18150', Table='WSPITSupp',
            Code=['6751', '6809'], bkfil=True, **kwargs):

    defaults = {'St_dt': '19951231', 'maxMonths': 14, 'add_lback': 10}
    defaults.update(kwargs)
    maxMonths = defaults['maxMonths']
    add_lback = defaults['add_lback']
    St_dt = defaults['St_dt']

    Sql_S = """
    Select cc.*, convert(varchar(8),dateadd(mm, {0}, cc.adj_PointDate),112) as maxUseDate
      from (
    Select bb.*,
        CASE when bb.PointDate <= bb.FILLyr
        THEN row_number() over (partition by bb.Item, bb.Code, bb.FiscalPrd, bb.adj_PointDate
                                    order by bb.PointDate desc) ELSE 1 END as RANK_NUM
    from (
            Select aa.Code, aa.PointDate, aa.FreqCode, aa.FiscalPrd, aa.Item,
                aa.Value_, aa.CalPrdEndDate, aa.MDiff,
                CASE WHEN datediff(mm, aa.CalPrdEndDate, aa.PointDate)>3
                            and aa.PointDate <= bkfil.FILLyr
                        THEN convert(varchar(8),dateadd(mm, 2, aa.CalPrdEndDate),112)
                ELSE convert(varchar(8), aa.PointDate, 112) END as adj_PointDate,
                convert(varchar(8), bkfil.FILLyr,112) as FILLyr, cod.Desc_,
                CASE WHEN cod.Code=5 THEN 1 WHEN cod.Code=1 THEN 2 WHEN cod.Code=2 THEN 3
                        WHEN cod.Code=6 THEN 4 WHEN cod.Code=3 THEN 5 ELSE 1 END as Prior
            from (
                    Select a.Code, Convert(varchar(8), a.PointDate, 112) as PointDate,
                        a.FreqCode, a.FiscalPrd,
                        a.Item, a.Value_, Convert(varchar(8), a.CalPrdEndDate, 112) as CalPrdEndDate,
                        datediff(mm,a.CalPrdEndDate,a.PointDate) as MDiff
                    from {1} a with (nolock)
                    where a.Item in ({2})
                    and a.Code in ({3})
    """.format(maxMonths, Table, list2sqlstr(Item), list2sqlstr(Code))
    if not bkfil:
        Sql_S += "and PointDate >= dateadd(M, -{}, convert(varchar(8), dateadd(d, -2, GETDATE()), 112))\n".format(
            maxMonths + add_lback)
    Sql_S += """
                    and a.Value_ <> -1e+38
                    and a.FreqCode in ('1','2')
                    ) aa
            left outer join (
                Select Item, Code, MAX(minPD) as FILLyr
                    from (Select Item, Code, FiscalPrd, CalPrdEndDate,
                                MIN(datediff(mm,CalPrdEndDate, PointDate)) as Filter,
                                MIN(PointDate) as minPD
                            from {0} with (nolock)
                            where FreqCode in (1,2) and Value_<>-1e+38
                            and Item in ({1})
                            and Code in ({2})
                            group by Item, Code, FiscalPrd, CalPrdEndDate) one
                    where one.Filter>3
            group by Item, Code
        ) bkfil
            on aa.Code=bkfil.Code and aa.Item=bkfil.Item
            left outer join WSPITCalPrd cal with (nolock)
            on aa.Code=cal.Code and aa.FreqCode=cal.FreqCode
            and aa.FiscalPrd=cal.FiscalPrd
            and aa.PointDate=cal.PointDate
            left join WSPITCode cod with (nolock) on cal.UpdTypeCode=cod.Code and cod.Type_=13
        ) bb where datediff(mm, bb.CalPrdEndDate, bb.adj_PointDate) < 18
        ) cc where cc.RANK_NUM='1'
    order by Item, Code, FiscalPrd, adj_PointDate, PointDate, Prior desc
    """.format(Table, list2sqlstr(Item), list2sqlstr(Code))
    CONNECT_QAD = connectDB(ODBC_NAME="MSSQL_QAD")
    DT = pd.read_sql(Sql_S, CONNECT_QAD)
    CONNECT_QAD.close()

    DT = DT.drop_duplicates(subset=['Code', 'Item', 'FiscalPrd', 'adj_PointDate', 'Value_'], keep='first')
    DT = DT.drop_duplicates(subset=['Code', 'Item', 'FiscalPrd', 'adj_PointDate'], keep='first')

    cols = ['Item', 'Code', 'adj_PointDate', 'FiscalPrd', 'CalPrdEndDate',
            'Value_', 'FILLyr', 'maxUseDate']
    if DT.shape[0] > 0:
        DT_REF = DT.groupby(['Item', 'Code', 'FiscalPrd'],
                            as_index=False)['adj_PointDate'].min()
        DT_REF['FiscalPrd'] = DT_REF['FiscalPrd'] - 1
        DT_REF = DT_REF.rename(columns={'adj_PointDate': 'nxtyr_st'})
        DT_ = pd.merge(DT, DT_REF, how='left', on=['Code', 'Item', 'FiscalPrd'])

        DT_['filt'] = 0
        DT_.loc[DT_['adj_PointDate'] > DT_['nxtyr_st'], 'filt'] = 1

        DT = DT_.loc[DT_['filt'] == 0, cols]
        DT = DT.rename(columns={'adj_PointDate': 'BASE_DT'})
        DT = DT.reset_index(drop=True)

        DT['filt'] = DT.groupby(['Item', 'Code', 'BASE_DT'])['FiscalPrd'].transform('max')
        DT = DT[DT['FiscalPrd'] == DT['filt']]
        DT = DT.drop('filt', axis=1)
        DT = DT.reset_index(drop=True)
    else:
        DT = DT[cols]
        DT = DT.rename(columns={'adj_PointDate': 'BASE_DT'})

    DT = _conv2strCol(DT)
    return(DT)


def _simple_SQL_qtr(Item='3995', Table='WSPITFinVal',
                    Code=['6751', '6809'], bkfil=True, **kwargs):
    defaults = {'St_dt': '19951231', 'maxMonths': 8, 'add_lback': 24}
    defaults.update(kwargs)
    maxMonths = defaults['maxMonths']
    St_dt = defaults['St_dt']
    add_lback = defaults['add_lback']

    Sql_S = """
    Select cc.*, convert(varchar(8),dateadd(mm, {0}, cc.adj_PointDate),112) as maxUseDate
    from (
    Select bb.*
        , CASE when bb.PointDate <= bb.FILLyr
            THEN row_number() over (partition by bb.Item, bb.Code, bb.FiscalPrd, bb.adj_PointDate
                                        order by bb.PointDate desc) ELSE 1 END as RANK_NUM
    from (
    Select aa.Code, aa.PointDate, aa.FreqCode, aa.FiscalPrd, aa.FiscalPrd2,
        aa.Item, aa.Value_, aa.CalPrdEndDate, aa.MDiff,
        CASE when datediff(mm, aa.CalPrdEndDate, aa.PointDate)>3 and aa.PointDate <= bkfil.FILLyr
                then convert(varchar(8),dateadd(mm, 2, aa.CalPrdEndDate),112)
        ELSE convert(varchar(8), aa.PointDate, 112) END as adj_PointDate,
        convert(varchar(8), bkfil.FILLyr, 112) as FILLyr,
        cod.Desc_, CASE WHEN cod.Code=5 THEN 1 WHEN cod.Code=1 THEN 2
                        WHEN cod.Code=2 THEN 3 WHEN cod.Code=6 THEN 4
                        WHEN cod.Code=3 THEN 5 ELSE 1 END as Prior
    from (
    Select a.Code, Convert(varchar(8),a.PointDate, 112) as PointDate, a.FreqCode, a.FiscalPrd,
        CONVERT(CHAR(4), (a.FISCALPRD / 4)) + 'Q' +
        CONVERT(CHAR(1), (a.FISCALPRD%4) + 1) as FiscalPrd2,
        a.Item, a.Value_, Convert(varchar(8), a.CalPrdEndDate, 112) as CalPrdEndDate,
        datediff(mm,a.CalPrdEndDate, a.PointDate) as MDiff
    from {1} a with (nolock)
    where a.Item in ({2})
    and a.Code in ({3})
    and a.FreqCode in ('8','3')
    and a.Value_ <> -1e+38
    """.format(maxMonths, Table, list2sqlstr(Item), list2sqlstr(Code))
    if not bkfil:
        Sql_S += "and PointDate >= dateadd(M, -{}, convert(varchar(8), dateadd(d, -2, GETDATE()), 112))\n".format(
            maxMonths + add_lback)
    Sql_S += """
           ) aa
    left outer join (
        Select Item, Code, MAX(minPD) as FILLyr from (
                Select Item, Code, FiscalPrd, CalPrdEndDate,
                        MIN(datediff(mm,CalPrdEndDate, PointDate)) as Filter,
                        MIN(PointDate) as minPD
                    from {0} with (nolock)
                where FreqCode in (3, 8) and Value_<>-1e+38
                    and Item in ({1})
                    and Code in ({2})
                group by Item, Code, FiscalPrd, CalPrdEndDate) one
                where one.Filter>3
            group by Item, Code
        ) bkfil
        on aa.Code=bkfil.Code and aa.Item=bkfil.Item
    left outer join WSPITCalPrd cal
        on aa.Code=cal.Code and aa.FreqCode=cal.FreqCode
    and aa.FiscalPrd=cal.FiscalPrd and aa.PointDate=cal.PointDate
    left outer join WSPITCode cod
        on cal.UpdTypeCode=cod.Code and cod.Type_=13
    ) bb where datediff(mm, bb.CalPrdEndDate, bb.adj_PointDate) < 18
    ) cc where cc.RANK_NUM='1'
    order by Item, Code, FiscalPrd, adj_PointDate, PointDate, Prior desc
    """.format(Table, list2sqlstr(Item), list2sqlstr(Code))
    CONNECT_QAD = connectDB(ODBC_NAME="MSSQL_QAD")
    DT = pd.read_sql(Sql_S, CONNECT_QAD)
    CONNECT_QAD.close()
    return DT


def WS_qtr(Item='3995', Table='WSPITFinVal',
           Code=['6751', '6809'], bkfil=True, **kwargs):
    fin_col = ['Item', 'Code', 'BASE_DT', 'FiscalPrd', 'FiscalPrd2', 'FILLyr',
               'Value_', 'CalPrdEndDate', 'maxUseDate']

    DT = _simple_SQL_qtr(Item=Item, Table=Table,
                         Code=Code, bkfil=bkfil, **kwargs)

    # Drop duplicates
    DT = DT.drop_duplicates(subset=['Code', 'Item', 'FiscalPrd', 'adj_PointDate', 'Value_'], keep='first')
    DT = DT.drop_duplicates(subset=['Code', 'Item', 'FiscalPrd', 'adj_PointDate'], keep='first')

    if DT.shape[0] > 0:
        DT = rm_backward(DT, sort_col=['Item', 'Code', 'adj_PointDate', 'FiscalPrd'],
                         grp_col=['Item', 'Code'], filter_col='FiscalPrd')
        DT = DT.rename(columns={'adj_PointDate': 'BASE_DT'})
        DT_fin = DT[fin_col].copy()
    else:
        DT_fin = pd.DataFrame(columns=fin_col)

    DT_fin = _conv2strCol(DT_fin)
    return DT_fin


def WS_qtr_avg(Item='3995', Table='WSPITFinVal',
               Code=['6751', '6809'], bkfil=True, min_cnt=2, **kwargs):
    fin_col = ['Item', 'Code', 'BASE_DT', 'FiscalPrd', 'FiscalPrd2', 'FILLyr',
               'Value_', 'CalPrdEndDate', 'maxUseDate']

    DT = _simple_SQL_qtr(Item=Item, Table=Table,
                         Code=Code, bkfil=bkfil, **kwargs)

    # Drop duplicates
    DT = DT.drop_duplicates(subset=['Code', 'Item', 'FiscalPrd', 'adj_PointDate', 'Value_'], keep='first')
    DT = DT.drop_duplicates(subset=['Code', 'Item', 'FiscalPrd', 'adj_PointDate'], keep='first')

    if DT.shape[0] > 0:

        # Make 'DT_': has each FiscalPrd with 4 prev FiscalPrd_back
        cols = ['Item', 'Code', 'FiscalPrd', 'FiscalPrd2', 'adj_PointDate']
        DT_ = DT[cols].copy()
        DT_ = rm_backward(DT_, sort_col=['Item', 'Code', 'adj_PointDate', 'FiscalPrd'],
                          grp_col=['Item', 'Code'], filter_col='FiscalPrd')

        DT_['m1'], DT_['m2'], DT_['m3'], DT_['m4'] = [0, 1, 2, 3]

        DT_ = DT_.melt(id_vars=cols, value_vars=['m1', 'm2', 'm3', 'm4'],
                       var_name='tmp', value_name='minus')
        DT_ = DT_.drop('tmp', axis=1)
        DT_['FiscalPrd_back'] = DT_['FiscalPrd'] - DT_['minus']
        DT_ = DT_.drop('minus', axis=1)

        DT_ = DT_.rename(columns={'adj_PointDate': 'ref_Point'})

        # Make 'DT_1': excludes FiscalPrd, FiscalPrd2 to join tables
        # Value_ aligns with FiscalPrd_back
        cols2 = ['Item', 'Code', 'PointDate', 'adj_PointDate', 'FreqCode',
                 'FiscalPrd', 'Value_', 'FILLyr',
                 'CalPrdEndDate', 'maxUseDate', 'Prior']
        DT_1 = DT[cols2].copy()
        DT_1 = DT_1.rename(columns={'FiscalPrd': 'FiscalPrd_back'})
        DT_proc = pd.merge(DT_, DT_1, on=['Item', 'Code', 'FiscalPrd_back'], how='inner')
        DT_proc = DT_proc.sort_values(
            ['Item', 'Code', 'ref_Point', 'FiscalPrd', 'FiscalPrd_back', 'adj_PointDate', 'Prior'])
        # (switch ref_point then FiscalPrd)

        # ref_Point is current date. adj_PointDate should be less than now
        DT_proc_ = DT_proc[DT_proc['ref_Point'] >= DT_proc['adj_PointDate']].copy()
        # sort to remove duplicates of FiscalPrd_back point-in-time
        DT_proc_0 = DT_proc_.drop_duplicates(
            ['Code', 'Item', 'ref_Point', 'FiscalPrd', 'FiscalPrd_back'], keep='last').copy()
        DT_proc_0['CNT'] = DT_proc_0.groupby(
            ['Code', 'Item', 'ref_Point', 'FiscalPrd'])['PointDate'].transform(len)
        DT_proc_0 = DT_proc_0[DT_proc_0['CNT'] >= min_cnt]

        if DT_proc_0.shape[0] >= 2:
            DT_fin = DT_proc_0.groupby(
                ['Item', 'Code', 'FiscalPrd', 'FiscalPrd2', 'ref_Point',
                 'FILLyr'], as_index=False,
            ).agg({'Value_': np.nanmean, 'CalPrdEndDate': max, 'maxUseDate': max})
            DT_fin = DT_fin.rename(columns={'ref_Point': 'BASE_DT'})
            # DT_fin = rm_backward(DT_fin, ['Item', 'Code', 'BASE_DT'],
            #                      ['Item', 'Code'], 'FiscalPrd', print_=True)
            DT_fin = DT_fin.reset_index(drop=True)

        else:
            DT_fin = pd.DataFrame(columns=fin_col)

    else:
        DT_fin = pd.DataFrame(columns=fin_col)

    DT_fin = _conv2strCol(DT_fin)
    return DT_fin


def WS_qtr_sum(Item='4860', Table='WSPITFinVal',
               Code=['6751'], bkfil=True, **kwargs):
    defaults = {'St_dt': '19951231', 'maxMonths': 8, 'add_lback': 24}
    defaults.update(kwargs)
    maxMonths = defaults['maxMonths']
    St_dt = defaults['St_dt']
    add_lback = defaults['add_lback']

    fin_col = ['Item', 'Code', 'BASE_DT', 'FiscalPrd', 'FiscalPrd2', 'FILLyr',
               'Value_', 'CalPrdEndDate', 'maxUseDate']

    Sql_S = """
    Select three.Code, three.PointDate, three.FreqCode, three.FiscalPrd,
        three.FiscalPrd2, three.Item, three.CalPrdEndDate, three.MDiff,
        three.VALUE as Value_, three.Value_semi, three.filt_rank,
        three.RANK_NUM, three.adj_PointDate, three.FILLyr,
        three.Prior, three.Desc_,
        convert(varchar(8),dateadd(mm, {0}, three.adj_PointDate),112) as maxUseDate
    from (
    Select two.*, CASE WHEN two.PointDate <= two.FILLyr
                    THEN row_number() over (partition by two.Item, two.Code,
                            two.FiscalPrd, two.adj_PointDate, two.FreqCode%5
                            order by two.PointDate desc)
                    ELSE 1 END as RANK_NUM
    from (
    Select one.*, convert(varchar(8),FILLyr,112) as FILLyr,
        datediff(mm,one.CalPrdEndDate,one.PointDate) as MDiff,
        CASE WHEN datediff(mm, one.CalPrdEndDate, one.PointDate)>3
                    and one.PointDate <= bkfil.FILLyr
                THEN convert(varchar(8),dateadd(mm,2,one.CalPrdEndDate),112)
                ELSE convert(varchar(8), one.PointDate,112) END as adj_PointDate,
        CASE WHEN (one.FiscalPrd%4+1)<>1 and one.FreqCode in (4,9)
                THEN one.Value_ - cum2.Value_
                ELSE one.Value_ END as VALUE,
        CASE WHEN (one.FiscalPrd%4+1) in (3,4) and one.FreqCode in (4,9)
                THEN one.Value_ - cum3.Value_
                WHEN (one.FiscalPrd%4+1) in (2) and one.FreqCode in (4,9)
                THEN one.Value_
                ELSE NULL END as VALUE_semi,
        CASE WHEN cum2.FiscalPrd is null THEN 1
                ELSE row_number() over (partition by one.PointDate, cum2.Code,
                    cum2.FiscalPrd, cum2.Item order by cum2.PointDate desc)
                END as filt_rank,
        cum2.PointDate as filtDate, cum2.Value_ as filtValue,
        cod.Desc_,
        CASE WHEN cod.Code=5 THEN 1
                WHEN cod.Code=1 THEN 2
                WHEN cod.Code=2 THEN 3
                WHEN cod.Code=6 THEN 4
                WHEN cod.Code=3 THEN 5 ELSE 1 END as Prior
    from (
        Select isnull(cum.Code,qtr.Code) as Code,
            isnull(cum.PointDate, qtr.PointDate) as PointDate,
            isnull(cum.FreqCode, qtr.FreqCode) as FreqCode,
            isnull(cum.FiscalPrd, qtr.FiscalPrd) as FiscalPrd,
            CONVERT(CHAR(4), (isnull(cum.FiscalPrd,qtr.FiscalPrd)/4)) + 'Q' +
            CONVERT(CHAR(1), (isnull(cum.FiscalPrd, qtr.FiscalPrd)%4) + 1)
                as FiscalPrd2,
            isnull(cum.Item, qtr.Item) as Item, isnull(cum.Value_,qtr.Value_) as Value_,
            isnull(cum.CalPrdEndDate,qtr.CalPrdEndDate) as CalPrdEndDate
        from (Select Code, Convert(varchar(8), PointDate, 112) as PointDate,
                    max(FreqCode) as FreqCode, FiscalPrd, Item, Value_,
                    Convert(varchar(8), CalPrdEndDate, 112) as CalPrdEndDate
                from {1} with (nolock)
                where FreqCode in (4,9) and Value_ <> -1e+38
                and Item in ({2})
                and Code in ({3})
    """.format(maxMonths, Table, list2sqlstr(Item), list2sqlstr(Code))
    if not bkfil:
        Sql_S += "and PointDate >= dateadd(M, -{}, convert(varchar(8), dateadd(d, -2, GETDATE()), 112))\n".format(
            maxMonths + add_lback)
    Sql_S += """
              group by Code, PointDate, Item, FiscalPrd, Value_, CalPrdEndDate
            ) qtr
        full outer join
            (Select Code, CONVERT(varchar(8),PointDate,112) as PointDate,
                    max(FreqCode) as FreqCode, FiscalPrd, Item, Value_,
                    CONVERT(varchar(8), CalPrdEndDate,112) as CalPrdEndDate
                from {0} with (nolock)
                where FreqCode in (3,8) and Value_<>  -1e+38
                and Item in ({1})
                and Code in ({2})
    """.format(Table, list2sqlstr(Item), list2sqlstr(Code))
    if not bkfil:
        Sql_S += "and PointDate >= dateadd(M, -{}, convert(varchar(8), dateadd(d, -2, GETDATE()), 112))\n".format(
            maxMonths + add_lback)
    Sql_S += """
              group by Code, PointDate, Item, FiscalPrd, Value_, CalPrdEndDate
            ) cum
            on cum.Item=qtr.Item
        and cum.Code=qtr.Code
        and cum.FiscalPrd=qtr.FiscalPrd
        and cum.CalPrdEndDate=qtr.CalPrdEndDate
        and cum.PointDate=qtr.PointDate
        ) one
    left outer join (
        Select Item, Code, MAX(minPD) as FILLyr from (
                Select Item, Code, FiscalPrd, CalPrdEndDate,
                        MIN(datediff(mm,CalPrdEndDate, PointDate)) as Filter,
                        MIN(PointDate) as minPD
                    from {0} with (nolock)
                where FreqCode in (3,4,8,9) and Value_<>-1e+38
                    and Item in ({1})
                    and Code in ({2})
                group by Item, Code, FiscalPrd, CalPrdEndDate) one
                where one.Filter>3
            group by Item, Code
        ) bkfil
            on one.Code=bkfil.Code and one.Item=bkfil.Item
    left outer join
        (Select Code, CONVERT(varchar(8),PointDate,112) as PointDate,
                max(FreqCode) as FreqCode,
                FiscalPrd, Item, Value_,
                CONVERT(varchar(8), CalPrdEndDate,112) as CalPrdEndDate
            from {0} with (nolock)
            where (FreqCode in (4,9) or
                    (FreqCode in (3,8) and (FiscalPrd%4+1)=1))
            and Value_<>  -1e+38
            group by Code, PointDate, Item, FiscalPrd, Value_, CalPrdEndDate) cum2
                on one.Item=cum2.Item
            and one.Code=cum2.Code
            and one.FiscalPrd-1=cum2.FiscalPrd
            and one.PointDate >= cum2.PointDate
    left outer join
        (Select Code, CONVERT(varchar(8),PointDate,112) as PointDate,
                max(FreqCode) as FreqCode,
                FiscalPrd, Item, Value_,
                CONVERT(varchar(8), CalPrdEndDate,112) as CalPrdEndDate
            from {0} with (nolock)
            where (FreqCode in (4,9) or (FreqCode in (3,8) and (FiscalPrd%4+1)=1))
            and Value_<>  -1e+38
            group by Code, PointDate, Item, FiscalPrd, Value_, CalPrdEndDate) cum3
                on one.Item=cum3.Item
            and one.Code=cum3.Code
            and one.FiscalPrd-2=cum3.FiscalPrd
            and one.PointDate >= cum3.PointDate
    left outer join WSPITCalPrd cal
        on one.Code=cal.Code and one.FreqCode=cal.FreqCode
    and one.FiscalPrd=cal.FiscalPrd and one.PointDate=cal.PointDate
    left join WSPITCode cod on cal.UpdTypeCode=cod.Code and cod.Type_=13
    ) two
    where two.filt_rank=1
    and (two.VALUE is not NULL or two.VALUE_semi is not NULL)
    and datediff(mm, two.CalPrdEndDate, two.adj_PointDate) <18
    ) three
    where three.RANK_NUM=1
    order by Item, Code, FiscalPrd, adj_PointDate, PointDate, Prior desc
    """.format(Table, list2sqlstr(Item), list2sqlstr(Code))
    CONNECT_QAD = connectDB(ODBC_NAME="MSSQL_QAD")
    DT = pd.read_sql(Sql_S, CONNECT_QAD)
    CONNECT_QAD.close()

    # Drop duplicates
    DT = DT.drop_duplicates(
        ['Code', 'Item', 'FiscalPrd', 'adj_PointDate', 'Value_', 'Value_semi'],
        keep='first')
    DT = DT.drop_duplicates(
        ['Code', 'Item', 'FiscalPrd', 'adj_PointDate'], keep='first')

    if DT.shape[0] > 0:
        # Fill qtrValue with semiValue - prev_qtrValue
        PT = DT[DT['Value_semi'].notnull() & DT['Value_'].isnull()].copy()
        PT_ = DT[~(DT['Value_semi'].notnull() & DT['Value_'].isnull())].copy()
        if PT.shape[0] > 0:
            PT['key'] = PT['FiscalPrd'] - 1
            DT_tmp = DT.rename(columns={
                'adj_PointDate': 'p_Date', 'FreqCode': 'p_FreqCode',
                'FiscalPrd': 'key', 'Value_': 'p_Value_', 'Prior': 'p_Prior'})
            cols = ['Code', 'Item', 'p_Date', 'p_FreqCode', 'key', 'p_Value_', 'p_Prior']
            PT_add = pd.merge(PT, DT_tmp.loc[DT_tmp['p_Value_'].notnull(), cols],
                              on=['Code', 'Item', 'key'], how='left')
            PT_add = PT_add[PT_add['p_Date'].isnull() |
                            (PT_add['adj_PointDate'] >= PT_add['p_Date'])]
            if PT_add.shape[0] > 0:
                PT_add = PT_add.sort_values(
                    ['Code', 'Item', 'FiscalPrd', 'adj_PointDate', 'Prior',
                     'key', 'p_Date', 'p_Prior'])
                PT_add = PT_add.drop_duplicates(
                    ['Code', 'Item', 'FiscalPrd', 'adj_PointDate', 'Prior', 'key'],
                    keep='last')
                PT_add['Value_'] = PT_add['Value_semi'] - PT_add['p_Value_']
                excl_cols = ['key', 'p_Date', 'p_FreqCode', 'p_Value_', 'p_Prior']
                PT_add = PT_add[PT_add.columns[~PT_add.columns.isin(excl_cols)]]
                DT1 = PT_.append(PT_add)
                DT1 = DT1.sort_values(
                    ['Code', 'Item', 'FiscalPrd', 'adj_PointDate']
                ).reset_index(drop=True)
            else:
                DT1 = PT_.copy()
        else:
            DT1 = PT_.copy()

        # Fill semiValue with qtrValue + prev_qtrValue
        PT = DT1[DT1['Value_semi'].isnull() & DT1['Value_'].notnull()].copy()
        PT_ = DT1[~(DT1['Value_semi'].isnull() & DT1['Value_'].notnull())].copy()
        if PT.shape[0] > 0:
            PT['key'] = PT['FiscalPrd'] - 1
            DT_tmp = DT1.rename(columns={
                'adj_PointDate': 'p_Date', 'FreqCode': 'p_FreqCode',
                'FiscalPrd': 'key', 'Value_': 'p_Value_', 'Prior': 'p_Prior'})
            cols = ['Code', 'Item', 'p_Date', 'p_FreqCode', 'key', 'p_Value_', 'p_Prior']
            PT_add = pd.merge(PT, DT_tmp.loc[DT_tmp['p_Value_'].notnull(), cols],
                              on=['Code', 'Item', 'key'], how='left')
            PT_add = PT_add[PT_add['p_Date'].isnull() |
                            (PT_add['adj_PointDate'] >= PT_add['p_Date'])]
            PT_add = PT_add.sort_values(
                ['Code', 'Item', 'FiscalPrd', 'adj_PointDate', 'Prior',
                 'key', 'p_Date', 'p_Prior'])
            PT_add = PT_add.drop_duplicates(
                ['Code', 'Item', 'FiscalPrd', 'adj_PointDate', 'Prior', 'key'],
                keep='last')
            PT_add['Value_semi'] = PT_add['Value_'] + PT_add['p_Value_']
            excl_cols = ['key', 'p_Date', 'p_FreqCode', 'p_Value_', 'p_Prior']
            PT_add = PT_add[PT_add.columns[~PT_add.columns.isin(excl_cols)]]
            DT2 = PT_.append(PT_add)
            DT2 = DT2.sort_values(
                ['Code', 'Item', 'FiscalPrd', 'adj_PointDate']
            ).reset_index(drop=True)
        else:
            DT2 = PT_.copy()
        DT = DT2

        # Get Summation by 4 Quarter
        # Make 'DT_': has each FiscalPrd with 4 prev FiscalPrd_back
        cols = ['Item', 'Code', 'FiscalPrd', 'FiscalPrd2', 'adj_PointDate']
        DT_ = DT.loc[DT['Value_'].notnull(), cols].copy()
        if DT_.shape[0] > 0:
            DT_ = rm_backward(DT_, sort_col=['Item', 'Code', 'adj_PointDate', 'FiscalPrd'],
                            grp_col=['Item', 'Code'], filter_col='FiscalPrd')
        DT_['m1'], DT_['m2'], DT_['m3'], DT_['m4'] = [0, 1, 2, 3]

        DT_ = DT_.melt(id_vars=cols, value_vars=['m1', 'm2', 'm3', 'm4'],
                       var_name='tmp', value_name='minus')
        DT_ = DT_.drop('tmp', axis=1)
        DT_['FiscalPrd_back'] = DT_['FiscalPrd'] - DT_['minus']
        DT_ = DT_.drop('minus', axis=1)
        DT_ = DT_.rename(columns={'adj_PointDate': 'ref_Point'})

        # Make 'DT_1': excludes FiscalPrd, FiscalPrd2 to join tables
        # Value_ aligns with FiscalPrd_back
        cols2 = ['Item', 'Code', 'PointDate', 'adj_PointDate', 'FreqCode',
                 'FiscalPrd', 'Value_', 'FILLyr',
                 'CalPrdEndDate', 'maxUseDate']
        DT_1 = DT.loc[DT['Value_'].notnull(), cols2].copy()
        DT_1 = DT_1.rename(columns={'FiscalPrd': 'FiscalPrd_back'})
        DT_proc = pd.merge(DT_, DT_1, on=['Item', 'Code', 'FiscalPrd_back'], how='inner')
        DT_proc = DT_proc.sort_values(
            ['Item', 'Code', 'ref_Point', 'FiscalPrd', 'FiscalPrd_back', 'adj_PointDate'])

        # ref_Point is current date. adj_PointDate should be less than now
        DT_proc_ = DT_proc[DT_proc['ref_Point'] >= DT_proc['adj_PointDate']].copy()
        # sort to remove duplicates of FiscalPrd_back point-in-time
        DT_proc_0 = DT_proc_.drop_duplicates(
            ['Code', 'Item', 'ref_Point', 'FiscalPrd', 'FiscalPrd_back'], keep='last').copy()
        if DT_proc_0.shape[0] > 0:
            DT_proc_0['CNT'] = DT_proc_0.groupby(
                ['Code', 'Item', 'ref_Point', 'FiscalPrd'])['PointDate'].transform(len)
        else:
            DT_proc_0['CNT'] = 0
        DT_proc_0 = DT_proc_0[DT_proc_0['CNT'] >= 2]

        # Get Summation by 2 Semi's
        # Make 'DT_': has each FiscalPrd with 4 prev FiscalPrd_back
        cols = ['Item', 'Code', 'FiscalPrd', 'FiscalPrd2', 'adj_PointDate']
        DT2_ = DT.loc[DT['Value_semi'].notnull(), cols].copy()
        DT2_ = rm_backward(DT2_, sort_col=['Item', 'Code', 'adj_PointDate', 'FiscalPrd'],
                           grp_col=['Item', 'Code'], filter_col='FiscalPrd')
        # DT2_ = DT.loc[DT['Value_semi'].notnull(), cols].copy().drop_duplicates(keep='last')
        DT2_['m1'], DT2_['m2'] = [0, 2]

        DT2_ = DT2_.melt(id_vars=cols, value_vars=['m1', 'm2'],
                         var_name='tmp', value_name='minus')
        DT2_ = DT2_.drop('tmp', axis=1)
        DT2_['FiscalPrd_back'] = DT2_['FiscalPrd'] - DT2_['minus']
        DT2_ = DT2_.drop('minus', axis=1)
        DT2_ = DT2_.rename(columns={'adj_PointDate': 'ref_Point'})

        # Make 'DT_1': excludes FiscalPrd, FiscalPrd2 to join tables
        # Value_ aligns with FiscalPrd_back
        cols2 = ['Item', 'Code', 'PointDate', 'adj_PointDate', 'FreqCode',
                 'FiscalPrd', 'Value_semi', 'FILLyr',
                 'CalPrdEndDate', 'maxUseDate']
        DT2_1 = DT.loc[DT['Value_semi'].notnull(), cols2].copy()
        DT2_1 = DT2_1.rename(columns={'FiscalPrd': 'FiscalPrd_back', 'Value_semi': 'Value_'})
        DT2_proc = pd.merge(DT2_, DT2_1, on=['Item', 'Code', 'FiscalPrd_back'], how='inner')
        DT2_proc = DT2_proc.sort_values(
            ['Item', 'Code', 'ref_Point', 'FiscalPrd', 'FiscalPrd_back', 'adj_PointDate'])

        # ref_Point is current date. adj_PointDate should be less than now
        DT2_proc_ = DT2_proc[DT2_proc['ref_Point'] >= DT2_proc['adj_PointDate']].copy()
        # sort to remove duplicates of FiscalPrd_back point-in-time
        DT2_proc_0 = DT2_proc_.drop_duplicates(
            ['Code', 'Item', 'ref_Point', 'FiscalPrd', 'FiscalPrd_back'], keep='last').copy()
        DT2_proc_0['CNT'] = DT2_proc_0.groupby(
            ['Code', 'Item', 'ref_Point', 'FiscalPrd'])['PointDate'].transform(len)
        DT2_proc_0 = DT2_proc_0[DT2_proc_0['CNT'] >= 1]

        # Add Qtr's together
        DT_proc_0 = DT_proc_0[DT_proc_0['CNT'] == 4]
        if DT_proc_0.shape[0] > 0:
            DT_sfin = DT_proc_0.groupby(
                ['Item', 'Code', 'FiscalPrd', 'FiscalPrd2', 'ref_Point',
                 'FILLyr'], as_index=False
            ).agg({'Value_': np.sum, 'CalPrdEndDate': max, 'maxUseDate': max})
            DT_sfin = DT_sfin.rename(columns={'ref_Point': 'BASE_DT'})
        else:
            DT_sfin = pd.DataFrame(columns=[
                'Item', 'Code', 'FiscalPrd', 'FiscalPrd2', 'BASE_DT', 'FILLyr',
                'Value_', 'CalPrdEndDate', 'maxUseDate'])

        # Add Semi's together
        DT2_proc_0 = DT2_proc_0[DT2_proc_0['CNT'] == 2]
        if DT2_proc_0.shape[0] > 0:
            DT2_sfin = DT2_proc_0.groupby(
                ['Item', 'Code', 'FiscalPrd', 'FiscalPrd2', 'ref_Point',
                 'FILLyr'], as_index=False
            ).agg({'Value_': np.sum, 'CalPrdEndDate': max, 'maxUseDate': max})
            DT2_sfin = DT2_sfin.rename(columns={'ref_Point': 'BASE_DT'})
        else:
            DT2_sfin = pd.DataFrame(columns=[
                'Item', 'Code', 'FiscalPrd', 'FiscalPrd2', 'BASE_DT', 'FILLyr',
                'Value_', 'CalPrdEndDate', 'maxUseDate'])

        # Combine 4Qtr_output and 2Semi_output together
        DT_sfin['num'] = 1
        DT2_sfin['num'] = 2
        DT_fin = DT_sfin.append(DT2_sfin).sort_values(
            ['Code', 'Item', 'BASE_DT', 'FiscalPrd', 'num'])
        DT_fin = DT_fin.drop_duplicates(
            ['Code', 'Item', 'BASE_DT', 'FiscalPrd'], keep='first')
        DT_fin = DT_fin.drop('num', axis=1)

        if DT_fin.shape[0] > 0:
            DT_fin = rm_backward(DT_fin, ['Item', 'Code', 'BASE_DT'],
                                 ['Item', 'Code'], 'FiscalPrd', print_=True)
            DT_fin = DT_fin.reset_index(drop=True)

        else:
            DT_fin = pd.DataFrame(columns=[
                'Item', 'Code', 'BASE_DT', 'FiscalPrd', 'FiscalPrd2', 'FILLyr',
                'Value_', 'CalPrdEndDate', 'maxUseDate'])

    else:
        DT_fin = pd.DataFrame(columns=fin_col)

    DT_fin = _conv2strCol(DT_fin)
    return DT_fin


# Needed for Payout Field!!
def WS_qtr_currToHist(Item='9502', Table='WSPITCmpIssFData',
                      Code=['6751', '6809'], bkfil=True, **kwargs):
    """
    Used for converting current value item, Payout,
    to Historical Field with FiscalPrd
    """

    defaults = {'St_dt': '19951231', 'maxMonths': 8, 'add_lback': 10}
    defaults.update(kwargs)
    maxMonths = defaults['maxMonths']
    St_dt = defaults['St_dt']
    add_lback = defaults['add_lback']

    fin_col = ['Item', 'Code', 'BASE_DT', 'FiscalPrd', 'FiscalPrd2',
               'Value_', 'CalPrdEndDate', 'maxUseDate']

    TMP = WS_qtr(Item='2999', Table='WSPITFinVal', St_dt=St_dt,
                          Code=Code, maxMonths=maxMonths, bkfil=bkfil)
    REF = TMP.groupby(['Item', 'Code', 'FiscalPrd', 'FiscalPrd2', 'CalPrdEndDate'],
                      as_index=False)['BASE_DT'].min()
    REF = REF.rename(columns={'BASE_DT': 'min_DT'})

    Sql_S = """
    Select Item, Code, convert(varchar(8), StartDate, 112) as BASE_DT,
        convert(varchar(8), dateadd(mm, {0}, StartDate), 112) as maxUseDate,
        Value_
    from {1} with (nolock)
    where Item in ({2})
    and Code in ({3})
    and Value_ <> -1e38
    """.format(maxMonths, Table, list2sqlstr(Item), list2sqlstr(Code))
    if not bkfil:
        Sql_S += "and StartDate >= dateadd(M, -{}, convert(varchar(8), dateadd(d, -2, GETDATE()), 112))\n".format(
            maxMonths + add_lback)
    CONNECT_QAD = connectDB(ODBC_NAME="MSSQL_QAD")
    DT = pd.read_sql(Sql_S, CONNECT_QAD)
    CONNECT_QAD.close()
    DT = _conv2strCol(DT)

    if DT.shape[0] > 0:
        REF_ = REF.drop('Item', axis=1)
        A = pd.merge(DT, REF_, on='Code', how='inner')
        A = A[A['BASE_DT'] >= A['min_DT']]
        DT_fin = A.sort_values(['Code', 'Item', 'BASE_DT', 'min_DT']
                               ).drop_duplicates(
            ['Code', 'Item', 'BASE_DT'], keep='last')
        DT_fin = DT_fin.drop('min_DT', axis=1)
        DT_fin = DT_fin.reset_index(drop=True)

    else:
        DT_fin = pd.DataFrame(columns=fin_col)

    DT_fin = _conv2strCol(DT_fin)
    return(DT_fin)
