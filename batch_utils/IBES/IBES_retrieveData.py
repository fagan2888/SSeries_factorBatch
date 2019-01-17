import pandas as pd
import numpy as np
from ..utils_db_alch2 import connectDB

from ..ItemInfo import IBESItem_lst
from ..common import list2sqlstr, chg_to_type


def IBES_year(Code=['28', '30'], St_Dt='19911231', **kwargs):
    default = {'TableMeasure': 8, 'TableAct': 'IBESActL1', 'TableEst': 'IBESEstL1',
               'var_Name': 'EPS', 'delay_type': 'dd', 'delay_per': 50}
    default.update(kwargs)
    TableMeasure = default['TableMeasure']
    TableAct = default['TableAct']
    TableEst = default['TableEst']
    var_Name = default['var_Name']
    delay_type = default['delay_type']
    delay_per = default['delay_per']

    Sql_S = """
    Select aa.Code, aa.Measure,  aa.PerType, convert(varchar(8), aa.EstDate, 112) as EstDate,
        convert(varchar(8), dateadd({0}, {1}, aa.EstDate), 112) as maxUseDate,
        convert(varchar(8), aa_.PerDate0,112) as FY0, aa_.ActValue0,
        convert(varchar(8), aa_.PriceDate,112) as PxDate0, aa_.Price as Px0, aa_.Shares as Sh0,
        convert(varchar(8),aa.PerDate1,112) as FY1, aa.Median1, aa.Mean1,
        convert(varchar(8),bb.PerDate2,112) as FY2, bb.Median2, bb.Mean2,
        convert(varchar(8),cc.PerDate3,112) as FY3, cc.Median3, cc.Mean3,
        DATEDIFF(mm, aa.EstDate, aa.PerDate1) as DTdiff1
    from (
    Select Code, Measure, PerType, EstDate, PerDate as PerDate1,
        NumEst as NumEst1, NumUp as NumUp1, NumDown as NumDown1,
        Median as Median1, Mean as Mean1, StdDev as StdDev1, High as High1, Low as Low1
    from {2} with (nolock)
    where PerType='1'and Period='1'
    and dateadd(mm, -4, EstDate) < PerDate
    ) aa
    left outer join (
    Select Code, Measure, Date_, FYDate as PerDate0, FYValue as ActValue0, PriceDate, Price, shares
    from {3} with (nolock)
    ) aa_
        on aa.Code=aa_.Code
    and aa.Measure=aa_.Measure
    and aa.EstDate=aa_.Date_
    left outer join (
    Select Code, Measure, EstDate, PerType, PerDate as PerDate2,
        NumEst as NumEst2, NumUp as NumUp2, NumDown as NumDown2,
        Median as Median2, Mean as Mean2, StdDev as StdDev2, High as High2, Low as Low2
    from {2} with (nolock)
    where PerType='1'and Period='2'
    and dateadd(mm, -16, EstDate) < PerDate
    ) bb
        on aa.Code=bb.Code
    and aa.Measure=bb.Measure
    and aa.EstDate=bb.EstDate
    and aa.PerType=bb.PerType
    left outer join (
    Select Code, Measure, EstDate, PerType, PerDate as PerDate3,
        NumEst as NumEst3, NumUp as NumUp3, NumDown as NumDown3,
        Median as Median3, Mean as Mean3, StdDev as StdDev3, High as High3, Low as Low3
    from {2} with (nolock)
    where PerType='1'and Period='3'
    and dateadd(mm, -28, EstDate) < PerDate
    ) cc
        on aa.Code=cc.Code
    and aa.Measure=cc.Measure
    and aa.EstDate=cc.EstDate
    and aa.PerType=cc.PerType
    where aa.Code in ({4})
    and aa.Measure='{5}'
    and aa.EstDate >= '{6}'
    """.format(delay_type, delay_per, TableEst, TableAct, list2sqlstr(Code), TableMeasure, St_Dt)
    CONNECT_QAD = connectDB(ODBC_NAME="MSSQL_QAD")
    DF = pd.read_sql(Sql_S, CONNECT_QAD)
    CONNECT_QAD.close()

    # convert null values
    def null_replace(x):
        x.replace([-99999, -9999, '-99999', '-9999'], np.nan, inplace=True)

    cols = ['ActValue0', 'Px0', 'Sh0', 'Median1', 'Mean1',
            'Median2', 'Mean2', 'Median3', 'Mean3']
    for col_ in cols:
        null_replace(DF[col_])

    cols = ['ActValue0', 'Px0', 'Sh0', 'Median1', 'Mean1',
            'Median2', 'Mean2', 'Median3', 'Mean3']
    DF = chg_to_type(DF, cols, type_=float)
    DF = chg_to_type(DF, ['EstDate', 'FY0', 'FY1', 'FY2', 'FY3'], type_=str)

    # start wrangle
    DF = DF[DF['EstDate'] >= St_Dt]
    DF['a0'] = DF['DTdiff1'].apply(lambda x: x / 12 if x / 12 > 0 else 0)

    def tmp1(x):
        a = (12 - x) / 12 if (12 - x) / 12 < 1 else 1
        b = (-x / 12) if (-x / 12) > 0 else 0
        return a - b

    DF['a1'] = DF['DTdiff1'].apply(tmp1)
    DF['a1_'] = DF['DTdiff1'].apply(lambda x: (12 - x) / 12 if (12 - x) / 12 < 1 else 1)
    DF['a2'] = DF['DTdiff1'].apply(lambda x: (-x / 12) if (-x / 12) > 0 else 0)

    # FY0, 1, 2 Values
    def test(x, val, wgt):
        """
        2-value vector sum-product with when
        sum of weight equals 1 without 'nan' value
        """
        if pd.isnull(x[val[0]]) & (x[wgt[1]] == 1):
            return x[wgt[1]] * x[val[1]]
        else:
            return x[wgt[0]] * x[val[0]] + x[wgt[1]] * x[val[1]]
    DF[var_Name + '_fy0'] = DF.apply(lambda x:
                                     test(x, ['ActValue0', 'Median1'], ['a0', 'a1_']), axis=1)

    def test2(x, val, wgt):
        """
        3-value vector sum-product
        with exception case
        """
        if (x[wgt[-1]] < 0.15) & pd.isnull(x[val[-1]]):
            out = 0
            for i in range(len(val) - 1):
                out += x[val[i]] * (x[wgt[i]] / (x[wgt[0]] + x[wgt[1]]))
            return out
        else:
            out = 0
            for i in range(len(val)):
                out += x[val[i]] * x[wgt[i]]
            return out
    DF[var_Name + '_fy1'] = DF.apply(lambda x:
                                     test2(x, ['Median1', 'Median2', 'Median3'], ['a0', 'a1', 'a2']), axis=1)
    DF[var_Name + '_fy2'] = DF.apply(lambda x:
                                     test(x, ['Median2', 'Median3'], ['a0', 'a1_']), axis=1)

    # 2Yr Growth (Annualized)
    def test3(x, val):
        if pd.isnull(x[val[0]]) | pd.isnull(x[val[-1]]):
            return np.nan
        elif (x[val[0]] < 0) & (x[val[-1]] < 0):
            return -5555
        elif x[val[0]] <= 0:
            return 10000
        elif x[val[-1]] < 0:
            return -10000
        else:
            return 100 * ((x[val[-1]] / x[val[0]]) ** (1 / 2) - 1)
    DF[var_Name + 'g_2yr'] = DF.apply(lambda x: test3(x, [var_Name + '_fy0', var_Name + '_fy2']), axis=1)

    # 2Yr Change (%)
    def test4(x, val):
        return x[val[-1]] - x[val[0]]
    DF[var_Name + 'chg_2yr'] = DF.apply(lambda x: test4(x, [var_Name + '_fy0', var_Name + '_fy2']), axis=1)

    cols = DF.columns[~DF.columns.isin([
        'DTdiff1', 'a0', 'a1', 'a1_', 'a2'])]
    DF = DF[cols].copy()

    # 12M Ratio
    def tmp_ratio(x, val):
        a = np.nan if pd.isnull(x[val[0]]) else x[val[0]]
        b = np.nan if x[val[-1]] <= 0 else x[val[-1]]
        return a / b
    DF['Ratio_f12m'] = DF.apply(lambda x: tmp_ratio(x, ['Px0', var_Name + '_fy1']), axis=1)

    # Finalize
    DF = DF.sort_values(['Code', 'EstDate', 'FY0']).reset_index(drop=True)
    DF[['Code', 'Measure']] = DF[['Code', 'Measure']].astype(str)
    return DF


def IBES_year_Ratio(Code=['28', '30'], St_Dt='19911231', **kwargs):
    default = {'TableMeasure': 29, 'TableAct': 'IBESActL3', 'TableEst': 'IBESEstL3',
               'var_Name': 'ROE', 'delay_type': 'dd', 'delay_per': 50}
    default.update(kwargs)
    TableMeasure = default['TableMeasure']
    TableAct = default['TableAct']
    TableEst = default['TableEst']
    var_Name = default['var_Name']
    delay_type = default['delay_type']
    delay_per = default['delay_per']

    Sql_S = """
    Select aa.Code, aa.Measure,  aa.PerType, convert(varchar(8), aa.EstDate, 112) as EstDate 
        , convert(varchar(8), dateadd({0}, {1}, aa.EstDate), 112) as maxUseDate
        , convert(varchar(8), aa_.PerDate0,112) as FY0, aa_.ActValue0                     
        , convert(varchar(8), aa_.PriceDate,112) as PxDate0, aa_.Price as Px0, aa_.Shares as Sh0
        , convert(varchar(8),aa.PerDate1,112) as FY1, aa.Median1, aa.Mean1                
        , convert(varchar(8),bb.PerDate2,112) as FY2, bb.Median2, bb.Mean2                  
        , convert(varchar(8),cc.PerDate3,112) as FY3, cc.Median3, cc.Mean3                  
        , DATEDIFF(mm, aa.EstDate, aa.PerDate1) as DTdiff1                                  
    from (                                                                                 
    Select Code, Measure, PerType, EstDate, PerDate as PerDate1                              
        , NumEst as NumEst1, NumUp as NumUp1, NumDown as NumDown1                         
        , Median as Median1, Mean as Mean1, StdDev as StdDev1, High as High1, Low as Low1 
    from {2} with (nolock)                                          
    where PerType='1'and Period='1'                                                         
    and dateadd(mm, -4, EstDate) < PerDate                                                
    ) aa                                                                                     
    left outer join (                                                                      
    Select Code, Measure, Date_, FYDate as PerDate0, FYValue as ActValue0, PriceDate, Price, shares
    from {3} with (nolock)                                          
    ) aa_                                                                                    
        on aa.Code=aa_.Code                                                                  
    and aa.Measure=aa_.Measure                                                            
    and aa.EstDate=aa_.Date_                                                              
    left outer join (                                                                      
    Select Code, Measure, EstDate, PerType, PerDate as PerDate2                              
        , NumEst as NumEst2, NumUp as NumUp2, NumDown as NumDown2                         
        , Median as Median2, Mean as Mean2, StdDev as StdDev2, High as High2, Low as Low2 
    from {2} with (nolock)
    where PerType='1'and Period='2'                                                         
    and dateadd(mm, -16, EstDate) < PerDate                                               
    ) bb                                                                                     
        on aa.Code=bb.Code                                                                   
    and aa.Measure=bb.Measure                                                             
    and aa.EstDate=bb.EstDate                                                             
    and aa.PerType=bb.PerType                                                             
    left outer join (                                                                      
    Select Code, Measure, EstDate, PerType, PerDate as PerDate3                              
        , NumEst as NumEst3, NumUp as NumUp3, NumDown as NumDown3                         
        , Median as Median3, Mean as Mean3, StdDev as StdDev3, High as High3, Low as Low3 
    from {2} with (nolock)
    where PerType='1'and Period='3'                                                         
    and dateadd(mm, -28, EstDate) < PerDate                                               
    ) cc                                                                                     
        on aa.Code=cc.Code                                                                   
    and aa.Measure=cc.Measure                                                             
    and aa.EstDate=cc.EstDate                                                             
    and aa.PerType=cc.PerType                                                             
    where aa.Code in ({4})                                        
    and aa.Measure='{5}'                                          
    and aa.EstDate >= '{6}'                                       
    """.format(delay_type, delay_per, TableEst, TableAct, list2sqlstr(Code), TableMeasure, St_Dt)
    CONNECT_QAD = connectDB(ODBC_NAME="MSSQL_QAD")
    DF = pd.read_sql(Sql_S, CONNECT_QAD)
    CONNECT_QAD.close()

    # convert null values
    def null_find(x):
        x.replace([-99999, -9999, '-99999', '-9999'], np.nan, inplace=True)

    cols = ['ActValue0', 'Px0', 'Sh0', 'Median1', 'Mean1',
            'Median2', 'Mean2', 'Median3', 'Mean3']
    for col_ in cols:
        null_find(DF[col_])

    # adjust datatype
    def chg_to_type(DF, chg_col, type_=float):
        tmp = DF[chg_col].dtypes == type_
        chg_col_ = tmp[~tmp]
        if chg_col_.shape[0] > 0:
            for col in chg_col_.index:
                DF[col] = DF[col].astype(type_)
        return DF
    cols = ['ActValue0', 'Px0', 'Sh0', 'Median1', 'Mean1',
            'Median2', 'Mean2', 'Median3', 'Mean3']
    DF = chg_to_type(DF, cols, type_=float)
    DF = chg_to_type(DF, ['EstDate', 'FY0', 'FY1', 'FY2', 'FY3'], type_=str)

    # start wrangle
    DF = DF[DF['EstDate'] >= St_Dt]
    DF['a0'] = DF['DTdiff1'].apply(lambda x: x / 12 if x / 12 > 0 else 0)

    def tmp1(x):
        a = (12 - x) / 12 if (12 - x) / 12 < 1 else 1
        b = (-x / 12) if (-x / 12) > 0 else 0
        return a - b

    DF['a1'] = DF['DTdiff1'].apply(tmp1)
    DF['a1_'] = DF['DTdiff1'].apply(lambda x: (12 - x) / 12 if (12 - x) / 12 < 1 else 1)
    DF['a2'] = DF['DTdiff1'].apply(lambda x: (-x / 12) if (-x / 12) > 0 else 0)

    # FY0, 1, 2 Values
    def test(x, val, wgt):
        """
        2-value vector sum-product with when
        sum of weight equals 1 without 'nan' value
        """
        if pd.isnull(x[val[0]]) & (x[wgt[1]] == 1):
            return x[wgt[1]] * x[val[1]]
        else:
            return x[wgt[0]] * x[val[0]] + x[wgt[1]] * x[val[1]]
    DF[var_Name + '_fy0'] = DF.apply(lambda x:
                                     test(x, ['ActValue0', 'Median1'], ['a0', 'a1_']), axis=1)

    def test2(x, val, wgt):
        """
        3-value vector sum-product
        with exception case
        """
        if (x[wgt[-1]] < 0.15) & pd.isnull(x[val[-1]]):
            out = 0
            for i in range(len(val) - 1):
                out += x[val[i]] * (x[wgt[i]] / (x[wgt[0]] + x[wgt[1]]))
            return out
        else:
            out = 0
            for i in range(len(val)):
                out += x[val[i]] * x[wgt[i]]
            return out
    DF[var_Name + '_fy1'] = DF.apply(lambda x:
                                     test2(x, ['Median1', 'Median2', 'Median3'], ['a0', 'a1', 'a2']), axis=1)
    DF[var_Name + '_fy2'] = DF.apply(lambda x:
                                     test(x, ['Median2', 'Median3'], ['a0', 'a1_']), axis=1)

    cols = DF.columns[~DF.columns.isin(['DTdiff1', 'a0', 'a1', 'a1_', 'a2'])]
    DF = DF[cols].copy()

    # Finalize
    DF = DF.sort_values(['Code', 'EstDate', 'FY0']).reset_index(drop=True)

    return DF
