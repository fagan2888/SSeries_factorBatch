import pandas as pd
import numpy as np
import time
import re
import datetime as dt

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

def chunker_count(seq, size):
    return (int(len(seq) / size) + int(len(seq) % size != 0))

def chk_dateFormat(x):
    out = re.match('^\d{8}$', x)
    return True if out is not None else False

def monthdelta(date, delta):
    m, y = (date.month+delta) % 12, date.year + ((date.month)+delta-1) // 12
    if not m: m = 12
    d = min(date.day, [31,
        29 if y%4==0 and not y%400==0 else 28,31,30,31,30,31,31,30,31,30,31][m-1])
    return date.replace(day=d, month=m, year=y)

# Deprecated:
# def chunker2(seq, size):
#     return [seq[pos:pos + size] for pos in range(0, len(seq), size)]


# worldscope fiscal_quarter into xxxx'Q'x format
def decode_FiscalPrd(x):
    return str(int(x / 4)) + 'Q' + str(x % 4 + 1)


# Batch update messeging (updating)
def batch_monitor_msg(i, tot_n, st_time, add_msg=None):
    tmp_ = time.time() - st_time
    if add_msg is None:
        print('Calculating Batch {} / {} ({:04.1f}%) - {:02d}m{:02d}s elapsed'.format(
            i + 1, tot_n, (i + 1) * 100 / tot_n,
            int(tmp_ / 60), int(tmp_ % 60)), end='\r')
    else:
        print('Calculating Batch {} / {} ({:04.1f}%) - {:02d}m{:02d}s elapsed: {}'.format(
            i + 1, tot_n, (i + 1) * 100 / tot_n,
            int(tmp_ / 60), int(tmp_ % 60), add_msg), end='\r')


# Batch update messeging (complete)
def batch_finish_msg(tot_n, st_time):
    tmp_ = time.time() - st_time
    print('Done! {0} / {0} (100%) - {1:02d}m{2:02d}s (Total Time)\n'.format(
        tot_n, int(tmp_ / 60), int(tmp_ % 60)))


# Delete duplicates
def rm_backward(DT, sort_col, grp_col, filter_col, print_=False):
    """
    filter_col must be 1-dim column : dtype -> str
    ex) sort_col = ['Item', 'Code', 'adj_PointDate', 'FiscalPrd']
        grp_col = ['Item', 'Code']
        filter_col = 'FiscalPrd'
        > When 'adj_PointDate' is updated but 'FiscalPrd' points previous fiture, drops it.
    """
    m = 0
    while True:
        DT = DT.sort_values(sort_col)
        DT['filter'] = DT.groupby(grp_col)[filter_col].transform(
            lambda x: pd.Series(np.append(1, np.diff(x)), index=x.index))
        if (DT['filter'] < 0).sum() == 0:
            DT = DT.drop('filter', axis=1)
            DT = DT.reset_index(drop=True)
            break
        DT = DT[DT['filter'] >= 0]
        DT = DT.drop('filter', axis=1)
        if print_:
            print(m, '_')
        m += 1
    return DT


# Convert key-columns to string-type
def _conv2strCol(DF):
    """
    Any column within
    ['Item', 'Code', 'BASE_DT', 'FiscalPrd', 'FiscalPrd2']
    is converted to string
    """
    str_cols = ['Item', 'Code', 'BASE_DT', 'FiscalPrd', 'FiscalPrd2']
    cols = DF.columns[DF.columns.isin(str_cols)]
    DF[cols] = DF[cols].astype(str)
    return DF


# Convert given columns to wanted type
def chg_to_type(DF, chg_col, type_=str):
    if chg_col is None:
        return DF
    else:
        DF[chg_col] = DF[chg_col].astype(type_)
        return DF


# Used for SQL converting list to 'x', 'y' order
def list2sqlstr(x):
    if isinstance(x, str):
        return "'{}'".format(x)
    else:
        return "'" + "', '".join(x) + "'"


# Used for Code -> TMSRS_CD mapping with startDT & endDT map
def add_mapped_tick(DF, map, on=['Code'], chk_col='StyleName'):
    if chk_col in DF.columns:
        chk_ = [chk_col]
    else:
        chk_ = []
    map.loc[map['endDT'].isnull(), 'endDT'] = '99991231'

    DF_ = pd.merge(DF, map, on=on)
    DF_ = DF_[(DF_['BASE_DT'] >= DF_['startDT']) & (DF_['BASE_DT'] <= DF_['endDT'])]
    DF_.sort_values(['BASE_DT', 'TMSRS_CD'] + on + ['startDT'],
                    ascending=True, inplace=True)
    DF_.drop_duplicates(
        subset=['BASE_DT', 'TMSRS_CD'] + on + chk_, keep='last', inplace=True)

    DF_.sort_values(['BASE_DT', 'TMSRS_CD', 'startDT'],
                    ascending=True, inplace=True)
    DF_.drop_duplicates(
        subset=['BASE_DT', 'TMSRS_CD'] + chk_, keep='last', inplace=True)
    DF_.reset_index(drop=True, inplace=True)

    DF_.drop(['startDT', 'endDT'], axis=1, inplace=True)
    return DF_


# Operation for Searching Date, Code Duplicates
def firstCheck_duplicates(*args, add_cols=None):
    print('>>> Checking Date, Code Duplicates')
    try:
        if add_cols is None:
            add_cols = []
        for i, df in enumerate(args):
            assert df.duplicated(subset=['BASE_DT', 'Code'] + add_cols).sum() == 0, (
                'Found Duplicates! Please Check')
            print('table#{}: OK!'.format(i))
        print('\n')
    except:
        print('table#{}: ERROR!'.format(i))
        err_log = df.loc[df.duplicated(subset=['BASE_DT', 'Code'] + add_cols),
                         ['BASE_DT', 'Code'] + add_cols].copy()
        print(err_log)
        err_log.to_csv('ERROR_LOG_{}.log'.format(dt.date.today().strftime('%Y%m%d')),
                       index=False, mode='a', header=True, sep=',')
        raise AssertionError('Check Above Records-')


# Check if Needed columns are present for uploading
must = set(
    ['BASE_DT', 'TMSRS_CD', 'Code', 'Value_', 'RGN_TP_CD', 'freq', 'StyleName'])
def secondCheck_columns(*args):
    print('>>> Checking Necessary Columns & NaN & Inf Values')
    for i, df in enumerate(args):
        df_colset = set(df.columns)
        try:
            assert must.issubset(df_colset), (
                'table#{}: Must have {} in columns'.format(
                    i, list(must.difference(df_colset)))
                )
        except:
            print('table#{}: ERROR!'.format(i))
            raise AssertionError('Check Columns..')

        try:
            assert df['Value_'].isnull().sum() == 0, (
                'table#{}: NaN values is Value_. Plz Check Logic'.format(i)
            )
        except:
            print('table#{}: ERROR!'.format(i))
            err_log = df[df['Value_'].isnull()].iloc[:5]
            print(err_log)
            err_log.to_csv('ERROR_LOG_{}.log'.format(dt.date.today().strftime('%Y%m%d')),
                           index=False, mode='a', header=True, sep=',')
            raise AssertionError('Check NaN Values..')

        try:
            assert df['Value_'].isin([np.Inf, -np.Inf]).sum() == 0, (
                'table#{}: Inf values is Value_. Plz Check Logic'.format(i)
            )
        except:
            print('table#{}: ERROR!'.format(i))
            err_log = df[df['Value_'].isin([np.Inf, -np.Inf])].iloc[:5]
            print(err_log)
            err_log.to_csv('ERROR_LOG_{}.log'.format(dt.date.today().strftime('%Y%m%d')),
                           index=False, mode='a', header=True, sep=',')
            raise AssertionError('Check Inf Values..')

        print('table#{}: OK!'.format(i))
    print('\n')


# Save Batch to either 'total_save' or 'batch_save'
def save_batch(bkfil, DF, mapping, fileName):
    import os
    currFolders = [f for f in os.listdir() if ~os.path.isfile(f)]
    if bkfil:
        folder = 'save_total'
        if folder not in currFolders:
            os.mkdir(folder)
        DF.to_pickle('{}/{}_{}.p'.format(folder, mapping, fileName))
    else:
        folder = 'save_batch'
        if folder not in currFolders:
            os.mkdir(folder)
        DF.to_pickle('{}/{}_{}.p'.format(folder, mapping, fileName))

    print('>>> Saved to => {}/{}_{}.p\n'.format(folder, mapping, fileName))
    return


# Check Level of Overlapping in Map-Table
def check_mapping_df(DF, mapCode_nm='Code'):
    cols = ['TMSRS_CD', mapCode_nm, 'startDT', 'endDT', 'RGN_TP_CD']
    codes = DF.loc[DF.duplicated(subset=cols), mapCode_nm]
    if codes.shape[0] > 0:
        print('>>> Mapping has high-level ({}) duplicates'.format(cols))
        out = DF.loc[DF[mapCode_nm].isin(codes)].sort_values([mapCode_nm, 'TMSRS_CD', 'startDT'])
        print('    Duplicates found (searched by {}):'.format(mapCode_nm))
        print(out)
        print('\n')
        return out

    cols = ['TMSRS_CD', mapCode_nm, 'startDT', 'endDT']
    codes = DF.loc[DF.duplicated(subset=cols), mapCode_nm]
    if codes.shape[0] > 0:
        print('>>> Mapping has high-level ({}) duplicates'.format(cols))
        out = DF.loc[DF[mapCode_nm].isin(codes)].sort_values([mapCode_nm, 'TMSRS_CD', 'startDT'])
        print('    Duplicates found (searched by {}):'.format(mapCode_nm))
        print(out)
        print('\n')
        return out

    cols = ['TMSRS_CD', mapCode_nm, 'startDT']
    codes = DF.loc[DF.duplicated(subset=cols), mapCode_nm]
    if codes.shape[0] > 0:
        print('>>> Mapping has high-level ({}) duplicates'.format(cols))
        out = DF.loc[DF[mapCode_nm].isin(codes)].sort_values([mapCode_nm, 'TMSRS_CD', 'startDT'])
        print('    Duplicates found (searched by {}):'.format(mapCode_nm))
        print(out)
        print('\n')
        return out

    cols = ['TMSRS_CD', mapCode_nm, 'endDT']
    codes = DF.loc[DF.duplicated(subset=cols), mapCode_nm]
    if codes.shape[0] > 0:
        print('>>> Mapping has high-level ({}) duplicates'.format(cols))
        out = DF.loc[DF[mapCode_nm].isin(codes)].sort_values([mapCode_nm, 'TMSRS_CD', 'startDT'])
        print('    Duplicates found (searched by {}):'.format(mapCode_nm))
        print(out)
        print('\n')
        return out

    cols = ['TMSRS_CD', mapCode_nm]
    codes = DF.loc[DF.duplicated(subset=cols), mapCode_nm]
    if codes.shape[0] > 0:
        print('>>> Mapping has medium-level ({}) duplicates'.format(cols))
        out = DF.loc[DF[mapCode_nm].isin(codes)].sort_values([mapCode_nm, 'TMSRS_CD', 'startDT'])
        print('    Duplicates found (searched by {}):'.format(mapCode_nm))
        print(out)
        print('\n')
        return out

    cols = [mapCode_nm]
    codes = DF.loc[DF.duplicated(subset=cols), mapCode_nm]
    if codes.shape[0] > 0:
        print('>>> Mapping has low-level ({}) duplicates'.format(cols))
        out = DF.loc[DF[mapCode_nm].isin(codes)].sort_values([mapCode_nm, 'TMSRS_CD', 'startDT'])
        print('    Duplicates found (searched by {}):'.format(mapCode_nm))
        print(out)
        print('\n')
        return out

    cols = ['TMSRS_CD']
    codes = DF.loc[DF.duplicated(subset=cols), mapCode_nm]
    if codes.shape[0] > 0:
        print('>>> Mapping has low-level ({}) duplicates'.format(cols))
        out = DF.loc[DF[mapCode_nm].isin(codes)].sort_values([mapCode_nm, 'TMSRS_CD', 'startDT'])
        print('    Duplicates found (searched by {}):'.format(mapCode_nm))
        print(out)
        print('\n')
        return out
