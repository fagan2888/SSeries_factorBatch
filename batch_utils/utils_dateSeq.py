import pandas as pd
import datetime as dt
from .common import chk_dateFormat

_today = dt.date.today() - dt.timedelta(days=1)

def batch_sequence(option, freq, batch_n=4, rtvDays=2373, ovrd_startDT=None):
    """
    option : either 'backfill' or 'batch'
    freq :   either 'W' or 'M'

    for backfill, 'M' will start from 19991231
    for backfill, 'W' will start from 20171117

    ovrd_startDT, overrides startDT for only 'backfill' option
    """
    if option == 'backfill':
        if freq == 'W':
            # backfill - Weekly
            bkfill = True
            thisWk = pd.date_range(end=_today, periods=7, freq='D')
            thisDT = thisWk[thisWk.weekday == 4][0].strftime('%Y%m%d')
            startDT_ = dt.datetime.strptime('20171117', '%Y%m%d').date()
            if ovrd_startDT is not None:
                assert chk_dateFormat(ovrd_startDT), "'ovrd_startDT' must be YYYYMMDD format!"
                startDT_ = dt.datetime.strptime(ovrd_startDT, '%Y%m%d').date()
            startDT = startDT_.strftime('%Y%m%d')
            seq_DT = pd.date_range(start=startDT, end=thisDT, freq='W-FRI')

            rtvStart = (startDT_ - dt.timedelta(days=rtvDays)).strftime('%Y%m%d')
            seq_DT = pd.Series(seq_DT.strftime('%Y%m%d'))

        elif freq == 'M':
            # backfill - Monthend
            bkfill = True
            thisDT = _today.strftime('%Y%m%d')
            startDT_ = dt.datetime.strptime('19991230', '%Y%m%d').date()
            if ovrd_startDT is not None:
                assert chk_dateFormat(ovrd_startDT), "'ovrd_startDT' must be YYYYMMDD format!"
                startDT_ = dt.datetime.strptime(ovrd_startDT, '%Y%m%d').date()
            startDT = startDT_.strftime('%Y%m%d')
            # seq_DT = pd.date_range(start=startDT, end=thisDT, freq='M')
            seq_DT = pd.date_range(start=startDT, end=thisDT, freq='MS')
            seq_DT += pd.DateOffset(days=25)
            seq_DT = seq_DT[seq_DT < pd.to_datetime(_today)]

            rtvStart = (startDT_ - dt.timedelta(days=rtvDays)).strftime('%Y%m%d')
            seq_DT = pd.Series(seq_DT.strftime('%Y%m%d'))

        else:
            raise TypeError("freq must be 'W' or 'M'")

    elif option == 'batch':
        if freq == 'W':
            # BATCH - Weekly (4 date-points)
            bkfill = False
            thisWk = pd.date_range(end=_today, periods=7, freq='D')
            thisDT = thisWk[thisWk.weekday == 4][0].strftime('%Y%m%d')
            seq_DT = pd.date_range(end=thisDT, periods=batch_n, freq='W-FRI')

            rtvStart = (seq_DT[0] - dt.timedelta(days=rtvDays)).strftime('%Y%m%d')
            seq_DT = pd.Series(seq_DT.strftime('%Y%m%d'))

        elif freq == 'M':
            # BATCH - Monthend (4 date-points)
            bkfill = False
            thisDT = _today.strftime('%Y%m%d')
            # seq_DT = pd.date_range(end=thisDT, periods=batch_n, freq='M')
            seq_DT = pd.date_range(end=thisDT, periods=batch_n, freq='MS')
            seq_DT += pd.DateOffset(days=25)
            seq_DT = seq_DT[seq_DT < pd.to_datetime(_today)]

            rtvStart = (seq_DT[0] - dt.timedelta(days=rtvDays)).strftime('%Y%m%d')
            seq_DT = pd.Series(seq_DT.strftime('%Y%m%d'))

        else:
            raise TypeError("freq must be 'W' or 'M'")

    else:
        raise TypeError("option must be 'backfill' or 'batch'")

    return bkfill, rtvStart, seq_DT


def cust_sequence(startDT, freq):
    """
    used in:
    make_5yrRel_EBITDA2px.py
    """
    assert freq in ['W', 'M'], "freq must be either 'W' or 'M'"

    thisDT = _today.strftime('%Y%m%d')
    if freq=='W':
        seq_DT = pd.date_range(start=startDT, end=thisDT, freq='W-FRI')
    elif freq=='M':
        # seq_DT = pd.date_range(start=startDT, end=thisDT, freq=freq)
        seq_DT = pd.date_range(start=startDT, end=thisDT, freq='MS')
        seq_DT += pd.DateOffset(days=25)
    
    seq_DT = pd.Series(seq_DT.strftime('%Y%m%d'))
    return seq_DT
    
