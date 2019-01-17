# (WorldScope) Year Retrieve (Historical)
from .WS_retrieveData import WS_year

# (WorldScope) Quarter Retrieve (Historical)
from .WS_retrieveData import WS_qtr
from .WS_retrieveData import WS_qtr_avg
from .WS_retrieveData import WS_qtr_sum

def WS_retrieve_custom(Item, Table, Code_lst, qtr_type, bkfil=True, **kwargs):
    default = {'add_lback_yr': 10, 'add_lback_qtr': 24}
    default.update(kwargs)
    add_lback_yr = default['add_lback_yr']
    add_lback_qtr = default['add_lback_qtr']

    DF_yr = WS_year(Item, Table, Code_lst, bkfil=bkfil, add_lback=add_lback_yr)
    if qtr_type == 'sum':
        DF_qtr = WS_qtr_sum(Item, Table, Code_lst, bkfil=bkfil, add_lback=add_lback_qtr)
    elif qtr_type == 'avg':
        DF_qtr = WS_qtr_avg(Item, Table, Code_lst, bkfil=bkfil, add_lback=add_lback_qtr)
    elif qtr_type is None:
        DF_qtr = WS_qtr(Item, Table, Code_lst, bkfil=bkfil, add_lback=add_lback_qtr)
    else:
        raise TypeError('"qtr_type" must be either "sum" or "avg" or None')
    return DF_yr, DF_qtr