"""

    """

import pickle

import pandas as pd
from pathlib import Path
from mirutil.df_utils import save_as_prq_wo_index as sprq


class Param :
    path = Path('LOB')
    sdir = Path('LOB1')

p = Param()

class ColName :
    tid = 'TSETMC_ID'
    date = 'Date'
    hour = 'hour'
    minute = 'minute'
    second = 'second'
    position = 'position'
    bid_volume = 'bid_volume'
    bid_quent = 'bid_quent'
    bid_price = 'bid_price'
    ask_volume = 'ask_volume'
    ask_quent = 'ask_quent'
    ask_price = 'ask_price'
    date_time = 'DateTime'

c = ColName()

cols_1 = {
        c.hour       : None ,
        c.minute     : None ,
        c.second     : None ,
        c.position   : None ,
        c.bid_volume : None ,
        c.bid_quent  : None ,
        c.bid_price  : None ,
        c.ask_volume : None ,
        c.ask_quent  : None ,
        c.ask_price  : None ,
        }

def get_stock_id(pth: Path) :
    return pth.stem.split('LOB_')[1]

def make_df_from_each_lob_stock(lob_path , sid) :
    dc = pd.read_pickle(lob_path)
    df = pd.DataFrame.from_dict(dc , orient = 'index')

    df = df.stack().to_frame()

    df.index = df.index.droplevel(1)
    df.index.name = 'Date'

    _df = pd.DataFrame()
    for i , cn in enumerate(cols_1.keys()) :
        _df[cn] = df[0].apply(lambda x : x[i])

    _df = _df.reset_index()
    _df[c.tid] = sid
    return _df

def make_datetime_col(df) :
    col = df[c.date].astype('string').str
    df[c.date] = col[:4] + '-' + col[4 :6] + '-' + col[6 :8]

    h = df[c.hour].astype('string')
    m = df[c.minute].astype('string')
    s = df[c.second].astype('string')

    df[c.date_time] = pd.to_datetime(df[c.date] + ' ' + h + ':' + m + ':' + s)
    return df

def rm_xtra_cols(df) :
    return df.drop(columns = [c.date , c.hour , c.minute , c.second])

def fix_columns_order(df) :
    _ord = {
            c.tid        : 0 ,
            c.date_time  : 1 ,
            c.position   : 2 ,
            c.bid_volume : 3 ,
            c.bid_quent  : 4 ,
            c.bid_price  : 5 ,
            c.ask_volume : 6 ,
            c.ask_quent  : 7 ,
            c.ask_price  : 8 ,
            }
    return df[_ord.keys()]

def fix_col_types(df) :
    _col = {
            c.position   : None ,
            c.bid_volume : None ,
            c.bid_quent  : None ,
            c.bid_price  : None ,
            c.ask_volume : None ,
            c.ask_quent  : None ,
            c.ask_price  : None ,
            }
    for cn in _col.keys() :
        df[cn] = df[cn].astype('Int64')
    return df

def read_clean_save(fp) :
    sid = get_stock_id(fp)
    df = make_df_from_each_lob_stock(fp , sid)
    df = make_datetime_col(df)
    df = rm_xtra_cols(df)
    df = fix_columns_order(df)
    df = fix_col_types(df)
    sfp = p.sdir / (sid + '.prq')
    sprq(df , sfp)

def main() :
    pass

    ##
    fps = list(p.path.glob('*.p'))

    ##
    for fp in fps :
        read_clean_save(fp)

    ##

    ##

    ##

    ##

##
if __name__ == "__main__" :
    main()
    print('Done!')
