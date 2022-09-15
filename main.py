"""

    """

import pickle

import pandas as pd
from pathlib import Path

class Param :
  path = Path('D:\LOB')

p = Param()

def get_stock_id(pth: Path) :
  return pth.stem.split('LOB_')[1]

def make_df_from_each_lob_stock(lob_path) :
  sid = get_stock_id(lob_path)

  dc = pd.read_pickle(lob_path)

  df = pd.DataFrame()
  for ky , vl in dc.items() :
    t = pd.DataFrame.from_dict(vl)

    t['TSETMC_ID'] = sid
    t["Date"] = ky

    df = pd.concat([df , t])
    df.columns = ["TSETMC_ID" , "Date" , "hour" , "minute" , "second" ,
                  "position" , "bid_volume" , "bid_quent" , "bid_price" ,
                  "ask_volume" , "ask_quent" , "ask_price"]
  return df

def main() :
  pass

  ##
  fps = list(p.path.glob('*.p'))

  ##
  x = fps[0]
  dc = pd.read_pickle(x)
  df = pd.DataFrame.from_dict(dc , orient = 'index')

  ##
  df1 = df.stack().to_frame()
  ##
  df1.index = df1.index.droplevel(1)
  ##
  df1.index.name = 'Date'
  ##
  x = df1.iloc[0 , 0]
  type(x)
  ##
  df2 = pd.Series(x)
  ##
  df2 = pd.DataFrame()
  cols = ["hour" , "minute" , "second" , "position" , "bid_volume" ,
          "bid_quent" , "bid_price" , "ask_volume" , "ask_quent" , "ask_price"]

  for i , cn in enumerate(cols) :
    df2[cn] = df1[0].apply(lambda x : x[i])

  ##
  df2 = df1.apply(lambda x : pd.Series(x[0]) ,
                  axis = 1 ,
                  result_type = 'expand')

  ##
  df1 = df1.reset_index()

  ##
  df = make_df_from_each_lob_stock(x)

  ##

  ##


  ##


  ##

##
if __name__ == "__main__" :
  main()
  print('Done!')