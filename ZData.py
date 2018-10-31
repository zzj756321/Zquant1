import tushare as ts
import numpy
import pandas as pd
import DataBase as DB
class ZData():
    def __init__(self,DirPath):
        ts.set_token("50810348a0827fbbb3dea44dda7804553b75784d314460d9ef1ab296")
        # m_StockDB = DB.DataBase()
        return
def getDataFromTushare(type,ID,start,end):
    if(type[0]=="stock"):
        out_data = getStockData(type[1],ID,start,end)
    return out_data
def getStockData(type,ID,start,end):
    pro = ts.pro_api()
    if(type[0]=="stock_basic"):
        data_out = pro.query('stock_basic', is_hs = type[1][0],exchange_id=type[1][1], list_status=type[1][2])
    elif(type[0] == "daily"):
        data_out = pro.query('daily',ts_code=ID, start_date=start, end_date=end)
        pro.daily(ts_code=ID, start_date='20180701', end_date='20180718')
    elif(type[0] == "index_daily"):
        data_out = pro.index_daily(ts_code=ID)
    return data_out
def getDataIdxofDB(stock,stockIdx):
    start = pd.Series()
    end = pd.Series()
    for ID in set(stock["ts_code"]):
        start[ID] = stock.database[stock.database["ts_code"] == ID]["trade_date"].min()
        end[ID] = stock.database[stock.database["ts_code"] == ID]["trade_date"].max()
    IdxData = pd.DataFrame()
    IdxData["start"] = start
    IdxData["end"] = end
    IdxData = IdxData.reset_index()
    IdxData = IdxData.rename(columns={"index": "ts_code"})
    stockIdx.updata(["ts_code"], ["start", "end"], IdxData)
    stockIdx.save()
    return

stock_path = "E:\资料\数据\量化数据\Stock.pickle"
stock_idx_path = "E:\资料\数据\量化数据\StockIdx.pickle"
stock = DB.DataBase(stock_path)
stockIdx = DB.DataBase(stock_idx_path)
getDataIdxofDB(stock, stockIdx)

# print(start,end)
# # pd.series()



# type = ["stock",["stock_basic",["H","SSE","L"]]]
# type = ["stock",["daily"]]
# ID = "000001.SZ"
# # fields = []
# # ZData_init()
# zdata = ZData(stock_path)
# mydata = getDataFromTushare(type,ID,"20180201","20180207")
# data_base = DB.DataBase(stock_path)
# data_base.updata(index=["ts_code","trade_date"],label =['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close','change', 'pct_change', 'vol', 'amount'],data = mydata)
# data_base.save()



