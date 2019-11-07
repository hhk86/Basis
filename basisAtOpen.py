import pandas as pd
import sys
sys.path.append("D:\\Program Files\\Tinysoft\\Analyse.NET")
import TSLPy3 as ts




class TsTickData(object):

    def __enter__(self):
        if ts.Logined() is False:
            self.__tsLogin()
            return self

    def __tsLogin(self):
        ts.ConnectServer("tsl.tinysoft.com.cn", 443)
        dl = ts.LoginServer("fzzqjyb", "fz123456")

    def __exit__(self, *arg):
        ts.Disconnect()



    def getCurrentPrice(self, ticker):
        ts_sql = ''' 
        setsysparam(pn_stock(),'{}'); 
        rds := rd(6);
        return rds;
        '''.format(ticker)
        fail, value, _ = ts.RemoteExecute(ts_sql, {})
        return value



def stock_historical_pnl(spot_file, date):
    spot_df = pd.read_excel(spot_file, encoding="gbk", skiprows = range(0, 4), index_col=None)
    spot_df.drop([spot_df.shape[0] - 1], axis=0, inplace=True)
    print(spot_df.columns)
    spot_df = spot_df[["证券代码", "发生日期", "成本价", "股份余额"]]
    spot_df["股份余额"] = spot_df["股份余额"].apply(lambda s: int(str(s).replace(',', '')))
    spot_df["证券代码"] = spot_df["证券代码"].astype(int)
    spot_df["证券代码"] = spot_df["证券代码"].astype(str)
    spot_df["证券代码"] = spot_df["证券代码"].apply(lambda s: "SH" + s if s.startswith('6') and len(s) == 6 else "SZ" + s.zfill(6))
    spot_df = spot_df[(spot_df["证券代码"] != "SZ511880") & (spot_df["证券代码"] != "SZ511990") & (spot_df["证券代码"] != "SZ511660")]
    print(spot_df)



if __name__ == "__main__":
    stock_historical_pnl("spot_1101.xlsx", "20191105")
    # Today
    # Manually delete the second row of spot.csv
    # Delete money fund.
    # Manually delete the last row of future.xls
    # Delete and adjust the inverse trade.
    # spot_df = pd.read_excel("spot_1101.xlsx", encoding="gbk", skiprows = range(0, 4), index_col=None)
    # spot_df.drop([spot_df.shape[0] - 1], axis=0, inplace=True)
    # spot_df = spot_df[["证券代码", "成交时间","成交价格", "成交数量", "成交结果"]]
    # spot_df["成交数量"] = spot_df["成交数量"].apply(lambda s: int(str(s).replace(',', '')))
    # spot_df["证券代码"] = spot_df["证券代码"].astype(str)
    # spot_df["证券代码"] = spot_df["证券代码"].apply(lambda s: "SH" + s if s.startswith('6') and len(s) == 6 else "SZ" + s.zfill(6))
    # future_df = pd.read_excel("future_83023055_1104.xls", encoding="gbk")
    # future_df = future_df[['成交时间','成交价格','成交数量','委托方向']]




    # # Make future_df
    # with TsTickData() as tsl:
    #     future_price = tsl.getCurrentPrice("IC1911")
    # future_df["current_price"] = future_price
    # future_df["pnl"] = (future_df["current_price"].sub(future_df["成交价格"])).mul(future_df["成交数量"])
    # future_pnl = future_df["pnl"].sum() * 200
    # future_net_num = future_df["成交数量"].sum()

    # Make spot_df
    # ticker_set = set(spot_df["证券代码"].tolist())
    # current_price_df = pd.DataFrame()
    # print(len(ticker_set))
    # j = 0
    # for ticker in ticker_set:
    #     with TsTickData() as tsl:
    #         price = tsl.getCurrentPrice(ticker)
    #     current_price_df = current_price_df.append(pd.DataFrame([[ticker, price], ], columns=["ticker", "current_price"]))
    #     print(j)
    #     j += 1
    # spot_df = pd.merge(spot_df, current_price_df, left_on="证券代码", right_on="ticker", how="outer")
    # spot_df["pnl"] = (spot_df["current_price"].sub(spot_df["成交价格"])).mul(spot_df["成交数量"])
    # spot_pnl = spot_df["pnl"].sum()

    # # Calculate average base at opening
    # # pnl = spot_pnl - future_pnl # 建仓
    # # base_change = - pnl / future_net_num / 200 # 建仓
    # pnl = future_pnl - spot_pnl # 平仓
    # base_change = pnl / future_net_num / 200   # 平仓
    # with TsTickData() as tsl:
    #     index_price = tsl.getCurrentPrice("SH000905")
    # current_base = future_price - index_price
    # openBase = round(current_base - base_change, 2)
    # # print(spot_pnl, future_pnl)
    # # print(base_change)
    # print(openBase)

