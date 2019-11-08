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

    def getHistoricalPrice(self, ticker, date):
        ts_sql = ''' 
        setsysparam(pn_stock(), "{0}");
        setsysparam(pn_date(), inttodate({1}));
        return close();
        '''.format(ticker, date)
        fail, value, _ = ts.RemoteExecute(ts_sql, {})
        return value


def makeDf(spot_file, future_file, direction: str) -> (pd.DataFrame, pd.DataFrame):
    spot_df = pd.read_excel(spot_file, encoding="gbk", skiprows=range(0, 4), index_col=None)
    spot_df.drop([spot_df.shape[0] - 1], axis=0, inplace=True)
    spot_df["成交数量"] = spot_df["成交数量"].apply(lambda s: int(str(s).replace(',', '')))
    spot_df["证券代码"] = spot_df["证券代码"].astype(int)
    spot_df["证券代码"] = spot_df["证券代码"].astype(str)
    spot_df["证券代码"] = spot_df["证券代码"].apply(
        lambda s: "SH" + s if s.startswith('6') and len(s) == 6 else "SZ" + s.zfill(6))
    if direction == "加仓":
        spot_df["same_direction"] = spot_df["成交结果"].apply(lambda s: 1 if s.startswith("买入") else -1)
    elif direction == "减仓":
        spot_df["same_direction"] = spot_df["成交结果"].apply(lambda s: 1 if s.startswith("卖出") else -1)
    else:
        raise ValueError("Wrong direction: " + direction)
    spot_df["adjusted_quantity"] = spot_df["成交数量"].mul(spot_df["same_direction"])
    spot_df = spot_df[["证券代码", "成交时间", "成交价格", "成交数量", "adjusted_quantity", "成交结果"]]
    spot_df = spot_df[
        (spot_df["证券代码"] != "SZ511880") & (spot_df["证券代码"] != "SZ511990") & (spot_df["证券代码"] != "SZ511660")]
    init_spot_net_sum = spot_df["成交价格"].mul(spot_df["adjusted_quantity"]).sum()
    print("Initial sum of total equity: ", init_spot_net_sum)

    future_df = pd.read_excel(future_file, encoding="gbk")
    future_df = future_df[['成交时间', '成交价格', '成交数量', '委托方向']]
    future_df.drop([future_df.shape[0] - 1], axis=0, inplace=True)
    if direction == "加仓":
        future_df["same_direction"] = future_df["委托方向"].apply(lambda s: 1 if s.startswith("卖出") else -1)
    elif direction == "减仓":
        future_df["same_direction"] = future_df["委托方向"].apply(lambda s: 1 if s.startswith("买入") else -1)
    future_df["adjusted_quantity"] = future_df["成交数量"].mul(future_df["same_direction"])
    init_future_net_sum = future_df["成交价格"].mul(future_df["adjusted_quantity"]).sum() * 200
    print("Initial sum of total future contracts: ", init_future_net_sum)

    return spot_df, future_df


def calculate_basis(spot_df: pd.DataFrame, future_df: pd.DataFrame) -> float:
    ticker_set = set(spot_df["证券代码"].tolist())
    current_price_df = pd.DataFrame()
    print(len(ticker_set))
    j = 0
    for ticker in ticker_set:
        with TsTickData() as tsl:
            price = tsl.getCurrentPrice(ticker)
        current_price_df = current_price_df.append(
            pd.DataFrame([[ticker, price], ], columns=["ticker", "current_price"]))
        print(j)
        j += 1
    spot_df = pd.merge(spot_df, current_price_df, left_on="证券代码", right_on="ticker", how="outer")
    spot_df["pnl"] = (spot_df["current_price"].sub(spot_df["成交价格"])).mul(spot_df["成交数量"])
    spot_pnl = spot_df["pnl"].sum()
    print("Spot pnl: ", spot_pnl)

    with TsTickData() as tsl:
        future_price = tsl.getCurrentPrice("IC1911")
    future_df["current_price"] = future_price
    future_df["pnl"] = (future_df["current_price"].sub(future_df["成交价格"])).mul(future_df["成交数量"])
    future_pnl = future_df["pnl"].sum() * 200
    future_net_num = future_df["adjusted_quantity"].sum()
    print("Future pnl: ", future_pnl)

    # 事实上，加仓减仓都一样
    pnl = spot_pnl - future_pnl  # 建仓
    basis_change = - pnl / future_net_num / 200  # 建仓
    # pnl = future_pnl - spot_pnl # 平仓
    # basis_change = pnl / future_net_num / 200   # 平仓
    print(pnl)
    with TsTickData() as tsl:
        index_price = tsl.getCurrentPrice("SH000905")
    current_basis = future_price - index_price
    open_basis = round(current_basis - basis_change, 2)

    return open_basis


def stock_historical_pnl(spot_file, date1, date2):
    spot_df = pd.read_excel(spot_file, encoding="gbk", skiprows=range(0, 4), index_col=None)
    spot_df.drop([spot_df.shape[0] - 1], axis=0, inplace=True)
    spot_df = spot_df[["证券代码", "发生日期", "成本价", "股份余额"]]
    spot_df["股份余额"] = spot_df["股份余额"].apply(lambda s: int(str(s).replace(',', '')))
    spot_df["证券代码"] = spot_df["证券代码"].astype(int)
    spot_df["证券代码"] = spot_df["证券代码"].astype(str)
    spot_df["证券代码"] = spot_df["证券代码"].apply(
        lambda s: "SH" + s if s.startswith('6') and len(s) == 6 else "SZ" + s.zfill(6))
    spot_df = spot_df[
        (spot_df["证券代码"] != "SZ511880") & (spot_df["证券代码"] != "SZ511990") & (spot_df["证券代码"] != "SZ511660")]
    ticker_set = set(spot_df["证券代码"].tolist())
    historical_price_df1 = pd.DataFrame()
    historical_price_df2 = pd.DataFrame()
    print(len(ticker_set))
    # print(ticker_set)
    j = 0
    for ticker in ticker_set:
        with TsTickData() as tsl:
            price = tsl.getHistoricalPrice(ticker, date1)
        historical_price_df1 = historical_price_df1.append(
            pd.DataFrame([[ticker, price], ], columns=["ticker", "historical_price"]))
        print(j)
        j += 1
    for ticker in ticker_set:
        with TsTickData() as tsl:
            price = tsl.getHistoricalPrice(ticker, date2)
        historical_price_df2 = historical_price_df2.append(
            pd.DataFrame([[ticker, price], ], columns=["ticker", "historical_price"]))
        print(j)
        j += 1
    spot_df = pd.merge(spot_df, historical_price_df1, left_on="证券代码", right_on="ticker", how="outer")
    spot_df = pd.merge(spot_df, historical_price_df2, left_on="证券代码", right_on="ticker", how="outer")
    spot_df["pnl"] = (spot_df["historical_price_y"].sub(spot_df["historical_price_x"])).mul(spot_df["股份余额"])
    spot_df.to_csv("debug.csv", encoding="gbk")
    spot_pnl = spot_df["pnl"].sum()
    print("Historical spot pnl: ", spot_pnl)


def future_historical_pnl(future_file, date1, date2):
    future_df = pd.read_excel(future_file, encoding="gbk")
    # print(list(future_df.columns))
    future_df.drop([future_df.shape[0] - 1], axis=0, inplace=True)
    # future_df = future_df[['证券代码', '当日买量', "当日卖量", '当日买入价', '当日卖出价']]
    future_df = future_df[['证券代码', "持仓数量"]]

    # 四列分别为8355多、空， 8305多、空
    future_df.iloc[1, 1] = - future_df.iloc[1, 1]
    future_df.iloc[3, 1] = - future_df.iloc[3, 1]
    with TsTickData() as tsl:
        future_price1 = tsl.getHistoricalPrice("IC1911", date1)
        future_price2 = tsl.getHistoricalPrice("IC1911", date2)
    future_df["price1"] = future_price1
    future_df["price2"] = future_price2
    future_df["pnl"] = future_df["price2"].sub(future_df["price1"]).mul(future_df["持仓数量"])
    future_pnl = future_df["pnl"].sum() * 200
    print(future_df)
    print("Historical future pnl: ", future_pnl)


if __name__ == "__main__":
    spot_df, future_df = makeDf("spot_1107_noon.xlsx", "future_1107_noon.xls", "加仓")
    print(calculate_basis(spot_df, future_df))


    # stock_historical_pnl("his_spot_1104.xlsx", "20191104", "20191105")
    # future_historical_pnl("his_future_1104.xls", "20191104", "20191105")
