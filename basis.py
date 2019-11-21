import pandas as pd
import sys
from jinja2 import Template
from dateutil.parser import parse as dateparse
import datetime as dt

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

    def getMarketTable(self, code, start_date, end_date):
        ts_template = Template('''begT:= StrToDate('{{start_date}}');
                                  endT:= StrToDate('{{end_date}}');
                                  setsysparam(pn_cycle(),cy_1s());
                                  setsysparam(pn_rate(),0);
                                  setsysparam(pn_RateDay(),rd_lastday);
                                  r:= select  ["StockID"] as 'ticker', datetimetostr(["date"]) as "time", ["price"]
                                      from markettable datekey begT to endT of "{{code}}" end;
                                  return r;''')
        ts_sql = ts_template.render(start_date=dateparse(start_date).strftime('%Y-%m-%d'),
                                    end_date=dateparse(end_date).strftime('%Y-%m-%d'),
                                    code=code)
        fail, data, _ = ts.RemoteExecute(ts_sql, {})

        def gbk_decode(strlike):
            if isinstance(strlike, (str, bytes)):
                strlike = strlike.decode('gbk')
            return strlike

        def bytes_to_unicode(record):
            return dict(map(lambda s: (gbk_decode(s[0]), gbk_decode(s[1])), record.items()))

        if not fail:
            unicode_data = list(map(bytes_to_unicode, data))
            return pd.DataFrame(unicode_data).set_index(['time', 'ticker'])
        else:
            raise Exception("Error when execute tsl")


def makeDf(spot_file, future_file,  start_time=None, end_time=None) -> (pd.DataFrame, pd.DataFrame):
    '''
    :param spot_file: 柜台系统，委托成交查询 - 当日成交数据，导出为xlsx格式，去除千位符
    :param future_file: O32系统，综合信息查询 - 成交回报，明细，导出为xls格式
    :param direction: “建仓”或“减仓”
    :param start_time: 截取日内时段的起始时间，格式“%Y-%m-%d %H:%M:%S"，默认为空
    :param end_time: 截取日内时段的结束时间，格式“%Y-%m-%d %H:%M:%S"， 默认为空
    :return:
    '''
    future_df = pd.read_excel(future_file, encoding="gbk")
    print("\n")


    if start_time is not None and end_time is not None:
        print("交易时间：", start_time, " ~ ", end_time)
    else:
        print("交易时间: 全天")
    spot_df = pd.read_excel(spot_file, encoding="gbk", skiprows=range(0, 4), index_col=None)
    spot_df.drop([spot_df.shape[0] - 1], axis=0, inplace=True)
    if end_time is not None and start_time is not None:
        spot_df = spot_df[(spot_df["成交时间"] >= start_time) & (spot_df["成交时间"] <= end_time)]
    spot_df["成交数量"] = spot_df["成交数量"].apply(lambda s: int(str(s).replace(',', '')))
    spot_df["证券代码"] = spot_df["证券代码"].astype(int)
    spot_df["证券代码"] = spot_df["证券代码"].astype(str)
    spot_df["证券代码"] = spot_df["证券代码"].apply(
        lambda s: "SH" + s if s.startswith('6') and len(s) == 6 else "SZ" + s.zfill(6))
    spot_df["direction"] = spot_df["成交结果"].apply(lambda s: 1 if s.startswith("买入") else -1)
    spot_df["成交数量"] = spot_df["成交数量"].mul(spot_df["direction"])
    spot_df = spot_df[["证券代码", "成交时间", "成交价格", "成交数量",  "成交结果"]]
    spot_df = spot_df[(spot_df["证券代码"] != "SZ511880") & (spot_df["证券代码"] != "SZ511990") \
                      & (spot_df["证券代码"] != "SZ511660") & (spot_df["成交数量"] != 0)]
    init_spot_net_sum = spot_df["成交价格"].mul(spot_df["成交数量"]).sum()
    print("现货交易净额: ", round(init_spot_net_sum / 1000000, 2) , "百万")

    # future_df = pd.read_excel(future_file, encoding="gbk")
    future_df = future_df[['成交时间', '成交价格', '成交数量', '委托方向']]
    future_df.drop([future_df.shape[0] - 1], axis=0, inplace=True)
    if end_time is not None and start_time is not None:
         future_df = future_df[(future_df["成交时间"] >= start_time[-8:]) & (future_df["成交时间"] <= end_time[-8:])]
    future_df["direction"] = future_df["委托方向"].apply(lambda s: 1 if s.startswith("买入") else -1)
    future_df["成交数量"] = future_df["成交数量"].mul(future_df["direction"])
    init_future_net_sum = future_df["成交价格"].mul(future_df["成交数量"]).sum() * 200
    print("期货交易净额: ", round(init_future_net_sum / 1000000, 2) , "百万")

    print("未匹配净额：", round((init_spot_net_sum + init_future_net_sum) / 1000000, 2) , "百万")

    return spot_df, future_df


def calculate_basis(spot_df: pd.DataFrame, future_df: pd.DataFrame, direction: str, theoretical_spot_pnl: float):
    ticker_set = set(spot_df["证券代码"].tolist())
    current_price_df = pd.DataFrame()
    with TsTickData() as tsl:
        for ticker in ticker_set:
            price = tsl.getCurrentPrice(ticker)
            current_price_df = current_price_df.append(pd.DataFrame([[ticker, price], ], columns=["ticker", "current_price"]))
    spot_df = pd.merge(spot_df, current_price_df, left_on="证券代码", right_on="ticker", how="outer")
    spot_df["pnl"] = (spot_df["current_price"].sub(spot_df["成交价格"])).mul(spot_df["成交数量"])
    spot_pnl = spot_df["pnl"].sum()
    print("现货盈亏: ", round(spot_pnl / 10000, 2), "万")

    with TsTickData() as tsl:
        future_price = tsl.getCurrentPrice("IC1912")
    future_df["current_price"] = future_price
    future_df["pnl"] = (future_df["current_price"].sub(future_df["成交价格"])).mul(future_df["成交数量"])
    future_pnl = future_df["pnl"].sum() * 200
    future_net_num = abs(future_df["成交数量"].sum())
    print("期货盈亏: ", round(future_pnl / 10000, 2), "万")
    pnl = spot_pnl + future_pnl
    print("总盈亏：", round(pnl / 10000, 2), "万")
    print("净期货张数：", future_net_num, "张")


    with TsTickData() as tsl:
        index_price = tsl.getCurrentPrice("SH000905")
    current_basis = future_price - index_price
    # 对于加仓而言，基差变负会产生浮动盈利，因此开仓基差 = 现基差 + pnl / 合约数 / 200
    # 对于减仓而言，基差变正会产生浮动盈利，因此开仓基差 = 先基差 - pnl / 合约数 / 200
    alpha_basis = (spot_pnl - theoretical_spot_pnl) / future_net_num / 200
    if direction == "加仓":
        open_basis = current_basis + pnl / future_net_num / 200
        adjusted_basis = open_basis - alpha_basis
    elif direction == "减仓":
        open_basis = current_basis - pnl / future_net_num / 200
        adjusted_basis = open_basis - alpha_basis
    else:
        raise ValueError("参数错误: " + direction)
    print("开仓基差：", round(open_basis, 2), "点")
    print("现货组合比指数高：", round(alpha_basis, 2), "点")
    print("去除现货Alpha影响后的开仓基差:", round(adjusted_basis, 2), "点")


def his_pos_spot_pnl(his_pos_spot_file, date1, date2):
    spot_df = pd.read_excel(his_pos_spot_file, encoding="gbk", skiprows=range(0, 4), index_col=None)
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
    # print(len(ticker_set))
    # print(ticker_set)
    j = 0
    for ticker in ticker_set:
        with TsTickData() as tsl:
            price = tsl.getHistoricalPrice(ticker, date1)
        historical_price_df1 = historical_price_df1.append(
            pd.DataFrame([[ticker, price], ], columns=["ticker", "historical_price"]))
        # print(j)
        j += 1
    for ticker in ticker_set:
        with TsTickData() as tsl:
            price = tsl.getHistoricalPrice(ticker, date2)
        historical_price_df2 = historical_price_df2.append(
            pd.DataFrame([[ticker, price], ], columns=["ticker", "historical_price"]))
        # print(j)
        j += 1
    spot_df = pd.merge(spot_df, historical_price_df1, left_on="证券代码", right_on="ticker", how="outer")
    spot_df = pd.merge(spot_df, historical_price_df2, left_on="证券代码", right_on="ticker", how="outer")
    spot_df["pnl"] = (spot_df["historical_price_y"].sub(spot_df["historical_price_x"])).mul(spot_df["股份余额"])
    spot_df.to_csv("debug.csv", encoding="gbk")
    spot_pnl = spot_df["pnl"].sum()
    print("现货底仓盈亏: ", spot_pnl)
    return spot_pnl


def his_pos_future_pnl(his_pos_future_file, date1, date2):
    future_df = pd.read_excel(his_pos_future_file, encoding="gbk")
    # print(list(future_df.columns))
    future_df.drop([future_df.shape[0] - 1], axis=0, inplace=True)
    # future_df = future_df[['证券代码', '当日买量', "当日卖量", '当日买入价', '当日卖出价']]
    future_df = future_df[['证券代码', "持仓数量"]]

    # 四列分别为8355多、空， 8305多、空
    future_df.iloc[1, 1] = - future_df.iloc[1, 1]
    future_df.iloc[3, 1] = - future_df.iloc[3, 1]
    with TsTickData() as tsl:
        future_price1 = tsl.getHistoricalPrice("IC1912", date1)
        future_price2 = tsl.getHistoricalPrice("IC1912", date2)
        future_price1 = 4852.4
        future_price2 = 4932.3
    future_df["price1"] = future_price1
    future_df["price2"] = future_price2
    future_df["pnl"] = future_df["price2"].sub(future_df["price1"]).mul(future_df["持仓数量"])
    future_pnl = future_df["pnl"].sum() * 200
    # print(future_df)
    print("期货底仓盈亏: ", future_pnl)
    return future_pnl


def his_trade_spot_pnl(his_trade_spot_file, date2) -> (pd.DataFrame, pd.DataFrame):
    spot_df = pd.read_excel(his_trade_spot_file, encoding="gbk", skiprows=range(0, 4), index_col=None)
    spot_df.drop([spot_df.shape[0] - 1], axis=0, inplace=True)
    spot_df["成交数量"] = spot_df["成交数量"].apply(lambda s: int(str(s).replace(',', '')))
    spot_df["证券代码"] = spot_df["证券代码"].astype(int)
    spot_df["证券代码"] = spot_df["证券代码"].astype(str)
    spot_df["证券代码"] = spot_df["证券代码"].apply(
        lambda s: "SH" + s if s.startswith('6') and len(s) == 6 else "SZ" + s.zfill(6))
    spot_df = spot_df[["证券代码", "成交时间", "成交价格", "成交数量", "成交结果"]]
    spot_df = spot_df[
        (spot_df["证券代码"] != "SZ511880") & (spot_df["证券代码"] != "SZ511990") & (spot_df["证券代码"] != "SZ511660")]
    ticker_set = set(spot_df["证券代码"].tolist())
    historical_price_df = pd.DataFrame()
    # print(len(ticker_set))
    j = 0
    for ticker in ticker_set:
        with TsTickData() as tsl:
            price = tsl.getHistoricalPrice(ticker, date2)
        historical_price_df = historical_price_df.append(
            pd.DataFrame([[ticker, price], ], columns=["ticker", "historical_price"]))
        # print(j)
        j += 1
    spot_df = pd.merge(spot_df, historical_price_df, left_on="证券代码", right_on="ticker", how="outer")
    spot_df["pnl"] = (spot_df["historical_price"].sub(spot_df["成交价格"])).mul(spot_df["成交数量"]).mul(spot_df["成交结果"].apply(lambda s: 1 if s == "买入" else -1))
    pd.set_option("display.max_columns", None)
    # print(spot_df)
    spot_pnl = spot_df["pnl"].sum()
    print("现货交易盈亏: ", spot_pnl)
    return spot_pnl



def his_trade_future_pnl(his_trade_future_file, date2) -> (pd.DataFrame, pd.DataFrame):
    future_df = pd.read_excel(his_trade_future_file, encoding="gbk")
    future_df = future_df[['成交均价', '成交数量', '委托方向']]
    future_df.drop([future_df.shape[0] - 1], axis=0, inplace=True)
    # print(future_df)
    with TsTickData() as tsl:
        future_price = tsl.getHistoricalPrice("IC1912", date2)
        future_price = 4932.3
    # future_df["future_close_price"] = future_price
    future_df["pnl"] = (future_price - future_df["成交均价"]).mul(future_df["成交数量"]).mul(
        future_df["委托方向"].apply(lambda s: 1 if s.startswith("买入") else -1))
    # print(future_df)
    future_pnl = future_df["pnl"].sum() * 200
    print("期货交易盈亏: ", future_pnl)
    return future_pnl


def spot_theoretical_profit(spot_df):
    date = spot_df.iloc[0, 1][:10]
    next_date = dt.datetime.strftime(dt.datetime.strptime(date, "%Y-%m-%d") + dt.timedelta(1), "%Y-%m-%d")
    with TsTickData() as tsl:
        market_table = tsl.getMarketTable("SH000905", date, next_date)
        spot_price = tsl.getCurrentPrice("SH000905")
        future_table = tsl.getMarketTable("IC1912", date, next_date)
    pd.set_option("display.max_columns", None)
    market_table = market_table.reset_index()
    market_table = market_table[["time", "price"]]
    market_table.rename(columns={"price": "spot_price"}, inplace=True)
    future_table = future_table.reset_index()
    future_table = future_table[["time", "price"]]
    future_table.rename(columns={"price": "future_price"}, inplace=True)
    df = pd.merge(spot_df, market_table, left_on="成交时间", right_on="time")
    df = pd.merge(df, future_table, left_on="成交时间", right_on="time")
    df = df[["证券代码", "成交时间", "成交价格", "成交数量", "spot_price", "future_price"]]
    df["basis"] =  df["future_price"] - df["spot_price"]
    df["amount"] = df["成交价格"].mul(df["成交数量"])
    df["return"] = spot_price / df["spot_price"] - 1
    df["pnl"] = df["amount"].mul(df["return"])
    # df.to_csv("debug_theoretical_spot.csv", encoding="gbk")
    theoretical_spot_pnl = df["pnl"].sum()
    print("完全复制的理论现货盈亏: ", round(theoretical_spot_pnl / 10000, 2), "万")
    return theoretical_spot_pnl

if __name__ == "__main__":
    # spot_df, future_df = makeDf("spot_1119_morning.xlsx", "future_1119_morning.xls", "加仓", "2019-11-19 11:00:00", "2019-11-19 11:20:00")

    spot_df, future_df = makeDf(spot_file="spot_1121.xlsx", future_file="future_1121.xls", start_time="2019-11-21 13:00:00", end_time="2019-11-21 15:00:00")
    theoretical_spot_pnl = spot_theoretical_profit(spot_df)
    calculate_basis(spot_df, future_df, direction="加仓", theoretical_spot_pnl=theoretical_spot_pnl)



    # pnl = his_trade_spot_pnl("spot_1119.xlsx", "20191119") \
    #       + his_trade_future_pnl("future_1119.xls", "20191119") \
    #      + his_pos_spot_pnl("his_pos_spot_1118.xlsx", "20191118", "20191119") \
    #       + his_pos_future_pnl("his_pos_future_1118.xls", "20191118", "20191119")\
    #
    # print("总盈亏: ", pnl)
