import pandas as pd
import sys
from jinja2 import Template
from dateutil.parser import parse as dateparse
import datetime as dt
sys.path.append("D:\\Program Files\\Tinysoft\\Analyse.NET")
import TSLPy3 as ts
import warnings
warnings.simplefilter(action='ignore')

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


class Basis():
    def __init__(self, **kwargs):
        if len(kwargs) != 2 and len(kwargs) != 4:
            raise ValueError("Wrong arguments:" + str(kwargs))
        elif len(kwargs) == 2:
            self.spot_df = pd.read_excel(kwargs["spot_file"], encoding="gbk", skiprows=range(0, 4), index_col=None)
            self.future_df = pd.read_excel(kwargs["future_file"], encoding="gbk")

    def make_today_df(self):
        spot_df = self.spot_df
        future_df = self.future_df
        start_time = input("Input start time:\n>>>")
        end_time = input("Input end time:\n>>>")
        print('\n' * 2)
        if len(start_time) == 8 and len(end_time) == 8:
            print("交易时间：", start_time, " ~ ", end_time)
            spot_df = spot_df[(spot_df["成交时间"] >= start_time) & (spot_df["成交时间"] <= end_time)]
        else:
            print("交易时间: 全天")
        spot_df.drop([spot_df.shape[0] - 1], axis=0, inplace=True)
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


        future_df = future_df[['成交时间', '成交价格', '成交数量', '委托方向']]
        future_df.drop([future_df.shape[0] - 1], axis=0, inplace=True)
        if len(end_time) == 8 and len(start_time) == 8:
             future_df = future_df[(future_df["成交时间"] >= start_time[-8:]) & (future_df["成交时间"] <= end_time[-8:])]
        future_df["direction"] = future_df["委托方向"].apply(lambda s: 1 if s.startswith("买入") else -1)
        future_df["成交数量"] = future_df["成交数量"].mul(future_df["direction"])
        init_future_net_sum = future_df["成交价格"].mul(future_df["成交数量"]).sum() * 200
        print("期货交易净额: ", round(init_future_net_sum / 1000000, 2) , "百万")
        print("未匹配净额：", round((init_spot_net_sum + init_future_net_sum) / 1000000, 2) , "百万")
        self.spot_df = spot_df
        self.future_df = future_df


    def spot_theoretical_profit(self):
        spot_df = self.spot_df
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
        df["basis"] = df["future_price"] - df["spot_price"]
        df["amount"] = df["成交价格"].mul(df["成交数量"])
        df["return"] = spot_price / df["spot_price"] - 1
        df["pnl"] = df["amount"].mul(df["return"])
        self.theoretical_spot_pnl = df["pnl"].sum()
        print("完全复制的理论现货盈亏: ", round(self.theoretical_spot_pnl / 10000, 2), "万")


    def calculate_basis(self):
        direction = input("Please input open position(+) or close position(-)\n >>>")
        self.make_today_df()
        self.spot_theoretical_profit()
        spot_df = self.spot_df
        future_df = self.future_df
        ticker_set = set(spot_df["证券代码"].tolist())
        current_price_df = pd.DataFrame()
        with TsTickData() as tsl:
            for ticker in ticker_set:
                price = tsl.getCurrentPrice(ticker)
                current_price_df = current_price_df.append(
                    pd.DataFrame([[ticker, price], ], columns=["ticker", "current_price"]))
        spot_df = pd.merge(spot_df, current_price_df, left_on="证券代码", right_on="ticker", how="outer")
        spot_df["pnl"] = (spot_df["current_price"].sub(spot_df["成交价格"])).mul(spot_df["成交数量"])
        spot_df.to_csv("现货核算.csv", encoding="gbk")
        spot_pnl = spot_df["pnl"].sum()
        print("现货盈亏: ", round(spot_pnl / 10000, 2), "万")

        with TsTickData() as tsl:
            future_price = tsl.getCurrentPrice("IC1912")
        future_df["current_price"] = future_price
        future_df["pnl"] = (future_df["current_price"].sub(future_df["成交价格"])).mul(future_df["成交数量"])
        future_df.to_csv("期货核算.csv", encoding="gbk")
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
        alpha_basis = (spot_pnl - self.theoretical_spot_pnl) / future_net_num / 200
        if direction == '+':
            open_basis = current_basis + pnl / future_net_num / 200
            adjusted_basis = open_basis - alpha_basis
            print("加仓基差：", round(open_basis, 2), "点")
            print("现货组合比指数高：", round(alpha_basis, 2), "点")
            print("去除现货Alpha影响后的加仓基差:", round(adjusted_basis, 2), "点")
        elif direction == '-':
            open_basis = current_basis - pnl / future_net_num / 200
            adjusted_basis = open_basis + alpha_basis
            print("减仓基差：", round(open_basis, 2), "点")
            print("现货组合比指数高：", round(alpha_basis, 2), "点")
            print("去除现货Alpha影响后的减仓基差:", round(adjusted_basis, 2), "点")
        else:
            raise ValueError("参数错误: " + direction)


if __name__ == "__main__":
    obj = Basis(spot_file="spot_1122.xlsx", future_file="future_1122.xls")
    obj.calculate_basis()
