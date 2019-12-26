import pandas as pd
import sys
from jinja2 import Template
from dateutil.parser import parse as dateparse
import datetime as dt
import pickle
import os
import cx_Oracle
import matplotlib.pyplot as plt
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

    def getSettlementPrice(selfself, ticker, date):
        ts_sql = ''' 
        setsysparam(pn_stock(), "{0}");
        setsysparam(pn_date(), inttodate({1}));
        return Settlement();
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

class OracleSql(object):
    '''
    Oracle数据库数据访问

    '''
    def __init__(self):
        '''
        初始化数据库连接
        '''
        self.host, self.oracle_port = '18.210.64.72', '1521'
        self.db, self.current_schema = 'tdb', 'wind'
        self.user, self.pwd = 'reader', 'reader'

    def __enter__(self):
        self.conn = self.__connect_to_oracle()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def __connect_to_oracle(self):
        dsn = self.host + ':' + self.oracle_port + '/' + self.db
        try:
            connection = cx_Oracle.connect(self.user, self.pwd, dsn, encoding="UTF-8", nencoding="UTF-8")
            connection.current_schema = self.current_schema
        except Exception:
            print('不能连接oracle数据库')
            connection = None
        return connection

    def query(self, sql):
        '''
        查询并返回数据

        '''
        return pd.read_sql(sql, self.conn)

    def execute(self, sql):
        '''
        对数据库执行插入、修改等数据上行操作

        '''
        self.conn.cursor().execute(sql)
        self.conn.commit()

def getTradeCalendar(start_date: str, end_date: str) -> list:
    '''
    获取某一日期区间内的所有交易日（包括起始日期和终止日期）。
    :param start_date: str, 起始日期, "YYYMMDD"
    :param end_date:str, 终止日期, "YYYMMDD"
    :return: list, 交易日列表
    '''
    sql = \
        '''
        SELECT
            TRADE_DAYS 
        FROM
            asharecalendar 
        WHERE
            S_INFO_EXCHMARKET = 'SSE' 
            AND trade_days BETWEEN {} AND {}
    '''.format(start_date, end_date)
    with OracleSql() as oracle:
        tradingDays = oracle.query(sql)
    return sorted(tradingDays.TRADE_DAYS.tolist())

class Analysis():
    def __init__(self):
        self.today = dt.datetime.now().strftime("%Y%m%d")
        calendar = getTradeCalendar(start_date="20191201", end_date=self.today)
        self.last_trading_day = calendar[calendar.index(self.today) - 1]
        with open("params.pkl", "rb") as f:
            params = pickle.load(f)
        self.long_short_list = params["long_short_list"]
        self.main_ticker = params["main_ticker"]
        self.make_df()


    def make_df(self):
        today_formatstr = self.today[:4] + '-' + self.today[4:6] + '-' + self.today[6:]
        last_day_fromatstr = self.last_trading_day[:4] + '-' + self.last_trading_day[4:6] + '-' + self.last_trading_day[6:]
        for root_ls, dir_ls, file_ls in os.walk(today_formatstr):
            for file in file_ls:
                if file.startswith("spot"):
                    self.spot_df = pd.read_excel(file, encoding="gbk", skiprows=range(0, 4), index_col=None)
                    if self.spot_df["成交时间"].tolist()[0][:10] != today_formatstr:
                        raise ValueError("数据时间错误！")
                    self.make_spot_df()
                if file.startswith("future"):
                    self.future_df = pd.read_excel(file, encoding="gbk")
                    if self.future_df["日期"].tolist()[0] != today_formatstr:
                        raise ValueError("数据时间错误！")
                    self.make_future_df()
                if file.startswith("his_spot"):
                    self.his_spot_df = pd.read_excel(file, encoding="gbk", skiprows=range(0, 4), index_col=None)
                    if self.his_spot_df["发生日期"].tolist()[0] != last_day_fromatstr:
                        raise ValueError("数据时间错误！")
                    self.make_his_spot_df()
                if file.startswith("his_future"):
                    self.his_future_df = pd.read_excel(file, encoding="gbk")
                    if self.his_future_df["持仓日期"].tolist()[0] != last_day_fromatstr:
                        raise ValueError("数据时间错误！")
                    self.make_his_future_df()
            break

        if "spot_df" not in dir(self):
            self.spot_df = pd.DataFrame(columns=["证券代码", "成交时间", "成交价格", "成交数量",  "成交结果", "交易费用"])
        if "future_df" not in dir(self):
            self.future_df = pd.DataFrame(columns=['成交时间', '成交价格', '成交数量', '委托方向', "证券代码", "结算费"])
        if "his_spot_df" not in dir(self):
            self.his_spot_df = pd.DataFrame(columns=["证券代码", "发生日期", "成本价", "股份余额"])
        if "his_future_df" not in dir(self):
            self.his_future_df = pd.DataFrame(columns=['证券代码', "持仓数量"])


    def make_spot_df(self):
        spot_df = self.spot_df
        spot_df.index = list(range(spot_df.shape[0]))
        spot_df.drop([spot_df.shape[0] - 1], axis=0, inplace=True)
        spot_df["成交数量"] = spot_df["成交数量"].apply(lambda s: int(str(s).replace(',', '')))
        spot_df["证券代码"] = spot_df["证券代码"].astype(int)
        spot_df["证券代码"] = spot_df["证券代码"].astype(str)
        spot_df["证券代码"] = spot_df["证券代码"].apply(
            lambda s: "SH" + s if s.startswith('6') and len(s) == 6 else "SZ" + s.zfill(6))
        spot_df["direction"] = spot_df["成交结果"].apply(lambda s: 1 if s.startswith("买入") else -1)
        spot_df["成交数量"] = spot_df["成交数量"].mul(spot_df["direction"])
        spot_df = spot_df[["证券代码", "成交时间", "成交价格", "成交数量",  "成交结果", "交易费用"]]
        spot_df = spot_df[(spot_df["证券代码"] != "SZ511880") & (spot_df["证券代码"] != "SZ511990") \
                          & (spot_df["证券代码"] != "SZ511660") & (spot_df["成交数量"] != 0)]
        self.spot_df = spot_df

    def make_future_df(self):
        future_df = self.future_df
        future_df = future_df[['成交时间', '成交价格', '成交数量', '委托方向', "证券代码", "结算费"]]
        future_df.drop([future_df.shape[0] - 1], axis=0, inplace=True)
        future_df["direction"] = future_df["委托方向"].apply(lambda s: 1 if s.startswith("买入") else -1)
        future_df["成交数量"] = future_df["成交数量"].mul(future_df["direction"])
        self.future_df = future_df

    def make_his_spot_df(self):
        spot_df = self.his_spot_df
        spot_df.drop([spot_df.shape[0] - 1], axis=0, inplace=True)
        spot_df = spot_df[["证券代码", "发生日期", "成本价", "股份余额"]]
        spot_df["股份余额"] = spot_df["股份余额"].apply(lambda s: int(str(s).replace(',', '')))
        spot_df["证券代码"] = spot_df["证券代码"].astype(int)
        spot_df["证券代码"] = spot_df["证券代码"].astype(str)
        spot_df["证券代码"] = spot_df["证券代码"].apply(
            lambda s: "SH" + s if s.startswith('6') and len(s) == 6 else "SZ" + s.zfill(6))
        spot_df = spot_df[
            (spot_df["证券代码"] != "SZ511880") & (spot_df["证券代码"] != "SZ511990") & (spot_df["证券代码"] != "SZ511660")]
        self.his_spot_df = spot_df

    def make_his_future_df(self):
        future_df = self.his_future_df
        future_df.drop([future_df.shape[0] - 1], axis=0, inplace=True)
        future_df = future_df[['证券代码', "持仓数量"]]
        for i in range(len(self.long_short_list)):
            sign = self.long_short_list[i]
            future_df.iloc[i, 1] = sign * future_df.iloc[i, 1]
        self.his_future_df = future_df

    def calculate_basis(self, price_type="close"):
        if price_type == "close":
            print("\n基差交易日：", self.today)
        if price_type == "settlement":
            print("\n交易日：", self.today)
        if price_type == "close" and (self.spot_df.shape[0] == 0 or self.future_df.shape[0] == 0):
            print("无法计算基差")
            return
        spot_df = self.spot_df
        if spot_df.shape[0] > 0:
            ticker_set = set(spot_df["证券代码"].tolist())
            current_price_df = pd.DataFrame()
            with TsTickData() as tsl:
                for ticker in ticker_set:
                    price = tsl.getCurrentPrice(ticker)
                    current_price_df = current_price_df.append(
                        pd.DataFrame([[ticker, price], ], columns=["ticker", "current_price"]))
            spot_df = pd.merge(spot_df, current_price_df, left_on="证券代码", right_on="ticker", how="outer")
            init_spot_net_sum = spot_df["current_price"].mul(spot_df["成交数量"]).sum()
            spot_df["pnl"] = (spot_df["current_price"].sub(spot_df["成交价格"])).mul(spot_df["成交数量"])
            spot_df.to_csv("现货核算.csv", encoding="gbk")
            spot_pnl = spot_df["pnl"].sum()
            if price_type == "settlement":
                spot_pnl -= spot_df["交易费用"].sum()
        else:
            print(self.today, "无现货成交")
            spot_pnl = 0
            init_spot_net_sum=0


        future_df = self.future_df
        if future_df.shape[0] > 0:
            future_net_num = abs(future_df["成交数量"].sum())
            ticker_set = set(future_df["证券代码"].tolist())
            current_price_df = pd.DataFrame()
            with TsTickData() as tsl:
                for ticker in ticker_set:
                    if price_type == "close":
                        price = tsl.getCurrentPrice(ticker)
                    else:
                        price = tsl.getSettlementPrice(ticker, self.today)
                    current_price_df = current_price_df.append(pd.DataFrame([[ticker, price], ], columns=["ticker", "current_price"]))


            future_df = pd.merge(future_df, current_price_df, left_on="证券代码", right_on="ticker", how="outer")
            init_future_net_sum = future_df["成交数量"].mul(future_df["current_price"]).sum() * 200
            future_df["pnl"] = (future_df["current_price"].sub(future_df["成交价格"])).mul(future_df["成交数量"])
            future_df.to_csv("期货核算.csv", encoding="gbk")
            future_pnl = future_df["pnl"].sum() * 200
            if price_type == "settlement":
                future_pnl -= future_df["结算费"].sum()
        else:
            print(self.today,"无期货成交")
            future_pnl = 0
            init_future_net_sum = 0
            future_net_num = 0
        self.trading_pnl = spot_pnl + future_pnl


        if price_type == "settlement":
            print("现货交易净额: ", round(init_spot_net_sum / 1000000, 2) , "百万")
            print("期货交易净额: ", round(init_future_net_sum / 1000000, 2), "百万")
            print("未匹配净额：", round((init_spot_net_sum + init_future_net_sum) / 1000000, 2), "百万")
            print("净期货张数：", future_net_num, "张")
            print("现货交易盈亏: ", round(spot_pnl / 10000, 2), "万")
            print("期货交易盈亏: ", round(future_pnl / 10000, 2), "万")
            print("交易总盈亏：", round(self.trading_pnl / 10000, 2), "万")

        if price_type == "close":
            self.spot_theoretical_profit()
            with TsTickData() as tsl:
                index_price = tsl.getCurrentPrice("SH000905")
                future_price = tsl.getCurrentPrice("IC2001")
            current_basis = future_price - index_price
            # 对于加仓而言，基差变负会产生浮动盈利，因此开仓基差 = 现基差 + pnl / 合约数 / 200
            # 对于减仓而言，基差变正会产生浮动盈利，因此开仓基差 = 先基差 - pnl / 合约数 / 200
            alpha_basis = (spot_pnl - self.theoretical_spot_pnl) / future_net_num / 200
            if init_spot_net_sum > 0:
                open_basis = current_basis + self.trading_pnl / future_net_num / 200
                adjusted_basis = open_basis - alpha_basis
                print("加仓基差：", round(open_basis, 2), "点")
                print("现货组合比指数高：", round(alpha_basis, 2), "点")
                print("去除现货Alpha影响后的加仓基差:", round(adjusted_basis, 2), "点")
            else:
                open_basis = current_basis - self.trading_pnl / future_net_num / 200
                adjusted_basis = open_basis + alpha_basis
                print("减仓基差：", round(open_basis, 2), "点")
                print("现货组合比指数高：", round(alpha_basis, 2), "点")
                print("去除现货Alpha影响后的减仓基差:", round(adjusted_basis, 2), "点")


    def spot_theoretical_profit(self):
        spot_df = self.spot_df
        date = spot_df.iloc[0, 1][:10]
        next_date = dt.datetime.strftime(dt.datetime.strptime(date, "%Y-%m-%d") + dt.timedelta(1), "%Y-%m-%d")
        with TsTickData() as tsl:
            market_table = tsl.getMarketTable("SH000905", date, next_date)
            spot_price = tsl.getCurrentPrice("SH000905")
            future_table = tsl.getMarketTable(self.main_ticker, date, next_date)
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
        # print("完全复制的理论现货盈亏: ", round(self.theoretical_spot_pnl / 10000, 2), "万")


    def calculate_position_pnl(self):
        print("\n底仓日:", self.last_trading_day)
        spot_df = self.his_spot_df
        if spot_df.shape[0] > 0:
            ticker_set = set(spot_df["证券代码"].tolist())
            historical_price_df1 = pd.DataFrame()
            historical_price_df2 = pd.DataFrame()
            with TsTickData() as tsl:
                for ticker in ticker_set:
                    price = tsl.getHistoricalPrice(ticker, self.last_trading_day)
                    historical_price_df1 = historical_price_df1.append(
                        pd.DataFrame([[ticker, price], ], columns=["ticker", "historical_price"]))
                for ticker in ticker_set:
                    price = tsl.getHistoricalPrice(ticker, self.today)
                    historical_price_df2 = historical_price_df2.append(
                        pd.DataFrame([[ticker, price], ], columns=["ticker", "historical_price"]))
            spot_df = pd.merge(spot_df, historical_price_df1, left_on="证券代码", right_on="ticker", how="outer")
            spot_df = pd.merge(spot_df, historical_price_df2, left_on="证券代码", right_on="ticker", how="outer")
            net_spot_sum = spot_df["股份余额"].mul(spot_df["historical_price_x"]).sum()
            spot_df["pnl"] = (spot_df["historical_price_y"].sub(spot_df["historical_price_x"])).mul(spot_df["股份余额"])
            spot_df.to_csv("历史现货核算.csv", encoding="gbk")
            spot_pnl = spot_df["pnl"].sum()
        else:
            print(self.last_trading_day, "无现货持仓")
            spot_pnl = 0
            net_spot_sum = 0


        future_df = self.his_future_df
        if future_df.shape[0] > 0:
            self.his_future_net_num = abs(future_df["持仓数量"].sum())
            ticker_set = set(future_df["证券代码"].tolist())
            his_future_price_df = pd.DataFrame()
            future_settlement_price_df = pd.DataFrame()
            with TsTickData() as tsl:
                for ticker in ticker_set:
                    price1 = tsl.getSettlementPrice(ticker, self.last_trading_day)
                    price2 = tsl.getSettlementPrice(ticker, self.today)
                    his_future_price_df = his_future_price_df.append(pd.DataFrame([[ticker, price1], ], columns=["证券代码", "price1"]))
                    future_settlement_price_df = future_settlement_price_df.append(pd.DataFrame([[ticker, price2], ], columns=["证券代码", "price2"]))

            future_settlement_price_df = pd.merge(his_future_price_df, future_settlement_price_df, on="证券代码")


            with TsTickData() as tsl:
                net_future_sum = future_df[future_df["证券代码"] == "IC1912"]["持仓数量"].sum() * tsl.getHistoricalPrice("IC1912", self.last_trading_day) * 200 \
                                + future_df[future_df["证券代码"] == "IC2001"]["持仓数量"].sum() * tsl.getHistoricalPrice("IC2001", self.last_trading_day) * 200


            future_df = pd.merge(future_df, future_settlement_price_df, on="证券代码", how="outer")
            future_df["pnl"] = future_df["price2"].sub(future_df["price1"]).mul(future_df["持仓数量"])
            future_df.to_csv("历史期货核算.csv", encoding="gbk")
            future_pnl = future_df["pnl"].sum() * 200
        else:
            print(self.last_trading_day, "无期货持仓")
            future_pnl = 0
            net_future_sum = 0
        self.position_pnl = spot_pnl + future_pnl



        print("昨日现货持仓:", round(net_spot_sum / 1000000, 2), "百万")
        print("昨日期货持仓:", round(net_future_sum / 1000000, 2), "百万") # 此处为精确计算
        print("未匹配金额:", round((net_spot_sum + net_future_sum) / 1000000, 2), "百万")
        print("现货底仓盈亏: ", round(spot_pnl / 10000, 2), "万")
        print("期货底仓盈亏: ", round(future_pnl / 10000, 2), "万")
        print("持仓总盈亏：", round(self.position_pnl / 10000, 2), "万")

    def total_pnl(self):
        self.calculate_basis(price_type="settlement")
        self.calculate_position_pnl()
        print("\n\n账户总盈亏：", round((self.trading_pnl + self.position_pnl) / 10000, 2), "万")

    def plot(self):
        fig = plt.figure(figsize=(5, 5))
        ax = fig.add_subplot(1, 1, 1)
        plt.subplots_adjust(bottom=0.4)
        axnext = plt.axes([0.78, 0.05, 0.2, 0.075])
        bnext = Button(axnext, 'Resume/Stop')
        bnext.on_clicked(stop_func)

if __name__ == "__main__":
    obj = Analysis()
    obj.calculate_basis()
    obj.total_pnl()
