import pandas as pd
import sys
from jinja2 import Template
import datetime as dt
import pickle
import os
import cx_Oracle
import matplotlib.pyplot as plt
from matplotlib.widgets import Button, Slider, TextBox
sys.path.append("D:\\Program Files\\Tinysoft\\Analyse.NET")
import TSLPy3 as ts
import matplotlib as mpl
from threading import Thread
mpl.rcParams['font.sans-serif'] = ['KaiTi']
mpl.rcParams['font.serif'] = ['KaiTi']
mpl.rcParams['axes.unicode_minus'] = False


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

    def getMarketTable(self, code, start_time, end_time):
        ts_template = Template('''  begT:= StrToDateTime('{{start_time}}');
                                    endT:= StrToDateTime('{{end_time}}');
                                    setsysparam(pn_cycle(),cy_1s());
                                    setsysparam(pn_rate(),0);
                                    setsysparam(pn_RateDay(),rd_lastday);
                                    r:= select  ["StockId"] as 'ticker', datetimetostr(["date"]) as "time", ["price"]
                                            from markettable datekey begT to endT of '{{code}}' end;
                                    return r;''')
        ts_sql = ts_template.render(start_time=start_time,
                                    end_time=end_time,
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
    def __init__(self, today=None):
        self.start_time = input("输入开始时间:")
        self.end_time = input("输入结束时间:")
        if len(self.start_time) == 0 and len(self.end_time) == 0:
            self.start_time = "09:30:00"
            self.end_time = "15:00:00"
        else:
            try:
                dt.datetime.strptime(self.start_time, "%H:%M:%S")
                dt.datetime.strptime(self.end_time, "%H:%M:%S")
            except ValueError:
                raise ValueError("时间参数错误, 正确格式为%H:%M:%S")
        if today is None:
            self.today = dt.datetime.now().strftime("%Y%m%d")
        else:
            self.today = today
        calendar = getTradeCalendar(start_date="20191201", end_date=self.today)
        self.last_trading_day = calendar[calendar.index(self.today) - 1]
        with open("params.pkl", "rb") as f:
            params = pickle.load(f)
        self.long_short_list = params["long_short_list"]
        self.main_ticker = params["main_ticker"]
        self.make_df()
        self.output = ['','','','']


    def make_df(self):
        today_formatstr = self.today[:4] + '-' + self.today[4:6] + '-' + self.today[6:]
        last_day_fromatstr = self.last_trading_day[:4] + '-' + self.last_trading_day[4:6] + '-' + self.last_trading_day[6:]
        for root_ls, dir_ls, file_ls in os.walk(today_formatstr):
            for file in file_ls:
                if file.startswith("spot"):
                    self.spot_df = pd.read_excel(today_formatstr + '/' + file, encoding="gbk", skiprows=range(0, 4), index_col=None)
                    if self.spot_df["成交时间"].tolist()[0][:10] != today_formatstr:
                        raise ValueError("数据时间错误！")
                    self.make_spot_df()
                if file.startswith("future"):
                    self.future_df = pd.read_excel(today_formatstr + '/' + file, encoding="gbk")
                    if self.future_df["日期"].tolist()[0] != today_formatstr:
                        raise ValueError("数据时间错误！")
                    self.make_future_df()
                if file.startswith("his_spot"):
                    self.his_spot_df = pd.read_excel(today_formatstr + '/' + file, encoding="gbk", skiprows=range(0, 4), index_col=None)
                    if self.his_spot_df["发生日期"].tolist()[0] != last_day_fromatstr:
                        raise ValueError("数据时间错误！")
                    self.make_his_spot_df()
                if file.startswith("his_future"):
                    self.his_future_df = pd.read_excel(today_formatstr + '/' + file, encoding="gbk")
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
        today_formatstr = self.today[:4] + '-' + self.today[4:6] + '-' + self.today[6:]
        start_dt = today_formatstr + ' ' + self.start_time
        end_dt = today_formatstr + ' ' + self.end_time
        spot_df = spot_df[(spot_df["成交时间"] >= start_dt) & (spot_df["成交时间"] <= end_dt)]
        self.spot_df = spot_df

    def make_future_df(self):
        future_df = self.future_df
        future_df = future_df[['成交时间', '成交价格', '成交数量', '委托方向', "证券代码", "结算费"]]
        future_df.drop([future_df.shape[0] - 1], axis=0, inplace=True)
        future_df["direction"] = future_df["委托方向"].apply(lambda s: 1 if s.startswith("买入") else -1)
        future_df["成交数量"] = future_df["成交数量"].mul(future_df["direction"])
        future_df = future_df[(future_df["成交时间"] >= self.start_time) & (future_df["成交时间"] <= self.end_time)]
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

    def matprint(self, n, *args) -> None:
        s = ''
        for string in args:
            s += str(string)
        self.output[n] += s + '\n'
        print(s)

    def calculate_basis(self, price_type="close"):
        self.output_basis = ""
        if price_type == "close":
            self.matprint(0, "基差交易日: ", self.today)
            self.matprint(0, "交易时间: ", self.start_time + " - " + self.end_time)
        if price_type == "settlement":
            self.matprint(1, "交易日: ", self.today)
            self.matprint(1, "交易时间: ", self.start_time + " - " + self.end_time)
        if price_type == "close" and (self.spot_df.shape[0] == 0 or self.future_df.shape[0] == 0):
            self.matprint(0, "无法计算基差")
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
            self.matprint(1, "无现货成交")
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
            self.matprint(1, "无期货成交")
            future_pnl = 0
            init_future_net_sum = 0
            future_net_num = 0
        self.trading_pnl = spot_pnl + future_pnl


        if price_type == "settlement":
            self.matprint(1,"现货交易净额: ", round(init_spot_net_sum / 1000000, 2) , "百万")
            self.matprint(1,"期货交易净额: ", round(init_future_net_sum / 1000000, 2), "百万")
            self.matprint(1,"未匹配净额: ", round((init_spot_net_sum + init_future_net_sum) / 1000000, 2), "百万")
            self.matprint(1,"净期货张数: ", future_net_num, "张")
            self.matprint(1,"现货交易盈亏: ", round(spot_pnl / 10000, 2), "万")
            self.matprint(1,"期货交易盈亏: ", round(future_pnl / 10000, 2), "万")
            self.matprint(1, "交易总盈亏: ", round(self.trading_pnl / 10000, 2), "万")

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
                self.matprint(0, "加仓基差: ", round(open_basis, 2), "点")
                self.matprint(0, "现货组合比指数高: ", round(alpha_basis, 2), "点")
                self.matprint(0, "去除现货Alpha影响后的加仓基差: ", round(adjusted_basis, 2), "点")
                self.matprint(3, "加仓基差: ", round(adjusted_basis, 2), "点")
            else:
                open_basis = current_basis - self.trading_pnl / future_net_num / 200
                adjusted_basis = open_basis + alpha_basis
                self.matprint(0, "减仓基差: ", round(open_basis, 2), "点")
                self.matprint(0, "现货组合比指数高: ", round(alpha_basis, 2), "点")
                self.matprint(0, "去除现货Alpha影响后的减仓基差: ", round(adjusted_basis, 2), "点")
                self.matprint(3, "减仓基差: ", round(adjusted_basis, 2), "点")


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
        self.matprint(2, "底仓日:", self.last_trading_day)
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
            self.matprint(2, "无现货持仓")
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
            self.matprint(2, "无期货持仓")
            future_pnl = 0
            net_future_sum = 0
        self.position_pnl = spot_pnl + future_pnl

        self.matprint(2,"昨日现货持仓:", round(net_spot_sum / 1000000, 2), "百万")
        self.matprint(2,"昨日期货持仓:", round(net_future_sum / 1000000, 2), "百万")
        self.matprint(2,"未匹配金额:", round((net_spot_sum + net_future_sum) / 1000000, 2), "百万")
        self.matprint(2,"现货底仓盈亏: ", round(spot_pnl / 10000, 2), "万")
        self.matprint(2,"期货底仓盈亏: ", round(future_pnl / 10000, 2), "万")
        self.matprint(2,"持仓总盈亏：", round(self.position_pnl / 10000, 2), "万")

    def total_pnl(self):
        self.calculate_basis(price_type="close")
        self.calculate_basis(price_type="settlement")
        self.calculate_position_pnl()
        self.matprint(3,"账户总盈亏：", round((self.trading_pnl + self.position_pnl) / 10000, 2), "万")


    def _cal_size(self, x):
        if abs(x) < 50000:
            return 0.5
        elif abs(x) < 500000:
            return 1
        elif abs(x) < 1000000:
            return 2
        elif abs(x) < 2000000:
            return 3
        elif abs(x) < 4000000:
            return 4
        elif abs(x) < 80000000:
            return 5
        elif abs(x) < 16000000:
            return 6
        elif abs(x) < 32000000:
            return 7
        elif abs(x) < 50000000:
            return 7
        else:
            return 8


    def plot(self):
        spot_df = self.spot_df
        future_df = self.future_df
        today_formatstr = self.today[:4] + '-' + self.today[4:6] + '-' + self.today[6:]
        with TsTickData() as tsl:
            print(self.main_ticker)
            spot_data = tsl.getMarketTable(code="SH000905", start_time=today_formatstr + " 09:30:00", end_time=today_formatstr + " 15:00:00")
            future_data = tsl.getMarketTable(code=self.main_ticker, start_time=today_formatstr  + " 09:30:00",
                                           end_time=today_formatstr  + " 15:00:00")
        spot_data["x"] = list(range(spot_data.shape[0]))
        future_data["x"] = list(range(future_data.shape[0]))
        map_df = spot_data.reset_index()
        map_df["time"] = map_df["time"].apply(lambda s: s[-8:])
        map_df = map_df[["time", 'x']]
        map_df = map_df[(map_df["time"] >= self.start_time) & (map_df["time"] <= self.end_time)]
        interval = map_df.shape[0] // 8
        map_df.index = list(range(map_df.shape[0]))
        xticks = [map_df.iloc[0, 1],]
        xticklabels = [map_df.iloc[0, 0],]
        for i in range(1, 8):
            xticks.append(map_df.iloc[i * interval, 1])
            xticklabels.append(map_df.iloc[i * interval, 0])
        start_x = map_df.iloc[0, 1]
        end_x = map_df.iloc[map_df.shape[0] - 1, 1]
        spot_data = spot_data.reset_index()
        future_data = future_data.reset_index()
        spot_data = spot_data[(spot_data["x"] >= start_x) & (spot_data["x"] <= end_x)]
        future_data = future_data[(future_data["x"] >= start_x) & (future_data["x"] <= end_x)]


        spot_df["time2min"] = spot_df["成交时间"].apply(lambda s: s[:16] +":00")
        spot_df["amount"] = spot_df["成交价格"].mul(spot_df["成交数量"])
        spot_df = spot_df.groupby(by="time2min").sum()
        spot_df["markersize"] = spot_df["amount"].apply(self._cal_size)
        spot_df["color"] = spot_df["amount"].apply(lambda x: "red" if x > 0 else "green")
        spot_df = pd.merge(spot_df, spot_data, left_index=True, right_on="time")
        future_df["time2min"] = future_df["成交时间"].apply(lambda s: s[:5] +":00")
        future_df["amount"] = future_df["成交价格"].mul(future_df["成交数量"]) * 200
        future_df = future_df.groupby(by="time2min").sum()
        future_df["markersize"] = future_df["amount"].apply(self._cal_size)
        future_df["color"] = future_df["amount"].apply(lambda x: "red" if x > 0 else "green")
        future_data["time"] = future_data["time"].apply(lambda s: s[-8:])
        future_df = pd.merge(future_df, future_data, left_index=True, right_on="time")


        fig = plt.figure(figsize=(15, 6))
        plt.subplots_adjust(bottom=0.4)
        self.ax2 = fig.add_subplot(1, 1, 1)
        base = [x - y for x, y in zip(future_data["price"].tolist(), spot_data["price"].tolist())]
        self.ax2.plot(spot_data["x"].tolist(), base, color="lightgray", linewidth=0.5)
        self.ax = self.ax2.twinx()
        self.ax.plot(spot_data["x"].tolist(), spot_data["price"].tolist(), color="wheat")
        self.ax.plot(future_data["x"].tolist(),future_data["price"].tolist(), color="lightskyblue")
        plt.xticks(xticks, xticklabels)

        for key, record in spot_df.iterrows():
            self.ax.plot([record["x"],], [record["price"],], marker="o", markersize=record["markersize"], color=record["color"])
        for key, record in future_df.iterrows():
            self.ax.plot([record["x"],], [record["price"],], marker="o", markersize=record["markersize"], color=record["color"])
        plt.grid()
        self.ax.legend(["000905", self.main_ticker,])
        self.ax2.legend(["basis", ], loc="upper left")


        loc1 = plt.axes([0.05, 0.02, 0.2, 0.28])
        self.tb1 = TextBox(loc1, label="", initial=self.output[0], color="white")
        loc2 = plt.axes([0.28, 0.02, 0.2, 0.28])
        tb2 = TextBox(loc2, label="", initial=self.output[1], color="white")
        loc3 = plt.axes([0.51, 0.02, 0.2, 0.28])
        tb3 = TextBox(loc3, label="", initial=self.output[2], color="white")
        loc4 = plt.axes([0.74, 0.02, 0.2, 0.28])
        tb4 = TextBox(loc4, label="", initial=self.output[3], color="white")


        plt.show()


if __name__ == "__main__":


    obj = Analysis(today="20191226")
    obj.total_pnl()
    obj.plot()
