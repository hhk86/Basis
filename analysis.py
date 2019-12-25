import pandas as pd
import sys
from jinja2 import Template
from dateutil.parser import parse as dateparse
import datetime as dt
import pickle
import os
import cx_Oracle
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
            print('连接oracle数据库')
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
        spot_exit = False
        his_exit = False
        today = dt.datetime.now().strftime("%Y-%m-%d")
        calendar = getTradeCalendar(start_date="20191201", end_date=today[:4] + today[5:7] + today[-2:])
        self.last_trading_day = calendar[calendar.index(today[:4] + today[5:7] + today[-2:]) - 1]
        last_day_fromatstr = self.last_trading_day[:4] +'-' + self.last_trading_day[4:6] + '-' + self.last_trading_day[6:]
        for root_ls, dir_ls, file_ls in os.walk(today):
            for file in file_ls:
                if file.startswith("spot"):
                    self.spot_df = pd.read_excel(file, encoding="gbk", skiprows=range(0, 4), index_col=None)
                    spot_exit = True
                    if self.spot_df["成交时间"].tolist()[0][:10] != today:
                        raise ValueError("数据时间错误！")
                if file.startswith("future"):
                    self.future_df = pd.read_excel(file, encoding="gbk")
                    if self.future_df["日期"].tolist()[0] != today:
                        raise ValueError("数据时间错误！")
                if file.startswith("his_spot"):
                    self.his_spot_df = pd.read_excel(file, encoding="gbk", skiprows=range(0, 4), index_col=None)
                    if self.his_spot_df["发生日期"].tolist()[0] != last_day_fromatstr:
                        raise ValueError("数据时间错误！")
                    his_exit = True
                if file.startswith("his_future"):
                    self.his_future_df = pd.read_excel(file, encoding="gbk")
                    if self.his_future_df["持仓日期"].tolist()[0] != last_day_fromatstr:
                        raise ValueError("数据时间错误！")
            break
        with open("params.pkl", "rb") as f:
            params = pickle.load(f)
        self.long_short_list = params["long_short_list"]
        self.main_ticker = params["main_ticker"]
        if spot_exit:
            self.make_today_df()
        if his_exit:
            self.make_historical_df()



    def make_today_df(self):
        spot_df = self.spot_df
        future_df = self.future_df
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
        future_df = future_df[['成交时间', '成交价格', '成交数量', '委托方向', "证券代码", "结算费"]]
        future_df.drop([future_df.shape[0] - 1], axis=0, inplace=True)
        future_df["direction"] = future_df["委托方向"].apply(lambda s: 1 if s.startswith("买入") else -1)
        future_df["成交数量"] = future_df["成交数量"].mul(future_df["direction"])
        self.spot_df = spot_df
        self.future_df = future_df

    def make_historical_df(self, long_short_ls=None):
        spot_df = self.his_spot_df
        future_df = self.his_future_df
        spot_df.drop([spot_df.shape[0] - 1], axis=0, inplace=True)
        spot_df = spot_df[["证券代码", "发生日期", "成本价", "股份余额"]]
        spot_df["股份余额"] = spot_df["股份余额"].apply(lambda s: int(str(s).replace(',', '')))
        spot_df["证券代码"] = spot_df["证券代码"].astype(int)
        spot_df["证券代码"] = spot_df["证券代码"].astype(str)
        spot_df["证券代码"] = spot_df["证券代码"].apply(
            lambda s: "SH" + s if s.startswith('6') and len(s) == 6 else "SZ" + s.zfill(6))
        spot_df = spot_df[
            (spot_df["证券代码"] != "SZ511880") & (spot_df["证券代码"] != "SZ511990") & (spot_df["证券代码"] != "SZ511660")]
        future_df.drop([future_df.shape[0] - 1], axis=0, inplace=True)
        future_df = future_df[['证券代码', "持仓数量"]]
        for i in range(len(self.long_short_list)):
            sign = self.long_short_list[i]
            future_df.iloc[i, 1] = sign * future_df.iloc[i, 1]
        self.his_spot_df = spot_df
        self.his_future_df = future_df

    def calculate_basis(self, price_type="close"):
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
        init_spot_net_sum = spot_df["current_price"].mul(spot_df["成交数量"]).sum()
        spot_df["pnl"] = (spot_df["current_price"].sub(spot_df["成交价格"])).mul(spot_df["成交数量"])
        spot_df.to_csv("现货核算.csv", encoding="gbk")
        spot_pnl = spot_df["pnl"].sum() - spot_df["交易费用"].sum()

        future_net_num = abs(future_df["成交数量"].sum())
        ticker_set = set(future_df["证券代码"].tolist())

        today = dt.datetime.now().strftime("%Y%m%d")
        current_price_df = pd.DataFrame()
        with TsTickData() as tsl:
            for ticker in ticker_set:
                if price_type == "close":
                    price = tsl.getCurrentPrice(ticker)
                else:
                    price = tsl.getSettlementPrice(ticker, today)
                current_price_df = current_price_df.append(pd.DataFrame([[ticker, price], ], columns=["ticker", "current_price"]))


        future_df = pd.merge(future_df, current_price_df, left_on="证券代码", right_on="ticker", how="outer")
        init_future_net_sum = future_df["成交数量"].mul(future_df["current_price"]).sum() * 200
        future_df["pnl"] = (future_df["current_price"].sub(future_df["成交价格"])).mul(future_df["成交数量"])
        future_df.to_csv("期货核算.csv", encoding="gbk")
        future_pnl = future_df["pnl"].sum() * 200 - future_df["结算费"].sum()
        self.trading_pnl = spot_pnl + future_pnl
        self.trading_spot_pnl = spot_pnl
        self.trading_future_pnl = future_pnl


        if price_type == "settlement":
            print("现货交易净额: ", round(init_spot_net_sum / 1000000, 2) , "百万")
            print("期货交易净额: ", round(init_future_net_sum / 1000000, 2), "百万")
            print("未匹配净额：", round((init_spot_net_sum + init_future_net_sum) / 1000000, 2), "百万")
            print("净期货张数：", future_net_num, "张")
            print("现货交易盈亏: ", round(spot_pnl / 10000, 2), "万")
            print("期货交易盈亏: ", round(future_pnl / 10000, 2), "万")
            print("总交易盈亏：", round(self.trading_pnl / 10000, 2), "万")

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



if __name__ == "__main__":
    obj = Analysis()
    obj.calculate_basis(price_type="close")

