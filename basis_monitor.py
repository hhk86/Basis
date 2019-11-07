import random
import matplotlib.pyplot as plt
from matplotlib.widgets import Button, Slider
import time
import sys
sys.path.append("D:\\Program Files\\Tinysoft\\Analyse.NET")
import TSLPy3 as ts
from colorama import init
init(strip=not sys.stdout.isatty()) # strip colors if stdout is redirected
from threading import Thread



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


def monitor():
    global val_min
    global stop
    while True:
        if stop:
            time.sleep(1)
            continue
        with TsTickData() as tsl:
            future_price = tsl.getCurrentPrice("IC1911")
            index_price = tsl.getCurrentPrice("SH000905")
            base = future_price - index_price
        if base <= val_min or base >= val_max:
            for i in range(30):
                ax.texts = list()
                ax.text(0 + random.random()/10, 0.2, str(int(base)), fontdict={"color": "red", "fontsize": 180})
                plt.draw()
                time.sleep(0.1)
            stop_func(None)
            ax.set_title("Suspend")
        time.sleep(1)


def stop_func(event):
    global stop
    stop = not stop
    if stop is False:
        ax.texts = list()
        ax.set_title("Working")
        plt.draw()
    else:
        ax.set_title("Suspend")
        plt.draw()


def update_min(val):
    global val_min
    val_min = val
    print("Set min: ", val_min)

def update_max(val):
    global val_max
    val_max = val
    print("Set max: ", val_max)


if __name__ == "__main__":
    stop = False
    val_min = -60
    val_max = 10
    fig = plt.figure(figsize=(5,5))
    ax = fig.add_subplot(1, 1, 1)
    plt.subplots_adjust(bottom=0.35)
    axnext = plt.axes([0.78, 0.05, 0.2, 0.075])
    bnext = Button(axnext, 'Resume/Stop')
    bnext.on_clicked(stop_func)
    left, bottom, width, height = 0.15, 0.05, 0.5, 0.1
    slider_ax_min = plt.axes([left, bottom, width, height])
    slider_min = Slider(slider_ax_min, 'Min', valmin=-60, valmax=10, valstep=1, valinit=-60)
    slider_min.on_changed(update_min)
    slider_ax_max = plt.axes([left, bottom+ 0.15, width, height])
    slider_max = Slider(slider_ax_max, 'Max', valmin=-60, valmax=10, valstep=1, valinit=10)
    slider_max.on_changed(update_max)
    ax.set_title("Working")
    t = Thread(target=monitor)
    t.start()
    plt.show()
