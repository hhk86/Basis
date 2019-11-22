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
import warnings
warnings.filterwarnings('ignore')



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
    with TsTickData() as tsl:
        while True:
            if stop:
                time.sleep(1)
                continue

            future_price = tsl.getCurrentPrice("IC1912")
            index_price = tsl.getCurrentPrice("SH000905")
            long_future_price = tsl.getCurrentPrice("IC2001")
            try:
                basis = future_price - index_price
                calendar_spread = long_future_price - future_price
            except TypeError:
                ax.texts = list()
                ax.text(0.02, 0.5,"Failed to get TSL data! Try again in 5 seconds.")
                plt.draw()
                time.sleep(5)
                continue
            if basis <= val_min or basis >= val_max:
                for i in range(50):
                    ax.texts = list()
                    ax.text(0 + random.random()/5, 0.2, str(int(basis)), fontdict={"color": "red", "fontsize": 180})
                    plt.draw()
                    time.sleep(0.1)
                stop_func(None)
                ax.set_title("Suspend")
            if calendar_spread <= val_min2 or calendar_spread >= val_max2:
                for i in range(50):
                    ax.texts = list()
                    ax.text(0 + random.random()/5, 0.2, str(int(calendar_spread)), fontdict={"color": "green", "fontsize": 180})
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

def update_min2(val):
    global val_min2
    val_min2 = val
    print("Set min2: ", val_min2)

def update_max2(val):
    global val_max2
    val_max2 = val
    print("Set max2: ", val_max2)


if __name__ == "__main__":
    stop = False
    val_min = -100
    val_max = 20
    val_min2 = -100
    val_max2 = 20
    fig = plt.figure(figsize=(5,5))
    ax = fig.add_subplot(1, 1, 1)
    plt.subplots_adjust(bottom=0.4)
    axnext = plt.axes([0.78, 0.05, 0.2, 0.075])
    bnext = Button(axnext, 'Resume/Stop')
    bnext.on_clicked(stop_func)
    left, bottom, width, height = 0.15, 0.05, 0.5, 0.05
    slider_ax_max = plt.axes([left, bottom + 0.24, width, height])
    slider_max = Slider(slider_ax_max, 'Max', valmin=-100, valmax=20, valstep=1, valinit=20)
    slider_max.on_changed(update_max)
    slider_ax_min = plt.axes([left, bottom + 0.16, width, height])
    slider_min = Slider(slider_ax_min, 'Min', valmin=-100, valmax=20, valstep=1, valinit=-100)
    slider_min.on_changed(update_min)
    slider_ax_max2 = plt.axes([left, bottom + 0.08, width, height])
    slider_max2 = Slider(slider_ax_max2, 'Max', valmin=-100, valmax=20, valstep=1, valinit=20)
    slider_max2.on_changed(update_max2)
    slider_ax_min2 = plt.axes([left, bottom, width, height])
    slider_min2 = Slider(slider_ax_min2, 'Min', valmin=-100, valmax=20, valstep=1, valinit=-100)
    slider_min2.on_changed(update_min2)
    ax.set_title("Working")
    t = Thread(target=monitor)
    t.start()
    plt.show()
