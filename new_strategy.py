import threading
from pybit.unified_trading import WebSocket, HTTP
import numpy as np
from collections import deque
from decimal import Decimal, getcontext
from time import sleep
import json

with open('settings.json', 'r') as f:
    settings = json.load(f)

from bybit_keys import bybit_api_key, bybit_secret_key

class TradingBot:
    def __init__(self, symbol, settings):
        self.symbol = symbol
        self.settings = settings
        self.ws = None
        self.session = None
        self.period = 20
        self.multiplier = 2
        self.closing_prices = deque(maxlen=self.period)
        self.in_position = False
        self.thread = None
        self.api_key = bybit_api_key
        self.api_secret = bybit_secret_key

    def start(self):
        self.session = HTTP(testnet=False, api_key=self.api_key, api_secret=self.api_secret)
        self.ws = WebSocket(testnet=True, channel_type="linear", api_key=self.api_key, api_secret=self.api_secret)
        self.thread = threading.Thread(target=self.run)
        self.thread.start()

    def run(self):
        self.ws.kline_stream(interval='5', symbol=self.symbol, callback=self.handle_message)
        
        while True:
            sleep(1)

    def handle_message(self, message):
        if 'data' in message and len(message['data']) > 0:
            candle = message['data'][0]
            closing_price = float(candle['close'])

            if candle['confirm'] == True:
                self.closing_prices.append(closing_price)

            lower_band, sma, upper_band = self.calculate_bollinger_bands(list(self.closing_prices), self.multiplier)
            if lower_band is not None and upper_band is not None:
                if not self.in_position:
                    if closing_price <= lower_band * 0.994:
                        print(f"{self.symbol}: Buy Signal detected on false breakout")
                        self.create_order("LONG", closing_price)
                    elif closing_price >= upper_band * 1.006:
                        print(f"{self.symbol}: Sell Signal detected on false breakout")
                        self.create_order("SHORT", closing_price)
                    else:
                        print(f"{self.symbol}: No trade conditions met.")
                else:
                    self.in_position = self.check_open_positions()

    def calculate_bollinger_bands(self, prices, multiplier):
        if len(prices) < self.period:
            return None, None, None
        average = np.mean(prices)
        std_dev = np.std(prices)
        upper_band = average + (std_dev * multiplier)
        lower_band = average - (std_dev * multiplier)
        return lower_band, average, upper_band

    def dynamic_round(self, number, step_size):
        getcontext().prec = 28
        number = Decimal(str(number))
        step_size = Decimal(str(step_size))
        decimal_places = len(str(step_size).split(".")[1]) if "." in str(step_size) else 0
        rounded_number = (number / step_size).quantize(1) * step_size
        return round(rounded_number, decimal_places)

    def create_order(self, side, open_price):
        cash = float(self.settings["сумма"])
        marzha = float(self.settings["маржа"])
        take = float(self.settings["тейк"])
        stop = float(self.settings["стоп"])

        instrument_info = self.session.get_instruments_info(category="linear", symbol=self.symbol)
        ord_step = float(instrument_info['result']['list'][0]['priceFilter']['tickSize'])
        qty_step = float(instrument_info['result']['list'][0]['lotSizeFilter']['qtyStep'])

        smart_quantity = cash * marzha / open_price
        rounded_smart_quantity = self.dynamic_round(smart_quantity, qty_step)

        try:
            result = self.session.place_active_order(
                symbol=self.symbol,
                side='Buy' if side == 'LONG' else 'Sell',
                order_type='Market',
                qty=rounded_smart_quantity,
                time_in_force='GoodTillCancel',
                reduce_only=False,
                close_on_trigger=False
            )
            print(f"{self.symbol}: Opened {side} position")
        except Exception as e:
            print(f"{self.symbol}: Error placing order - {e}")
            return

        if result and result.get('ret_code') == 0:
            order_id = result['result']['order_id']
            self.set_trading_stop(order_id, open_price, side)

    def set_trading_stop(self, order_id, open_price, side):
        take = float(self.settings["тейк"])
        stop = float(self.settings["стоп"])
        marzha = float(self.settings["маржа"])

        new_price = open_price
        
        if side == 'LONG':
            take_price = new_price + (new_price * take / (100 * marzha))
            stop_price = new_price - (new_price * stop / (100 * marzha))
        else:
            take_price = new_price - (new_price * take / (100 * marzha))
            stop_price = new_price + (new_price * stop / (100 * marzha))
        
        take_price = self.dynamic_round(take_price, ord_step)
        stop_price = self.dynamic_round(stop_price, ord_step)

        try:
            self.session.set_trading_stop(
                symbol=self.symbol,
                side='Buy' if side == 'LONG' else 'Sell',
                take_profit=take_price,
                stop_loss=stop_price
            )
            print(f"{self.symbol}: Set TP at {take_price} and SL at {stop_price}")
        except Exception as e:
            print(f"{self.symbol}: Error setting TP/SL - {e}")

    def check_open_positions(self):
        try:
            response = self.session.get_positions(category='linear', symbol=self.symbol)
            if response['ret_code'] == 0 and response['result']:
                if any(float(position['size']) > 0 for position in response['result']['list']):
                    print(f"{self.symbol}: There is an open position.")
                    return True
                else:
                    print(f"{self.symbol}: No active positions.")
                    return False
        except Exception as e:
            print(f"{self.symbol}: Error checking open positions - {e}")
            return None

    def stop(self):
        if self.ws:
            self.ws.close()
        if self.thread.is_alive():
            self.thread.join()

symbols = ['SOLUSDT', 'ADAUSDT', 'XRPUSDT']
bots = [TradingBot(symbol, settings) for symbol in symbols]

for bot in bots:
    bot.start()

for bot in bots:
    bot.stop()