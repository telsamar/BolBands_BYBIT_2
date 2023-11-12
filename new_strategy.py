from bybit_keys import bybit_api_key, bybit_secret_key
from pybit.unified_trading import WebSocket, HTTP
from time import sleep
import numpy as np
from collections import deque
import json
from decimal import Decimal, getcontext
import threading
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='coin_trader.log', filemode='w')

class CoinTrader:
    def __init__(self, symbol, settings):
        self.symbol = symbol
        self.settings = settings
        self.api_key = bybit_api_key
        self.api_secret = bybit_secret_key
        self.ws = WebSocket(testnet=True, channel_type="linear", api_key=self.api_key, api_secret=self.api_secret)
        self.session = HTTP(testnet=False, api_key=self.api_key, api_secret=self.api_secret)
        self.cash = float(settings["сумма"])
        self.marzha = float(settings["маржа"])
        self.take = float(settings["тейк"])
        self.stop = float(settings["стоп"])
        self.period = 20
        self.multiplier = 2
        self.closing_prices = deque(maxlen=self.period)
        self.in_position = False
        self._setup_leverage()
        self._get_wallet_balance()

    def _setup_leverage(self):
        try:
            self.session.set_leverage(category='linear', symbol=self.symbol, buyLeverage=str(self.marzha), sellLeverage=str(self.marzha))
        except Exception as e:
            pass

    def _get_wallet_balance(self):
        try:
            data = self.session.get_wallet_balance(accountType="UNIFIED", coin="USDT")
            usdt_balance = data['result']['list'][0]['coin'][0]['walletBalance']
            logging.info(f'bybit USDT: {usdt_balance}')
        except Exception as e:
            logging.error("Ошибка при получении баланса кошелька: %s", e)

    def calculate_bollinger_bands(self, prices):
        if len(prices) < self.period:
            return None, None, None
        average = np.mean(prices)
        std_dev = np.std(prices)
        upper_band = average + (std_dev * self.multiplier)
        lower_band = average - (std_dev * self.multiplier)
        return lower_band, average, upper_band

    def create_order(self, side, open_price):
        self.in_position = True
        dataz = self.session.get_instruments_info(category="linear", symbol=self.symbol)
        ord_step = dataz['result']['list'][0]['priceFilter']['tickSize']
        ord_step_num = float(ord_step)

        qty_step = dataz['result']['list'][0]['lotSizeFilter']['qtyStep']
        qty_step_num = float(qty_step)

        def dynamic_round(number, step_size):
            logging.info("number: %s", number)
            logging.info("step_size: %s", step_size)

            getcontext().prec = 10
            number = Decimal(str(number))
            step_size = Decimal(str(step_size))
            decimal_places = len(str(step_size).split(".")[1]) if "." in str(step_size) else 0
            logging.info("decimal_places: %s", decimal_places)

            rounded_number = (number // step_size) * step_size
            logging.info("rounded_number: %s", rounded_number)
            return rounded_number.quantize(Decimal(10) ** -decimal_places)

        smartQuontity = self.cash * self.marzha / open_price
        rounded_smartQuontity = dynamic_round(smartQuontity, qty_step_num)

        try:
            if side == 'LONG':
                result = self.session.place_order(category = 'linear', symbol = self.symbol, side = 'Buy', orderType = 'Market', isLeverage = 1, qty = rounded_smartQuontity)
                logging.info(f'{self.symbol}. Открыли LONG')
            elif side == 'SHORT':
                result = self.session.place_order(category = 'linear', symbol = self.symbol, side = 'Sell', orderType = 'Market', isLeverage = 1, qty = rounded_smartQuontity)
                logging.info(f'{self.symbol}. Открыли SHORT')
            else:
                logging.info(f"{self.symbol}. Куда растем?")
        except Exception as e:
            logging.info(e)

        datay = self.session.get_order_history(category="linear", orderId = result.get('result', {}).get('orderId', None))
        new_price = float(datay.get('result', {}).get('list', [])[0].get('avgPrice', 'Не найдено'))
        logging.info(f'{self.symbol}. Средняя цена открытой рыночной сделки: {new_price}')

        take_price_ch_short = dynamic_round((new_price - (self.take * new_price) / (self.marzha * 100)), ord_step_num)
        stop_price_ch_short = dynamic_round((new_price + (self.stop * new_price) / (self.marzha * 100)), ord_step_num)
        take_price_ch_long = dynamic_round((new_price + (self.take * new_price) / (self.marzha * 100)), ord_step_num)
        stop_price_ch_long = dynamic_round((new_price - (self.stop * new_price) / (self.marzha * 100)), ord_step_num)

        try:
            if side == 'LONG':
                sl_tp_order = self.session.set_trading_stop(category = 'linear', symbol = self.symbol, takeProfit=str(take_price_ch_long), tpTriggerBy="MarkPrice", tpslMode="Partial", tpOrderType="Limit", tpSize=str(rounded_smartQuontity), tpLimitPrice = str(take_price_ch_long),
                    stopLoss=str(stop_price_ch_long), slTriggerB="LastPrice", slOrderType="Limit", slSize=str(rounded_smartQuontity), slLimitPrice = str(stop_price_ch_long))
                logging.info(f"{self.symbol}. TP и SL успешно открыты в long")
            elif side == 'SHORT':
                sl_tp_order = self.session.set_trading_stop(category = 'linear', symbol = self.symbol, takeProfit=str(take_price_ch_short), tpTriggerBy="MarkPrice", tpslMode="Partial", tpOrderType="Limit", tpSize=str(rounded_smartQuontity), tpLimitPrice = str(take_price_ch_short),
                    stopLoss=str(stop_price_ch_short), slTriggerBy="LastPrice", slOrderType="Limit", slSize=str(rounded_smartQuontity), slLimitPrice = str(stop_price_ch_short))
                logging.info(f"{self.symbol}. TP и SL успешно открыты в short")
            else:
                logging.error("Где стоп?")
        except Exception as e:
            logging.info(f"{self.symbol}. Не удалось создать TP и SL: {e}")

    def handle_message(self, message):
        if 'data' in message and len(message['data']) > 0:
            candle = message['data'][0]
            closing_price = float(candle['close'])

            if candle['confirm'] == True:
                self.closing_prices.append(closing_price)

            lower_band, sma, upper_band = self.calculate_bollinger_bands(list(self.closing_prices))
            if lower_band is not None and upper_band is not None:
                if not self.in_position:
                    if closing_price <= lower_band * 0.975:
                        logging.info(f"{self.symbol} Сигнал на покупку")
                        self.create_order("LONG", closing_price)
                    elif closing_price >= upper_band * 1.025:
                        logging.info(f"{self.symbol} Сигнал на продажу")
                        self.create_order("SHORT", closing_price)
                    else:
                        logging.debug(f"{self.symbol} Условия не выполняются")
                else:
                    self.in_position = self.check_open_positions()

    def check_open_positions(self):
        try:
            response = self.session.get_positions(category='linear', symbol=self.symbol)
            if response['retCode'] == 0 and response['result']:
                if any(float(position['size']) > 0 for position in response['result']['list']):
                    logging.info(f"У вас есть открытая позиция на {self.symbol}.")
                    return True
                else:
                    logging.info(f"{self.symbol} Активных позиций нет")
                    return False
        except Exception as e:
            logging.error(f"Ошибка при проверке открытых позиций: {e}")
            return None

    def start_trading(self):
        self.ws.kline_stream(interval='5', symbol=self.symbol, callback=lambda msg: self._run_in_thread(self.handle_message, msg))
        try:
            while True:
                sleep(1)
        except KeyboardInterrupt:
            self.ws.close()
            logging.info(f"{self.symbol} WebSocket closed.")

    def _run_in_thread(self, fn, *args):
        thread = threading.Thread(target=fn, args=args)
        thread.daemon = True
        thread.start()
        return thread

def run_trader(symbol, settings):
    trader = CoinTrader(symbol, settings)
    trader.start_trading()

if __name__ == "__main__":
    with open('settings.json', 'r') as f:
        settings = json.load(f)

    symbols = ['XRPUSDT', 'LINKUSDT', 'DOGEUSDT', 'GASUSDT', 'AVAXUSDT', 'ZRXUSDT', 'RUNEUSDT', 'ORDIUSDT', 'TRBUSDT', 'MATICUSDT', 'APTUSDT', 'ADAUSDT']

    threads = []
    for symbol in symbols:
        thread = threading.Thread(target=run_trader, args=(symbol, settings))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()