from bybit_keys_test import bybit_api_key, bybit_secret_key
from pybit.unified_trading import WebSocket, HTTP
from time import sleep
import numpy as np
import pandas as pd
from collections import deque
import json
from decimal import Decimal, getcontext
import threading
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='coin_trader.log', filemode='w', encoding='utf-8')

multipliers = {
    'GALAUSDT': 3.6,  # 0 | 0 | 3 -> 3.3 (1/2) -> 4 (1/0) -> 3.8 (2/0) -> 3.6 (4/0)
    'AXSUSDT': 5,     # 0 | 0 | 2.3 (11/2) -> 3.1 (36/4) -> 4 (9/1) -> 5 (3/0)
    'BNBUSDT': 5,     # 0 | 0 | 2.7 (11/2) -> 3.5 (16/2) -> 4.3 (4/1) -> 5 (4/0)
    'LINKUSDT': 8.5,  # 0 | 0 | 5.5 (11/1) -> 5.75 (16/2) -> 7.5 (6/1) -> 8.5 (3/0)

    'ADAUSDT': 4.8,   # 0 | 0 | 4.5 (12/1) -> 4.3 (22/1) -> 4.8 (/)
    'ORDIUSDT': 5.8,  # 0 | 0 | 5 -> 5.5 (16/1) -> 5.8 (/)
    'MATICUSDT': 6,   # 0 | 0 | 3.7 (2/1) -> 3.2 (3/1) -> 4 (4/0) -> 4.1 (0/1) -> 5 (4/1) -> 6 (/)
    'XRPUSDT': 5,     # 0 | 0 | 3.5 (6/1) -> 3.6 (13/1) -> 3.8 (9/1) -> 4.5 (1/1) -> 5 (/)
    'TRBUSDT': 8,     # 0 | 0 | 6 -> 5.7 (11/2) -> 6 (0/1) -> 6.3 (5/1) -> 6.5 (18/3) -> 8 (/)
    'DOTUSDT': 7.7,   # 0 | 0 | 5 -> 4.7 (5/1) -> 5 (31/2) -> 6 (9/1) -> 7.7 (/)
    'DOGEUSDT': 5.1,  # 0 | 0 | 3 -> 3.8 (1/1) -> 3.3 (3/1) -> 4 (22/1) -> 4.5 (1/1) -> 5 (6/1) -> 5.1 (/)
    'AVAXUSDT': 6,    # 0 | 0 | 6 (/)
    'NEARUSDT': 6,     # 0 | 0 | 6 (/)
}
# с 13:00 01.01

class CoinTrader:
    def __init__(self, symbol, settings):
        self.symbol = symbol
        self.settings = settings
        self.api_key = bybit_api_key
        self.api_secret = bybit_secret_key
        self.ws = WebSocket(testnet=True, channel_type="linear", api_key=self.api_key, api_secret=self.api_secret)
        self.session = HTTP(testnet=False, api_key=self.api_key, api_secret=self.api_secret)
        self.cash = float(settings["summa"])
        self.marzha = float(settings["marzha"])
        self.take = float(settings["take"])
        self.stop = float(settings["stop"])
        self.period = 60
        self.multiplier = multipliers.get(symbol, 7)
        self.closing_prices = deque(maxlen=self.period)
        self.open_prices = deque(maxlen=self.period)
        self.high_prices = deque(maxlen=self.period)
        self.low_prices = deque(maxlen=self.period)
        self.volumes = deque(maxlen=self.period)
        self.turnovers = deque(maxlen=self.period)
        self.in_position = False
        self.in_second_position = False
        self._setup_leverage()
        self._get_wallet_balance()
        self._load_historical_data()

    def _setup_leverage(self):
        try:
            self.session.switch_position_mode(category = 'linear', symbol=self.symbol, mode=3)
            self.session.set_leverage(category = 'linear', symbol=self.symbol, buyLeverage=str(self.marzha), sellLeverage=str(self.marzha))
            self.session.switch_margin_mode(category = 'linear', symbol=self.symbol, tradeMode = 0, buyLeverage=str(self.marzha), sellLeverage=str(self.marzha))
        except Exception as e:
            logging.debug("Произошла ошибка в установке маржи или мода: %s", e)

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
        
        prices_series = pd.Series(prices)
        average = prices_series.ewm(span=self.period).mean().iloc[-1]
        std_dev = np.std(prices, ddof=1)
        # print(self.multiplier)
        upper_band = average + (std_dev * self.multiplier)
        lower_band = average - (std_dev * self.multiplier)
        return lower_band, average, upper_band

    def create_order(self, side, open_price):
        self.in_position = True
        open_price = float(open_price)
        dataz = self.session.get_instruments_info(category="linear", symbol=self.symbol)
        ord_step = dataz['result']['list'][0]['priceFilter']['tickSize']
        ord_step_num = float(ord_step)

        qty_step = dataz['result']['list'][0]['lotSizeFilter']['qtyStep']
        qty_step_num = float(qty_step)

        def dynamic_round(number, step_size):
            getcontext().prec = 10
            number = Decimal(str(number))
            step_size = Decimal(str(step_size))
            decimal_places = len(str(step_size).split(".")[1]) if "." in str(step_size) else 0

            rounded_number = (number // step_size) * step_size
            return rounded_number.quantize(Decimal(10) ** -decimal_places)

        smartQuontity = self.cash * self.marzha / open_price
        rounded_smartQuontity = dynamic_round(smartQuontity, qty_step_num)

        try:
            if side == 'LONG':
                result = self.session.place_order(category = 'linear', symbol = self.symbol, side = 'Buy', orderType = 'Market', isLeverage = 1, qty = rounded_smartQuontity, positionIdx = 1)
                logging.info(f'{self.symbol}. Открыли LONG')
            elif side == 'SHORT':
                result = self.session.place_order(category = 'linear', symbol = self.symbol, side = 'Sell', orderType = 'Market', isLeverage = 1, qty = rounded_smartQuontity, positionIdx = 2)
                logging.info(f'{self.symbol}. Открыли SHORT')
        except Exception as e:
            logging.error("Произошла ошибка в выставлении заявки на покупку: %s", e)

        time.sleep(0.5)

        for attempt in range(5):
            try:
                datay = self.session.get_order_history(category="linear", orderId = result.get('result', {}).get('orderId', None))
                list_data = datay.get('result', {}).get('list', [])
                if not list_data:
                    raise ValueError("Список истории заказов пуст")

                new_price = float(list_data[0].get('avgPrice', 0))
                if new_price == 0:
                    raise ValueError("Не удалось получить среднюю цену")
                logging.info(f'{self.symbol}. Средняя цена открытой рыночной сделки: {new_price}')

                if side == 'LONG':
                    take_price_ch_long = dynamic_round((new_price + (self.take * new_price) / (self.marzha * 100)), ord_step_num)
                    stop_price_ch_long = dynamic_round((new_price - (self.stop * new_price) / (self.marzha * 100)), ord_step_num)
                    order = self.session.set_trading_stop(category = 'linear', symbol = self.symbol, takeProfit=str(take_price_ch_long), tpTriggerBy="MarkPrice", tpslMode="Partial", tpOrderType="Limit", tpSize=str(rounded_smartQuontity), tpLimitPrice = str(take_price_ch_long), positionIdx = 1)
                    # order = self.session.set_trading_stop(category = 'linear', symbol = self.symbol, takeProfit=str(take_price_ch_long), tpTriggerBy="MarkPrice", tpslMode="Partial", tpOrderType="Limit", tpSize=str(rounded_smartQuontity), tpLimitPrice = str(take_price_ch_long), stopLoss=str(stop_price_ch_long), slTriggerBy="LastPrice", slOrderType="Limit", slSize=str(rounded_smartQuontity), slLimitPrice = str(stop_price_ch_long), positionIdx = 1)
                    logging.info("%s. TP и SL успешно открыты в long", self.symbol)
                elif side == 'SHORT':
                    take_price_ch_short = dynamic_round((new_price - (self.take * new_price) / (self.marzha * 100)), ord_step_num)
                    stop_price_ch_short = dynamic_round((new_price + (self.stop * new_price) / (self.marzha * 100)), ord_step_num)         
                    order = self.session.set_trading_stop(category = 'linear', symbol = self.symbol, takeProfit=str(take_price_ch_short), tpTriggerBy="MarkPrice", tpslMode="Partial", tpOrderType="Limit", tpSize=str(rounded_smartQuontity), tpLimitPrice = str(take_price_ch_short), positionIdx = 2)  
                    # order = self.session.set_trading_stop(category = 'linear', symbol = self.symbol, takeProfit=str(take_price_ch_short), tpTriggerBy="MarkPrice", tpslMode="Partial", tpOrderType="Limit", tpSize=str(rounded_smartQuontity), tpLimitPrice = str(take_price_ch_short), stopLoss=str(stop_price_ch_short), slTriggerBy="LastPrice", slOrderType="Limit", slSize=str(rounded_smartQuontity), slLimitPrice = str(stop_price_ch_short), positionIdx = 2)            
                    logging.info("%s. TP и SL успешно открыты в short", self.symbol)
                break
            except Exception as e:
                logging.error(f"Попытка {attempt + 1} - Ошибка при установлении TP и SL: {e}")
                if attempt == 4:
                    logging.error("Не удалось установить TP и SL после 5 попыток")
                time.sleep(1)

    def handle_message(self, message):
        if 'data' in message and len(message['data']) > 0:
            candle = message['data'][0]

            if candle['confirm'] == True:
                self.open_prices.append(float(candle['open']))
                self.high_prices.append(float(candle['high']))
                self.low_prices.append(float(candle['low']))
                self.closing_prices.append(float(candle['close']))
                self.volumes.append(float(candle['volume']))
                self.turnovers.append(float(candle['turnover']))

            current_close_price = float(candle['close'])
            lower_band, ema, upper_band = self.calculate_bollinger_bands(list(self.closing_prices) + [current_close_price])
            if lower_band is not None and upper_band is not None:
                if not self.in_position:
                    if current_close_price <= lower_band:
                        self.create_order("LONG", candle['close'])
                        logging.info(f"{self.symbol} Сигнал на покупку lower_band: {lower_band}, ema: {ema}, upper_band: {upper_band}")
                    elif current_close_price >= upper_band: 
                        self.create_order("SHORT", candle['close'])
                        logging.info(f"{self.symbol} Сигнал на продажу lower_band: {lower_band}, ema: {ema}, upper_band: {upper_band}")
                    else:
                        logging.debug(f"{self.symbol} Условия не выполняются")
                else:
                    self.in_position = self.check_open_positions()

    def check_open_positions(self):
        try:
            response = self.session.get_positions(category='linear', symbol=self.symbol)
            if response['retCode'] == 0 and response['result']:
                if any(float(position['size']) > 0 for position in response['result']['list']):
                    return True
                else:
                    return False
        except Exception as e:
            logging.error(f"Ошибка при проверке открытых позиций: {e}")
            return None

    def _load_historical_data(self, interval=1):
        try:
            historical_data = self.session.get_kline(symbol=self.symbol, interval=interval, limit=self.period)
            if 'result' in historical_data and 'list' in historical_data['result']:
                for candle in historical_data['result']['list']:
                    self.open_prices.append(float(candle[1]))    # open price
                    self.high_prices.append(float(candle[2]))    # high price
                    self.low_prices.append(float(candle[3]))     # low price
                    self.closing_prices.append(float(candle[4])) # close price
                    self.volumes.append(float(candle[5]))        # volume
                    self.turnovers.append(float(candle[6]))      # turnover
                logging.info(f"{self.symbol} Исторические данные загружены")
        except Exception as e:
            logging.error(f"Ошибка загрузки исторических данных: {e}")

    def start_trading(self):
        self.ws.kline_stream(interval='1', symbol=self.symbol, callback=lambda msg: self._run_in_thread(self.handle_message, msg))
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

    symbols = ['SOLUSDT', 'LINKUSDT', 'BNBUSDT', 'AXSUSDT', 'MATICUSDT', 'GALAUSDT', 'ORDIUSDT', 'APEUSDT', 'XRPUSDT', 'ADAUSDT', 'DOGEUSDT', 'TRBUSDT', 'DOTUSDT']
    
    # symbols = ['BTCUSDT']

    threads = []
    for symbol in symbols:
        thread = threading.Thread(target=run_trader, args=(symbol, settings))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()