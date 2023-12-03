from bybit_keys import bybit_api_key, bybit_secret_key
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
        self.period = 90
        self.multiplier = 2
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
                    logging.info("%s. TP и SL успешно открыты в long", self.symbol)
                elif side == 'SHORT':
                    take_price_ch_short = dynamic_round((new_price - (self.take * new_price) / (self.marzha * 100)), ord_step_num)
                    stop_price_ch_short = dynamic_round((new_price + (self.stop * new_price) / (self.marzha * 100)), ord_step_num)         
                    order = self.session.set_trading_stop(category = 'linear', symbol = self.symbol, takeProfit=str(take_price_ch_short), tpTriggerBy="MarkPrice", tpslMode="Partial", tpOrderType="Limit", tpSize=str(rounded_smartQuontity), tpLimitPrice = str(take_price_ch_short), positionIdx = 2)   
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
                    if current_close_price <= lower_band * 0.995:
                        self.create_order("LONG", candle['close'])
                        logging.info(f"{self.symbol} Сигнал на покупку lower_band: {lower_band}, ema: {ema}, upper_band: {upper_band}")
                    elif current_close_price >= upper_band * 1.005: 
                        self.create_order("SHORT", candle['close'])
                        logging.info(f"{self.symbol} Сигнал на продажу lower_band: {lower_band}, ema: {ema}, upper_band: {upper_band}")
                    else:
                        logging.debug(f"{self.symbol} Условия не выполняются")
                else:
                    self.in_position = self.check_open_positions()
                    # try:
                    #     self.in_second_position = self.check_second_positions()
                    #     if not self.in_second_position:
                    #         self.open_second_position()
                    # except Exception as e:
                    #     logging.error(f"Ошибка при открытии усредняющей сделки: {e}")

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

    def check_second_positions(self):
        try:
            response = self.session.get_positions(category='linear', symbol=self.symbol)
            if response['retCode'] == 0 and response['result']:
                active_positions = [position for position in response['result']['list'] if float(position['size']) > 0]
                num_active_positions = len(active_positions)
                return num_active_positions >= 2
            else:
                return False
        except Exception as e:
            logging.error(f"Ошибка при проверке открытых позиций: {e}")
            return False

    def open_second_position(self):
        self.in_second_position = True
        try:
            response = self.session.get_positions(category='linear', symbol=self.symbol)
            if response['retCode'] != 0 or not response['result']:
                logging.error(f"Не удалось получить текущую позицию: {response.get('retMsg', 'Unknown Error')}")
                return
            
            def dynamic_round(number, step_size):
                getcontext().prec = 10
                number = Decimal(str(number))
                step_size = Decimal(str(step_size))
                decimal_places = len(str(step_size).split(".")[1]) if "." in str(step_size) else 0

                rounded_number = (number // step_size) * step_size
                return rounded_number.quantize(Decimal(10) ** -decimal_places)
            
            dataz = self.session.get_instruments_info(category="linear", symbol=self.symbol)
            ord_step = dataz['result']['list'][0]['priceFilter']['tickSize']
            ord_step_num = float(ord_step)

            current_position = response['result']['list'][0]
            size = float(current_position['size'])
            side = current_position['side']
            unrealised_pnl = float(current_position['unrealisedPnl'])
            position_value = size * float(current_position['avgPrice'])

            if unrealised_pnl * self.marzha / position_value  <= -30:
                logging.info(f"side {side} unrealised_pnl * self.marzha / position_value {unrealised_pnl * self.marzha / position_value}")
                # try:
                #     result = self.session.place_order(category='linear', symbol=self.symbol, side=side, 
                #                                 orderType='Market', qty=size, isLeverage=1, 
                #                                 positionIdx=1 if side == 'Buy' else 2)
                #     logging.info(f"{self.symbol}. Усредненная позиция открыта")
                # except Exception as e:
                #     logging.error("Произошла ошибка в выставлении УСРЕДНЯЮЩЕЙ сделки: %s", e)
                
                # time.sleep(0.5)

                # for attempt in range(5):
                #     try:
                #         datay = self.session.get_order_history(category="linear", orderId=result.get('result', {}).get('orderId', None))
                #         list_data = datay.get('result', {}).get('list', [])
                #         if not list_data:
                #             raise ValueError("Список истории заказов пуст усредняющей сделки")

                #         new_avg_price = float(list_data[0].get('avgPrice', 0))
                #         if new_avg_price == 0:
                #             raise ValueError("Не удалось получить среднюю цену усредняющей сделки")
                #         logging.info(f'{self.symbol}. Средняя цена открытой усредняющей сделки: {new_avg_price}')

                #         take_price_ch_long = dynamic_round((new_avg_price + (2 * self.take * new_avg_price) / (self.marzha * 100)), ord_step_num)
                #         take_price_ch_short = dynamic_round((new_avg_price - (2 * self.take * new_avg_price) / (self.marzha * 100)), ord_step_num)

                #         self.session.set_trading_stop(category='linear', symbol=self.symbol, 
                #                                     takeProfit=str(take_price_ch_long) if side == 'Buy' else str(take_price_ch_short), 
                #                                     tpTriggerBy="MarkPrice", 
                #                                     tpslMode="Partial", tpOrderType="Limit", 
                #                                     tpSize=str(size), tpLimitPrice=str(take_price_ch_long) if side == 'Buy' else str(take_price_ch_short), 
                #                                     positionIdx=1 if side == 'Buy' else 2)
                #         logging.info(f"{self.symbol}. Тейк-профит усредняющей сделки установлен на уровне {str(take_price_ch_long) if side == 'Buy' else str(take_price_ch_short)}")
                #     except Exception as e:
                #         logging.error(f"Попытка {attempt + 1} - Ошибка при установлении TP и SL усредняющей сделки: {e}")
                #         if attempt == 4:
                #             logging.error("Не удалось установить TP и SL усредняющей сделки после 5 попыток")
                #         time.sleep(1)
            else:
                logging.debug(f"{self.symbol} Условия для усреднения не выполнены")
        except Exception as e:
            logging.error(f"Ошибка при усреднении позиции: {e}")

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

    symbols = ['XRPUSDT', 'TRBUSDT', 'DOTUSDT', 'BTCUSDT', 'ETHUSDT', 'AVAXUSDT', 'MATICUSDT', 'ADAUSDT', 'APTUSDT', 'BNBUSDT', 'LINKUSDT', 'LTCUSDT']
    # symbols = ['BTCUSDT']

    threads = []
    for symbol in symbols:
        thread = threading.Thread(target=run_trader, args=(symbol, settings))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()