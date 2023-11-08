from bybit_keys import bybit_api_key, bybit_secret_key
from pybit.unified_trading import WebSocket
from pybit.unified_trading import HTTP
from time import sleep
import numpy as np
from collections import deque
import json
import time
from datetime import datetime, timedelta
from decimal import Decimal, getcontext
import threading
import asyncio

class WebSocketManager:
    def __init__(self, api_key, api_secret, symbol, testnet=True):
        self.api_key = api_key
        self.api_secret = api_secret
        self.symbol = symbol
        self.testnet = testnet
        self.ws = None  # WebSocket connection will be stored here

    async def connect(self):
        """Асинхронное подключение к WebSocket."""
        self.ws = WebSocket(testnet=self.testnet, channel_type="linear", api_key=self.api_key, api_secret=self.api_secret)
        await self.ws.connect()

    async def subscribe_to_kline(self, interval, trade_manager):
        """Асинхронная подписка на поток Kline."""
        await self.ws.kline_stream(interval=interval, symbol=self.symbol, callback=lambda msg: asyncio.create_task(trade_manager.analyze_market_data(msg)))

    async def close(self):
        """Асинхронное закрытие соединения."""
        await self.ws.close()

    async def subscribe_to_kline_multiple(self, symbols, interval, trade_managers):
        """Асинхронная подписка на потоки Kline для нескольких символов."""
        for symbol in symbols:
            await self.ws.kline_stream(
                interval=interval, 
                symbol=symbol, 
                callback=lambda msg, symbol=symbol: asyncio.create_task(trade_managers[symbol].analyze_market_data(msg))
            )

def dynamic_round(number, step_size):
    getcontext().prec = 28
    number = Decimal(str(number))
    step_size = Decimal(str(step_size))
    decimal_places = len(str(step_size).split(".")[1]) if "." in str(step_size) else 0
    rounded_number = (number / step_size).quantize(1) * step_size
    return round(rounded_number, decimal_places)

class TradeManager:
    def __init__(self, api_key, api_secret, symbol, settings):
        self.api_key = api_key
        self.api_secret = api_secret
        self.symbol = symbol
        self.settings = settings
        self.session = None
        self.in_position = False
        self.closing_prices = deque(maxlen=settings["period"])
        self.period = settings['period']
        self.multiplier = settings['multiplier']
        self.cash = settings['сумма']
        self.marzha = settings['маржа']
        self.take = settings['тейк']
        self.stop = settings['стоп']
        self.closing_prices = deque(maxlen=self.period)

    def calculate_bollinger_bands(self, prices):
        if len(prices) < self.period:
            return None, None, None
        average = np.mean(prices)
        std_dev = np.std(prices)
        upper_band = average + (std_dev * self.multiplier)
        lower_band = average - (std_dev * self.multiplier)
        return lower_band, average, upper_band

    async def analyze_market_data(self, message):
        # Обработка сообщений от вебсокета
        if 'data' in message and len(message['data']) > 0:
            candle = message['data'][0]
            closing_price = float(candle['close'])

            if candle['confirm'] == True:
                self.closing_prices.append(closing_price)

            lower_band, sma, upper_band = self.calculate_bollinger_bands(list(self.closing_prices))
            if lower_band is not None and upper_band is not None:
                if not self.in_position:
                    if closing_price <= lower_band * 0.994:
                        print("Buy Signal detected on false breakout")
                        await self.execute_trade("LONG", closing_price)
                    elif closing_price >= upper_band * 1.006:
                        print("Sell Signal detected on false breakout")
                        await self.execute_trade("SHORT", closing_price)
                    else:
                        print("Условия не выполняются")
                else:
                    self.in_position = await self.check_open_positions()

    async def execute_trade(self, side, open_price):
        self.in_position = True
        dataz = await self.session.get_instruments_info(category="linear", symbol=self.symbol)
        ord_step = dataz['result']['list'][0]['priceFilter']['tickSize']
        ord_step_num = float(ord_step)

        qty_step = dataz['result']['list'][0]['lotSizeFilter']['qtyStep']
        qty_step_num = float(qty_step)

        smartQuantity = self.cash * self.marzha / open_price
        rounded_smartQuantity = dynamic_round(smartQuantity, qty_step_num)
        try:
            if side == 'LONG':
                result = await self.session.place_order(category='linear', symbol=self.symbol, side='Buy', orderType='Market', isLeverage=1, qty=rounded_smartQuantity)  # Исправлено здесь
                print('bybit. Открыли LONG')
            elif side == 'SHORT':
                result = await self.session.place_order(category='linear', symbol=self.symbol, side='Sell', orderType='Market', isLeverage=1, qty=rounded_smartQuantity)  # Исправлено здесь
                print('bybit. Открыли SHORT')
            else:
                print("bybit. Куда растем?")
        except Exception as e:
            print(e)

        datay = await self.session.get_order_history(category="linear", orderId=result.get('result', {}).get('orderId', None))  # Добавлено await и self здесь
        new_price = float(datay.get('result', {}).get('list', [])[0].get('avgPrice', 'Не найдено'))
        print('bybit. Средняя цена открытой рыночной сделки:', new_price)

        take_price_ch_short = dynamic_round((new_price - (self.take * new_price) / (self.marzha * 100)), ord_step_num)
        stop_price_ch_short = dynamic_round((new_price + (self.stop * new_price) / (self.marzha * 100)), ord_step_num)
        take_price_ch_long = dynamic_round((new_price + (self.take * new_price) / (self.marzha * 100)), ord_step_num)
        stop_price_ch_long = dynamic_round((new_price - (self.stop * new_price) / (self.marzha * 100)), ord_step_num)

        try:
            if side == 'LONG':
                sl_tp_order = await self.session.set_trading_stop(category='linear', symbol=self.symbol, takeProfit=str(take_price_ch_long), tpTriggerBy="MarkPrice", tpslMode="Partial", tpOrderType="Limit", tpSize=str(rounded_smartQuantity), tpLimitPrice=str(take_price_ch_long),
                    stopLoss=str(stop_price_ch_long), slTriggerBy="MarkPrice", slOrderType="Limit", slSize=str(rounded_smartQuantity), slLimitPrice=str(stop_price_ch_long))  # Исправлено здесь
                print("bybit. TP и SL успешно открыты в long")
            elif side == 'SHORT':
                sl_tp_order = await self.session.set_trading_stop(category='linear', symbol=self.symbol, takeProfit=str(take_price_ch_short), tpTriggerBy="MarkPrice", tpslMode="Partial", tpOrderType="Limit", tpSize=str(rounded_smartQuantity), tpLimitPrice=str(take_price_ch_short),
                    stopLoss=str(stop_price_ch_short), slTriggerBy="MarkPrice", slOrderType="Limit", slSize=str(rounded_smartQuantity), slLimitPrice=str(stop_price_ch_short))  # Исправлено здесь
                print("bybit. TP и SL успешно открыты в short")
            else:
                print("Где стоп?")
        except Exception as e:
            print(f"bybit. Не удалось создать TP и SL: {e}")

    async def check_open_positions(self):
        try:
            response = await self.session.get_positions(category='linear', symbol=self.symbol)
            if response['retCode'] == 0 and response['result']:
                if any(float(position['size']) > 0 for position in response['result']['list']):
                    print(f"У вас есть открытая позиция на {symbol}.")
                    return True
                else:
                    print("Активных позиций нет.")
                    return False
        except Exception as e:
            print(f"Ошибка при проверке открытых позиций: {e}")
            return None

    async def setup(self):
        """Асинхронная настройка сессии."""
        self.session = HTTP(testnet=False, api_key=self.api_key, api_secret=self.api_secret)

async def main():
    # Загрузка настроек из файла
    with open('settings.json', 'r') as f:
        settings = json.load(f)

    # Загрузка настроек и инициализация классов
    symbols = ['SOLUSDT', 'BTCUSDT', 'ETHUSDT']  # Пример списка монет
    trade_managers = {symbol: TradeManager(bybit_api_key, bybit_secret_key, symbol, settings) for symbol in symbols}
    ws_manager = WebSocketManager(bybit_api_key, bybit_secret_key)

    # Настройка соединения и подписка на потоки данных
    for symbol, trade_manager in trade_managers.items():
        await trade_manager.setup()

    await ws_manager.connect()
    await ws_manager.subscribe_to_kline_multiple(symbols, '5', trade_managers)

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        await ws_manager.close()

# Запуск основного цикла асинхронно
asyncio.run(main())
