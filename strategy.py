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

with open('settings.json', 'r') as f:
    settings = json.load(f)

api_key = bybit_api_key
api_secret = bybit_secret_key

ws = WebSocket(testnet=True, channel_type="linear", api_key=api_key, api_secret=api_secret)

cash = float(settings["сумма"])
marzha = float(settings["маржа"])
take = float(settings["тейк"])
stop = float(settings["стоп"])

period = 20
multiplier = 2
closing_prices = deque(maxlen=period)
in_position = False
symbol = 'SOLUSDT'
session = HTTP(testnet=False, api_key=bybit_api_key, api_secret=bybit_secret_key)
data = session.get_wallet_balance(accountType="UNIFIED", coin="USDT")
usdt_balance = data['result']['list'][0]['coin'][0]['walletBalance']
print(f'bybit USDT: {usdt_balance}')

try:
    session.set_leverage(category = 'linear', symbol = symbol, buyLeverage=str(marzha), sellLeverage=str(marzha))
except Exception as e:
    pass

def calculate_bollinger_bands(prices, multiplier):
    if len(prices) < period:
        return None, None, None
    average = np.mean(prices)
    std_dev = np.std(prices)
    upper_band = average + (std_dev * multiplier)
    lower_band = average - (std_dev * multiplier)
    return lower_band, average, upper_band

def create_order(symbol, session, side, open_price, cash, marzha, take, stop):
    global in_position
    in_position = True
    dataz = session.get_instruments_info(category="linear", symbol = symbol)
    ord_step = dataz['result']['list'][0]['priceFilter']['tickSize']
    ord_step_num = float(ord_step)

    qty_step = dataz['result']['list'][0]['lotSizeFilter']['qtyStep']
    qty_step_num = float(qty_step)
    
    def dynamic_round(number, step_size):
        getcontext().prec = 28
        number = Decimal(str(number))
        step_size = Decimal(str(step_size))
        decimal_places = len(str(step_size).split(".")[1]) if "." in str(step_size) else 0
        rounded_number = (number / step_size).quantize(1) * step_size

        return round(rounded_number, decimal_places)

    smartQuontity = cash * marzha / open_price
    rounded_smartQuontity = dynamic_round(smartQuontity, qty_step_num)
    try:
        if side == 'LONG':
            result = session.place_order(category = 'linear', symbol = symbol, side = 'Buy', orderType = 'Market', isLeverage = 1, qty = rounded_smartQuontity)
            print('bybit. Открыли LONG')
        elif side == 'SHORT':
            result = session.place_order(category = 'linear', symbol = symbol, side = 'Sell', orderType = 'Market', isLeverage = 1, qty = rounded_smartQuontity)
            print('bybit. Открыли SHORT')
        else:
            print("bybit. Куда растем?")
    except Exception as e:
        print(e)

    datay = session.get_order_history(category="linear", orderId = result.get('result', {}).get('orderId', None))
    new_price = float(datay.get('result', {}).get('list', [])[0].get('avgPrice', 'Не найдено'))
    print('bybit. Средняя цена открытой рыночной сделки:', new_price)

    take_price_ch_short = dynamic_round((new_price - (take * new_price) / (marzha * 100)), ord_step_num)
    stop_price_ch_short = dynamic_round((new_price + (stop * new_price) / (marzha * 100)), ord_step_num)
    take_price_ch_long = dynamic_round((new_price + (take * new_price) / (marzha * 100)), ord_step_num)
    stop_price_ch_long = dynamic_round((new_price - (stop * new_price) / (marzha * 100)), ord_step_num)

    try:
        if side == 'LONG':
            sl_tp_order = session.set_trading_stop(category = 'linear', symbol = symbol, takeProfit=str(take_price_ch_long), tpTriggerBy="MarkPrice", tpslMode="Partial", tpOrderType="Limit", tpSize=str(rounded_smartQuontity), tpLimitPrice = str(take_price_ch_long),
                stopLoss=str(stop_price_ch_long), slTriggerB="MarkPrice", slOrderType="Limit", slSize=str(rounded_smartQuontity), slLimitPrice = str(stop_price_ch_long))
            print("bybit. TP и SL успешно открыты в long")
        elif side == 'SHORT':
            sl_tp_order = session.set_trading_stop(category = 'linear', symbol = symbol, takeProfit=str(take_price_ch_short), tpTriggerBy="MarkPrice", tpslMode="Partial", tpOrderType="Limit", tpSize=str(rounded_smartQuontity), tpLimitPrice = str(take_price_ch_short),
                stopLoss=str(stop_price_ch_short), slTriggerBy="MarkPrice", slOrderType="Limit", slSize=str(rounded_smartQuontity), slLimitPrice = str(stop_price_ch_short))
            print("bybit. TP и SL успешно открыты в short")
        else:
            print("Где стоп?")
    except Exception as e:
        print(f"bybit. Не удалось создать TP и SL: {e}")

def run_in_thread(fn, *args):
    thread = threading.Thread(target=fn, args=args)
    thread.daemon = True
    thread.start()
    return thread

def handle_message(message):
    global in_position, closing_prices
    if 'data' in message and len(message['data']) > 0:
        candle = message['data'][0]
        closing_price = float(candle['close'])

        if candle['confirm'] == True:
            closing_prices.append(closing_price)

        lower_band, sma, upper_band = calculate_bollinger_bands(list(closing_prices), multiplier)
        if lower_band is not None and upper_band is not None:
            if not in_position:
                if closing_price <= lower_band * 0.994:
                    print("Buy Signal detected on false breakout")
                    create_order(symbol, session, "LONG", closing_price, cash, marzha, take, stop)
                elif closing_price >= upper_band * 1.006:
                    print("Sell Signal detected on false breakout")
                    create_order(symbol, session, "SHORT", closing_price, cash, marzha, take, stop)
                else:
                    print("Условия не выполняются")
            else:
                in_position = check_open_positions(symbol, session)

def check_open_positions(symbol, session):
    try:
        response = session.get_positions(category='linear', symbol=symbol)
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

ws.kline_stream(interval='5', symbol=symbol, callback=lambda msg: run_in_thread(handle_message, msg))

try:
    while True:
        sleep(1)
except KeyboardInterrupt:
    ws.close()
    print("WebSocket closed.")