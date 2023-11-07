import json
from pybit.unified_trading import HTTP
from bybit_keys import bybit_api_key, bybit_secret_key
import time
from apscheduler.schedulers.background import BackgroundScheduler
import sys
import csv
import os
from datetime import datetime, timedelta
import asyncio
import pytz
from decimal import Decimal, getcontext

FILENAME = 'settings.json'
def load_settings():
    with open(FILENAME, 'r', encoding='utf-8') as f:
        return json.load(f)

def bybit_save_to_csv(data):
    filename = "bybit.csv"
    file_exists = os.path.isfile(filename)
    with open(filename, 'a', newline='') as csvfile:
        headers = ["Symbol", "Side", "Price", "Qty", "Pnl", "Funding", "Cash", "Marzha", "Take", "Stop", "Time", "Error"]
        writer = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n', fieldnames=headers)
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)

def bybit_search():
    session = HTTP()
    tickers = session.get_tickers(category="linear")
    
    if tickers['retMsg'] == 'OK':
        bybit_list = []
        for ticker in tickers['result']['list']:
            symbol = ticker['symbol']
            if "USDT" in symbol:
                bybit_list.append({
                    'name': symbol,
                    'last_price': ticker['lastPrice'],
                    'funding_rate': round(float(ticker['fundingRate']) * 100, 7),
                    'next_funding_time': ticker['nextFundingTime']
                })
        return bybit_list

    else:
        print("Error:", tickers['retMsg'])
        return None

def compare(bybit_data, stavka):
    bybit_dict = {item['name']: item for item in bybit_data}

    compare_result = []
    status = ["LONG", "SHORT"]
    for symbol, bybit_item in bybit_dict.items():

        bybit_rate = bybit_item['funding_rate']
        comparison = {
            'symbol': symbol,
            'bybit_rate': bybit_rate,
            'difference': abs(bybit_rate),
            'stavka': stavka,
            'message': '',
            'status': {'bybit': ''},
            'price': {'bybit': bybit_item['last_price']}
        }

        if abs(bybit_rate) > stavka:
            comparison['status']['bybit'] = status[0] if bybit_rate < 0 else status[1]

        if comparison['status']['bybit']:
            direction = "LONG" if bybit_rate < 0 else "SHORT"
            comparison['message'] = f"{direction} Bybit ({bybit_rate}%)"
            compare_result.append(comparison)
    return compare_result

def max_comp(compare_result):
    max_diff_item = max(compare_result, key=lambda x: x['difference'])
    return max_diff_item

async def bybit_order(by_res, cash, marzha, take, stop):
    session = HTTP(testnet=False, api_key=bybit_api_key, api_secret=bybit_secret_key)

    data = session.get_wallet_balance(accountType="UNIFIED", coin="USDT")
    usdt_balance = data['result']['list'][0]['coin'][0]['walletBalance']
    print(f'bybit USDT: {usdt_balance}')

    try:
        session.set_leverage(category = 'linear', symbol = by_res['symbol'], buyLeverage=str(marzha), sellLeverage=str(marzha))
    except Exception as e:
        pass
   
    open_price = float(session.get_tickers(category="linear",symbol = by_res['symbol'])["result"]["list"][0]["markPrice"])
    dataz = session.get_instruments_info(category="linear", symbol = by_res['symbol'])
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
        if by_res['status']['bybit'] == 'LONG':
            result = session.place_order(category = 'linear', symbol = by_res['symbol'], side = 'Buy', orderType = 'Market', isLeverage = 1, qty = rounded_smartQuontity, positionIdx = 1)
            print('bybit. Открыли LONG')
        elif by_res['status']['bybit'] == 'SHORT':
            result = session.place_order(category = 'linear', symbol = by_res['symbol'], side = 'Sell', orderType = 'Market', isLeverage = 1, qty = rounded_smartQuontity, positionIdx = 2)
            print('bybit. Открыли SHORT')
        else:
            print("bybit. Куда растем?")
    except Exception as e:
        pass
    
    datay = session.get_order_history(category="linear", orderId = result.get('result', {}).get('orderId', None))
    new_price = float(datay.get('result', {}).get('list', [])[0].get('avgPrice', 'Не найдено'))
    print('bybit. Средняя цена открытой рыночной сделки:', new_price)

    take_price_ch_short = dynamic_round((new_price - (take * new_price) / (marzha * 100)), ord_step_num)
    stop_price_ch_short = dynamic_round((new_price + (stop * new_price) / (marzha * 100)), ord_step_num)
    take_price_ch_long = dynamic_round((new_price + (take * new_price) / (marzha * 100)), ord_step_num)
    stop_price_ch_long = dynamic_round((new_price - (stop * new_price) / (marzha * 100)), ord_step_num)

    try:
        if by_res['status']['bybit'] == 'LONG':
            sl_tp_order = session.set_trading_stop(category = 'linear', symbol = by_res['symbol'], takeProfit=str(take_price_ch_long), tpTriggerBy="MarkPrice", tpslMode="Partial", tpOrderType="Limit", tpSize=str(rounded_smartQuontity), tpLimitPrice = str(take_price_ch_long),
                stopLoss=str(stop_price_ch_long), slTriggerB="MarkPrice", slOrderType="Limit", slSize=str(rounded_smartQuontity), slLimitPrice = str(stop_price_ch_long), positionIdx = 1)
            print("bybit. TP и SL успешно открыты в long")
        elif by_res['status']['bybit'] == 'SHORT':
            sl_tp_order = session.set_trading_stop(category = 'linear', symbol = by_res['symbol'], takeProfit=str(take_price_ch_short), tpTriggerBy="MarkPrice", tpslMode="Partial", tpOrderType="Limit", tpSize=str(rounded_smartQuontity), tpLimitPrice = str(take_price_ch_short),
                stopLoss=str(stop_price_ch_short), slTriggerBy="MarkPrice", slOrderType="Limit", slSize=str(rounded_smartQuontity), slLimitPrice = str(stop_price_ch_short), positionIdx = 2)
            print("bybit. TP и SL успешно открыты в short")
        else:
            print("Где стоп?")
    except Exception as e:
        print(f"bybit. Не удалось создать TP и SL: {e}")

    def convert_timestamp_to_human_readable(timestamp):
        dt = datetime.fromtimestamp(int(timestamp) / 1000.0)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    
    order_data = {
        "Symbol": by_res['symbol'],
        "Side": by_res['status']['bybit'],
        "Price": new_price,
        "Qty": smartQuontity,
        "Pnl": None,
        "Funding": by_res['bybit_rate'],
        "Cash": cash,
        "Marzha": marzha,
        "Take": take,
        "Stop": stop,
        "Time": convert_timestamp_to_human_readable(result['time']),
        "Error": "successfully"
        }

    bybit_save_to_csv(order_data)

async def open_order(res, cash, marzha, take, stop):
    print(res)
    task_bybit = asyncio.create_task(bybit_order(res, cash, marzha, take, stop))

    await asyncio.gather(task_bybit)

async def main(stavka, cash, marzha, take, stop):
    try:
        schedule_time_str = sys.argv[1]
        schedule_time = datetime.strptime(schedule_time_str, '%Y-%m-%d %H:%M:%S')
        order_time = schedule_time + timedelta(minutes=2)
        compare_result = compare(bybit_search(), stavka)
        MAX = max_comp(compare_result)
        current_time = datetime.now()
        print('Ожидаем указанное время исполнения заявки')
        if current_time < order_time:
            await asyncio.sleep((order_time - current_time).total_seconds())
        await open_order(MAX, cash, marzha, take, stop)
    except Exception as e:
        print('Что-то пошло не так: {}'.format(e))
        utc_zone = pytz.timezone('UTC')

        current_time = datetime.now(utc_zone).strftime('%Y-%m-%d %H:%M:%S')
        data = {
            "Symbol": None, "Side": None, "Price": None, "Qty": None, 
            "Pnl": None, "Funding": None, "Cash": None, "Marzha": None, 
            "Take": None, "Stop": None, "Time": current_time, "Error": str(e)
        }
        bybit_save_to_csv(data)

if __name__ == "__main__":
    settings = load_settings()
    asyncio.run(main(float(settings["ставка"]), int(settings["сумма"]), int(settings["маржа"]), int(settings["тейк"]), int(settings["стоп"])))
