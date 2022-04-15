import mysql.connector
import json
import os
import ast
import datetime
import time
import http.client
import pika
import uuid
import ssl
import platform
import requests
import robot_many as many
import robot_conditions as conditions
from datetime import timedelta
from urllib.parse import urlparse
from sys import argv
from loguru import logger

#from binance.client import Client

ssl._create_default_https_context = ssl._create_unverified_context

empty_time_candles = 1
spread = 0.03
skip_min = 2

SYMBOL = 'batusdt'
SYMBOL = argv[1]
MYSQL_TABLE_READ = SYMBOL + '_price'

TELEGRAM_METADATA = {"channel_id":"-1001541461039", "token":"1878579785:AAH0yC1onsi-5bVkAIygXEHG5PK18-UZisI"}
MYSQL_TABLE_CONFIG = '0_config'

print('=============================================================================')

launch = {}
data_base_name = 'dbconfig.json'


directory = os.path.dirname(__file__)
print(directory)
file_keys_path = f"{os.path.dirname(__file__)}{data_base_name}"
#file_keys_path = f"{os.path.dirname(__file__)}\\{data_base_name}"

print(f"{file_keys_path=}")
logger.add("debug.log", format="{time} {level} {message}", level="DEBUG")

def get_data(file_keys_path):
    with open(file_keys_path, 'r', encoding='utf-8') as data_file:
        data = json.load(data_file)
    return data

def get_keys(file_keys_path):
    with open(file_keys_path, 'r') as data_file:
        data = json.load(data_file)
        API_KEY =data['trading_apikey_1']
        API_SECRET = data['trading_secretkey_1']
    return API_KEY, API_SECRET

API_KEY, API_SECRET = get_keys(file_keys_path)
data = get_data(file_keys_path)

launch['db'] = {}
launch['db']['user'] = data['db_user']
launch['db']['password'] = data['db_pass']
launch['db']['host'] = data['db_host']
launch['db']['database'] = data['db_name']

launch['mode'] = data['db_name'][:-16]
launch['traiding_mode'] = 'many'

launch['db']['api_key'] = API_KEY
launch['db']['api_sekret'] = API_SECRET
launch['db']['pair'] = SYMBOL


def get_db_connection(user, password, host, database):
    while True:
        try:
            cnx = mysql.connector.connect(user=user, password=password,
                                          host=host,
                                          database=database,
                                          connection_timeout=2)
            cnx.autocommit = True
            break
        except Exception as e:
            time.sleep(2)
            print(e)

    return cnx


cn_db = get_db_connection(launch['db']['user'], launch['db']['password'], launch['db']['host'],
                          launch['db']['database'])
cursor_db = cn_db.cursor()

keys_candle_table = []

def get_0_config(cursor, table):
    query = f"SELECT percent_level, trading_status, stream_1, stream_2 FROM {table} WHERE symbol = '{SYMBOL.upper()}'"
    cursor.execute(query)
    rows = cursor.fetchone()
    launch['frame'] = str(rows[0])
    launch['trading_status'] = str(rows[1])
    algorithm = [str(a) for a in rows[2:4] if int(a)]
    launch['many_metadata'] = {'streams': [{'algorithm': a, 'id': str(id + 1)} for id, a in enumerate(algorithm) if a], 'balance': 'currency'}
    print(f"{launch['many_metadata']=}")


def custom_round(price):
    price = round(price, 2)
    price_ceil = float(int(price))
    ost = float(price) - price_ceil
    if ost >= 0 and ost < 0.25:
        return price_ceil
    elif ost >= 0.25 and ost < 0.75:
        return price_ceil + 0.5
    else:
        return price_ceil + 1


def get_trading_status():
    global cn_db

    try:

        cursor_local = cn_db.cursor(buffered=True)
        query = (f"SELECT trading_status FROM {MYSQL_TABLE_CONFIG} WHERE symbol = '{SYMBOL.upper()}'")
        cursor_local.execute(query)
        result = cursor_local.fetchone()
        for (trading_status) in result:
            return trading_status

        return 'on'
    except Exception as e:
        print(e)
        cn_db = get_db_connection(launch['db']['user'], launch['db']['password'], launch['db']['host'],
                                  launch['db']['database'])
        return get_trading_status()


cnx = get_db_connection(launch['db']['user'], launch['db']['password'], launch['db']['host'], launch['db']['database'])
cursor_candles = cnx.cursor()

cnx2 = get_db_connection(launch['db']['user'], launch['db']['password'], launch['db']['host'], launch['db']['database'])
cursor = cnx2.cursor()

cn_pos = get_db_connection(launch['db']['user'], launch['db']['password'], launch['db']['host'],
                           launch['db']['database'])


def get_new_order(order, launch):
    if order == None:
        order = {}

    order['balance'] = 1

    order['open_price_position'] = 0
    order['close_price_position'] = 0

    order['open_time_order'] = 0
    order['open_time_position'] = 0
    order['close_time_position'] = 0
    order['close_time_order'] = 0

    order['trailings'] = {}
    order['abs'] = {}
    order['reject'] = {}
    order['candle_direction'] = {}
    order['uuid'] = str(uuid.uuid4())

    order['leverage'] = 1
    order['price_indent'] = 0
    order['direction'] = None
    order['order_type'] = None
    order['state'] = 'start'
    order['path'] = ''

    order['proboi'] = {}

    order['condition_checked_candle'] = None

    order['cache_conditions'] = {}

    if launch.get('many_metadata') != None and launch.get('many_metadata') != {}:
        order['leverage'] = 0
        order['equity'] = 1

    order['max_equity'] = order['equity']

    return order

@logger.catch
def init_launch(launch):
    algorithm_prefix = 'algorithm_'

    get_0_config(cursor, MYSQL_TABLE_CONFIG)

    launch['start_time'] = datetime.datetime(2012, 1, 1)
    launch['end_time'] = datetime.datetime(2032, 1, 1)

    launch['telegram_metadata'] = TELEGRAM_METADATA
    launch['exchange_metadata'] = {"exchange": "binance"}

    launch['streams'] = []
    launch['id_candle'] = 0
    launch['last_price'] = 0
    launch['empty_time_candles'] = 0


    symbol_numb = launch['frame'].find(".")

    if symbol_numb != -1:
        launch.setdefault('renko', {})
        launch['renko']['step'] = float(launch['frame'])
    else:
        launch['time_frame'] = int(launch['frame'])

    if launch['traiding_mode'] == 'many':
        for ord_many in launch['many_metadata']['streams']:
            stream_element = {}
            stream_element['algorithm'] = algorithm_prefix + str(ord_many['algorithm'])
            stream_element['order'] = get_new_order(None, launch)
            stream_element['cur_conditions_group'] = {}
            stream_element['id'] = ord_many['id']
            stream_element['url'] = ord_many.setdefault('url', 'http://localhost:1010')
            stream_element['balancing_symbol'] = ord_many.setdefault('balancing_symbol', '')
            #stream_element['symbol'] = ord_many.setdefault('symbol', '')
            launch['streams'].append(stream_element)
    else:
        stream_element = {}
        stream_element['algorithm'] = algorithm_prefix + str(1)
        stream_element['order'] = get_new_order(None, launch)
        stream_element['cur_conditions_group'] = {}
        launch['streams'].append(stream_element)


init_launch(launch)


def db_get_algorithm(stream):
    print("db_get_algorithm")
    try:
        cursor.execute(f"SELECT * FROM {SYMBOL}_{stream['algorithm']}")
    except Exception as e:
        print('Ошибка получения таблицы с настройками, причина: ')
        print(e)
    rows1 = cursor.fetchall()

    stream['algorithm_data'] = {}

    stream['algorithm_data']['block_order'] = {}
    iter = 0

    stream['algorithm_data']['blocks_data'] = rows1
    for gg in rows1:
        stream['algorithm_data']['block_order'][str(gg[0])] = iter
        iter = iter + 1


cn_tick = get_db_connection(launch['db']['user'], launch['db']['password'], launch['db']['host'],
                            launch['db']['database'])

launch['price_table_name'] = MYSQL_TABLE_READ

cur_time_utc = datetime.datetime.utcnow()
cur_time_frame = {}

keys = []

table_result = SYMBOL + '_positions'

"""if launch['mode'] != 'robot':
    try:
        cursor.execute("TRUNCATE TABLE {0}".format(table_result))
    except Exception as e:
        print('Ошибка получения таблицы с результами, причина: ')
        print(e)"""

candle = {}
prev_candle = {}
prev_prev_candle = {}
next_candle = {}
robot_is_stoped = True


# ---------- mode ---------------

def log_condition(time, info):
    if time == None:
        time = datetime.datetime.utcnow()
    print(str(time) + " --- " + info)


def get_cur_time():
    return datetime.datetime.utcnow()


def update_candle(launch):
    launch['id_candle'] = launch['id_candle'] + 1


def get_cur_timeframe(cur_time_frame, cur_time, time_frame):
    if cur_time_frame == {}:
        cur_time_frame['start'] = cur_time.replace(hour=0, minute=0, second=0, microsecond=0)
        cur_time_frame['finish'] = cur_time_frame['start'] + timedelta(minutes=time_frame)

    while True:
        if cur_time_frame['start'] <= cur_time and cur_time < cur_time_frame['finish']:
            break
        else:
            cur_time_frame['start'] = cur_time_frame['start'] + timedelta(minutes=time_frame)
            cur_time_frame['finish'] = cur_time_frame['start'] + timedelta(minutes=time_frame)

    return cur_time_frame


def set_candle(launch, keys, cursor, candle, prev_candle, prev_prev_candle, stat):
    print("set_candle")
    #if launch['mode'] == 'tester':
    get_price_from_table(launch, candle, 0)
    if candle == {}:
        return
    candle['price'] = float(candle['price'])
    cur_time = candle['time']

    #if launch['mode'] == 'robot':
    #    candle.clear()
    #    cur_time = get_cur_time()
        #if launch['exchange_metadata']['exchange'] == 'binance_futures':
        #    price = get_binance_price(launch)
        #elif launch['exchange_metadata']['exchange'] == 'deribit':
        #    price = get_deribit_price(launch)
    #    price = get_deribit_price(launch)
    #    if price != None:
    #        candle['price'] = price
    #        candle['time'] = cur_time


    if cur_time_frame == {} or cur_time > cur_time_frame['finish']:
        cur_time_frame.update(get_cur_timeframe(cur_time_frame, cur_time, launch['time_frame']))
        launch['cur_candle'] = {}
        launch['cur_candle']['open'] = candle['price']
    else:
        if launch['mode'] != 'robot':
            return

    prev_candle_time = cur_time_frame['start'] - launch['time_frame'] * datetime.timedelta(seconds=60)
    prev_candle_prom = get_indicators(prev_candle_time, launch['price_table_name'])
    if prev_candle_prom != None and prev_candle_prom != {}:
        if ((prev_candle == {}) or (prev_candle != {} and prev_candle['time'] != prev_candle_prom['time'])):
            for stream in launch['streams']:
                stream['was_close'] = False
                stream['was_open'] = False
            many.set_equity(launch, prev_candle, prev_prev_candle, stat, cursor, candle)
            update_candle(launch)
            if launch['mode'] == 'robot':
                print("prev_candle: " + str(prev_candle_prom))
        prev_candle.update(prev_candle_prom)
    elif prev_candle_prom == None:
        prev_candle.clear()

    prev_prev_candle_time = cur_time_frame['start'] - 2 * launch['time_frame'] * datetime.timedelta(seconds=60)
    prev_prev_candle_prom = get_indicators(prev_prev_candle_time, launch['price_table_name'])
    if prev_prev_candle_prom != None and prev_prev_candle_prom != {}:
        if ((prev_prev_candle == {}) or (
                prev_prev_candle != {} and prev_prev_candle['time'] != prev_prev_candle_prom['time'])):
            if launch['mode'] == 'robot':
                print("prev_prev_candle: " + str(prev_prev_candle_prom))
        prev_prev_candle.update(prev_prev_candle_prom)
    elif prev_prev_candle_prom == None:
        prev_prev_candle.clear()


def get_max_tick():
    global cn_db
    global cursor_db

    tick_table_name = MYSQL_TABLE_READ
    query = ("select MAX(id) from {0}".format(tick_table_name))
    cursor.execute(query)
    for (max_id) in cursor:
        if len(max_id) != 0:
            return max_id[0]

    return 0


def set_candle_renko(launch, keys, cursor, candle, prev_candle, prev_prev_candle, next_candle, stat):
    print("set_candle_renko")


    id_tick = 0
    get_price_from_table(launch, candle, id_tick)

    if candle == {}:
        return

    candle['price'] = float(candle['price'])
    cur_time = candle['time']

    if next_candle == {} or cur_time >= next_candle['time']:
        cur_candle = select_renko_candles(cur_time, launch['price_table_name'], prev_candle, prev_prev_candle,
                                          next_candle)
        launch['cur_candle'] = {}
        launch['cur_candle']['open'] = candle['price']

        if launch['renko'].get('last_candle_id') != None and launch['renko']['last_candle_id'] == prev_candle['id']:
            return
        many.set_equity(launch, prev_candle, prev_prev_candle, stat, cursor, candle)
        for stream in launch['streams']:
            stream['was_close'] = False
            stream['was_open'] = False

        cur_time_frame['start'] = cur_candle['time']
        if prev_candle != {}:
            launch['renko']['last_candle_id'] = prev_candle['id']


def get_indicators(candle_time, table_name):
    result = select_candle(candle_time, table_name)
    if result != {}:
        return result
    else:
        return None


def select_candle(date_time, table_name):
    global cn_db
    global cursor_db
    global keys_candle_table

    try:

        date_time = date_time.replace(second=0)
        date_time = date_time.replace(microsecond=0)
        insert_stmt = ("select {0} from {1} where time = '{2}'".format("*", table_name, date_time))

        cursor_db.execute(insert_stmt)

        if len(keys_candle_table) == 0:
            keys_name = cursor_db.description
            for row in keys_name:
                keys_candle_table.append(row[0])

        candle = {}

        for row in cursor_db:
            for ss in keys_candle_table:
                candle[ss] = row[keys_candle_table.index(ss)]

        return candle

    except Exception as e:
        print(e)
        cn_db = get_db_connection(launch['db']['user'], launch['db']['password'], launch['db']['host'],
                                  launch['db']['database'])
        cursor_db = cn_db.cursor()
        return select_candle(date_time, table_name)


def select_renko_candles(date_time, table_name, prev_candle, prev_prev_candle, next_candle):
    global cn_db
    global cursor_db
    global keys_candle_table
    global cur_time_frame

    try:

        query = (
            "select {0} from {1} where time <= '{2}' order by time desc LIMIT 3".format("*", table_name, date_time))
        cursor_db.execute(query)

        if len(keys_candle_table) == 0:
            keys_name = cursor_db.description
            for row in keys_name:
                keys_candle_table.append(row[0])

        candle = {}

        i = 1
        for row in cursor_db:
            for ss in keys_candle_table:
                if i == 1:
                    candle[ss] = row[keys_candle_table.index(ss)]
                elif i == 2:
                    prev_candle[ss] = row[keys_candle_table.index(ss)]
                elif i == 3:
                    prev_prev_candle[ss] = row[keys_candle_table.index(ss)]
            i = i + 1

        next_candle.clear()

        query = ("select {0} from {1} where time > '{2}' order by time asc LIMIT 1".format("*", table_name, date_time))
        cursor_db.execute(query)

        for row in cursor_db:
            for ss in keys_candle_table:
                next_candle[ss] = row[keys_candle_table.index(ss)]

        return candle

    except Exception as e:
        print(e)
        cn_db = get_db_connection(launch['db']['user'], launch['db']['password'], launch['db']['host'],
                                  launch['db']['database'])
        cursor_db = cn_db.cursor()
        return select_renko_candles(date_time, table_name, next_candle)


def get_deribit_price(launch):
    #для launch нет symbol
    launch['symbol'] = 'BTC-PERPETUAL'
    #launch['symbol'] = launch['db']['pair']
    launch['exchange_metadata']['host'] = 'www.deribit.com'
    try:
        connection = http.client.HTTPSConnection(launch['exchange_metadata']['host'], timeout=7)
        connection.request("GET", "/api/v2/public/get_last_trades_by_instrument?count=1&instrument_name={0}".format(
            launch['symbol']))
        response = json.loads(connection.getresponse().read().decode())
        connection.close()

        if response.get('result') != None and response['result'].get('trades') != None and len(
                response['result']['trades']) > 0:
            price = response['result']['trades'][0]['price']
            print(f"deribit price = {price}, time = {datetime.datetime.utcnow()}")
            return price
        else:
            time.sleep(2)
            print(f"deribit error, time = {datetime.datetime.utcnow()}")
            return None
    except Exception as e:
        time.sleep(2)
        print(e)
        print(f"deribit exception, time = {datetime.datetime.utcnow()}")
        return None

#def get_binance_price(launch):
#    try:
#        client = Client(launch['db']['api_key'], launch['db']['api_sekret'])
#        price = float(client.futures_aggregate_trades(symbol=launch['db']['pair'])[0]['p'])
#        print(f"binance price = {price}, time = {datetime.datetime.utcnow()}")
#        return price
#    except Exception as e:
#        time.sleep(2)
#        print(e)
#        print(f"binance exception, time = {datetime.datetime.utcnow()}")
#        return None


def get_price_from_table(launch, candle, last_id):
    print("get_price_from_table")
    tick_table_name = MYSQL_TABLE_READ
    #print(f"1{launch['ticks']=}")
    #print(f"{launch.get('ticks')=}")
    if launch.get('ticks') == None:
        launch['ticks'] = {}
        ticks = launch['ticks']
        ticks['connection'] = cn_tick
        ticks['cursor'] = ticks['connection'].cursor()
        query = (f"select id, time, close from {tick_table_name} where id > {last_id} and time BETWEEN %s AND %s")
        ticks['cursor'].execute(query, (launch['start_time'], launch['end_time']))
        print(f"2{launch['ticks']=}")
        ticks['keys'] = []
        keys_name = ticks['cursor'].description
        #for row in keys_name:
        #    ticks['keys'].append(row[0])
        ticks['keys'].extend(['id', 'time', 'price'])
    print(f"{launch['ticks']=}")
    try:
        row = launch['ticks']['cursor'].fetchone()

    except Exception as e:
        print(e)
        print('error - last id - 1')
        id = launch['ticks']['last_id']
        print('error - last id - 2')
        launch['ticks'] = None
        print('error - last id - 3')
        get_price_from_table(launch, candle, id)
        print('error - last id - 4')
        return
    print(f"{row=}")
    if row == None:
        launch['ticks']['connection'].close()
        candle.clear()
    else:
        launch['ticks']['last_id'] = row[0]

        for ss in launch['ticks']['keys']:
            candle[ss] = row[launch['ticks']['keys'].index(ss)]
        print(f"{candle=}")


# ---------- constructors ---------------

def get_new_statistics():
    stat = {}
    stat['profit_points'] = 0
    stat['loss_points'] = 0
    stat['loss_sum'] = 0
    stat['profit_sum'] = 0
    stat['percent_position'] = 0
    stat['last_percent_position'] = 0
    stat['percent_positions'] = 0
    stat['percent_series'] = 0

    stat['losses_money'] = 0

    stat['cur_month'] = 0
    stat['month_percent'] = 0
    stat['last_month_percent'] = 0

    stat['max_month_percent'] = 0
    stat['rollback_month_percent'] = 0

    stat['many'] = {}

    return stat


def manage_order_tester(order, prev_candle, launch, candle):
    skip_order = False
    if launch['mode'] != 'tester':
        return skip_order

    if launch['renko'] == None:
        if prev_candle == {}:
            launch['empty_time_candles'] = launch['empty_time_candles'] + 1
            if launch['empty_time_candles'] >= empty_time_candles:
                skip_order = True
        else:
            launch['empty_time_candles'] = 0
    else:
        if prev_candle != {} and skip_min != 0:
            percent = 2 * skip_min * launch['renko']['step']
            if candle['price'] > float(prev_candle['close']) * (1 + percent / 100):
                skip_order = True
            elif candle['price'] < float(prev_candle['close']) * (1 - percent / 100):
                skip_order = True

    if skip_order:
        order = get_new_order(order, launch)
        launch['cur_conditions_group'] = {}
        print('skip order')

    return skip_order


def get_new_tick(price, time):
    tick = {}
    tick['price'] = float(price)
    tick['time'] = time

    return tick


def get_proboi_id(block, condition):
    return block['alg_number'] + '_' + condition['number'] + '_' + condition['name']


stat = get_new_statistics()


# ---------- conditions -----------------

def check_candle_direction(condition, block, candle, order, prev_candle, prev_prev_candle, launch):
    if condition.get('side') == None:
        return False

    condition.setdefault('offset', -1)

    if condition['offset'] == -1:
        cond_candle = prev_candle
    elif condition['offset'] == -2:
        cond_candle = prev_prev_candle
    else:
        return False

    if cond_candle == None or cond_candle.get('open') == None or cond_candle.get('close') == None:
        return False

    result = False

    if cond_candle['close'] >= cond_candle['open'] and condition['side'] == 'buy':
        result = candle['price']
    elif cond_candle['close'] < cond_candle['open'] and condition['side'] == 'sell':
        result = candle['price']

    if result != False:
        log_condition(candle['time'],
                      "candle_direction(" + "candle = " + str(condition['offset']) + ", side = " + condition[
                          'side'] + ", price=" + str(candle['price']) + ")")

    return result


def check_abs(condition, block, candle, order, prev_candle, prev_prev_candle, launch):
    if condition.get('parameter') == None or condition.get('operation') == None:
        return False

    condition.setdefault('offset', -1)

    abs = order['abs'].setdefault(str(block['number']), {})
    if abs == {}:
        if condition['offset'] == -1:
            cond_candle = prev_candle
        elif condition['offset'] == -2:
            cond_candle = prev_prev_candle
        else:
            return False

        parameter = cond_candle.get(condition['parameter'])
        if parameter == None:
            return False

        abs['value'] = parameter
        abs['operation'] = condition['operation']
        abs['cur_time_frame'] = cur_time_frame['start']

        return False

    if abs['cur_time_frame'] == cur_time_frame['start']:
        return False

    left_value = candle['price']
    right_value = abs['value']

    result = False
    if abs['operation'] == '>=' and left_value >= right_value:
        result = right_value
    elif abs['operation'] == '<=' and left_value <= right_value:
        result = right_value
    elif abs['operation'] == '=' and left_value == right_value:
        result = right_value
    elif abs['operation'] == '>' and left_value > right_value:
        result = right_value
    elif abs['operation'] == '<' and left_value < right_value:
        result = right_value

    if result != False:
        log_condition(candle['time'], "abs(" + "candle = " + str(condition['offset']) + ", parameter = " + condition[
            'parameter'] + ", operation = " + condition['operation'] + ", abs_value = " + str(abs['value'])
                      + "), time=" + str(candle['time']) + ", price=" + str(candle['price']))

    return result


def check_value_change(condition, block, candle, order, prev_candle, prev_prev_candle, launch):
    if prev_candle == {}:
        return False

    if prev_prev_candle == {}:
        return False

    indicator = prev_candle.get(condition['name'])
    if indicator == None:
        return False

    last_ind = prev_prev_candle.get(condition['name'])
    if last_ind == None:
        return False

    if condition.get('value') != None:
        ind_oper = condition['value'].split(' ')[0]
        ind_value = float(condition['value'].split(' ')[1])
    else:
        ind_oper = ''
        ind_value = 0

    if condition.get('change'):
        change = condition['change']
    else:
        change = ''

    change_check = False

    if change == 'more':
        if indicator > last_ind:
            change_check = True
    elif change == 'less':
        if indicator < last_ind:
            change_check = True
    elif change == '':
        change_check = True

    if change_check == True:
        if ind_oper == '>=':
            if indicator >= ind_value:
                return True
        elif ind_oper == '<=':
            if indicator <= ind_value:
                return True
        elif ind_oper == '<':
            if indicator < ind_value:
                return True
        elif ind_oper == '>':
            if indicator > ind_value:
                return True
        elif ind_oper == '=':
            if indicator == ind_value:
                return True
        elif ind_oper == '':
            return True
        else:
            return True

    return False


def check_exit_price_by_step(condition, block, candle, order, prev_candle):
    pid = get_proboi_id(block, condition)

    side = condition['side']
    check = condition['check']

    try:
        # если уже было открытие в данной свече
        if launch.get('was_open') != None and launch['was_open'] == True:
            return False
        # если уже было закрытие в данной свече
        if launch.get('was_close') != None and launch['was_close'] == True:
            return False
        if check == 'low':
            if float(candle['price']) < float(order['proboi'].get(pid)['proboi']):
                proc = (float(order['proboi'].get(pid)['proboi']) - float(candle['price'])) / (
                            float(order['proboi'].get(pid)['proboi']) / 100)
                print('time=' + str(candle['time']) + ',side=' + str(side) + ', check=' + str(check) + ',proboi=' + str(
                    order['proboi'].get(pid)['proboi']) + ', name=' + str(condition['name']))
                return proc
        if check == 'close':
            if side == 'high':
                if float(candle['close']) > float(order['proboi'].get(pid)['proboi']):
                    proc = (float(candle['close']) - float(order['proboi'].get(pid)['proboi'])) / (
                                float(order['proboi'].get(pid)['proboi']) / 100)
                    print('time=' + str(candle['time']) + ',side=' + str(side) + ', check=' + str(
                        check) + ', close=' + str(candle['close']) + ',proboi=' + str(
                        order['proboi'].get(pid)['proboi']) + ', name=' + str(condition['name']))
                    return proc
            if side == 'low':
                if float(order['proboi'].get(pid)['proboi']) > float(candle['close']):
                    proc = (float(order['proboi'].get(pid)['proboi']) - float(candle['close'])) / (
                                float(order['proboi'].get(pid)['proboi']) / 100)
                    print('time=' + str(candle['time']) + ',side=' + str(side) + ', check=' + str(
                        check) + ', close=' + str(candle['close']) + ',proboi=' + str(
                        order['proboi'].get(pid)['proboi']) + ', name=' + str(condition['name']))
                    return proc
        if check == 'high':
            if float(candle['price']) > float(order['proboi'][pid]['proboi']):
                proc = (float(candle['price']) - float(order['proboi'][pid]['proboi'])) / (
                            float(order['proboi'][pid]['proboi']) / 100)
                print('time=' + str(candle['time']) + ',side=' + str(side) + ', check=' + str(check) + ',proboi=' + str(
                    order['proboi'].get(pid)['proboi']) + ', name=' + str(condition['name']))
                return proc
    except:
        return False

    return False


def check_exit_price_by_steps(condition, block, candle, order, prev_candle):
    pid = get_proboi_id(block, condition)

    order['proboi'].setdefault(pid, {})

    order['proboi'].get(pid).setdefault('status', 0)
    order['proboi'].get(pid).setdefault('step', 0)
    order['proboi'].get(pid).setdefault('exit_price_price', False)
    order['proboi'].get(pid).setdefault('proboi', 0)
    order['proboi'].get(pid).setdefault('old_proboi', 0)
    order['proboi'].get(pid).setdefault('line_proc', 0)

    condition.setdefault('min_breakouts', 1)

    side = condition['side']
    check = condition['check']

    exit_price_percent = float(condition.setdefault('exit_price_percent', 0))

    if prev_candle != None:
        old_proboi = order['proboi'].get(pid)['proboi']

        level_name = condition['name'] + '-' + condition['side']
        proboi = prev_candle.get(level_name)
        if proboi == None:
            proboi = 0
        proboi = float(proboi)
        order['proboi'].get(pid)['proboi'] = proboi
        if order['proboi'].get(pid)['status'] == 0:
            order['proboi'].get(pid)['old_proboi'] = order['proboi'].get(pid)['proboi']
    else:
        order['proboi'].get(pid)['proboi'] = 0
        old_proboi = 0

    if order['proboi'].get(pid)['proboi'] == 0:
        return False

    if condition.get('min_breakouts') == None:
        min_breakouts = 1
    else:
        min_breakouts = int(condition['min_breakouts'])

    if order['proboi'].get(pid)['status'] != 0 and side == 'high' and order['proboi'].get(pid)['proboi'] < old_proboi:
        order['proboi'][pid] = {}
        return False
    if order['proboi'].get(pid)['status'] != 0 and side == 'low' and order['proboi'].get(pid)['proboi'] > old_proboi:
        order['proboi'][pid] = {}
        return False

    func_result = False
    price = 0
    result = check_exit_price_by_step(condition, block, candle, order, prev_candle)
    if result:

        order['proboi'].get(pid)['status'] = 1
        order['proboi'].get(pid)['line_proc'] = result + order['proboi'].get(pid)['line_proc']

        order['proboi'].get(pid)['step'] = order['proboi'].get(pid)['step'] + 1
        if order['proboi'].get(pid)['step'] >= min_breakouts and order['proboi'].get(pid)[
            'line_proc'] >= exit_price_percent:
            if order['open_time_position'] != 0:
                if check == 'low':
                    price = float(order['proboi'].get(pid)['proboi']) - (
                                (float(order['proboi'].get(pid)['proboi']) / 100) * exit_price_percent)
                    if candle['price'] <= price:
                        order['close_price_position'] = custom_round(price)
                        func_result = True
                if check == 'close':
                    price = float(candle['close'])
                    order['close_price_position'] = custom_round(price)
                    func_result = True
                if check == 'high':
                    price = float(order['proboi'].get(pid)['proboi']) + (
                                (float(order['proboi'].get(pid)['proboi']) / 100) * exit_price_percent)
                    if candle['price'] >= price:
                        order['close_price_position'] = custom_round(price)
                        func_result = True
            if order['open_time_position'] == 0:
                if check == 'low':
                    price = float(order['proboi'].get(pid)['proboi']) - (
                                (float(order['proboi'].get(pid)['proboi']) / 100) * exit_price_percent)
                    if candle['price'] <= price:
                        order['open_price_position'] = custom_round(price)
                        func_result = True
                if check == 'close':
                    price = float(candle['close'])
                    order['open_price_position'] = custom_round(price)
                    func_result = True
                if check == 'high':
                    price = float(order['proboi'].get(pid)['proboi']) + (
                                (float(order['proboi'].get(pid)['proboi']) / 100) * exit_price_percent)
                    if candle['price'] >= price:
                        order['open_price_position'] = custom_round(price)
                        func_result = True
            if func_result:
                order['proboi'][pid] = {}
                return price
            else:
                return False
        else:
            return False

    return False


def check_trailing(condition, block, candle, order, launch):
    direction = order['direction']

    back_percent = float(condition['back_percent'])

    result = False

    trailing = order['trailings'].setdefault(str(block['number']), {})

    trailing.setdefault('price', 0)
    trailing.setdefault('max_price', 0)
    trailing.setdefault('min_price', 0)

    if condition.get("type_trailing") == "one_candle":
        start = launch['cur_candle']['open']
    else:
        start = order['open_price_position']

    price_change = True
    if direction == 'long' and (candle['price'] > trailing['max_price'] or trailing['max_price'] == 0):
        trailing['price'] = candle['price'] - (candle['price'] - start) * back_percent / 100
        trailing['max_price'] = candle['price']
    elif direction == 'short' and (candle['price'] < trailing['min_price'] or trailing['min_price'] == 0):
        trailing['price'] = candle['price'] + (start - candle['price']) * back_percent / 100
        trailing['min_price'] = candle['price']
    else:
        price_change = False

    if price_change:
        print("trailing_price(change)=" + str(trailing['price']) + ", time = " + str(candle['time']) + ", price=" + str(
            candle['price']) + ", open_price=" + str(order['open_price_position']))

    if trailing['price'] != 0:
        if direction == 'long' and candle['price'] <= trailing['price']:
            result = trailing['price']
        elif direction == 'short' and candle['price'] >= trailing['price']:
            result = trailing['price']

    if result != False:
        print("trailing_price(finish)=" + str(result) + ", time = " + str(candle['time']) + ", price=" + str(
            candle['price']))

    return result


def check_candle(condition, block, candle, order, launch):
    value = condition.get("value")
    if value == None:
        return False

    par_name = "candle" + "_" + value + "_" + str(block['number'])
    if order['cache_conditions'].get(par_name) == None:
        order['cache_conditions'][par_name] = cur_time_frame['start']

    result = order['cache_conditions'][par_name] != cur_time_frame['start']
    if result:
        log_condition(candle['time'], condition['type'] + " (" + condition["value"] + ")")

    return result


def check_reject(condition, block, candle, order, prev_candle, prev_prev_candle, launch):
    if prev_candle == {}:
        return False

    side = condition["side"]
    name = condition["name"] + "-" + condition["side"]
    candle_count = condition["candle"]

    if prev_candle.get(name) == None:
        return False

    reject = order['reject'].setdefault(name + '_' + str(block['number']), {})
    if reject == {}:
        result_side = prev_candle['close'] == prev_candle.get(name)
        if result_side == True:
            reject['side'] = side
            reject['candle_count'] = candle_count
            reject['cur_time_frame'] = cur_time_frame['start']
            log_condition(candle['time'], "reject(start)")
        return False

    if reject['cur_time_frame'] == cur_time_frame['start']:
        return False

    next_result_side = ((side == 'high' and prev_candle['open'] > prev_candle['close'])
                        or (side == 'low' and prev_candle['open'] < prev_candle['close']))

    if next_result_side == False:
        reject.clear()
        return False

    reject['candle_count'] = reject['candle_count'] - 1
    reject['cur_time_frame'] = cur_time_frame['start']

    result = reject['candle_count'] == 0
    if result:
        log_condition(candle['time'], "reject(finish)")

    return result


def check_percent(condition, block, candle, order, prev_candle, prev_prev_candle, launch):
    if prev_candle == {}:
        return False

    condition.setdefault('offset_1', -1)
    condition.setdefault('offset_2', -1)

    if condition['offset_2'] == -2 and prev_prev_candle == None:
        return False

    if condition.get('value') == None:
        return False

    if condition['offset_1'] == -1:
        source_candle_1 = prev_candle
    elif condition['offset_1'] == -2:
        source_candle_1 = prev_prev_candle
    elif condition['offset_1'] == -3:
        if prev_prev_candle == {}:
            return False
        else:
            res = db_get_candle(prev_prev_candle['id'] - 1, prev_prev_candle)
            if res == False:
                return False
            else:
                source_candle_1 = res
    else:
        return False

    if condition['offset_2'] == -1:
        source_candle_2 = prev_candle
    elif condition['offset_2'] == -2:
        source_candle_2 = prev_prev_candle
    elif condition['offset_2'] == -3:
        if prev_prev_candle == {}:
            return False
        else:
            res = db_get_candle(prev_prev_candle['id'] - 1, prev_prev_candle)
            if res == False:
                return False
            else:
                source_candle_2 = res
    else:
        return False

    param_1 = source_candle_1.get(condition['param_1'])
    if param_1 == None:
        return False
    param_1 = float(param_1)


    param_2 = source_candle_2.get(condition['param_2'])
    if param_2 == None:
        return False
    param_2 = float(param_2)

    operator = condition['value'].split(' ')[0]
    percent = float(condition['value'].split(' ')[1])

    percent_fact = ((param_1 - param_2) / param_1) * 100

    result = False

    if operator == '>=':
        if percent_fact >= percent:
            result = True
    elif operator == '<=':
        if percent_fact <= percent:
            result = True
    elif operator == '<':
        if percent_fact < percent:
            result = True
    elif operator == '>':
        if percent_fact > percent:
            result = True
    elif operator == '=':
        if percent_fact == percent:
            result = True
    elif operator == '':
        result = True
    else:
        result = False

    if result == True:
        log_condition(candle['time'], "check_percent: " + str(condition))

    return result


# ---------- engine -----------------

def get_leverage(order, action, stat):
    leverage_start = action.get('leverage_start')
    if leverage_start == None:
        return float(action.get('leverage', 1))

    leverage_max = action.get('leverage_max')
    if leverage_max == None:
        return float(action.get('leverage', 1))

    order['leverage_start'] = float(leverage_start)

    if stat['losses_money'] >= 0:
        return float(leverage_start)

    leverage_max = float(action.get('leverage_max'))
    leverage_take_price_percent = float(action.get('leverage_take_price_percent', '1'))

    leverage_take_money = order['open_price_position'] / 100 * float(leverage_take_price_percent) * float(
        leverage_start)

    leverage_compensation = (-stat['losses_money'] + float(leverage_take_money)) / float(leverage_take_money) * float(
        leverage_start)

    if leverage_max != None and leverage_max < leverage_compensation:
        leverage_compensation = leverage_max

    return leverage_compensation


def set_block_data(table_row, alg_number, col_number, col_conditions_a, col_activations):
    c_a = ast.literal_eval(table_row[col_conditions_a])
    if c_a.get('conditions') == None:
        conditions = []
    else:
        conditions = c_a['conditions']

    if c_a.get('actions') == None:
        actions = []
    else:
        actions = c_a['actions']

    block_data = {}
    block_data['conditions'] = conditions
    block_data['actions'] = actions
    block_data['number'] = table_row[col_number]
    block_data['activations'] = table_row[col_activations]
    block_data['alg_number'] = alg_number
    return block_data


def get_activation_blocks(action_block, algorithm_data):
    blocks_data = algorithm_data['blocks_data']
    block_order = algorithm_data['block_order']

    blocks = []
    activation_blocks = []

    if action_block != '0':
        activations = action_block['activations'].split(',')
        for activtation in activations:
            if activtation == '0':
                continue
            else:
                if activtation == '':
                    action_block = '0'
                else:
                    activation_block = {}
                    activation_block['id'] = activtation.split('_')[0]
                    activation_block['direction'] = activtation.split('_')[1]
                    activation_blocks.append(activation_block)

    if action_block == '0':
        for block in blocks_data:
            if '0' in block[5].split(','):
                block_data = set_block_data(block, '1', 0, 4, 5)
                blocks.append(block_data)
            if '0' in block[7].split(','):
                block_data = set_block_data(block, '2', 0, 6, 7)
                blocks.append(block_data)
    else:
        for activation_block in activation_blocks:

            index = block_order[activation_block['id']]

            if activation_block['direction'] == '1':
                block_data = set_block_data(blocks_data[index], '1', 0, 4, 5)
                blocks.append(block_data)
            else:
                block_data = set_block_data(blocks_data[index], '2', 0, 6, 7)
                blocks.append(block_data)

    return blocks


def check_blocks_condition(candle, order, prev_candle, prev_prev_candle, launch, stream):
    blocks = stream['activation_blocks']
    for block in blocks:
        stream['cur_conditions_group'].setdefault(str(block['number']), [])
        print(f"{block['number']=} {block_conditions_done(block, candle, order, prev_candle, prev_prev_candle, launch, stream)} {block=}")
        print(f"{candle=}")

        if block_conditions_done(block, candle, order, prev_candle, prev_prev_candle, launch, stream):
            stream['cur_conditions_group'].clear()
            return block

    return None


def set_done_conditions_group(conditions_group):
    for condition in conditions_group:
        condition['done'] = True


def undone_conditions_group(conditions_group):
    for condition in conditions_group:
        condition['done'] = False
        condition['id_candle'] = None


def block_conditions_done(block, candle, order, prev_candle, prev_prev_candle, launch, stream):
    if stream['was_close']:
        return False

    cur_condition_number = None
    cond_done_id_candle = None
    stream['prices'] = []

    cur_conditions_group = stream['cur_conditions_group'][str(block['number'])]

    # если изменилась свеча для текущего намбера, то обнуляем
    if len(cur_conditions_group) > 0 and cur_conditions_group[0]['id_candle'] != launch['id_candle']:
        undone_conditions_group(cur_conditions_group)
        stream['cur_conditions_group'][str(block['number'])] = []

    for condition in block['conditions']:

        print(f"{launch['cur_id']=} {candle=}")
        if candle['id'] == launch['cur_id']:
            if launch['mode'] == 'robot':
                time.sleep(1)
                pass

            return False


        condition.setdefault('done', False)

        condition.setdefault('number', "1")

        # пропускаем те условия, которые отработали в данной свече
        if condition.get('id_candle') != None and condition['id_candle'] == launch['id_candle']:
            continue

        # если изменился намбер в цикле, то возвращаем False
        if cur_condition_number != None and condition['number'] != cur_condition_number:
            set_done_conditions_group(cur_conditions_group)
            stream['cur_conditions_group'][str(block['number'])] = []
            return False

        # если условие выполнилось, то продолжаем
        if condition['done'] == True:
            continue

        if cond_done_id_candle != None and cond_done_id_candle == launch[
            'id_candle']:  # ждем пока не появится новая свеча, чтобы проверить группу с новым намбером
            return False

        if condition['type'] == 'pnl':
            result = conditions.check_pnl(condition, block, candle, order)
            if result == False:
                return False
            else:
                stream['prices'].append(result)
                order['close_time_order'] = candle['time']
                order['last_condition_type'] = 'realtime'
        elif condition['type'] == 'value_change':
            result = check_value_change(condition, block, candle, order, prev_candle, prev_prev_candle, launch)
            if result == False:
                return False
            else:
                order['condition_checked_candle'] = prev_candle
                order['last_condition_type'] = 'history'
                order['close_time_order'] = 0
        elif condition['type'] == 'check_percent':
            print(f"{condition['type']=}")
            result = check_percent(condition, block, candle, order, prev_candle, prev_prev_candle, launch)
            print(f"{result=}")
            if result == False:
                return False
            else:
                stream['prices'].append(result)
                order['close_time_order'] = candle['time']
                order['last_condition_type'] = 'realtime'
        elif condition['type'] == 'price':
            result = conditions.check_price(condition, block, candle, order, launch, stream, cursor, prev_candle)
            if result == False:
                return False
            else:
                stream['prices'].append(result)
                order['close_time_order'] = candle['time']
                order['last_condition_type'] = 'realtime'
        elif condition['type'] == 'trailing':
            result = check_trailing(condition, block, candle, order, launch)
            if result == False:
                return False
            else:
                stream['prices'].append(result)
                order['close_time_order'] = candle['time']
                order['last_condition_type'] = 'realtime'
        elif condition['type'] == 'abs':
            result = check_abs(condition, block, candle, order, prev_candle, prev_prev_candle, launch)
            if result == False:
                return False
            else:
                stream['prices'].append(result)
                order['close_time_order'] = candle['time']
                order['last_condition_type'] = 'realtime'
        elif condition['type'] == 'candle_direction':
            result = check_candle_direction(condition, block, candle, order, prev_candle, prev_prev_candle, launch)
            if result == False:
                return False
            else:
                stream['prices'].append(result)
                order['close_time_order'] = candle['time']
                order['last_condition_type'] = 'realtime'
        elif condition['type'] == 'exit_price':
            check = condition['check']
            if check == 'high' or check == 'low':
                result = check_exit_price_by_steps(condition, block, candle, order, prev_candle)
                if result != False:
                    order['last_condition_type'] = 'realtime'
            else:
                result = check_exit_price_by_steps(condition, block, prev_candle, order, prev_prev_candle)
                if result != False:
                    order['last_condition_type'] = 'history'
            if result == False:
                return False
            else:
                stream['prices'].append(result)
                order['close_time_order'] = candle['time']
        elif condition['type'] == 'candle':
            result = check_candle(condition, block, candle, order, launch)
            if result == False:
                return False
            else:
                order['condition_checked_candle'] = prev_candle
                order['close_time_order'] = 0
                order['last_condition_type'] = 'history'
        elif condition['type'] == 'reject':
            result = check_reject(condition, block, candle, order, prev_candle, prev_prev_candle, launch)
            if result == False:
                return False
            else:
                order['condition_checked_candle'] = prev_candle
                order['close_time_order'] = 0
                order['last_condition_type'] = 'history'
        else:
            return False



        # если условие выполнилось
        print(f" {launch['id_candle']=}")
        print(f"{cur_condition_number=} {condition['number']=}")
        condition['id_candle'] = launch['id_candle']
        cur_condition_number = condition['number']

        launch['cur_id'] = candle['id']

        stream['cur_conditions_group'][str(block['number'])].append(condition)

        if order['condition_checked_candle'] == None:
            order['condition_checked_candle'] = candle

    set_done_conditions_group(cur_conditions_group)
    stream['cur_conditions_group'][str(block['number'])] = []




    if len(stream['prices']) > 0:
        if candle['price'] < launch['last_price']:
            stream['price'] = min(stream['prices'])
        else:
            stream['price'] = max(stream['prices'])
    else:
        stream['price'] = 0
    if launch['mode'] == 'robot':
        time.sleep(1) #задержка времени для проверки условия
    return True


def execute_block_actions(candle, order, stat, launch, stream):
    block = stream['action_block']

    saved_close_time = 0
    saved_close_price = 0

    was_close = False
    print(f"{block=}")
    for action in block['actions']:

        if action.get('done') and action['done'] == True:
            continue

        if action['order'] == "close":
            if action['direction'] != order['direction']:
                return False
            if order['close_time_order'] == 0:
                order['close_time_order'] = candle['time']
            result = close_position(order, block, candle, stat, action, launch, stream)
            if result:
                action['done'] = True
                print('Закрытие позиции: ' + str(stat['percent_position']) + ', ' + str(order['close_time_position']))
                print('-------------------------------------------------------')
                saved_close_time = order['close_time_order']
                saved_close_price = order['close_price_position']
                order = get_new_order(order, launch)
                if order['last_condition_type'] == 'realtime':
                    was_close = True
                if launch['trading_status'] == 'on':
                    continue
                elif launch['trading_status'] == 'off_after_close':
                    return None
            else:
                action['done'] = False
                return False
        elif action['order'] == "open":
            if order['state'] == 'start':

                # если уже было закрытие в данной свече
                if stream.get('was_close') != None and stream['was_close'] == True:
                    return False

                order['order_type'] = action['order_type']
                order['direction'] = action['direction']
                if saved_close_time == 0:
                    order['open_time_order'] = candle['time']
                else:
                    order['open_time_order'] = saved_close_time
                if saved_close_price != 0:
                    order['open_price_position'] = custom_round(saved_close_price)
                order['state'] = 'order_is_opened'
            if order['state'] == 'order_is_opened':
                result = open_position(order, block, candle, stat, action, prev_candle, launch, stream)
                if result:
                    action['done'] = True
                    print('Открытие позиции: ' + order['direction'] + ', ' + str(order['leverage']) + ', ' + str(
                        order['open_time_position']))
                    stream['was_open'] = True
                else:
                    action['done'] = False
                    return False
            else:
                return False
        elif action['order'] == "open_many" and launch['traiding_mode'] == 'many':
            order['order_type'] = action['order_type']
            order['direction'] = action['direction']
            order['path'] = str(block['number']) + '_' + block['alg_number']
            result = many.open_position_many(order, block, candle, stat, action, stream, launch, cn_pos, cursor,
                                             prev_candle)
            if result == False:
                return False
        elif action['order'] == "update_many" and launch['traiding_mode'] == 'many':
            order['order_type'] = action['order_type']
            if action.get('stream_target') == None or action['stream_target'] == stream['id']:
                order['direction'] = action['direction']
            order['path'] = str(block['number']) + '_' + block['alg_number']
            result = many.update_position_many(order, block, candle, stat, action, stream, launch, cursor, cn_pos,
                                               prev_candle)
            if result == False:
                return False
            if order['last_condition_type'] == 'realtime':
                was_close = True
        elif action['order'] == "balance_many" and launch['traiding_mode'] == 'many':
            order['order_type'] = action['order_type']
            order['direction'] = action['direction']
            result = many.balance_position_many(launch, block, candle, stat, action, cn_pos, cursor, prev_candle)
            if result == False:
                return False
            if order['last_condition_type'] == 'realtime':
                was_close = True
            if result == None:
                stream['was_close'] = was_close and order['open_time_position'] == 0
                return None

    stream['was_close'] = was_close and order['open_time_position'] == 0

    return True


def change_activation_block(action_block, stream):
    # stream['activation_blocks'] = get_activation_blocks(stream['action_block'], stream['algorithm_data'])
    stream['activation_blocks'] = get_activation_blocks(action_block, stream['algorithm_data'])
    stream['strategy_state'] = 'check_blocks_conditions'
    db_save_state(launch, stat)


def open_position(order, block, candle, stat, action, prev_candle, launch, stream):
    result = False

    if order['direction'] == 'long':
        order['open_time_position'] = order['open_time_order']
        result = True
    elif order['direction'] == 'short':
        order['open_time_position'] = order['open_time_order']
        result = True

    if result == True:
        price_old = prev_candle['close']
        if order['direction'] == 'long':
            price = float(price_old) - (float(price_old) / 100) * float(order['price_indent'])
        elif order['direction'] == 'short':
            price = float(price_old) + (float(price_old) / 100) * float(order['price_indent'])
        if stream.get('price') != None and stream['price'] != 0:
            order['open_price_position'] = custom_round(stream['price'])
        if order['open_price_position'] == 0:
            order['open_price_position'] = custom_round(price)
        if order['path'] == '':
            pr_str = ''
        else:
            pr_str = ','
        order['path'] = order['path'] + pr_str + str(block['number']) + '_' + block['alg_number']
        order['leverage'] = round(get_leverage(order, action, stat), 2)
        if launch['mode'] == 'robot':
            db_open_position(order)

    stream['price'] = 0

    return result


def close_position(order, block, candle, stat, action, launch, stream):
    points_position = 0

    if ((order['direction'] == 'long') or
            (order['direction'] == 'short')):

        # если уже было закрытие в данной свече
        if stream.get('was_close') != None and stream['was_close'] == True:
            order['close_time_order'] = 0
            return False

        if type(block) == str:
            order['path'] = order['path'] + ', ' + block
        else:
            order['path'] = order['path'] + ', ' + str(block['number']) + '_' + block['alg_number']

        if stream.get('price') != None and stream['price'] != 0:
            order['close_price_position'] = custom_round(stream['price'])
        stream['price'] = 0

        if order['close_price_position'] == 0:
            if order['condition_checked_candle'] == None:
                order['close_price_position'] = custom_round(float(candle['close']))
            else:
                order['close_price_position'] = custom_round(float(order['condition_checked_candle']['close']))
        if order['direction'] == 'long':
            if order['close_price_position'] >= order['open_price_position']:
                result_position = 'profit'
                stat['profit_sum'] = stat['profit_sum'] + 1
            else:
                result_position = 'loss'
                stat['loss_sum'] = stat['loss_sum'] + 1
            points_position = order['close_price_position'] - order['open_price_position']
        else:
            if order['open_price_position'] >= order['close_price_position']:
                result_position = 'profit'
                stat['profit_sum'] = stat['profit_sum'] + 1
            else:
                result_position = 'loss'
                stat['loss_sum'] = stat['loss_sum'] + 1
            points_position = order['open_price_position'] - order['close_price_position']

        rpl = points_position * float(order['leverage'])
        if order.get('leverage_start') != None and order['leverage'] > order['leverage_start'] and points_position >= 0:
            rpl_comp = (points_position * float(order['leverage'])) - (points_position * float(order['leverage_start']))
        else:
            rpl_comp = rpl

        if result_position == 'profit':
            stat['profit_points'] = stat['profit_points'] + points_position
            if stat['losses_money'] < 0:
                stat['losses_money'] = stat['losses_money'] + rpl_comp
        elif result_position == 'loss':
            stat['loss_points'] = stat['loss_points'] + points_position
            stat['losses_money'] = stat['losses_money'] + rpl_comp

        if stat['losses_money'] > 0: stat['losses_money'] = 0

        stat['percent_position'] = (points_position / order['open_price_position']) * 100 * float(
            order['leverage']) - 2 * spread * float(order['leverage'])

        stat['percent_positions'] = stat['percent_positions'] + stat['percent_position']

        price_perecent = points_position / order['open_price_position'] * 100

        if result_position == 'profit':
            if stat['percent_series'] <= 0:
                stat['percent_series'] = stat['percent_position']
            else:
                stat['percent_series'] = stat['percent_series'] + stat['percent_position']
        elif result_position == 'loss':
            if stat['percent_series'] >= 0:
                stat['percent_series'] = stat['percent_position']
            else:
                stat['percent_series'] = stat['percent_series'] + stat['percent_position']

        order['close_time_position'] = order['close_time_order']

        if order['open_time_position'].month == stat['cur_month']:
            stat['month_percent'] = stat['month_percent'] + stat['percent_position']
        else:
            stat['month_percent'] = stat['percent_position']
            stat['cur_month'] = order['open_time_position'].month
            stat['max_month_percent'] = 0
            stat['rollback_month_percent'] = 0

        if stat['month_percent'] - stat['max_month_percent'] < stat['rollback_month_percent']:
            stat['rollback_month_percent'] = stat['month_percent'] - stat['max_month_percent']

        if stat['month_percent'] > stat['max_month_percent']:
            stat['max_month_percent'] = stat['month_percent']

        if launch['mode'] == 'robot':
            db_close_position(order, result_position, points_position, rpl, price_perecent, stat)
        else:
            db_insert_position(order, result_position, points_position, rpl, price_perecent, stat)

        stat['last_percent_position'] = stat['percent_position']

        return True

    return False


# ---------- telegram ----------------------

def send_open_position_telegram(launch, order):
    if launch['mode'] != 'robot':
        return

    global cn_db
    global cursor_db

    text = ''
    try:
        query = "select id, leverage from {0} where id_position = '{1}'".format(table_result, order['uuid'])
        cursor_db.execute(query)
        for (id, leverage) in cursor_db:
            text = ('*ID ' + str(id) + "*" +
                    "\n" + order['direction'] + " open" +
                    "\n" + str(order['open_price_position']) +
                    "\n" + "L " + str(leverage))

        if text != '':
            send_telegram(launch, text)
    except Exception as e:
        print(e)


def send_close_position_telegram(launch, order):
    if launch['mode'] != 'robot':
        return

    global cn_db
    global cursor_db

    text = ''
    try:
        query = "select id, percent_position, month_percent, leverage from {0} where id_position = '{1}'".format(
            table_result, order['uuid'])
        cursor_db.execute(query)
        for (id, percent_position, month_percent, leverage) in cursor_db:
            text = ('*ID ' + str(id) + "*"
                                       "\n" + order['direction'] + " close" +
                    "\n" + str(order['close_price_position']) +
                    "\n" + "R " + str(round(percent_position, 2)) + "%" +
                    "\n" + "*L " + str(leverage) + "*"
                                                   "\n" + "*M " + str(round(month_percent, 2)) + "%" + "*"
                    )
        if text != '':
            send_telegram(launch, text)
    except Exception as e:
        print(e)


def send_telegram(launch, text):
    token = launch['telegram_metadata']['token']  # ключ тг бота
    url = "https://api.telegram.org/bot"
    channel_id = launch['telegram_metadata']['channel_id']
    url += token
    method = url + "/sendMessage"

    requests.post(method, data={
        "chat_id": channel_id,
        "text": text,
        "parse_mode": "Markdown"})


# ---------- database ----------------------

def db_open_position(order):
    global cn_db
    global cursor_db
    print(f"db_open_position {table_result}")

    try:
        insert_stmt = (
            "INSERT INTO {0}(id_position, side, open_type_order, open_time_order, open_price_position, open_time_position, leverage, blocks_id)"
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)".format(table_result)
        )
        data = (
            order['uuid'], order['direction'], order['order_type'], order['open_time_order'],
            order['open_price_position'], order['open_time_position'], order['leverage'], order['path'])

        cursor_db.execute(insert_stmt, data)
        cn_db.commit()
        send_open_position_telegram(launch, order)
    except Exception as e:
        print(e)
        cn_db = get_db_connection(launch['db']['user'], launch['db']['password'], launch['db']['host'],
                                  launch['db']['database'])
        cursor_db = cn_db.cursor()
        db_open_position(order)


def db_close_position(order, result_position, points_position, rpl, price_perecent, stat):
    global cn_db
    global cursor_db
    print(f"db_close_position {table_result}")

    try:
        insert_stmt = (
            "UPDATE {0} SET close_order_type = %s, close_time_order = %s, close_price_position = %s, close_time_position = %s, result_position = %s, points_position = %s, percent_position = %s, percent_series = %s, percent_price_deviation = %s, blocks_id = %s, percent_positions = %s, rpl = %s, losses_money = %s, price_perecent = %s, month_percent = %s, rollback_month_percent = %s"
            " where id_position = %s".format(table_result)
        )
        data = (
            order['order_type'], order['close_time_order'], order['close_price_position'], order['close_time_position'],
            result_position, points_position,
            stat['percent_position'], stat['percent_series'], 0, order['path'], stat['percent_positions'], rpl,
            stat['losses_money'], price_perecent,
            stat['month_percent'], stat['rollback_month_percent'], order['uuid'])
        cursor_db.execute(insert_stmt, data)
        cn_db.commit()
        send_close_position_telegram(launch, order)
    except Exception as e:
        print('db_close_position')
        print(e)
        cn_db = get_db_connection(launch['db']['user'], launch['db']['password'], launch['db']['host'],
                                  launch['db']['database'])
        cursor_db = cn_db.cursor()
        db_close_position(order, result_position, points_position, rpl, price_perecent)


def db_insert_position(order, result_position, points_position, rpl, price_perecent, stat):
    global cn_pos
    cursor_local = cn_pos.cursor()
    print(f"db_insert_position {table_result}")

    try:
        insert_stmt = (
            "INSERT INTO {0}(id_position, side, open_type_order, open_time_order, open_price_position, open_time_position, leverage, blocks_id, month_percent,"
            "close_order_type, close_time_order, close_price_position, close_time_position , result_position , points_position , percent_position , percent_series , percent_price_deviation , percent_positions , rpl , losses_money , price_perecent, rollback_month_percent) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(
                table_result)
        )
        data = (
            order['uuid'], order['direction'], order['order_type'], order['open_time_order'],
            order['open_price_position'], order['open_time_position'], order['leverage'], order['path'],
            stat['month_percent'],
            order['order_type'], order['close_time_order'], order['close_price_position'], order['close_time_position'],
            result_position, points_position,
            stat['percent_position'], stat['percent_series'], 0, stat['percent_positions'], rpl, stat['losses_money'],
            price_perecent, stat['rollback_month_percent'])

        cursor_local.execute(insert_stmt, data)
        cn_pos.commit()
        cursor_local.close()

        send_open_position_telegram(launch, order)
        send_close_position_telegram(launch, order)

    except Exception as e:
        print('db_insert_position')
        print(e)
        cn_pos = get_db_connection(launch['db']['user'], launch['db']['password'], launch['db']['host'],
                                   launch['db']['database'])
        db_insert_position(order, result_position, points_position, rpl, price_perecent, cn_pos)


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, datetime.datetime):
        return obj.isoformat()


def load_with_datetime(pairs, format='%Y-%m-%dT%H:%M:%S'):
    """Load with dates"""
    d = {}
    for k, v in pairs:
        ok = False
        try:
            d[k] = datetime.datetime.strptime(v, format).date()
            ok = True
        except:
            d[k] = v
        if ok == False:
            try:
                d[k] = datetime.datetime.strptime(v, '%Y-%m-%dT%H:%M:%S.%f').date()
            except:
                d[k] = v
    return d


def db_save_state(launch, stat):
    if launch['mode'] != 'robot':
        return False

    global cnx2
    global cursor

    launch_data = json.dumps(launch, default=json_serial)
    stat_data = json.dumps(stat, default=json_serial)

    try:
        update_query = (f"UPDATE {MYSQL_TABLE_CONFIG} SET launch = %s, stat = %s where symbol = '{SYMBOL.upper()}'")
        data = (launch_data, stat_data)
        cursor.execute(update_query, data)
        cnx2.commit()
    except Exception as e:
        print(e)


def db_get_state(launch, stat):
    print(db_get_state)
    if launch['mode'] != 'robot':
        return False

    global cnx2
    global cursor

    launch_data = json.dumps(launch, default=json_serial)
    stat_data = json.dumps(stat, default=json_serial)

    try:
        query = f"SELECT launch, stat, orders FROM {MYSQL_TABLE_CONFIG} WHERE symbol = '{SYMBOL.upper()}'"
        cursor.execute(query)
        for (launch_data, stat_data, orders_data) in cursor:
            print(f"{(launch_data, stat_data, orders_data)=}")
            if launch_data == "null" or stat_data == "null" or launch_data == None or stat_data == None:
                print("!")
                return False

        launch_data = json.loads(launch_data, object_pairs_hook=load_with_datetime)
        stat_data = json.loads(stat_data, object_pairs_hook=load_with_datetime)

        launch.update(launch_data)
        stat.update(stat_data)

        return True

    except Exception as e:
        print(e)
        return False


def db_clear_state():
    try:
        update_query = (f"UPDATE {MYSQL_TABLE_CONFIG} SET launch = %s, stat = %s, order = %s where symbol = '{SYMBOL.upper()}'")
        data = (None, None, None)
        cursor.execute(update_query, data)
        cnx2.commit()
        print("Контекст очищен")
    except Exception as e:
        print(e)


def db_get_candle(id_candle, source_candle):
    print("db_get_candle")
    f_candle = source_candle.copy()

    global cnx2
    cursor = cnx2.cursor(dictionary=True)

    try:
        query = ("SELECT * FROM {0} where id={1}".format(launch['price_table_name'], str(id_candle)))
        cursor.execute(query)
        for row in cursor:
            f_candle.update(row)
            return f_candle

        return False

    except Exception as e:
        print(e)
        return False


# ---------- main programm -----------------

def init_algo(launch):
    print("init_algo")
    many.delete_equity(launch, cursor)

    for stream in launch['streams']:
        db_get_algorithm(stream)
        stream['was_close'] = False
        stream['was_open'] = False
        stream['strategy_state'] = 'check_blocks_conditions'
        stream['action_block'] = None
        stream['activation_blocks'] = get_activation_blocks('0', stream['algorithm_data'])
        if len(stream['activation_blocks']) == 0:
            raise Exception('There is no first block in startegy')

        #if launch['mode'] == 'robot':
        if launch['mode'] != 'robot':
            try:
                cursor.execute(f"TRUNCATE TABLE {SYMBOL}_positions_{stream['id']}")
            except Exception as e:
                print(e)


if db_get_state(launch, stat) != True:
    init_algo(launch)
launch['cur_id'] = 0
while True:  # цикл по тикам

    if launch['mode'] == 'robot':
        try:
            trading_status = get_trading_status()
        except Exception as e:
            print(e)
            continue

        if robot_is_stoped and trading_status == 'on':
            robot_is_stoped = False
            print('Робот запущен')

        robot_must_stop = (trading_status == 'off'
                           or (launch['trading_status'] == "off_after_close" and order['open_time_position'] == 0)
                           or trading_status == 'off_now_close')

        if robot_must_stop and robot_is_stoped == False:
            robot_is_stoped = True
            print('Робот остановлен')
            if trading_status != 'off':
                db_clear_state()
                launch = init_launch()
                init_algo(launch)

        launch['trading_status'] = trading_status

        if robot_is_stoped:
            continue

    try:
        if launch.get('renko') == None:
            set_candle(launch, keys, cursor_candles, candle, prev_candle, prev_prev_candle, stat)
        else:
            set_candle_renko(launch, keys, cursor_candles, candle, prev_candle, prev_prev_candle, next_candle, stat)
    except Exception as e:
        print(e)
        continue

    if candle == {}:
        if launch['mode'] != 'robot':
            break
        else:
            time.sleep(1)
            log_condition(get_cur_time(), "wait tick")
            continue

    for stream in launch['streams']:

        res = many.set_first_position(stream, candle, launch, cn_pos)
        if res == True:
            continue

        order = stream['order']

        while True:  # цикл по блокам

            # проверка условий активных блоков
            if stream['strategy_state'] == 'check_blocks_conditions':
                stream['action_block'] = check_blocks_condition(candle, order, prev_candle, prev_prev_candle, launch,
                                                                stream)

                if stream['action_block'] != None:
                    stream['strategy_state'] = 'execute_block_actions'
                    print(f"{stream=}")
                    # если в блоке нет текущих действий, то активным блоком назначаем следующий
                    if len(stream['action_block']['actions']) == 0:
                        stream['activation_blocks'] = get_activation_blocks(stream['action_block'],
                                                                            stream['algorithm_data'])

                        # назначаем только, если он (блок) один и в нем нет условий
                        if len(stream['activation_blocks']) == 1 and len(
                                stream['activation_blocks'][0]['conditions']) == 0:
                            stream['action_block'] = stream['activation_blocks'][0]
                else:
                    break

            # исполнение действий блока
            if stream['strategy_state'] == 'execute_block_actions':
                result = execute_block_actions(candle, order, stat, launch, stream)
                if result == True:
                    change_activation_block(stream['action_block'], stream)
                else:
                    break

    if candle != {}:
        launch['last_price'] = candle['price']

if cn_db:
    cn_db.close()

if cnx:
    cnx.close()

if cn_pos:
    cn_pos.close()

if cnx2:
    cnx2.close()

if launch['mode'] == 'tester' and cn_tick:
    cn_tick.close()
