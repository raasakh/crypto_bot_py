import time
import requests
import datetime
import robot_db as db
import os


class Equity_1():
    def __init__(self):
        self.start = False

    def first_start(self, balance, leverage, size_order, price_order, size_position, price_position, close):
        self.balance = float(balance)
        self.leverage = float(leverage)
        self.size_order = float(size_order)
        self.price_order = float(price_order)
        self.size_position = float(size_position)
        self.price_position = float(price_position)
        self.close = float(close)
        self.pnl = (self.close - self.price_position) * self.size_position
        self.equity_1 = self.balance + self.pnl
        self.rpl = 0
        self.start = True

    def update(self, leverage, close):
        leverage_0 = self.leverage
        order_leverage = leverage - self.leverage
        self.leverage = leverage
        self.price_order = self.close
        if order_leverage > 0:
            self.size_order = order_leverage * self.equity_1 / self.price_order
            self.price_position += (self.price_order - self.price_position) * (
                        self.size_order / (self.size_position + self.size_order))
        elif order_leverage <= 0 and leverage_0 != 0:
            self.size_order = order_leverage * self.size_position / leverage_0
        else:
            self.size_order = 0
            #return self.balance, self.leverage, self.size_order, self.price_order, self.size_position, self.price_position, \
            #       self.close, self.pnl, self.equity_1, self.rpl
        self.balance += self.rpl
        if self.size_order < 0 and self.size_position != 0:
            self.rpl = self.pnl * (-self.size_order / self.size_position)
        else:
            self.rpl = 0
        self.size_position += self.size_order
        self.close = close

    def update_long(self):
        self.pnl = (self.close - self.price_position) * self.size_position
        self.equity_1 = self.balance + self.pnl

        return self.balance, self.leverage, self.size_order, self.price_order, self.size_position, self.price_position,\
               self.close, self.pnl, self.equity_1, self.rpl

    def update_short(self):
        self.pnl = (self.price_position - self.close) * self.size_position
        self.equity_1 = self.balance + self.pnl

        return self.balance, self.leverage, self.size_order, self.price_order, self.size_position, self.price_position,\
               self.close, self.pnl, self.equity_1, self.rpl

equity_1 = [[Equity_1(), Equity_1()], [Equity_1(), Equity_1()]] #обязательный рефакторинг!!!!

def is_many(launch):

    if launch.get('traiding_mode') == None:
        return False

    if launch['traiding_mode'] == 'many':
        return True
    else:
        return False

def open_position_many(order, block, candle, stat, action, stream, launch, cn_pos, cursor, prev_candle):
    print("open_position_many")
    if action.get('leverage') == None:
        return False

    order['leverage'] = action['leverage']
    #stream['order']['leverage'] = action['leverage']
    many_params_source = get_many_params(stream, cursor, candle, launch, prev_candle)
    order['update_position_price'] = many_params_source['price_position']
    db_insert_position_many(stream, candle, many_params_source, launch, cn_pos)

    log_text = "Открытие many-позиции: time = " + str(candle['time']) + ', price = ' + str(candle['price'])
    if launch['mode'] == 'tester':
        log_text = log_text + ", id = " + str(candle['id'])

    print(log_text)

    send_leverage_many_robot(launch, stream)

    return True

def update_position_many(order, block, candle, stat, action, stream, launch, cursor, cn_pos, prev_candle):
    print("update_position_many")
    leverage_up = action.get('leverage_up')
    leverage_max = action.get('leverage_max')
    leverage_down = action.get('leverage_down')
    leverage_min = action.get('leverage_min')
    leverage_source = action.get('leverage_source')
    leverage_fix = action.get('leverage_fix')
    stream_target = action.get('stream_target')

    try:

        stream_local = None
        if stream_target != None and stream['id'] != stream_target:
            for stream_loop in launch['streams']:
                if stream_loop['id'] == stream_target:
                    stream_local = stream_loop
                    break
        else:
            stream_local = stream

        if stream_local == None:
            return False
 
        stream_source = None
        if leverage_source != None and stream['id'] != leverage_source:
            for stream_loop in launch['streams']:
                if stream_loop['id'] == leverage_source:
                    stream_source = stream_loop
                    break
        else:
            stream_source = stream

        if stream_source == None:
            return False

        many_params_source = get_many_params(stream_local, cursor, candle, launch, prev_candle)

        if leverage_up != None:
            leverage_condition = leverage_up
            act = 'up'
        elif leverage_down != None:
            leverage_condition = leverage_down
            act = 'down'
        elif leverage_fix == True:
            position = candle['price'] * order['equity'] * order['leverage']
            order_fix = stream_local['order']
            if order_fix == None:
                return False
            balance_fix = candle['price'] * order_fix['equity']
            leverage_condition = position/balance_fix
            act = 'fix'

            if stream_local['order']['direction'] == "":
                return False

        else:
            return False    
        print(f"{stream_local['order']['direction']=}")
        #stream_local['order']['leverage'] = get_leverage_action(leverage_condition, many_params_source['leverage'], leverage_min, leverage_max, act, stream_local['order']['leverage'])
        #stream_local['order']['update_position_price'] = many_params_source['price_position']
        stream_local['order']['leverage'] = get_leverage_action(leverage_condition, many_params_source['leverage'], leverage_min, leverage_max, act, stream_local['order']['leverage'])
        many_params_source = get_many_params(stream_local, cursor, candle, launch, prev_candle)
        stream_local['order']['update_position_price'] = many_params_source['price_position']
    except Exception as e:
        print(e)
        return False
    db_insert_position_many(stream_local, candle, many_params_source, launch, cn_pos)
    log_text = "Обновление many-позиции: time = " + str(candle['time']) + ', price = ' + str(candle['price'])
    if launch['mode'] == 'tester':
        log_text = log_text + ", id = " + str(candle['id'])

    print(log_text)

    print('---------------------------------------------')

    send_leverage_many_robot(launch,stream)

    return True

def balance_position_many(launch, block, candle, stat, action, cn_pos, cursor, prev_candle):
    print("balance_position_many")
    total_equity = get_total_equity()
    if total_equity == None:
        return False

    balancing = float(total_equity) / float(len(launch['streams']))

    launch['balancing'] = total_equity

    for stream in launch['streams']:
        stream['order']['equity'] = balancing
        stream['order']['max_equity'] = balancing
        stream['order']['path'] = str(block['number']) + '_' + block['alg_number']
        many_params_source = get_many_params(stream, cursor, candle, launch, prev_candle)
        db_insert_position_many(stream, candle, many_params_source, launch, cn_pos)
        block.change_activation_block('0', stream)

    log_text = "Ребалансировка many-позици: time = " + str(candle['time']) + ', price = ' + str(candle['price'])
    if launch['mode'] == 'tester':
        log_text = log_text + ", id = " + str(candle['id'])

    print(log_text)

    print('---------------------------------------------')

    send_balancing_robot(launch, stream)

    return None #лайфхак

def set_equity(launch, prev_candle, prev_prev_candle, stat, cursor, candle):
    print("set_equity")
    print(f"{prev_candle=} {prev_prev_candle=}")
    if launch['traiding_mode'] != 'many':
        return

    if prev_candle == {} or prev_prev_candle == {}:
        return

    set_query = ""
    total_equity = 0.0
    for stream in launch['streams']:

        order = stream['order']
        last_order = get_many_params(stream, cursor, candle, launch, prev_candle)
        equity = get_equity_many(launch, stream, prev_candle, prev_prev_candle, last_order)
        if equity != None:
            order['equity'] = equity
            if order['equity'] > order['max_equity']:
                order['max_equity'] = order['equity']

        if set_query == '':
            razd = ''
        else:
            razd = ','
        set_query = set_query + razd + "equity_{0}={1}, max_equity_{0}={2}".format(stream['id'], str(stream['order']['equity']), str(stream['order']['max_equity']))
        total_equity = total_equity + stream['order']['equity']
        prev_candle['total_equity'] = total_equity
        prev_candle['equity_{0}'.format(stream['id'])] = stream['order']['equity']
        prev_candle['max_equity_{0}'.format(stream['id'])] = stream['order']['max_equity']
    
    calculate_stat_many(stat, prev_candle['time'], total_equity)

    launch.setdefault('balancing', total_equity)
    prev_candle['balancing'] = launch['balancing']

    insert_stmt = ("UPDATE {0} SET {1}, total_equity=%s, balancing=%s, total_equity_sum_percent=%s, total_equity_month_percent=%s where id=%s".format(launch['price_table_name'], set_query))
    print(f"{set_query=}")
    data = (total_equity, launch['balancing'], 
        stat['many']['total_equity_sum_percent'],
        stat['many']['total_equity_month_percent'],
        prev_candle['id'])

    cursor.execute(insert_stmt, data)

def delete_equity(launch, cursor):
    print("delete_equity")

    if launch['mode'] != 'tester':
        return

    if launch['traiding_mode'] != 'many':
        return

    set_query = ""
    for stream in launch['streams']:
        if set_query == '':
            razd = ''
        else:
            razd = ','
        set_query = set_query + razd + "equity_{0} = NULL, max_equity_{0} = NULL".format(stream['id'])
    
    insert_stmt = ("UPDATE {0} SET {1}, total_equity = NULL, total_equity_month_percent = NULL, total_equity_sum_percent = NULL".format(launch['price_table_name'], set_query))
    cursor.execute(insert_stmt)

def set_first_position(stream, candle, launch, cn_pos):
    print("set_first_position")
    if launch.get('many_metadata') == None:
        return False

    if stream.get('is_set_first_block') != None:
        return False
    
    stream['is_set_first_block'] = True

    many_params_source = {}
    many_params_source['size_order'] = 0
    many_params_source['price_position'] = 0
    many_params_source['size_position'] = 0
    many_params_source['price_order'] = 0
    many_params_source['balance'] = 1
    many_params_source['leverage'] = 0

    db_insert_position_many(stream, candle, many_params_source, launch, cn_pos)

    return True

def get_last_order(stream, cursor, launch):

    order_local = stream['order']

    many_params = {}

    query = (f"SELECT balance, leverage, price_order, size_order, price_position, size_position FROM {launch['db']['pair']}_positions_{stream['id']} order by id desc LIMIT 1")
    cursor.execute(query)
    #for (many_params['leverage'], many_params['price_order'], many_params['open_equity'], many_params['size_order'], many_params['price_position'], many_params['size_position']) in cursor:
    for (many_params['balance'], many_params['leverage'], many_params['price_order'], many_params['size_order'], many_params['price_position'],
         many_params['size_position']) in cursor:
        return many_params

    return many_params


def get_many_params(stream, cursor, candle, launch, prev_candle):
    print(f"prev_candle{prev_candle}")


    order_local = stream['order']
    print(f"{candle=}{order_local['leverage']=}")

    many_params = get_last_order(stream, cursor, launch)
    direction = stream['activation_blocks'][0]['actions'][0]['direction']
    print(f"{direction=}")

    if many_params == {}:
        many_params['balance'] = float(1)
        many_params['leverage'] = float(0)
        many_params['price_order'] = float(0)
        many_params['size_order'] = float(0)
        many_params['price_position'] = float(0)
        many_params['size_position'] = float(0)
        many_params['last'] = False

    if not equity_1[int(stream['id']) - 1][0].start:
        equity_1[int(stream['id']) - 1][0].first_start(many_params['balance'], many_params['leverage'], many_params['size_order'],
                                        many_params['price_order'], many_params['size_position'], many_params['price_position'], prev_candle['close'])
    if not equity_1[int(stream['id']) - 1][1].start:
        equity_1[int(stream['id']) - 1][1].first_start(many_params['balance'], many_params['leverage'], many_params['size_order'],
                                        many_params['price_order'], many_params['size_position'], many_params['price_position'], prev_candle['close'])

    if direction == 'long':
        equity_1[int(stream['id']) - 1][0].update(float(order_local['leverage']), float(prev_candle['close']))
        params = equity_1[int(stream['id']) - 1][0].update_long()
    elif direction == 'short':
        equity_1[int(stream['id']) - 1][1].update(float(order_local['leverage']), float(prev_candle['close']))
        params = equity_1[int(stream['id']) - 1][1].update_short()

    #params = equity_1.update(float(order_local['leverage']), float(prev_candle['close']))
    params_name = ('balance', 'leverage', 'size_order', 'price_order', 'size_position', 'price_position', 'close', 'pnl', 'equity_1', 'rpl')
    params_dict = dict(zip(params_name, params))
    for k in params_dict:
        many_params[k] = params_dict[k]
    many_params['last'] = True
    print(f"many_params {many_params}")

    return many_params

def get_leverage_action(leverage_condition, leverage_source, leverage_min, leverage_max, act, order_leverage):

    if str(leverage_condition).find("%") != -1:
        leverage_condition = float(leverage_condition.replace('%', ''))
        result = leverage_condition * leverage_source / 100
    else:
        if act == 'up':
            result = leverage_source + leverage_condition
        elif act == 'down':
            result = leverage_source - leverage_condition
        elif act == 'fix':
            if leverage_condition < order_leverage:
                result = order_leverage
            else:
                result = leverage_condition
        else:
            return None

    if leverage_min != None:
        if leverage_min > result:
            return leverage_min
        else:
            return result    

    if leverage_max != None:
        if leverage_max > result:
            return result
        else:
            return leverage_max

    return result

def get_equity_many(launch, stream, prev_candle, prev_prev_candle, last_order):

    if launch['mode'] == 'tester':
        if launch['many_metadata']['balance'].upper() == 'CURRENCY':
            result = last_order['equity_1']
        else:
            return None

    #if launch['mode'] == 'tester':
    #    if stream['order']['direction'] == 'long':
    #        if launch['many_metadata']['balance'].upper() == 'CURRENCY':
    #          result = last_order['open_equity'] + (float(prev_candle['close']) - last_order['price_position']) * last_order['size_position']
            # result = last_order['open_equity'] + ((float(prev_candle['close']) - last_order['price_position']) * last_order['size_position'] / last_order['price_position'])
    #    elif stream['order']['direction'] == 'short':
    #        if launch['many_metadata']['balance'].upper() == 'CURRENCY':
    #          result = last_order['open_equity'] - (float(prev_candle['close']) - last_order['price_position']) * last_order['size_position']
            # result = last_order['open_equity'] -  ((float(prev_candle['close']) - last_order['price_position']) * last_order['size_position'] / last_order['price_position'])
    #    else:
    #        return None

        return round(result, 8)
    elif launch['mode'] == 'robot':
        return get_equity_many_robot(stream)

def get_order_many(streams, id):

    for stream in streams:
        if stream['id'] == id:
            return stream['order']

    return None



def get_equity_many_robot(stream):

    try:
        
        http_data='{"equity": "BTC"}'
        r = requests.post(stream['url'], data=http_data, timeout=7)
        if r.status_code != 200:
            raise Exception("Status code = " + str(r.status_code))
        equity = r.text
        print("equity = " + equity + ", time = " + str(datetime.datetime.utcnow()))
        return float(equity)
    except Exception as e:
        time.sleep(2)
        print(e)
        print("equity exception" + ", time = " + str(datetime.datetime.utcnow()))
        return None

def send_leverage_many_robot(launch, stream):

    if launch['mode'] != 'robot':
        return

    try:

        order = stream['order']

        http_data='{"symbol":"' + stream['symbol'] + '","side":"' +  order['direction'] + '","leverage":"' + str(order['leverage']) + '","order_type":"' +  order['order_type'] + '"}'

        r = requests.post(stream['url'], data=http_data, timeout=7)
        if r.status_code != 200:
            raise Exception("Leverage Status code = " + str(r.status_code))
    except Exception as e:
        time.sleep(2)
        print(e)
        print("leverage send exception" + ", time = " + str(datetime.datetime.utcnow()))

def send_balancing_robot(launch, stream):

    if launch['mode'] != 'robot':
       return

    try:
        http_data='{"equity_balancing":"' + stream['balancing_symbol'] + '"}'
        r = requests.post(stream['url'], data=http_data, timeout=7)
        if r.status_code != 200:
            raise Exception("Leverage Status code = " + str(r.status_code))

    except Exception as e:
        time.sleep(2)
        print(e)
        print("leverage send exception" + ", time = " + str(datetime.datetime.utcnow()))


def get_total_equity(launch, prev_candle, cursor):

    if launch['traiding_mode'] != 'many':
        return None

    if launch['mode'] == 'tester' and prev_candle != {}:
        query = "select id, total_equity from {0} where id={1}".format(launch['price_table_name'], prev_candle['id'])
        cursor.execute(query)
        for (id, total_equity) in cursor:
            return total_equity

    return None

def calculate_stat_many(stat, date_time, total_equity):

    
    local_stat = stat['many']

    if local_stat.get('start_total_equity') == None:
        local_stat['start_total_equity'] = total_equity
        local_stat['total_equity_sum_percent'] = 0


        local_stat['start_date_month_total_equity'] = date_time
        local_stat['start_month_total_equity'] = total_equity
        local_stat['total_equity_month_percent'] = 0
        return

    local_stat['total_equity_sum_percent'] = (total_equity - local_stat['start_total_equity'])/local_stat['start_total_equity'] * 100
    
    if local_stat['start_date_month_total_equity'].month != date_time.month or local_stat['start_date_month_total_equity'].year != date_time.year:
        local_stat['start_date_month_total_equity'] = date_time
        local_stat['total_equity_month_percent'] = 0
        local_stat['start_month_total_equity'] = total_equity
        return

    local_stat['total_equity_month_percent'] = (total_equity - local_stat['start_month_total_equity'])/local_stat['start_month_total_equity'] * 100

def db_insert_position_many(stream, candle, many_params_source, launch, cn_pos):

    cursor_local = cn_pos.cursor()

    try:
        insert_stmt = (
            f"INSERT INTO {launch['db']['pair']}_positions_{stream['id']} (side, balance, time_order, price_order, leverage, size_order, price_position, size_position, type_order, blocks_id)"
            f" VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        data = (
            stream['order']['direction'],
            many_params_source['balance'],
            candle['time'],
            many_params_source['price_order'], 
            stream['order']['leverage'],
            #many_params_source['leverage'],
            many_params_source['size_order'],
            many_params_source['price_position'],
            many_params_source['size_position'],
            stream['order']['order_type'], 
            stream['order']['path']
        )
            
        cursor_local.execute(insert_stmt, data)
        cn_pos.commit()
        cursor_local.close()
        print(f" {many_params_source=}{stream['order']['leverage']=}")

    except Exception as e:
        print(e)
        cn_pos = db.get_db_connection(launch['db']['user'], launch['db']['password'], launch['db']['host'], launch['db']['database'])
        db_insert_position_many(stream, candle, many_params_source, launch, cn_pos)
