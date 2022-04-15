import robot_many as many

def check_pnl(condition, block, candle, order):
    
    direction = order['direction']

    ind_oper = condition['value'].split(' ')[0]
    ind_value = float(condition['value'].split(' ')[1])
    if direction == 'short':
        pnl = order['open_price_position'] - (((order['open_price_position'] / 100) * ind_value))/float(order['leverage'])
    else:
        pnl = order['open_price_position'] + (((order['open_price_position'] / 100) * ind_value))/float(order['leverage'])

    if candle.get('price') == None:
        return False

    if direction == 'long':
        left_value = candle['price']
        right_value = pnl
    else:
        left_value = pnl
        right_value = candle['price']

    if ind_oper == '>=' and left_value >= right_value:
        print("pnl(" + direction + ", " + condition['value'] +")=" + str(pnl) + ", time=" + str(candle['time']) + ", price=" + str(candle['price']))
        return pnl
    elif ind_oper == '<=' and left_value <= right_value:
        print("pnl(" + direction + ", " + condition['value'] +")=" + str(pnl) + ", time=" + str(candle['time']) + ", price=" + str(candle['price']))
        return pnl
    elif ind_oper == '=' and left_value == right_value:
        print("pnl(" + direction + ", " + condition['value'] +")=" + str(pnl) + ", time=" + str(candle['time']) + ", price=" + str(candle['price']))
        return pnl
    elif ind_oper == '>' and left_value > right_value:
        print("pnl(" + direction + ", " + condition['value'] +")=" + str(pnl) + ", time=" + str(candle['time']) + ", price=" + str(candle['price']))
        return pnl
    elif ind_oper == '<' and left_value < right_value:
        print("pnl(" + direction + ", " + condition['value'] +")=" + str(pnl) + ", time=" + str(candle['time']) + ", price=" + str(candle['price']))
        return pnl
    else:
        return False

def check_price(condition, block, candle, order, launch, stream, cursor, prev_candle):
    
    # {"type":"price","change_percent":"> 11","number":"1"}

    # для many
    # {"type":"price", "source": "price_order", "change_percent":"> 11","number":"1"}

    direction = order['direction']

    if condition.get("type_change") == "one_candle":
        start = launch['cur_candle']['open']
    else:
        if many.is_many(launch):

            source = condition.get('source')
            if source == None:
                return False
            
            last_position = many.get_last_order(stream, cursor, launch)

            start = float(last_position.get(source))
            if source == None:
                return False
        else:
            start = order['open_price_position']

    ind_oper = condition['change_percent'].split(' ')[0]
    ind_value = float(condition['change_percent'].split(' ')[1])
    if direction == 'short':
        pnl = start - start / 100 * ind_value
    else:
        pnl = start + start / 100 * ind_value

    if candle.get('price') == None:
        return False

    if direction == 'long':
        left_value = candle['price']
        right_value = pnl
    else:
        left_value = pnl
        right_value = candle['price']

    if ind_oper == '>=' and left_value >= right_value:
        result = pnl
    elif ind_oper == '<=' and left_value <= right_value:
        result = pnl
    elif ind_oper == '=' and left_value == right_value:
        result = pnl
    elif ind_oper == '>' and left_value > right_value:
        result = pnl
    elif ind_oper == '<' and left_value < right_value:
        result = pnl
    else:
        result = False

    if result != False:
        print("price(" + direction + ", " + str(ind_value) +")=" + str(pnl) + ", time=" + str(candle['time']) + ", price=" + str(candle['price']))

    return result 
