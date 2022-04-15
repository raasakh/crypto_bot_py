import mysql.connector
import time
import json
import datetime

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

def db_save_state(launch, stat, cnx2, cursor):

    if launch['mode'] != 'robot':
        return False

    launch_data = json.dumps(launch, default=json_serial)
    stat_data = json.dumps(stat, default=json_serial)

    try:
        update_query = ("UPDATE launch SET launch_data = %s, stat_data = %s where id = 1")
        data = (launch_data, stat_data)
        cursor.execute(update_query, data)
        cnx2.commit()
    except Exception as e:
        print(e)

def json_serial(obj):

    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
