from __future__ import print_function, absolute_import
import MySQLdb
from configs.base_config import database

def load_ohlcv_from_database(table, limit=500, timeframe='d1'):
    output=dict()
    db = MySQLdb.connect(user=database['user'], passwd=database['passwd'], db=database['db'])
    cursor = db.cursor()
    db.commit()
    db_query = "SELECT * FROM {0}_ohlcv WHERE timeframe = '{1}' ORDER BY date DESC LIMIT {2}".format(table,
                                                                                                     timeframe,
                                                                                                     limit)
    cursor.execute(db_query)
    for row in cursor:
        output[int(row[0])] = [float(row[2]), float(row[3]), float(row[4]), float(row[5]), float(row[6])]
    return output

def load_closeline_from_database(table, limit=500, timeframe='d1'):
    output=dict()
    db = MySQLdb.connect(user=database['user'], passwd=database['passwd'], db=database['db'])
    cursor = db.cursor()
    db.commit()
    db_query = "SELECT * FROM {0}_ohlcv WHERE timeframe = '{1}' ORDER BY date DESC LIMIT {2}".format(table,
                                                                                                     timeframe,
                                                                                                     limit)
    cursor.execute(db_query)
    for row in cursor:
        output[int(row[0])] = [float(row[5]), float(row[6])]

def load_ticker_from_database(exchange, pair):
    output = dict()
    db = MySQLdb.connect(user=database['user'], passwd=database['passwd'], db=database['db'])
    cursor = db.cursor()
    db.commit()
    db_query = "SELECT * FROM tickers WHERE exchange like '{0}' and pair like '{1}';".format(exchange, pair)
    cursor.execute(db_query)
    for row in cursor:
        output['exchange'] = row[1]
        output['pair'] = row[2]
        output['bid'] = row[3]
        output['ask'] = row[4]
        output['high'] = row[5]
        output['low'] = row[6]
        output['last'] = row[7]
        output['timestamp'] = row[8]
        output['volume'] = row[9]
    return output

def load_all_tickers_from_database():
    output, tickers_dict = dict(), dict()
    db = MySQLdb.connect(user=database['user'], passwd=database['passwd'], db=database['db'])
    cursor = db.cursor()
    db.commit()
    db_query = "SELECT * FROM tickers;"
    cursor.execute(db_query)
    tickers = cursor.fetchall()
    id = 1
    for index, ticker in enumerate(tickers):
        output['exchange'] = tickers[index][1]
        output['pair'] = tickers[index][2]
        output['bid'] = tickers[index][3]
        output['ask'] = tickers[index][4]
        output['high'] = tickers[index][5]
        output['low'] = tickers[index][6]
        output['last'] = tickers[index][7]
        output['timestamp'] = int(tickers[index][8])
        output['volume'] = tickers[index][9]
        tickers_dict[tickers[index][1]+"_"+tickers[index][2]] = output
        output = {}
    return tickers_dict

def load_trades_from_database(table, limit=50):
    if limit > 500:
        limit = 500
    return_data, trades_dict, output = dict(), dict(), dict()
    db = MySQLdb.connect(user=database['user'], passwd=database['passwd'], db=database['db'])
    cursor = db.cursor()
    db.commit()
    db_query = "SELECT * from {0} ORDER BY ID DESC LIMIT {1}".format(table, limit)
    cursor.execute(db_query)
    trades = cursor.fetchall()
    print(trades)
    for index, trade in enumerate(trades):
        output['date'] = int(trades[index][1])
        output['price'] = float(trades[index][2])
        output['amount'] = float(trades[index][3])
        trades_dict[trades[index][0]] = output
        output = dict()
    return_data['trades'] = trades_dict
    return_data['pair'] = table.split("_")[1]
    return_data['exchange'] = table.split("_")[0]
    return return_data
