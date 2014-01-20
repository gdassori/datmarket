from __future__ import print_function, absolute_import
import time

import MySQLdb

from configs.base_config import database
from includes import sql_queries

timeframes = {'m1': 60, 'm5': 300, 'm15': 900, 'm30': 1800, 'h1': 3600, 'h4': 14400, 'd1': 86400}

class Ohlcv():
    def __init__(self, exchanger_pair):
        self.trades_table = exchanger_pair
        self.name = exchanger_pair.split("_")[0]
        self.pair = exchanger_pair.split("_")[1]
        self.ohlcv_table = exchanger_pair + '_ohlcv'
        self.db = MySQLdb.connect(user=database['user'], passwd=database['passwd'], db=database['db'])
        self.cursor = self.db.cursor()
        self.timeframes = {'m1': 60, 'm5': 300, 'm15': 900, 'm30': 1800, 'h1': 3600, 'h4': 14400, 'd1': 86400}

    def flush_ohlcv_table(self):
        db_query ="RENAME TABLE {0} TO {0}_temp".format(self.ohlcv_table)
        self.cursor.execute(db_query)
        self.db.commit()
        db_query ="CREATE TABLE {0} LIKE {0}_temp".format(self.ohlcv_table)
        self.cursor.execute(db_query)
        self.db.commit()
        db_query ="DROP TABLE {0}_temp".format(self.ohlcv_table)
        self.cursor.execute(db_query)
        self.db.commit()

    def find_dates(self, table):
        db_query = "SELECT MIN(DATE) FROM {}".format(table)
        self.cursor.execute(db_query)
        mindate = self.cursor.fetchone()[0]
        if not mindate:
            mindate = 0
        else:
            mindate = int(mindate)
        db_query = "SELECT MAX(DATE) FROM {}".format(table)
        self.cursor.execute(db_query)
        maxdate = self.cursor.fetchone()[0]
        if not maxdate:
            maxdate = 0
        else:
            maxdate = int(maxdate)
        return mindate, maxdate

    def find_max_candle(self, timeframe):
        db_query = "SELECT MAX(DATE) FROM `{0}` WHERE timeframe = '{1}'".format (self.ohlcv_table, timeframe)
        self.cursor.execute(db_query)
        maxdate = self.cursor.fetchone()[0]
        return maxdate

    def load_trades_in_range(self, rangemin=1, rangemax=int(time.time())):
        db_query = "SELECT * FROM {0} WHERE DATE BETWEEN {1} AND {2}".format(self.trades_table, rangemin, rangemax)
        self.cursor.execute(db_query)
        output = dict()
        for row in self.cursor:
            output[int(row[0])] = [int(row[1]), row[2], row[3]]
        return output

    def load_ohlcv_candle_from_database(self, candle):
        timeframes = {'m1': 60, 'm5': 300, 'm15': 900, 'm30': 1800, 'h1': 3600, 'h4': 14400, 'd1': 86400}
        temp_ohlcv = {x: {} for x in timeframes}
        for timeframe in timeframes:
            db_query="""SELECT *
                        FROM `{0}`
                        WHERE `date` ={1}
                        AND `timeframe` LIKE '{2}'
                        """.format(self.ohlcv_table, candle, timeframe)
            self.cursor.execute(db_query)
            for row in self.cursor:
                temp_ohlcv[timeframe] = {row[0]: [row[2], row[3], row[4], row[5], row[6]]}
        return temp_ohlcv


    def build_ohlcv(self, trades):
        ordered_trades = sorted(trades.items(), key=lambda t:(t[1][0],t[0]))
        ohlcv = {}
        for timeframe in timeframes:
            tfsecs = timeframes[timeframe]
            ohlcv[timeframe] = {}
            startfrom = self.find_father_candle(ordered_trades[0][1][0], timeframe)
            ohlcv[timeframe] = {startfrom:[0,-1,9999999,0,0]}
            for id, trade in enumerate(ordered_trades):
                candle = ohlcv[timeframe][startfrom]
                this = ordered_trades[id]
                tr_price = this[1][1]
                tr_vol = this[1][2]
                openpr, highpr, lowpr, closepr, vol = candle[0], candle[1], candle[2], candle[3], candle[4]
                if this[1][0] < startfrom + tfsecs:
                    if not openpr: openpr = tr_price
                    if highpr < tr_price: highpr = tr_price
                    if lowpr > tr_price: lowpr = tr_price
                    closepr = tr_price
                    vol += tr_vol
                    ohlcv[timeframe][startfrom] = [openpr, highpr, lowpr, closepr, vol]
                else:
                    if this[1][0] > startfrom + tfsecs:
                        for miss_candle in range(startfrom+tfsecs, this[1][0], tfsecs):
                            ohlcv[timeframe][miss_candle] = (closepr, closepr, closepr, closepr, 0)
                            startfrom = miss_candle
                        ohlcv[timeframe][startfrom] = [tr_price, tr_price, tr_price, tr_price, 0]
                    else:
                        startfrom += tfsecs
                        ohlcv[timeframe][startfrom] = [tr_price, tr_price, tr_price, tr_price, 0]
        return ohlcv

    def save_ohlcv_to_database(self, ohlcv):
        ohlcv_to_be_updated = {x: {} for x in timeframes}
        for timeframe in ohlcv:
            for timestamp in ohlcv[timeframe]:
                ohlcv_to_be_updated[timeframe][timestamp] = ohlcv[timeframe][timestamp]
        for timeframe in ohlcv_to_be_updated:

            for timestamp in ohlcv_to_be_updated[timeframe]:
                db_query = "REPLACE INTO {0} (date, timeframe, open, high, low, close, volume)\
                                VALUES ({1}, '{2}', {3}, {4}, {5}, {6}, {7})".format(self.ohlcv_table,
                                                                                     timestamp,
                                                                                     timeframe,
                                ohlcv_to_be_updated[timeframe][timestamp][0],
                                ohlcv_to_be_updated[timeframe][timestamp][1],
                                ohlcv_to_be_updated[timeframe][timestamp][2],
                                ohlcv_to_be_updated[timeframe][timestamp][3],
                                ohlcv_to_be_updated[timeframe][timestamp][4])
                self.cursor.execute(db_query)
        self.db.commit()

    def delete_database_entry(self, table, rangemin, rangemax):
        db_query = "DELETE FROM {0} where date between {1} and {2}".format(table, rangemin, rangemax)
        self.db.commit()
        self.cursor.execute(db_query)

    def delete_last_ohlcv_candles(self, timeframe, how_many):
        rangemax = time.time()
        rangemin = how_many * timeframes[timeframe]
        db_query = "DELETE FROM {0} where date between {1} and {2} and timeframe LIKE '{3}".format(self.ohlcv_table,
                                                                                                   rangemin,
                                                                                                   rangemax,
                                                                                                   timeframe)

        self.cursor.execute(db_query)
        self.db.commit()

    def load_candles_from_database(self, rangemin, rangemax, timeframe):
        temp_ohlcv = {}

        db_query = "SELECT * from {0} where date between {0} and {1} and timestamp like '{2}".format(self.ohlcv_table,
                                                                                                    rangemin,
                                                                                                    rangemax,
                                                                                                    timeframe)
        self.cursor.execute(db_query)
        for row in self.cursor:
            temp_ohlcv[row[0]] = [row[2], row[3], row[4], row[5], row[6]]
        return temp_ohlcv

    @staticmethod
    def find_father_candle(timestamp, timeframe):
        found = False
        while not found:
            if timestamp % timeframes[timeframe] != 0:
                timestamp -= 1
            else:
                found = True
        return timestamp

    def new_candles_builder(self):
        executed = 0
        for timeframe in self.timeframes:
            now = int(time.time())
            maxdate = self.find_max_candle(timeframe)
            nextcandle = maxdate + self.timeframes[timeframe]
            if now > nextcandle:
                executed = 1
                db_query = """SELECT *
                FROM `{0}`
                WHERE `date` = {1}
                AND `timeframe` LIKE '{2}'
                LIMIT 0 , 1""".format(self.ohlcv_table, maxdate, timeframe)
                self.cursor.execute(db_query)
                tempcandle = self.cursor.fetchall()
                closeprice = tempcandle[0][5]
                db_query = "REPLACE INTO {0} (date, timeframe, open, high, low, close, volume)\
                            VALUES ({1}, '{2}', {3}, {4}, {5}, {6}, {7})".format(self.ohlcv_table,
                            nextcandle, timeframe, closeprice, closeprice, closeprice, closeprice, 0)
                self.cursor.execute(db_query)
        if executed:
            self.db.commit()
        return True


class Ticker:
    def __init__(self):
        self.tickers_table = "tickers"
        self.db = MySQLdb.connect(user=database['user'], passwd=database['passwd'], db=database['db'])
        self.cursor = self.db.cursor()

    def save_ticker_to_database(self, ticker_name, ticker):
        if ticker['volume'] and ticker['timestamp'] and ticker['bid'] and ticker['ask']:
            exchanger = ticker_name.split("_")[0]
            pair = ticker_name.split("_")[1]
            db_query = sql_queries.save_ticker_to_database.format(self.tickers_table, exchanger, pair, ticker)
            self.cursor.execute(db_query)
            self.db.commit()
        else:
            pass

    def save_all_tickers_to_database(self, tickers):
        for ticker in tickers:
            if tickers[ticker]:
                exchanger = ticker.split("_")[0]
                pair = ticker.split("_")[1]
                db_query = sql_queries.save_ticker_to_database.format(self.tickers_table,
                                                                      exchanger, pair, tickers[ticker])
                self.cursor.execute(db_query)
        self.db.commit()

    def load_all_tickers_from_database(self):
        tickers = {}
        db_query = sql_queries.load_all_tickers_from_database.format(self.tickers_table)
        self.db.commit()
        self.cursor.execute(db_query)
        for row in self.cursor:
            ticker = {}
            exchanger = row[1]
            pair = row[2]
            ticker['bid'] = row[3]
            ticker['ask'] = row[4]
            ticker['high'] = row[5]
            ticker['low'] = row[6]
            ticker['last'] = row[7]
            ticker['timestamp'] = int(row[8])
            ticker['volume'] = row[9]
            tickers[exchanger + "_" + pair] = ticker
        return tickers