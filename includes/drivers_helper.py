# Copyright mn3monic @ freenode, 2013
# License: GPL (check GPL.txt for terms)


# class Data_provider is generic method for generating\saving OHLCV data from exchangers API
# exchangers are specialized as 'drivers', check other exchangers for examples.

from __future__ import print_function, absolute_import
import requests
import time
import json
import MySQLdb
from requests import exceptions

output_feeds_path = "feeds/"

class Data_provider():
    """
    Usage: Exchanger(json_feed_url, name, pair, database)
    """
###################### Here begins the external json feeds part ###################


    def __init__(self, url, orderbookurl, tickerurl, name, pair, database):
        self.update_status = 'No update yet'
        self.url = url
        self.orderbookurl = orderbookurl
        self.tickerurl = tickerurl
        self.name = name
        self.pair = pair
        self.ohlcv = {'m1': {}, 'm5': {}, 'm15': {}, 'm30': {}, 'h1': {}, 'h4': {}, 'd1': {}}
        self.table = "{}_{}".format(name, pair)
        self.database = database
        self.jsonfeed = {'old':[], 'last':[], 'news':[]}
        self.ticker = {}
        self.orderbook = {}
        self.db = MySQLdb.connect(user=database['user'], passwd=database['passwd'], db=database['db'])
        self.cursor = self.db.cursor()
        self.tickerstable = "tickers"
        self.timeframes = {'m1': 60, 'm5': 300, 'm15': 900, 'm30': 1800, 'h1': 3600, 'h4': 14400, 'd1': 86400}

    def imp_json(self, url_override=False, params='', timeout=10, max_retry=5):
        """
        Import json feed self.url
        """
        retry = 0
        while retry <= max_retry:
            try:
                if params:
                    r = requests.get("{}".format(url_override or self.url) + params, verify=False, timeout=timeout)
                else:
                    r = requests.get("{}".format(url_override or self.url), verify=False, timeout=timeout)
                jsonfeed = r.json()
                return jsonfeed
            except:
                retry += 1
                time.sleep(0.5)
                continue
        retry = 0
        if params:
            r = requests.get("{}".format(url_override or self.url) + params, verify=False, timeout=timeout)
        else:
            r = requests.get("{}".format(url_override or self.url), verify=False, timeout=timeout)
        jsonfeed = r.json()
        return jsonfeed

###################### Here ends the external json feeds part #####################

###################### Here begins the SQL part ###################################
    def create_tickers_table(self):
        db_query = """CREATE TABLE IF NOT EXISTS `{0}` (
                  `id` int NOT NULL AUTO_INCREMENT,
                  `exchange` varchar(60) NOT NULL,
                  `pair` varchar(8) NOT NULL,
                  `bid` float NOT NULL,
                  `ask` float NOT NULL,
                  `high` float NOT NULL,
                  `low` float NOT NULL,
                  `last` float NOT NULL,
                  `timestamp` int NOT NULL,
                  `volume` float NOT NULL,
                  PRIMARY KEY (`id`),
                  UNIQUE KEY `id` (`id`),
                  UNIQUE KEY `CLUSTERED` (`exchange`,`pair`)
                ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""".format(self.tickerstable)
        self.cursor.execute(db_query)
        self.db.commit()

    def create_database(self):
        """
        Create a database ID,DATE,PRICE,AMOUNT.
        """
        db_query = """CREATE TABLE IF NOT EXISTS `{0}` (
          `ID` int(11) NOT NULL,
          `DATE` int(11) NOT NULL,
          `PRICE` float NOT NULL,
          `AMOUNT` float NOT NULL,
          PRIMARY KEY (`ID`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""".format(self.table)
        self.cursor.execute(db_query)

    def load_data_in_range(self, rangemin=1, rangemax=int(time.time()), limit=1000):
        """
        Read data into a specified range.
        Usage func(rangemin, rangemax, limit=n[default: 0])
        Output is self.rawdata
        """
        db_query = "SELECT * FROM {0} WHERE DATE BETWEEN {1} AND {2}".format(self.table, rangemin, rangemax)
        #cursor = self.db.cursor(cursors.SSCursor)
        self.cursor.execute(db_query)
        output = dict()
        for row in self.cursor:
            output[int(row[0])] = [int(row[1]), row[2], row[3]]
        self.rawdata = output

    def load_trades(self, range):
        db_query = """SELECT *
                        FROM `{0}`
                        ORDER BY `{0}`.`ID` DESC
                        LIMIT 0 , {1}
                        """.format(self.table, range)
        self.cursor.execute(db_query)
        output = list()
        for row in self.cursor:
            output.append({'tid':int(row[0]), 'date':int(row[1]), "price": float(row[2]), "amount":float(row[3])})
        return output


    def dump_data_into_sql(self, rawdata, skipDups=True):
        """
        Dump raw data into Database.
        Data must comes from an exchange driver in format: ID,DATE,PRICE,AMOUNT.
        ID Duplicates skipped..
        """
        if skipDups:
            skip = 'IGNORE'
        else:
            skip = ''
        for (offset, item) in enumerate(rawdata):
            db_query="""INSERT {0} INTO {1} (ID, DATE, PRICE, AMOUNT) VALUES
            ({2}, {3}, {4}, {5})""".format(skip, self.table, int(rawdata[offset]['tid']), rawdata[offset]['date'],
            float(rawdata[offset]['price']), float(rawdata[offset]['amount']))
            self.cursor.execute(db_query)
            self.db.commit()


    def find_dates(self):
        db_query = "SELECT MIN(DATE) FROM {}".format(self.table)
        self.cursor.execute(db_query)
        self.first_trade_time = int(self.cursor.fetchone()[0])
        db_query = "SELECT MAX(DATE) FROM {}".format(self.table)
        self.cursor.execute(db_query)
        self.last_trade_time = int(self.cursor.fetchone()[0])

    def set_max_tid(self):
        self.db.commit()
        db_query = "SELECT * FROM {} ORDER BY ID DESC LIMIT 1;".format(self.table)
        self.cursor.execute(db_query)
        self.max_tid = self.cursor.fetchone()[0]

    def find_closeprice(self, date):
        self.db.commit()
        db_query = "SELECT PRICE FROM {} WHERE DATE <= {} AND\
        PRICE IS NOT NULL ORDER BY DATE DESC LIMIT 1".format(self.table, date)
        self.cursor.execute(db_query)
        last_close_price = int(self.cursor.fetchone()[0])
        return last_close_price

###################### Here ends the SQL part ###################################

###################### Here begins the OHLCV part ###############################


    def build_ohlcv_candles_from_new_trades(self, newtrades):
        if newtrades:
            newtrades_dict = {}
            for id, trade in enumerate(newtrades):
                tid = int(newtrades[id]['tid'])
                price = float(newtrades[id]['price'])
                amount = float(newtrades[id]['amount'])
                date = int(newtrades[id]['date'])
                newtrades_dict[tid] = [date, price, amount]
            ordered_trades = sorted(newtrades_dict.items(), key=lambda t:(t[1][0],t[0]))
            ohlcv = {}
            for timeframe in self.timeframes:
                tfsecs = self.timeframes[timeframe]
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
            self.new_ohlcv = ohlcv
        else:
            self.newohlcv = {x:{} for x in self.timeframes}

    def find_father_candle(self, timestamp, timeframe):
        found = False
        while not found:
            if timestamp % self.timeframes[timeframe] != 0:
                timestamp -= 1
            else:
                found = True
        return timestamp


    def update_ohlcv_candles(self):
        for timeframe in self.new_ohlcv:
            for timestamp in self.new_ohlcv[timeframe]:
                try:
                    self.ohlcv[timeframe][timestamp]
                    fresh_candle = False
                except KeyError:
                    fresh_candle = True
                    self.ohlcv[timeframe][timestamp] = [0,0,0,0,0]
                if not fresh_candle:
                    if self.ohlcv[timeframe][timestamp][0] == 0:
                        self.ohlcv[timeframe][timestamp][0] = self.new_ohlcv[timeframe][timestamp][0]
                    if self.new_ohlcv[timeframe][timestamp][1] > self.ohlcv[timeframe][timestamp][1]:
                        self.ohlcv[timeframe][timestamp][1] = self.new_ohlcv[timeframe][timestamp][1]
                    if self.new_ohlcv[timeframe][timestamp][2] < self.ohlcv[timeframe][timestamp][2]:
                        self.ohlcv[timeframe][timestamp][2] = self.new_ohlcv[timeframe][timestamp][2]
                    self.ohlcv[timeframe][timestamp][3] = self.new_ohlcv[timeframe][timestamp][3]
                    self.ohlcv[timeframe][timestamp][4] += self.new_ohlcv[timeframe][timestamp][4]
                else:
                    self.ohlcv[timeframe][timestamp] = self.new_ohlcv[timeframe][timestamp]

    def save_updated_ohlcv_candles_to_database(self):
        ohlcv_to_be_updated = {'m1': {}, 'm5': {}, 'm15': {}, 'm30': {}, 'h1': {}, 'h4': {}, 'd1': {}}
        for timeframe in self.new_ohlcv:
            for timestamp in self.new_ohlcv[timeframe]:
                ohlcv_to_be_updated[timeframe][timestamp] = self.ohlcv[timeframe][timestamp]
        for timeframe in ohlcv_to_be_updated:
            for timestamp in ohlcv_to_be_updated[timeframe]:
                db_query = "REPLACE INTO {0}_ohlcv (date, timeframe, open, high, low, close, volume)\
                                VALUES ({1}, '{2}', {3}, {4}, {5}, {6}, {7})".format(self.table,
                                                                                     timestamp,
                                                                                     timeframe,
                                ohlcv_to_be_updated[timeframe][timestamp][0],
                                ohlcv_to_be_updated[timeframe][timestamp][1],
                                ohlcv_to_be_updated[timeframe][timestamp][2],
                                ohlcv_to_be_updated[timeframe][timestamp][3],
                                ohlcv_to_be_updated[timeframe][timestamp][4])
                self.cursor.execute(db_query)
        self.db.commit()


    def load_ohlcv_candle_from_database(self, candle):
        timeframes = {'m1': 60, 'm5': 300, 'm15': 900, 'm30': 1800, 'h1': 3600, 'h4': 14400, 'd1': 86400}
        temp_ohlcv = {x: {} for x in timeframes}
        for timeframe in timeframes:
            db_query="""SELECT *
                        FROM `{0}_ohlcv`
                        WHERE `date` ={1}
                        AND `timeframe` LIKE '{2}'
                        """.format(self.table, candle, timeframe)
            self.cursor.execute(db_query)
            for row in self.cursor:
                temp_ohlcv[timeframe] = {row[0]: [row[2], row[3], row[4], row[5], row[6]]}
        return temp_ohlcv


    def save_ohlcv_to_database(self, timeframe='d1'):
        if self.ohlcv[timeframe]:
            for date in self.ohlcv[timeframe]:
                db_query = "REPLACE INTO {0}_ohlcv (date, timeframe, open, high, low, close, volume)\
                VALUES ({1}, '{2}', {3}, {4}, {5}, {6}, {7})".format(self.table, date, timeframe,
                self.ohlcv[timeframe][date][0], self.ohlcv[timeframe][date][1], self.ohlcv[timeframe][date][2],
                self.ohlcv[timeframe][date][3], self.ohlcv[timeframe][date][4])
                self.cursor.execute(db_query)
            self.db.commit()
        #cursor.close()


###################### Here ends the OHLCV part ##################################

###################### Here begin the orderbook part #############################

    def get_orderbook(self):
        orderbook = self.imp_json(url_override=self.orderbookurl)
        bids_sums, asks_sums = [], []
        newbook = {'bids':[],'asks':[]}
        for bid in orderbook['bids']:
            price = float("{:<8.7g}".format(float(bid[0])))
            amount = float("{:<8.7g}".format(float(bid[1])))
            bids_sums.append(amount)
            newbook['bids'].append([price, amount, sum(bids_sums)])

        for ask in orderbook['asks']:
            price = float("{:<8.7g}".format(float(ask[0])))
            amount = float("{:<8.7g}".format(float(ask[1])))
            asks_sums.append(amount)
            newbook['asks'].append([price, amount, sum(asks_sums)])
        self.orderbook = newbook

    def save_orderbook_to_disk(self):
        pointer = open("{0}{1}_{2}_orderbook.json".format(output_feeds_path, self.name, self.pair), 'wb')
        json.dump(self.orderbook, pointer)
        pointer.close()

    def update_orderbook(self):
        try:
            self.get_orderbook()
        except (ValueError, exceptions.ConnectionError):
            print(int(time.time()), "-", "unable to fetch orderbook for " + self.name + "_" + self.pair + "...")
            return
        self.save_orderbook_to_disk()

    def save_ticker_to_database(self):
        db_query = """REPLACE INTO {0} (exchange,
        pair, bid, ask, high, low, last, timestamp, volume)
           VALUES ('{1}', '{2}', {3}, {4}, {5}, {6}, {7}, {8}, {9)""".format(self.tickerstable,
                                                                       self.name,
                                                                       self.pair,
                                                                       self.ticker['bid'],
                                                                       self.ticker['ask'],
                                                                       self.ticker['high'],
                                                                       self.ticker['low'],
                                                                       self.ticker['last'],
                                                                       self.ticker['timestamp'],
                                                                       self.ticker['volume'])
        self.cursor.execute(db_query)
        self.db.commit()

    def load_ticker_from_database(self):
        db_query = "SELECT * FROM `{0}` WHERE exchange LIKE '{1}' and PAIR like '{2}'".format(self.tickerstable,
                                                                                              self.name,
                                                                                              self.pair)
        self.cursor.execute(db_query)
        output = {}
        for row in self.cursor:
            output['bid'] = row[3]
            output['ask'] = row[4]
            output['high'] = row[5]
            output['low'] = row[6]
            output['last'] = row[7]
            output['timestamp'] = int(row[8])
            output['volume'] = row[9]
        return output