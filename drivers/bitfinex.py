# Copyright mn3monic @ freenode, 2013
# License: GPL (check GPL.txt for terms)

from __future__ import print_function, absolute_import
from datetime import datetime
from requests import exceptions
from includes.drivers_helper import Data_provider
from configs.base_config import database

__author__ = 'mn3monic'

class BitFinex(Data_provider):
    def __init__(self, url, orderbookurl, tickerurl, name, pair):
        Data_provider.__init__(self, url, orderbookurl, tickerurl, name, pair, database)
        self.database = database
        self.find_dates()
        self.new_trades_available = None
        self.orderbookurl = orderbookurl

    def build_database(self):
        pairs = {'ltcxbt': 'ltcbtc', 'xbtusd': 'btcusd', 'ltcusd': 'ltcusd' }
        candles = self.imp_json(url_override="https://api.bitfinex.com/v1/candles/{}?timestamp=1".format(pairs[self.pair]))
        candles.reverse()
        tradeid, rawdata, candles300 = 0.1, [], []
        for candle, item in enumerate(candles):         # Filtering candles
            if candles[candle]['period'] == 300:
                candles300.append(candles[candle])
        for candle, item in enumerate(candles300):
            rawdata.append({'date': candles300[candle]['start_at'], 'price': candles300[candle]['close'],
                            'amount': candles300[candle]['volume'],
                            'tid': candles300[candle]['start_at']+tradeid})
            tradeid += 0.1
        self.jsonfeed['last'] = rawdata
        self.dump_data_into_sql(self.jsonfeed)
        self.find_dates()

    def check_if_new_trades_available(self):
        self.last_check = datetime.now().strftime('%d/%m/%y %H:%M:%S')
        ltt = self.last_trade_time + 1
        trades = self.imp_json(params="?timestamp={}".format(ltt))
        if len(trades) >= 1:
            self.new_trades_available = len(trades)
        else:
            self.new_trades_available = False

    def resume_database(self):
        self.dump_data_into_sql(self.jsonfeed['last'])
        self.find_dates()
        self.set_max_tid()

    def update_database(self):
        self.dump_data_into_sql(self.jsonfeed['news'])
        self.find_dates()
        self.set_max_tid()


    def get_trades(self):
        newtrades = []
        self.set_max_tid()
        tradeid_on_database = self.max_tid
        tradeid_resume = tradeid_on_database + 0.1
        ltt = self.last_trade_time +1
        trades = self.imp_json(params="?timestamp={}".format(ltt))
        trades.reverse()
        rawdata = []
        for trade, item in enumerate(trades):
            rawdata.append({'date': int(trades[trade]['timestamp']), 'price': float(trades[trade]['price']),
                            'amount': float(trades[trade]['amount']),
                            'tid': float(str(tradeid_resume))})
            tradeid_resume += 0.1
        self.jsonfeed['old'] = self.jsonfeed['last']
        self.jsonfeed['last'] = rawdata
        for key, item in enumerate(self.jsonfeed['last']):
            if item in self.jsonfeed['old']:
                pass
            else:
                newtrades.append(item)
        self.jsonfeed['news'] = newtrades
        print(int(datetime.now().strftime('%s')), "-", self.name+"_"+self.pair, ":",
                      len(self.jsonfeed['news']), "new trades available")


# rewriting generic funcs - we enable auto_increment on TID and
# disable TID from feed to SQL, because lack of TID on BitFinex
# and to avoid skipping trades cause they're seen as TID duplicates (and ignored)


    def create_database(self):
        """
        Create a database ID,DATE,PRICE,AMOUNT.
        """
        db_query = """CREATE TABLE IF NOT EXISTS `{0}` (
          `ID` float(11) NOT NULL,
          `DATE` int(11) NOT NULL,
          `PRICE` float NOT NULL,
          `AMOUNT` float NOT NULL,
          PRIMARY KEY (`ID`)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""".format(self.table)
        self.cursor.execute(db_query)
        self.db.commit()

    def dump_data_into_sql(self, rawdata, skipDups=True):
        """
        Dump raw data into Database.
        Data must comes from an exchange driver in format: ID,DATE,PRICE,AMOUNT.
        ID Duplicates skipped.
        """
        if skipDups:
            skip = 'IGNORE'
        else:
            skip = ''
        for (offset, item) in enumerate(rawdata):
            db_query="""INSERT {0} INTO {1} (ID, DATE, PRICE, AMOUNT) VALUES
                        ({2}, {3}, {4}, {5})""".format(skip, self.table,
                                                  rawdata[offset]['tid'],
                                                  rawdata[offset]['date'],
                                                  rawdata[offset]['price'],
                                                  rawdata[offset]['amount'])
            self.cursor.execute(db_query)
            self.db.commit()



### Bitfinex needs an adjustment on orderbook to made it compatible with dat market.

    def get_orderbook(self):
        orderbook = self.imp_json(url_override=self.orderbookurl)
        bids_sums, asks_sums = [], []
        newbook = {'bids':[],'asks':[]}
        for bid in orderbook['bids']:
            price = "{:<8.7g}".format(float(bid['price']))
            amount = "{:<8.7g}".format(float(bid['amount']))
            bids_sums.append(float(bid['amount']))
            newbook['bids'].append([price, amount, "{:<8.7g}".format(sum(bids_sums))])

        for ask in orderbook['asks']:
            price = "{:<8.7g}".format(float(ask['price']))
            amount = "{:<8.7g}".format(float(ask['amount']))
            asks_sums.append(float(ask['amount']))
            newbook['asks'].append([price, amount, "{:<8.7g}".format(sum(asks_sums))])
        self.orderbook = newbook

    def get_ticker(self, max_retry=5):
        pairs = {'ltcxbt': 'ltcbtc', 'xbtusd': 'btcusd', 'ltcusd': 'ltcusd' }
        output = {}
        try:
            ticker = self.imp_json(url_override=self.tickerurl)
            today = self.imp_json(url_override="https://api.bitfinex.com/v1/today/{0}".format(pairs[self.pair]))
            output['bid'] = ticker['bid']
            output['ask'] = ticker['ask']
            output['high'] = today['high']
            output['low'] = today['low']
            output['last'] = ticker['last_price']
            output['timestamp'] = int(float(ticker['timestamp']))
            output['volume'] = float("{0:.3f}".format(float(today['volume'])))
            self.ticker = output
        except (exceptions.SSLError, ValueError, exceptions.Timeout, exceptions.ConnectionError):
            self.ticker = {}