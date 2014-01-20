# Copyright mn3monic @ freenode, 2013
# License: GPL (check GPL.txt for terms)

from __future__ import print_function, absolute_import
from datetime import datetime

from requests import exceptions

from includes.drivers_helper import Data_provider
from configs.base_config import database

__author__ = 'mn3monic'


class BitStamp(Data_provider):
    def __init__(self, url, orderbookurl, tickerurl, name, pair):
        Data_provider.__init__(self, url, orderbookurl, tickerurl, name, pair, database)
        self.last_trade_time = 0
        self.first_trade_time = 0
        self.new_trades_available = None

    def check_if_new_trades_available(self):
        trades = self.imp_json()
        tids = []
        for index, trade in enumerate(trades):
            tids.append(trades[index]['tid'])
        max_trades_tid = max(tids)
        if max_trades_tid > self.max_tid:
            self.new_trades_available = max_trades_tid - self.max_tid
        else:
            self.new_trades_available = False

    def get_trades(self):
        newtrades = []
        self.jsonfeed['old'] = self.jsonfeed['last']
        self.jsonfeed['last'] = self.imp_json()
        for key, item in enumerate(self.jsonfeed['last']):
            if item in self.jsonfeed['old']:
                pass
            else:
                newtrades.append(item)
        self.jsonfeed['news'] = newtrades
        print(int(datetime.now().strftime('%s')), "-", self.name+"_"+self.pair, ":",
                      len(self.jsonfeed['news']), "new trades available")

    def update_database(self):
        self.dump_data_into_sql(self.jsonfeed['news'])
        self.set_max_tid()

    def resume_database(self):
        self.dump_data_into_sql(self.jsonfeed['last'])
        self.set_max_tid()

    def get_ticker(self, max_retry=5):
        try:
            ticker = self.imp_json(url_override=self.tickerurl, timeout=2)
            output = dict()
            output['bid'] = ticker['bid']
            output['ask'] = ticker['ask']
            output['high'] = ticker['high']
            output['low'] = ticker['low']
            output['last'] = ticker['last']
            output['timestamp'] = ticker['timestamp']
            output['volume'] = float("{0:.3f}".format(float(ticker['volume'])))
            self.ticker = output
        except (exceptions.SSLError, ValueError, exceptions.Timeout, exceptions.ConnectionError):
            output = dict()
            output['bid'] = ""
            output['ask'] = ""
            output['high'] = ""
            output['low'] = ""
            output['last'] = ""
            output['timestamp'] = ""
            output['volume'] = ""
            self.ticker = output