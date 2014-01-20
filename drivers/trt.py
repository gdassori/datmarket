# Copyright mn3monic @ freenode, 2013
# License: GPL (check GPL.txt for terms)

from __future__ import print_function, absolute_import
from datetime import datetime

from requests import exceptions

from configs.base_config import database
from includes.drivers_helper import Data_provider


__author__ = 'mn3monic'

class TRT(Data_provider):
    def __init__(self, url, orderbookurl, tickerurl, name, pair):
        Data_provider.__init__(self, url, orderbookurl, tickerurl, name, pair, database)
        self.find_dates()
        self.new_trades_available = None
        self.orderbookurl = orderbookurl

    def build_database_table(self):
        self.jsonfeed['last'] = self.imp_json(params="?timestamp=1")
        self.dump_data_into_sql(self.jsonfeed['last'])
        self.find_dates()

    def check_if_new_trades_available(self):
        ltt = self.last_trade_time + 1
        self.last_check = datetime.now().strftime('%d/%m/%y %H:%M:%S')
        trades = self.imp_json(params="?timestamp={}".format(ltt))
        if len(trades) >= 1:
            self.new_trades_available = len(trades)
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
        self.dump_data_into_sql(self.jsonfeed['last'])
        self.find_dates()

    def resume_database(self):
        self.dump_data_into_sql(self.jsonfeed['news'])
        self.find_dates()

    def get_ticker(self, max_retry=5):
        try:
            output = {}
            ticker = self.imp_json(url_override=self.tickerurl)
            temp_timestamp = self.imp_json(url_override=self.url)
            timestamp = temp_timestamp[0]['date']
            output['bid'] = ticker['result'][0]['bid']
            output['ask'] = ticker['result'][0]['ask']
            output['high'] = ticker['result'][0]['ask'] # ticker['result'][0]['high']
            output['low'] = ticker['result'][0]['bid'] # ticker['result'][0]['low']
            output['last'] = ticker['result'][0]['last']
            output['timestamp'] = timestamp
            output['volume'] = 0 # ticker['result'][0]['volume']
            self.ticker = output
        except (exceptions.SSLError, ValueError, exceptions.Timeout, exceptions.ConnectionError):
            self.ticker = {}