# Copyright mn3monic @ freenode, 2013
# License: GPL (check GPL.txt for terms)

from __future__ import print_function

__author__ = 'mn3monic'

from datetime import datetime
from includes.drivers_helper import Data_provider
from configs.base_config import database
from requests import exceptions


class BTCe(Data_provider):
    def __init__(self, url, orderbookurl, tickerurl, name, pair):
        controller = pair.replace("xbt", "btc")
        location = url.find(controller)                         # edit feed url from 'btcusd' to 'btc_usd' (i.e.)
        url = url[:location+3] + "_" + url[location+3:]
        orderbookurl = orderbookurl[:location+3] + "_" + orderbookurl[location+3:]
        tickerurl = tickerurl[:location+3] + "_" + tickerurl[location+3:]
        Data_provider.__init__(self, url, orderbookurl, tickerurl, name, pair, database)
        self.last_trade_time = 0
        self.first_trade_time = 0
        self.new_trades_available = None


    def build_database(self, filename):
        """
        This build database from CSV data.
        At the time I'm writing this, SierraChart Historical CSV data is:
        [Date, Time, Open, High, Low, Last, Volume, Number of Trades, Bid Volume, Ask Volume]
        and I took [Date, Time, Last, Volume] to build the DB.
        Remember to gen a CSV where the end_datetime_offset is the first in online btce
        /trades to avoid duplicates because lack of TIDs consistency.
        """
        self.data = list()
        trade_id = 0
        file = open(filename, "r")
        file.readline() # skip first line
        while True:
            line = file.readline().split(",")
            trade_id += 1
            if not line:
                file.close()
                break
            else:
                dateslist = line[0].split("/") + line[1].split(":")
                date_time = tuple([int(x) for x in dateslist])
                epoch_datetime = int(datetime(*date_time).strftime("%s"))
                self.jsonfeed['last'] = [{'date': epoch_datetime, 'price': float(line[5]),
                                  'amount':(float(line[6]) * 0.001), 'tid':trade_id}]
                self.dump_data_into_sql(self.jsonfeed['last'])

    def check_if_new_trades_available(self):
        trades = self.imp_json(params="/1")
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
        self.jsonfeed['last'] = self.imp_json(params="/2000", timeout=30)
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
            ticker = self.imp_json(url_override="{0}".format(self.tickerurl))
            output = dict()
            output['bid'] = ticker['ticker']['buy']
            output['ask'] = ticker['ticker']['sell']
            output['high'] = ticker['ticker']['high']
            output['low'] = ticker['ticker']['low']
            output['last'] = ticker['ticker']['last']
            output['timestamp'] = ticker['ticker']['updated']
            output['volume'] = float("{0:.3f}".format(float(ticker['ticker']['vol_cur'])))
            self.ticker = output
        except (exceptions.SSLError, ValueError, exceptions.Timeout, exceptions.ConnectionError):
            self.ticker = {}