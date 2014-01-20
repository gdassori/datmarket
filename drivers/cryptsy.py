from __future__ import print_function, absolute_import
from datetime import datetime

from configs.base_config import database
from includes.drivers_helper import Data_provider

class Cryptsy(Data_provider):
    def __init__(self, url, orderbookurl, tickerurl, name, pair):
        Data_provider.__init__(self, url, orderbookurl, tickerurl, name, pair, database)
        self.new_trades_available = None
        self.orderbookurl = orderbookurl
        self.publickey = "your cryptsy public key here"
        self.secretkey = "your cryptsy secret key here"
        self.url = url
        self.pair = pair
        self.markets = {"colxpm": 110, "redltc": 87, "nvcxbt": 13, "lk7xbt": 116, "amcxbt": 43,
                        "capxbt": 53, "emdxbt": 69, "centltc": 97, "ltcxbt": 3, "xjoxbt": 115, "rycltc": 37,
                        "gmeltc": 84, "ifcxpm": 105, "frcxbt": 39, "xpmxbt": 63, "hbnxbt": 80, "jkcltc": 35,
                        "adtxpm": 113, "ftcxbt": 5, "floltc": 61, "cscxbt": 68, "ezcltc": 55, "ascxpm": 112,
                        "btbxbt": 23, "kgcxbt": 65, "cprltc": 91, "tekxbt": 114, "glcxbt": 76, "yacxbt": 11,
                        "glxxbt": 78, "phsxbt": 86, "mstltc": 62, "nrbxbt": 54, "ascltc": 111, "gldxbt": 30,
                        "colltc": 109, "crcxbt": 58, "elcxbt": 12, "dblltc": 46, "nblxbt": 32, "gdcxbt": 82,
                        "sptxbt": 81, "btexbt": 49, "cgbxbt": 70, "dgcxbt": 26, "ybcxbt": 73, "bqcxbt": 10,
                        "alfxbt": 57, "tixxpm": 103, "ixcxbt": 38, "mncxbt": 7, "memltc": 56, "bukxbt": 102,
                        "fstxbt": 44, "adtltc": 94, "qrkxbt": 71, "cmcxbt": 74, "netltc": 108, "wdcxbt": 14,
                        "wdcltc": 21, "tagxbt": 117, "mecxbt": 45, "pxcltc": 101, "sxcltc": 98, "argxbt": 48,
                        "xncltc": 67, "yacltc": 22, "tixltc": 107, "zetxbt": 85, "lkyxbt": 34, "srcxbt": 88,
                        "clrxbt": 95, "ppcxbt": 28, "frkxbt": 33, "sbcxbt": 51, "netxpm": 104, "trcxbt": 27,
                        "btgxbt": 50, "dmdxbt": 72, "elpltc": 93, "ancxbt": 66, "ifcltc": 60, "xpmltc": 106,
                        "nmcxbt": 29, "gldltc": 36, "pycxbt": 92, "necxbt": 90, "mecltc": 100, "pxcxbt": 31,
                        "dvcltc": 52, "dgcltc": 96}

    def fetch_trades(self):
        trades_list = []
        trades = self.imp_json(url_override=self.url + str(self.markets[self.pair]))
        for coin in trades['return']['markets']:
            recent_trades = trades['return']['markets'][coin]['recenttrades']
        for index, trade in enumerate(recent_trades):
            date = recent_trades[index]['time']
            date = date.split("-")
            date = date[:2] + date[2].split(" ")
            date = date[:3] + date[3].split(":")
            date = [int(x) for x in date]
            epochdate = int(datetime(*date).strftime('%s'))
            price = float(recent_trades[index]['price'])
            amount = float(recent_trades[index]['quantity'])
            tid = int(recent_trades[index]['id'])
            trades_list.append({'tid':tid, 'date':epochdate, 'price':price, 'amount':amount})
        return trades_list

    def get_orderbook(self):
        temp_dict, orderbook_dict = {}, {}
        orderbook_dict['bids'] = []
        orderbook_dict['asks'] = []
        orderbook = self.imp_json(url_override=self.orderbookurl + str(self.markets[self.pair]))
        for coin in orderbook['return']:
            temp_dict['bids'] = orderbook['return'][coin]['sellorders']
            temp_dict['asks'] = orderbook['return'][coin]['buyorders']
        vol_sum = []
        for index, order in enumerate(temp_dict['bids']):
            price = temp_dict['bids'][index]['price']
            vol = temp_dict['bids'][index]['quantity']
            vol_sum.append(float(vol))
            orderbook_dict['bids'].append([float(price), float(vol), sum(vol_sum)])
        vol_sum = []
        for index, order in enumerate(temp_dict['asks']):
                    price = temp_dict['asks'][index]['price']
                    vol = temp_dict['asks'][index]['quantity']
                    vol_sum.append(float(vol))
                    orderbook_dict['asks'].append([float(price), float(vol), sum(vol_sum)])
        self.orderbook = orderbook_dict

    def check_if_new_trades_available(self):
        trades = self.fetch_trades()
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
        self.jsonfeed['last'] = self.fetch_trades()
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

    def get_ticker(self):
        output = {}
        ticker = self.imp_json(url_override="{0}{1}".format(self.tickerurl, self.markets[self.pair]), timeout=30)
        self.test_ticker = ticker
        prices_last_trades = []
        if ticker['return']['markets']:
            for dictionary in ticker['return']['markets']:
                last_trade_time = ticker['return']['markets'][dictionary]['lasttradetime']
                last_trade_time = last_trade_time.split("-")
                last_trade_time = last_trade_time[:2] + last_trade_time[2].split(" ")
                last_trade_time = last_trade_time[:3] + last_trade_time[3].split(":")
                last_trade_time = [int(x) for x in last_trade_time]
                epochdate_last_trade_time = int(datetime(*last_trade_time).strftime('%s'))
                for key, item in enumerate(ticker['return']['markets'][dictionary]['recenttrades']):
                    prices_last_trades.append(float(ticker['return']['markets'][dictionary]['recenttrades'][key]['price']))
                lowprice = max(prices_last_trades)
                highprice = min(prices_last_trades)
                output['bid'] = float(ticker['return']['markets'][dictionary]['buyorders'][0]['price'])
                output['ask'] = float(ticker['return']['markets'][dictionary]['sellorders'][0]['price'])
                output['high'] = highprice
                output['low'] = lowprice
                output['last'] = float(ticker['return']['markets'][dictionary]['lasttradeprice'])
                output['timestamp'] = epochdate_last_trade_time
                output['volume'] = float("{0:.3f}".format(float(ticker['return']['markets'][dictionary]['volume'])))
                self.ticker = output
        else:
            pass