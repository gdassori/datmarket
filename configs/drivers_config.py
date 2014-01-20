# Copyright mn3monic @ freenode, 2013
# License: GPL (check GPL.txt for terms)

# this awesome simply config is all you have to setup to run a driver.


from drivers.trt import TRT
from drivers.bitfinex import BitFinex
from drivers.btce import BTCe
from drivers.bitstamp import BitStamp
from drivers.cryptsy import Cryptsy

data_sources = dict()


#######################################################################################
#   Only for Cryptsy, since they var their market availability often:

cryptsy_pairs = ("cryptsy_ltcxbt", "cryptsy_ftcxbt", "cryptsy_argxbt", "cryptsy_btbxbt",
                 "cryptsy_clrxbt", "cryptsy_nvcxbt", "cryptsy_ppcxbt", "cryptsy_xpmxbt",
                 "cryptsy_wdcltc", "cryptsy_xpmltc", "cryptsy_cprltc", "cryptsy_wdcxbt",
                 "cryptsy_yacxbt")

"""def get_cryptsy_pairs():
    available_cryptsy_pairs = list()
    temp_cryptsy = Cryptsy("https://www.cryptsy.com/api", "", "", "cryptsy", "ltcxbt")
    temp_cryptsy.save_marketslist_to_json()
    for pair in temp_cryptsy.markets:
        available_cryptsy_pairs.append("cryptsy_" + pair.replace("btc", "xbt"))
    return available_cryptsy_pairs

available_cryptsy_pairs = get_cryptsy_pairs()
cryptsy_running_pairs = []

for pair in cryptsy_pairs:
    if pair in available_cryptsy_pairs:
        cryptsy_running_pairs.append(pair)
"""
########################################################################################


data_sources['trt'] = {"url" : "https://www.therocktrading.com/api/trades/",
                       "urlparam" : "",
                       "orderbookurl": "https://www.therocktrading.com/api/orderbook/",
                       "orderbookurlparam": "",
                       "tickerurl": "https://www.therocktrading.com/api/ticker/",
                       "tickerurlparam": "",
                       "add_pair_in_url": True,
                       "driver" : TRT,
                       "split_remote_pair_with_underscore": False,
                       "pairs" : ("trt_xbteur", "trt_ltceur", "trt_ltcxbt"),
                       }

data_sources['bitfinex'] = {"url": "https://api.bitfinex.com/v1/trades/",   # ..url/pair/urlparam
                            "urlparam" : "",
                            "orderbookurl": "https://api.bitfinex.com/v1/book/",
                            "orderbookurlparam": "",
                            "tickerurl": "https://api.bitfinex.com/v1/ticker/",
                            "tickerurlparam": "",
                            "add_pair_in_url": True,
                            "driver": BitFinex, # class
                            "split_remote_pair_with_underscore": False,
                            "pairs": ("bitfinex_xbtusd", "bitfinex_ltcusd", "bitfinex_ltcxbt")
                        }

data_sources['btce'] = {"url": "https://btc-e.com/api/2/",
                        "urlparam" : "/trades",
                        "orderbookurl": "https://btc-e.com/api/2/",
                        "orderbookurlparam": "/depth",
                        "tickerurl": "https://btc-e.com/api/2/",
                        "tickerurlparam": "/ticker",
                        "add_pair_in_url": True,
                        "driver": BTCe,
                        "split_remote_pair_with_underscore": True,
                        "pairs": ("btce_xbtusd", "btce_ltcxbt", "btce_ltcusd", "btce_ftcxbt", "btce_nmcxbt",
                                  "btce_nmcusd", "btce_nvcxbt", "btce_nvcusd", "btce_trcxbt", "btce_xpmxbt",
                                  "btce_ppcxbt" )
                        }

data_sources['bitstamp'] = {"url" : "https://www.bitstamp.net/api/transactions/",
                            "urlparam" : "",
                            "orderbookurl": "https://www.bitstamp.net/api/order_book/",
                            "orderbookurlparam": "",
                            "tickerurl": "https://www.bitstamp.net/api/ticker/",
                            "tickerurlparam": "",
                            "ticker_update_value": "volume",
                            "add_pair_in_url": False,
                            "driver" : BitStamp,
                            "split_remote_pair_with_underscore": False,
                            "pairs" : ("bitstamp_xbtusd",)
                            }

data_sources['cryptsy'] = {"url" :  "http://pubapi.cryptsy.com/api.php?method=singlemarketdata&marketid=",
                           "urlparam" : "",
                           "orderbookurl": "http://pubapi.cryptsy.com/api.php?method=singleorderdata&marketid=",
                           "orderbookurlparam": "",
                           "tickerurl": "http://pubapi.cryptsy.com/api.php?method=singlemarketdata&marketid=",
                           "tickerurlparam": "",
                           "add_pair_in_url": False,
                           "driver" : Cryptsy,
                           "split_remote_pair_with_underscore": False,
                           "pairs": cryptsy_pairs
                            }