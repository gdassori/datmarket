import unittest

import configs.drivers_config

class Tests(unittest.TestCase):
    def setUp(self):
        pass

    def test_init_class_instances(self):
        for source in configs.drivers_config.data_sources:
            trades_url = configs.drivers_config.data_sources[source]['url']
            ticker_url =  configs.drivers_config.data_sources[source]['tickerurl']
            orderbook_url = configs.drivers_config.data_sources[source]['orderbookurl']
            driver = configs.drivers_config.data_sources[source]['driver']
            for exchange_pair in configs.drivers_config.data_sources[source]['pairs']:
                name, pair = exchange_pair.split("_")
                instance = driver(trades_url, ticker_url, orderbook_url, name, pair)
                self.assertIsInstance(instance, driver)

    def test_imp_json_method(self):
        for source in configs.drivers_config.data_sources:
            if configs.drivers_config.data_sources[source]['add_pair_in_url']:
                url_param = configs.drivers_config.data_sources[source]['urlparam']
                ob_url_param = configs.drivers_config.data_sources[source]['orderbookurlparam']
                tk_url_param = configs.drivers_config.data_sources[source]['tickerurlparam']
                trades_url = configs.drivers_config.data_sources[source]['url']
                ticker_url =  configs.drivers_config.data_sources[source]['tickerurl']
                orderbook_url = configs.drivers_config.data_sources[source]['orderbookurl']
                driver = configs.drivers_config.data_sources[source]['driver']
                for exchange_pair in configs.drivers_config.data_sources[source]['pairs']:
                    name, pair = exchange_pair.split("_")
                    remote_pair = pair.replace("xbt", "btc")
                    instance = driver(trades_url+remote_pair+url_param, ticker_url+remote_pair+tk_url_param,
                                      orderbook_url+remote_pair+ob_url_param, name, pair)
                    test_feed = instance.imp_json()
                    self.assertTrue(test_feed)
            else:
                trades_url = configs.drivers_config.data_sources[source]['url']
                ticker_url =  configs.drivers_config.data_sources[source]['tickerurl']
                url_param = configs.drivers_config.data_sources[source]['urlparam']
                ob_url_param = configs.drivers_config.data_sources[source]['orderbookurlparam']
                tk_url_param = configs.drivers_config.data_sources[source]['tickerurlparam']
                orderbook_url = configs.drivers_config.data_sources[source]['orderbookurl']
                driver = configs.drivers_config.data_sources[source]['driver']
                for exchange_pair in configs.drivers_config.data_sources[source]['pairs']:
                    name, pair = exchange_pair.split("_")
                    instance = driver(trades_url+url_param, ticker_url+tk_url_param, orderbook_url+ob_url_param,
                                      name, pair)
                    test_feed = instance.imp_json()
                    self.assertTrue(test_feed)

    def test_dates_and_rawdata_in_db(self):
        for source in configs.drivers_config.data_sources:
            if configs.drivers_config.data_sources[source]['add_pair_in_url']:
                url_param = configs.drivers_config.data_sources[source]['urlparam']
                ob_url_param = configs.drivers_config.data_sources[source]['orderbookurlparam']
                tk_url_param = configs.drivers_config.data_sources[source]['tickerurlparam']
                trades_url = configs.drivers_config.data_sources[source]['url']
                ticker_url =  configs.drivers_config.data_sources[source]['tickerurl']
                orderbook_url = configs.drivers_config.data_sources[source]['orderbookurl']
                driver = configs.drivers_config.data_sources[source]['driver']
                for exchange_pair in configs.drivers_config.data_sources[source]['pairs']:
                    name, pair = exchange_pair.split("_")
                    remote_pair = pair.replace("xbt", "btc")
                    instance = driver(trades_url+remote_pair+url_param, ticker_url+remote_pair+tk_url_param,
                                      orderbook_url+remote_pair+ob_url_param, name, pair)
                    instance.find_dates()
                    instance.load_data_in_range(instance.first_trade_time, instance.last_trade_time)
                    self.assertTrue(instance.rawdata)
                    self.assertGreater(instance.last_trade_time, instance.first_trade_time)
            else:
                trades_url = configs.drivers_config.data_sources[source]['url']
                ticker_url =  configs.drivers_config.data_sources[source]['tickerurl']
                url_param = configs.drivers_config.data_sources[source]['urlparam']
                ob_url_param = configs.drivers_config.data_sources[source]['orderbookurlparam']
                tk_url_param = configs.drivers_config.data_sources[source]['tickerurlparam']
                orderbook_url = configs.drivers_config.data_sources[source]['orderbookurl']
                driver = configs.drivers_config.data_sources[source]['driver']
                for exchange_pair in configs.drivers_config.data_sources[source]['pairs']:
                    name, pair = exchange_pair.split("_")
                    instance = driver(trades_url+url_param, ticker_url+tk_url_param, orderbook_url+ob_url_param,
                                      name, pair)
                    instance.find_dates()
                    instance.load_data_in_range(instance.first_trade_time, instance.last_trade_time)
                    self.assertTrue(instance.rawdata)
                    self.assertGreater(instance.last_trade_time, instance.first_trade_time)


if __name__ == '__main__':
    unittest.main()