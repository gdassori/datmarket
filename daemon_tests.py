__author__ = 'mn3monic'
import unittest
import MySQLdb
from configs.base_config import database
from includes.daemon_funcs import *
from includes.daemon_helper import Ohlcv, Ticker
from includes.drivers_helper import Data_provider

class Tests(unittest.TestCase):

    def setUp(self):
        pass

    def test_ohlcv_instances(self):
        ohlcv_instances = set_ohlcv_instances()
        for instance in ohlcv_instances:
            instance = ohlcv_instances[instance]
            self.assertIsInstance(instance, Ohlcv)

    def test_ticker_instances(self):
        ticker_instances = set_ticker_instances()
        for instance in ticker_instances:
            instance = ticker_instances[instance]
            self.assertIsInstance(instance, Data_provider)

    def test_fetch_instances(self):
        fetch_instances = set_fetch_instances()
        for instance in fetch_instances:
            instance = fetch_instances[instance]
            self.assertIsInstance(instance, Data_provider)

    def test_tickers_availability(self):
        tickers_main_instance = Ticker()
        tickers = tickers_main_instance.load_all_tickers_from_database()
        self.assertTrue(tickers)

    def test_exchangers(self):
        available_exchangers = build_exchangers_json_dict()
        self.assertTrue(available_exchangers)

    def test_pairs(self):
        available_pairs = build_pairs_json_dict()
        self.assertTrue(available_pairs)

    def test_database(self):
        db = MySQLdb.connect(user=database['user'], passwd=database['passwd'], db=database['db'])
        self.assertTrue(db)

if __name__ == '__main__':
    unittest.main()