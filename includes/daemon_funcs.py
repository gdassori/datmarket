from __future__ import print_function
from multiprocessing import Process
import time
import json

import gevent
from requests import exceptions
#from gevent import monkey

from includes.daemon_helper import Ohlcv, Ticker
from configs.drivers_config import data_sources


timeframes = {'m1': 60, 'm5': 300, 'm15': 900, 'm30': 1800, 'h1': 3600, 'h4': 14400, 'd1': 86400}

init_ohlcv = True

if init_ohlcv:

    # Step 1 - Setting up OHLCV instances
    def set_ohlcv_instances():
        ohlcv_instances = {}
        for instance in data_sources:
            for exchanger_pair in data_sources[instance]['pairs']:
                ohlcv_instances[exchanger_pair] = Ohlcv(exchanger_pair)
        return ohlcv_instances

    # Step 2 - Resuming / Initializing OHLCV
    def resume_ohlcv(instance):
        trades_dates = instance.find_dates(instance.trades_table)
        ohlcv_dates = instance.find_dates(instance.ohlcv_table)
        end_point = trades_dates[1]
        begin_point = ohlcv_dates[1]
        if not begin_point:
            begin_point = 0
        if begin_point < trades_dates[0]:
            begin_point = trades_dates[0]
        trades_raw = instance.load_trades_in_range(begin_point, end_point)
        if trades_raw:
            new_candles = instance.build_ohlcv(trades_raw)
            instance.save_ohlcv_to_database(new_candles)


    # Step 2.5 - OHLCV Multiprocessing
    def fetch_ohlcv(max_instances=2):
        ohlcv_instances = set_ohlcv_instances()
        running_instances, already_run, launched_instances = {}, [], []
        queue = list(ohlcv_instances.viewkeys())
        max_concurrent_ohlcv_resume_instances = max_instances
        while queue or launched_instances:
            for instance_name in ohlcv_instances:
                if instance_name in queue:
                    if instance_name not in already_run:
                        if len(launched_instances)   < max_concurrent_ohlcv_resume_instances:
                            running_instances[instance_name] = Process(target=resume_ohlcv,
                                                                        args=(ohlcv_instances[instance_name],),
                                                                        name=instance_name)
                            running_instances[instance_name].start()
                            print(int(time.time()), "-", "(re)building ohlcv:", instance_name)
                            launched_instances.append(instance_name)
                            already_run.append(instance_name)
                            queue.remove(instance_name)
            for instance_name in running_instances:
                if not running_instances[instance_name].is_alive():
                    if instance_name in launched_instances:
                        launched_instances.remove(instance_name)
                time.sleep(2)

    # Step 3 - Loading tickers from database
    def init_tickers():
        tickers_instance = Ticker()
        tickers = tickers_instance.load_all_tickers_from_database()
        return tickers

    # Step 4 - Setting up data-fetching instances:
    def set_fetch_instances():
        fetch_instances = {}
        for exchanger in data_sources:
            for instance_name in data_sources[exchanger]['pairs']:
                name, pair = instance_name.split("_")
                remote_pair = pair.replace("xbt", "btc")
                trades_url = data_sources[exchanger]['url']
                trades_url_param = data_sources[exchanger]['urlparam']
                orderbook_url = data_sources[exchanger]['orderbookurl']
                orderbook_url_param = data_sources[exchanger]['orderbookurlparam']
                ticker_url = data_sources[exchanger]['tickerurl']
                ticker_url_param = data_sources[exchanger]['tickerurlparam']
                add_pair_in_url = data_sources[exchanger]['add_pair_in_url']
                driver = data_sources[exchanger]['driver']
                if add_pair_in_url:
                    fetch_instances[instance_name] = driver(trades_url + remote_pair + trades_url_param,
                                                      orderbook_url + remote_pair + orderbook_url_param,
                                                      ticker_url + remote_pair + ticker_url_param,
                                                      name, pair)
                else:
                    fetch_instances[instance_name] = driver(trades_url +  trades_url_param,
                                                      orderbook_url +  orderbook_url_param,
                                                      ticker_url +  ticker_url_param,
                                                      name, pair)
        return fetch_instances

    def set_ticker_instances():
        ticker_instances = {}
        for exchanger in data_sources:
            for instance_name in data_sources[exchanger]['pairs']:
                name, pair = instance_name.split("_")
                remote_pair = pair.replace("xbt", "btc")
                trades_url = data_sources[exchanger]['url']
                trades_url_param = data_sources[exchanger]['urlparam']
                orderbook_url = data_sources[exchanger]['orderbookurl']
                orderbook_url_param = data_sources[exchanger]['orderbookurlparam']
                ticker_url = data_sources[exchanger]['tickerurl']
                ticker_url_param = data_sources[exchanger]['tickerurlparam']
                add_pair_in_url = data_sources[exchanger]['add_pair_in_url']
                driver = data_sources[exchanger]['driver']
                if add_pair_in_url:
                    ticker_instances["ticker_" + instance_name] = driver(trades_url + remote_pair + trades_url_param,
                                                      orderbook_url + remote_pair + orderbook_url_param,
                                                      ticker_url + remote_pair + ticker_url_param,
                                                      name, pair)
                else:
                    ticker_instances["ticker_" + instance_name] = driver(trades_url +  trades_url_param,
                                                      orderbook_url +  orderbook_url_param,
                                                      ticker_url +  ticker_url_param,
                                                      name, pair)
        return ticker_instances

    def set_orderbook_instances():
        orderbook_instances = {}
        for exchanger in data_sources:
            for instance_name in data_sources[exchanger]['pairs']:
                name, pair = instance_name.split("_")
                remote_pair = pair.replace("xbt", "btc")
                trades_url = data_sources[exchanger]['url']
                trades_url_param = data_sources[exchanger]['urlparam']
                orderbook_url = data_sources[exchanger]['orderbookurl']
                orderbook_url_param = data_sources[exchanger]['orderbookurlparam']
                ticker_url = data_sources[exchanger]['tickerurl']
                ticker_url_param = data_sources[exchanger]['tickerurlparam']
                add_pair_in_url = data_sources[exchanger]['add_pair_in_url']
                driver = data_sources[exchanger]['driver']
                if add_pair_in_url:
                    orderbook_instances["orderbook_" + instance_name] = driver(trades_url + remote_pair + trades_url_param,
                                                      orderbook_url + remote_pair + orderbook_url_param,
                                                      ticker_url + remote_pair + ticker_url_param,
                                                      name, pair)
                else:
                    orderbook_instances["orderbook_" + instance_name] = driver(trades_url +  trades_url_param,
                                                      orderbook_url +  orderbook_url_param,
                                                      ticker_url +  ticker_url_param,
                                                      name, pair)
        return orderbook_instances

    def fetch_tickers(fetch_instances):
        #gevent.monkey.patch_ssl()
        #gevent.monkey.patch_socket()
        jobs = [gevent.spawn(fetch_instances[instance].get_ticker) for instance in fetch_instances]
        gevent.joinall(jobs, timeout=30)
        ticker = {instance.replace("ticker_", ""): fetch_instances[instance].ticker for instance in fetch_instances}
        return ticker


    # Step 5 - Start comparing tickers
    def update_tickers(tickers, fetch_instances):
        new_tickers = fetch_tickers(fetch_instances)
        output_tickers = {}
        fresh_tickers = {}
        for ticker in new_tickers:
            if new_tickers[ticker]:
                output_tickers[ticker] = new_tickers[ticker]
            else:
                output_tickers[ticker] = tickers[ticker]
        for ticker in output_tickers:
            if output_tickers[ticker] != tickers[ticker]:
                fresh_tickers[ticker] = output_tickers[ticker]
        return fresh_tickers, output_tickers

    def get_new_tickers(fetch_instances):
        tickers = fetch_tickers(fetch_instances)
        return tickers

    def set_fetch_queue(old_tickers, tickers):
        fetch_queue = []
        for ticker in tickers:
            if ticker.split("_")[0] == 'trt':
                if old_tickers[ticker]['timestamp'] != tickers[ticker]['timestamp']:
                    fetch_queue.append(ticker)
            else:
                if old_tickers[ticker]['volume'] != tickers[ticker]['volume']:
                    fetch_queue.append(ticker)
        return fetch_queue

    def queue_semaphore(fetch_queue, max_instances):
        if len(fetch_queue) < max_instances:
            return True
        else:
            return False

    def data_fetch(fetch_instance):
        #gevent.monkey.patch_ssl()
        #gevent.monkey.patch_socket()
        instance = fetch_instance
        name_pair = fetch_instance.name + "_" + fetch_instance.pair
        instance.set_max_tid()
        instance.find_dates()
        try:
            instance.check_if_new_trades_available()
        except(exceptions.ConnectionError, exceptions.SSLError, exceptions.Timeout, ValueError):
            print(int(time.time()), "-", name_pair + " : [df:108] connection error while checking new available trades")
        if instance.new_trades_available:
            try:
                instance.get_trades()
            except(exceptions.ConnectionError, exceptions.SSLError, exceptions.Timeout, ValueError):
                print(int(time.time()), "-", name_pair + " : [df:193] connection error while getting trades")
                return
            init_time = int(time.time())
            instance.update_database()
            instance.build_ohlcv_candles_from_new_trades(instance.jsonfeed['news'])
            instance.update_ohlcv_candles()
            instance.save_updated_ohlcv_candles_to_database()
            end_time = int(time.time()) - init_time
            print(int(time.time()), "-", name_pair + " : job done in", end_time, "seconds")
        else:
            print(int(time.time()), "-", name_pair + " : no new trades available")


    def fetch_orderbooks(fetch_instances):
        jobs = [gevent.spawn(fetch_instances[instance].update_orderbook) for instance in fetch_instances]
        print(int(time.time()), "-", "fetching orderbooks...")
        gevent.joinall(jobs, timeout=30)


    def update_candles(candles_instances):
        jobs = [gevent.spawn(candles_instances[instance].new_candles_builder) for instance in candles_instances]
        gevent.joinall(jobs, timeout=30)


    def get_ticker(fetch_instances, fetch_instance):
        instance = fetch_instances[fetch_instance]
        instance.get_ticker()
        instance.save_ticker_to_database()

    def build_exchangers_json_dict():
        json_list_exchangers = dict()
        for container in data_sources:
            for exchanger_pair in data_sources[container]['pairs']:
                exchangerpairtuple = exchanger_pair.split("_")
                json_list_exchangers[exchangerpairtuple[0]] = []
            for exchanger_pair in data_sources[container]['pairs']:
                exchangerpairtuple = exchanger_pair.split("_")
                json_list_exchangers[exchangerpairtuple[0]].append(exchangerpairtuple[1])
        return json_list_exchangers

    def save_available_exchangers_json(available_exchangers):
        with open("feeds/available_exchangers.json", 'wb') as pointer:
            json.dump(available_exchangers, pointer)

    def build_pairs_json_dict():
        json_list_pairs = dict()
        for container in data_sources:
            for exchanger_pair in data_sources[container]['pairs']:
                exchangerpairtuple = exchanger_pair.split("_")
                json_list_pairs[exchangerpairtuple[1]] = []
        for container in data_sources:
            for exchanger_pair in data_sources[container]['pairs']:
                exchangerpairtuple = exchanger_pair.split("_")
                json_list_pairs[exchangerpairtuple[1]].append(exchangerpairtuple[0])
        return json_list_pairs

    def save_available_pairs_json(available_pairs):
            with open("feeds/available_pairs.json", 'wb') as pointer:
                json.dump(available_pairs, pointer)

    def ticker_child():
        #gevent.monkey.patch_ssl()
        #gevent.monkey.patch_socket()
        print(int(time.time()), "-", "setting up tickers instances...")
        ticker_instances = set_ticker_instances()
        print(int(time.time()), "-", "fetching new tickers...")
        tickers = get_new_tickers(ticker_instances)
        local_ticker_instance = Ticker()
        while True:
            print(int(time.time()), "-", "updating tickers...")
            fresh_tickers, tickers = update_tickers(tickers, ticker_instances)
            local_ticker_instance.save_all_tickers_to_database(fresh_tickers)
            time.sleep(2)

    def orderbook_child():
        #gevent.monkey.patch_ssl()
        #gevent.monkey.patch_socket()
        print(int(time.time()), "-", "setting up orderbooks instances...")
        orderbook_instances = set_orderbook_instances()
        while True:
            fetch_orderbooks(orderbook_instances)
            time.sleep(2)
