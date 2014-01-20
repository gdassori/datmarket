from __future__ import absolute_import, print_function
import time
from includes.daemon_funcs import *
from includes.daemon_helper import Ticker
init_ohlcv = False
rebuild_ohlcv_indexes = False
start_collecting_cycle = True
max_concurrent_ohlcv_resume_instances = 1
from tendo import singleton
me = singleton.SingleInstance()

if __name__ == "__main__":
    if rebuild_ohlcv_indexes:
        ohlcv_instances = set_ohlcv_instances()
        for instance in ohlcv_instances:
            instance = ohlcv_instances[instance]
            instance.flush_ohlcv_table()


    if init_ohlcv:
        print(int(time.time()), "-", "(re)building ohlcvs.")
        fetch_ohlcv(max_instances=max_concurrent_ohlcv_resume_instances)


    if start_collecting_cycle:
        tickers_process = Process(target=ticker_child)
        tickers_process.start()
        orderbooks_process = Process(target=orderbook_child)
        orderbooks_process.start()
        tickers_main_instance = Ticker()
        main_tickers = tickers_main_instance.load_all_tickers_from_database()
        available_exchangers = build_exchangers_json_dict()
        save_available_exchangers_json(available_exchangers)
        available_pairs = build_pairs_json_dict()
        save_available_pairs_json(available_pairs)

        fetch_instances = set_fetch_instances()
        jobs = list()
        while True:
            new_main_tickers = tickers_main_instance.load_all_tickers_from_database()
            queue = set_fetch_queue(main_tickers, new_main_tickers)
            main_tickers = new_main_tickers
            if queue:
                print(int(time.time()), "-", "fetch queue (trades): ", queue)
            for instance in queue:
                jobs.append(gevent.spawn(data_fetch, fetch_instances[instance]))
            gevent.joinall(jobs, timeout=30)
            pairs_queued = set(pair.split("_")[-1] for pair in queue)
            if not tickers_process.is_alive():
                tickers_process = Process(target=ticker_child)
                tickers_process.start()
            if not orderbooks_process.is_alive():
                orderbooks_process = Process(target=orderbook_child).start()
                orderbooks_process.start()
            time.sleep(5)
