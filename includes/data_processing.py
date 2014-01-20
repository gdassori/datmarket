import MySQLdb
from configs.base_config import database
from includes.daemon_funcs import build_pairs_json_dict, build_exchangers_json_dict

class PairAnalysis():
    def __init__(self, pair):
        self.db = MySQLdb.connect(user=database['user'], passwd=database['passwd'], db=database['db'])
        self.cursor = self.db.cursor()
        self.pairs_dict = build_pairs_json_dict()
        self.exchangers_dict = build_exchangers_json_dict()
        self.pair = pair
        self.paired_exchangers = self.build_paired_exchangers_list()

    def build_paired_exchangers_list(self):
        paired_exchangers = [x + "_" + self.pair for x in self.pairs_dict[self.pair]]
        return paired_exchangers

    def load_market_data(self, timeframe, limit=1):
        close_prices = {'pair': '', 'market_data':{}}
        if int(limit):
            for data_source in self.paired_exchangers:
                db_query = """SELECT * FROM {0}_ohlcv
                                WHERE timeframe = '{1}'
                                ORDER BY date DESC
                                LIMIT {2}""".format(data_source, timeframe, limit)
                self.cursor.execute(db_query)
                data = self.cursor.fetchall()
                for row in data:
                    exchanger = data_source.split("_")[0]
                    try: close_prices['market_data'][row[0]]
                    except: close_prices['market_data'][row[0]] = {}
                    close_prices['market_data'][row[0]].update({exchanger: [row[2], row[3], row[4], row[5], row[6]]})
                close_prices['pair'] = self.pair
        return close_prices

    def build_weighted_ohlcv(self, close_prices):
        weighted = {}
        prices = {'o':[], 'h':[], 'l':[], 'c':[], 'v':[]}
        if close_prices['market_data']:
            for timestamp in close_prices['market_data']:
                for exchanger in close_prices['market_data'][timestamp]:
                    open_price = close_prices['market_data'][timestamp][exchanger][0]
                    high_price = close_prices['market_data'][timestamp][exchanger][1]
                    low_price = close_prices['market_data'][timestamp][exchanger][2]
                    close_price = close_prices['market_data'][timestamp][exchanger][3]
                    volume = close_prices['market_data'][timestamp][exchanger][4]
                    prices['o'].append(open_price)
                    prices['h'].append(high_price)
                    prices['l'].append(low_price)
                    prices['c'].append(close_price)
                    prices['v'].append(volume)
                howmany = len(prices['o'])
                weighted.update({timestamp: [sum(prices['o'])/ howmany, sum(prices['h'])/ howmany,
                                               sum(prices['l'])/ howmany, sum(prices['c'])/ howmany,
                                               sum(prices['v'])]})
        return weighted

