__author__ = 'mn3monic'



### daemon_helper.py

inittools_flush_ohlcv_query = """CREATE TABLE IF NOT EXISTS `{0}` (
                                  `date` int(11) NOT NULL,
                                  `timeframe` varchar(3) NOT NULL,
                                  `open` float NOT NULL,
                                  `high` float NOT NULL,
                                  `low` float NOT NULL,
                                  `close` float NOT NULL,
                                  `volume` float NOT NULL,
                                  `id` int(11) NOT NULL AUTO_INCREMENT,
                                  PRIMARY KEY (`id`),
                                  UNIQUE KEY `id` (`id`),
                                  UNIQUE KEY `CLUSTERED` (`timeframe`,`date`)
                                ) ENGINE=InnoDB  DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;"""

save_ticker_to_database = """REPLACE INTO {0} (exchange,
                                                pair,
                                                bid,
                                                ask,
                                                high,
                                                low,
                                                last,
                                                timestamp,
                                                volume)
                            VALUES ('{1}',
                                    '{2}',
                                    {3[bid]},
                                    {3[ask]},
                                    {3[high]},
                                    {3[low]},
                                    {3[last]},
                                    {3[timestamp]},
                                    {3[volume]});"""

load_all_tickers_from_database = "SELECT * FROM `{0}`"
