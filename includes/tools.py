__author__ = 'mn3monic'

def flush_ohlcv():


    query_db = """CREATE TABLE IF NOT EXISTS `bitfinex_ltcusd_ohlcv` (
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
    ) ENGINE=InnoDB  DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;
        """