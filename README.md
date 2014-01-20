I'll never finish this so, this is now public before it's completed. This is the old readme with roadmap:


DAT MARKET backend -
------------------

8.11.13:

- Exchangers: Cryptsy, BitFinex, BitStamp, BTC-e, TRT
- Layout: 2 childs, sockets by gevent.


Roadmap:

-9/11 - Cryptsy orderbook & trades by public API
-9/11 - Tickers on json_server
-9/11 - Trades on json_server
-9/11 - Childs monitor and respawn

-10/11 - Cryptsy orderbook fixed.
-10/11 - Sockets spawn fixed

-11/11 - Re-implement realtime candles builder as a child process. <- 12/11 Nope, fixed empty candles.
-11/11 - Weigted prices database table for each pair.
-11/11 - Volume p\l % over last week

-12/11 - catch exit codes and set max error retry before give up

-13-14/11 unit tests

-15/11 - Daemon args
-15/11 - Database consistency pre-boot: new tables creation if arg --build_new_tables
-15/11 - Static cache

-16/11 - Email warnings on errors

-17~22/11 - SQLAlchemy + PostgreSQL

-23~26/11 - Financial instruments module

-27~29/11 OpenExchangeRate for multiple currency as parameter into jsons.

- Form Fiat Exchangerate
- Daemon console
- Trading API Module (Sandbox)
- Bitcoin-qt dialog API
