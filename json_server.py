# -*- coding: utf-8 -*-

from flask import Flask, jsonify, send_file
from flask import request
from includes.json_server_funcs import *

app = Flask(__name__)
app.config.update(JSONIFY_PRETTYPRINT_REGULAR=False)

@app.route("/ohlcv/<pair>/<exchangername>/<timeframe>")

def quote(pair, exchangername, timeframe):
    """
    if exchangername not in jsons:
        return ""
    if pair not in jsons[exchangername]:
        return ""
    """
    limit = request.args.get('limit')
    table = exchangername + "_" + pair
    if limit:
        if limit.isdigit():
            return jsonify(load_ohlcv_from_database(table, limit=limit, timeframe=timeframe))
        else:
            return ""
    else:
        return jsonify(load_ohlcv_from_database(table, timeframe=timeframe))

@app.route("/line/<pair>/<exchangername>/<timeframe>")

def line(pair, exchangername, timeframe):
    """
    if exchangername not in jsons:
        return ""
    if pair not in jsons[exchangername]:
        return ""
    """
    limit = request.args.get('limit')
    table = exchangername + "_" + pair
    if limit:
        if limit.isdigit():
            return jsonify(load_closeline_from_database(table, limit=limit, timeframe=timeframe))
        else:
            return ""
    else:
        return jsonify(load_closeline_from_database(table, timeframe=timeframe))

@app.route("/trades/<pair>/<exchangername>")

def trades(pair, exchangername):
    """
    if exchangername not in jsons:
        return ""
    if pair not in jsons[exchangername]:
        return ""
    """
    limit = request.args.get('limit')
    table = exchangername + "_" + pair
    if limit:
        if limit.isdigit():
            return jsonify(load_trades_from_database(table, limit=limit))
        else:
            return ""
    else:
        return jsonify(load_trades_from_database(table))

@app.route("/orderbook/<pair>/<exchange>/")
def send_pic(exchange, pair):
    return send_file('feeds/{0}_{1}_orderbook.json'.format(exchange, pair))


@app.route("/available_exchangers")
def send_available_exchangers():
    return send_file("feeds/available_exchangers.json")

@app.route("/available_pairs")
def send_available_pairs():
    return send_file("feeds/available_pairs.json")

@app.route("/tickers/<pair>/<exchange>")
def send_ticker(exchange, pair):
    return jsonify(load_ticker_from_database(exchange, pair))

@app.route("/tickers")
def send_all_tickers():
    return jsonify(load_all_tickers_from_database())

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=1666)
