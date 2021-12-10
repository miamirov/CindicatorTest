import asyncio
import json
import logging
import sys
import websockets

from datetime import datetime
from urllib.parse import urljoin

BASE = "wss://stream.binance.com:9443/ws/"
SYMBOLS = ['btcusdt', 'ethusdt', 'bnbbtc']
SIZE = 20


async def handle_socket(pair: str, size: int):
    async with websockets.connect(urljoin(BASE, f'{pair}@kline_1m')) as websocket:
        prices = []
        log = logging.getLogger(f"{pair}_logger")
        log.setLevel(logging.INFO)
        file_handler = logging.StreamHandler(sys.stdout)
        basic_formater = logging.Formatter('%(message)s')
        file_handler.setFormatter(basic_formater)
        log.addHandler(file_handler)
        async for message in websocket:
            json_message = json.loads(message)
            symbol, close_price, is_closed = parse_message(json_message)
            if is_closed:
                prices.append(close_price)
                log.info(
                    f'{datetime.now()}: Moving average for last {size} minutes'
                    f' for pair {symbol} is {get_moving_average(size, prices)}')


def parse_message(json_message):
    symbol = json_message['s']
    candlestick = json_message['k']
    close_price = float(candlestick['c'])
    is_closed = candlestick['x']
    return symbol, close_price, is_closed


def get_moving_average(size, prices):
    if size > len(prices):
        return sum(prices) / len(prices)
    return sum(prices[-size:]) / size


async def handler():
    await asyncio.wait([handle_socket(pair, SIZE) for pair in SYMBOLS])


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(handler())
