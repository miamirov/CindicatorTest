import json
import pytest
import websockets

from contextlib import AbstractAsyncContextManager
import datetime
from main import parse_message, handle_socket, get_moving_average
from freezegun import freeze_time


def test_parse_message():
    message = {
        "e": "kline",
        "E": 123456789,
        "s": "BNBBTC",
        "k": {
            "t": 123400000,
            "T": 123460000,
            "s": "BNBBTC",
            "i": "1m",
            "f": 100,
            "L": 200,
            "o": "0.0010",
            "c": "0.0020",
            "h": "0.0025",
            "l": "0.0015",
            "v": "1000",
            "n": 100,
            "x": False,
            "q": "1.0000",
            "V": "500",
            "Q": "0.500",
            "B": "123456",
        }
    }

    symbol, close_price, is_closed = parse_message(message)

    assert symbol == "BNBBTC"
    assert close_price == 0.0020
    assert is_closed is False


@pytest.mark.parametrize('prices, size, expected', [([1, 2, 3, 4, 5], 2, 4.5), ([3, 2, 1], 3, 2), ([1], 2, 1)])
def test_get_moving_average(prices, size, expected):
    assert get_moving_average(size, prices) == expected


def gen_message(price, is_closed):
    message = {
        "s": "test_pair",
        "k": {
            "x": is_closed,
            "c": price,
        }
    }
    return json.dumps(message)


def create_generator(prices, is_closed):
    async def generator(*args):
        for price, flag in zip(prices, is_closed):
            yield gen_message(price, flag)

    return generator


class WebSocketMock(AbstractAsyncContextManager):
    def __init__(self, *args, **kwargs):
        pass

    async def __aexit__(self, exc_type, exc_value, traceback):
        return None

    @classmethod
    async def __aiter__(cls):
        pass


@freeze_time(datetime.datetime(2021, 3, 2))
@pytest.mark.parametrize("prices,is_closed,size", [([1, 2, 3, 4, 5], [True] * 5, 2)])
@pytest.mark.asyncio
async def test_handle_socket_simple(monkeypatch, prices, is_closed, size, caplog):
    monkeypatch.setattr(WebSocketMock, '__aiter__', create_generator(prices, is_closed))
    monkeypatch.setattr(websockets, 'connect', WebSocketMock)
    await handle_socket("test_pair", size)
    messages = [record.message for record in caplog.records]
    assert messages == ['2021-03-02 00:00:00: Moving average for last 2 minutes for pair test_pair is 1.0',
                        '2021-03-02 00:00:00: Moving average for last 2 minutes for pair test_pair is 1.5',
                        '2021-03-02 00:00:00: Moving average for last 2 minutes for pair test_pair is 2.5',
                        '2021-03-02 00:00:00: Moving average for last 2 minutes for pair test_pair is 3.5',
                        '2021-03-02 00:00:00: Moving average for last 2 minutes for pair test_pair is 4.5',
                        ]


@freeze_time(datetime.datetime(2021, 3, 2))
@pytest.mark.asyncio
async def test_only_closed_kindle_handled(monkeypatch, caplog):
    size = 2
    prices = [1, 2, 3]
    is_closed = [True, False, True]
    monkeypatch.setattr(WebSocketMock, '__aiter__', create_generator(prices, is_closed))
    monkeypatch.setattr(websockets, 'connect', WebSocketMock)
    await handle_socket("test_pair", size)
    messages = [record.message for record in caplog.records]
    assert messages == ['2021-03-02 00:00:00: Moving average for last 2 minutes for pair test_pair is 1.0',
                        '2021-03-02 00:00:00: Moving average for last 2 minutes for pair test_pair is 2.0',
                        ]
