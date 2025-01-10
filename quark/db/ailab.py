#!/usr/bin/env python
#-*- coding:utf-8 -*-


import time
import pymongo
import pandas as pd

from datetime import datetime as dt
from datetime import timedelta as td

from quark.db.base import ClientBase


SRC_EVENT_MAP = {
    "PK": "bookTicker",
    "DP": "depthUpdate",
    "AT": "aggTrade",
}


class Client(ClientBase):

    def __init__(self):
        super().__init__()
        self.tickers = [f'{C}USD_PERP' for C in ['BTC', 'ETH', 'BNB', 'DOGE']]
        self.tickers += [f'{C}USDT' for C in ['BTC', 'ETH', 'BNB', 'DOGE']]
        self.dtypes = ['bar', 'tick', 'funding_rate']

    def list(self):
        print(f'>>>[INFO] supported tickers={self.tickers}!')
        print(f'>>>[INFO] supported data types={self.dtypes}!')

    def overview(self, dtype):
        dbo = self.db[f'{dtype}_overview']
        df = pd.DataFrame(dbo.find())
        return df

    def read(
        self, tickers, dtype='bar',
        start_time=None, end_time=None,
        sort=True, return_df=True, projection={'_id': 0}):
        # function body
        store = self.db[f'{dtype}_data']
        if len(tickers) == 1:
            query = {'symbol': tickers[0]}
        else:
            query = {'symbol':{"$in": tickers}}
        if start_time:
            query['datetime'] = {'$gte': pd.to_datetime(start_time)}
        if end_time:
            if 'datetime' in query:
                query['datetime']['$lte'] = pd.to_datetime(end_time)
            else:
                query['datetime'] = {'$lte': pd.to_datetime(end_time)}
        if sort:
            dat = store.find(query, projection).sort([('datetime', 1)])
        else:
            dat = store.find(query, projection)
        if return_df:
            dat = pd.DataFrame(dat)
        return dat

    def read2(
        self, tickers, source='PK',
        start_time=None, end_time=None,
        sort=True, return_df=True, projection={'_id': 0}):
        # function body
        store = self.db['um']
        event = SRC_EVENT_MAP[source]
        if len(tickers) == 1:
            query = {'e': event, 's': tickers[0]}
        else:
            query = {'e': event, 's':{"$in": tickers}}
        if start_time:
            query['E'] = {'$gte': pd.to_datetime(start_time).timestamp() * 1e3}
        if end_time:
            if 'E' in query:
                query['E']['$lte'] = pd.to_datetime(end_time).timestamp() * 1e3
            else:
                query['E'] = {'$lte': pd.to_datetime(end_time).timestamp() * 1e3}
        if sort:
            dat = store.find(query, projection).sort([('E', 1)])
        else:
            dat = store.find(query, projection)
        if return_df:
            dat = pd.DataFrame(dat)
        return dat
