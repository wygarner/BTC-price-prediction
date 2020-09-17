

import cbpro
import time
import datetime

import pandas as pd
import numpy as np

import urllib3
import certifi
import sqlite3


public_client = cbpro.PublicClient()
http= urllib3.PoolManager(cert_reqs= 'CERT_REQUIRED', ca_certs= certifi.where())

conn= sqlite3.connect('btc.db')


def get_oir(bids, asks):
    bid_side = 0
    ask_side = 0

    for x, y in bids.iterrows():
        bid_side += np.exp(-0.5 * x + 1) * y["size"]

    for x, y in asks.iterrows():
        ask_side += np.exp(-0.5 * x + 1) * y["size"]

    order_imbalance_ratio = (bid_side - ask_side) / (bid_side + ask_side)

    return order_imbalance_ratio


def get_voi(bids, asks):
    bid_side = 0
    ask_side = 0

    for x, y in bids.iterrows():
        bid_side +=y["size"]

    for x, y in asks.iterrows():
        ask_side +=y["size"]

    volume_order_imbalance = bid_side - ask_side

    return volume_order_imbalance


trades_gen = public_client.get_product_trades(product_id='BTC-USD')
trades = list(trades_gen)

for trade in trades[:-1]:
    trade_df = pd.DataFrame(trade,index=[0])
   
    trade_df.to_sql('trades',conn,if_exists='append')


while True:
    
    orderbook = public_client.get_product_order_book('BTC-USD',level=2)
    decimal_time = time.time()
    timestamp = datetime.datetime.now()
    
    try:
        bids = pd.DataFrame(orderbook['bids'],columns=['price','size','order_count'])
        bids['price'] = pd.to_numeric(bids['price'])
        bids['size'] = pd.to_numeric(bids['size'])
        bids['size'] = bids['size']*bids['order_count']
        bids['time_ms'] = decimal_time
        bids['timestamp'] = timestamp
        bids= bids.drop(columns=['order_count'])
        
        asks = pd.DataFrame(orderbook['asks'],columns=['price','size','order_count'])
        asks['price'] = pd.to_numeric(asks['price'])
        asks['size'] = pd.to_numeric(asks['size'])
        asks['size'] = asks['size']*asks['order_count']
        asks['time_ms'] = decimal_time
        asks['timestamp'] = timestamp
        asks= asks.drop(columns=['order_count'])
        
        oir = get_oir(bids,asks)
        voi = get_voi(bids,asks)
        spread = asks['price'].min() - bids['price'].max()
        midprice = bids['price'].max() + (spread/2)
        
        books_df = pd.DataFrame({'decimal_time':decimal_time,
                                  'timestamp':timestamp,'oir':oir,'voi':voi,
                                  'spread':spread,
                                  'midprice':midprice},index=[0])
        
        # books_df.to_sql('books',conn,if_exists='append')
        # print(books_df,'\n')
        # time.sleep(1)
        
        trades = public_client.get_product_trades(before=trades[0]['trade_id'] , product_id='BTC-USD')
        trades = (list(trades))

        for trade in trades[:-1]:
            trade_df = pd.DataFrame(trade,index=[0])
   
            trade_df.to_sql('trades',conn,if_exists='append')
        
        print(trade_df,'\n')
        
        
    except(KeyError):
        print('...','\n')