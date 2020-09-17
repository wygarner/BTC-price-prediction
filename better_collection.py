

import cbpro
import datetime
import time

import pandas as pd
import numpy as np

import sqlite3

import requests


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



def main(lag):
    count = 0

    public_client = cbpro.PublicClient()
    
    while True:
        if count==0:
            orderbook = public_client.get_product_order_book('BTC-USD',level=2)
            
            start_time = datetime.datetime.now()
            
            bids = pd.DataFrame(orderbook['bids'],columns=['price','size','order_count'])
            asks = pd.DataFrame(orderbook['asks'],columns=['price','size','order_count'])
            asks = asks.drop(columns=['price','order_count']).astype(float)
            bids = bids.drop(columns=['price','order_count']).astype(float)
            
            midprice = ""
            total_bids_diff= ""
            total_asks_diff = ""
            
            count+=1
            
        else:
            orderbook = public_client.get_product_order_book('BTC-USD',level=2)
            
            current_time = datetime.datetime.now()
            
            # print(current_time)
            # sys.exit()
            
            print('Seconds: ',(current_time - start_time).seconds)
            
            if (current_time - start_time).seconds == lag:
                oir = get_oir(total_bids_diff,total_asks_diff)
                voi = get_voi(total_bids_diff,total_asks_diff)
                
                trades = public_client.get_product_trades(after=start_time , product_id='BTC-USD')
                trades = pd.DataFrame(list(trades)[:-1])
                trades['time'] = pd.to_datetime(trades['time'])
                # trades.to_sql("all_trades", conn, if_exists='append')
                
                # print(trades)
                
                buys = trades.loc[trades['side'] == 'buy']
                sells = trades.loc[trades['side'] == 'sell']
                
                buy_quantity = buys['size'].astype(float).sum()
                sell_quantity = sells['size'].astype(float).sum()

                tfi = buy_quantity / sell_quantity
                
                features_d = {"time": current_time, "lag period": lag, 
                              "midprice": midprice, "oir": oir, "voi": voi, "tfi": tfi}
              
                features = pd.DataFrame(features_d,index=[0])
                print(features,"\n")
                features.to_sql("book_imbalances_lag_"+str(lag), conn, if_exists='append')
                print("`^*^`"*24,"\n")
                
                start_time = current_time + datetime.timedelta(seconds=1)
                
                count = 0
                
            else:
                try:
                    new_bids = pd.DataFrame(orderbook['bids'],columns=['price','size','order_count'])
                    new_asks = pd.DataFrame(orderbook['asks'],columns=['price','size','order_count'])
                except(KeyError):
                    print('Key Error','\n')
                    time.sleep(1)
                    public_client = cbpro.PublicClient()
                    orderbook = public_client.get_product_order_book('BTC-USD',level=2)
                    new_bids = pd.DataFrame(orderbook['bids'],columns=['price','size','order_count'])
                    new_asks = pd.DataFrame(orderbook['asks'],columns=['price','size','order_count'])
                
                spread = new_asks['price'].astype(float).min() - new_bids['price'].astype(float).max()
                midprice = new_bids['price'].astype(float).max() + (spread/2)
                
                new_asks = new_asks.drop(columns=['price','order_count']).astype(float)
                new_bids = new_bids.drop(columns=['price','order_count']).astype(float)

                bids_diff = new_bids.subtract(bids)
                asks_diff = new_asks.subtract(asks)
                
                try:
                    total_bids_diff = total_bids_diff.add(bids_diff)
                    total_asks_diff = total_asks_diff.add(asks_diff)
                except(AttributeError):
                    total_bids_diff = bids_diff
                    total_asks_diff = asks_diff
               
                bids = new_bids
                asks = new_asks
                
            count+=1
            print('  Count: ',count)
            if count>=lag + 5:
                return False
                
            time.sleep(1)


lag = 10
while True:
    try:
        run = main(lag)
        if run == False:
            print('Restarting...','\n')
            time.sleep(5)
            run = main(lag)
    except(requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError, ValueError):
        time.sleep(5)

