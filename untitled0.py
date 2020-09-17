

import pandas as pd
import numpy as np

import bitmex

import sqlite3

from joblib import load

api_key = "-iXO_17idCPYYQgGsRtB7HfI"
api_secret = "yod97ok0bHhrdumSb4XnF0KDAg1LH0QciHkVyHb3sF0z0Y8J"

client = bitmex.bitmex(test=True, api_key=api_key, api_secret=api_secret)

conn = sqlite3.connect("btc.db")
c = conn.cursor()

model = load("BTC_100m_forecast.joblib")

def get_midprice():
    result = client.Instrument.Instrument_get(symbol="XBTUSD", reverse=True, count=1).result()
    midprice = result[0][0]["midPrice"]
    return midprice

def place_limit_order(quantity, price):
    client.Order.Order_new(symbol="XBTUSD", orderQty=quantity, price=price).result()
    
def place_stop_loss(quantity, price):
    client.Order.Order_new(symbol="XBTUSD", ordType="StopLimit", orderQty=quantity, stopPx=price, price=price).result()
    
def place_take_profit(quantity, price):
    client.Order.Order_new(symbol="XBTUSD", ordType="LimitIfTouched", orderQty=quantity, price=price).result()

def update_leverage(leverage):
    client.Position.Position_updateLeverage(symbol="XBTUSD", leverage=leverage).result()
    
def get_position():
    return client.Position.Position_get().result()

def place_orders(direction):
    current_price = get_midprice()
    if direction=="long":
        place_limit_order(1500, (current_price + (current_price*0.0025)))
        place_stop_loss(-1500, (current_price - (current_price*0.01)))
        place_take_profit(-1500, (current_price + (current_price*0.02)))
    else:
        place_limit_order(-1500, (current_price - (current_price*0.0025)))
        place_stop_loss(1500, (current_price + (current_price*0.01)))
        place_take_profit(-1500, (current_price - (current_price*0.02)))
        

