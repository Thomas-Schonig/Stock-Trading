#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import alpaca_trade_api as tradeapi
import pandas as pd
import timeit
from datetime import datetime
from IPython.display import clear_output
import numpy as np


# In[2]:


APCA_API_BASE_URL = os.environ['APCA_API_PAPER_BASE_URL']
APCA_API_KEY_ID = os.environ['APCA_API_PAPER_KEY_ID']
APCA_API_SECRET_KEY = os.environ['APCA_API_PAPER_SECRET_KEY']
api = tradeapi.REST(APCA_API_KEY_ID,APCA_API_SECRET_KEY,APCA_API_BASE_URL)


# In[3]:


now = datetime.now()
print('*****   *****   PRICING SCRIPT STARTING   *****   *****')
print('SCRIPT STARTED AT: ',now)


# In[4]:


active_assets=api.list_assets(status='active')
symbols = [a.symbol for a in active_assets]
symbols_len = len(symbols)
print('Number of Securities in Tradable Universe:',symbols_len)


# In[5]:


print('Old Working Directory:',os.getcwd())
os.chdir('/media/tom/Data2/Alpha Hound/Equity Pricing/Daily Pricing')
print('New Working Directory:',os.getcwd())


# In[6]:


bar_resolution = '1D' #1Min, 5Min, 15Min, 1H, 1D
fails = []
c = 0
s = 0
start = timeit.default_timer()
time_elapsed = 0
minutes_elapsed = np.round((time_elapsed/60),decimals=2)
for symbol in symbols:
    file = ''
    lastdate = str(pd.Timestamp(year=2006,month=1,day=1).isoformat()+'-04:00') #+'-04:00' is necessary for get_barset to respect timestamp
    c +=1
    clear_output(wait=True)
    try:
        stop = timeit.default_timer()
        time_elapsed = stop-start
        minutes_elapsed = np.round((time_elapsed/60),decimals=2)
        percent = np.round((c)/len(symbols),decimals=5)*100
        expected_time = np.round((minutes_elapsed/percent*100),decimals=2)
        file = pd.read_csv('{0}.csv'.format(symbol)).set_index('time')
        lastd = file.index[-1]
        lastd = str(pd.Timestamp(lastd).isoformat())
        pricing_data = api.get_barset(symbol,bar_resolution,start=lastd).df
        pricing = pricing_data[symbol]
        file = file[:-1]
        update = pricing.loc[lastd:]
        file = file.append(update)
        file.to_csv('{0}.csv'.format(symbol))
        s += 1
    except:
        stop = timeit.default_timer()
        time_elapsed = stop-start
        minutes_elapsed = np.round((time_elapsed/60),decimals=2)
        percent = np.round((c)/len(symbols),decimals=5)*100
        expected_time = np.round((minutes_elapsed/percent*100),decimals=2)
        pricing_data = api.get_barset(symbol,bar_resolution,start=lastdate).df
        pricing_data[symbol].to_csv('{0}.csv'.format(symbol))
print('*****   *****   PRICING SCRIPT FINISHED   *****   *****')
print('FINISHED AT: ',datetime.now())
print('TOTAL RUNTIME: ',minutes_elapsed,'minutes')
print('SUCCESSFUL DATA UPDATES: ',s)
print('FAILED DATA PULLS: ',len(fails))
print(fails)


# In[ ]:




