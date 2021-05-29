import datetime

import requests
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json

url = 'https://www.binance.com/api/v1/klines'
params = {
      'symbol': 'EGLDUSDT',
      'interval': '1m',
      'limit': 1000,
      'startTime': 1598832000000
    }
save = {}

try:
    resp = requests.get(url, params)
    tmp_data = resp.json()
    while len(tmp_data):
        for data in tmp_data:
            t = datetime.datetime.utcfromtimestamp(data[6]/1000)
            if t.year not in save:
                save[t.year] = {}
            if t.month not in save[t.year]:
                save[t.year][t.month] = {}
            if t.day not in save[t.year][t.month]:
                save[t.year][t.month][t.day] = {}
            if t.hour not in save[t.year][t.month][t.day]:
                save[t.year][t.month][t.day][t.hour] = {}
            save[t.year][t.month][t.day][t.hour][t.minute] = data
            print(t, data[1])


        params['startTime'] = tmp_data[-1][6]
        resp = requests.get(url, params)
        tmp_data = resp.json()

except (ConnectionError, Timeout, TooManyRedirects) as e:
    print(e)
with open("EGLDUSD", "w") as fp:
    json.dump(save, fp, indent=4)