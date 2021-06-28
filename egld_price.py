import datetime

import requests
from boto3.dynamodb.conditions import Key
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects

import boto3
import time
from datetime import datetime

url = 'https://www.binance.com/api/v1/klines'

save = {}
genesis = {
    'timestamp': 1596112200,
    'epoch': 0,
}

binance_listing = 1599102000

session = boto3.Session(profile_name='default')
dynamodb = session.resource('dynamodb', region_name='eu-west-1')
EGLDUSD = dynamodb.Table('EGLDUSD')
last_timestamp = 0

def getEpoch(timestamp):
    diff = timestamp - genesis['timestamp']
    return genesis['epoch'] + diff // (60 * 60 * 24)


def update_db(table, timestamp=binance_listing):
    global last_timestamp
    params = {
        'symbol': 'EGLDUSDT',
        'interval': '1m',
        'limit': 1000,
        'startTime': timestamp * 1000
    }
    try:
        resp = requests.get(url, params)
        tmp_data = resp.json()
        while len(tmp_data):
            for data in tmp_data:
                timestamp = data[6] // 1000
                price = (float(data[1]) + float(data[2]) + float(data[3]) + float(data[4])) / 4
                item = {'epoch': getEpoch(timestamp), 'timestamp': timestamp, 'price': '{:.2f}'.format(price)}
                table.put_item(Item=item)
                last_timestamp = timestamp
                print(getEpoch(timestamp), timestamp, '{:.2f}'.format(price))

            params['startTime'] = tmp_data[-1][6]
            resp = requests.get(url, params)
            tmp_data = resp.json()

    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)


def add_erd(table, timestamp=genesis['timestamp']):
    global last_timestamp
    params = {
        'symbol': 'ERDUSDT',
        'interval': '1m',
        'limit': 1000,
        'startTime': timestamp * 1000
    }
    try:
        resp = requests.get(url, params)
        tmp_data = resp.json()
        while len(tmp_data):
            if timestamp >= binance_listing:
                break
            for data in tmp_data:
                timestamp = data[6] // 1000
                if timestamp >= binance_listing:
                    break
                price = (float(data[1]) + float(data[2]) + float(data[3]) + float(data[4])) / 4
                price *= 1000
                item = {'epoch': getEpoch(timestamp), 'timestamp': timestamp, 'price': '{:.2f}'.format(price)}
                table.put_item(Item=item)
                last_timestamp = timestamp
                print(getEpoch(timestamp), timestamp, '{:.2f}'.format(price))

            params['startTime'] = tmp_data[-1][6]
            resp = requests.get(url, params)
            tmp_data = resp.json()

    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)


def main():
    global last_timestamp

    # if db is empty load all the data from genesis
    if EGLDUSD.item_count == 0:
        add_erd(EGLDUSD)
        update_db(EGLDUSD)
        try:
            with open('last_timestamp', 'w') as f:
                f.write(str(last_timestamp))
        except:
            print("Could not read file.")

    while True:
        try:
            with open('last_timestamp', 'r') as f:
                last_timestamp = int(f.readline())
        except:
            print("Could not read file.")
        update_db(EGLDUSD, last_timestamp + 1)
        try:
            with open('last_timestamp', 'w') as f:
                f.write(str(last_timestamp))
        except:
            print("Could not read file.")
        time.sleep(60)


def example():
    now = int(datetime.utcnow().timestamp())
    kce = Key('epoch').eq(getEpoch(now)) & Key('timestamp').between(now - 30, now + 30)
    timestamp = EGLDUSD.query(KeyConditionExpression=kce, ScanIndexForward=False, Limit=1)
    print(len(timestamp['Items']))
    print(timestamp['Items'])


if __name__ == '__main__':
    main()
