import json
import argparse
import requests
from requests_oauthlib import OAuth1
import time
from tqdm import tqdm
import sys
import datetime
from datetime import timedelta
import pandas as pd
import csv

_auth = OAuth1(
    '6KPAiLBgVqUQWb7IIGS5WWf3a',
    'BkscbGABDd1Nzwojak2YGwTvRfXYfeo0hqj7Xfc0sV8VNNm2BS',
    '1256267455875305472-rdQ6D50Ie10GzvZTd31X6QcWC7bk7D',
    'GSLKFRUM4DRJ0F3CHToywmL6sKAmlD3PHUrOa6Bme379A'
)

users = "bbcnewsjapan,cnn_co_jp,BloombergJapan,HuffPostJapan,WSJJapan,ReutersJapan"
query = "from:"
for i in range(len(users.split(","))):
    if i == len(users.split(","))-1:
        query += users.split(",")[i]
    else:
        query = query + users.split(",")[i] + " OR from:"

df = pd.DataFrame(columns=['created_at', 'id_str', 'full_text', 'coordinates', 'retweet_count', 'favorite_count', 'lang'])
min_id = 0

while(True):
    now = datetime.datetime.now().date()
    time.sleep(5)
    params = {'q':query, 'count':100, 'lang':'ja'}
    f = open('%s.jsonl' % (now.isoformat()) ,'a')
    
    while(True):
        try:
            check_d =  datetime.datetime.now().date()
            if check_d != now:
                break
            time.sleep(120)
            
            stream = requests.get(
                "https://api.twitter.com/1.1/search/tweets.json?tweet_mode=extended",
                auth = _auth,
                params = params
            )
            jsons = stream.json()
            
            for i in range(len(jsons['statuses'])):
                try:
                    tweet = jsons['statuses'][i]
                    if i == 0:
                        stock_id = tweet['id']
                    if tweet['id'] <= min_id:
                        break
                    json.dump(tweet, f, ensure_ascii=False)
                    df = df.append({'created_at':tweet["created_at"], 'id_str':tweet["id_str"], 'full_text':tweet["full_text"], 'coordinates':tweet["coordinates"], 'retweet_count':tweet["retweet_count"], 'favorite_count':tweet["favorite_count"], 'lang':tweet["lang"]}, ignore_index=True)
                    f.write('\n')
                except json.decoder.JSONDecodeError:
                    print('error?')
                    continue
                    
            min_id = stock_id
        
        except requests.exceptions.ConnectionError:
            time.sleep(300)
        except Exception as e:
            print(e)
            break

    df.to_csv("%s.csv" % (now.isoformat()), encoding="utf-8-sig")
