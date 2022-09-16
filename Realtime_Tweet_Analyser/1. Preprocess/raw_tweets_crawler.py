import time
import json
import tweepy
import uuid

from kafka import KafkaProducer

API_KEY = 'VJxBF1sdZFwY5mxIL5Bv06CCg'
API_SECRET_KEY = '7b7awTTK2uO2yjattiWsMpyuUc9cAvTQggdv97ENQ2sb4y7195'
ACCESS_TOKEN = '907259227118477313-UoWYxdiNMilzT0kHpmWRRdv5dSrZXJr'
ACCESS_TOKEN_SECRET = 'c2UG3iaDhqDfs4NVbwCYCRp7Iv9vnUf7vQF2NuoAq7p3Y'

class MyStreamListener(tweepy.StreamListener):
    def __init__(self, api, producer):
        self.api = api
        self.producer = producer
        self.backof_cnt = 0

    def on_data(self, data):
        tweet = json.loads(data)
        # filter out retweeted and quoted tweets
        if 'retweeted_status' not in tweet and 'quoted_status' not in tweet:
            self.backof_cnt += 1
            # extract needed data from tweet
            out = {}
            out['uuid'] = str(uuid.uuid4())
            out['id'] = tweet['id']
            out['id_str'] = tweet['id_str']
            out['created_at'] = tweet['created_at']
            out['text'] = tweet['extended_tweet']['full_text'] if tweet['truncated'] else tweet['text']
            out['user'] = {
                'id': tweet['user']['id'],
                'id_str': tweet['user']['id_str'], 
                'name': tweet['user']['name'], 
                'screen_name': tweet['user']['screen_name']
                }
            out['source'] = 'twitter'
            # send tweet to kafka
            try:
                key_bytes = bytes('raw', encoding='utf-8')
                value_bytes = bytes(json.dumps(out), encoding='utf-8')
                producer.send('raw_tweets', key=key_bytes, value=value_bytes)
                producer.flush()
                print('Tweet %s published successfully.' % (out['uuid']))
            except Exception as e:
                print('Exception in publishing tweet.')
                print(str(e))
            
        if self.backof_cnt == 100:
            self.backof_cnt = 0
            time.sleep(5)

    def on_error(self, error_code):
        print(error_code)

if __name__ == '__main__':
    auth = tweepy.OAuthHandler(API_KEY, API_SECRET_KEY)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    producer = None
    try:
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    except Exception as e:
        print('Exception while connecting Kafka')
        print(str(e))
    else:
        api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        if api.verify_credentials():
            track_list = []
            with open('track_list.txt') as f:
                track_list = [w.strip() for w in f.readlines()]
            
            # build stream
            tweets_listener = MyStreamListener(api, producer)
            stream = tweepy.Stream(api.auth, tweets_listener)
            stream.filter(languages=['fa'], track=track_list)
        else:
            print("Error creating Tweepy API")