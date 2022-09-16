import json
import re

from kafka import KafkaConsumer

from utils import text_cleaner

from hazm import Normalizer

normalizer = Normalizer()

if __name__ == '__main__':
    consumer = None
    try:
        consumer = KafkaConsumer('raw_tweets', auto_offset_reset='earliest', bootstrap_servers=['localhost:9092'])
    except Exception as e:
        print('Exception while connecting Kafka')
        print(str(e))
    else:
        idx = 0 
        for msg in consumer:
            raw_tweet = msg.value
            tweet = json.loads(raw_tweet)
            text = tweet['text']
            # clean text 
            text = text_cleaner(text)
            # normalize text 
            text = normalizer.normalize(text)

            if len(text) > 100:
                with open('tweets_dataset.txt', 'a') as f:
                    f.write(text)
                    f.write('\n')

                idx = idx + 1
            if idx == 500:
                break