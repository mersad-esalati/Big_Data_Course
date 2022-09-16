import json
import datetime
import re

from kafka import KafkaConsumer

from elasticsearch import Elasticsearch

es = Elasticsearch([{'host':'localhost', 'port': 9200}])

if __name__ == '__main__':
    consumer = None
    try:
        consumer = KafkaConsumer('parsed_tweets', auto_offset_reset='earliest', bootstrap_servers=['localhost:9092'])
    except Exception as e:
        print('Exception while connecting Kafka')
        print(str(e))
    else:
        es.indices.create(
            index='tweets',
            body={
                "settings": {
                    "analysis": {
                        "filter": {
                            "persian_stop": {
                                "type": "stop",
                                "stopwords_path": "stop_words.txt" 
                            }
                        },
                        "analyzer": {
                            "parsi_analyzer": {
                                "tokenizer": "standard",
                                "filter": [
                                    "decimal_digit",
                                    "persian_stop"
                                ]
                            }
                        }
                    }
                },
                "mappings": {
                    "properties": {
                        "text": {
                            "type": "text",
                            "analyzer": "parsi_analyzer"
                        },
                        "created_at": {
                            "type": "date",
                            "format": "yyyy-MM-dd HH:mm:ss"
                        }
                    }
                }
            },
            ignore=400)
        
        for msg in consumer:
            out = {}
            tweet = json.loads(msg.value)
            # id fields 
            out['uuid'] = tweet['uuid']
            # datetime fields
            dt = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
            out['created_at'] = str(dt)
            # text fields 
            out['text_raw'] = tweet['text']

            # clean text
            text = tweet['text']
            # mentions
            text = re.sub(r'@\w*', '', text)
            # urls
            text = re.sub(r'https?://[^ ]+', '', text)
            # arabic to farsi
            text = re.sub('ك', 'ک', text)
            text = re.sub('ي', 'ی', text)
            out['text'] = text

            # user fields 
            out['user'] = tweet['user']
            # entities fields 
            out['entities'] = tweet['entities']
            es.index(index='tweets', id=tweet['uuid'], body=out)
            print('Tweet %s indexed successfully.' % (tweet['uuid']))
