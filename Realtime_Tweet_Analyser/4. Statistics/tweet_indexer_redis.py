import json
import datetime
import redis

from kafka import KafkaConsumer

r = redis.Redis(host='localhost', port=6379)

consumer = KafkaConsumer('parsed_tweets', auto_offset_reset='earliest', bootstrap_servers=['localhost:9092'])

for msg in consumer:
    tweet = json.loads(msg.value)
    dt = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
    exp_dt = datetime.datetime.combine(dt.date()+datetime.timedelta(7), datetime.datetime.min.time())

    p1 = r.pipeline()
    p2 = r.pipeline()
    p3 = r.pipeline()
    p4 = r.pipeline()

    key = '%02d_%02d' % (dt.day, dt.hour)
    p1.incr(key)
    p1.expireat(key, exp_dt)

    key = '%02d' % (dt.day)
    p1.incr(key)
    p1.expireat(key, exp_dt)

    p2.rpush('recent_tweets', tweet['text'])
    p2.ltrim('recent_tweets', -100, -1)

    for hashtag in tweet['entities']['hashtags']:
        if len(hashtag[1:])>1:
            key = '%02d_%02d_hashtags' % (dt.day, dt.hour)
            p3.sadd(key, hashtag[1:])
            p3.expireat(key, exp_dt)

            p4.rpush('recent_hashtags', hashtag[1:])
            p4.ltrim('recent_hashtags', -1000, -1)
    
    p1.execute()
    p2.execute()
    p3.execute()
    p4.execute()
    
    print('Tweet %s processed successfully.' % (tweet['uuid']))
    