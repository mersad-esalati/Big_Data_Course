# insert tweets
import json
import datetime

from kafka import KafkaConsumer

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

try:
    consumer = KafkaConsumer('parsed_tweets', auto_offset_reset='earliest', bootstrap_servers=['localhost:9092'])
except Exception as e:
    print('Exception while connecting Kafka')
    print(str(e))
else:
    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    cluster = Cluster(['localhost'], port='9042', auth_provider=auth_provider)
    session = cluster.connect('tweets')

    stmt=session.prepare("INSERT INTO tweets (day, hour, uuid) VALUES (?,?,?)")
    for msg in consumer:
        tweet = json.loads(msg.value)
        uuid = tweet['uuid']
        dt = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
        day = dt.day
        hour = dt.hour
        qry=stmt.bind([day, hour, uuid])
        session.execute(qry)
        print('Tweet %s inserted successfully.' % (tweet['uuid']))