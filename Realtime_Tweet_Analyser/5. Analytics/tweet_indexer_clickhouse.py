# insert tweets
import json
import datetime

from kafka import KafkaConsumer

from clickhouse_driver import Client

try:
    consumer = KafkaConsumer('parsed_tweets', auto_offset_reset='earliest', bootstrap_servers=['localhost:9092'])
except Exception as e:
    print('Exception while connecting Kafka')
    print(str(e))
else:
    client = Client(host='localhost')
    # create tweets table
    client.execute('''
    CREATE TABLE IF NOT EXISTS twitter.tweets(
        uuid String,
        created_at DateTime,
        text String
    ) ENGINE = MergeTree()
    PARTITION BY (toYear(created_at), toMonth(created_at), toDayOfMonth(created_at))
    ORDER BY (created_at, uuid);
    ''')

    # create users table
    client.execute('''
    CREATE TABLE IF NOT EXISTS twitter.users(
        uuid String,
        created_at DateTime,
        user String
    ) ENGINE = MergeTree()
    PARTITION BY (toYear(created_at), toMonth(created_at), toDayOfMonth(created_at))
    ORDER BY (created_at, uuid);
    ''')

    # create hashtags table
    client.execute('''
    CREATE TABLE IF NOT EXISTS twitter.hashtags(
        uuid String,
        created_at DateTime,
        hashtag String
    ) ENGINE = MergeTree()
    PARTITION BY toDayOfMonth(created_at)
    ORDER BY (created_at, uuid);
    ''')

    # create keywords table
    client.execute('''
    CREATE TABLE IF NOT EXISTS twitter.keywords(
        uuid String,
        created_at DateTime,
        keyword String
    ) ENGINE = MergeTree()
    PARTITION BY (toYear(created_at), toMonth(created_at), toDayOfMonth(created_at))
    ORDER BY (created_at, uuid);
    ''')

    for msg in consumer:
        tweet = json.loads(msg.value)
        dt = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

        # insert into tweets table
        client.execute(
            'INSERT INTO twitter.tweets (uuid, created_at, text) VALUES',
            [{'uuid': tweet['uuid'], 'created_at': dt, 'text': tweet['text']}]
        )

        # insert into users table
        client.execute(
            'INSERT INTO twitter.users (uuid, created_at, user) VALUES',
            [{'uuid': tweet['uuid'], 'created_at': dt, 'user': tweet['user']['screen_name']}]
        )

        # insert into hashtags table
        for h in tweet['entities']['hashtags']:
            if len(h[1:]) > 0:
                client.execute(
                    'INSERT INTO twitter.hashtags (uuid, created_at, hashtag) VALUES',
                    [{'uuid': tweet['uuid'], 'created_at': dt, 'hashtag': h}]
                )

        # insert into keywords table
        for kw in tweet['entities']['keywords']:
            client.execute(
                'INSERT INTO twitter.keywords (uuid, created_at, keyword) VALUES',
                [{'uuid': tweet['uuid'], 'created_at': dt, 'keyword': kw}]
            )

        print('Tweet %s processed successfully.' % (tweet['uuid']))