import elasticsearch

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

es = elasticsearch.Elasticsearch(['74.220.20.101'])

auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(['74.220.20.101'], port='9042', auth_provider=auth_provider)
session = cluster.connect('tweets')

qry = '''
SELECT id_str FROM tweet_hashtags WHERE hashtag='روحانی' and day=19 limit 10;
'''

res = session.execute(qry)

body = {
    "query": {
        "ids": {
            "values": [r[0] for r in res]
        }
    }
}

f_res = es.search(index='tweets', body=body)

for hit in f_res['hits']['hits']:
    print(hit['_source']['text_raw'])