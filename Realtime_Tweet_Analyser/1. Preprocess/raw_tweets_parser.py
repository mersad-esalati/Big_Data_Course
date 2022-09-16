import json
import re

from collections import Counter

from kafka import KafkaConsumer, KafkaProducer

from hazm import word_tokenize, POSTagger

from utils import text_cleaner

from tf_idf_scratch import fit_idf

from hazm import Normalizer

######
HASHTAG_PATTERN = re.compile(r'#\w*')
def extract_hashtags(text):
    hashtags = HASHTAG_PATTERN.findall(text)
    return list(dict.fromkeys(hashtags))

######
MENTION_PATTERN = re.compile(r'@\w*')
def extract_mentions(text):
    mentions = MENTION_PATTERN.findall(text)
    return list(dict.fromkeys(mentions))

######
stop_words = []
with open('stop_words.txt') as f:
    stop_words = f.readlines()
stop_words = [w.strip() for w in stop_words]
def remove_stop_words(text):
    for w in stop_words:
        text = text.replace(' '+w+' ', ' ')
    return text

######
tagger = POSTagger(model='resources/postagger.model')
def remove_verbs(text):
    l = tagger.tag(word_tokenize(text))
    fl = filter(lambda item: item[1] != 'V' and item[1] != 'ADV', l)
    return ' '.join([item[0] for item in fl])

######
documents = []
with open('tweets_dataset.txt') as f:
    documents = f.readlines()
vocab, idf_values = fit_idf(documents)

track_list = []
with open('track_list.txt') as f:
    track_list = f.readlines()

normalizer = Normalizer()
def extract_keywords(text):
    # clean text 
    text = text_cleaner(text)
    # normalize text 
    text = normalizer.normalize(text)
    # remove stop words
    text = remove_stop_words(text)
    # remove verbs + adverbs
    text = remove_verbs(text)

    tokens = word_tokenize(text)
    tf = Counter(tokens)
    keywords = []
    for word in tokens:
        if word in list(vocab.keys()):
            tf_idf_value = (tf[word]/len(tokens))*(idf_values[word])
            if tf_idf_value > 0.25:
                keywords.append(word)
    return list(dict.fromkeys(keywords))

######
def parse_tweet(raw_tweet):
    tweet = json.loads(raw_tweet)

    text = tweet['text']
    # extract hashtags
    hashtags = extract_hashtags(text)
    # extract mentions
    mentions = extract_mentions(text)
    # extract keywords
    keywords = extract_keywords(text)

    tweet['entities'] = {}
    tweet['entities']['hashtags'] = hashtags
    tweet['entities']['mentions'] = mentions
    tweet['entities']['keywords'] = keywords
    return tweet

if __name__ == '__main__':
    producer = None
    consumer = None
    try:
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
        consumer = KafkaConsumer('raw_tweets', auto_offset_reset='earliest', bootstrap_servers=['localhost:9092'])
    except Exception as e:
        print('Exception while connecting Kafka')
        print(str(e))
    else:
        for msg in consumer:
            raw_tweet = msg.value
            p_tweet = parse_tweet(raw_tweet)
            try:
                key_bytes = bytes('parsed', encoding='utf-8')
                value_bytes = bytes(json.dumps(p_tweet), encoding='utf-8')
                producer.send('parsed_tweets', key=key_bytes, value=value_bytes)
                producer.flush()
                print('Tweet %s parsed successfully.' % (p_tweet['uuid']))
            except Exception as e:
                print('Exception in parsing tweet')
                print(str(e))