from collections import Counter
import math
from hazm import word_tokenize

def idf(corpus, unique_words):
    idf_dict={}
    N=len(corpus)
    for i in unique_words:
        count=0
        for sen in corpus:
            if i in sen.split():
                count=count+1
        idf_dict[i]=(math.log((1+N)/(count+1)))+1
    return idf_dict

def fit_idf(corpus):
    unique_words = set()
    for d in corpus:
        for w in word_tokenize(d):
            if len(w)<3:
                continue
            unique_words.add(w)
    unique_words = sorted(list(unique_words))
    vocab = {j:i for i,j in enumerate(unique_words)}
    idf_values = idf(corpus, unique_words)
    return vocab, idf_values