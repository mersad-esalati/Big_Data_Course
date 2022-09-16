import datetime
import redis

from flask import Flask
from flask import render_template

from flask_paginate import Pagination, get_page_args

app = Flask(__name__)
if __name__ == '__main__':
	app.run(host="0.0.0.0", debug=True, port=8585)

@app.route("/hello")
def hello_world():
    return "<p>Hello, World!</p>"

@app.route("/dashboard")
def dashboard():
    r = redis.Redis(host='74.220.20.101', port=6379)

    cur_dt = datetime.datetime.utcnow()

    keys = ['%02d_%02d' % (dt.day, dt.hour) for dt in [cur_dt-datetime.timedelta(hours=n) for n in range(6)]]
    c_6h_ago = sum([int(0 if v is None else v) for v in r.mget(keys)])

    c_today = r.get('%02d' % cur_dt.day)
    c_today = int(0 if c_today is None else c_today)

    uh_1h_ago = r.smembers('%02d_%02d_hashtags' % (cur_dt.day, cur_dt.hour))

    content = {
        'count_6h_ago': c_6h_ago,
        'count_today': c_today,
        'unique_hashtags_1h_ago': len(uh_1h_ago),
    }
    
    return render_template('dashboard.html', **content)

@app.route("/dashboard/recent_tweets")
def recent_tweets():
    r = redis.Redis(host='74.220.20.101', port=6379)

    all_tweets = r.lrange('recent_tweets', 0, -1)
    all_tweets = [item.decode() for item in all_tweets]
    all_tweets = all_tweets[::-1]

    page, per_page, offset = get_page_args()
    tweets_disp = all_tweets[offset: offset + per_page]
    pagination = Pagination(page=page, per_page=per_page, total=len(all_tweets),
     css_framework='bootstrap4')

    return render_template('recent_tweets.html', tweets_disp=tweets_disp,
     page=page, per_page=per_page, pagination=pagination)


@app.route("/dashboard/recent_hashtags")
def recent_hashtags():
    r = redis.Redis(host='74.220.20.101', port=6379)

    all_hashtags = r.lrange('recent_hashtags', 0, -1)
    all_hashtags = [item.decode() for item in all_hashtags]
    all_hashtags = all_hashtags[::-1]

    page, per_page, offset = get_page_args()
    hashtags_disp = all_hashtags[offset: offset + per_page]
    pagination = Pagination(page=page, per_page=per_page, total=len(all_hashtags),
     css_framework='bootstrap4')

    return render_template('recent_hashtags.html', hashtags_disp=hashtags_disp,
     page=page, per_page=per_page, pagination=pagination)