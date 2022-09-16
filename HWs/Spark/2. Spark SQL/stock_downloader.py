import requests
import io
import re

NAME_RE = re.compile(r"^[\u0600-\u06FF\s]+[0-9]?$")

import pandas as pd

for month in range(10, 13):
    for day in range(1, 32):
        # donwload xlsx file
        date = '1399-%02d-%02d' % (month, day)
        headers = {'user-agent': 'Chrome/61.0'}
        query = {'d': date}
        url = 'http://members.tsetmc.com/tsev2/excel/MarketWatchPlus.aspx'
        r = requests.get(url, params=query, headers=headers, allow_redirects=True)
        if len(r.content) > 10240:
            # convert xlsx to csv
            with io.BytesIO(r.content) as fh:
                df = pd.io.excel.read_excel(fh, index_col=0, header=2)

                # filter symbol name
                idx = [bool(re.match(NAME_RE, symbol)) for symbol in df.index]
                df = df[idx]
                df.index.names = ['symbol']
                df.columns = ['name', 'count', 'capacity', 'value',
              		       'yesterday', 'first',
              		       'last_deal', 'last_deal - diff', 'last_deal - percent',
              		       'final_price', 'final_price - diff', 'final_price - percent',
              		       'min', 'max']
                # add trade date column
                df['trade_date'] = '1399-%02d-%02d' % (month, day)
                df['year'] = 1399
                df['month'] = month
                df['day'] = day
                filename = './stocks/1399-%02d-%02d.csv' % (month, day)
                df.to_csv(filename)
                print('%s --> done' % filename)
