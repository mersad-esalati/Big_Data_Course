import re

HASHTAG_PATTERN = re.compile(r'#\w*')
MENTION_PATTERN = re.compile(r'@\w*')
EMOJIS_PATTERN = re.compile(u'([๐ค๐ฅฐ๐คช๐คฃ๐คจ๐คฌ๐ต๐ฅต๐ค๐คฎ๐ค๐คซ๐ค๐ฅบ๐ฅณ๐คฉ๐ค๐คฅ๐ง๐คญ๐๐๐๐ค๐๐๐๐๐ท๐๐ถ๐๐ค๐๐๐ฅฒ๐ค๐ฅถ๐ฅฑ๐ฅด๐คค๐คง๐คฏ๐ค ๐คข๐ค๐คก๐คฆ๐คท๐คต๐ง๐ง๐ค๐ค๐๐คฒ๐ค๐ค๐ค๐คฒ๐ค๐ค๐ค๐๐ขโญ๏ธ])|([\U00002600-\U000027BF])|([\U0001f300-\U0001f64F])|([\U0001f680-\U0001f6FF])')
SMILEYS_PATTERN = re.compile(r"(\s?:X|:|;|=)(?:-)?(?:\)+|\(|O|D|P|S|\\|\/\s){1,}", re.IGNORECASE)
NUMBERS_PATTERN = re.compile(r"(^|\s)(-?\d+([.,]?\d+)*)")

def text_cleaner(text):
    # clean hashtags
    text = HASHTAG_PATTERN.sub('', text)
    # clean mentions
    text = MENTION_PATTERN.sub('', text)
    # clean urls
    text = re.sub(r'https?://[^ ]+', '', text)
    text = re.sub(r'www.[^ ]+', '', text)
    # clean special chars
    text = re.sub(r'[\[\]!$()&@:\\#/\*|ูช{}<>?ุ=.\"\'โฆยปยซ;,ุ]+', ' ', text)
    # clean emojis
    text = EMOJIS_PATTERN.sub('', text)
    # clean smiley
    text = SMILEYS_PATTERN.sub('', text)
    # clean spaces
    text = re.sub(r'\s+', ' ', text)
    # clean numbers
    text = NUMBERS_PATTERN.sub('', text)
    
    text = text.strip()
    return text