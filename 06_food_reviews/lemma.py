"""
@author: krakowiakpawel9@gmail.com
@site: e-smartdata.org
"""

from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from nltk import pos_tag


lemmatizer = WordNetLemmatizer()
stop_words = stopwords.words('english')

print(stop_words)

# print(lemmatizer.lemmatize('computers'))
# print(lemmatizer.lemmatize('mice'))
# print(lemmatizer.lemmatize('books'))

print(pos_tag(['big']))
print(pos_tag(['book']))