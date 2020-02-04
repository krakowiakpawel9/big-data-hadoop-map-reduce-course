"""
@author: krakowiakpawel9@gmail.com
@site: e-smartdata.org
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from nltk import pos_tag
# import nltk
import re

# nltk.download('stopwords')

# $ python 04_top_20_common_adjective.py -r emr --num-core-instances 4 prep_reviews.tsv --output-dir=s3://big-data-hadoop/output/job3

WORD_RE = re.compile(r"[\w]+")

lemmatizer = WordNetLemmatizer()
stop_words = stopwords.words('english')


class MRFood(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper),
            MRStep(mapper=self.mapper_get_keys,
                   reducer=self.reducer),
            MRStep(mapper=self.mapper_get_1_and_5,
                   reducer=self.reducer_get_20_words)
        ]

    def mapper(self, _, line):
        (Id, ProductId, UserId, ProfileName, HelpfulnessNumerator, HelpfulnessDenominator,
         Score, Time, Summary, Text) = line.split('\t')
        words = WORD_RE.findall(Text)
        words = filter(lambda word: len(word) > 1, words)
        words = map(str.lower, words)
        words = map(lemmatizer.lemmatize, words)
        words = filter(lambda word: word not in stop_words, words)
        for word in words:
            if pos_tag([word])[0][1] == 'JJ':
                yield Score, word

    def mapper_get_keys(self, key, value):
        yield (key, value), 1

    def reducer(self, key, values):
        yield key, sum(values)

    def mapper_get_1_and_5(self, key, value):
        if key[0] == '1':
            yield key[0], (key[1], value)
        if key[0] == '5':
            yield key[0], (key[1], value)

    def reducer_get_20_words(self, key, values):
        results = {}
        for value in values:
            results[value[0]] = value[1]
        sorted_results = sorted([(val, key) for key, val in results.items()], reverse=True)

        yield key, sorted_results[:20]


if __name__ == '__main__':
    MRFood.run()
