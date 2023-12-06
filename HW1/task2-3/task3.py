
from mrjob.job import MRJob, MRStep

import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.util import bigrams

class MRBigrams(MRJob):
    def mapper_init(self):
         nltk.download('punkt')
         nltk.download('stopwords')

    
    def mapper(self, _, line):
        if not line.startswith('"character" "dialogue"'):
            _, _, phrase = line.split(" ", 2)
            p = phrase.strip('"').strip("\\")
            words = word_tokenize(p)
            stop_words = set(stopwords.words('english'))
            
            # в задании явно не указано убирать стоп слова, но по примеру кажется что надо
            words = [word for word in words if word.isalnum() and word not in stop_words]
            
            words = [word.lower() for word in words]
            for b in list(bigrams(words)):
                yield b, 1

    def reducer_aggregate(self, bigram, counts):
        yield None, (bigram, sum(counts))

    def reducer(self, _, pairs):
        bigram2cnt = [(f'{p[0][0]} {p[0][1]}', p[1]) for p in pairs]
        yield from sorted(bigram2cnt, key=lambda x: -x[1])[:20]

    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper,
                reducer=self.reducer_aggregate,
            ),
            MRStep(reducer=self.reducer)
        ]

if __name__ == "__main__":
    MRBigrams.run()
