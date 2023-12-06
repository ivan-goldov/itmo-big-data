
import argparse
from mrjob.job import MRJob, MRStep
from collections import defaultdict

reducer_output = set()
class MRTopQuoteCount(MRJob):
    def mapper_init(self):
        self.cnt = defaultdict(int)

    def mapper(self, _, line):
        if not line.startswith('"character" "dialogue"'):
            _, character, _ = line.split(" ", 2)
            self.cnt[character.strip('"').strip("\\")] += 1

    def mapper_final(self):
        yield from self.cnt.items()

    def reducer_aggregate(self, character, counts):
        yield None, (character, sum(counts))

    def reducer(self, _, pairs):
        char2cnt = [(p[0], p[1]) for p in pairs]
        yield from sorted(char2cnt, key=lambda x: -x[1])[:20]

    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper,
                mapper_final=self.mapper_final,
                reducer=self.reducer_aggregate,
            ),
            MRStep(reducer=self.reducer)
        ]

if __name__ == "__main__":
    MRTopQuoteCount.run()
