
from mrjob.job import MRJob, MRStep

class MRLongestPhrase(MRJob):
    def mapper(self, _, line):
        if not line.startswith('"character" "dialogue"'):
            _, character, phrase = line.split(" ", 2)
            c = character.strip('"').strip("\\")
            p = phrase.strip('"').strip("\\")
            yield c, len(p)

    def reducer_aggregate(self, character, lengths):
        yield None, (character, max(lengths))

    def reducer(self, _, pairs):
        char2len = [(p[0], p[1]) for p in pairs]
        yield from sorted(char2len, key=lambda x: -x[1])

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer_aggregate,
            ),
            MRStep(reducer=self.reducer)
        ]

if __name__ == "__main__":
    MRLongestPhrase.run()
