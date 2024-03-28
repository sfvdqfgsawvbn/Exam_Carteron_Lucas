from mrjob.job import MRJob

class TagsParFilm(MRJob):
    def mapper(self, _, line):  
        try:
            (userID, movieID, rating, timestamp) = line.split('\t')
            yield movieID, 1
        except Exception:
            pass

    def reducer(self, movieID, occurrences):
        yield movieID, sum(occurrences)

if __name__ == '__main__':
    TagsParFilm.run()