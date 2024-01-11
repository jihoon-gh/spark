import re

from pyspark import SparkConf, SparkContext


def normalize_word(word):
    return re.compile(r'\W+', re.UNICODE).split(word.lower())


conf = (SparkConf()
        .setMaster('local')
        .setAppName('countWords'))

sc = SparkContext.getOrCreate(conf=conf)
lines = sc.textFile("./SparkCourse/book.txt")
words = lines.flatMap(normalize_word)
wordCount = (words
             .map(lambda x: (x, 1))
             .reduceByKey(lambda x, y: x + y)
             )

wordCountSorted = (wordCount.map(lambda x: (x[1], x[0]))
                   .sortByKey())

results = wordCountSorted.collect()
for r in results:
    count = str(r[0])
    word = r[1].encode("ascii", "ignore")
    if word:
        print("{0} :\t\t{1}".format(word, count))
