from pyspark import SparkConf, SparkContext
import collections

conf = (SparkConf()
        .setMaster("local")
        .setAppName("test"))
sc = SparkContext(conf = conf)

lines = sc.textFile("/ml-100k/u.data")
ratings = lines.map(lambda x : x.split()[2])
result = ratings.countByValue()
sortedResult = collections.OrderedDict(sorted(result.items()))
for k, v in sortedResult.items():
        print("%s %i" % (k, v))