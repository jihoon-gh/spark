from pyspark import SparkConf, SparkContext


def parse_lines(x):
    splits = x.split(",")
    customer_id = int(splits[0])
    price = float(splits[2])
    return customer_id, price


conf = SparkConf().setMaster('local').setAppName('totalPrice')
sc = SparkContext.getOrCreate(conf=conf)

lines = sc.textFile("./SparkCourse/customer-orders.csv")
parsedLines = lines.map(parse_lines)
totalPriceById = parsedLines.reduceByKey(lambda x, y: x + y)
results = (totalPriceById
           .sortBy(lambda x: x[1], ascending=False)
           .collect())

for r in results:
    print("{0}\t{1:.2f}".format(r[0], r[1]))