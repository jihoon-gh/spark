from pyspark import SparkConf, SparkContext


def parse_line(x):
    splits = x.split(',')
    station_id = splits[0]
    entry_type = splits[2]
    temperature = float(splits[3]) * 0.1
    return station_id, entry_type, temperature


conf = SparkConf().setMaster('local').setAppName('minTemper')
sc = SparkContext.getOrCreate(conf=conf)

lines = sc.textFile("./SparkCourse/1800.csv")
parsedLines = lines.map(parse_line)
minTemperature = parsedLines.filter(lambda x: "TMIN" in x[1])
stationTemps = minTemperature.map(lambda x: (x[0], x[2]))
results = (stationTemps
           .reduceByKey(lambda x, y: max(x, y))
           .sortBy(lambda x: x[1])
           .collect())

for r in results:
    print(r[0] + "\t{0:.2f}" .format(r[1]))