from pyspark.sql import SparkSession, Row, functions as func

spark = SparkSession.builder.appName("count_words").getOrCreate()

inputDF = spark.read.text("./SparkCourse/book.txt")

words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
words.filter(words.word != "")

lowerCase = words.select(func.lower(words.word).alias("word"))

wordsCount = lowerCase.groupBy("word").count()

wordsCountSorted = wordsCount.orderBy("count", ascending=False)

wordsCountSorted.show(wordsCountSorted.count())