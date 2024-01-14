from pyspark.sql import SparkSession, Row, functions as func


spark = SparkSession.builder.appName("fakefriends").getOrCreate()

df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("./SparkCourse/fakefriends-header.csv"))

(df
 .select("age", "friends")
 .groupBy("age")
 .avg("friends")
 .orderBy("avg(friends)").show()
 )

(df
 .select("age", "friends")
 .groupBy("age")
 .agg(func.round(func.avg("friends"), 2).alias("friends_avg"))
 .orderBy("age")
 .show())
spark.stop()