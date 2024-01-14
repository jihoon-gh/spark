from pyspark.sql import SparkSession, Row


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
