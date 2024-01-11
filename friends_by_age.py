from pyspark import SparkConf, SparkContext

def get_age_and_friends(x):
    splits = x.split(',')
    age = int(splits[2])
    nums = int(splits[3])
    return age, nums


conf = SparkConf().setMaster('local').setAppName("ageAndFriends")
sc = SparkContext(conf=conf)

rdd = sc.textFile('./sparkCourse/fakefriends.csv')
ageAndFriends = rdd.map(get_age_and_friends)
ageAndFriendsWithKey = (ageAndFriends
                        .mapValues(lambda x: (x, 1))
                        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])))

avg = ageAndFriendsWithKey.mapValues(lambda x: x[0] // x[1])
results = avg.sortByKey().collect()
for r in results:
    print(r)
