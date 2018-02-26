from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("NASA")
sc = SparkContext(conf = conf)

rdd1 = sc.textFile("access_log_Jul95_clean.txt")
rdd2 = sc.textFile("access_log_Aug95_clean.txt")
lines = rdd1.union(rdd2)
lines.cache()

average_404_day = lines.map(lambda line: (line.split("- [")[1].split(":")[0], int(line.split(" ")[-2].replace("-","0")))).filter(lambda x: x[1] == 404).mapValues(lambda x: 1).reduceByKey(lambda a, b: a + b).values().mean()

print (average_404_day)
