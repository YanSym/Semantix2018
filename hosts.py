from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("NASA")
sc = SparkContext(conf = conf)

rdd1 = sc.textFile("access_log_Jul95_clean.txt")
rdd2 = sc.textFile("access_log_Aug95_clean.txt")
lines = rdd1.union(rdd2)
lines.cache()

hosts = lines.map(lambda line: (line.split(" ")[0], 1)).reduceByKey(lambda a, b: a + b).filter(lambda x: x[1] == 1).keys().collect()

print (len(hosts))
