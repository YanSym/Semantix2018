from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("NASA")
sc = SparkContext(conf = conf)

rdd1 = sc.textFile("access_log_Jul95_clean.txt")
rdd2 = sc.textFile("access_log_Aug95_clean.txt")
lines = rdd1.union(rdd2)
lines.cache()

bytes = lines.map(lambda line: int(line.split(" ")[-1].replace("-","0"))).sum()

print (bytes)
