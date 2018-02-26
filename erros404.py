from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("NASA")
sc = SparkContext(conf = conf)

rdd1 = sc.textFile("access_log_Jul95_clean.txt")
rdd2 = sc.textFile("access_log_Aug95_clean.txt")
lines = rdd1.union(rdd2)
lines.cache()

erros404 = lines.map(lambda line: line.split(" ")[-2].replace("-","0")).filter(lambda x: x == '404').map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).values().collect()

print (erros404[0])
