from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("NASA")
sc = SparkContext(conf = conf)

rdd1 = sc.textFile("access_log_Jul95_clean.txt")
rdd2 = sc.textFile("access_log_Aug95_clean.txt")
lines = rdd1.union(rdd2)
lines.cache()

url404 = lines.map(lambda line: (line.split(" ")[0], line.split(" ")[-2].replace("-","0"))).filter(lambda x: x[1] == '404').map(lambda (x,y):(x,1)).reduceByKey(lambda a, b: a + b).collect()

sorted_url404 = sorted(url404, key=lambda x: x[1], reverse=True)

for key,value in sorted_url404[:5]:
	print ("{0} | {1}".format(key,value))
