import sys,os
from pyspark import SparkConf, SparkContext
conf = SparkConf("local").setAppName("myApp") 
sc = SparkContext(conf = conf)
lines = sc.textFile("epa-http.txt")
pairlines = lines.map(lambda x: (x.split(" ")[0], x.split(" ")[6]))
intlines = pairlines.mapValues(lambda x:int(x))
res = intlines.reduceByKey(lambda x,y:x+y)
ressorted = res.sortBy(lambda x: -x[1])
k = int(sys.argv[1])
topk = ressorted.take(k)
print topk