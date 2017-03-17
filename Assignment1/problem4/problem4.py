from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys
conf = SparkConf("local").setAppName("myApp") 
sc = SparkContext(conf = conf)
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .enableHiveSupport() \
    .getOrCreate()
df = spark.read.option("header","true").option("inferSchema","true").csv("epa-http.csv")
df.createOrReplaceTempView("dftable")
lines = sc.textFile("epa-http.csv")
pairlines = lines.map(lambda x: (x.split(".")[0], x))
lines2 = sc.parallelize([sys.argv[2]])
pairlines2 = lines2.map(lambda x: (x.split(".")[0], x))
res=pairlines2.join(pairlines)
res_key=res.keys().distinct().collect()
res_value=res.values()
res_value_value=res_value.values()
iplist=res_value_value.map(lambda x: x.split(",")[0])
ipdistinct=iplist.distinct().map(lambda x:(x,x))
schemaip=spark.createDataFrame(ipdistinct)
schemaip.createOrReplaceTempView("iptable")
k = sys.argv[1]
sqlres=spark.sql("SELECT IPaddress,sum(Bytes) FROM iptable JOIN dftable ON iptable._1=dftable.IPaddress WHERE Hour=' "+k+" ' GROUP BY IPaddress").show()
print (res_key[0]+'.XXX.XXX.XXX')
print('From %s : 00 to %s : 59 \n' ) %(k,k)
print('\n \n \n \n')