import sys
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .enableHiveSupport() \
    .getOrCreate()
df = spark.read.option("header","true").option("inferSchema","true").csv("epa-http.csv")
k = sys.argv[1]
df.createOrReplaceTempView("dftable")
sqlDF = spark.sql("SELECT IPaddress,sum(Bytes)  FROM dftable WHERE Hour=' "+k+" ' GROUP BY IPaddress")
sqlDF.show()
print('From %s : 00 to %s : 59' ) %(k,k)
print('\n \n \n \n')