from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext

conf = (SparkConf()
        .setMaster("spark://127.0.0.1:7077")
        .setAppName("My app")
        .set("spark.executor.memory", "1g"))
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)
my_dataframe = sqlContext.sql("Select count(1) from logs.fmnews_dim_where")
my_dataframe.show()
