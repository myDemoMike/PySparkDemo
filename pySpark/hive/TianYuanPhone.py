# -*- coding: UTF-8 -*-
# coding:utf-8

from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName("pyspark") \
    .getOrCreate()
# schema1 = StructType([StructField("words", StringType(), True))])

schema1 = StructType([StructField("words", StringType(), True),
                      StructField("out", StringType(), True)])

df = spark.sparkContext.parallelize(
    [("r z h k p",), ("z y x w v u t s"), ("s x o n r"), ("x z y m t s q e"), ("x z y m t s q e"), ("x z y m t s q e")])
# df = spark.sparkContext.parallelize([("a", "a"), ("b", "a"), ("c", "a")])

swimmers = spark.createDataFrame(df, schema1)

aa = swimmers.drop("out")

df2 = spark.createDataFrame([("A B  c",), ("aaa  aaaa",)], ["text"])
df2.show()
