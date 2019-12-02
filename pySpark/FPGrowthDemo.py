# -*- coding: UTF-8 -*-
# coding:utf-8
from pyspark.ml.fpm import FPGrowth
from pyspark.sql import SparkSession
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("pyspark") \
    .getOrCreate()

data = spark.read.text("data/sample_fpgrowth.txt").select(split("value", "\s+").alias("items"))
data.show(truncate=False)
fp = FPGrowth(minSupport=0.2, minConfidence=0.7)
fpm = fp.fit(data)
fpm.freqItemsets.show(5)
fpm.associationRules.show(5)
new_data = spark.createDataFrame([(["t", "s"],)], ["items"])
sorted(fpm.transform(new_data).first().prediction)
