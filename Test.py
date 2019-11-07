# -*- coding: UTF-8 -*-
# coding:utf-8

from os.path import abspath

from pyspark.sql import SparkSession

# 4775221
warehouse_location = abspath('hdfs://10.31.1.11:8020/user/hive/warehouse')
spark = SparkSession \
    .builder \
    .master("local[4]") \
    .appName("pyspark") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport().getOrCreate()

data1 = spark.sql("select recipientid,price0,price1,price2 from TMP_LISHU.LISHU_JIEGUO_SUCC_18 limit 20")
data1.show()