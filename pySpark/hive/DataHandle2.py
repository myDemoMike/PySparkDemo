# -*- coding: UTF-8 -*-
# coding:utf-8

from os.path import abspath

from pyspark.sql import SparkSession

warehouse_location = abspath('hdfs://10.31.1.11:8020/user/hive/warehouse')
spark = SparkSession \
    .builder \
    .appName("pyspark") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport().getOrCreate()

# 中通方面的手机号

data11 = spark.sql(
    """

    """



).distinct()


data11.write.mode("overwrite").saveAsTable("tmp_lishu.zto_06_11")


