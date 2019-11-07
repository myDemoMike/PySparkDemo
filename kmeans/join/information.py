# -*- coding: UTF-8 -*-
# coding:utf-8

import jieba
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from os.path import abspath
##    客户信息

warehouse_location = abspath('hdfs://10.31.1.11:8020/user/hive/warehouse')
spark = SparkSession \
    .builder \
    .appName("pyspark") \
    .enableHiveSupport().getOrCreate()
sql = ""
print(sql)
data11 = spark.sql(sql);


data11.write.mode("overwrite").saveAsTable("tmp_fk_am.zdy_client_cluster_02_action_v1_result_re")
