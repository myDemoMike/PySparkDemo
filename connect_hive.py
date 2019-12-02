# -*- coding: UTF-8 -*-
# coding:utf-8

import re
from os.path import abspath

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName("pyspark") \
    .config("spark.debug.maxToStringFields", "100") \
    .enableHiveSupport().getOrCreate()
#    .config("spark.sql.warehouse.dir", warehouse_location) \
#    .config("hive.groupby.skewindta", "true") \

d1 = spark.sql("""
select *
from portrait.ind_cus_attr_coll ca
""")
d1.show()
# print(d1.count())
# d1.write.mode("overwrite").saveAsTable("tmp_lishu.test_01")