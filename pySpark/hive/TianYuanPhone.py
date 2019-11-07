# -*- coding: UTF-8 -*-
# coding:utf-8

import re
from os.path import abspath

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

warehouse_location = abspath('hdfs://10.31.1.11:8020/user/hive/warehouse')
spark = SparkSession \
    .builder \
    .appName("pyspark") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport().getOrCreate()

# ztoB
data11 = spark.sql(
    "select send_name,send_mobile as phone ,send_prov,send_city,send_county,send_address,start_time as p_month from tmp_lishu.to_tianyuan_zto_order where send_mobile is not null")

data12 = spark.sql(
    "select send_name,send_phone as phone ,send_prov,send_city,send_county,send_address,start_time as p_month from tmp_lishu.to_tianyuan_zto_order where send_phone is not null")

data21 = spark.sql(
    "select send_name,send_mobile as phone ,send_prov,send_city,send_county,send_address,p_month from tmp_lishu.zto_order_201802 where send_mobile is not null")

data22 = spark.sql(
    "select send_name,send_phone as phone ,send_prov,send_city,send_county,send_address, p_month from tmp_lishu.zto_order_201802 where send_phone is not null")

data31 = spark.sql(
    "select send_name,send_mobile as phone ,send_prov,send_city,send_county,send_address,p_month from tmp_lishu.zto_order_201803 where send_mobile is not null")

data32 = spark.sql(
    "select send_name,send_phone as phone ,send_prov,send_city,send_county,send_address, p_month from tmp_lishu.zto_order_201803 where send_phone is not null")

data41 = spark.sql(
    "select send_name,send_mobile as phone ,send_prov,send_city,send_county,send_address,p_month from tmp_lishu.zto_order_201804 where send_mobile is not null")

data42 = spark.sql(
    "select send_name,send_phone as phone ,send_prov,send_city,send_county,send_address, p_month from tmp_lishu.zto_order_201804 where send_phone is not null")

dataFilter = data11.union(data12).union(data21).union(data22) \
    .union(data31).union(data32).union(data41).union(data42).distinct()


def getPhone(phone):
    phone_pat = re.compile(
        "[1][3,5,8,9][0-9]{9}$|^[9][2,8][0-9]{9}$|^14[5|6|7|8|9][0-9]{8}$|^16[1|2|4|5|6|7][0-9]{8}$|^17[0-8][0-9]{8}")
    res = phone_pat.search(phone)
    if res is None:
        return "0";
    else:
        return "1";


seg_udf = udf(getPhone, StringType())
result = dataFilter.distinct().filter(seg_udf(dataFilter.phone) == "1")
print("ZTO数据手机号码请吸收的数量是" + str(result.count()))
result.write.mode("overwrite").saveAsTable("tmp_lishu.ztoA_send_05_22")



