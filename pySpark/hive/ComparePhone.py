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
data11 = spark.sql("select DISTINCT(receiv_mobile) as phone from tmp_lishu.to_tianyuan_zto_order where receiv_mobile is not null")

data12 = spark.sql("select DISTINCT(receiv_phone) as phone from tmp_lishu.to_tianyuan_zto_order where receiv_phone is not null")

# data22 = spark.sql("select DISTINCT(recipientmobile) as phone from tmp_lishu.lishu_sf where recipientmobile is not null")
0
data31 = spark.sql("select DISTINCT(receiv_mobile) as phone from tmp_lishu.zto_order_201802 where receiv_mobile is not null")

data32 = spark.sql("select DISTINCT(receiv_phone) as phone from tmp_lishu.zto_order_201802 where receiv_phone is not null")

data41 = spark.sql("select DISTINCT(receiv_mobile) as phone from tmp_lishu.zto_order_201803 where receiv_mobile is not null")

data42 = spark.sql("select DISTINCT(receiv_phone) as phone from tmp_lishu.zto_order_201803 where receiv_phone is not null")

data51 = spark.sql("select DISTINCT(receiv_mobile) as phone from tmp_lishu.zto_order_201804 where receiv_mobile is not null")

data52 = spark.sql("select DISTINCT(receiv_phone) as phone from tmp_lishu.zto_order_201804 where receiv_phone is not null")

dataFilter = data11.union(data12)\
     .union(data31).union(data32).union(data41).union(data42).union(data51).union(data52).distinct()

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
# result.write.mode("overwrite").saveAsTable("tmp_lishu.ztoA_05_16")
# 303343844
# print("ZTO数据手机号码去重后的数量是" + str(result.count()))

#  用户 20W
databqd = spark.sql("select DISTINCT(mobiletelephone) as phone from opd.to_bqd_ind_info where mobiletelephone is not null")
result2 = databqd.distinct().filter(seg_udf(databqd.phone) == "1")
# 428240
# print("车贷数据手机号码去重后的数量是" + str(result2.count()))

# 取交集
com = result.join(result2,result.phone == result2.phone, 'inner').select(result.phone)

com.write.mode("overwrite").saveAsTable("tmp_lishu.ztoA_05_16_phone_NOSF")
# 95622     加上SF
# 54894     不加SF
print("匹配数据手机号码数量是" + str(com.count()))
# print("占ZTO数据" +  str(com.count()/ result.count()))
# print("占车贷数据" +  str(com.count()/ result2.count()))

spark.stop()


