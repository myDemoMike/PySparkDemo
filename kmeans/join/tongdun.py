# -*- coding: UTF-8 -*-
# coding:utf-8

import jieba
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from os.path import abspath
##  同盾

warehouse_location = abspath('hdfs://10.31.1.11:8020/user/hive/warehouse')
spark = SparkSession \
    .builder \
    .appName("pyspark") \
    .enableHiveSupport().getOrCreate()
sql = "select a.*,b.pred6 as pred6 ,m.pred7 as pred7,d.pred10 as pred10,e.pred11 as pred11 from tmp_fk_am.zdy_client_cluster_02_action_v1 a " \
      "left join tmp_fk_am.zdy_client_cluster_02_action_v1_result_6 b on a.contract_no = b.contract_no " \
      "left join tmp_fk_am.zdy_client_cluster_02_action_v1_result_7 m  on a.contract_no = m.contract_no " \
      "left join tmp_fk_am.zdy_client_cluster_02_action_v1_result_10 d on a.contract_no = d.contract_no " \
      "left join tmp_fk_am.zdy_client_cluster_02_action_v1_result_11 e on a.contract_no = e.contract_no"
print(sql)
data11 = spark.sql(sql);
print(data11.show(10))


data11.write.mode("overwrite").saveAsTable("tmp_fk_am.zdy_client_cluster_02_action_v1_result_re")
