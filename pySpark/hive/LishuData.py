# -*- coding: UTF-8 -*-
# coding:utf-8
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("pyspark") \
    .config("spark.sql.crossJoin.enabled", "true") \
    .config("spark.shuffle.consolidateFiles", "true") \
    .enableHiveSupport().getOrCreate()

# 粒数数据
data1 = spark.sql("select * from opd.to_ccf_cs_account").distinct()
data2 = spark.sql("select * from opd.to_ccf_cs_account").distinct()
data3 = data1.union(data2)
print("最后的 的数据量有 " + str(data3.count()))


# spark2-submit  --master yarn --deploy-mode cluster --num-executors 4 --executor-memory 10G --archives hdfs:///anaconda3.zip#anaconda3 --files /etc/spark2/conf.cloudera.spark2_on_yarn/yarn-conf/hive-site.xml
# --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./anaconda3/anaconda3/bin/python3 LishuData.py