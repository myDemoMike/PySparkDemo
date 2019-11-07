# -*- coding: UTF-8 -*-
# coding:utf-8

from os.path import abspath

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

warehouse_location = abspath('hdfs://10.31.1.11:8020/user/hive/warehouse')
spark = SparkSession \
    .builder \
    .appName("pyspark") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .config("hive.groupby.skewindta","true") \
    .enableHiveSupport().getOrCreate()

data11 = spark.sql(
    """
select cp.id_number,
   p.phone_number as phoneNum,
   p.customer_id as customerId,
   ca.contract_no as contractno
from opd.to_csm_cs_phones p, opd.to_csm_cs_person cp,opd.to_csm_cs_account ca ,opd.to_csm_cs_case_main cmm
where ca.syskey=cmm.syskey and cmm.customerid=p.customer_id and cp.customer_id = p.customer_id
 and ca.total_o_d_days>60
and ca.contract_no in
   (select  distinct cf.contract_no
from opd.to_csm_cs_CASE_MAIN t, opd.to_csm_cs_account cf
where t.syskey = cf.syskey and( t.flag_out_of_contact = 1
  or t.flag_dif_connection = 1))
    """
).distinct();

def getPhone(phone):
    phone_pat = re.compile(
        "[1][3,5,8,9][0-9]{9}$|^[9][2,8][0-9]{9}$|^14[5|6|7|8|9][0-9]{8}$|^16[1|2|4|5|6|7][0-9]{8}$|^17[0-8][0-9]{8}")
    res = phone_pat.search(phone)
    if res is None:
        return "0";
    else:
        return "1";

seg_udf = udf(getPhone, StringType())
data22 = data11.filter(data11.phoneNum.isNotNull())
# dropDuplicates 根据某几列去重
result = data22.distinct().filter(seg_udf(data22.phoneNum) == "1").dropDuplicates(['phoneNum'])
print("数据量" + str(result.count()))
# quchonghou    graph.test_05_09
result.write.mode("overwrite").saveAsTable("graph.test_06_10")
