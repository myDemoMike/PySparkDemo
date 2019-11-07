# -*- coding: utf-8 -*-
# 第一行必须有，否则报中文字符非ascii码错误
# import urllib
import hashlib
import urllib.parse

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from os.path import abspath

ak = 'wZONX8P0QZForVa3k5LC2vKAt4uiK7i2'
sk = 'bubYE1teyl4NBCYw4bmrBUG8Tla0aiMg'
# ak = 'yG4eEdcAmi4GZQhy7cMRrTx8FGLmwIKU'
# sk = '6HftS2KGZxmCM7Ee7gZYdonakoAPYDt1'
warehouse_location = abspath('hdfs://10.31.1.11:8020/user/hive/warehouse')
spark = SparkSession \
    .builder \
    .appName("pyspark") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .config("hive.groupby.skewindta", "true") \
    .enableHiveSupport().getOrCreate()


def getResult(input):
    # input = "内蒙古自治区锡林郭勒盟正蓝旗内蒙古锡林郭勒上都镇敖包希热小区3号楼5单园302"
    location = input.replace('#', '')
    querystr = '/geocoder/v2/?address=' + location + '&output=json&ak=' + ak
    # 对queryStr进行转码，safe内的保留字符不转换
    encodedstr = urllib.parse.quote(querystr, safe="/:=&?#+!$,;'@()*[]")
    # encodedStr = urllib.parse.quote(queryStr)
    rawstr = encodedstr + sk
    sn = hashlib.md5(urllib.parse.quote_plus(rawstr).encode("utf-8")).hexdigest()
    url2 = "http://api.map.baidu.com/geocoder/v2/?address={0}&output=json&ak={1}&sn={2}".format(location, ak, sn)
    ret = requests.get(url2).text
    ret_dic = eval(ret)
    lng = round(ret_dic['result']['location']['lng'], 2)
    lat = round(ret_dic['result']['location']['lat'], 2)
    result = str(lng) + ":" + str(lat)
    return str(result)


# 374129
datainput = spark.sql("select * from portrait.zto_sf_car_address_to_gps limit  10")
datainput.show(20)

seg_udf = udf(getResult, StringType())

dataoutput = datainput \
    .withColumn("LngLat", round(seg_udf(datainput.address),2))

dataoutput.show(20)

# dataoutput.write.mode("overwrite").saveAsTable("graph.tianyuan_result_03_06_02")
# df.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable("")
# tianyuan_recipientAdd_tmp
# tianyuan_02_22_result_03
