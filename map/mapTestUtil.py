# -*- coding: utf-8 -*-
# 第一行必须有，否则报中文字符非ascii码错误
# import urllib
import hashlib
import urllib.parse
import requests
import geohash2 as geo
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from os.path import abspath

ak = 'wZONX8P0QZForVa3k5LC2vKAt4uiK7i2'
sk = 'bubYE1teyl4NBCYw4bmrBUG8Tla0aiMg'

warehouse_location = abspath('hdfs://10.31.1.125:8020/user/hive/warehouse')
spark = SparkSession \
    .builder \
    .appName("pyspark") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .config("hive.groupby.skewindta", "true") \
    .enableHiveSupport().getOrCreate()


def getResult(location):
    try:
        querystr = '/geocoder/v2/?address=' + location + '&output=json&ak=' + ak
        # 对queryStr进行转码，safe内的保留字符不转换
        encodedstr = urllib.parse.quote(querystr, safe="/:=&?#+!$,;'@()*[]")
        # encodedStr = urllib.parse.quote(queryStr)
        rawstr = encodedstr + sk
        sn = hashlib.md5(urllib.parse.quote_plus(rawstr).encode("utf-8")).hexdigest()
        url2 = "http://api.map.baidu.com/geocoder/v2/?address={0}&output=json&ak={1}&sn={2}".format(location, ak, sn)
        ret = requests.get(url2).text
        ret_dic = eval(ret)
        lng = ret_dic['result']['location']['lng']
        lat = ret_dic['result']['location']['lat']
        geohash = geo.encode(lng, lat, precision=8)
        print(location + "-------" + geohash)
    except:
        geohash = "-"
        print(location + "------------------------")
    return geohash


# 23846
datainput = spark.sql("select * from portrait.tp_user_info limit 100")
datainput.show(100)

# tianyuan_recipientAdd_tmp
# tianyuan_02_22_result_03
#
