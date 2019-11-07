# -*- coding: utf-8 -*-
# 第一行必须有，否则报中文字符非ascii码错误
# import urllib
import hashlib
import urllib.parse

import requests
import geohash2 as geo
ak = 'wZONX8P0QZForVa3k5LC2vKAt4uiK7i2'
sk = 'bubYE1teyl4NBCYw4bmrBUG8Tla0aiMg'

input = "广东省深圳市龙岗区宝安区龙华新区大浪华昌工业第五栋第一卫"
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
lng = round(ret_dic['result']['location']['lng'],2)
lat = round(ret_dic['result']['location']['lat'],2)
result = str(lng) + "----------------" + str(lat)
geohash = geo.encode(lng, lat, precision=8)
print(result)
print(geohash)
