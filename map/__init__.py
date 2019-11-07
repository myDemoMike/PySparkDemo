# -*- coding: utf-8 -*-
# 第一行必须有，否则报中文字符非ascii码错误
# import urllib
import hashlib
import urllib.parse
import requests
import geohash2 as geo

# import json

#ak = "wZONX8P0QZForVa3k5LC2vKAt4uiK7i2"  # --self
#sk = "bubYE1teyl4NBCYw4bmrBUG8Tla0aiMa"
ak = 'Nt96nImmUWSu1p4ESQeMY3W9vZem2qmV'  # -- 测试
sk = 'nGfms2LwIQ7iZEfLG3nsXSYKKIOcAo65'
# ak = 'yG4eEdcAmi4GZQhy7cMRrTx8FGLmwIKU'
# sk = '6HftS2KGZxmCM7Ee7gZYdonakoAPYDt1'
def getResult(location, length):
    try:
        # location = input.replace('#','')
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
        geohash = geo.encode(lng, lat)
        print(str(location) + "----"+ str(geohash))
    except:
        geohash = "-"
    return geohash


#print(getResult("山西省运城市盐湖区条山街同城公寓402",8))

#print(getResult("深圳市福田区彩田路7018号新浩e都A座19楼", 8))
print(getResult("北京市昌平区科技园区昌盛路18号B1座1-5层", 8))