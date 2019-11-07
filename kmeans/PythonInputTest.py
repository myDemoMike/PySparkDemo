# coding: utf-8

from os.path import abspath

from pyspark.ml.clustering import KMeans
from pyspark.sql import SparkSession

warehouse_location = abspath('hdfs://10.31.1.11:8020/user/hive/warehouse')
spark = SparkSession \
    .builder \
    .appName("pyspark") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport().getOrCreate()

sql_salary_classification = "select * from  tmp_fk_am.zdy_client_cluster_02_ind_info_v2 limit 100"
# 读取hive的数据
df = spark.sql(sql_salary_classification)
print(df.show())

kmeans = KMeans(df,n_clusters=8, random_state=9)
model = kmeans.fit(df)
#print("------------------ 分界点  --------------------------")

label_pred = model.labels_ #获取聚类标签
centroids = model.cluster_centers_ #获取聚类中心
inertia = model.inertia_ # 获取聚类准则的总

# df = pd.DataFrame(dataSet,index=labels,columns=['x','y'])
for i in range(5,30,1):
    clf = KMeans(n_clusters=i)
    s = clf.fit(df)
    print (i , clf.inertia_)
#df_labels = pd.DataFrame(label_pred,columns=['clus_res'])
# df = pd.concat([trainData, df_labels], axis = 1)
