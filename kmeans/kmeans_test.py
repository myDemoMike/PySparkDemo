# -*- coding: utf-8 -*-

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import RFormula
from pyspark.sql import SparkSession

# 解决编码问题
import sys

sys.setdefaultencoding('utf8')

# 创建sparkSession   spark 程序的入口
spark = SparkSession.builder.appName("News Kmeans Cluster").enableHiveSupport().getOrCreate()

sql = "select * from  tmp_fk_am.zdy_client_cluster_02_ind_info_v2"
# 读取hive的数据
df = spark.sql(sql)
df.show()

# 切分训练集与预测集  未使用
splits = df.randomsplit([0.7, 0.3], 1234)
train = splits[0]
test = splits[1]

# 使用RFormula 进行特征处理  输入的特征列名为features
formula = RFormula.setFormula("~.-contract_no").setFeaturesCol("features")

# 数据特征处理
output =  formula.fit(df).transform(df)

#  使用kmeans算法  使用k-means|| 算法   输出列为prediction
kmeans = KMeans(featuresCol="features", predictionCol="prediction", k=10,
                initMode="k-means||", initSteps=2, tol=1e-4, maxIter=20, seed=1024)

# 训练数据
model = kmeans.fit(df)

# 得到模型的中心点
centers = model.clusterCenters()
print(" 中心点:Cluster Centers: ")
for center in centers:
    print(center)

# 计算损失函数qq
wssse = model.computeCost(output)

# 输出聚类后的结果
result = model.fit(output).transform(output).drop('prediction')

# 将聚类结果保存为一张表
result.write.mode("overwrite").saveAsTable("tmp_lishu.test2222")