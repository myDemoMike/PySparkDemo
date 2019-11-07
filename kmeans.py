# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
import jieba
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import HashingTF,Tokenizer,IDF

import sys
reload(sys)
sys.setdefaultencoding('utf8')

# 创建sparkSession
spark = SparkSession.builder.appName("Kmeans Test").enableHiveSupport().getOrCreate()

# 读取hive的数据
df = spark.sql("select content from badou.new_no_seg")
df.show()

# 定义结巴切词方法
def seg(text):
    return ' '.join(jieba.cut(text,cut_all=True))
seg_udf = udf(seg, StringType())

# 对数据进行结巴切词
df_seg = df.withColumn('seg',seg_udf(df.content)).select('seg')
df_seg.show()
# 将分词做成
tokenizer = Tokenizer(inputCol='seg',outputCol='words')
df_seg_arr=tokenizer.transform(df_seg).select('words')
df_seg_arr.show()

# 切词之后的文本特征的处理
tf = HashingTF(numFeatures=1<<18,binary=False,inputCol='words',outputCol='rawfeatures')
df_tf = tf.transform(df_seg_arr).select('rawfeatures')
df_tf.show()

idf = IDF(inputCol='rawfeatures',outputCol='features')
idfModel = idf.fit(df_tf)
df_tfidf = idfModel.transform(df_tf)
df_tfidf.show()


# 切分训练集和预测集
splits = df_tfidf.randomSplit([0.7, 0.3], 1234)
train = splits[0]
test = splits[1]

# 定义模型
kmeans = KMeans(featuresCol="features", predictionCol="prediction", k=6,
                initMode="k-means||", initSteps=5, tol=1e-4, maxIter=20, seed=None)
# 模型训练
model = kmeans.fit(train)
# 获取中心点
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
	print(center)
wssse = model.computeCost(train)
print("Within Set Sum of Squared Errors = " + str(wssse))
