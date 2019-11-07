# coding=utf-8

from pyspark.sql import SparkSession
import jieba
from pyspark.sql.functions import *
from pyspark.sql.types import *
import reload

# 解决编码问题


reload(sys)
sys.setdefaultencoding('utf-8')

# 创建sparkSession
spark = SparkSession.builder.appName("NB Test").enableHiveSupport.getOrCreate()

# 读取hive的数据
df = spark.sql("select content,label from test.news_no_seg limit 10")
df.show()


# 定义结巴分词的方法
def seg(text):
    return ' '.join(jieba.cut(text, cut_all=True))

sed_udf = udf(seg, StringType)

# 对数据进行结巴分词
df_1 = df.withColumn('seg', sed_udf(df.content))
df_1.show()
