# -*- coding: utf-8 -*-
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

# 初始化数据


# 初始化spark DataFrame
sc = SparkContext()
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("testDataFrame") \
        .getOrCreate()

# 初始化spark DataFrame
sentenceData = spark.createDataFrame([
    (0.0, "I like Spark"),
    (1.0, "Pandas is useful"),
    (2.0, "They are coded by Python ")
], ["label", "sentence"])
sentenceData.show()
print("====================")
# 初始化pandas DataFrame
df = pd.DataFrame([[1, 2, 3], [4, 5, 6]], index=['row1', 'row2'], columns=['c1', 'c2', 'c3'])

# 显示数据
senselect = sentenceData.select("label")

sqlContest = SQLContext(sc)
# pandas.DataFrame 转换成 spark.DataFrame
spark_df = sqlContest.createDataFrame(df)

# 显示数据
spark_df.select("c1").show()

# spark.DataFrame 转换成 pandas.DataFrame
pandas_df = spark_df.toPandas()

# 打印数据
print("spark.DataFrame 转换成 pandas.DataFrame" + "*"*10)
print(pandas_df)

