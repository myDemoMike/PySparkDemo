import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
import seaborn as sns

spark = SparkSession \
    .builder \
    .appName("pyspark") \
    .getOrCreate()
user_data = spark.sparkContext.textFile("./data/u.user")
# 初步看一样数据的样子
print(user_data.first())

user_fields = user_data.map(lambda line: line.split('|'))
# num_user = user_fields.map(lambda field: field[0]).count()
# num_gender = user_fields.map(lambda field: field[2]).distinct().count()
# num_occupation = user_fields.map(lambda field: field[3]).distinct().count()
# num_zipcode = user_fields.map(lambda field: field[4]).distinct().count()
# print("共有用户：%d户,性别：%d类,职业%d类,邮编：%d种" % (num_user, num_gender, num_occupation, num_zipcode))

# 下面来查看年龄的分布
ages = user_fields.map(lambda x: int(x[1])).collect()  # 提出年龄所有数据项
plt.hist(ages, 20, color='red')
fig = plt.gcf()

# fig.set_size_inches(16, 10)
# plt.show()

# 下面统计职业的频率直方图
# 当数据不大时，我们可以用这种方法将所有元素收集起来
fig = plt.figure()
occupations = user_fields.map(lambda field: field[3]).collect()
ax = sns.countplot(x=occupations)
plt.show()
