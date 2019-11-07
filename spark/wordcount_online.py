import re
from pyspark import SparkConf, SparkContext

conf = SparkConf()
conf.setMaster("local").setAppName("wordcount")

sc = SparkContext(conf=conf)


# lower the text and split bu regular expression

def normalizewords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


input = sc.textFile("hdfs://master:9000/The_Man_of_Property.txt")
words = input.flatMap(normalizewords)

# sort by word
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda x, y: (y, x)).sortByKey()

results = wordCountsSorted.collect()

wordCountsSorted.map(lambda x: '\t'.join([x[1], str(x[0])])).saveAsTextFile(
    "hdfs://master:9000/python_word_count_output")

# spark-submit wordcount_online.py
