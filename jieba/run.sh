#!/usr/bin/env bash
PYSPARK_PYTHON=./ANACONDA/dev/bin/python
cd /usr/local/src/spark-2.2.0-bin-hadoop2.7/ \
./bin/spark-submit \
        --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./ANACONDA/dev/bin/python \
        --master yarn-cluster \
        --jars /usr/local/src/apache-hive-1.2.2-bin/lib/mysql-connector-java-5.1.34.jar,/usr/local/src/spark-2.2.0-bin-hadoop2.7/jars/datanucleus-api-jdo-3.2.6.jar,/usr/local/src/spark-2.2.0-bin-hadoop2.7/jars/datanucleus-core-3.2.10.jar,/usr/local/src/spark-2.2.0-bin-hadoop2.7/jars/datanucleus-rdbms-3.2.9.jar,/usr/local/src/spark-2.2.0-bin-hadoop2.7/jars/guava-14.0.1.jar \
        --files /usr/local/src/spark-2.2.0-bin-hadoop2.7/conf/hive-site.xml \
        --archives /usr/local/src/dev.zip#ANACONDA \   #自己导入需要的包
       /usr/local/src/jiebaPython.py
