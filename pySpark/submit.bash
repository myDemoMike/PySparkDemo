spark2-submit  --master yarn \
--deploy-mode cluster \
--num-executors 4 \
--executor-memory 10G \
--archives hdfs:///anaconda3.zip#anaconda3 \
--files /etc/spark2/conf.cloudera.spark2_on_yarn/yarn-conf/hive-site.xml \
--conf spark.yarn.appMasterEnvq.PYSPARK_PYTHON=./anaconda3/anaconda3/bin/python3 t3.py