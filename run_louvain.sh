#! /bin/bash
#hdfs dfs -rm -r /user/tele/data/nanning/feature/graph/localSimTest

#export YARN_CONF_DIR=/opt/hadoop-2.2.0/etc/hadoop

$SPARK_HOME/bin/spark-submit \
--class wtist.driver.louvainDGA \
--master spark://dell06:7077 \
--executor-memory 19g \
--total-executor-cores 96 \
./TravelPlanning-1.0.jar \
/user/tele/chenqingqing/201508EdgeForLpaByCoLocation_washed_above100 \
/user/tele/chenqingqing/201508LouvainByColocation \
200 \
1 \

