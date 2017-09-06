#! /bin/bash
#hdfs dfs -rm -r /user/tele/data/nanning/feature/graph/localSimTest

#export YARN_CONF_DIR=/opt/hadoop-2.2.0/etc/hadoop

$SPARK_HOME/bin/spark-submit \
--master spark://dell06:7077 \
--executor-memory 19g \
--total-executor-cores 96 \
./TravelPlanning-1.0.jar \
/user/tele/chenqingqing/GroupDetection/201508/mergeEdges \
/user/tele/chenqingqing/GroupDetection/201508/Community \
200 \
1 \

