#!/usr/bin/env bash

FWDIR=$(cd `dirname $0`/..;pwd)
cd $FWDIR

# dependent jar
#streaming-core-0.1.0-SNAPSHOT.jar
#kafka_2.10-0.8.1.1.jar


APPLIB_DIR=/Users/Users_datadir_docker/app-libs/streaming-app/
for libjar in `ls $APPLIB_DIR/*jar|xargs`
do
  CLASSPATH=$CLASSPATH:$libjar
done


scala -cp $CLASSPATH \
  com.asiainfo.ocdc.streaming.tool.Kafka2Hdfs \
  conf/kafka2hdfs-test.xml

