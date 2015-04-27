#!/usr/bin/env bash

WDIR=$(cd `dirname $0`;pwd)
cd $WDIR

#class: com.asiainfo.ocdc.streaming.tool.areaMapping.LoadFile2Redis
#para: filePath redis/codis_host:port hashKey formatType lacColIdx cellColIdx areaColIdx [areaName(used when formatType=1)]

#run
#APPLIB_DIR=/Users/Users_datadir_docker/app-libs/streaming-app/
FWDIR=$(cd `dirname $0`/../;pwd)
APPLIB_DIR=$FWDIR/lib
for libjar in `ls $APPLIB_DIR/*jar|xargs`
do
  CLASSPATH=$CLASSPATH:$libjar
done

scala -cp $CLASSPATH \
  com.asiainfo.ocdc.streaming.tool.areaMapping.LoadFile2Redis \
  /data01/data/datadir_github/ai-projects/streaming-app-201503/testdata/mc-test.log redis1:6379 lacci2area 1 3 4 2 WLAN
