#!/usr/bin/env bash

WDIR=$(cd `dirname $0`;pwd)
cd $WDIR

#class: com.asiainfo.ocdc.streaming.tool.areaMapping.LoadFile2Redis
#para: filePath redis/codis_host:port hashKey formatType lacColIdx cellColIdx areaColIdx [areaName(used when formatType=1)]

#run
#APPLIB_DIR=/Users/Users_datadir_docker/app-libs/streaming-app/
FWDIR=$(cd `dirname $0`/../;pwd)

CONFILE=$FWDIR/conf
CLASSPATH=$CLASSPATH:$CONFILE

APPLIB_DIR=$FWDIR/lib
for libjar in `ls $APPLIB_DIR/*jar|xargs`
do
  CLASSPATH=$CLASSPATH:$libjar
done

scala -cp $CLASSPATH \
  com.asiainfo.ocdc.streaming.source.socket.AutoProducer
