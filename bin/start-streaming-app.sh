#!/usr/bin/env bash

FWDIR="$(cd `dirname $0`/..; pwd)"
Time=`date +%Y-%m-%d#%H:%M`
log=$FWDIR/log/streaming-app-$Time.log
nohup $FWDIR/bin/spark-class com.asiainfo.ocdc.streaming.StreamingApp $@ > "$log" &