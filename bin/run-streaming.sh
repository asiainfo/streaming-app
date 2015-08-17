FWDIR=$(cd `dirname $0`/..; pwd)

SPARK_HOME=$FWDIR
SPARK_JAVA_OPTS="-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -X:loggc:${SPARK_HOME}/logs/app-gc.log -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${SPARK_HOME}/logs/"

for jarFile in `ls $SPARK_HOME/lib/*jar`
do
  SPARK_CLASSPATH=$SPARK_CLASSPATH:$jarFile
done
SPARK_CLASSPATH=$SPARK_CLASSPATH:/home/spark/app/hadoop-2.3.0-cdh5.0.2-och3.2.0/share/hadoop/common/lib/hadoop-lzo.jar:/home/spark/app/hbase/conf:/home/spark/app/hadoop/lib/nati
ve
echo SPARK_CLASSPATH=$SPARK_CLASSPATH

cd $FWDIR
nohup sh bin/spark-submit \
  --class com.asiainfo.ocdc.streaming.MainFrame \
  --driver-memory 512m \
  --executor-memory 512m \
  lib/streaming-core*jar \
  spark://spark1:4050,spark2:4050 OCDC-Streaming-App 2>&1 > logs/`basename $0`.log &
  #local[3] OCDC-Streaming-App 2>&1 > logs/OCDC-Streaming.log &

#tail -30f logs/`basename $0`.log