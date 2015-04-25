#class: com.asiainfo.ocdc.streaming.tool.areaMapping.LoadFile2Redis
#para: filePath redis/codis_host:port hashKey formatType lacColIdx cellColIdx areaColIdx [areaName(used when formatType=1)]

#run
CLASSPATH=~/.m2/repository/redis/clients/jedis/2.7.0/jedis-2.7.0.jar:~/.m2/repository/org/apache/commons/commons-pool2/2.3/commons-pool2-2.3.jar:core/target/streaming-core-0.1.0-SNAPSHOT.jar
scala -cp $CLASSPATH \
  com.asiainfo.ocdc.streaming.tool.areaMapping.LoadFile2Redis \
  /data01/data/datadir_github/ai-projects/streaming-app-201503/testdata/mc-test.log redis1:6379 lacci2area 1 3 4 2 WLAN
