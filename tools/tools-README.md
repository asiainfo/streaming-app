# tools 模块介绍

## redis 导入程序

程序	| 用途 | 依赖的jar
--- | --- | ---
tools.redis.load.Load2Redis |	现场应用情景：用于从数据库导入用户资料信息到redis/codis	| commons-pool2-2.3.jar jedis-2.7.0.jar slf4j-api-1.7.7.jar streaming-tools-0.1.0-SNAPSHOT.jar mysql-connector-java-5.1.30.jar tomcat-jdbc-7.0.62.jar tomcat-juli-7.0.62.jar
tools.redis.load.Sync2Redis |	现场应用情景：用于从数据库导入用户资料信息到redis/codis，支持增量同步	| commons-pool2-2.3.jar jedis-2.7.0.jar slf4j-api-1.7.7.jar streaming-tools-0.1.0-SNAPSHOT.jar mysql-connector-java-5.1.30.jar tomcat-jdbc-7.0.62.jar tomcat-juli-7.0.62.jar
tools.redis.load.areamap.Load2Redis |	现场应用情景：用于从数据库导入类似区域映射信息到redis/codis，支持hash field进制转换，hash value取值映射转换	| commons-pool2-2.3.jar jedis-2.7.0.jar slf4j-api-1.7.7.jar streaming-tools-0.1.0-SNAPSHOT.jar mysql-connector-java-5.1.30.jar tomcat-jdbc-7.0.62.jar tomcat-juli-7.0.62.jar

###部署使用方法

 * 部署
   解压部署包tools-redis-soft.tar.gz
   
   ```
tar zxf tools-redis-soft.tar.gz
   ```
   
>tar ztf tools-redis-soft.tar.gz 
 tools-app-soft/redis/
 tools-app-soft/redis/bin/
 tools-app-soft/redis/conf/
 tools-app-soft/redis/lib/
 tools-app-soft/redis/logs/
 tools-app-soft/redis/lib/commons-pool2-2.3.jar
 tools-app-soft/redis/lib/jedis-2.7.0.jar
 tools-app-soft/redis/lib/log4j-1.2.17.jar
 tools-app-soft/redis/lib/mysql-connector-java-5.1.30.jar
 tools-app-soft/redis/lib/slf4j-api-1.7.7.jar
 tools-app-soft/redis/lib/slf4j-log4j12-1.7.7.jar
 tools-app-soft/redis/lib/streaming-tools-0.1.0-SNAPSHOT.jar
 tools-app-soft/redis/lib/tomcat-jdbc-7.0.62.jar
 tools-app-soft/redis/lib/tomcat-juli-7.0.62.jar
 tools-app-soft/redis/conf/areamap-file2hashes-test.xml
 tools-app-soft/redis/conf/areamap-file2onehash-test.xml
 tools-app-soft/redis/conf/areamap-jdbc2hashes-test.xml
 tools-app-soft/redis/conf/areamap-jdbc2hashes0-test.xml
 tools-app-soft/redis/conf/areamap-jdbc2onehash-test.xml
 tools-app-soft/redis/conf/areamap-load2redis-template.xml
 tools-app-soft/redis/conf/areamap-load2redis0-template.xml
 tools-app-soft/redis/conf/file2hashes-test.xml
 tools-app-soft/redis/conf/file2onehash-test.xml
 tools-app-soft/redis/conf/jdbc2hashes-test.xml
 tools-app-soft/redis/conf/jdbc2onehash-test.xml
 tools-app-soft/redis/conf/load2redis-template.xml
 tools-app-soft/redis/conf/log4j.properties
 tools-app-soft/redis/conf/sync2redis-file2hashes-test.xml
 tools-app-soft/redis/conf/sync2redis-jdbc2hashes-test.xml
 tools-app-soft/redis/conf/sync2redis-template.xml
 tools-app-soft/redis/bin/areamap-file2hashes.sh
 tools-app-soft/redis/bin/areamap-file2onehash.sh
 tools-app-soft/redis/bin/areamap-jdbc2hashes.sh
 tools-app-soft/redis/bin/areamap-jdbc2onehash.sh
 tools-app-soft/redis/bin/file2hashes.sh
 tools-app-soft/redis/bin/file2onehash.sh
 tools-app-soft/redis/bin/jdbc2hashes.sh
 tools-app-soft/redis/bin/jdbc2onehash.sh

 * 更新 jar
    
   ```
\#git clone https://github.com/asiainfo/streaming-app
git pull
cd streaming-app
mvn clean package -DskipTests -pl tools
   ```
      
   复制tools/target/streaming-tools-0.1.0-SNAPSHOT.jar到到CLASSPATH下

 * 使用指南
 
   根据配置文件模板，配置加载需求
   tools-app-soft/redis/conf/load2redis-template.xml
   tools-app-soft/redis/conf/sync2redis-template.xml
   conf/areamap-load2redis-template.xml

