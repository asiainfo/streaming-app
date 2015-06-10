# tools 模块介绍

## redis 导入程序

程序	| 用途 | 依赖的jar
--- | --- | ---
tools.redis.load.jdbc2Hashes |	现场应用情景：用于从数据库导入用户资料信息到redis/codis	| commons-pool2-2.3.jar jedis-2.7.0.jar slf4j-api-1.7.7.jar streaming-tools-0.1.0-SNAPSHOT.jar mysql-connector-java-5.1.30.jar tomcat-jdbc-7.0.62.jar tomcat-juli-7.0.62.jar
tools.redis.load.jdbc2SingleHash |	现场应用场景: 用于从数据库导入类似基站区域映射信息到redis/codis，没有进制转换和数据映射的情景	| 
tools.redis.loadAreaMap.Jdbc2SingleHash |	现场应用场景：用于从数据库导入基站区域映射表到redis/codis，支持进制转换和数据映射的情景 |	
tools.redis.load.File2Hashes |	现场应用情景：用于从文件导入用户资料信息到redis/codis |	"commons-pool2-2.3.jar jedis-2.7.0.jar slf4j-api-1.7.7.jar streaming-tools-0.1.0-SNAPSHOT.jar"
tools.redis.load.File2SingleHash |	现场应用场景: 用于从文件导入类似基站区域映射信息到redis/codis，没有进制转换和数据映射的情景 |	
tools.redis.loadAreaMap.File2SingleHash |	现场应用场景：用于从文件导入基站区域映射表到redis/codis，支持进制转换和数据映射的情景 |	

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
tools-app-soft/redis/lib/commons-pool2-2.3.jar
tools-app-soft/redis/lib/jedis-2.7.0.jar
tools-app-soft/redis/lib/mysql-connector-java-5.1.30.jar
tools-app-soft/redis/lib/slf4j-api-1.7.7.jar
tools-app-soft/redis/lib/streaming-tools-0.1.0-SNAPSHOT.jar
tools-app-soft/redis/lib/tomcat-jdbc-7.0.62.jar
tools-app-soft/redis/lib/tomcat-juli-7.0.62.jar
tools-app-soft/redis/conf/file2hashes-test.xml
tools-app-soft/redis/conf/file2singlehash-test.xml
tools-app-soft/redis/conf/jdbc2hashes-test.xml
tools-app-soft/redis/conf/jdbc2singlehash-test.xml
tools-app-soft/redis/conf/loadAreaMap-file2singlehash-test.xml
tools-app-soft/redis/conf/loadAreaMap-jdbc2singlehash-test.xml
tools-app-soft/redis/bin/areamap-file2singlehash.sh
tools-app-soft/redis/bin/areamap-jdbc2singlehash.sh
tools-app-soft/redis/bin/file2hashes.sh
tools-app-soft/redis/bin/file2singlehash.sh
tools-app-soft/redis/bin/jdbc2hashes.sh
tools-app-soft/redis/bin/jdbc2singlehash.sh

 * 更新 jar
    
   ```
\#git clone https://github.com/asiainfo/streaming-app
git pull
cd streaming-app
   ```
      
   复制tools/target/streaming-tools-0.1.0-SNAPSHOT.jar到到CLASSPATH下

 * 使用示例

   测试 tools.redis.load.jdbc2Hashes
示例：导入需求：
      1导入mysql数据库中表tab_test_jdbc2hashes的数据到redis/codis,
      2每行记录对应一个redis中的1个hash，id1,id2两列使用冒号拼接，再添加前缀 jdbc2hashes:
      3 col3,col4两列的取值分别作为 field1,field2(field1/2是 hash的属性名)的值导入
      4 加载采用覆盖方式

   测试数据如下： 100条记录
      >mysql> select * from tab_test_jdbc2hashes limit 5;
+------+------+--------------+--------------+
| id1  | id2  | col3         | col4         |
+------+------+--------------+--------------+
|    0 |    0 | value-test-0 | value-test-0 |
|    0 |    1 | value-test-0 | value-test-1 |
|    0 |    2 | value-test-0 | value-test-2 |
|    0 |    3 | value-test-0 | value-test-3 |
|    0 |    4 | value-test-0 | value-test-4 |
+------+------+--------------+--------------+
5 rows in set (0.00 sec)

   配置
      vi conf/tools-redis-loadfile2hashes-test.xml

      ```
<configuration>
    <redis>
        <servers>codis1:29001, codis1:29002</servers> <!-- 设置redis/codis地址, 格式为 ip:port -->
        <database>0</database> <!-- 设置导入到redis/codis哪个db，默认有16个db，编号0-15 -->
        <timeout>10000</timeout> <!-- 设置连接redis/codis最大超时,单位ms -->
        <password></password> <!-- 设置连接redis/codis的密码 -->
    </redis>

    <jedisPool>
        <maxTotal>100</maxTotal> <!-- 设置连接池最大连接数 -->
        <maxIdle>15</maxIdle> <!-- 设置连接池最大空闲连接数 -->
        <minIdle>0</minIdle> <!-- 设置连接池最小空闲连接数 -->
    </jedisPool>

    <jdbcPool>
        <maxActive>100</maxActive>
        <initialSize>10</initialSize>
        <maxIdle>100</maxIdle>
        <minIdle>10</minIdle>
        <maxAage>0</maxAage>
    </jdbcPool>

    <load>
        <from>db</from>

        <driver>com.mysql.jdbc.Driver</driver>
        <url>jdbc:mysql://mysql1:3306/test</url> <!-- 设置数据库的URL -->
        <username>root</username>
        <password>mysql123</password>
        <table>tab_test_jdbc2hashes</table>

        <hashNamePrefix>jdbc2hashes:</hashNamePrefix> <!-- 设置hashkey名前缀 -->
        <hashColumnNames>id1,id2</hashColumnNames> <!-- 设置 hashkey取值字段的字段名,可以使用多个字段组合作为hashkey名，使用逗号分隔 -->
        <hashSeperator>:</hashSeperator> <!-- 设置 hashkey名多个字段值组合的分隔符 -->

        <valueColumnNames>col3,col4</valueColumnNames> <!-- 设置加载列的列名，可以加载多个列数据，使用逗号分隔 -->
        <fieldNames>field1, field2</fieldNames> <!-- 设置加载数据时Redis中对应的的hash属性名 -->

        <batchLimit>30</batchLimit> <!-- 设置每个线程每批次加载的记录数量 -->
        <batchLimit.redis>6</batchLimit.redis> <!-- 设置redis每批次加载(pipeline_hset, hmset)的记录数量 -->

        <numThreads>2</numThreads> <!-- 设置并行线程数-->
        <method>hmset</method> <!-- 设置加载方法 hset, hmset, pipeline_hset  -->
        <!--<method>pipeline_hset</method> -->

        <overwrite>true</overwrite> <!-- 设置加载过程中采用覆盖，还是追加 -->
        <appendSeperator>,</appendSeperator> <!-- 设置加载过程中追加时使用的分隔符 -->

    </load>

</configuration>
      ```

      脚本
      vi bin/jdbc2hashes.sh

      ```
FWDIR=$(cd `dirname $0`/../;pwd)

APPLIB_DIR=$FWDIR/lib
for libjar in `ls $APPLIB_DIR/*jar|xargs`
do
  CLASSPATH=$CLASSPATH:$libjar
done

echo CLASSPATH=$CLASSPATH

CMD="scala -cp $CLASSPATH tools.redis.load.Jdbc2Hashes $FWDIR/conf/jdbc2hashes-test.xml"
echo "CMD="+$CMD
nohup $CMD 2>&1 >> $FWDIR/logs/`basedir $0`.log &
      ```

      chmod +x bin/jdbc2hashes.sh
      bin/jdbc2hashes.sh


      验证数据
      >[hadoop@codis1 test-local-cluster]$ sh ck.sh
codis1 7380 dbsize =  50
codis1 7381 dbsize =  0
codis1 7382 dbsize =  50
codis1 7383 dbsize =  0
100
[hadoop@codis1 test-local-cluster]$

       >127.0.0.1:7380> hgetall jdbc2hashes:3:9
1) "field1"
2) "value-test-3"
3) "field2"
4) "value-test-9"
127.0.0.1:7380>

测试 tools.redis.load.jdbc2SingleHash

示例导入需求：
      1 导入mysql中表tab_test_jdbc2singlehash的数据到redis/codis, 每行记录对应redis中指定hash的一个元素(一对field/value对)
      2 id1,id2两列用冒号拼接作为hash的field
      3 col3,col4两列数据使用井号拼接作为hash的value
      4 加载采用覆盖方式

测试数据：100条记录
      >mysql> select * from tab_test_jdbc2singlehash limit 5;
+------+------+--------------+--------------+
| id1  | id2  | col3         | col4         |
+------+------+--------------+--------------+
|    0 |    0 | value-test-0 | value-test-0 |
|    0 |    1 | value-test-0 | value-test-1 |
|    0 |    2 | value-test-0 | value-test-2 |
|    0 |    3 | value-test-0 | value-test-3 |
|    0 |    4 | value-test-0 | value-test-4 |
+------+------+--------------+--------------+
5 rows in set (0.00 sec)

配置
      vi conf/jdbc2singlehash-test.xml

      ```
<configuration>
    <redis>
        <servers>codis1:29001, codis1:29002</servers> <!-- 设置redis/codis地址, 格式为 ip:port -->
        <database>0</database> <!-- 设置导入到redis/codis哪个db，默认有16个db，编号0-15 -->
        <timeout>10000</timeout> <!-- 设置连接redis/codis最大超时,单位ms -->
        <password></password> <!-- 设置连接redis/codis的密码 -->
    </redis>

    <jedisPool>
        <maxTotal>100</maxTotal> <!-- 设置连接池最大连接数 -->
        <maxIdle>15</maxIdle> <!-- 设置连接池最大空闲连接数 -->
        <minIdle>0</minIdle> <!-- 设置连接池最小空闲连接数 -->
    </jedisPool>

    <jdbcPool>
        <maxActive>100</maxActive>
        <initialSize>10</initialSize>
        <maxIdle>100</maxIdle>
        <minIdle>10</minIdle>
        <maxAage>0</maxAage>
    </jdbcPool>

    <load>
        <from>db</from>

        <driver>com.mysql.jdbc.Driver</driver>
        <url>jdbc:mysql://mysql1:3306/test</url> <!-- 设置数据库的URL -->
        <username>root</username>
        <password>mysql123</password>
        <table>tab_test_jdbc2singlehash</table>

        <hashName>jdbc2singlehash</hashName> <!-- 设置hashkey名 -->
        <fieldColumnNames>id1,id2</fieldColumnNames> <!-- 设置 hashkey取值字段的字段名,可以使用多个字段组合作为hashkey名，使用逗号分隔 -->
        <fieldSeperator>:</fieldSeperator> <!-- 设置 hashkey名多个字段值组合的分隔符 -->

        <valueColumnNames>col3,col4</valueColumnNames> <!-- 设置加载列的列名，可以加载多个列数据，使用逗号分隔 -->
        <valueSeperator>#</valueSeperator> <!-- 设置 hashkey名多个字段值组合的分隔符 -->

        <batchLimit>30</batchLimit> <!-- 设置每个线程每批次加载的记录数量 -->
        <batchLimit.redis>6</batchLimit.redis> <!-- 设置redis每批次加载(pipeline_hset, hmset)的记录数量 -->

        <numThreads>2</numThreads> <!-- 设置并行线程数-->
        <method>hmset</method> <!-- 设置加载方法 hset, hmset, pipeline_hset  -->
        <!--<method>pipeline_hset</method> -->

        <overwrite>true</overwrite> <!-- 设置加载过程中采用覆盖，还是追加 -->
        <appendSeperator>,</appendSeperator> <!-- 设置加载过程中追加时使用的分隔符 -->

    </load>

</configuration>
      ```

      脚本

      ```
FWDIR=$(cd `dirname $0`/../;pwd)

APPLIB_DIR=$FWDIR/lib
for libjar in `ls $APPLIB_DIR/*jar|xargs`
do
  CLASSPATH=$CLASSPATH:$libjar
done

echo CLASSPATH=$CLASSPATH

CMD="scala -cp $CLASSPATH tools.redis.load.Jdbc2SingleHash $FWDIR/conf/jdbc2singlehash-test.xml"
echo "CMD="+$CMD
nohup $CMD 2>&1 >> $FWDIR/logs/`basedir $0`.log &
      ```

      chmod +x bin/jdbc2singlehash.sh
      bin/jdbc2singlehash.sh

      验证数据
      >127.0.0.1:7382> hlen jdbc2singlehash
(integer) 100
127.0.0.1:7382> hgetall jdbc2singlehash
  1) "0:5"
  2) "value-test-0#value-test-5"
  3) "0:1"
  4) "value-test-0#value-test-1"
……


测试 tools.redis.loadAreaMap.Jdbc2SingleHash

示例：导入需求：
      1 导入mysql中表tab_areamap_jdbc2singlehash的数据到redis, 每行数据对应redis中指定hash的一个元素(一对field/value对)
      2 id1,id2两列数据使用冒号拼接作为hash的field取值
      3 col4 列的数据如果是1（区域标识位），映射为指定的区域名 WLAN
      4 加载时采用追加方式不覆盖已存在数据,追加分隔符是逗号

      测试数据 100条记录
      >mysql> select * from tab_areamap_jdbc2singlehash limit 5;
+------+------+--------+------+
| id1  | id2  | col3   | col4 |
+------+------+--------+------+
|  100 |  100 | area-0 | 0    |
|  100 |  101 | area-1 | 1    |
|  100 |  102 | area-2 | 0    |
|  100 |  103 | area-3 | 1    |
|  100 |  104 | area-4 | 0    |
+------+------+--------+------+
5 rows in set (0.00 sec)

      配置

      vi conf/loadAreaMap-jdbc2singlehash-test.xml

      ```
<configuration>
    <redis>
        <servers>codis1:29001, codis1:29002</servers> <!-- 设置redis/codis地址, 格式为 ip:port -->
        <database>4</database> <!-- 设置导入到redis/codis哪个db，默认有16个db，编号0-15 -->
        <timeout>10000</timeout> <!-- 设置连接redis/codis最大超时,单位ms -->
        <password></password> <!-- 设置连接redis/codis的密码 -->
    </redis>

    <jedisPool>
        <maxTotal>100</maxTotal> <!-- 设置连接池最大连接数 -->
        <maxIdle>15</maxIdle> <!-- 设置连接池最大空闲连接数 -->
        <minIdle>0</minIdle> <!-- 设置连接池最小空闲连接数 -->
    </jedisPool>

    <jdbcPool>
        <maxActive>100</maxActive>
        <initialSize>10</initialSize>
        <maxIdle>100</maxIdle>
        <minIdle>10</minIdle>
        <maxAage>0</maxAage>
    </jdbcPool>

    <load>
        <from>db</from>

        <driver>com.mysql.jdbc.Driver</driver>
        <url>jdbc:mysql://mysql1:3306/test</url> <!-- 设置数据库的URL -->
        <username>root</username>
        <password>mysql123</password>
        <table>tab_areamap_jdbc2singlehash</table>

        <hashName>areaMap1</hashName> <!-- 设置hashkey名 -->

        <fieldColumnNames>id1,id2</fieldColumnNames> <!-- 设置哪几列作为 hash 的 field, 支持取多列数据进行组合 -->
        <fieldSeperator>:</fieldSeperator> <!-- 设置多列数据组合hash 的 field名时使用的分隔符 -->

        <conversion10to16.columnNames>id1,id2</conversion10to16.columnNames> <!-- 设置取值时需要进制转换的字段索引 -->

        <valueColumnName>col4</valueColumnName> <!-- 设置哪几列作为 hash 的 value，支持写多个，会进行拼接 -->
        <valueMapEnabled>true</valueMapEnabled> <!-- 设置是否进行value取值映射 -->
        <valueMap>WLAN</valueMap> <!-- 设置value的映射值 -->

        <batchLimit>30</batchLimit> <!-- 设置每个线程每批次加载的记录数量 -->
        <batchLimit.redis>6</batchLimit.redis> <!-- 设置redis每批次加载(pipeline_hset, hmset)的记录数量 -->

        <numThreads>2</numThreads>
        <method>hmset</method> <!-- 设置加载方法 hset, hmset, pipeline_hset  -->
        <!--<method>pipeline_hset</method>-->

        <overwrite>false</overwrite> <!-- 设置加载过程中采用覆盖，还是追加 -->
        <appendSeperator>,</appendSeperator> <!-- 设置加载过程中追加时使用的分隔符 -->

    </load>

</configuration>
      ```

      脚本

      vi ./bin/areamap-jdbc2singlehash.sh

      ```
FWDIR=$(cd `dirname $0`/../;pwd)

APPLIB_DIR=$FWDIR/lib
for libjar in `ls $APPLIB_DIR/*jar|xargs`
do
  CLASSPATH=$CLASSPATH:$libjar
done

CMD="scala -cp $CLASSPATH tools.redis.loadAreaMap.Jdbc2SingleHash $FWDIR/conf/loadAreaMap-jdbc2singlehash-test.xml"
echo "CMD="+$CMD
nohup $CMD 2>&1 >> $FWDIR/logs/`basedir $0`.log &
      ```

      chmod +x ./bin/areamap-jdbc2singlehash.sh
      ./bin/areamap-jdbc2singlehash.sh

      验证数据
      >127.0.0.1:7382> hlen areaMap1
(integer) 50
127.0.0.1:7382> hgetall areaMap1
  1) "103:100"
  2) "WLAN"
  3) "103:108"
  4) “WLAN"


测试 tools.redis.load.File2Hashes

      示例：导入需求：
      1 从指定文件导入数据到redis/codis，每行记录对应redis中的1个hash
      2 指定位置索引(0,1)的两列数据使用冒号拼接，作为hash名的1部分，和指定前缀load2hashes: 一起拼接为hash名
      3 指定位置索引(2,3)的两列数据分别作为指定属性名(field1,field2)的取值
      4 加载时采用覆盖方式

      测试数据
      >tsingfuMBP:bin tsingfu$ head /data01/data/datadir_github/ai-projects/streaming-app-201503/tools/src/test/resources/load2hashes-thread-test.data
0,0,value1-test-0,value2-test-0
0,1,value1-test-0,value2-test-1
0,2,value1-test-0,value2-test-2
0,3,value1-test-0,value2-test-3
0,4,value1-test-0,value2-test-4
0,5,value1-test-0,value2-test-5
0,6,value1-test-0,value2-test-6
0,7,value1-test-0,value2-test-7
0,8,value1-test-0,value2-test-8
0,9,value1-test-0,value2-test-9
tsingfuMBP:bin tsingfu$

      配置

      vi conf/file2hashes-test.xml

      ```
<configuration>
    <redis>
        <servers>codis1:29001, codis1:29002</servers> <!-- 设置redis/codis地址, 格式为 ip:port -->
        <database>0</database> <!-- 设置导入到redis/codis哪个db，默认有16个db，编号0-15 -->
        <timeout>10000</timeout> <!-- 设置连接redis/codis最大超时,单位ms -->
        <password></password> <!-- 设置连接redis/codis的密码 -->
    </redis>

    <jedisPool>
        <maxTotal>100</maxTotal> <!-- 设置连接池最大连接数 -->
        <maxIdle>15</maxIdle> <!-- 设置连接池最大空闲连接数 -->
        <minIdle>0</minIdle> <!-- 设置连接池最小空闲连接数 -->
    </jedisPool>

    <load>
        <from>file</from>

        <filename>/data01/data/datadir_github/ai-projects/streaming-app-201503/tools/src/test/resources/load2hashes-thread-test.data</filename> <!-- 设置数据文件的路径 -->
        <fileEncode>UTF-8</fileEncode> <!-- 设置数据文件的编码格式 -->
        <columnSeperator>,</columnSeperator> <!-- 设置数据文件字段分隔符 -->

        <hashNamePrefix>load2hashes:</hashNamePrefix> <!-- 设置hashkey名前缀 -->
        <hashIdxes>0,1</hashIdxes> <!-- 设置 hashkey取值字段的索引位置,可以使用多个列组合作为hashkey名，使用逗号分隔 -->
        <hashSeperator>:</hashSeperator> <!-- 设置 hashkey名多个字段值组合的分隔符 -->

        <valueIdxes>2,3</valueIdxes> <!-- 设置加载列的索引，可以加载多个列数据，使用逗号分隔 -->
        <fieldNames>field1, field2</fieldNames> <!-- 设置加载数据对应的的属性名 -->

        <batchLimit>30</batchLimit> <!-- 设置每个线程每批次加载的记录数量 -->
        <batchLimit.redis>6</batchLimit.redis> <!-- 设置redis每批次加载(pipeline_hset, hmset)的记录数量 -->

        <numThreads>2</numThreads> <!-- 设置并行线程数-->
        <method>hmset</method> <!-- 设置加载方法 hset, hmset, pipeline_hset  -->
        <!--<method>pipeline_hset</method> -->

        <overwrite>true</overwrite> <!-- 设置加载过程中采用覆盖，还是追加 -->
        <appendSeperator>,</appendSeperator> <!-- 设置加载过程中追加时使用的分隔符 -->

    </load>

    <description>
        reids部分：
          servers：设置 redis/codis 的服务ip地址和端口
          database: 设置导入到哪个 db
          timeout: 设置连接的超时，单位 ms
          password: 设置访问密码

        jedisPool部分：
          maxTotal: 设置连接池最大连接数
          maxIdle: 设置连接池最大空闲连接数
          minIdle: 设置连接池最小空闲连接数


    </description>

</configuration>

      ```

      脚本
      vi bin/file2hashes.sh
      ```
FWDIR=$(cd `dirname $0`/../;pwd)

APPLIB_DIR=$FWDIR/lib
for libjar in `ls $APPLIB_DIR/*jar|xargs`
do
  CLASSPATH=$CLASSPATH:$libjar
done

CMD="scala -cp $CLASSPATH tools.redis.load.File2Hashes $FWDIR/conf/file2hashes-test.xml"
echo "CMD="+$CMD
nohup $CMD 2>&1 >> $FWDIR/logs/`basedir $0`.log &
      ```

      chmod +x bin/file2hashes.sh
      bin/file2hashes.sh


      验证数据
      >[hadoop@codis1 test-local-cluster]$ sh ck.sh
codis1 7380 dbsize =  50
codis1 7381 dbsize =  0
codis1 7382 dbsize =  50
codis1 7383 dbsize =  0
100
[hadoop@codis1 test-local-cluster]$

      >127.0.0.1:7380> hgetall load2hashes:8:3
1) "field1"
2) "value1-test-8"
3) "field2"
4) "value2-test-3"
127.0.0.1:7380>

测试tools.redis.load.File2SingleHash

      示例：导入需求：
      1 从指定文件导入数据到redis/codis指定hash中，每行记录对应redis中指定hash的一个元素
      2 指定位置索引(0,1)的两列数据使用冒号拼接，作为一个field
      3 指定位置索引(2,3)的两列数据使用井号拼接，作为一个value
      4 加载时采用覆盖方式

      配置
      vi conf/file2singlehash-test.xml

      ```
<configuration>
    <redis>
        <servers>codis1:29001, codis1:29002</servers> <!-- 设置redis/codis地址, 格式为 ip:port -->
        <database>4</database> <!-- 设置导入到redis/codis哪个db，默认有16个db，编号0-15 -->
        <timeout>10000</timeout> <!-- 设置连接redis/codis最大超时,单位ms ,默认2000 -->
        <password></password> <!-- 设置连接redis/codis的密码 -->
    </redis>

    <jedisPool>
        <maxTotal>100</maxTotal> <!-- 设置连接池最大连接数 -->
        <maxIdle>15</maxIdle> <!-- 设置连接池最大空闲连接数 -->
        <minIdle>0</minIdle> <!-- 设置连接池最小空闲连接数 -->
    </jedisPool>

    <load>
        <from>file</from>

        <filename>/data01/data/datadir_github/ai-projects/streaming-app-201503/tools/src/test/resources/load2hashes-thread-test.data</filename> <!-- 设置数据文件的路径 -->
        <fileEncode>UTF-8</fileEncode> <!-- 设置数据文件的编码格式 -->
        <columnSeperator>,</columnSeperator> <!-- 设置数据文件字段分隔符 -->

        <hashName>load2singlehash</hashName> <!-- 设置hashkey名 -->

        <fieldIdxes>0,1</fieldIdxes> <!-- 设置哪几列作为 hash 的 field, 支持取多列数据进行组合 -->
        <fieldSeperator>:</fieldSeperator> <!-- 设置多列数据组合hash 的 field名时使用的分隔符 -->
        <valueIdxes>2,3</valueIdxes> <!-- 设置哪几列作为 hash 的 value，支持写多个，会进行拼接 -->
        <valueSeperator>#</valueSeperator> <!-- 设置多列数据组合hash 的 value时使用的分隔符 -->

        <batchLimit>30</batchLimit> <!-- 设置每个线程每批次加载的记录数量 -->
        <batchLimit.redis>6</batchLimit.redis> <!-- 设置redis每批次加载(pipeline_hset, hmset)的记录数量 -->

        <numThreads>2</numThreads> <!-- 设置并行线程数-->
        <method>hmset</method> <!-- 设置加载方法 hset, hmset, pipeline_hset  -->
        <!--<method>pipeline_hset</method> -->

        <overwrite>true</overwrite> <!-- 设置加载过程中采用覆盖，还是追加 -->
        <appendSeperator>,</appendSeperator> <!-- 设置加载过程中追加时使用的分隔符 -->
    </load>

</configuration>
      ```

      脚本
      vi bin/file2singlehash.sh

      ```
FWDIR=$(cd `dirname $0`/../;pwd)

APPLIB_DIR=$FWDIR/lib
for libjar in `ls $APPLIB_DIR/*jar|xargs`
do
  CLASSPATH=$CLASSPATH:$libjar
done

echo CLASSPATH=$CLASSPATH

CMD="scala -cp $CLASSPATH tools.redis.load.File2SingleHash $FWDIR/conf/file2singlehash-test.xml"
echo "CMD="+$CMD
nohup $CMD 2>&1 >> $FWDIR/logs/`basedir $0`.log &
      ```

      chmod +x bin/file2singlehash.sh
      bin/file2singlehash.sh

      验证数据
      >[hadoop@codis1 test-local-cluster]$ sh ck.sh
codis1 7380 dbsize =  51
codis1 7381 dbsize =  0
codis1 7382 dbsize =  50
codis1 7383 dbsize =  0
101
[hadoop@codis1 test-local-cluster]$

      >127.0.0.1:7380> hlen load2singlehash
(integer) 100
127.0.0.1:7380> hgetall load2singlehash
  1) "0:5"
  2) "value1-test-0#value2-test-5"
  3) "0:3"
........

测试 tools.redis.loadAreaMap.File2SingleHash

      示例：导入需求：
      1 从指定文件导入基站区域映射数据到redis/codis指定hash中，每行记录对应redis中指定hash的一个元素
      2 指定位置索引(0,1)的两列数据进行进制转换后使用冒号拼接，作为一个field
      3 指定位置索引(3)的两列数据如果是1或true，取指定映射的值WLAN，作为一个value，如果不是1或true，不导入
      4 加载时采用覆盖方式

      测试数据：100条记录
      >tsingfuMBP:bin tsingfu$ head /data01/data/datadir_github/ai-projects/streaming-app-201503/tools/src/test/resources/loadAreaMap-thread-test.data
100,100,area-0,0
100,101,area-1,1
100,102,area-2,0
100,103,area-3,1
100,104,area-4,0
100,105,area-5,1

      配置
      vi conf/loadAreaMap-file2singlehash-test.xml

      ```
<configuration>
    <redis>
        <servers>codis1:29001, codis1:29002</servers> <!-- 设置redis/codis地址, 格式为 ip:port -->
        <database>4</database> <!-- 设置导入到redis/codis哪个db，默认有16个db，编号0-15 -->
        <timeout>10000</timeout> <!-- 设置连接redis/codis最大超时,单位ms -->
        <password></password> <!-- 设置连接redis/codis的密码 -->
    </redis>

    <jedisPool>
        <maxTotal>100</maxTotal> <!-- 设置连接池最大连接数 -->
        <maxIdle>15</maxIdle> <!-- 设置连接池最大空闲连接数 -->
        <minIdle>0</minIdle> <!-- 设置连接池最小空闲连接数 -->
    </jedisPool>

    <load>
        <from>file</from> <!-- 目前仅支持file, 后期扩展支持 db -->

        <filename>/data01/data/datadir_github/ai-projects/streaming-app-201503/tools/src/test/resources/loadAreaMap-thread-test.data</filename> <!-- 设置数据文件的路径 -->
        <fileEncode>UTF-8</fileEncode> <!-- 设置数据文件的编码格式 -->
        <columnSeperator>,</columnSeperator> <!-- 设置数据文件字段分隔符 -->

        <hashName>areaMap1</hashName> <!-- 设置hashkey名 -->

        <fieldIdxes>0,1</fieldIdxes> <!-- 设置哪几列作为 hash 的 field, 支持取多列数据进行组合 -->
        <fieldSeperator>:</fieldSeperator> <!-- 设置多列数据组合hash 的 field名时使用的分隔符 -->

        <conversion10to16.idxes>0,1</conversion10to16.idxes> <!-- 设置取值时需要进制转换的字段索引 -->

        <valueIdx>3</valueIdx> <!-- 设置哪几列作为 hash 的 value，支持写多个，会进行拼接 -->
        <valueMapEnabled>true</valueMapEnabled> <!-- 设置是否进行value取值映射 -->
        <valueMap>WLAN</valueMap> <!-- 设置value的映射值 -->

        <batchLimit>30</batchLimit> <!-- 设置每个线程每批次加载的记录数量 -->
        <batchLimit.redis>6</batchLimit.redis> <!-- 设置redis每批次加载(pipeline_hset, hmset)的记录数量 -->

        <numThreads>2</numThreads>
        <method>hmset</method> <!-- 设置加载方法 hset, hmset, pipeline_hset  -->
        <!--<method>pipeline_hset</method>-->

        <overwrite>true</overwrite> <!-- 设置加载过程中采用覆盖，还是追加 -->
        <appendSeperator>,</appendSeperator> <!-- 设置加载过程中追加时使用的分隔符 -->


    </load>

</configuration>
      ```

      脚本

      ```
FWDIR=$(cd `dirname $0`/../;pwd)

APPLIB_DIR=$FWDIR/lib
for libjar in `ls $APPLIB_DIR/*jar|xargs`
do
  CLASSPATH=$CLASSPATH:$libjar
done

CMD="scala -cp $CLASSPATH tools.redis.loadAreaMap.File2SingleHash $FWDIR/conf/loadAreaMap-file2singlehash-test.xml"
echo "CMD="+$CMD
nohup $CMD 2>&1 >> $FWDIR/logs/`basedir $0`.log &
      ```

      chmod +x bin/areamap-file2singlehash.sh
      bin/areamap-file2singlehash.sh

      验证数据
      >[hadoop@codis1 test-local-cluster]$ redis-cli -p 7382
127.0.0.1:7382> keys *
1) "areaMap1"
127.0.0.1:7382> hlen areaMap1
(integer) 50
127.0.0.1:7382> hgetall areaMap1
  1) "0067:006A"
  2) "WLAN"
  3) "0068:0065"
  4) "WLAN"
  5) "0067:0066"
  6) "WLAN"