# tools 模块介绍

## redis 导入程序

 * 比较通用的导入程序
`tools.redis.LoadFileIntoHashes` //从文件读记录导入Redis，每行记录对应Redis中的一个 hash
`tools.redis.LoadFileIntoSingleHash` //从文件读记录导入Redis中指定的一个hash中，每行记录对应指定hash中的一个元素

 * 区域映射表之类的导入程序，需求特别，涉及进制转换，故单独提供
`tools.redis.LoadAreaMap` 

 * 如果数据量大，可以多线程并行导入（以下是基于 spark 实现的类，需要先将文件存放到hdfs

`tools.redis.sparkImpl.LoadFile2Hashes` //从文件读记录导入Redis，每行记录对应Redis中的一个 hash
`tools.redis.sparkImpl.LoadFile2SingleHash` //从文件读记录导入Redis中指定的一个hash中，每行记录对应指定hash中的一个元素

### 使用示例

 * 示例1： 导入区域映射
 
   依赖的jar
   ```
     commons-pool2-2.3.jar 
     jedis-2.7.0.jar 
     slf4j-api-1.7.7.jar 
     streaming-tools-0.1.0-SNAPSHOT.jar
   ```
   
   配置
   vi conf/loadareaMap-test.xml
       
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
 
         <!--<filename>tools/src/test/resources/tools-redis-loadareamap2.log</filename> &lt;!&ndash; 设置数据文件的路径 &ndash;&gt;-->
         <filename>/Users/Users_datadir_docker/tmp/tools-redis-loadareamap1.log</filename>
         <fileEncode>UTF-8</fileEncode> <!-- 设置数据文件的编码格式 -->
         <columnSeperator>,</columnSeperator> <!-- 设置数据文件字段分隔符 -->
 
         <hashName>areaMap1</hashName> <!-- 设置hashkey名 -->
 
         <fieldIdxes>4,5</fieldIdxes> <!-- 设置哪几列作为 hash 的 field, 支持取多列数据进行组合 -->
         <fieldSeperator>:</fieldSeperator> <!-- 设置多列数据组合hash 的 field名时使用的分隔符 -->
 
         <conversion10to16.idxes>4,5</conversion10to16.idxes> <!-- 设置取值时需要进制转换的字段索引 -->
 
         <valueIdx>3</valueIdx> <!-- 设置哪几列作为 hash 的 value，支持写多个，会进行拼接 -->
         <valueMap.enabled>true</valueMap.enabled>
         <valueMap>WLAN</valueMap>
 
         <overwrite>false</overwrite>
         <valueSeperator>,</valueSeperator>
 
         <batchLimit>5</batchLimit> <!-- 设置批量加载(pipeline_hset, hmset)的批次数量 -->
 
         <!--<numThreads>2</numThreads>-->
         <method>hmset</method> <!-- 设置加载方法 hset, hmset, pipeline_hset  -->
         <!--<method>pipeline_hset</method> -->
 
     </load>
 
 </configuration>
     ```
     
     脚本
     *NOTE* 可以不用部署在 spark 目录下
     vi bin/run_loadareamap.sh
     
     ```
 FWDIR=$(cd `dirname $0`/../;pwd)
 
 APPLIB_DIR=$FWDIR/lib
 for libjar in `ls $APPLIB_DIR/*jar|xargs`
 do
   CLASSPATH=$CLASSPATH:$libjar
 done
 
 scala -cp $CLASSPATH \
   tools.redis.LoadAreaMap \
   $FWDIR/conf/loadAreaMap-test.xml
     ```

     执行
     
     ```
chmod +x bin/run_loadareamap.sh
bin/run_loadareamap.sh
     ```

 * 示例2： 导入用户信息表（基于spark并行）

   配置 
    cd spark/
    vi conf/loadfile2hashes-test.xml
    
    ```
<configuration>
    <redis>
        <!--<servers>redis1:6379</servers> --><!--  设置redis/codis地址, 格式为 ip:port-->
        <servers>codis1:29001, codis1:29002</servers> <!-- 设置redis/codis地址, 格式为 ip:port -->
        <database>3</database> <!-- 设置导入到redis/codis哪个db，默认有16个db，编号0-15 -->
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

        <!--<filename>tools/src/test/resources/tools-redis-loadfile2hashes-test1.data</filename>-->
        <filename>/user/tsingfu/test/userinfo-test.txt</filename> <!-- 设置数据文件的路径 -->
        <fileEncode>UTF-8</fileEncode> <!-- 设置数据文件的编码格式 -->
        <columnSeperator>,</columnSeperator> <!-- 设置数据文件字段分隔符 -->

        <hashNamePrefix>userinfo:imsi:</hashNamePrefix> <!-- 设置hashkey名前缀 -->
        <hashIdxes>0</hashIdxes> <!-- 设置 hashkey取值字段的索引位置,可以使用多个列组合作为hashkey名，使用逗号分隔 -->
        <hashSeperator>:</hashSeperator> <!-- 设置 hashkey名多个字段值组合的分隔符 -->

        <valueIdxes>1,2</valueIdxes> <!-- 设置加载列的索引，可以加载多个列数据，使用逗号分隔 -->
        <fieldNames>phoneNo, areaId</fieldNames> <!-- 设置加载数据对应的的属性名 -->

       <batchLimit>1000</batchLimit>   <!-- 设置批量加载(pipeline_hset, hmset)的批次数量 -->

        <numThreads>8</numThreads>
        <method>pipeline_hset</method> <!-- 设置加载方法 hset, hmset, pipeline_hset  -->
        <!--<method>pipeline_hset</method> -->
    </load>
</configuration>
    ```
    
    脚本
    
    vi bin/spark_run_loadfile2hashes.sh
    
    ```
FWDIR=$(cd `dirname $0`/..;pwd)
WDIR=$(cd `dirname $0`/;pwd)
cd $WDIR

CMD="$FWDIR/bin/spark-submit 
  --class tools.redis.sparkImpl.LoadFile2Hashes 
  --master spark://spark1:7077 
  streaming-tools*jar /home/hadoop/app/spark/conf/loadfile2hashes-test.xml"
echo "===== CMD="$CMD
nohup sh $CMD 2>&1 >> $FWDIR/logs/`basename $0`.log &

tail -30f $FWDIR/logs/`basename $0`.log
    ```
    
    执行
    chmod +x bin/spark_run_loadfile2hashes.sh
    bin/spark_run_loadfile2hashes.sh
    

 * 示例3： 导入用户信息表（单机单线程）

   配置 

    vi conf/loadfileintosinglehash-test.xml

    ```
<configuration>
    <redis>
        <!-- <servers>redis1:6379</servers> --> <!-- 设置redis/codis地址, 格式为 ip:port -->
        <servers>codis1:29001, codis1:29002</servers>
        <database>3</database> <!-- 设置导入到redis/codis哪个db，默认有16个db，编号0-15 -->
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

        <filename>/docker_vol01/tmp/tools-redis-loadareamap2.log</filename> <!-- 设置数据文件的路径 -->
        <fileEncode>UTF-8</fileEncode> <!-- 设置数据文件的编码格式 -->
        <columnSeperator>,</columnSeperator> <!-- 设置数据文件字段分隔符 -->

        <hashName>singleHashTest</hashName> <!-- 设置hashkey名 -->

        <fieldIdxes>0,1</fieldIdxes> <!-- 设置哪几列作为 hash 的 field, 支持取多列数据进行组合 -->
        <fieldSeperator>:</fieldSeperator> <!-- 设置多列数据组合hash 的 field名时使用的分隔符 -->
        <valueIdxes>2</valueIdxes> <!-- 设置哪几列作为 hash 的 value，支持写多个，会进行拼接 -->
        <valueSeperator>,</valueSeperator> <!-- 设置多列数据组合hash 的 value时使用的分隔符 -->

        <batchLimit>5</batchLimit> <!-- 设置批量加载(pipeline_hset, hmset)的批次数量 -->

        <numThreads>2</numThreads>
        <method>hmset</method> <!-- 设置加载方法 hset, hmset, pipeline_hset  -->
        <!--<method>pipeline_hset</method> -->

    </load>

</configuration>
    ```
    
    脚本
    
    vi bin/run_loadfileintosinglehash.sh

    ```
FWDIR=$(cd `dirname $0`/../;pwd)

APPLIB_DIR=$FWDIR/lib
for libjar in `ls $APPLIB_DIR/*jar|xargs`
do
  CLASSPATH=$CLASSPATH:$libjar
done

echo CLASSPATH=$CLASSPATH

scala -cp $CLASSPATH \
  tools.redis.LoadFileIntoSingleHash $FWDIR/conf/tools-redis-loadfile2singlehash-test.xml
    ```
    
    执行
    chmod +x bin/run_loadfileintosinglehash.sh
    bin/run_loadfileintosinglehash.sh


## kafka2hdfs
`tools.kafka.sparkIml.Kafka2Hdfs` //基于 spark 实现的类

`tools.kafka.Kafka2Hdfs` //基于 producer-consumer 模式实现的类，使用时可能遇到追加到hdfs报错的问题，待改进


