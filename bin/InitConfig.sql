-- init MainFrameProp

---- followings are used for Cache related
-- 设置redis/codis访问地址
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("CodisProxy","codis1:29001,codis2:29002");
-- 设置 cacheManager, CodisCacheManager 与RedisCacheManager的区别是什么？
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("DefaultCacheManager","CodisCacheManager");
-- 设置jedisPool的最大idle连接数
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("JedisMaxIdle","300");
-- 设置jedisPool的最大连接数
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("JedisMaxTotal","1000");
-- 设置jedisPool的？
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("JedisMEM","600000");
-- 设置spark-streaming batch duration，单位秒
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("internal","15");
-- 设置 jedis 访问超时，单位毫秒
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("JedisTimeOut",10000);
-- 设置自动更新cache的周期（暂时没有使用）
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("cache_update_interval", 24 * 60 * 60 * 1000);
 -- 设置checkpoint目录，提高数据安全（暂时没有使用）
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("checkpoint_dir","streaming/checkpoint");
-- 设置每天处理数据的时间范围起始值，和信令时间对比
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("morning8time","00:00:01");
-- 设置每天处理数据的时间范围终止值，和信令时间对比
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("afternoon8time","23:59:59");
-- 设置每次批量查询redis/codis cache时并发查询任务的线程数，每个executor一个查询线程池
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("codisQryThreadNum",500);
-- 设置每次批量查询redis/codis cache时每个查询任务的查询数量
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("pipeLineBatch",200);
-- 设置自动更新配置的周期（没有生效）
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("BaseConfUpdateInteval",1*60*60*1000);
-- 设置业务事件发送到kafka时使用的borkerlist
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("brokerlist","spark1:9092,spark2:9092,spark3:9092");

INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("serializerclass","kafka.serializer.StringEncoder");



-- init EventSources
-- INSERT INTO `EventSource` (`name`,`type`,`delim`,`formatlength`,`classname`,`batchsize`,`enabled`) VALUES ("MC_HDFS","hdfs",",","8","com.asiainfo.ocdc.streaming.MCEventSource",100,1);
INSERT INTO `EventSource` (`name`,`type`,`delim`,`formatlength`,`classname`,`batchsize`,`enabled`) VALUES ("MC_Kafka","kafka_direct",",","15","com.asiainfo.ocdc.streaming.MCEventSource", 100, 1);
-- INSERT INTO `EventSource` (`name`,`type`,`delim`,`formatlength`,`classname`,`batchsize`,`enabled`) VALUES ("MC_Kafka","kafka",",","15","com.asiainfo.ocdc.streaming.MCEventSource", 100, 1);

-- init EventSourcesDetail
-- kafka
-- 设置kafka数据源 kafka的topic
-- bin/kafka-topics.sh --zookeeper spark1:2181 --create --partitions 3 --replication-factor 2 --topic topic_stream1
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("topic","topic_stream1",1);
-- 设置kafka数据源 kafka的 消费group（对于kafka direct api无效）
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("group","g1",1);
-- 设置kafka数据源 kafka的 消费时并发线程数（对于kafka direct api无效）
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("receivernum","2",1);
-- 设置kafka数据源 kafka的 消费时使用zookeeper信息
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("zookeeper","spark1:2181,spark2:2181,spark3:2181",1);
-- 设置kafka数据源 repartition的 分区数
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("shufflenum","1",1);
-- 设置业务事件发送时使用的 brokers
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("brokers","spark1:9092,spark2:9092,spark3:9092",1);
-- 设置数据源中时间字段的位置索引（暂时没有使用，在MCBSEvent getTime 定义）
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("timeIdx","1",1);

-- hdfs
-- 数据源是HDFS时，设置path
-- INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("path","hdfs://spark1:9000/user/tsingfu/streaming",1);

-- init LabelRules
INSERT INTO `LabelRules` (`classname`,`esourceid`) VALUES ("com.asiainfo.ocdc.streaming.UserBaseInfoRule",1);
INSERT INTO `LabelRules` (`classname`,`esourceid`) VALUES ("com.asiainfo.ocdc.streaming.SiteRule",1);
INSERT INTO `LabelRules` (`classname`,`esourceid`) VALUES ("com.asiainfo.ocdc.streaming.LocationStayRule",1);

-- init LabelRulesProp
-- UserBaseInfoRule 标签规则，设置 codis/redis中用户资料缓存中需要作为label的属性名
INSERT INTO `LabelRulesProp` (`name`,`pvalue`,`lrid`) VALUES ("user_info_cols","product_no,country_id,city_id",1);
-- LocationStayRule 标签规则，
INSERT INTO `LabelRulesProp` (`name`,`pvalue`,`lrid`) VALUES ("stay.limits","600000, 300000, 180000",3);
INSERT INTO `LabelRulesProp` (`name`,`pvalue`,`lrid`) VALUES ("stay.matchMax","true",3);
INSERT INTO `LabelRulesProp` (`name`,`pvalue`,`lrid`) VALUES ("stay.outputThreshold","true",3);
INSERT INTO `LabelRulesProp` (`name`,`pvalue`,`lrid`) VALUES ("stay.timeout","1800000",3);

-- init EventRules
INSERT INTO `EventRules` (`esourceid`,`classname`) VALUES (1,"com.asiainfo.ocdc.streaming.MCEventRule");

-- init EventRulesProp
-- 设置事件规则的过滤条件
INSERT INTO `EventRulesProp` (`name`,`pvalue`,`erid`) VALUES ("filterExp","labels['area_onsite']['ZHUJIAYU']='true'",1);

-- init BusenessEvents
INSERT INTO `BusenessEvents` (`id`,`classname`) VALUES (1,"com.asiainfo.ocdc.streaming.MCBSEvent");

-- init BusenessEventsMapEventSources
INSERT INTO `BusenessEventsMapEventSources` (`beid`,`esid`) VALUES (1,1);

-- init BusenessEventsMapEventRules
INSERT INTO `BusenessEventsMapEventRules` (`beid`,`erid`) VALUES (1,1);

-- init BusenessEventsProp
INSERT INTO `BusenessEventsProp` (`name`,`pvalue`,`beid`) VALUES ("selectExp","labels['user_info']['product_no'],imsi,time,labels['area_onsite']['ZHUJIAYU'],labels['area_onsite_stay']['ZHUJIAYU']",1);
INSERT INTO `BusenessEventsProp` (`name`,`pvalue`,`beid`) VALUES ("delim",",",1);
INSERT INTO `BusenessEventsProp` (`name`,`pvalue`,`beid`) VALUES ("outputtype","kafka",1);
INSERT INTO `BusenessEventsProp` (`name`,`pvalue`,`beid`) VALUES ("kafkakeycol","0",1);
INSERT INTO `BusenessEventsProp` (`name`,`pvalue`,`beid`) VALUES ("output_topic","topic_out_zhujiayu",1);
INSERT INTO `BusenessEventsProp` (`name`,`pvalue`,`beid`) VALUES ("brokerlist","spark1:9092,spark2:9092,spark3:9092",1);
INSERT INTO `BusenessEventsProp` (`name`,`pvalue`,`beid`) VALUES ("serializerclass","kafka.serializer.StringEncoder",1);
INSERT INTO `BusenessEventsProp` (`name`,`pvalue`,`beid`) VALUES ("interval",86400000,1);
INSERT INTO `BusenessEventsProp` (`name`,`pvalue`,`beid`) VALUES ("delaytime",1800000,1);

INSERT INTO `BusenessEventsProp` (`name`,`pvalue`,`beid`) values ('batchLimit', 10000, 1); -- 设置业务在多事件订阅查询事件cache的并发
INSERT INTO `BusenessEventsProp` (`name`,`pvalue`,`beid`) values ('userKeyIdx',0,1); -- 设置selectExp输出字段中用于表示事件cache的key的位置索引
