-- init MainFrameProp

---- followings are used for Cache related
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("CodisProxy","ochadoop94:19100,ochadoop94:19500");
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("DefaultCacheManager","CodisCacheManager");
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("JedisMaxIdle","300");
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("JedisMaxTotal","1000");
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("JedisMEM","600000");
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("internal","4");


-- init EventSources
--INSERT INTO `EventSource` (`name`,`type`,`delim`,`formatlength`,`classname`) VALUES ("MC_Kafka","kafka",",","100","com.asiainfo.ocdc.streaming.MCEventSource");
INSERT INTO `EventSource` (`name`,`type`,`delim`,`formatlength`,`classname`,`batchsize`,`enabled`) VALUES ("MC_HDFS","hdfs",",","8","com.asiainfo.ocdc.streaming.MCEventSource",100,true);

-- init EventSourcesDetail
-- kafka
/*
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("topic","mc_signal",4);
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("group","g1",4);
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("receivernum","2",4);
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("zookeeper","localhost:2181",4);
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("repartitionnum","1",4);

*/
-- hdfs
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("path","hdfs://ochadoop94:9000/user/ochadoop/streaming",3);

-- init LabelRules
INSERT INTO `LabelRules` (`classname`,`esourceid`) VALUES ("com.asiainfo.ocdc.streaming.UserBaseInfoRule",1);
INSERT INTO `LabelRules` (`classname`,`esourceid`) VALUES ("com.asiainfo.ocdc.streaming.SiteRule",1);
INSERT INTO `LabelRules` (`classname`,`esourceid`) VALUES ("com.asiainfo.ocdc.streaming.LocationStayRule",1);

-- init LabelRulesProp
INSERT INTO `LabelRulesProp` (`name`,`pvalue`,`lrid`) VALUES ("stay.limits","10 * 60 * 1000, 5 * 60 * 1000, 3 * 60 * 1000",2);
INSERT INTO `LabelRulesProp` (`name`,`pvalue`,`lrid`) VALUES ("stay.matchMax","true",2);
INSERT INTO `LabelRulesProp` (`name`,`pvalue`,`lrid`) VALUES ("stay.outputThreshold","true",2);
INSERT INTO `LabelRulesProp` (`name`,`pvalue`,`lrid`) VALUES ("stay.timeout","30 * 60 * 1000",2);
INSERT INTO `LabelRulesProp` (`name`,`pvalue`,`lrid`) VALUES ("user_info_cols","productNo,countryId,cityId",1);

-- init EventRules
INSERT INTO `EventRules` (`esourceid`,`classname`) VALUES (1,"com.asiainfo.ocdc.streaming.MCEventRule");

-- init EventRulesProp
INSERT INTO `EventRulesProp` (`name`,`pvalue`,`erid`) VALUES ("selectExp","eventID,imei,imsi,time,labels['onsite']['AAAA'],labels['stay']['AAAA']",2);
INSERT INTO `EventRulesProp` (`name`,`pvalue`,`erid`) VALUES ("filterExp","labels['onsite']['AAAA']='true'",2);
INSERT INTO `EventRulesProp` (`name`,`pvalue`,`erid`) VALUES ("delim",",",2);
INSERT INTO `EventRulesProp` (`name`,`pvalue`,`erid`) VALUES ("inputLength","4",2);
INSERT INTO `EventRulesProp` (`name`,`pvalue`,`erid`) VALUES ("outputtype","kafka",2);
INSERT INTO `EventRulesProp` (`name`,`pvalue`,`erid`) VALUES ("kafkakeycol","2",2);
INSERT INTO `EventRulesProp` (`name`,`pvalue`,`erid`) VALUES ("output_topic","wlan_signal",2);
INSERT INTO `EventRulesProp` (`name`,`pvalue`,`erid`) VALUES ("brokerlist","spark10:9093",2);
INSERT INTO `EventRulesProp` (`name`,`pvalue`,`erid`) VALUES ("serializerclass","kafka.serializer.StringEncoder",2);

INSERT INTO `EventRulesProp` (`name`,`pvalue`,`erid`) VALUES ("outputdir","/user/ochadoop/streaming/output",2);

-- init BusenessEvents
INSERT INTO `BusenessEvents` (`esourceid`,`classname`) VALUES (1,"com.asiainfo.ocdc.streaming.WLANEvent");

-- init BusenessEventsMapEventSources
INSERT INTO `BusenessEventsMapEventSources` (`beid`,`esid`) VALUES (1,1);

-- init BusenessEventsMapEventRules
INSERT INTO `BusenessEventsMapEventRules` (`beid`,`erid`) VALUES (1,1);

-- init BusenessEventsProp
INSERT INTO `BusenessEventsProp` (`name`,`pvalue`,`beid`) VALUES ("selectExp","eventID,imei,imsi,time,labels['onsite']['AAAA'],labels['stay']['AAAA']",1);
INSERT INTO `BusenessEventsProp` (`name`,`pvalue`,`beid`) VALUES ("delim",",",1);
INSERT INTO `BusenessEventsProp` (`name`,`pvalue`,`beid`) VALUES ("outputtype","kafka",1);
INSERT INTO `BusenessEventsProp` (`name`,`pvalue`,`beid`) VALUES ("kafkakeycol","2",1);
INSERT INTO `BusenessEventsProp` (`name`,`pvalue`,`beid`) VALUES ("output_topic","wlan_signal",1);
INSERT INTO `BusenessEventsProp` (`name`,`pvalue`,`beid`) VALUES ("brokerlist","localhost:9092",1);
INSERT INTO `BusenessEventsProp` (`name`,`pvalue`,`beid`) VALUES ("serializerclass","kafka.serializer.StringEncoder",1);

