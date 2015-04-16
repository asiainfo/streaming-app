-- init MainFrameProp

---- followings are used for Cache related
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("CodisProxy","ochadoop94:19100,ochadoop94:19500");
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("DefaultCacheManager","CodisCacheManager");
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("JedisMaxIdle","300");
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("JedisMaxTotal","1000");
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("JedisMEM","600000");

-- init EventSources
--INSERT INTO `EventSource` (`name`,`type`,`delim`,`formatlength`,`classname`) VALUES ("MC_Kafka","kafka",",","100","com.asiainfo.ocdc.streaming.MCEventSource");
INSERT INTO `EventSource` (`name`,`type`,`delim`,`formatlength`,`classname`) VALUES ("MC_HDFS","hdfs",",","8","com.asiainfo.ocdc.streaming.MCEventSource");

-- init EventSourcesDetail
-- kafka
/*
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("topic","topic1",1);
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("group","groupid1",1);
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("zookeeper","zookeeper1",1);
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("brokerlist","brokerlist1",1);
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("serializerclass","serializerclass1",1);
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("msgkey","msgkey1",1);
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("autooffset","autooffset1",1);
*/
-- hdfs
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("path","hdfs://ochadoop94:9000/user/ochadoop/streaming",1);

-- init LabelRules
INSERT INTO `LabelRules` (`classname`,`esourceid`) VALUES ("com.asiainfo.ocdc.streaming.SiteRule",1);
INSERT INTO `LabelRules` (`classname`,`esourceid`) VALUES ("com.asiainfo.ocdc.streaming.LocationStayRule",1);


-- init LabelRulesProp
INSERT INTO `LabelRulesProp` (`name`,`pvalue`,`lrid`) VALUES ("stay.limits","10 * 60 * 1000, 5 * 60 * 1000, 3 * 60 * 1000",2);
INSERT INTO `LabelRulesProp` (`name`,`pvalue`,`lrid`) VALUES ("stay.matchMax","true",2);
INSERT INTO `LabelRulesProp` (`name`,`pvalue`,`lrid`) VALUES ("stay.outputThreshold","true",2);
INSERT INTO `LabelRulesProp` (`name`,`pvalue`,`lrid`) VALUES ("stay.timeout","30 * 60 * 1000",2);

-- init EventRules
INSERT INTO `EventRules` (`esourceid`,`classname`) VALUES (1,"com.asiainfo.ocdc.streaming.MCEventRule");

-- init EventRulesProp
INSERT INTO `EventRulesProp` (`name`,`pvalue`,`erid`) VALUES ("selectExp","eventID,imei,imsi,time,labels['onsite']['AAAA'],labels['stay']['AAAA']",2);
INSERT INTO `EventRulesProp` (`name`,`pvalue`,`erid`) VALUES ("filterExp","labels['onsite']['AAAA']='true'",2);
INSERT INTO `EventRulesProp` (`name`,`pvalue`,`erid`) VALUES ("delim",",",2);
INSERT INTO `EventRulesProp` (`name`,`pvalue`,`erid`) VALUES ("inputLength","4",2);
INSERT INTO `EventRulesProp` (`name`,`pvalue`,`erid`) VALUES ("outputdir","/user/ochadoop/streaming/output",2);