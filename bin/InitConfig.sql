-- init MainFrameProp

---- followings are used for Cache related
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("CodisProxy","localhost:1000");
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("DefaultCacheManager","CodisCacheManager");
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("JedisMaxIdle","300");
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("JedisMaxActive","1000");
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("JedisMEM","600000");

-- init EventSources
--INSERT INTO `EventSource` (`name`,`type`,`delim`,`formatlength`,`classname`) VALUES ("MC_Kafka","kafka",",","100","com.asiainfo.ocdc.streaming.MCEventSource");
INSERT INTO `EventSource` (`name`,`type`,`delim`,`formatlength`,`classname`) VALUES ("MC_HDFS","hdfs",",","8","com.asiainfo.ocdc.streaming.MCEventSource");

-- init EventSourcesDetail
-- kafka
/*
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("topic","topic1",1);
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("groupid","groupid1",1);
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("zookeeper","zookeeper1",1);
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("brokerlist","brokerlist1",1);
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("serializerclass","serializerclass1",1);
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("msgkey","msgkey1",1);
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("autooffset","autooffset1",1);
*/
-- hdfs
INSERT INTO `EventSourceProp` (`name`,`pvalue`,`esourceid`) VALUES ("path","hdfs://localhost:9000/user/leo/streaming",3);

-- init LabelRules
INSERT INTO `LabelRules` (`classname`,`esourceid`) VALUES ("com.asiainfo.ocdc.streaming.LocationStayRule",3);
INSERT INTO `LabelRules` (`classname`,`esourceid`) VALUES ("com.asiainfo.ocdc.streaming.SiteRule",3);


-- init LabelRulesProp
INSERT INTO `LabelRulesProp` (`name`,`pvalue`,`lrid`) VALUES ("stay.limits","10 * 60 * 1000, 5 * 60 * 1000, 3 * 60 * 1000",3);
INSERT INTO `LabelRulesProp` (`name`,`pvalue`,`lrid`) VALUES ("stay.matchMax","true",3);
INSERT INTO `LabelRulesProp` (`name`,`pvalue`,`lrid`) VALUES ("stay.outputThreshold","true",3);
INSERT INTO `LabelRulesProp` (`name`,`pvalue`,`lrid`) VALUES ("stay.timeout","30 * 60 * 1000",3);

-- init EventRules
INSERT INTO `EventRules` (`esourceid`,`classname`) VALUES (3,"com.asiainfo.ocdc.streaming.MCEventRule");

-- init EventRulesProp
INSERT INTO `EventRulesProp` (`name`,`pvalue`,`erid`) VALUES ("selectExp","eventID,imei,imsi,time,labels['onsite']['AAAA']",2);
INSERT INTO `EventRulesProp` (`name`,`pvalue`,`erid`) VALUES ("filterExp","labels['onsite']['AAAA']='true'",2);