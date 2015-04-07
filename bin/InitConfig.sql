-- init MainFrameProp
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("internal","20");

-- init EventSources
INSERT INTO `EventSource` (`name`,`type`,`delim`,`formatlength`,`classname`) VALUES ("MC_Kafka","kafka",",","100","com.asiainfo.ocdc.streaming.MCEventSource");
INSERT INTO `EventSource` (`name`,`type`,`delim`,`formatlength`,`classname`) VALUES ("MC_HDFS","hdfs",",","100","com.asiainfo.ocdc.streaming.MCEventSource");

-- init EventSourcesDetail
-- kafka
INSERT INTO `EventSourceDetail` (`name`,`pvalue`,`esourceid`) VALUES ("topic","topic1",1);
INSERT INTO `EventSourceDetail` (`name`,`pvalue`,`esourceid`) VALUES ("groupid","groupid1",1);
INSERT INTO `EventSourceDetail` (`name`,`pvalue`,`esourceid`) VALUES ("zookeeper","zookeeper1",1);
INSERT INTO `EventSourceDetail` (`name`,`pvalue`,`esourceid`) VALUES ("brokerlist","brokerlist1",1);
INSERT INTO `EventSourceDetail` (`name`,`pvalue`,`esourceid`) VALUES ("serializerclass","serializerclass1",1);
INSERT INTO `EventSourceDetail` (`name`,`pvalue`,`esourceid`) VALUES ("msgkey","msgkey1",1);
INSERT INTO `EventSourceDetail` (`name`,`pvalue`,`esourceid`) VALUES ("autooffset","autooffset1",1);
-- hdfs
INSERT INTO `EventSourceDetail` (`name`,`pvalue`,`esourceid`) VALUES ("path","",2);

-- init LabelRules
INSERT INTO `LabelRules` (`esourceid`,`classname`) VALUES (1,"com.asiainfo.ocdc.streaming.MCLabelRule");
INSERT INTO `LabelRules` (`esourceid`,`classname`) VALUES (1,"com.asiainfo.ocdc.streaming.MCLabelRule2");

-- init LabelRulesProp
INSERT INTO `LabelRulesProp` (`name`,`pvalue`,`lrid`) VALUES ("name11","pvalue11",1);
INSERT INTO `LabelRulesProp` (`name`,`pvalue`,`lrid`) VALUES ("name12","pvalue12",1);
INSERT INTO `LabelRulesProp` (`name`,`pvalue`,`lrid`) VALUES ("name21","pvalue21",2);
INSERT INTO `LabelRulesProp` (`name`,`pvalue`,`lrid`) VALUES ("name22","pvalue22",2);
INSERT INTO `LabelRulesProp` (`name`,`pvalue`,`lrid`) VALUES ("name23","pvalue23",2);

-- init EventRules
INSERT INTO `EventRules` (`esourceid`,`classname`) VALUES (1,"");

-- init EventRulesProp
INSERT INTO `EventRulesProp` (`name`,`pvalue`,`erid`) VALUES ("","",1);