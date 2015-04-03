-- init MainFrameProp
INSERT INTO `MainFrameProp` (`name`,`pvalue`) VALUES ("internal","20");

-- init Kafka Sources
INSERT INTO `KafkaSource` (`topic`,`groupid`,`zookeeper`,`brokerlist`,`serializerclass`,`msgkey`,`autooffset`) VALUES ("topic1","groupid1","zookeeper1","brokerlist1","serializerclass1","msgkey1","autooffset1");

-- init EventSources
INSERT INTO `EventSource` (`type`,`sourceid`,`delim`,`formatlength`,`classname`) VALUES ("kafka",1,",","100","com.asiainfo.ocdc.streaming.MCEventSource");


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
INSERT INTO `EventRules` (`esourceid`,`classname`) VALUES (,"");

-- init EventRulesProp
INSERT INTO `EventRulesProp` (`name`,`pvalue`,`erid`) VALUES ();