-- 1 MainFrameProp
CREATE TABLE `MainFrameProp` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(30) NOT NULL,
  `pvalue` varchar(50) NOT NULL,
  PRIMARY KEY (`id`)
);


-- 2 Kafka Sources
CREATE TABLE `KafkaSource` (
  `id` int NOT NULL AUTO_INCREMENT,
  `topic` varchar(30) NOT NULL,
  `groupid` varchar(20) NOT NULL,
  `zookeeper` varchar(100) NOT NULL,
  `brokerlist` varchar(100) NOT NULL,
  `serializerclass` varchar(100),
  `msgkey`  varchar(30),
  `autooffset`  varchar(30),
  PRIMARY KEY (`id`)
);

-- 3 HDFS Sources

-- 4 EventSources
CREATE TABLE `EventSource` (
  `id` int NOT NULL AUTO_INCREMENT,
  `type` varchar(10) NOT NULL,
  `sourceid` int NOT NULL,
  `delim` varchar(10) NOT NULL,
  `formatlength` int NOT NULL,
  `classname` varchar(200) NOT NULL,
  PRIMARY KEY (`id`)
);
alter table EventSource add foreign key (sourceid) references KafkaSource(id) ON
DELETE CASCADE;

-- 5 LabelRules
CREATE TABLE `LabelRules` (
  `id` int NOT NULL AUTO_INCREMENT,
  `esourceid` int NOT NULL,
  `classname` varchar(200) NOT NULL,
  PRIMARY KEY (`id`)
);
alter table LabelRules add foreign key (esourceid) references EventSource(id) ON
DELETE CASCADE;

-- 6 LabelRulesProp
CREATE TABLE `LabelRulesProp` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(30) NOT NULL,
  `pvalue` varchar(50) NOT NULL,
  `lrid` int NOT NULL,
  PRIMARY KEY (`id`)
);
alter table LabelRulesProp add foreign key (lrid) references LabelRules(id) ON
DELETE CASCADE;

-- 7 EventRules
CREATE TABLE `EventRules` (
  `id` int NOT NULL AUTO_INCREMENT,
  `esourceid` int NOT NULL,
  `classname` varchar(200) NOT NULL,
  PRIMARY KEY (`id`)
);
alter table EventRules add foreign key (esourceid) references EventSource(id) ON
DELETE CASCADE;

-- 8 EventRulesProp
CREATE TABLE `EventRulesProp` (
  `id` int NOT NULL,
  `name` varchar(30) NOT NULL,
  `pvalue` varchar(50) NOT NULL,
  `erid` int NOT NULL,
  PRIMARY KEY (`id`)
);
alter table EventRulesProp add foreign key (erid) references EventRules(id) ON
DELETE CASCADE;