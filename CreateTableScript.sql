-- Kafka Sources
CREATE TABLE `KafkaSource` (
  `id` int NOT NULL,
  `topic` varchar(30) NOT NULL,
  `groupid` varchar(20) NOT NULL,
  `zookeeper` varchar(100) NOT NULL,
  `brokerlist` varchar(100) NOT NULL,
  `serializerclass` varchar(100),
  `msgkey`  varchar(30),
  `autooffset`  varchar(30),
  PRIMARY KEY (`id`)
);

-- HDFS Sources

-- EventSources
CREATE TABLE `EventSource` (
  `id` int NOT NULL,
  `type` varchar(10) NOT NULL,
  `sourceid` int NOT NULL,
  `delim` varchar(10) NOT NULL,
  `formatlength` int NOT NULL,
  PRIMARY KEY (`id`)
);



-- EventRules


-- LabelRules