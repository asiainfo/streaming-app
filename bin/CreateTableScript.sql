-- 1 MainFrameProp
DROP TABLE IF EXISTS `MainFrameProp`;
CREATE TABLE `MainFrameProp` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(30) NOT NULL,
  `pvalue` varchar(50) NOT NULL,
  PRIMARY KEY (`id`)
);

-- 2 EventSources
DROP TABLE IF EXISTS `EventSource`;
CREATE TABLE `EventSource` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(50) NOT NULL,
  `type` varchar(20) NOT NULL,
  `delim` varchar(10) NOT NULL,
  `formatlength` int NOT NULL,
  `classname` varchar(200) NOT NULL,
  PRIMARY KEY (`id`)
);

-- 3 EventSourcesDetail
DROP TABLE IF EXISTS `EventSourceProp`;
CREATE TABLE `EventSourceProp` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(30) NOT NULL,
  `pvalue` varchar(50) NOT NULL,
  `esourceid` int NOT NULL,
  PRIMARY KEY (`id`)
);
alter table EventSourceProp add foreign key (esourceid) references EventSource(id) ON
DELETE CASCADE;

-- 4 LabelRules
DROP TABLE IF EXISTS `LabelRules`;
CREATE TABLE `LabelRules` (
  `id` int NOT NULL AUTO_INCREMENT,
  `esourceid` int NOT NULL,
  `classname` varchar(200) NOT NULL,
  PRIMARY KEY (`id`)
);
alter table LabelRules add foreign key (esourceid) references EventSource(id) ON
DELETE CASCADE;

-- 5 LabelRulesProp
DROP TABLE IF EXISTS `LabelRulesProp`;
CREATE TABLE `LabelRulesProp` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(30) NOT NULL,
  `pvalue` varchar(50) NOT NULL,
  `lrid` int NOT NULL,
  PRIMARY KEY (`id`)
);
alter table LabelRulesProp add foreign key (lrid) references LabelRules(id) ON
DELETE CASCADE;

-- 6 EventRules
DROP TABLE IF EXISTS `EventRules`;
CREATE TABLE `EventRules` (
  `id` int NOT NULL AUTO_INCREMENT,
  `esourceid` int NOT NULL,
  `classname` varchar(200) NOT NULL,
  PRIMARY KEY (`id`)
);
alter table EventRules add foreign key (esourceid) references EventSource(id) ON
DELETE CASCADE;

-- 7 EventRulesProp
DROP TABLE IF EXISTS `EventRulesProp`;
CREATE TABLE `EventRulesProp` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(30) NOT NULL,
  `pvalue` varchar(200) NOT NULL,
  `erid` int NOT NULL,
  PRIMARY KEY (`id`)
);
alter table EventRulesProp add foreign key (erid) references EventRules(id) ON
DELETE CASCADE;