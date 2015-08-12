
# 20150805

## new feature
* 新增事件复用功能，在配置表 EventRules 中新增父事件 parentEventRuleId ，可以基于已存在父事件的事件集合进行过滤
  parentEventRuleId取值 eventRules 中已存在的 id， 取-1 代表没有父事件
  
  需要更新配置表：
  
  ```
alter table EventRules add parentEventRuleId int NOT NULL DEFAULT -1;
alter table EventRules add name varchar(30);
  ```
  
* 更新配置：BusenessEventsProp 中 timeIdx 设置时间字段位置索引不再使用，可以在配置表中删除
   
* 为MC数据源新增日志有效时间窗口， 在 EventSource 中新增字段 validWindowsTimeMs
  validWindowsTimeMs 用于设置实时流分析的有效时间窗口，如果日志时间不在有效时间窗口内，认为无效数据不分析； 取-1 代表不启用有效时间窗口过滤功能
  //TODO: 这里需要考虑对于不在有效时间窗口内的日志处理策略，是不处理还是处理在业务订阅时过滤
  
  需要更新配置表：
  
  ```
alter table EventSource add validWindowsTimeMs int NOT NULL DEFAULT -1;
  ```