
Configuration
=====
  conf/spark-env.sh
  <pre>
  export HADOOP_CONF_DIR=/home/ocdc/hadoop-2.3.0-cdh5.0.0-och3.1.0/etc/hadoop
  export SPARK_YARN_APP_JAR=/home/ocdc/spark_0.9.1_streaming/examples/target/scala-2.10/spark-examples-assembly-0.9.1.jar
  export SPARK_JAR=/home/ocdc/spark_0.9.1_streaming/assembly/target/scala-2.10/spark-assembly-0.9.1-hadoop2.3.0-cdh5.0.0.jar
  </pre>
  
  conf/Sample.xml
  <pre>
  .........
     <dataSource name="ds1">
        <class>com.asiainfo.ocdc.streaming.impl.KafkaSource</class>
        <zkQuorum></zkQuorum>
        <topics>cmbb3</topics>
        <groupId>test-consumer-group</groupId>
        <consumerNum>3</consumerNum>
        <separator>" "</separator>
        <stream_columns>a,b,c,d,e,f,count,fee</stream_columns>
    </dataSource>

    <step>
        <class>com.asiainfo.ocdc.streaming.impl.StreamFilter</class>
        <HBaseTable>t1</HBaseTable>
        <HBaseCell>cell</HBaseCell>
        <HBaseKey>lac,cell</HBaseKey>
        <output>b,c,t1.cell,count,fee</output>
        <where>t1.cell!=null</where>
    </step>

    <step>
        <class>com.asiainfo.ocdc.streaming.impl.DynamicOperate</class>
        <HBaseTable>t2</HBaseTable>
        <HBaseKey>b</HBaseKey>
        <HBaseCells>Count,Fee</HBaseCells>
        <expressions>t2.Count+count,t2.Fee+fee</expressions>
        <output>b,c,t1.cell,count,fee</output>
    </step>

    <step>
        <class>com.asiainfo.ocdc.streaming.impl.KafkaOut</class>
        <topic>topicName</topic>
        <broker>dev001:9092</broker>
        <OutCol>b,c</OutCol>
    </step>
    ......
  </pre>
Building Spark
=====
  <pre>
  mvn package
  </pre>
Start spark streaming
=====
./bin/start-streaming-app.sh  conf/Sample.xml



  
