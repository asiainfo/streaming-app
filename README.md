
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
  <configuration>
  
  </configuration>
  </pre>
  
Start spark streaming
=====
./bin/start-streaming-app.sh  conf/Sample.xml



  
