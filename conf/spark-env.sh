export SPARK_JAVA_OPTS="$SPARK_JAVA_OPTS -Dspark.master=yarn-client"
export SPARK_YARN_MODE=true

export HADOOP_CONF_DIR=

#spark assembly jar
export SPARK_JAR=
#spark example jar
export SPARK_YARN_APP_JAR=

export SPARK_WORKER_INSTANCES=2
export SPARK_WORKER_CORES=1

export SPARK_WORKER_MEMORY=1g
export SPARK_MASTER_MEMORY=1g
export SPARK_YARN_APP_NAME=ocdc
