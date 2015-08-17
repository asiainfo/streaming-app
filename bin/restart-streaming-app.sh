#####################################################################################
#脚本作用：自动重启spark-app driver，支持spark standalone 模式下检查executor是否停止，没有停止，执行kill
#脚本处理流程：
# 1 从driver日志取 spark app 的appId
# 2 获取driver的pid
# 3 kill spark app driver，同时检查spark-app对应 executors是否正常停止，如果没有执行 kill
# 4 启动 spark app driver
#脚本使用方法：
# 1 设置环境变量 JAVA_HOME, SPARK_HOME, StartScript, KillPidScript, DriverLog, DriverNode, WorkerNodes, PROCESS_ID_STR
# 2 在spark standalone cluster 各节点部署 KillPidScript

DEBUG=0 #logDebug 使用，调试时设置为1

#设置JDK环境
JAVA_HOME=
JPS=$JAVA_HOME/bin/jps

#设置 spark 环境
SPARK_HOME=/home/spark/app/spark-1.4.0-cdh5.0.2
StartScript=$SPARK_HOME/bin/run_streaming.sh
KillPidScript=$SPARK_HOME/killPids.sh
DriverLog=$SPARK_HOME/logs/run_streaming.sh.log
DriverNode="spark1"
WorkerNodes="spark1 spark2 spark3 spark4 spark5"
#PROCESS_ID_STR="OCDC_Streaming"
PROCESS_ID_STR="OCDC-Streaming-App"


# 定义日志输出使用的格式化函数
logInfo() {
  level=$1
  prefix="[INFO${level}]"
  msg=$2
  if [ $level -eq 1 ]; then
    echo "============================================================================="
  fi
  echo `date +'%F:%T'` ${prefix} ${msg}
}

logDebug() {
  level=$1
  prefix="[DEBUG${level}]"
  msg=$2
  if [ $DEBUG -eq 1 ]; then
    echo `date +'%F:%T'` ${prefix} ${msg}
  fi
}

logWarn() {
  level=$1
  prefix="[WARN${level}]"
  msg=$2
  if [ $level -eq 1 ]; then
    echo "============================================================================="
  fi
  echo `date +'%F:%T'` ${prefix} ${msg}
}

# 定义远程停止 spark executors 的函数，调用远程主机上 $SPARK_HOME/killPids.sh ，传递 pids
stop_executors() {
  appId=$1
  logDebug 1 "appId=$appId"
  if [ "$appId" != "" ]; then

  logInfo 1 "stop executors on WorkerNodes = $WorkerNodes"
  for node in `echo $WorkerNodes`
  do

    echo "ssh $node exec \"$JPS -m|grep CoarseGrainedExecutorBackend|grep \\\"app-id $appId\\\"|grep -v grep|awk '{print \$1}' \""
    remote_pidsStr=`ssh $node exec "$JPS -m|grep CoarseGrainedExecutorBackend|grep \"app-id $appId\"|grep -v grep"`
    remote_pids=`echo $remote_pidsStr|awk '{print $1}'|xargs`
    logInfo 2 "check executors for $appId with remote_pids=$remote_pids on $node"
    if [ "$remote_pids" != "" ]; then
      logInfo 2 "kill process with remote_pids=$remote_pids on $node"
      ssh $node "sh $KillPidScript $remote_pids"
    fi
  done

  else
    logWarn 2 "appId = $appId is empty and invalid"
  fi
}


#1 从driver日志取 spark app 的appId
echo "ssh $DriverNode exec \"head -1000 $DriverLog|grep \\\"app ID app\\\"|awk '{print \$NF}'\""
appIdLine=`ssh $DriverNode exec "head -1000 $DriverLog|grep \"app ID app\""`
appId=`echo $appIdLine|awk '{print $NF}'`
export appId
logInfo 1 "appId=$appId"

#2 获取driver的pid
pid_count=`ssh $DriverNode exec "$JPS -m|grep SparkSubmit|grep $PROCESS_ID_STR|grep -v grep|wc -l"`
pidsStr=`ssh $DriverNode exec "$JPS -m|grep SparkSubmit|grep $PROCESS_ID_STR|grep -v grep"`
pids=`echo $pidsStr|awk '{print $1}'|xargs`

#3 kill spark app driver
logInfo 1 "kill process with idString $PROCESS_ID_STR, pid_count=$pid_count, pids=$pids"
ssh $DriverNode exec "$JPS -m|grep SparkSubmit|grep $PROCESS_ID_STR|grep -v grep"
if [ -n "$pids" ]; then

for pid in `echo $pids`
do
  KILL_CMD="kill -9 $pid"
  logInfo 2 "executing $KILL_CMD"
  $KILL_CMD
  sleep 1

  echo "stop_executors $appId"
  stop_executors $appId

  sleep 1
  pids2=`$JPS -m|grep SparkSubmit|grep $PROCESS_ID_STR|grep -v grep|awk '{print $1}'|xargs`

  if [ -n "$pids2" ]; then
    logInfo 2 "Stop process with pid=$pid successfully."
  else
    logInfo 2 "Failed top stop process with pid=$pid."
  fi
done

else
  echo `date +'%F:%T'` "WARN Not found running process with idString $PROCESS_ID_STR, you should check it ..."
fi


#4 启动 spark app driver
logInfo 1 "start process with idString $PROCESS_ID_STR"
#ssh $DriverNode "sh $StartScript"
#ssh $DriverNode exec "sh $StartScript" #此方式启动，不是后台模式
sh $StartScript
sleep 1
#$JPS -m|grep SparkSubmit|grep $PROCESS_ID_STR|grep -v grep
ssh $DriverNode exec "$JPS -m|grep SparkSubmit|grep $PROCESS_ID_STR|grep -v grep"

#pid_count=`$JPS -m|grep SparkSubmit|grep $PROCESS_ID_STR|grep -v grep|wc -l`
pid_count=`ssh $DriverNode exec "$JPS -m|grep SparkSubmit|grep $PROCESS_ID_STR|grep -v grep|wc -l"`

if [ $pid_count -ne 0 ]; then
  echo `date +'%F:%T'` "INFO start process with idStr $PROCESS_ID_STR successfully"
else
  echo `date +'%F:%T'` "WARN Failed to start process with idStr $PROCESS_ID_STR"
fi