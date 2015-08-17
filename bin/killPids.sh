pids=$1


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

logWarn() {
  level=$1
  prefix="[WARN${level}]"
  msg=$2
  if [ $level -eq 1 ]; then
    echo "============================================================================="
  fi
  echo `date +'%F:%T'` ${prefix} ${msg}
}


#pids=$1
#kill -9 $pids

if [ "$pids" = "" ]; then
  logWarn 2 "Found pids is empty and invalid."
else
  for pid in `echo $pids`
  do
    if [ "$pid" = "" ]; then
      logWarn 2 "Found pid is empty and invalid"
    else
      logInfo 2 "kill process with pid=$pid"
      kill -9 $pid
    fi
  done
fi