FWDIR=$(cd `dirname $0`/../;pwd)

APPLIB_DIR=$FWDIR/lib
for libjar in `ls $APPLIB_DIR/*jar|xargs`
do
  CLASSPATH=$CLASSPATH:$libjar
done

echo CLASSPATH=$CLASSPATH

#For log4j
CLASSPATH=$CLASSPATH:$FWDIR/conf/

CMD="scala -cp $CLASSPATH tools.redis.load.Load2Redis $FWDIR/conf/jdbc2onehash-test.xml"
echo "CMD="+$CMD
nohup $CMD 2>&1 >> $FWDIR/logs/`basename $0`.log &

tail -30f $FWDIR/logs/`basename $0`.log

