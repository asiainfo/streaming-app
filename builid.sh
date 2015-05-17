mvn package;
cp core/target/streaming-core-0.1.0-SNAPSHOT.jar /mnt/hgfs/CentOS/ShareFile;
echo "copy core jar"
cp event/mc-signal/target/streaming-event-mc-0.1.0-SNAPSHOT.jar /mnt/hgfs/CentOS/ShareFile;
echo "copy event jar"
cp adapter/target/streaming-adapter-0.1.0-SNAPSHOT.jar /mnt/hgfs/CentOS/ShareFile;
echo "copy adapter jar"
cp tools/target/streaming-tools-0.1.0-SNAPSHOT.jar /mnt/hgfs/CentOS/ShareFile;
echo "copy tools jar"

