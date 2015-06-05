package tools.kafka

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.xml.XML

/**
 * Created by tsingfu on 15/4/23.
 */
class Kafka2HdfsSuite extends FunSuite with BeforeAndAfter {

  val confFile = "tools/conf/kafka2hdfs2-test.xml"
  val xml = XML.load(confFile)


  test("test createHadoopFileIfNonExist") {
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.default.name", "hdfs://spark1:9000")
    val baseDir = "/user/tsingfu/tmp/kafka2hdfs/baseDir"
    val dir1 = baseDir + "/dir3/dir2/dir1"
    val file1 = baseDir + "/Dir3/Dir2/file1"

    val fs = FileSystem.get(URI.create(baseDir), hadoopConf)
    Kafka2Hdfs.createHadoopFileIfNonExist(hadoopConf, dir1, true)
    Kafka2Hdfs.createHadoopFileIfNonExist(hadoopConf, file1, false)

    assert(fs.exists(new Path(dir1)))
    assert(fs.exists(new Path(file1)))

    fs.delete(new Path(baseDir), true)


    val path1 = "hdfs://spark1:9000/user/tsingfu/tmp/kafka2hdfs/mytest"
    fs.delete(new Path(path1), true)

    val hadoop_outputDir = (xml \ "hadoop" \ "outputDir").text.trim
    val hadoop_prefix = (xml \ "hadoop" \ "test-output").text.trim


    val hadoop_outputWithPrefix = hadoop_outputDir + "/" + hadoop_prefix

    Kafka2Hdfs.createHadoopFileIfNonExist(hadoopConf, hadoop_outputWithPrefix, false)
    assert(fs.exists(new Path(hadoop_outputWithPrefix)))
    fs.delete(new Path(hadoop_outputWithPrefix), true)
  }

}
