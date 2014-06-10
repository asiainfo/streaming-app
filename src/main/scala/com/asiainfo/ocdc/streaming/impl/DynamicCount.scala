package com.asiainfo.ocdc.streaming

import scala.collection.mutable.LinkedList
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.SparkContext
import org.apache.spark.rdd.NewHadoopRDD
import scala.collection.JavaConverters.asScalaBufferConverter
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Get
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import scala.collection.mutable.LinkedList
/**
 *@author 宿荣全
 *@deprecated 动态累加
 *@version 1.0
 *@since 2014.6.10
 */
object DynamicCount {

  def main(args: Array[String]) {

    val (master, serverIP, serverPort) = ("local[2]", "localhost", "9999")

    // 创建StreamingContext，10秒一个批次
    val ssc = new StreamingContext(master, "Sutest", Seconds(10), System.getenv("SPARK_HOME"), StreamingContext.jarOfClass(DynamicCount.this.getClass))
    // 创建一个DStream来连接
    val lines = ssc.socketTextStream(serverIP, serverPort.toInt)

    // words 应该为从xml文件读取一些规则后的流数据，此处只是个例子
    val words: DStream[(String,Int)] = lines.flatMap(_.split(" ")).map(word => (word, 1))
    // 动态累加数据处理core
    val hbaseiterm = words.groupByKey.map(f => new DynamicCount().doAction(f))
    hbaseiterm.print
    //    // 输出结果
    ssc.start(); // 开始
    ssc.awaitTermination(); // 计算完毕退出
  }
}

/**
 *@author 宿荣全
 *@deprecated 动态累加
 *@version 1.0
 *@since 2014.6.10
 */
class DynamicCount {
  
  /**
   * 功能：从HBASE动态表中取值累加计算Dtream的值重新存到HBASE中去
   */
  def doAction (f:(String, Seq[Int])) {
    // TODO hbparam：应为从外部xml读取的数据，为测试此处手动设假值
      val hbparam: Array[String] = new Array[String](4)
      //表名
      hbparam(0) = "dynamicCount"
      // 行键
      hbparam(1) = f._1
      // 列族
      hbparam(2) = "count"
      // 列名
      hbparam(3) = "value"
      updateHbase(hbparam, editData(readHbase(hbparam), f._2))
  }

  /**
   * 根据表名,行键,列族,列名从HBASE中取值<br>
   * hbparam中有四个参数如下：<br>
   * tableName：表名<br>
   * rowKey：行键<br>
   * cloumn：列族<br>
   * iterm：列名<br>
   * return :返回根据以上四个条件从hbase中查出的值［String］
   */
  def readHbase(hbparam: Array[String]): String = {
    // 传入的参数不全，则返回null
    hbparam.foreach(f => { if (f == null || f.isEmpty()) { Console.err.println("输入查询hbase的条件不足，请检查参数配置！"); return null } })

    val (tableName, rowKey, cloumn, iterm) = (hbparam(0), hbparam(1), hbparam(2), hbparam(3))
    val table: HTable = connect(tableName)
    val theget = new Get(Bytes.toBytes(rowKey.trim()))
    val result = table.get(theget)

    var rsString: String = null
    if (result == null || result.isEmpty) {
      Console println table.getName() + "表中无此记录！KEY＝［" + rowKey + "]"
    } else {
      val reset = result.getValue(Bytes.toBytes(cloumn.trim()), Bytes.toBytes(iterm.trim()))
      if (!(reset != null && reset.isEmpty)) rsString = new String(reset)
    }
    Console println tableName + "-[" + rowKey + "-" + cloumn + ":" + iterm + "]=" + rsString
    return rsString
  }

  /**
   * 编辑从hbase取来的数据并返回
   */
  def editData(itemValue: String, seqs: Seq[Int]): String = {

    if (itemValue == null || itemValue.isEmpty()) return null
    // Dstream中的值想加
    var total = 0l
    seqs.foreach(f => (total = total + f))
    // 编辑从hbase取来看数据
    Console println "[editData:]动态累加结果值：" + (itemValue.toLong + total)
    val result = (itemValue.toLong + total).toString
    return result
  }

  /**
   * 把动态累完成的数据按rowkey更新到hbase中
   */
  def updateHbase(hbparam: Array[String], value: String) {
    // 更新数据check
     if (value == null || value.isEmpty()) return
    // 传入的参数不全，则返回null
    hbparam.foreach(f => { if (f == null || f.isEmpty()) { Console.err.println("输入更新hbase的条件不足，请检查参数配置！"); return }})
    
    val (tableName, rowKey, cloumn, iterm) = (hbparam(0), hbparam(1), hbparam(2), hbparam(3))
    val table: HTable = connect(tableName)
    var theput = new Put(Bytes.toBytes(rowKey))
    Console println "[updateHbase:]动态累加结果更新值：［" + hbparam(0)+"-"+hbparam(1)+"-"+hbparam(2)+":"+hbparam(3)+"="+ value +"]"
    theput.add(Bytes.toBytes(cloumn), Bytes.toBytes(iterm), Bytes.toBytes(value))
    table.put(theput)
  }

  /**
   * 取得hbase表
   */
  def connect(TableName: String): HTable = {
    // 连接hbase的配置
    val hbaseConfiguration = HBaseConfiguration.create()
    hbaseConfiguration.addResource("./mnt/hgfs/linuxshare/ochadoop-och3.1.0/hbase-0.96.1.1-cdh5.0.0-och3.1.0/conf/hbase-site.xml")
    hbaseConfiguration.set(TableInputFormat.INPUT_TABLE, TableName)

    // 从表中取数据get
    val table = new HTable(hbaseConfiguration, TableName)
    return table
  }
}