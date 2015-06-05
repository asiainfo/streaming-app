package tools.redis.sparkImpl

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.JedisPool
import tools.redis.RedisUtils

import scala.collection.convert.wrapAsJava._
import scala.collection.mutable.ArrayBuffer
import scala.xml.XML

/**
 * Created by tsingfu on 15/6/3.
 */
object LoadFile2SingleHash {

  def main(args: Array[String]): Unit ={
    val confXmlFile = args(0)

    val props = init_props_fromXml(confXmlFile)

    val servers = props.getProperty("redis.servers").trim
    val database = props.getProperty("redis.database", "0").trim.toInt
    val timeout = props.getProperty("redis.timeout").trim.toInt
    val password = props.getProperty("redis.password")

    val maxTotal = props.getProperty("jedisPool.maxTotal").trim.toInt
    val maxIdle = props.getProperty("jedisPool.maxIdle").trim.toInt
    val minIdle = props.getProperty("jedisPool.minIdle").trim.toInt

    val from = props.getProperty("load.from").trim
    assert(from=="file", "WARN: support only file From now")
    val filename = props.getProperty("load.filename").trim
    val fileEncode = props.getProperty("load.fileEncode").trim
    val columnSeperator = props.getProperty("load.columnSeperator").trim

    val hashName = props.getProperty("load.hashName").trim
    val fieldIdxes = props.getProperty("load.fieldIdxes").trim.split(",").map(_.trim.toInt)
    val fieldSeperator = props.getProperty("load.fieldSeperator").trim
    val valueIdxes = props.getProperty("load.valueIdxes").trim.split(",").map(_.trim.toInt)
    val valueSeperator = props.getProperty("load.valueSeperator").trim

    val batchLimit = props.getProperty("load.batchLimit").trim.toInt
    val numThreads = props.getProperty("load.numThreads").trim.toInt
    val loadMethod = props.getProperty("load.method").trim

    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(sparkConf)

    val data = sc.textFile(filename, numThreads)

    data.mapPartitions(iter =>{

//      val startMs = System.currentTimeMillis()

      val jedisPools = servers.split(",").map(server=>{
        val hostPort = server.split(":").map(_.trim)
        val host = hostPort(0)
        val port = hostPort(1).toInt
        println("host = " + host +", port ="+port)
        RedisUtils.init_jedisPool(host, port, timeout, database, password, maxTotal, maxIdle, minIdle)
      })
      val numPools = jedisPools.length

      var numBatches = 0
      val mMap1 = scala.collection.mutable.Map[String, String]()
      def jedisPoolId = numBatches % numPools

      new Iterator[String]{
        private[this] var current: String = _
        private[this] var currentPos: Int = -1
        private[this] var batchArray: Array[String] = _
        private[this] val batchArrayBuffer = new ArrayBuffer[String]()

        override def hasNext: Boolean ={
          iter.hasNext && batchNext()
        }

        override def next(): String ={
          batchArray(0)
        }

        var batchSize = 0
        println("[debug] currentPos = "+ currentPos+ ", numBatches = "+ numBatches +", batchSize = " + batchSize +
                ", numPools = "+numPools+ ", numBatches % numPools = "+ jedisPoolId)

        def batchNext(): Boolean ={
          var result = false
          batchArrayBuffer.clear()

          while (iter.hasNext && batchSize < batchLimit) {
            current = iter.next()
            batchArrayBuffer.append(current)

            batchSize += 1
            currentPos += 1
          }

          if(batchArrayBuffer.length > 0) {
            batchArray = batchArrayBuffer.toArray
            result = true

            // 批量取数据过程中，执行数据导入
            println("[debug] currentPos = "+ currentPos+ ", numBatches = "+ numBatches +", batchSize = " + batchSize +
                    ", numPools = "+numPools+ ", numBatches % numPools = "+ jedisPoolId)

            load2singlehash(jedisPools(jedisPoolId),batchArrayBuffer.toArray,
              columnSeperator, hashName, fieldIdxes, fieldSeperator, valueIdxes, valueSeperator, loadMethod )

            batchSize = 0
            numBatches += 1
          }

          result
        }

        def load2singlehash(jedisPool: JedisPool, batch: Array[String],
                        columnSeperator:String,
                        hashName:String,
                        fieldIdxes:Array[Int], fieldSeperator:String,
                        valueIdxes:Array[Int], valueSeperator:String,
                        loadMethod: String = "hmset"): Unit ={

          val jedis = jedisPool.getResource
          val pipeline = jedis.pipelined()

          for(line<-batch){

            val lineArray = line.split(columnSeperator).map(_.trim)

            val field = fieldIdxes.map(lineArray(_)).mkString(fieldSeperator)
            val value = valueIdxes.map(lineArray(_)).mkString(valueSeperator)

            loadMethod match {
              case "hset" =>
                jedis.hset(hashName, field, value)
              case "hmset" =>
                mMap1.put(field, value)
              case "pipeline_hset" =>
                pipeline.hset(hashName, field, value)
              case _ =>
                println("Error: unsupported loadMethod = " + loadMethod)
            }
          }

          loadMethod match {
            case "hset" =>
            case "hmset" =>
              jedis.hmset(hashName, mMap1)
              mMap1.clear()
            case "pipeline_hset" =>
              pipeline.sync()
            case _ =>
              println("Error: unsupported loadMethod = " + loadMethod)
          }

          jedisPool.returnResourceObject(jedis)
        }
      }
    }).count()
  }

  def init_props_fromXml(confXmlFile: String): Properties ={

    val conf = XML.load(confXmlFile)
    val servers = (conf \ "redis" \ "servers").text.trim

    val database = (conf \ "redis" \ "database").text.trim
    val timeout = (conf \ "redis" \ "timeout").text.trim
    val passwd = (conf \ "redis" \ "password").text.trim
    val password = if (passwd == "" || passwd == null) null else passwd

    val maxTotal = (conf \ "jedisPool" \ "maxTotal").text.trim
    val maxIdle = (conf \ "jedisPool" \ "maxIdle").text.trim
    val minIdle = (conf \ "jedisPool" \ "minIdle").text.trim

    val from = (conf \ "load" \ "from").text.trim
    val filename = (conf \ "load" \ "filename").text.trim
    val fileEncode = (conf \ "load" \ "fileEncode").text.trim

    val columnSeperator = (conf \ "load" \ "columnSeperator").text.trim
    val hashName = (conf \ "load" \ "hashName").text.trim
    val fieldIdxes = (conf \ "load" \ "fieldIdxes").text.trim
    val fieldSeperator = (conf \ "load" \ "fieldSeperator").text.trim
    val valueIdxes = (conf \ "load" \ "valueIdxes").text.trim
    val valueSeperator = (conf \ "load" \ "valueSeperator").text.trim

    val batchLimit = (conf \ "load" \ "batchLimit").text.trim
    val numThreads = (conf \ "load" \ "numThreads").text.trim
    val loadMethod = (conf \ "load" \ "method").text.trim

    val props = new Properties()
    props.put("redis.servers", servers)
    props.put("redis.database", database)
    props.put("redis.timeout", timeout)

    if(password != null || password == "") props.put("redis.password", password)

    props.put("jedisPool.maxTotal", maxTotal)
    props.put("jedisPool.maxIdle", maxIdle)
    props.put("jedisPool.minIdle", minIdle)

    props.put("load.from", from)
    props.put("load.filename", filename)
    props.put("load.fileEncode", fileEncode)
    props.put("load.columnSeperator", columnSeperator)

    props.put("load.hashName", hashName)
    props.put("load.fieldIdxes", fieldIdxes)
    props.put("load.fieldSeperator", fieldSeperator)
    props.put("load.valueIdxes", valueIdxes)
    props.put("load.valueSeperator", valueSeperator)

    props.put("load.batchLimit", batchLimit)
    props.put("load.numThreads", numThreads)
    props.put("load.method", loadMethod)

    println("="*80)
    props.list(System.out)
    println("="*80)

    props
  }

  def verifyConf(props: Properties): Unit ={

  }
}
