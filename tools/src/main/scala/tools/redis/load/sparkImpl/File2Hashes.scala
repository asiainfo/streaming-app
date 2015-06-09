package tools.redis.load.sparkImpl

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisPool
import tools.redis.RedisUtils

import scala.collection.convert.wrapAsJava._
import scala.collection.convert.wrapAsScala._

import scala.collection.mutable.ArrayBuffer
import scala.xml.XML

/**
 * Created by tsingfu on 15/6/3.
 */
object File2Hashes {

  val logger = LoggerFactory.getLogger(this.getClass)

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
    val filename = props.getProperty("load.filename").trim
    val fileEncode = props.getProperty("load.fileEncode").trim
    val columnSeperator = props.getProperty("load.columnSeperator").trim

    val hashNamePrefix = props.getProperty("load.hashNamePrefix").trim
    val hashIdxes = props.getProperty("load.hashIdxes").trim.split(",").map(_.trim.toInt)
    val hashSeperator = props.getProperty("load.hashSeperator").trim

    val valueIdxes = props.getProperty("load.valueIdxes").trim.split(",").map(_.trim.toInt)
    val fieldNames = props.getProperty("load.fieldNames").trim.split(",").map(_.trim)

    val batchLimit = props.getProperty("load.batchLimit").trim.toInt
    val batchLimitForRedis = props.getProperty("load.batchLimit.redis").trim.toInt
    val numThreads = props.getProperty("load.numThreads").trim.toInt
    val loadMethod = props.getProperty("load.method").trim

    val overwrite = props.getProperty("load.overwrite").trim.toBoolean
    val appendSeperator = props.getProperty("load.appendSeperator").trim

    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(sparkConf)

    val data = sc.textFile(filename, numThreads)

    data.mapPartitions(iter =>{

      val startMs = System.currentTimeMillis()

      val jedisPools = servers.split(",").map(server=>{
        val hostPort = server.split(":").map(_.trim)
        val host = hostPort(0)
        val port = hostPort(1).toInt
        println("host = " + host +", port ="+port)
        RedisUtils.init_jedisPool(host, port, timeout, database, password, maxTotal, maxIdle, minIdle)
      })
      val numPools = jedisPools.length
      var numBatches = 0

      def jedisPoolId = numBatches % numPools

      val mMap1 = scala.collection.mutable.Map[String, String]()
      var hashName = ""

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

            load2hashes(batchArrayBuffer.toArray, columnSeperator,
              hashNamePrefix, hashIdxes, hashSeperator,
              fieldNames, valueIdxes,
              jedisPools(jedisPoolId),
              loadMethod, batchLimitForRedis, overwrite, appendSeperator)
            
            batchSize = 0
            numBatches += 1
          }

          result
        }

        def load2hashes(lines: Array[String], columnSeperator: String,
                        hashNamePrefix: String, hashIdxes: Array[Int], hashSeperator: String,
                        fieldNames: Array[String], valueIdxes: Array[Int],
                        jedisPool: JedisPool, loadMethod: String = "hmset", batchLimit:Int,
                        overwrite: Boolean, appendSeperator: String): Unit ={

          val jedis = jedisPool.getResource
          val pipeline = jedis.pipelined()

          var numProcessed = 0
          var numInBatch = 0
          var numBatches = 0

          val fvMap = scala.collection.mutable.Map[String, String]()
          val fieldArray = ArrayBuffer[String]()
          val valueArray = ArrayBuffer[String]()
          val hashNameArray = ArrayBuffer[String]()

          for(line<-lines) {

            val lineArray = line.split(columnSeperator).map(_.trim)
            numInBatch += 1
            //      logger.debug("[debug1] hashIdxes = " + hashIdxes.mkString("[",",","]") + " # " * 1  + "hashValues = " + (for (idx <- hashIdxes) yield lineArray(idx)).mkString("[",",","]"))
            val hashName = hashNamePrefix + (for (idx <- hashIdxes) yield lineArray(idx)).mkString(hashSeperator)

            fieldNames.zip(valueIdxes).foreach(kv => {
              val (k, i) = kv
              loadMethod match {
                case "hset" =>
                  if (overwrite) {
                    //如果 overwrite == true， 直接插入
                    logger.debug("[debug2] hset(" + hashName + ", " + k + "," + lineArray(i) + ")")

                    jedis.hset(hashName, k, lineArray(i))
                  } else {
                    //如果 overwrite != true， 先查询，检查是否含有要append的值，没有append，有则不做操作
                    val value_exist = jedis.hget(hashName, k)
                    if (value_exist == null) {
                      jedis.hset(hashName, k, lineArray(i))
                    } else {
                      if (!value_exist.split(appendSeperator).contains(lineArray(i))) {
                        jedis.hset(hashName, k, value_exist + appendSeperator + lineArray(i))
                      }
                    }
                  }

                case "hmset" =>
                  fvMap.put(k, lineArray(i)) // 新增hmset 的值
                  if (!overwrite) {
                    //如果 overwrite!=true, 保存hash的field, value
                    fieldArray.append(k)
                    valueArray.append(lineArray(i))
                  }
                case "pipeline_hset" =>
                  if (overwrite) {
                    //如果 overwrite==true, 新增pipeline.hset的值，
                    pipeline.hset(hashName, k, lineArray(i))

                  } else {
                    //如果overwrite!=true, 保存hash的field, value
                    pipeline.hget(hashName, k)
                    hashNameArray.append(hashName)
                    fieldArray.append(k)
                    valueArray.append(lineArray(i))
                  }
              }
            })

            loadMethod match {
              case "hset" =>
              case "hmset" => // 因为每一行的hashName不同，所以hmset操作比较特别，每行一次提交
                if (overwrite) {
                  jedis.hmset(hashName, fvMap)
                } else {
                  //如果overwrite != true, 批量获取已存在的值，如果值存在且不含有要加载的值，则追加，如果值存在且含有要加载的值，跳过；如果值不存在，插入
                  val values_exist = jedis.hmget(hashName, fieldArray: _*)
                  values_exist.zipWithIndex.foreach(v0idx => {
                    val (v_exist, i) = v0idx
                    if (v_exist != null) {
                      if (!v_exist.split(appendSeperator).contains(valueArray(i))) {
                        fvMap.put(fieldArray(i), v_exist + appendSeperator + valueArray(i))
                      } else {
                        fvMap.remove(fieldArray(i))
                      }
                    }
                  })
                  if (fvMap.size > 0) {
                    jedis.hmset(hashName, fvMap)
                  }
                }

                fvMap.clear()
                fieldArray.clear()
                valueArray.clear()

              case "pipeline_hset" =>
              case _ =>
                println("Error: unsupported loadMethod = " + loadMethod)
            }

            if (numInBatch == batchLimit) {
              // numInbatch对pipeline_hset有效，hset每个set一次提交；hmset每行一次提交
              loadMethod match {
                case "hset" =>
                case "hmset" =>
                case "pipeline_hset" => //如果 overwrite==true，批量覆盖；
                  if (!overwrite) {
                    //如果overwrite!=true，批量获取已存在值，如果值存在且不含有要加载的值，则追加，如果值存在且含有要加载的值，跳过；如果值不存在，插入
                    val values_exist = pipeline.syncAndReturnAll().asInstanceOf[List[String]]
                    values_exist.zipWithIndex.foreach(v0idx => {
                      val (v_exist, i) = v0idx
                      if (v_exist != null) {
                        if (!v_exist.split(appendSeperator).contains(valueArray(i))) {
                          pipeline.hset(hashNameArray(i), fieldArray(i), v_exist + appendSeperator + valueArray(i))
                        }
                      } else {
                        pipeline.hset(hashNameArray(i), fieldArray(i), valueArray(i))
                      }
                    })
                  }

                  pipeline.sync()
                  hashNameArray.clear()
                  fieldArray.clear()
                  valueArray.clear()
                case _ =>
                  println("Error: unsupported loadMethod = " + loadMethod)
              }
              numBatches += 1
              numInBatch = 0
            }
            numProcessed += 1
          }

          if(numInBatch > 0){ //pipeline_hset如果 0 < numInBatches < batchLimit，再执行一次
            loadMethod match {
              case "hset" =>
              case "hmset" =>
              case "pipeline_hset" =>
                if(!overwrite){
                  val values_exist = pipeline.syncAndReturnAll().asInstanceOf[List[String]]
                  values_exist.zipWithIndex.foreach(v0idx=>{
                    val (v_exist, i) = v0idx
                    if(v_exist !=null){
                      if(!v_exist.split(appendSeperator).contains(valueArray(i))){
                        pipeline.hset(hashNameArray(i), fieldArray(i), v_exist + appendSeperator + valueArray(i))
                      }
                    } else {
                      pipeline.hset(hashNameArray(i), fieldArray(i), valueArray(i))
                    }
                  })
                }

                pipeline.sync()
                hashNameArray.clear()
                fieldArray.clear()
                valueArray.clear()
              case _ =>
                println("Error: unsupported loadMethod = " + loadMethod)
            }
            numBatches += 1
            numInBatch = 0
          }

          //回收资源，释放jedis，但不释放 jedisPool
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

    val hashNamePrefix = (conf \ "load" \ "hashNamePrefix").text.trim
    val hashIdxes = (conf \ "load" \ "hashIdxes").text.trim
    val hashSeperator = (conf \ "load" \ "hashSeperator").text.trim
    val valueIdxes = (conf \ "load" \ "valueIdxes").text.trim
    val fieldNames = (conf \ "load" \ "fieldNames").text.trim

    val batchLimit = (conf \ "load" \ "batchLimit").text.trim
    val batchLimitForRedis = (conf \ "load" \ "batchLimit.redis").text.trim
    val numThreads = (conf \ "load" \ "numThreads").text.trim
    val loadMethod = (conf \ "load" \ "method").text.trim

    val overwrite = (conf \ "load" \ "overwrite").text.trim
    val appendSeperator = (conf \ "load" \ "appendSeperator").text.trim

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

    props.put("load.hashNamePrefix", hashNamePrefix)
    props.put("load.hashIdxes", hashIdxes)
    props.put("load.hashSeperator", hashSeperator)

    props.put("load.valueIdxes", valueIdxes)
    props.put("load.fieldNames", fieldNames)

    props.put("load.batchLimit", batchLimit)
    props.put("load.batchLimit.redis", batchLimitForRedis)
    props.put("load.numThreads", numThreads)
    props.put("load.method", loadMethod)

    props.put("load.overwrite", overwrite)
    props.put("load.appendSeperator", appendSeperator)

    println("="*80)
    props.list(System.out)
    println("="*80)

    props
  }

  def verifyConf(props: Properties): Unit ={

  }
}
