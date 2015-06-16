package tools.redis.load.prototype.sparkImpl

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.JedisPool
import tools.redis.RedisUtils

import scala.collection.convert.wrapAsJava._
import scala.collection.convert.wrapAsScala._

import scala.collection.mutable.ArrayBuffer
import scala.xml.XML

/**
 * Created by tsingfu on 15/6/3.
 */
object File2SingleHash {

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
    val batchLimitForReids = props.getProperty("load.batchLimit.redis").trim.toInt
    val numThreads = props.getProperty("load.numThreads").trim.toInt
    val loadMethod = props.getProperty("load.method").trim

    val overwrite = props.getProperty("load.overwrite").trim.toBoolean
    val appendSeperator = props.getProperty("load.appendSeperator").trim

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

            load2singlehash(batchArrayBuffer.toArray, columnSeperator,
              hashName,
              fieldIdxes, fieldSeperator,
              valueIdxes, valueSeperator,
              jedisPools(jedisPoolId),loadMethod, batchLimitForReids,
            overwrite, appendSeperator)

            batchSize = 0
            numBatches += 1
          }

          result
        }

        def load2singlehash(lines: Array[String], columnSeperator:String,
                        hashName:String,
                        fieldIdxes:Array[Int], fieldSeperator:String,
                        valueIdxes:Array[Int], valueSeperator:String,
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

          for(line<-lines){

            val lineArray = line.split(columnSeperator).map(_.trim)

            val field = fieldIdxes.map(lineArray(_)).mkString(fieldSeperator)
            val value = valueIdxes.map(lineArray(_)).mkString(valueSeperator)

            if(value != null){

              loadMethod match {
                case "hset" =>
                  if(overwrite) {//如果 overwrite == true， 直接插入
                    jedis.hset(hashName, field, value)
                  } else {//如果 overwrite != true， 先查询，检查是否含有要append的值，没有append，有则不做操作
                  val value_exist = jedis.hget(hashName, field)
                    if(value_exist == null){
                      jedis.hset(hashName, field, value)
                    } else {
                      if(!value_exist.split(appendSeperator).contains(value)){
                        jedis.hset(hashName, field, value_exist + appendSeperator + value)
                      }
                    }
                  }
                case "hmset" =>
                  fvMap.put(field, value) // 新增hmset 的值
                  if(!overwrite) { //如果 overwrite!=true, 保存hash的field, value
                    fieldArray.append(field)
                    valueArray.append(value)
                  }
                case "pipeline_hset" =>
                  if(overwrite){//如果 overwrite==true, 新增pipeline.hset的值，
                    pipeline.hset(hashName, field, value)
                  } else {//如果overwrite!=true, 保存hash的field, value
                    pipeline.hget(hashName, field)
                    fieldArray.append(field)
                    valueArray.append(value)
                  }
                case _ =>
                  println("Error: unsupported loadMethod = " + loadMethod)
              }

              if(numInBatch == batchLimit){ // numInbatch对pipeline_hset，hmset有效，hset每个set一次提交
                loadMethod match {
                  case "hset" =>
                  case "hmset" =>
                    if(overwrite){//如果 overwrite==true，批量覆盖；
                      jedis.hmset(hashName, fvMap)
                    } else {//如果overwrite!=true，批量获取已存在值，如果值存在且不含有要加载的值，则追加，如果值存在且含有要加载的值，跳过；如果值不存在，插入
                    val values_exist = jedis.hmget(hashName, fieldArray: _*)
                      values_exist.zipWithIndex.foreach(v0idx=>{
                        val (v_exist, i) = v0idx
                        if(v_exist != null){
                          if(!v_exist.split(appendSeperator).contains(valueArray(i))){
                            fvMap.put(fieldArray(i), v_exist + appendSeperator+ valueArray(i))
                          } else {
                            fvMap.remove(fieldArray(i))
                          }
                        }
                      })
                      if(fvMap.size > 0){
                        jedis.hmset(hashName, fvMap)
                      }
                    }

                    fvMap.clear()
                    fieldArray.clear()
                    valueArray.clear()

                  case "pipeline_hset" => //如果 overwrite==true，批量覆盖；
                    if(!overwrite){//如果overwrite!=true，批量获取已存在值，如果值存在且不含有要加载的值，则追加，如果值存在且含有要加载的值，跳过；如果值不存在，插入
                    val values_exist = pipeline.syncAndReturnAll().asInstanceOf[List[String]]
                      values_exist.zipWithIndex.foreach(v0idx=>{
                        val (v_exist, i) = v0idx
                        if(v_exist !=null){
                          if(!v_exist.split(appendSeperator).contains(valueArray(i))){
                            pipeline.hset(hashName, fieldArray(i), v_exist + appendSeperator + valueArray(i))
                          }
                        } else {
                          pipeline.hset(hashName, fieldArray(i), valueArray(i))
                        }
                      })
                    }

                    pipeline.sync()
                    fieldArray.clear()
                    valueArray.clear()

                  case _ =>
                    println("Error: unsupported loadMethod = " + loadMethod)
                }

                numBatches += 1
                numInBatch = 0
              }

            } else {
              println("[INFO] filtered record = " + line)
            }
          }

          if(numInBatch > 0){//hmset/pipeline_hset，如果 0 < numInBatches < batchLimit，再执行一次
            loadMethod match {
              case "hset" =>
              case "hmset" =>
                if(overwrite){
                  jedis.hmset(hashName, fvMap)
                } else {
                  val values_exist = jedis.hmget(hashName, fieldArray: _*)
                  values_exist.zipWithIndex.foreach(v0idx=>{
                    val (v_exist, i) = v0idx
                    if(v_exist != null){
                      if(!v_exist.split(appendSeperator).contains(valueArray(i))){
                        fvMap.put(fieldArray(i), v_exist + appendSeperator+ valueArray(i))
                      } else {
                        fvMap.remove(fieldArray(i))
                      }
                    }
                  })
                  if(fvMap.size > 0){
                    jedis.hmset(hashName, fvMap)
                  }
                }

                fvMap.clear()
                fieldArray.clear()
                valueArray.clear()
              case "pipeline_hset" =>
                if(!overwrite){
                  val values_exist = pipeline.syncAndReturnAll().asInstanceOf[List[String]]
                  values_exist.zipWithIndex.foreach(v0idx=>{
                    val (v_exist, i) = v0idx
                    if(v_exist !=null){
                      if(!v_exist.split(appendSeperator).contains(valueArray(i))){
                        pipeline.hset(hashName, fieldArray(i), v_exist + appendSeperator + valueArray(i))
                      }
                    } else {
                      pipeline.hset(hashName, fieldArray(i), valueArray(i))
                    }
                  })
                }

                pipeline.sync()
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
    val hashName = (conf \ "load" \ "hashName").text.trim
    val fieldIdxes = (conf \ "load" \ "fieldIdxes").text.trim
    val fieldSeperator = (conf \ "load" \ "fieldSeperator").text.trim
    val valueIdxes = (conf \ "load" \ "valueIdxes").text.trim
    val valueSeperator = (conf \ "load" \ "valueSeperator").text.trim

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

    props.put("load.hashName", hashName)
    props.put("load.fieldIdxes", fieldIdxes)
    props.put("load.fieldSeperator", fieldSeperator)
    props.put("load.valueIdxes", valueIdxes)
    props.put("load.valueSeperator", valueSeperator)

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
