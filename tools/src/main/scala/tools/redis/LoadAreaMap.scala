package tools.redis

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import redis.clients.jedis.Jedis

import scala.collection.mutable.ArrayBuffer
import scala.xml.XML

import scala.collection.convert.wrapAsScala._
import scala.collection.convert.wrapAsJava._

/**
 * Created by tsingfu on 15/4/27.
 */
object LoadAreaMap {

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println("WARN: args.length = " + args.length + "\n" + "You should specify a confXmlFile")
      System.exit(-1)
    }

    val confXmlFile = args(0)
    loadAreaMap(confXmlFile)

  }


  def convertHex(in:String):String ={
    var lc = Integer.toHexString(in.toInt).toUpperCase
    while(lc.length<4){lc = "0" + lc}
    lc
  }


  /**
   * 将基站与区域映射加载到指定 redis 中
   * 提供的文件支持2种格式：
   * 格式1：formatType=1, lacIdx, cellIdx, XXXareaFlag
   * 格式2：formatType=2, lacIdx, cellIdx, areaName
   *
   * 20150604 支持使用多个 codis 代理
   */
  def loadAreaMap(confXmlFile: String): Unit = {

    val props = init_props_fromXml(confXmlFile)

    val servers = props.getProperty("redis.servers").trim
    val database = props.getProperty("redis.database", "0").trim.toInt
    val timeout = props.getProperty("redis.timeout").trim.toInt
    val password = props.getProperty("redis.password", null)

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

    val conversion10to16Idxes = props.getProperty("load.conversion10to16.idxes").trim.split(",").map(_.trim.toInt)

    val valueIdx = props.getProperty("load.valueIdx").trim.toInt
    val valueMapEnabled = props.getProperty("load.valueMapEnabled").trim
    val valueMap = props.getProperty("load.valueMap")

    val overwrite = props.getProperty("load.overwrite").trim
    val valueSeperator = props.getProperty("load.valueSeperator").trim

    val batchLimit = props.getProperty("load.batchLimit").trim.toInt
    //    val numThreads = props.getProperty("load.numThreads").trim.toInt
    val loadMethod = props.getProperty("load.method").trim

    val startMS = System.currentTimeMillis()
    var runningTime: Long = -1

    // 初始化 jedisPool, jedis, pipeline
    val jedisPools = servers.split(",").map(server=>{
      val hostPort = server.split(":").map(_.trim)
      val host = hostPort(0)
      val port = hostPort(1).toInt
      println("host = " + host +", port ="+port)
      RedisUtils.init_jedisPool(host, port, timeout, database, password, maxTotal, maxIdle, minIdle)
    })
    val numPools = jedisPools.length
    val jedises = jedisPools.map(_.getResource)
    val pipelines = jedises.map(_.pipelined)

    //获取文件记录数
    val fileRecordsNum = scala.io.Source.fromFile(filename, fileEncode).getLines().length

    var positionIdx = 0
    var numInBatch = 0
    var numBatches = 0
    def jedisPoolId = numBatches % numPools

    val fieldValueMap = scala.collection.mutable.Map[String, String]() //记录批量hmset的field-value对
    val fieldArray = new ArrayBuffer[String]() //记录批量hmget的 fields
    val valueArray = new ArrayBuffer[String]() // 记录文件中的value


    for (line <- scala.io.Source.fromFile(filename, fileEncode).getLines()) {
      try{
        val lineArray = line.split(columnSeperator)
        //      val field = lineArray(lacColIdx) + ":" + lineArray(cellColIdx)
//        val field = convertHex(lineArray(lacColIdx)) + ":" + convertHex(lineArray(cellColIdx))

        positionIdx += 1
        numInBatch += 1

        val field = fieldIdxes.map(idx=>{
          if(conversion10to16Idxes.contains(idx)) {
            convertHex(lineArray(idx))
          } else {
            lineArray(idx)
          }
        }).mkString(fieldSeperator)

        val value0 =
          if(valueMapEnabled == "true"){
            if ( lineArray(valueIdx) == "1" || lineArray(valueIdx).toLowerCase == "true"){
              valueMap
            } else {
              null
            }
          } else {
            lineArray(valueIdx)
          }

        if(value0 != null){

          loadMethod match {
            case "hset" =>
              val value1 = jedises(jedisPoolId).hget(hashName, field)
              if(value1 != null){
                if (!value1.split(valueSeperator).contains(value0)){
                  jedises(jedisPoolId).hset(hashName, field, value1 + valueSeperator + value0)
                }
              } else {
                jedises(jedisPoolId).hset(hashName, field, value0)
              }
            case "hmset" =>
              fieldArray.append(field)
              valueArray.append(value0)
            case "pipeline_hset" =>
              fieldArray.append(field)
              valueArray.append(value0)
              pipelines(jedisPoolId).hget(hashName, field)
            case _ =>
              println("Error: unsupported loadMethod = " + loadMethod)
          }

          if(numInBatch == batchLimit){
            loadMethod match {
              case "hset" =>
              case "hmset" =>
                val values_tmp = jedises(jedisPoolId).hmget(hashName, fieldArray: _*)

                values_tmp.zipWithIndex.foreach(v1idx =>{
                  val (v1, idx) = v1idx // v1 查询的值
                  if (v1 != null){
                    if(! v1.split(valueSeperator).contains(valueArray(idx))){
                      fieldValueMap.put(fieldArray(idx), v1 + valueSeperator + valueArray(idx))
                    }
                  } else {
                    fieldValueMap.put(fieldArray(idx), valueArray(idx))
                  }
                })
                jedises(jedisPoolId).hmset(hashName, fieldValueMap)
                fieldArray.clear()
                valueArray.clear()

                numInBatch = 0
                numBatches += 1

              case "pipeline_hset" =>
                val values_tmp = pipelines(jedisPoolId).syncAndReturnAll()

                values_tmp.zipWithIndex.foreach(v1idx =>{
                  val (v1, idx) = v1idx // v1 查询的值
                  if (v1 != null){
                    if(! v1.asInstanceOf[String].split(valueSeperator).contains(valueArray(idx))){
                      pipelines(jedisPoolId).hset(hashName, fieldArray(idx), v1 + valueSeperator + valueArray(idx))
                    }
                  } else {
                    pipelines(jedisPoolId).hset(hashName, fieldArray(idx), valueArray(idx))
                  }
                })

                pipelines(jedisPoolId).syncAndReturnAll()

                fieldArray.clear()
                valueArray.clear()

                numInBatch = 0
                numBatches += 1

              case _ =>
                println("Error: unsupported loadMethod = " + loadMethod)
            }

            runningTime = System.currentTimeMillis() - startMS
            println(sdf.format(new Date()) + " [info] loading file " + filename +
                    ", status : positionIdx = " + positionIdx + ", progress percent = " + (positionIdx * 100.0 / fileRecordsNum) +" %" +
                    ", runningTime = " + runningTime +", speed = " + positionIdx / (runningTime / 1000.0) +" records/s")
          }

        } else {
          println("[INFO] filtered record = " + line)
        }
      }catch {
        case ex: Exception =>
          println("[WARN] got excetion: " + ex.printStackTrace())
          println("[WARN] problem record: " + line)

          //中途出现异常，并不退出
          println(sdf.format(new Date()) + " retry with new jedis after sleep 30 s ...")
          jedisPools(jedisPoolId).returnResourceObject(jedises(jedisPoolId))
          Thread.sleep(5 * 1000)

          jedises(jedisPoolId) = jedisPools(jedisPoolId).getResource
          pipelines(jedisPoolId) = jedises(jedisPoolId).pipelined()
      }
    }

    println("[debug] fieldArray.length = "+ fieldArray.length)
    // 捕获最后一次执行异常，退出
    try{
      if(fieldArray.length > 0){
        loadMethod match {
          case "hset" =>
          case "hmset" =>
            val values_tmp = jedises(jedisPoolId).hmget(hashName, fieldArray: _*)

            values_tmp.zipWithIndex.foreach(v1idx =>{
              val (v1, idx) = v1idx // v1 查询的值
              if (v1 != null){
                if(! v1.split(valueSeperator).contains(valueArray(idx))){
                  fieldValueMap.put(fieldArray(idx), v1 + valueSeperator + valueArray(idx))
                }
              } else {
                fieldValueMap.put(fieldArray(idx), valueArray(idx))
              }
            })
            jedises(jedisPoolId).hmset(hashName, fieldValueMap)
            fieldArray.clear()
            valueArray.clear()

            numInBatch = 0
            numBatches += 1

          case "pipeline_hset" =>
            val values_tmp = pipelines(jedisPoolId).syncAndReturnAll()

            values_tmp.zipWithIndex.foreach(v1idx =>{
              val (v1, idx) = v1idx // v1 查询的值
              if (v1 != null){
                if(! v1.asInstanceOf[String].split(valueSeperator).contains(valueArray(idx))){
                  pipelines(jedisPoolId).hset(hashName, fieldArray(idx), v1 + valueSeperator + valueArray(idx))
                }
              } else {
                pipelines(jedisPoolId).hset(hashName, fieldArray(idx), valueArray(idx))
              }
            })

            pipelines(jedisPoolId).syncAndReturnAll()

            fieldArray.clear()
            valueArray.clear()

            numInBatch = 0
            numBatches += 1

          case _ =>
          println("Error: unsupported loadMethod = " + loadMethod)
        }
      }
    }catch {
      case ex:Exception =>
        println("=" * 30 + " " + sdf.format(new Date()) + "\n" + ex.printStackTrace())
    }

    //打印统计信息
    val elapsedMS = System.currentTimeMillis() - startMS
    println(sdf.format(new Date()) + " [info] load file " + filename + " finished. " +
            "records = "+positionIdx+
            ", elspsed = " + elapsedMS + " ms, " + elapsedMS / 1000.0 + " s, " + elapsedMS / 1000.0 / 60.0 + " min" +
            ", speed = " + positionIdx / (elapsedMS / 1000.0) +" records/s" )

    //释放资源
    for(i<- 0 until numPools){
      jedisPools(i).returnResourceObject(jedises(i))
    }
    jedisPools.foreach(_.close())

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

    val conversion10to16Idxes = (conf \ "load" \"conversion10to16.idxes").text.trim

    val valueIdx = (conf \ "load" \ "valueIdx").text.trim

    val valueMapEnabled = (conf \ "load" \ "valueMapEnabled").text.trim
    val valueMap = (conf \ "load" \ "valueMap").text

    val overwrite = (conf \ "load" \ "overwrite").text.trim
    val valueSeperator = (conf \ "load" \ "valueSeperator").text.trim

    val batchLimit = (conf \ "load" \ "batchLimit").text.trim
    //    val numThreads = (conf \ "load" \ "numThreads").text.trim
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

    props.put("load.conversion10to16.idxes", conversion10to16Idxes)

    props.put("load.valueIdx", valueIdx)

    props.put("load.valueMapEnabled", valueMapEnabled)
    props.put("load.valueMap", valueMap)
    props.put("load.overwrite", overwrite)
    props.put("load.valueSeperator", valueSeperator)

    props.put("load.batchLimit", batchLimit)
    //    props.put("load.numThreads", numThreads)
    props.put("load.method", loadMethod)

    println("="*80)
    props.list(System.out)
    println("="*80)

    props
  }

  /**
   * 将基站与区域映射加载到指定 redis 中
   * 提供的文件支持2种格式：
   * 格式1：formatType=1, lacIdx, cellIdx, XXXareaFlag
   * 格式2：formatType=2, lacIdx, cellIdx, areaName
   *
   * @param filename 映射文件路径
   * @param serverPort redis/codis地址
   * @param hashName 加载到redis时，可以指定hash名
   */
  def loadAreaMap(filename: String, serverPort: String, dbNum: Int, hashName: String,
                  formatType: Int,
                  colSep: String = ",",
                  lacColIdx: Int = 0,
                  cellColIdx: Int = 1,
                  areaColIdx: Int = 2,
                  areaName: String = null): Unit = {

    val startMS = System.currentTimeMillis()

    if (formatType == 1) {
      assert(areaName != null, "specified invalid value for parameter areaName while formatType=" + formatType)
    }
    val serverPortArray = serverPort.split(":")
    val host = serverPortArray(0)
    val port = serverPortArray(1).toInt

    val password = if (serverPortArray.length == 3) serverPortArray(2) else null

    val jedis = new Jedis(host, port)
    jedis.select(dbNum)
    if (password != null) jedis.auth(password)

    for (line <- scala.io.Source.fromFile(filename, "UTF-8").getLines()) {
      try{
        val array = line.split(colSep)
        //      val lac_cell = array(lacColIdx) + ":" + array(cellColIdx)
        val lac_cell = convertHex(array(lacColIdx)) + ":" + convertHex(array(cellColIdx))

        val areaFlag = array(areaColIdx)
        val newAreaName = if (formatType == 1) {
          if (areaFlag == "1" || areaFlag == "true") areaName else "non-" + areaName
        } else {
          if (areaFlag == "") null else areaFlag
        }

        if (newAreaName != null) {
          var fieldValue = jedis.hget(hashName, lac_cell)
          val areaArray = if (fieldValue != null) fieldValue.split(",") else Array[String]()
          if (fieldValue == null) {
            fieldValue = newAreaName
            jedis.hset(hashName, lac_cell, fieldValue)
          } else {
            if (!areaArray.contains(newAreaName)) {
              fieldValue += "," + newAreaName
              jedis.hset(hashName, lac_cell, fieldValue)
            }
          }
        }

      }catch {
        case ex: Exception =>
          println("[WARN] got excetion: " + ex.getStackTrace)
          println("[WARN] problem record: " + line)
      }
      jedis.close()
    }
    val elapsedMS = System.currentTimeMillis() - startMS
    println("[info] load file " + filename + " finished. " +
            "elspsed = " + elapsedMS + " ms, " + elapsedMS / 1000.0 + " s, " + elapsedMS / 1000.0 / 60.0 + " min")
  }


}
