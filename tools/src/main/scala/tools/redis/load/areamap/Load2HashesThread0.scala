package tools.redis.load.areamap

import java.util.concurrent.Callable

import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisPool
import tools.redis.RedisUtils
import tools.redis.load.FutureTaskResult

import scala.collection.mutable.ArrayBuffer
import scala.collection.convert.wrapAsJava._
import scala.collection.convert.wrapAsScala._

/**
 * Created by tsingfu on 15/6/12.
 */
class Load2HashesThread0(lines: Array[String], columnSeperator: String,
                        hashNamePrefix: String, hashIdxes: Array[Int], hashSeperator: String, conversion10to16Idxes: Array[Int],
                        fieldName: String,
                        valueIdx: Int, valueMapEnabled: Boolean, valueMap: String,
                        jedisPool: JedisPool,
                        loadMethod:String, batchLimit:Int,
                        overwrite: Boolean, appendSeperator: String,
                        taskResult: FutureTaskResult) extends Callable[FutureTaskResult] with Thread.UncaughtExceptionHandler {

  val logger = LoggerFactory.getLogger(this.getClass)

  override def call(): FutureTaskResult = {

    val jedis = jedisPool.getResource
    val pipeline = jedis.pipelined()

    var numProcessed = 0
    var numInBatch = 0
    var numBatches = 0

    val fvMap = scala.collection.mutable.Map[String, String]()
    val fieldArray = ArrayBuffer[String]()
    val valueArray = ArrayBuffer[String]()
    val hashNameArray = ArrayBuffer[String]()

    logger.info("batchSize = " + lines.length)
    for(line <- lines){
      numProcessed += 1
      logger.debug("numProcessed = "+numProcessed+", numBatches = "+ numBatches +", numInBatch = "+ numInBatch +", line => " + line.split(columnSeperator).mkString("[",",","]"))

      try{
        val lineArray = line.split(columnSeperator).map(_.trim)
        numInBatch += 1
        val hashName = hashNamePrefix + (for (idx <- hashIdxes)
          yield if (conversion10to16Idxes.contains(idx))
            RedisUtils.convertDecimaltoHex(lineArray(idx))
          else lineArray(idx)
        ).mkString(hashSeperator)

        val value = if(valueMapEnabled) {
          if(lineArray(valueIdx) == "true" || lineArray(valueIdx) == "1") valueMap else null
        } else {
          lineArray(valueIdx)
        }
        logger.debug("field="+fieldName+", value="+value)

        if(value != null){
          loadMethod match {
            case "hset" =>
              if(overwrite) {//如果 overwrite == true， 直接插入
                logger.debug("hset(" + hashName +", "+fieldName +","+value+")")

                jedis.hset(hashName, fieldName, value)
              } else {//如果 overwrite != true， 先查询，检查是否含有要append的值，没有append，有则不做操作
              val value_exist = jedis.hget(hashName, fieldName)
                if(value_exist == null){
                  logger.debug("hset(" + hashName +", "+fieldName +","+value+")")
                  jedis.hset(hashName, fieldName, value)
                } else {
                  if(!value_exist.split(appendSeperator).contains(value)){
                    logger.debug("hset(" + hashName +", "+fieldName +","+(value_exist + appendSeperator + value)+")")
                    jedis.hset(hashName, fieldName, value_exist + appendSeperator + value)
                  }
                }
              }

            case "hmset" =>
              fvMap.put(fieldName, value) // 新增hmset 的值
              if(!overwrite){ //如果 overwrite!=true, 保存hash的field, value
                fieldArray.append(fieldName)
                valueArray.append(value)
              }
            case "pipeline_hset" =>
              if(overwrite){ //如果 overwrite==true, 新增pipeline.hset的值，
                pipeline.hset(hashName, fieldName, value)
              } else {  //如果overwrite!=true, 保存hash的field, value
                pipeline.hget(hashName, fieldName)
                hashNameArray.append(hashName)
                fieldArray.append(fieldName)
                valueArray.append(value)
              }
          }

          loadMethod match {
            case "hset" =>
            case "hmset" => // 因为每一行的hashName不同，所以hmset操作比较特别，每行一次提交
              if(overwrite){
                jedis.hmset(hashName, fvMap)
              } else { //如果overwrite != true, 批量获取已存在的值，如果值存在且不含有要加载的值，则追加，如果值存在且含有要加载的值，跳过；如果值不存在，插入
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
            case _ =>
              logger.error("Error: unsupported loadMethod = " + loadMethod)
          }

          if(numInBatch == batchLimit){ // numInbatch对pipeline_hset有效，hset每个set一次提交；hmset每行一次提交
            loadMethod match {
              case "hset" =>
              case "hmset" =>
              case "pipeline_hset" => //如果 overwrite==true，批量覆盖；
                if(!overwrite){ //如果overwrite!=true，批量获取已存在值，如果值存在且不含有要加载的值，则追加，如果值存在且含有要加载的值，跳过；如果值不存在，插入
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
                logger.error("Error: unsupported loadMethod = " + loadMethod)
            }
            numBatches += 1
            numInBatch = 0
          }
        } else {
          logger.debug("filtered record = " + line)
          numInBatch -= 1
        }

      }catch{
        case e:Exception => e.printStackTrace()
        case x: Throwable =>
          println("= = " * 20)
          logger.error("get unknown exception")
          println("= = " * 20)
      }
    }


    if(numInBatch > 0){ //pipeline_hset如果 0 < numInBatches < batchLimit，再执行一次
      try{
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
            logger.error("Error: unsupported loadMethod = " + loadMethod)
        }
        numBatches += 1
        numInBatch = 0
      }catch {
        case e:Exception => e.printStackTrace()
        case x: Throwable =>
          println("= = " * 20)
          logger.error("get unknown exception")
          println("= = " * 20)
      }
    }
    logger.info("finished batchSize = " + lines.length)


    //回收资源，释放jedis，但不释放 jedisPool
    jedisPool.returnResourceObject(jedis)

    taskResult.copy(numProcessed=numProcessed)
  }

  override def uncaughtException(thread: Thread, throwable: Throwable): Unit = {
    logger.error("thread "+thread+" got exception:")
    throwable.printStackTrace()
    throw throwable
  }
}

