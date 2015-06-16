package tools.redis.load.prototype

import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisPool

import scala.collection.convert.wrapAsJava._
import scala.collection.convert.wrapAsScala._
import scala.collection.mutable.ArrayBuffer


/**
 * Created by tsingfu on 15/6/7.
 */
class Load2HashesThread(lines: Array[String], columnSeperator: String,
                        hashNamePrefix: String, hashIdxes: Array[Int], hashSeperator: String,
                        fieldNames: Array[String], valueIdxes: Array[Int],
                        jedisPool: JedisPool,
                        loadMethod:String, batchLimit:Int,
                        overwrite: Boolean, appendSeperator: String) extends Runnable with Thread.UncaughtExceptionHandler {

  val logger = LoggerFactory.getLogger(this.getClass)

  override def run(): Unit = {

    var jedis = jedisPool.getResource
    var pipeline = jedis.pipelined()

    var numProcessed = 0
    var numInBatch = 0
    var numBatches = 0

    val fvMap = scala.collection.mutable.Map[String, String]()
    val fieldArray = ArrayBuffer[String]()
    val valueArray = ArrayBuffer[String]()
    val hashNameArray = ArrayBuffer[String]()

    logger.info("batchSize = " + lines.length)
    for(line <- lines){
      logger.debug("numProcessed = "+numProcessed+",numBatches = "+ numBatches +", numInBatch = "+ numInBatch +", line => " + line.split(columnSeperator).mkString("[",",","]"))
      try{
        val lineArray = line.split(columnSeperator).map(_.trim)
        numInBatch += 1
        val hashName = hashNamePrefix + (for (idx <- hashIdxes) yield lineArray(idx)).mkString(hashSeperator)

        fieldNames.zip(valueIdxes).foreach(kv => {
          val (k, i) = kv
          loadMethod match {
            case "hset" =>
              if(overwrite) {//如果 overwrite == true， 直接插入
                logger.debug("[debug2] hset(" + hashName +", "+k +","+lineArray(i)+")")

                jedis.hset(hashName, k, lineArray(i))
              } else {//如果 overwrite != true， 先查询，检查是否含有要append的值，没有append，有则不做操作
              val value_exist = jedis.hget(hashName, k)
                if(value_exist == null){
                  logger.debug("[debug2] hset(" + hashName +", "+k +","+lineArray(i)+")")
                  jedis.hset(hashName, k, lineArray(i))
                } else {
                  if(!value_exist.split(appendSeperator).contains(lineArray(i))){
                    logger.debug("[debug2] hset(" + hashName +", "+k +","+(value_exist + appendSeperator + lineArray(i))+")")
                    jedis.hset(hashName, k, value_exist + appendSeperator + lineArray(i))
                  }
                }
              }

            case "hmset" =>
              fvMap.put(k, lineArray(i)) // 新增hmset 的值
              if(!overwrite){ //如果 overwrite!=true, 保存hash的field, value
                fieldArray.append(k)
                valueArray.append(lineArray(i))
              }
            case "pipeline_hset" =>
              if(overwrite){ //如果 overwrite==true, 新增pipeline.hset的值，
                pipeline.hset(hashName, k, lineArray(i))

              } else {  //如果overwrite!=true, 保存hash的field, value
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
        numProcessed += 1
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
  }

  override def uncaughtException(thread: Thread, throwable: Throwable): Unit = {
    logger.error("thread "+thread+" got exception:")
    throwable.printStackTrace()
    throw throwable
  }
}
