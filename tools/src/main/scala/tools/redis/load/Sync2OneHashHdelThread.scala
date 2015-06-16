package tools.redis.load

import java.util.concurrent.Callable

import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisPool

/**
 * Created by tsingfu on 15/6/15.
 */
class Sync2OneHashHdelThread(lines: Array[String], columnSeperator: String,
                            hashName: String,
                            fieldIdxes: Array[Int], fieldSeperator: String,
                            jedisPool: JedisPool,
                            loadMethod:String, batchLimit:Int,
                            taskResult: FutureTaskResult) extends Callable[FutureTaskResult] with Thread.UncaughtExceptionHandler {

  val logger = LoggerFactory.getLogger(this.getClass)

  override def call(): FutureTaskResult = {
    val jedis = jedisPool.getResource
    val pipeline = jedis.pipelined()

    var numProcessed = 0
    var numInBatch = 0
    var numBatches = 0

    logger.info("batchSize = " + lines.length)
    for(line <- lines){
      numProcessed += 1
      logger.debug("numProcessed = "+numProcessed+", numBatches = "+ numBatches +", numInBatch = "+ numInBatch +", line => " + line.split(columnSeperator).mkString("[",",","]"))

      try{
        val lineArray = line.split(columnSeperator).map(_.trim)
        numInBatch += 1

        val fieldName = (for(fieldIdx<-fieldIdxes) yield lineArray(fieldIdx)).mkString(fieldSeperator)

          loadMethod match {
            case "hset" =>
              logger.debug("hdel(" + hashName +", " + fieldName + ")")
              jedis.hdel(hashName, fieldName)
            case "hmset" =>
              jedis.hdel(hashName, fieldName)
            case "pipeline_hset" =>
              pipeline.hdel(hashName, fieldName)
            case _ =>
              logger.error("Error: unsupported loadMethod = " + loadMethod)
          }

        if(numInBatch == batchLimit){ // numInbatch对pipeline_hset有效，hset每个set一次提交；hmset每行一次提交
          loadMethod match {
            case "hset" =>
            case "hmset" =>
            case "pipeline_hset" => //如果 overwrite==true，批量覆盖；
              pipeline.sync()
            case _ =>
              logger.error("Error: unsupported loadMethod = " + loadMethod)
          }
          numBatches += 1
          numInBatch = 0
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
            pipeline.sync()
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