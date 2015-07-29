package tools.redis.export

import org.slf4j.LoggerFactory
import tools.redis.FindMultiImsi2OneProduct._
import tools.redis.RedisUtils

import scala.collection.convert.wrapAsScala._

/**
 * Created by tsingfu on 15/7/25.
 */
object Hgetall extends App {

  val loggerName = this.getClass.getCanonicalName.replace("$", "")
  println("loggerName = " + loggerName)
  val logger = LoggerFactory.getLogger(loggerName)

  val confXmlFile = args(0)
  val hashKeyName = args(1)

  val props = init_props_fromXml(confXmlFile)
  val redis_serverPort = props.getProperty("redis.servers").split(",").head.split(":").map(_.trim)
  val redis_server = redis_serverPort(0)
  val redis_port = redis_serverPort(1).toInt
  val redis_timeout = props.getProperty("redis.timeout").trim.toInt
  val redis_passwd = props.getProperty("redis.password")
  val redis_password = if(redis_passwd == null || redis_passwd == "") null else redis_passwd
  val redis_database = props.getProperty("redis.database").trim.toInt
  val maxTotal = props.getProperty("jedisPool.maxTotal").toInt
  val maxIdle = props.getProperty("jedisPool.maxIdle").toInt
  val minIdle = props.getProperty("jedisPool.minIdle").toInt

  val jedisPool = RedisUtils.init_jedisPool(redis_server, redis_port, redis_timeout, redis_database, redis_password, maxTotal, maxIdle, minIdle)

  val jedis = jedisPool.getResource
  try{
    val map1 = jedis.hgetAll(hashKeyName)
//    val fieldNames = map1.to
//    val fieldNames2 = map1.keySet().toArray[String].sortWith(_ < _)
    val fieldNames = map1.map(_._1).toArray.sortWith(_ < _)

    println(fieldNames.mkString("\t"))
    println(fieldNames.map(f => {
      "-" * f.length
    }
    ).mkString("\t"))
    for( field <- fieldNames){
      print(map1.get(field)+"\t")
    }
    println()
  } catch {
//    case ex: Exception => ex.printStackTrace()
    case _ => println("unknown exception")
  } finally {
    jedis.close()
  }

}
