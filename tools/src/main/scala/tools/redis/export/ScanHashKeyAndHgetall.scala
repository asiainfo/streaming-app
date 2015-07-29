package tools.redis.export

import org.slf4j.LoggerFactory
import tools.redis.FindMultiImsi2OneProduct.init_props_fromXml
import tools.redis.RedisUtils

/**
 * Created by tsingfu on 15/7/25.
 */
object ScanHashKeyAndHgetall extends App {

  val loggerName = this.getClass.getCanonicalName.replace("$", "")
  println("loggerName = " + loggerName)
  val logger = LoggerFactory.getLogger(loggerName)

  val confXmlFile = args(0)
  val pattern_prefix = if (args(1).toLowerCase == "null") null else args(1)
  val pattern_mid = if (args(2).toLowerCase == "null") null else args(2)
  val pattern_suffix = if (args(3).toLowerCase == "null") null else args(3)
  val scanBatchLimit = if(args.length==5) args(4).toInt else 10

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
    RedisUtils.scanHashKeyAndHgetall(jedis, pattern_prefix, pattern_mid, pattern_suffix, scanBatchLimit)
  } catch {
    case ex: Exception => ex.printStackTrace()
    case _ => println("Unknown Exception.")
  } finally {
    jedis.close()
  }

}
