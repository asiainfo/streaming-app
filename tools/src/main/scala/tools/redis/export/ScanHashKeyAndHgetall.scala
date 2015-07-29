package tools.redis.export

import java.util.Properties

import org.slf4j.LoggerFactory
import tools.redis.RedisUtils

import scala.xml.XML

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

  /**
   * 从xml文件中初始化配置
   * @param confXmlFile
   * @return
   */
  def init_props_fromXml(confXmlFile: String): Properties ={

    val props = new Properties()

    val conf = XML.load(confXmlFile)

    val servers = (conf \ "redis" \ "servers").text.trim
    val database = (conf \ "redis" \ "database").text.trim
    val timeout = (conf \ "redis" \ "timeout").text.trim
    val passwd = (conf \ "redis" \ "password").text.trim
    val password = if (passwd == "" || passwd == null) null else passwd

    props.put("redis.servers", servers)
    props.put("redis.database", database)
    props.put("redis.timeout", timeout)

    if(password != null || password == "") props.put("redis.password", password)

    val maxTotal = (conf \ "jedisPool" \ "maxTotal").text.trim
    val maxIdle = (conf \ "jedisPool" \ "maxIdle").text.trim
    val minIdle = (conf \ "jedisPool" \ "minIdle").text.trim

    props.put("jedisPool.maxTotal", maxTotal)
    props.put("jedisPool.maxIdle", maxIdle)
    props.put("jedisPool.minIdle", minIdle)


    println("="*80)
    //TODO: 解决properties配置项输出乱序问题
    props.list(System.out)//问题，没有顺序
    println("="*80)

    props
  }
}
