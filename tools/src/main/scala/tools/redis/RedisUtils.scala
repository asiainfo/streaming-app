package tools.redis

import java.util.ArrayList

import org.slf4j.LoggerFactory
import redis.clients.jedis.{JedisPool, Jedis, JedisPoolConfig}

import scala.collection.mutable

/**
 * Created by tsingfu on 15/4/27.
 */
object RedisUtils {

  val logger = LoggerFactory.getLogger("tools.redis.RedisUtils")

  /**
   * 初始化 jedisPoolConfig
   * @param maxIdel
   * @param maxTotal
   * @param minIdel
   * @return
   */
  def init_jedisPoolConfig(maxIdel: Int, maxTotal: Int, minIdel: Int): JedisPoolConfig ={

    val jedisPoolConf = new JedisPoolConfig()

    //最大空闲连接数, 默认8个
    //    jedisPoolConf.setMaxIdle(8)
    jedisPoolConf.setMaxIdle(maxIdel)

    //最大连接数, 默认8个
    //    jedisPoolConf.setMaxTotal(8)
    jedisPoolConf.setMaxTotal(maxTotal)

    //最小空闲连接数, 默认0
    //    jedisPoolConf.setMinIdle(0)
    jedisPoolConf.setMinIdle(minIdel)

    jedisPoolConf
  }

  val jedisPoolMap = mutable.HashMap[String, JedisPool]()

  def get_jedisPool(jedisPoolConfig: JedisPoolConfig, host:String, port: Int,
                    timeout:Int = 2000, password: String = null, dbNum: Int = 0): JedisPool  ={
    val jedisId = host +":" + port +"/" + dbNum +":" +password +":" +timeout
    if(jedisPoolMap.contains(jedisId)){
      jedisPoolMap.get(jedisId).get
    } else {
      val jedisPool = new JedisPool(jedisPoolConfig, host, port, timeout, password, dbNum)
      jedisPoolMap.put(jedisId, jedisPool)
      jedisPool
    }
  }

  def init_jedisPool(host: String,
                     port: Int,
                     timeout: Int,
                     dbnum: Int = 0,
                     password: String = null,
                     maxTotal: Int = 8,
                     maxIdle: Int = 8,
                     minIdle: Int = 0,
                     blockWhenExhausted: Boolean = true,
                     maxWaitMillis: Long = -1,
                     testOnBorrow: Boolean = false,
                     testWhileIdle: Boolean = false,
                     evictionPolicyClassName: String = "org.apache.commons.pool2.impl.DefaultEvictionPolicy",
                     softMinEvictableIdleTimeMillis: Long = 1800000,
                     timeBetweenEvictionRunsMillis: Long = -1,
                     minEvictableIdleTimeMillis: Long = 1800000,
                     numTestsPerEvictionRun: Int = 3): JedisPool = {

    val jedisPoolConf = init_jedisPoolConfig(maxTotal, maxIdle, minIdle, blockWhenExhausted, maxWaitMillis,
      testOnBorrow, testWhileIdle,
      evictionPolicyClassName, softMinEvictableIdleTimeMillis, timeBetweenEvictionRunsMillis, minEvictableIdleTimeMillis, numTestsPerEvictionRun)

    val jedisPool = new JedisPool(jedisPoolConf, host, port, timeout, password, dbnum)
    jedisPool
  }

  /**
   * 初始化 jedisPoolConfig
   * @param maxTotal 最大连接数, 默认8个
   * @param maxIdle 最大空闲连接数, 默认8个
   * @param minIdle 最小空闲连接数, 默认0
   * @param blockWhenExhausted 连接耗尽时是否阻塞, false报异常,ture阻塞直到超时, 默认true
   * @param maxWaitMillis 获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted),如果超时就抛异常, 小于零:阻塞不确定的时间,  默认-1
   * @param testOnBorrow 在获取连接的时候是否检查有效性, 默认false
   * @param testWhileIdle 在空闲时是否检查有效性, 默认false
   * @param evictionPolicyClassName 设置的逐出策略类名, 默认DefaultEvictionPolicy(当连接超过最大空闲时间,或连接数超过最大空闲连接数)
   * @param softMinEvictableIdleTimeMillis 对象空闲多久后逐出, 当空闲时间>该值 且 空闲连接>最大空闲数 时直接逐出,不再根据MinEvictableIdleTimeMillis判断  (默认逐出策略)
   * @param timeBetweenEvictionRunsMillis 逐出扫描的时间间隔(毫秒) 如果为负数,则不运行逐出线程, 默认-1
   * @param minEvictableIdleTimeMillis 逐出连接的最小空闲时间 默认1800000毫秒(30分钟)
   * @param numTestsPerEvictionRun 每次逐出检查时 逐出的最大数目 如果为负数就是 : 1/abs(n), 默认3
   * @return
   */
  def init_jedisPoolConfig(maxTotal: Int = 8,
                           maxIdle: Int = 8,
                           minIdle: Int = 0,
                           blockWhenExhausted: Boolean = true,
                           maxWaitMillis: Long = -1,
                           testOnBorrow: Boolean = false,
                           testWhileIdle: Boolean = false,
                           evictionPolicyClassName: String = "org.apache.commons.pool2.impl.DefaultEvictionPolicy",
                           softMinEvictableIdleTimeMillis: Long = 1800000,
                           timeBetweenEvictionRunsMillis: Long = -1,
                           minEvictableIdleTimeMillis: Long = 1800000,
                           numTestsPerEvictionRun: Int = 3
                                  ): JedisPoolConfig = {

    val jedisPoolConf = new JedisPoolConfig()

    //最大连接数, 默认8个
    //    jedisPoolConf.setMaxTotal(8)
    jedisPoolConf.setMaxTotal(maxTotal)

    //最大空闲连接数, 默认8个
    //    jedisPoolConf.setMaxIdle(8)
    jedisPoolConf.setMaxIdle(maxIdle)

    //最小空闲连接数, 默认0
    //    jedisPoolConf.setMinIdle(0)
    jedisPoolConf.setMinIdle(minIdle)

    //连接耗尽时是否阻塞, false报异常,ture阻塞直到超时, 默认true
    //    jedisPoolConf.setBlockWhenExhausted(true)
    jedisPoolConf.setBlockWhenExhausted(blockWhenExhausted)

    //获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted),如果超时就抛异常, 小于零:阻塞不确定的时间,  默认-1
    //    jedisPoolConf.setMaxWaitMillis(-1)
    jedisPoolConf.setMaxWaitMillis(maxWaitMillis)


    //在获取连接的时候是否检查有效性, 默认false
    //    jedisPoolConf.setTestOnBorrow(false)
    jedisPoolConf.setTestOnBorrow(testOnBorrow)

    //在空闲时是否检查有效性, 默认false
    //    jedisPoolConf.setTestWhileIdle(false)
    jedisPoolConf.setTestWhileIdle(testWhileIdle)



    //设置的逐出策略类名, 默认DefaultEvictionPolicy(当连接超过最大空闲时间,或连接数超过最大空闲连接数)
    //    jedisPoolConf.setEvictionPolicyClassName("org.apache.commons.pool2.impl.DefaultEvictionPolicy")
    jedisPoolConf.setEvictionPolicyClassName(evictionPolicyClassName)

    //对象空闲多久后逐出, 当空闲时间>该值 且 空闲连接>最大空闲数 时直接逐出,不再根据MinEvictableIdleTimeMillis判断  (默认逐出策略)
    //    jedisPoolConf.setSoftMinEvictableIdleTimeMillis(1800000)
    jedisPoolConf.setSoftMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis)

    //逐出扫描的时间间隔(毫秒) 如果为负数,则不运行逐出线程, 默认-1
    //    jedisPoolConf.setTimeBetweenEvictionRunsMillis(-1)
    jedisPoolConf.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis)

    //逐出连接的最小空闲时间 默认1800000毫秒(30分钟)
    //    jedisPoolConf.setMinEvictableIdleTimeMillis(1800000)
    jedisPoolConf.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis)


    //每次逐出检查时 逐出的最大数目 如果为负数就是 : 1/abs(n), 默认3
    //    jedisPoolConf.setNumTestsPerEvictionRun(3)
    jedisPoolConf.setNumTestsPerEvictionRun(numTestsPerEvictionRun)




    //是否启用pool的jmx管理功能, 默认true
    jedisPoolConf.setJmxEnabled(true)

    //MBean ObjectName = new ObjectName("org.apache.commons.pool2:type=GenericObjectPool,name=" + "pool" + i); 默 认为"pool", JMX不熟,具体不知道是干啥的...默认就好.
    jedisPoolConf.setJmxNamePrefix("pool")

    //是否启用后进先出, 默认true
    jedisPoolConf.setLifo(true)



    jedisPoolConf
  }


  /**
   * For String
   * @param jedis
   * @param hashName
   * @param mapKVs
   */
  def mset(jedis: Jedis, hashName:String, mapKVs: Map[String, String]): Unit ={
    val arrayList = new ArrayList[Array[Byte]]()

    import scala.collection.convert.wrapAsScala._

    mapKVs.map(kv =>{
      val (k, v) = kv
      arrayList.add(k.getBytes)
      arrayList.add(v.getBytes)
    })
    jedis.mset(arrayList.toIndexedSeq: _*)
  }

  /**
   * For hash get
   * @param serverPort
   * @param hashName
   * @param key
   * @return
   */
  def hget(serverPort: String, dbNum: Int = 0, hashName: String, key:String): String ={
    val serverPortArray = serverPort.split(":")
    val host = serverPortArray(0)
    val port = serverPortArray(1).toInt

    val jedis = new Jedis(host, port)
    jedis.select(dbNum)

    jedis.hget(hashName, key)
  }


  def hset(jedis: Jedis, hashName:String, mapKVs: Map[String, String]): Unit ={
    mapKVs.map(kv =>{
      val (k, v) = kv
      jedis.hset(hashName, k, v)
    })
  }


  /**
   * 10进制字符串转换为16进制字符串
   * @param decimalStr
   * @return
   */
  def convertDecimaltoHex(decimalStr: String):String ={
    var hexStr = Integer.toHexString(decimalStr.toInt).toUpperCase
    while(hexStr.length<4){hexStr = "0" + hexStr}
    hexStr
  }
}
