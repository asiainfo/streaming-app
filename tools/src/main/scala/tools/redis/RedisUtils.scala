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

}
