/*
package com.asiainfo.ocdc.streaming.tool

import scala.util.Random
import redis.clients.jedis.{JedisPoolConfig, JedisPool, Jedis}
import actors._, Actor._

/**
 * Created by leo on 5/28/15.
**/

object CodisPerformenceTest {

  var list = Set[Actor]()

  for (i <- 0 to 99) {
    list = list.+(getActor())
  }

  def main(args: Array[String]) {
    val t1 = System.currentTimeMillis()
    list.foreach(act => {
      val x = act ! 1000
      act ! "bye"
    })
    println(" End time " + (System.currentTimeMillis()-t1))
  }

  def getActor() = {
    actor {
      val jedis: Jedis = CodisCacheManager.getResource
      var work = true
      while (work) {
        receive {
          case x: Int => {
            val t1 = System.currentTimeMillis()
//            println(" start time " + t1)
            for (i <- 0 to x) {
              val key = "Label:" + Random.nextInt(999999999)
              var value = "Value:" + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabcdsdjflsjfslfjslkjfsldfsienflnslkdnfn sdnfenfkennfnsldnfnsdnflsdknfsldknfskldfjsfeifnlsndlfknsndflkjslkfjsldjfsdjfsdlfnsvnsdnvsdvds"
              jedis.set(key, value + System.currentTimeMillis())
            }
            println(" exec 10w cost time " + (System.currentTimeMillis()-t1))
          }
          case y: String => work = false
        }
      }
    }
  }
}

object CodisCacheManager extends RedisCacheManager {

  private val jedisPool: JedisPool = {

    val JedisConfig = new JedisPoolConfig()
    JedisConfig.setMaxIdle(10000)
    JedisConfig.setMaxTotal(10000)
    JedisConfig.setMinEvictableIdleTimeMillis(600000)
    JedisConfig.setTestOnBorrow(true)

    new JedisPool(JedisConfig, "localhost", 19000, 60000)
  }

  override def getResource = jedisPool.getResource
}
*/
