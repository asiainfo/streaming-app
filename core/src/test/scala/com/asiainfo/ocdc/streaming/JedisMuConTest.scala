/*

package com.asiainfo.ocdc.streaming

import redis.clients.jedis.Jedis

/**
 * Created by leo on 4/25/15.
 */
object JedisMuConTest {

  private val currentJedis = new ThreadLocal[Jedis] {
    override def initialValue = new Jedis("localhost", 6379)
  }

  //  final def getJedis = currentJedis.get()

  val jedis = new Jedis("localhost", 6379)

  final def getJedis = jedis

  val a1 = actor {
    val jedis: Jedis = getJedis
    var work = true
    while (work) {
      receive {
        case x: Int => {
          println(jedis)
          var i = 0
          while (i < 100000000) {
            println(jedis.get("a"))
            println(jedis.hget("ha", "aa"))
            println(jedis.get("b"))
            println(jedis.get("kkkkk"))
            println(jedis.get("kkk"))
            val k1 = ("ddd").getBytes()
            val v1 = ("zzzzz" + i).getBytes()
            val k2 = ("ddd11").getBytes()
            val v2 = ("zzzzz" + i).getBytes()
            jedis.mset(k1, v1, k2, v2)
            jedis.mset(k2, v2, k1, v1)
            println(jedis.hget("ha", "aa"))
            println(jedis.get("kkk"))
            println(jedis.get("kkkkk"))
            println(jedis.mget("ddd11".getBytes(), "ddd".getBytes()))
            println(jedis.mget("ddd".getBytes(), "ddd11".getBytes()))
            println(jedis.get("ddd11"))
            println("a1")
            i = i + 1
          }
        }
        case y: String => work = false
      }
    }
  }

  val a2 = actor {
    val jedis: Jedis = getJedis
    var work = true
    while (work) {
      receive {
        case x: Int => {
          println(jedis)
          var i = 0
          while (i < 100000000) {
            println(jedis.get("a"))
            println(jedis.hget("ha", "aa"))
            println(jedis.get("b"))
            println(jedis.get("kkkkk"))
            println(jedis.get("kkk"))
            val k1 = ("ddd").getBytes()
            val v1 = ("zzzzz" + i).getBytes()
            val k2 = ("ddd11").getBytes()
            val v2 = ("zzzzz" + i).getBytes()
            jedis.mset(k2, v2, k1, v1)
            jedis.mset(k1, v1, k2, v2)
            println(jedis.hget("ha", "aa"))
            println(jedis.get("kkk"))
            println(jedis.get("kkkkk"))
            println(jedis.mget("ddd11".getBytes(), "ddd".getBytes()))
            println(jedis.get("ddd"))
            println(jedis.get("ddd11"))
            println("a2")
            i = i + 1
          }
        }
        case y: String => work = false
      }
    }
  }


  def main(args: Array[String]) {
    a1 ! 1
    a2 ! 1
    /*for (i <- 0 to 1000000) {

    }*/
  }


}

*/
