package tools.redis.load

import org.scalatest.{BeforeAndAfter, FunSuite}
import tools.redis.RedisUtils

/**
 * Created by tsingfu on 15/6/8.
 */
class Load2HashesThreadSuite extends FunSuite with BeforeAndAfter {

  test("1 测试100条记录，1个线程，每6条记录批量提交， hset, 覆盖加载"){

    val columnSeperator = ","

    //数据：如2,3,value1-test-2,value2-test-3
    val lines = for(i<- 0 until 10;j<-0 until 10) yield i+ columnSeperator + j + columnSeperator + "value1-test-"+i + columnSeperator +"value2-test-" + j
    println("- - " * 20)
//    lines.foreach(println(_))
    println(lines(23))
    println("- - " * 20)

    val hashNamePrefix = "load2hashes:"
    val hashIdxes = (0 to 1).toArray
    println("hashIdxes="+hashIdxes.mkString(","))
    val hashSeperator = ":"

    val fieldNames = Array("field1", "field2")
    val valueIdxes = (2 to 3).toArray

    val timeout = 10000
    val database = 0
    val password = null
    val maxTotal = 100
    val maxIdle = 10
    val minIdle = 0
    val servers = "codis1:29001, codis1:29002"
    // 初始化 jedisPool, jedis, pipeline
    val jedisPools = servers.split(",").map(server=>{
      val hostPort = server.split(":").map(_.trim)
      val host = hostPort(0)
      val port = hostPort(1).toInt
      println("host = " + host +", port ="+port)
      RedisUtils.init_jedisPool(host, port, timeout, database, password, maxTotal, maxIdle, minIdle)
    })
    val numPools = jedisPools.length
    val jedises = jedisPools.map(_.getResource)
    val pipelines = jedises.map(_.pipelined)

//    val loadMethod = "pipeline_hset"
    val loadMethod = "hset"
    val batchLimitForRedis = 6
    val overwrite = true
    val appendSeperator = ","

    val worker1 = new Load2HashesThread(lines.toArray, columnSeperator,
      hashNamePrefix, hashIdxes, hashSeperator,
      fieldNames, valueIdxes,
      jedisPools(0),loadMethod, batchLimitForRedis, overwrite, appendSeperator)

    val thread1 = new Thread(worker1)
    thread1.join()
    thread1.start()

    Thread.sleep(1 * 1000)
    //检查结果1
    val rs1 = jedises(0).hgetAll(hashNamePrefix + "2:3")
    println("rs1 = "+rs1)

    assert(rs1.get("field1").toString=="value1-test-2")
    assert(rs1.get("field2").toString=="value2-test-3")
    assert(rs1.size() == 2)
  }


  test("2 测试100条记录，1个线程，每6条记录批量提交， hmset, 覆盖加载"){

    val columnSeperator = ","

    val lines = for(i<- 0 until 10;j<-0 until 10) yield i+ columnSeperator + j + columnSeperator + "value1-test-"+i + columnSeperator +"value2-test-" + j
    println("- - " * 20)
    //    lines.foreach(println(_))
    println(lines(23))
    println("- - " * 20)

    val hashNamePrefix = "load2hashes:"
    val hashIdxes = (0 to 1).toArray
    println("hashIdxes="+hashIdxes.mkString(","))
    val hashSeperator = ":"

    val fieldNames = Array("field1", "field2")
    val valueIdxes = (2 to 3).toArray

    val timeout = 10000
    val database = 0
    val password = null
    val maxTotal = 100
    val maxIdle = 10
    val minIdle = 0
    val servers = "codis1:29001, codis1:29002"
    // 初始化 jedisPool, jedis, pipeline
    val jedisPools = servers.split(",").map(server=>{
      val hostPort = server.split(":").map(_.trim)
      val host = hostPort(0)
      val port = hostPort(1).toInt
      println("host = " + host +", port ="+port)
      RedisUtils.init_jedisPool(host, port, timeout, database, password, maxTotal, maxIdle, minIdle)
    })
    val numPools = jedisPools.length
    val jedises = jedisPools.map(_.getResource)
    val pipelines = jedises.map(_.pipelined)

    //    val loadMethod = "pipeline_hset"
    val loadMethod = "hmset"
    val batchLimitForRedis = 6
    val overwrite = true
    val appendSeperator = ","

    val worker1 = new Load2HashesThread(lines.toArray, columnSeperator,
      hashNamePrefix, hashIdxes, hashSeperator,
      fieldNames, valueIdxes,
      jedisPools(0),loadMethod, batchLimitForRedis, overwrite, appendSeperator)

    val thread1 = new Thread(worker1)
    thread1.join()
    thread1.start()

    Thread.sleep(1 * 1000)
    //检查结果1
    val rs1 = jedises(0).hgetAll(hashNamePrefix + "2:3")
    println("rs1 = "+rs1)

    assert(rs1.get("field1").toString=="value1-test-2")
    assert(rs1.get("field2").toString=="value2-test-3")
    assert(rs1.size() == 2)
  }


  test("3 测试100条记录，1个线程，每6条记录批量提交， pipeline_hset, 覆盖加载"){

    val columnSeperator = ","

    val lines = for(i<- 0 until 10;j<-0 until 10) yield i+ columnSeperator + j + columnSeperator + "value1-test-"+i + columnSeperator +"value2-test-" + j
    println("- - " * 20)
    //    lines.foreach(println(_))
    println(lines(23))
    println("- - " * 20)

    val hashNamePrefix = "load2hashes:"
    val hashIdxes = (0 to 1).toArray
    println("hashIdxes="+hashIdxes.mkString(","))
    val hashSeperator = ":"

    val fieldNames = Array("field1", "field2")
    val valueIdxes = (2 to 3).toArray

    val timeout = 10000
    val database = 0
    val password = null
    val maxTotal = 100
    val maxIdle = 10
    val minIdle = 0
    val servers = "codis1:29001, codis1:29002"
    // 初始化 jedisPool, jedis, pipeline
    val jedisPools = servers.split(",").map(server=>{
      val hostPort = server.split(":").map(_.trim)
      val host = hostPort(0)
      val port = hostPort(1).toInt
      println("host = " + host +", port ="+port)
      RedisUtils.init_jedisPool(host, port, timeout, database, password, maxTotal, maxIdle, minIdle)
    })
    val numPools = jedisPools.length
    val jedises = jedisPools.map(_.getResource)
    val pipelines = jedises.map(_.pipelined)

    val loadMethod = "pipeline_hset"
    val batchLimitForRedis = 6
    val overwrite = true
    val appendSeperator = ","

    val worker1 = new Load2HashesThread(lines.toArray, columnSeperator,
      hashNamePrefix, hashIdxes, hashSeperator,
      fieldNames, valueIdxes,
      jedisPools(0),loadMethod, batchLimitForRedis, overwrite, appendSeperator)

    val thread1 = new Thread(worker1)
    thread1.join()
    thread1.start()

    //注意：没有Thread.sleep，测试会报错
    Thread.sleep(1 * 1000)

    //检查结果1
    val rs1 = jedises(0).hgetAll(hashNamePrefix + "2:3")
    println("rs1 = "+rs1)

    assert(rs1.get("field1").toString=="value1-test-2")
    assert(rs1.get("field2").toString=="value2-test-3")
    assert(rs1.size() == 2)
  }

  test("4 测试100条记录，1个线程，每6条记录批量提交， hset, 追加加载"){

    val columnSeperator = ","

    //数据：如2,3,value1-test-2,value2-test-3
    val lines = for(i<- 0 until 10;j<-0 until 10) yield i+ columnSeperator + j + columnSeperator + "value1-test-"+i + columnSeperator +"value2-test-" + j
    println("- - " * 20)
    //    lines.foreach(println(_))
    println(lines(23))
    println("- - " * 20)

    //数据 2,3,value1-test2-2,value2-test2-3
    val lines2 = for(i<- 0 until 10;j<-0 until 10) yield i+ columnSeperator + j + columnSeperator + "value1-test2-"+i + columnSeperator +"value2-test2-" + j
    println("- - " * 20)
    //    lines2.foreach(println(_))
    println(lines2(23))
    println("- - " * 20)

    val hashNamePrefix = "load2hashes:"
    val hashIdxes = (0 to 1).toArray
    println("hashIdxes="+hashIdxes.mkString(","))
    val hashSeperator = ":"

    val fieldNames = Array("field1", "field2")
    val valueIdxes = (2 to 3).toArray

    val timeout = 10000
    val database = 0
    val password = null
    val maxTotal = 100
    val maxIdle = 10
    val minIdle = 0
    val servers = "codis1:29001, codis1:29002"
    // 初始化 jedisPool, jedis, pipeline
    val jedisPools = servers.split(",").map(server=>{
      val hostPort = server.split(":").map(_.trim)
      val host = hostPort(0)
      val port = hostPort(1).toInt
      println("host = " + host +", port ="+port)
      RedisUtils.init_jedisPool(host, port, timeout, database, password, maxTotal, maxIdle, minIdle)
    })
    val numPools = jedisPools.length
    val jedises = jedisPools.map(_.getResource)
    val pipelines = jedises.map(_.pipelined)

    //    val loadMethod = "pipeline_hset"
    val loadMethod = "hset"
    val batchLimitForRedis = 6
    val overwrite = false
    val appendSeperator = ","

    val worker1 = new Load2HashesThread(lines.toArray, columnSeperator,
      hashNamePrefix, hashIdxes, hashSeperator,
      fieldNames, valueIdxes,
      jedisPools(0),loadMethod, batchLimitForRedis, overwrite, appendSeperator)

    val thread1 = new Thread(worker1)
    thread1.join()
    thread1.start()

    val worker2 = new Load2HashesThread(lines2.toArray, columnSeperator,
      hashNamePrefix, hashIdxes, hashSeperator,
      fieldNames, valueIdxes,
      jedisPools(0),loadMethod, batchLimitForRedis, overwrite, appendSeperator)

    val thread2 = new Thread(worker2)
    thread2.join()
    thread2.start()

    Thread.sleep(1 * 1000)
    //检查结果1
    val rs1 = jedises(0).hgetAll(hashNamePrefix + "2:3")
    println("rs1 = "+rs1)

    assert(rs1.get("field1").toString=="value1-test-2,value1-test2-2")
    assert(rs1.get("field2").toString=="value2-test-3,value1-test2-3")
    assert(rs1.size() == 2)
  }
}
