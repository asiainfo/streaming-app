package tools.redis.loadAreaMap

import org.scalatest.{BeforeAndAfter, FunSuite}
import tools.redis.RedisUtils

/**
 * Created by tsingfu on 15/6/8.
 */
class Load2SingleHashThreadSuite extends FunSuite with BeforeAndAfter {


  test("1 测试100条记录，1个线程，每6条记录批量提交， hset，覆盖加载, 不做区域映射"){

    val columnSeperator = ","

    //数据 102,103,area-5,1
    val lines = for(i<- 0 until 10;j<-0 until 10) yield (100+i)+ columnSeperator + (100+j) + columnSeperator + "area-"+((i+j)%10 +
            columnSeperator+(i+j)%2)
    println("- - " * 20)
        lines.foreach(println(_))
//    println(lines(23))
    println("- - " * 20)

    val hashName = "areaMap1"
    val fieldIdxes = (0 to 1).toArray
    val fieldSeperator = ":"
    println("fieldIdxes="+fieldIdxes.mkString(","))
    val valueIdx = 2
    val conversion10to16Idxes = fieldIdxes
    val valueMapEnabled = false
    val valueMap = "WLAN"

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

    val loadMethod = "hset"
    val batchLimitForRedis = 6
    val overwrite = true
    val appendSeperator = ","


    val worker1 = new Load2SingleHashThread(lines.toArray,columnSeperator,
      hashName,
      fieldIdxes, fieldSeperator, conversion10to16Idxes,
      valueIdx, valueMapEnabled, valueMap,
      jedisPools(0),loadMethod, batchLimitForRedis, overwrite, appendSeperator)

    val thread1 = new Thread(worker1)
    thread1.join()
    thread1.start()

    Thread.sleep(1 * 1000)
    //检查结果1
    val rs1 = jedises(0).hgetAll(hashName)
    println("rs1 = "+rs1)

//    assert(rs1.get("2:3").toString=="area-5")
    assert(rs1.get("0066:0067").toString=="area-5")
//    assert(rs1.get("8:9").toString=="area-7")
    assert(rs1.get("006C:006D").toString=="area-7")
    assert(rs1.size() == 100)
  }

  test("2 测试100条记录，1个线程，每6条记录批量提交， hmset，覆盖加载, 不做区域映射"){

    val columnSeperator = ","

    //数据 102,103,area-5,1
    val lines = for(i<- 0 until 10;j<-0 until 10) yield (100+i)+ columnSeperator + (100+j) + columnSeperator + "area-"+((i+j)%10 +
            columnSeperator+(i+j)%2)
    println("- - " * 20)
    //    lines.foreach(println(_))
    println(lines(23))
    println("- - " * 20)

    val hashName = "areaMap1"
    val fieldIdxes = (0 to 1).toArray
    val fieldSeperator = ":"
    println("fieldIdxes="+fieldIdxes.mkString(","))
    val valueIdx = 2
    val conversion10to16Idxes = fieldIdxes
    val valueMapEnabled = false
    val valueMap = "WLAN"

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

    val loadMethod = "hmset"
    val batchLimitForRedis = 6
    val overwrite = true
    val appendSeperator = ","


    val worker1 = new Load2SingleHashThread(lines.toArray,columnSeperator,
      hashName,
      fieldIdxes, fieldSeperator, conversion10to16Idxes,
      valueIdx, valueMapEnabled,valueMap,
      jedisPools(0),loadMethod, batchLimitForRedis, overwrite, appendSeperator)

    val thread1 = new Thread(worker1)
    thread1.join()
    thread1.start()

    Thread.sleep(1 * 1000)
    //检查结果1
    val rs1 = jedises(0).hgetAll(hashName)
    println("rs1 = "+rs1)

    //    assert(rs1.get("2:3").toString=="area-5")
    assert(rs1.get("0066:0067").toString=="area-5")
    //    assert(rs1.get("8:9").toString=="area-7")
    assert(rs1.get("006C:006D").toString=="area-7")
    assert(rs1.size() == 100)
  }


  test("3 测试100条记录，1个线程，每6条记录批量提交， pipeline_hset，覆盖加载, 不做区域映射"){

    val columnSeperator = ","

    //数据 102,103,area-5,1
    val lines = for(i<- 0 until 10;j<-0 until 10) yield (100+i)+ columnSeperator + (100+j) + columnSeperator + "area-"+((i+j)%10 +
            columnSeperator+(i+j)%2)
    println("- - " * 20)
    //    lines.foreach(println(_))
    println(lines(23))
    println("- - " * 20)

    val hashName = "areaMap1"
    val fieldIdxes = (0 to 1).toArray
    val fieldSeperator = ":"
    println("fieldIdxes="+fieldIdxes.mkString(","))
    val valueIdx = 2
    val conversion10to16Idxes = fieldIdxes
    val valueMapEnabled = false
    val valueMap = "WLAN"

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


    val worker1 = new Load2SingleHashThread(lines.toArray,columnSeperator,
      hashName,
      fieldIdxes, fieldSeperator, conversion10to16Idxes,
      valueIdx, valueMapEnabled, valueMap,
      jedisPools(0),loadMethod, batchLimitForRedis, overwrite, appendSeperator)

    val thread1 = new Thread(worker1)
    thread1.join()
    thread1.start()

    Thread.sleep(1 * 1000)
    //检查结果1
    val rs1 = jedises(0).hgetAll(hashName)
    println("rs1 = "+rs1)

    //    assert(rs1.get("2:3").toString=="area-5")
    assert(rs1.get("0066:0067").toString=="area-5")
    //    assert(rs1.get("8:9").toString=="area-7")
    assert(rs1.get("006C:006D").toString=="area-7")
    assert(rs1.size() == 100)
  }

  test("4 测试100条记录，1个线程，每6条记录批量提交， pipeline_hset，覆盖加载, 做区域映射"){

    val columnSeperator = ","

    //数据 102,103,area-5,1
    val lines = for(i<- 0 until 10;j<-0 until 10) yield (100+i)+ columnSeperator + (100+j) + columnSeperator + "area-"+((i+j)%10 +
            columnSeperator+(i+j)%2)
    println("- - " * 20)
        lines.foreach(println(_))
//    println(lines(23))
    println("- - " * 20)

    val hashName = "areaMap1"
    val fieldIdxes = (0 to 1).toArray
    val fieldSeperator = ":"
    println("fieldIdxes="+fieldIdxes.mkString(","))
    val valueIdx = 3
    val conversion10to16Idxes = fieldIdxes
    val valueMapEnabled = true
    val valueMap = "WLAN"

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


    val worker1 = new Load2SingleHashThread(lines.toArray,columnSeperator,
      hashName,
      fieldIdxes, fieldSeperator, conversion10to16Idxes,
      valueIdx, valueMapEnabled, valueMap,
      jedisPools(0),loadMethod, batchLimitForRedis, overwrite, appendSeperator)

    val thread1 = new Thread(worker1)
    thread1.join()
    thread1.start()


    Thread.sleep(3 * 1000)
    //检查结果1
    val rs1 = jedises(0).hgetAll(hashName)
    println("rs1 = "+rs1)

    //    assert(rs1.get("2:3").toString=="area-5")
    assert(rs1.get("0066:0067").toString=="WLAN")
    //    assert(rs1.get("8:9").toString=="area-7")
    assert(rs1.get("006C:006D").toString=="WLAN")
    assert(rs1.get("006C:006C") == null)

    assert(rs1.size() == 50)
  }


  test("5 测试100条记录，1个线程，每6条记录批量提交， pipeline_hset，覆盖加载, 做区域映射, 测试中文区域"){

    val columnSeperator = ","

    //数据 102,103,area-5,1
    val lines = for(i<- 0 until 10;j<-0 until 10) yield (100+i)+ columnSeperator + (100+j) + columnSeperator + "area-"+((i+j)%10 +
            columnSeperator+(i+j)%2)
    println("- - " * 20)
    //    lines.foreach(println(_))
    println(lines(23))
    println("- - " * 20)

    val hashName = "areaMap1"
    val fieldIdxes = (0 to 1).toArray
    val fieldSeperator = ":"
    println("fieldIdxes="+fieldIdxes.mkString(","))
    val valueIdx = 3
    val conversion10to16Idxes = fieldIdxes
    val valueMapEnabled = true
    val valueMap = "学校"

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


    val worker1 = new Load2SingleHashThread(lines.toArray,columnSeperator,
      hashName,
      fieldIdxes, fieldSeperator, conversion10to16Idxes,
      valueIdx, valueMapEnabled, valueMap,
      jedisPools(0),loadMethod, batchLimitForRedis, overwrite, appendSeperator)

    val thread1 = new Thread(worker1)
    thread1.join()
    thread1.start()


    Thread.sleep(1 * 1000)
    //检查结果1
    val rs1 = jedises(0).hgetAll(hashName)
    println("rs1 = "+rs1)

    //    assert(rs1.get("2:3").toString=="area-5")
    assert(rs1.get("0066:0067").toString=="学校")
    //    assert(rs1.get("8:9").toString=="area-7")
    assert(rs1.get("006C:006D").toString=="学校")
    assert(rs1.get("006C:006C") == null)

    assert(rs1.size() == 50)
  }


  test("6 测试100条记录，1个线程，每6条记录批量提交， pipeline_hset，追加加载, 做区域映射"){

    val columnSeperator = ","

    //数据 102,103,area-5,1
    val lines = for(i<- 0 until 10;j<-0 until 10) yield (100+i)+ columnSeperator + (100+j) + columnSeperator + "area-"+((i+j)%10 +
            columnSeperator+(i+j)%2)
    println("- - " * 20)
    //    lines.foreach(println(_))
    println(lines(23))
    println("- - " * 20)

    val hashName = "areaMap1"
    val fieldIdxes = (0 to 1).toArray
    val fieldSeperator = ":"
    println("fieldIdxes="+fieldIdxes.mkString(","))
    val valueIdx = 3
    val conversion10to16Idxes = fieldIdxes
    val valueMapEnabled = true
    val valueMap = "WLAN"

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
    val overwrite = false
    val appendSeperator = ","


    val worker1 = new Load2SingleHashThread(lines.toArray,columnSeperator,
      hashName,
      fieldIdxes, fieldSeperator, conversion10to16Idxes,
      valueIdx, valueMapEnabled, valueMap,
      jedisPools(0),loadMethod, batchLimitForRedis, overwrite, appendSeperator)

    val thread1 = new Thread(worker1)
    thread1.join()
    thread1.start()

    Thread.sleep(3 * 100)

    val valueMap2="学校"
    val worker2 = new Load2SingleHashThread(lines.toArray,columnSeperator,
      hashName,
      fieldIdxes, fieldSeperator, conversion10to16Idxes,
      valueIdx, valueMapEnabled, valueMap2,
      jedisPools(0),loadMethod, batchLimitForRedis, overwrite, appendSeperator)

    val thread2 = new Thread(worker2)
    thread2.join()
    thread2.start()

    Thread.sleep(3 * 1000)
    //检查结果1
    val rs1 = jedises(0).hgetAll(hashName)
    println("rs1 = "+rs1)

    //    assert(rs1.get("2:3").toString=="area-5")
    assert(rs1.get("0066:0067").split(appendSeperator).toSet==Set("WLAN","学校"))
    //    assert(rs1.get("8:9").toString=="area-7")
    assert(rs1.get("006C:006D").split(appendSeperator).toSet==Set("WLAN","学校"))
    assert(rs1.get("006C:006C") == null)

    assert(rs1.size() == 50)
  }

}
