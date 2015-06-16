package tools.redis.load

import java.text.SimpleDateFormat
import java.util.concurrent.{ExecutorService, Executors, FutureTask, TimeUnit}
import java.util.{Date, Timer}

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.slf4j.LoggerFactory
import redis.clients.jedis.{Jedis, JedisPool, Pipeline}
import tools.redis.RedisUtils

/**
 * Created by tsingfu on 15/6/8.
 */
class Load2OneHashThreadSuite extends FunSuite with BeforeAndAfter {

  val logger = LoggerFactory.getLogger(this.getClass)
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")


  //进度信息监控
  var loadStatus: LoadStatus = _
  var taskMap: scala.collection.mutable.HashMap[Long, FutureTask[FutureTaskResult]] = _
  var timer: Timer = _ //用于调度reporter任务，定期输出进度信息


  //通用配置
  val servers = "codis1:29001, codis1:29002"
  val timeout = 10000
  val database = 0
  val password = null
  val maxTotal = 100
  val maxIdle = 10
  val minIdle = 0

  val hashName = "load2singlehash"

  val reportEnabled = true
  val reportDelaySeconds = 1
  val reportIntervalSeconds = 10

  //文件
  val columnSeperator = ","

  val batchLimitForRedis = 6


  //初始化线程池
  var threadPool: ExecutorService = _
  var threadPool2: ExecutorService = _

  // 初始化 jedisPool, jedis, pipeline
  var jedisPools: Array[JedisPool] = _
  var numPools: Int = _
  var jedises: Array[Jedis] = _
  var pipelines: Array[Pipeline] = _


  before{
    //进度信息监控
    loadStatus = new LoadStatus()
    taskMap = scala.collection.mutable.HashMap[Long, FutureTask[FutureTaskResult]]()
    timer = new Timer() //用于调度reporter任务，定期输出进度信息

    //记录开始时间
    loadStatus.startTimeMs = System.currentTimeMillis()
    logger.info("startTimeMs = " + loadStatus.startTimeMs + "")

    //初始化线程池
    threadPool = Executors.newFixedThreadPool(2)
    threadPool2 = Executors.newFixedThreadPool(1)

    // 初始化 jedisPool, jedis, pipeline
    jedisPools = servers.split(",").map(server=>{
      val hostPort = server.split(":").map(_.trim)
      val host = hostPort(0)
      val port = hostPort(1).toInt
      println("host = " + host +", port ="+port)
      RedisUtils.init_jedisPool(host, port, timeout, database, password, maxTotal, maxIdle, minIdle)
    })
    numPools = jedisPools.length
    jedises = jedisPools.map(_.getResource)
    pipelines = jedises.map(_.pipelined)

    //遍历数据之前，先启动进度监控线程和进度信息更新线程
    //如果启用定期输出进度信息功能，启动reporter任务
    if(reportEnabled){
      timer.schedule(new MonitorTask(loadStatus, reportIntervalSeconds),
        reportDelaySeconds * 1000, reportIntervalSeconds * 1000)
    }

    //启动进度信息更新线程
    val loadStatusUpdateTask = new LoadStatusUpdateThread(loadStatus, taskMap)
    threadPool2.submit(loadStatusUpdateTask)
  }

  after{
    //释放资源
    logger.info("Release jedis Pool resources...")
    for(i <- 0 until numPools){
      jedisPools(i).returnResourceObject(jedises(i))
    }
    jedisPools.foreach(_.close())
    timer = null
  }

  test("1 测试100条记录，1个线程，每6条记录批量提交， hset，覆盖加载"){

    val columnSeperator = ","

    //生成测试数据 2,3,value1-test-2,value2-test-3
    val lines = for(i<- 0 until 10;j<-0 until 10) yield i+ columnSeperator + j + columnSeperator + "value1-test-"+i + columnSeperator +"value2-test-" + j
    println("- - " * 20)
    //    lines.foreach(println(_))
    println(lines(23))
    println("- - " * 20)

    loadStatus.numTotal = 100

    //1，2两列使用冒号拼接，作为field
    val fieldIdxes = (0 to 1).toArray
    val fieldSeperator = ":"
    //3，4两列使用井号拼接，作为对应value
    val valueIdxes = (2 to 3).toArray
    val valueSeperator = "#"


    val loadMethod = "hset"
    val overwrite = true
    val appendSeperator = ","

    loadStatus.numScanned = 100
    val task = new Load2OneHashThread(lines.toArray, columnSeperator,
      hashName, fieldIdxes, fieldSeperator,
      valueIdxes, valueSeperator,
      jedisPools(0),loadMethod, batchLimitForRedis, overwrite, appendSeperator,
      FutureTaskResult(loadStatus.numBatches, 100, 0))

    val futureTask = new FutureTask[FutureTaskResult](task)

    threadPool.submit(futureTask)
    taskMap.put(loadStatus.numBatches, futureTask)

    loadStatus.numBatches += 1

    threadPool.shutdown()
    threadPool.awaitTermination(Long.MaxValue, TimeUnit.DAYS)


    //加载线程任务都完成后，关闭监控线程，并输出最终的统计信息
    loadStatus.loadFinished = true
    threadPool2.shutdown()
    threadPool2.awaitTermination(Long.MaxValue, TimeUnit.DAYS)
    if(reportEnabled) timer.cancel()

    val runningTimeMs = System.currentTimeMillis() - loadStatus.startTimeMs
    val loadSpeedPerSec = loadStatus.numProcessed * 1.0 / runningTimeMs * 1000 //记录加载运行期间加载平均速度
    val loadSpeedPerSecLastMonitored = (loadStatus.numProcessed - loadStatus.numProcessedlastMonitored) * 1.0 / reportIntervalSeconds //记录最近一次输出进度信息的周期对应的加载平均速度
    loadStatus.numProcessedlastMonitored = loadStatus.numProcessed

    println(sdf.format(new Date()) + " [INFO] finished load, statistics: numTotal = "+loadStatus.numTotal+ ", numScanned = " + loadStatus.numScanned +", numBatches = "+loadStatus.numBatches +
            ", numProcessed = " + loadStatus.numProcessed + ", numProcessedBatches = "+loadStatus.numBatchesProcessed+
            ", runningTime = " + runningTimeMs +" ms <=> " + runningTimeMs / 1000.0  +" s <=> " + runningTimeMs / 1000.0 / 60 +" min" +
            ", loadSpeed = " + loadSpeedPerSec +", records/s => " + (loadSpeedPerSec / 60) + " records/min <=> " + (loadSpeedPerSec / 60 / 60) +" records/h" +
            ", loadSpeedLastMonitored = " + loadSpeedPerSecLastMonitored +" records/s <=> " + loadSpeedPerSecLastMonitored / 60 +" records/min" +
            ", loadProgress percent of numProcessed = " + loadStatus.numProcessed * 1.0 / loadStatus.numTotal * 100 +"%" +
            ", loadProgress percent of numBatchesProcessed = " + loadStatus.numBatchesProcessed * 1.0 / loadStatus.numBatches * 100 +"%" )


    //检查结果
    val rs1 = jedises(0).hgetAll(hashName)
    println("rs1 = "+rs1)

    assert(rs1.get("2:3").toString=="value1-test-2#value2-test-3")
    assert(rs1.get("8:9").toString=="value1-test-8#value2-test-9")
    assert(rs1.size() == 100)
  }


  test("2 测试100条记录，1个线程，每6条记录批量提交， hmset，覆盖加载"){

    val columnSeperator = ","

    //生成测试数据 2,3,value1-test-2,value2-test-3
    val lines = for(i<- 0 until 10;j<-0 until 10) yield i+ columnSeperator + j + columnSeperator + "value1-test-"+i + columnSeperator +"value2-test-" + j
    println("- - " * 20)
    //    lines.foreach(println(_))
    println(lines(23))
    println("- - " * 20)

    loadStatus.numTotal = 100

    //1，2两列使用冒号拼接，作为field
    val fieldIdxes = (0 to 1).toArray
    val fieldSeperator = ":"
    //3，4两列使用井号拼接，作为对应value
    val valueIdxes = (2 to 3).toArray
    val valueSeperator = "#"


    val loadMethod = "hmset"
    val overwrite = true
    val appendSeperator = ","

    loadStatus.numScanned = 100
    val task = new Load2OneHashThread(lines.toArray, columnSeperator,
      hashName, fieldIdxes, fieldSeperator,
      valueIdxes, valueSeperator,
      jedisPools(0),loadMethod, batchLimitForRedis, overwrite, appendSeperator,
      FutureTaskResult(loadStatus.numBatches, 100, 0))

    val futureTask = new FutureTask[FutureTaskResult](task)

    threadPool.submit(futureTask)
    taskMap.put(loadStatus.numBatches, futureTask)

    loadStatus.numBatches += 1

    threadPool.shutdown()
    threadPool.awaitTermination(Long.MaxValue, TimeUnit.DAYS)


    //加载线程任务都完成后，关闭监控线程，并输出最终的统计信息
    loadStatus.loadFinished = true
    threadPool2.shutdown()
    threadPool2.awaitTermination(Long.MaxValue, TimeUnit.DAYS)
    if(reportEnabled) timer.cancel()

    val runningTimeMs = System.currentTimeMillis() - loadStatus.startTimeMs
    val loadSpeedPerSec = loadStatus.numProcessed * 1.0 / runningTimeMs * 1000 //记录加载运行期间加载平均速度
    val loadSpeedPerSecLastMonitored = (loadStatus.numProcessed - loadStatus.numProcessedlastMonitored) * 1.0 / reportIntervalSeconds //记录最近一次输出进度信息的周期对应的加载平均速度
    loadStatus.numProcessedlastMonitored = loadStatus.numProcessed

    println(sdf.format(new Date()) + " [INFO] finished load, statistics: numTotal = "+loadStatus.numTotal+ ", numScanned = " + loadStatus.numScanned +", numBatches = "+loadStatus.numBatches +
            ", numProcessed = " + loadStatus.numProcessed + ", numProcessedBatches = "+loadStatus.numBatchesProcessed+
            ", runningTime = " + runningTimeMs +" ms <=> " + runningTimeMs / 1000.0  +" s <=> " + runningTimeMs / 1000.0 / 60 +" min" +
            ", loadSpeed = " + loadSpeedPerSec +", records/s => " + (loadSpeedPerSec / 60) + " records/min <=> " + (loadSpeedPerSec / 60 / 60) +" records/h" +
            ", loadSpeedLastMonitored = " + loadSpeedPerSecLastMonitored +" records/s <=> " + loadSpeedPerSecLastMonitored / 60 +" records/min" +
            ", loadProgress percent of numProcessed = " + loadStatus.numProcessed * 1.0 / loadStatus.numTotal * 100 +"%" +
            ", loadProgress percent of numBatchesProcessed = " + loadStatus.numBatchesProcessed * 1.0 / loadStatus.numBatches * 100 +"%" )


    //检查结果
    val rs1 = jedises(0).hgetAll(hashName)
    println("rs1 = "+rs1)

    assert(rs1.get("2:3").toString=="value1-test-2#value2-test-3")
    assert(rs1.get("8:9").toString=="value1-test-8#value2-test-9")
    assert(rs1.size() == 100)
  }


  test("3 测试100条记录，1个线程，每6条记录批量提交， pipeline_hset，覆盖加载"){

    val columnSeperator = ","

    //生成测试数据 2,3,value1-test-2,value2-test-3
    val lines = for(i<- 0 until 10;j<-0 until 10) yield i+ columnSeperator + j + columnSeperator + "value1-test-"+i + columnSeperator +"value2-test-" + j
    println("- - " * 20)
    //    lines.foreach(println(_))
    println(lines(23))
    println("- - " * 20)

    loadStatus.numTotal = 100

    //1，2两列使用冒号拼接，作为field
    val fieldIdxes = (0 to 1).toArray
    val fieldSeperator = ":"
    //3，4两列使用井号拼接，作为对应value
    val valueIdxes = (2 to 3).toArray
    val valueSeperator = "#"


    val loadMethod = "pipeline_hset"
    val overwrite = true
    val appendSeperator = ","

    loadStatus.numScanned = 100
    val task = new Load2OneHashThread(lines.toArray, columnSeperator,
      hashName, fieldIdxes, fieldSeperator,
      valueIdxes, valueSeperator,
      jedisPools(0),loadMethod, batchLimitForRedis, overwrite, appendSeperator,
      FutureTaskResult(loadStatus.numBatches, 100, 0))

    val futureTask = new FutureTask[FutureTaskResult](task)

    threadPool.submit(futureTask)
    taskMap.put(loadStatus.numBatches, futureTask)

    loadStatus.numBatches += 1

    threadPool.shutdown()
    threadPool.awaitTermination(Long.MaxValue, TimeUnit.DAYS)


    //加载线程任务都完成后，关闭监控线程，并输出最终的统计信息
    loadStatus.loadFinished = true
    threadPool2.shutdown()
    threadPool2.awaitTermination(Long.MaxValue, TimeUnit.DAYS)
    if(reportEnabled) timer.cancel()

    val runningTimeMs = System.currentTimeMillis() - loadStatus.startTimeMs
    val loadSpeedPerSec = loadStatus.numProcessed * 1.0 / runningTimeMs * 1000 //记录加载运行期间加载平均速度
    val loadSpeedPerSecLastMonitored = (loadStatus.numProcessed - loadStatus.numProcessedlastMonitored) * 1.0 / reportIntervalSeconds //记录最近一次输出进度信息的周期对应的加载平均速度
    loadStatus.numProcessedlastMonitored = loadStatus.numProcessed

    println(sdf.format(new Date()) + " [INFO] finished load, statistics: numTotal = "+loadStatus.numTotal+ ", numScanned = " + loadStatus.numScanned +", numBatches = "+loadStatus.numBatches +
            ", numProcessed = " + loadStatus.numProcessed + ", numProcessedBatches = "+loadStatus.numBatchesProcessed+
            ", runningTime = " + runningTimeMs +" ms <=> " + runningTimeMs / 1000.0  +" s <=> " + runningTimeMs / 1000.0 / 60 +" min" +
            ", loadSpeed = " + loadSpeedPerSec +", records/s => " + (loadSpeedPerSec / 60) + " records/min <=> " + (loadSpeedPerSec / 60 / 60) +" records/h" +
            ", loadSpeedLastMonitored = " + loadSpeedPerSecLastMonitored +" records/s <=> " + loadSpeedPerSecLastMonitored / 60 +" records/min" +
            ", loadProgress percent of numProcessed = " + loadStatus.numProcessed * 1.0 / loadStatus.numTotal * 100 +"%" +
            ", loadProgress percent of numBatchesProcessed = " + loadStatus.numBatchesProcessed * 1.0 / loadStatus.numBatches * 100 +"%" )


    //检查结果
    val rs1 = jedises(0).hgetAll(hashName)
    println("rs1 = "+rs1)

    assert(rs1.get("2:3").toString=="value1-test-2#value2-test-3")
    assert(rs1.get("8:9").toString=="value1-test-8#value2-test-9")
    assert(rs1.size() == 100)
  }


  test("4 测试100条记录，1个线程，每6条记录批量提交， hset，追加加载"){

    val columnSeperator = ","

    //生成测试数据 2,3,value1-test-2,value2-test-3
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

    loadStatus.numTotal = 200

    //1，2两列使用冒号拼接，作为field
    val fieldIdxes = (0 to 1).toArray
    val fieldSeperator = ":"
    //3，4两列使用井号拼接，作为对应value
    val valueIdxes = (2 to 3).toArray
    val valueSeperator = "#"


    val loadMethod = "hset"
    val overwrite = true
    val appendSeperator = ","

    loadStatus.numScanned = 200
    val task = new Load2OneHashThread(lines.toArray, columnSeperator,
      hashName, fieldIdxes, fieldSeperator,
      valueIdxes, valueSeperator,
      jedisPools(0),loadMethod, batchLimitForRedis, overwrite, appendSeperator,
      FutureTaskResult(loadStatus.numBatches, 100, 0))

    val futureTask = new FutureTask[FutureTaskResult](task)

    threadPool.submit(futureTask)
    taskMap.put(loadStatus.numBatches, futureTask)

    loadStatus.numBatches += 1

    val overwrite2 = false
    val task2 = new Load2OneHashThread(lines.toArray, columnSeperator,
      hashName, fieldIdxes, fieldSeperator,
      valueIdxes, valueSeperator,
      jedisPools(0),loadMethod, batchLimitForRedis, overwrite2, appendSeperator,
      FutureTaskResult(loadStatus.numBatches, 100, 0))

    val futureTask2 = new FutureTask[FutureTaskResult](task)

    threadPool.submit(futureTask2)
    taskMap.put(loadStatus.numBatches, futureTask2)

    loadStatus.numBatches += 1

    threadPool.shutdown()
    threadPool.awaitTermination(Long.MaxValue, TimeUnit.DAYS)


    //加载线程任务都完成后，关闭监控线程，并输出最终的统计信息
    loadStatus.loadFinished = true
    threadPool2.shutdown()
    threadPool2.awaitTermination(Long.MaxValue, TimeUnit.DAYS)
    if(reportEnabled) timer.cancel()

    val runningTimeMs = System.currentTimeMillis() - loadStatus.startTimeMs
    val loadSpeedPerSec = loadStatus.numProcessed * 1.0 / runningTimeMs * 1000 //记录加载运行期间加载平均速度
    val loadSpeedPerSecLastMonitored = (loadStatus.numProcessed - loadStatus.numProcessedlastMonitored) * 1.0 / reportIntervalSeconds //记录最近一次输出进度信息的周期对应的加载平均速度
    loadStatus.numProcessedlastMonitored = loadStatus.numProcessed

    println(sdf.format(new Date()) + " [INFO] finished load, statistics: numTotal = "+loadStatus.numTotal+ ", numScanned = " + loadStatus.numScanned +", numBatches = "+loadStatus.numBatches +
            ", numProcessed = " + loadStatus.numProcessed + ", numProcessedBatches = "+loadStatus.numBatchesProcessed+
            ", runningTime = " + runningTimeMs +" ms <=> " + runningTimeMs / 1000.0  +" s <=> " + runningTimeMs / 1000.0 / 60 +" min" +
            ", loadSpeed = " + loadSpeedPerSec +", records/s => " + (loadSpeedPerSec / 60) + " records/min <=> " + (loadSpeedPerSec / 60 / 60) +" records/h" +
            ", loadSpeedLastMonitored = " + loadSpeedPerSecLastMonitored +" records/s <=> " + loadSpeedPerSecLastMonitored / 60 +" records/min" +
            ", loadProgress percent of numProcessed = " + loadStatus.numProcessed * 1.0 / loadStatus.numTotal * 100 +"%" +
            ", loadProgress percent of numBatchesProcessed = " + loadStatus.numBatchesProcessed * 1.0 / loadStatus.numBatches * 100 +"%" )


    //检查结果
    val rs1 = jedises(0).hgetAll(hashName)
    println("rs1 = "+rs1)

    assert(rs1.get("2:3").toString=="value1-test-2#value2-test-3")
    assert(rs1.get("8:9").toString=="value1-test-8#value2-test-9")
    assert(rs1.size() == 100)
  }

}
