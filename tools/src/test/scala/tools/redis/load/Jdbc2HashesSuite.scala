package tools.redis.load

import java.text.SimpleDateFormat
import java.util.Timer
import java.util.concurrent.{ExecutorService, Executors, FutureTask}

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.slf4j.LoggerFactory
import redis.clients.jedis.{Jedis, JedisPool, Pipeline}
import tools.jdbc.JdbcUtils
import tools.redis.RedisUtils

/**
 * Created by tsingfu on 15/6/8.
 */
class Jdbc2HashesSuite extends FunSuite with BeforeAndAfter{

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


  before {
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

  after {
    //释放资源
    logger.info("Release jedis Pool resources...")
    for(i <- 0 until numPools){
      jedisPools(i).returnResourceObject(jedises(i))
    }
    jedisPools.foreach(_.close())
    timer = null
  }

  test("1 生成测试数据"){

    threadPool.shutdownNow()
    threadPool2.shutdownNow()
    if(reportEnabled) timer.cancel()


    val confXmlFile = "tools/conf/redis-load/jdbc2hashes-test.xml"
    val props = Jdbc2Hashes.init_props_fromXml(confXmlFile)

    val redisServers = props.getProperty("redis.servers")
    val redisDatabase = props.getProperty("redis.database").trim.toInt
    val redisTimeout = props.getProperty("redis.timeout").trim.toInt
    val redisPasswd = props.getProperty("redis.password")
    val redisPassword = if(redisPasswd==null||redisPasswd == "") null else redisPasswd

    val jedisPoolMaxToal = props.getProperty("jedisPool.maxTotal").trim.toInt
    val jedisPoolMaxidle = props.getProperty("jedisPool.maxIdle").trim.toInt
    val jedisPoolMinidle = props.getProperty("jedisPool.minIdle").trim.toInt

    val jdbcPoolMaxActive = props.getProperty("jdbcPool.maxActive").trim.toInt
    val jdbcPoolInitialSize = props.getProperty("jdbcPool.initialSize").trim.toInt
    val jdbcPoolMaxIdle = props.getProperty("jdbcPool.maxIdle").trim.toInt
    val jdbcPoolMinIdle = props.getProperty("jdbcPool.minIdle").trim.toInt

    val from = props.getProperty("load.from").trim

    val jdbcDriver = props.getProperty("load.driver").trim
    val jdbcUrl = props.getProperty("load.url").trim
    val jdbcUsername = props.getProperty("load.username").trim
    val jdbcPassword = props.getProperty("load.password").trim
    val jdbcTable = props.getProperty("load.table").trim

    val hashNamePrefix = props.getProperty("load.hashNamePrefix").trim
    val hashColumnNames = props.getProperty("load.hashColumnNames").trim.split(",").map(_.trim)
    val hashSeperator = props.getProperty("load.hashSeperator").trim

    val valueColumnNames = props.getProperty("load.valueColumnNames").trim.split(",").map(_.trim)
    val fieldNames = props.getProperty("load.fieldNames").trim.split(",").map(_.trim)

    val batchLimit = props.getProperty("load.batchLimit").trim.toInt
    val batchLimitForRedis = props.getProperty("load.batchLimit.redis").trim.toInt

    val numThreads = props.getProperty("load.numThreads").trim.toInt
    val loadMethod = props.getProperty("load.method").trim

    val overwrite = props.getProperty("load.overwrite").trim.toBoolean
    val appendSeperator = props.getProperty("load.appendSeperator").trim

    val ds = JdbcUtils.init_dataSource(jdbcDriver, jdbcUrl, jdbcUsername, jdbcPassword,
      jdbcPoolMaxActive, jdbcPoolInitialSize, jdbcPoolMaxIdle, jdbcPoolMinIdle)

    val conn = ds.getConnection
    val stmt = conn.createStatement()

    val tabName_test = "tab_test_jdbc2hashes"

//    stmt.execute("create table if not exists "+tabName_test+" (id1 int, id2 int, col3 varchar(50), col4 varchar(50))")
//    stmt.execute("truncate table "+tabName_test)
//
//    for(i <- 0 until 10; j<- 0 until 10){
//      stmt.execute("insert into "+tabName_test+" value ("+i+","+j+",\"value-test-"+i+"\", \"value-test-"+j+"\")")
//    }

    val rs = stmt.executeQuery("select * from "+tabName_test)
    rs.absolute(24)
    println(rs.getString(1))
    assert(rs.getString(1)=="2")
    println(rs.getString(2))
    assert(rs.getString(2)=="3")
    println(rs.getString(3))
    assert(rs.getString(3)=="value-test-"+2)
    println(rs.getString(4))
    assert(rs.getString(4)=="value-test-"+3)


    //为 Jdbc2SingleHash准备数据
    val tabName_test2="tab_test_jdbc2singlehash"
    stmt.execute("create table if not exists "+tabName_test2 +" as select * from " + tabName_test)

    val tabName_test3 = tabName_test + "_change"
//    stmt.execute("create table if not exists "+tabName_test3+" (sync_flag int, id1 int, id2 int, col3 varchar(50), col4 varchar(50))")
//    stmt.execute("truncate table "+tabName_test3)

    val insertId = "11"
    val updateId1 = "2"
    val updateId2 = "3"
    val deleteId1 = "3"
    val deleteId2 = "2"

    stmt.execute("insert into "+tabName_test3+" value (1," + insertId + ","+ insertId+",\"value-test-"+insertId+"\", \"value-test-"+insertId+"\")")
    stmt.execute("insert into "+tabName_test3+" value (0," + updateId1 + ","+ updateId2+",\"value-test2-"+updateId1+"\", \"value-test2-"+updateId2+"\")")
    stmt.execute("insert into "+tabName_test3+" value (-1," + deleteId1 + ","+ deleteId2+",\"value-test-"+deleteId1+"\", \"value-test-"+deleteId2+"\")")

    JdbcUtils.closeQuiet(rs, stmt, conn)
  }



  test("2 测试 tools/conf/redis-load/jdbc2hashes-test.xml"){

    //测试数据，如：2,3,value-test-2,value-test-3
    // 从mysql tab_test_jdbc2hashes表中导入id1,id2,col3,col4 4列数据到redis中,
    // id1,id2取值使用冒号拼接，和前缀jdbc2hashes:共同组成hash名
    // col3,col4取值作为value，属性名分别对应field1,field2
    val confXmlFile = "tools/conf/redis-load/jdbc2hashes-test.xml"
    Jdbc2Hashes.jdbc2Hashes(confXmlFile)

    val props = Jdbc2Hashes.init_props_fromXml(confXmlFile)
    val hashNamePrefix = props.getProperty("load.hashNamePrefix")

    //检查结果
    println("hgetall " + hashNamePrefix + "\"2:3\"")
    val rs1 = jedises(0).hgetAll(hashNamePrefix + "2:3")
    println("rs1 = "+rs1)

    assert(rs1.get("field1")=="value-test-2")
    assert(rs1.get("field2")=="value-test-3")
    assert(rs1.size() == 2)
  }
}
