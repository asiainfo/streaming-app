package tools.redis.loadAreaMap

import org.scalatest.{BeforeAndAfter, FunSuite}
import tools.jdbc.JdbcUtils

/**
 * Created by tsingfu on 15/6/8.
 */
class Jdbc2SingleHashSuite extends FunSuite with BeforeAndAfter{

  test("1 生成areamap测试数据"){

    val confXmlFile = "tools/conf/redis-load/loadAreaMap-jdbc2singlehash-test.xml"
    val props = Jdbc2SingleHash.init_props_fromXml(confXmlFile)

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

    val hashName = props.getProperty("load.hashName").trim
    val fieldColumnNames = props.getProperty("load.fieldColumnNames").trim.split(",").map(_.trim)
    val fieldSeperator = props.getProperty("load.fieldSeperator").trim
    val valueColumnName = props.getProperty("load.valueColumnName").trim
    val valueMapEnabled = props.getProperty("load.valueMapEnabled").trim.toBoolean
    val valueMap = props.getProperty("load.valueMap").trim
    val conversion10to16ColumnNames = props.getProperty("load.conversion10to16.columnNames").trim.split(",").map(_.trim)

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

    val tabName_test = "tab_areamap_jdbc2singlehash"

    stmt.execute("create table if not exists "+tabName_test+" (id1 int, id2 int, col3 varchar(50), col4 varchar(50))")
    stmt.execute("truncate table "+tabName_test)

    for(i <- 0 until 10; j<- 0 until 10){
      stmt.execute("insert into "+tabName_test+" value ("+(100+i)+","+(100+j)+",\"area-"+((i+j)%10)+"\", "+(i+j)%2 +")")
    }

    val rs = stmt.executeQuery("select * from "+tabName_test)
    rs.absolute(24)
    println(rs.getString(1))
    assert(rs.getString(1)=="102")
    println(rs.getString(2))
    assert(rs.getString(2)=="103")
    println(rs.getString(3))
    assert(rs.getString(3)=="area-"+5)
    println(rs.getString(4))
    assert(rs.getString(4)=="1")


    JdbcUtils.closeQuiet(rs, stmt, conn)

  }
}
