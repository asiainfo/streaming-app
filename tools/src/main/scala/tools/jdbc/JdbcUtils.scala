package tools.jdbc

import java.sql._

import org.apache.tomcat.jdbc.pool.{DataSource, PoolProperties}

/**
 * Created by tsingfu on 15/6/7.
 */
object JdbcUtils {

  /**
   * 初始化jdbc pool的数据源
   * @param driverClassName
   * @param url
   * @param username
   * @param password
   * @param maxActive
   * @param initialSize
   * @param maxIdle
   * @param minIdle
   * @param maxWaitMs
   * @param testOnBorrow
   * @param testOnReturn
   * @param testWhileIdle
   * @param validationInterval
   * @param validationQuery
   * @param validationTimeout
   * @param timeBetweenEvictionRunsMillis
   * @param minEvictableIdleTimeMillis
   * @return
   */
  def init_dataSource(driverClassName: String,
                      url: String,
                      username: String,
                      password: String,
                      maxActive: Int = 100,
                      initialSize: Int = 10,
                      maxIdle: Int = 100,
                      minIdle: Int = 10,
                      maxWaitMs: Int = 30000,
                     maxAge: Long = 0,
                      testOnBorrow: Boolean = false,
                      testOnReturn: Boolean = false,
                      testWhileIdle: Boolean = false,
                      validationInterval: Long = 30000,
                      validationQuery: String = "select 1",
                      validationTimeout: Int = -1,
                      timeBetweenEvictionRunsMillis: Int = 5000,
                      minEvictableIdleTimeMillis: Int = 60000): DataSource = {

    val ds = new DataSource()
    val jdbcPoolProps = init_jdbcPoolConfig(driverClassName, url, username, password,
      maxActive, initialSize, maxIdle, minIdle, maxWaitMs, maxAge,
      testOnBorrow, testOnReturn, testWhileIdle, validationInterval, validationQuery, validationTimeout,
      timeBetweenEvictionRunsMillis, minEvictableIdleTimeMillis)

    ds.setPoolProperties(jdbcPoolProps)

    ds
  }

  /**
   * 初始化 jdbcPoolConfig
   * @param driverClassName jdbc驱动类名
   * @param url 访问jdbc的url
   * @param username 访问jdbc的用户名
   * @param password 访问jdbc的密码
   * @param maxActive 最大连接数, 默认100个,设为0表示无限制
   * @param initialSize 初始化连接数量,默认10个
   * @param maxIdle 最小空闲连接数, 默认100
   * @param minIdle 最小空闲连接数, 默认10
   * @param maxWaitMs 获取连接时的最大等待毫秒数,如果超时就抛异常, -1 表示不限制等待时间,  默认30000
   * @param validationInterval 启用有效性检查的频率,默认30000
   * @param testOnBorrow 在获取连接的时候是否检查有效性, 默认false
   * @param testOnReturn 在返回连接的时候是否检查有效性, 默认false
   * @param testWhileIdle 在空闲时是否检查有效性, 默认false
   * @param validationQuery 启用有效性检查时使用的sql ,如： mysql/mssql=select 1, oracle=select 1 from dual;
   * @param validationTimeout 有效性检查超时时间，默认-1， <=0表示不启用超时机制
   * @param timeBetweenEvictionRunsMillis 运行空闲连接校验和清理线程的的时间间隔(毫秒) ， 默认5000
   * @param minEvictableIdleTimeMillis 空闲连接可以被驱逐之前的时间； 默认60000毫秒(60秒)
   * @return PoolProperties jdbc pool 的配置对象
   * 参考: [Attributes](http://tomcat.apache.org/tomcat-7.0-doc/jdbc-pool.html#Common_Attributes)
   */
  def init_jdbcPoolConfig(driverClassName: String,
                          url: String,
                          username: String,
                          password: String,
                          maxActive: Int = 100,
                          initialSize: Int = 10,
                          maxIdle: Int = 100,
                          minIdle: Int = 10,
                          maxWaitMs: Int = 30000,
                         maxAge: Long = 0,
                          testOnBorrow: Boolean = false,
                          testOnReturn: Boolean = false,
                          testWhileIdle: Boolean = false,
                          validationInterval: Long = 30000,
                          validationQuery: String = "select 1",
                          validationTimeout: Int = -1,
                          timeBetweenEvictionRunsMillis: Int = 5000,
                          minEvictableIdleTimeMillis: Int = 60000
                                 ): PoolProperties = {

    val jdbcPoolProps = new PoolProperties()

    jdbcPoolProps.setDriverClassName(driverClassName)
    jdbcPoolProps.setUrl(url)
    jdbcPoolProps.setUsername(username)
    jdbcPoolProps.setPassword(password)

    //最大连接数, 默认100个, 设为0表示无限制
    //    jdbcPoolProps.setMaxTotal(8)
    jdbcPoolProps.setMaxActive(maxActive)

    //初始化连接数, 默认10个
    jdbcPoolProps.setInitialSize(initialSize)

    //最小空闲连接数, 默认0，没有限制
    jdbcPoolProps.setMinIdle(minIdle)

    //获取连接时的最大等待毫秒数,如果超时就抛异常, -1 表示不限制等待时间,  默认-1
    jdbcPoolProps.setMaxWait(maxWaitMs)

    jdbcPoolProps.setMaxAge(maxAge)

    //在获取连接的时候是否检查有效性, 默认false
    //    jdbcPoolProps.setTestOnBorrow(false)
    jdbcPoolProps.setTestOnBorrow(testOnBorrow)

    //在空闲时是否检查有效性, 默认false
    //    jdbcPoolProps.setTestWhileIdle(false)
    jdbcPoolProps.setTestWhileIdle(testWhileIdle)

    //归还连接时是否检查连接有效性，默认false
    jdbcPoolProps.setTestOnReturn(testOnReturn)

    //检查连接有效的时间间隔， 0以下的话不检查。默认是0
    jdbcPoolProps.setValidationInterval(validationInterval)

    //验证连接有效性的方式,一条sql语句，用来验证数据库连接是否正常。这条语句必须是一个查询模式，并至少返回一条数据。可以为任何可以验证数据库连接是否正常的sql
    jdbcPoolProps.setValidationQuery(validationQuery)

    //运行判断连接超时任务（逐出扫描）的时间间隔(毫秒) 如果为负数,则不运行逐出线程, 默认5000
    //    jdbcPoolProps.setTimeBetweenEvictionRunsMillis(5000)
    jdbcPoolProps.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis)

    //逐出连接的最小空闲时间 默认1800000毫秒(30分钟)
    //    jdbcPoolProps.setMinEvictableIdleTimeMillis(1800000)
    jdbcPoolProps.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis)

    //是否启用pool的jmx管理功能, 默认true
    jdbcPoolProps.setJmxEnabled(true)

    jdbcPoolProps
  }


  def closeQuiet(rs: ResultSet, stmt: Statement, conn: Connection) {
    try {
      rs.close()
    } catch {
      case e: SQLException => e.printStackTrace()
    } finally {
      try {
        stmt.close()
      } catch {
        case e: SQLException => e.printStackTrace()
      } finally {
        try {
          conn.close()
        } catch {
          case e: SQLException => e.printStackTrace()
        }
      }
    }
  }
}

