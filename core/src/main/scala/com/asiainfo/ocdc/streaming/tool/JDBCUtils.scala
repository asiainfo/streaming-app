package com.asiainfo.ocdc.streaming.tool

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import com.asiainfo.ocdc.streaming.constant.CommonConstant

import scala.collection.mutable.ArrayBuffer
import scala.xml.XML

/**
 * Created by tianyi on 3/30/15.
 */
object JDBCUtils {

  private def connection: Connection = {
    val spark_home = System.getenv("SPARK_HOME")
    val xml = XML.loadFile(spark_home + "/" + CommonConstant.commonConfFileName)
//    val xml = XML.loadFile(CommonConstant.commonConfFileName)
    val mysqlNode = (xml \ "mysql")
    val url = (mysqlNode \ "url").text
    val username = (mysqlNode \ "username").text
    val password = (mysqlNode \ "password").text
//    classOf[com.mysql.jdbc.Driver]
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection(url, username, password)
  }

  def query(sql: String): Array[Map[String, String]] = {
    var statement: Statement = null
    var rs: ResultSet = null
    var result = ArrayBuffer[Map[String, String]]()
    try {
      // Configure to be Read Only
      statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      // Execute Query
      rs = statement.executeQuery(sql)

      // Getting column names
      val md = rs.getMetaData

      // Iterate Over ResultSet
      while (rs.next) {
        val line: Map[String, String] = (1 to md.getColumnCount).map(index => {
          (md.getColumnLabel(index), rs.getString(index))
        }).toMap[String, String]
        result += line
      }
      result.toArray
    }
    finally {
      if (statement != null) statement.close()
      if (rs != null) rs.close()
    }
  }
  
  // add by surq at 2015.8.13 start  
  // 等效 query(sql: String)
    def query2(sql: String): Array[Array[String]] = {
    var statement: Statement = null
    var rs: ResultSet = null
    val result = ArrayBuffer[Array[String]]()
    val conn = connection
    try {
      statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
       rs = statement.executeQuery(sql)
      // Getting column names
      val md = rs.getMetaData
      // 所取列数
      val ColumnCount = md.getColumnCount
      while (rs.next) result += (1 to md.getColumnCount).map(index =>rs.getString(index)).toArray
      result.toArray
    }
    finally {
      if (statement != null) statement.close()
      if (rs != null) rs.close()
      if (conn != null) conn.close()
    }
  }
 // add by surq at 2015.8.13 end   
    
  def execute(sql: String): Unit = {
    var statement: Statement = null
    try {
      // Configure to be Read Only
      statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      // Execute Query
      statement.execute(sql)
    }
    finally {
      if (statement != null) statement.close()
    }
  }
}
