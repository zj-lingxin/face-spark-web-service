package com.zjlp.face.spark.base.factory

import javax.annotation.PostConstruct

import com.zjlp.face.spark.base.{Props, ISparkBaseFactory, SQLContextSingleton}
import com.zjlp.face.spark.util.DateUtils
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.Logging
import org.springframework.stereotype.Component

@Component
class SparkBaseFactoryImpl extends ISparkBaseFactory with Logging {
  /**
   * 获取sqlContext
   * @return 返回值
   */
  def getSQLContext: SQLContext = {
    SQLContextSingleton.getInstance()
  }

  /**
   * 更新数据源
   */
  def updateSQLContext: Unit = {
    logInfo("数据更新时间:" + DateUtils.getStrDate("yyyy-MM-dd hh:mm:ss"))
    SQLContextSingleton.getInstance().read.format("jdbc").options(Map(
      "url" -> Props.get("jdbc_conn"),
      "dbtable" -> "(select rosterID,username,loginAccount,userID as userID from view_ofroster where sub=3) ofRoster",
      "driver" -> Props.get("jdbc_driver"),
       "partitionColumn" -> "rosterID",
      "lowerBound" -> "1",
      "upperBound" -> upperBound,
      "numPartitions" -> Props.get("spark.table.numPartitions")
    )).load().registerTempTable("ofRoster")
    SQLContextSingleton.getInstance().sql("cache table ofRoster")
  }

  private def upperBound = {
    SQLContextSingleton.getInstance().read.format("jdbc").options(Map(
      "url" -> Props.get("jdbc_conn"),
      "dbtable" -> "(select count(1) as totalAmount from view_ofroster where sub=3) table_num",
      "driver" -> Props.get("jdbc_driver")
    )).load().select("totalAmount").map(_(0).toString).collect()(0)
  }

  @PostConstruct
  private def initSparkBase = {
    updateSQLContext
  }
}
