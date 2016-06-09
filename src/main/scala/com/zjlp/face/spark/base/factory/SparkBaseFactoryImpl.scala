package com.zjlp.face.spark.base.factory

import javax.annotation.PostConstruct

import com.zjlp.face.spark.base.{Props, ISparkBaseFactory, JdbcDF, SQLContextSingleton}
import com.zjlp.face.spark.util.DateUtils
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
      "dbtable" -> "(select rosterID,username,loginAccount,userId from view_ofroster where sub=3) ofRoster",
      "driver" -> Props.get("jdbc_driver"),
      "partitionColumn" -> "rosterID",
      "lowerBound" -> Props.get("spark.table.lowerBound"),
      "upperBound" -> Props.get("spark.table.upperBound"),
      "numPartitions" -> Props.get("spark.table.numPartitions")
    )).load().registerTempTable("ofRoster")
  }

  @PostConstruct
  private def initSparkBase = {
    updateSQLContext
  }
}
