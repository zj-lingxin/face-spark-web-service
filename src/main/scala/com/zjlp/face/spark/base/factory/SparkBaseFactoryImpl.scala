package com.zjlp.face.spark.base.factory

import java.util.Date
import java.util.concurrent.TimeUnit
import javax.annotation.PostConstruct

import akka.actor.ActorSystem
import com.zjlp.face.spark.base.{Constants, Props, ISparkBaseFactory, SQLContextSingleton}
import org.apache.spark.sql.SQLContext
import org.apache.spark.Logging
import org.springframework.stereotype.Component

import scala.concurrent.duration.FiniteDuration

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
    removeTempTables
    updateData
  }

  private def updateData = {
    dropOfRosterTable
    cacheOfRosterTable
  }

  private def cacheOfRosterTable = {
    val newOfRoster = s"ofRoster_${new Date().getTime}"

    getSQLContext.read.format("jdbc").options(Map(
      "url" -> Props.get("jdbc_conn"),
      "dbtable" -> s"(select rosterID,username,loginAccount,userID as userID from view_ofroster where sub=3 and userID is not null) tb",
      "driver" -> Props.get("jdbc_driver"),
      "partitionColumn" -> "rosterID",
      "lowerBound" -> "1",
      "upperBound" -> Props.get("roster_upper_bound"),
      "numPartitions" -> Props.get("spark.table.numPartitions")
    )).load().registerTempTable(newOfRoster)
    getSQLContext.sql(s"cache table ${newOfRoster}")
    logInfo(s"缓存 $newOfRoster 临时表")
  }

  /**
   * 删除旧的ofRoster临时表,只保留最新的两个ofRoster表
   */
  private def dropOfRosterTable = {
    val ofRosters = getSQLContext.tableNames()
      .filter(_.startsWith("ofRoster_")).sorted.reverse
    if (ofRosters.length >= 2) {
      for (i <- 1 until ofRosters.length) {
        getSQLContext.dropTempTable(ofRosters(i))
        logInfo(s"删除 ${ofRosters(i)} 临时表")
      }
    }
  }

  private def removeTempTables = {

    getSQLContext.tableNames().filter(name => name.startsWith(Constants.relationsTable)
      || name.startsWith(Constants.comFriendsTable)).foreach {
      tableName =>
        getSQLContext.dropTempTable(tableName)
        logInfo(s"删除临时表:$tableName")
    }
  }

  private def getUpperBound = {
    val maxCount = SQLContextSingleton.getInstance().read.format("jdbc").options(Map(
      "url" -> Props.get("jdbc_conn"),
      "dbtable" -> "(select count(1) as totalAmount from view_ofroster) table_num",
      "driver" -> Props.get("jdbc_driver")
    )).load().select("totalAmount").map(_ (0).toString).collect()(0)
    maxCount.toLong.toString
  }

  @PostConstruct
  def initSparkBase = {
    import scala.concurrent.ExecutionContext.Implicits.global
    ActorSystem("sparkDataScheduler").scheduler.schedule(FiniteDuration(0, TimeUnit.MINUTES), FiniteDuration(Props.get("app.update.interval.minutes").toInt, TimeUnit.MINUTES))(updateSQLContext)
  }

}
