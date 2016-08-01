package com.zjlp.face.spark.base.factory

import java.util.Date
import java.util.concurrent.TimeUnit
import javax.annotation.PostConstruct

import akka.actor.ActorSystem
import com.zjlp.face.spark.base.{Constants, Props, SQLContextSingleton}
import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.springframework.stereotype.Component

import scala.concurrent.duration.FiniteDuration

@Component
class IndustryContactsFactoryImpl extends Logging {
  /**
   * 获取sqlContext
   * @return 返回值
   */
  def getSQLContext: SQLContext = {
    SQLContextSingleton.getInstance()
  }

  /**
   * 删除旧的ofRoster临时表,只保留最新的两个ofRoster表
   */
  private def dropTable = {
    val tables = getSQLContext.tableNames()
      .filter(_.startsWith("IndustryContacts_")).sorted.reverse
    if (tables.length >= 2) {
      for (i <- 1 until tables.length) {
        getSQLContext.dropTempTable(tables(i))
        logInfo(s"删除 ${tables(i)} 临时表")
      }
    }
  }

  /**
   * 更新数据源
   */
  def updateSQLContext: Unit = {
    dropTable
    cacheOfIndustryContactsTable
  }

  private def cacheOfIndustryContactsTable = {
    val industryContacts = s"IndustryContacts_${new Date().getTime}"

    getSQLContext.read.format("jdbc").options(Map(
      "url" -> Props.get("jdbc_conn"),
      "dbtable" ->
        s"""(SELECT user.ID as userID,
           | user.LOGIN_ACCOUNT as loginAccount,
           | user.HEADIMGURL as headImgUrl,
           | user.CERTIFY as certify,
           | user.NICKNAME as nickname,
           | prestige.AMOUNT as prestigeAmount,
           | business_card.POSITION as position,
           | business_card.COMPANY_NAME as companyName,
           | business_card.V_AREA_NAME as areaName,
           | business_card.INDUSTRY_PROVIDE as industryProvide,
           | business_card.INDUSTRY_REQUIREMENT as industryRequirement,
           | business_card.INDUSTRY_CODE as industryCode,
           | business_card.V_AREA_CODE as areaCode,
           | user.CREATE_TIME as createTime
           | FROM business_card INNER JOIN user ON business_card.USER_ID = user.ID
           | INNER JOIN prestige ON prestige.USER_ID = business_card.USER_ID
           | WHERE business_card.STATUS = 1 and user.STATUS = 1) industry_contacts""".stripMargin,
      "driver" -> Props.get("jdbc_driver"),
      "partitionColumn" -> "userID",
      "lowerBound" -> "1",
      "upperBound" -> Props.get("industry_contacts_upper_bound"),
      "numPartitions" -> Props.get("spark.table.numPartitions")
    )).load().registerTempTable(industryContacts)
    getSQLContext.sql(s"cache table ${industryContacts}")
    logInfo(s"缓存 $industryContacts 临时表")
  }

  @PostConstruct
  def initSparkBase = {
    import scala.concurrent.ExecutionContext.Implicits.global
    ActorSystem("IndustryContactsScheduler").scheduler.schedule(FiniteDuration(0, TimeUnit.MINUTES), FiniteDuration(Props.get("app.update.interval.minutes").toInt, TimeUnit.MINUTES))(updateSQLContext)
  }

/*  private def getUpperBound = {
    val maxCount = SQLContextSingleton.getInstance().read.format("jdbc").options(Map(
      "url" -> Props.get("jdbc_conn"),
      "dbtable" -> """(SELECT count(1) as totalAmount FROM business_card INNER JOIN user ON business_card.USER_ID = user.ID  INNER JOIN prestige ON prestige.USER_ID = business_card.USER_ID ) table_num_ic""",
      "driver" -> Props.get("jdbc_driver")
    )).load().select("totalAmount").map(_(0).toString).collect()(0)
    maxCount.toLong.toString
  }*/
}
