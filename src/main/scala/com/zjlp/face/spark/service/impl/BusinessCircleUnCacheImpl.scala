package com.zjlp.face.spark.service.impl

import java.util
import javax.annotation.Resource

import com.zjlp.face.spark.base.{Constants, SQLContextSingleton, ISparkBaseFactory, Props}
import com.zjlp.face.spark.bean.{CommonFriendNum, PersonRelation}
import com.zjlp.face.spark.service.IBusinessCircle
import com.zjlp.face.spark.util.Utils
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.springframework.stereotype.Service

import scala.beans.BeanProperty
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

@Service(value = "businessCircleUnCacheImpl")
class BusinessCircleUnCacheImpl extends IBusinessCircle with Logging {
  private val paramIsShow = Props.get("app.param.show").toBoolean
  @Resource
  @BeanProperty var sparkBaseFactory: ISparkBaseFactory = _

  /**
   * 一度好友查询
   * @param loginAccount 用户ID
   * @return 返回结果集
   */
  def searchFriendFrame(loginAccount: String): DataFrame = {
    val sqlContext = sparkBaseFactory.getSQLContext
    val ofRoster = getNewOfRoster()
    sqlContext.sql(s"select distinct loginAccount from $ofRoster where username = '$loginAccount' and username <> loginAccount ")
  }

  /**
   * 二度好友查询
   * @param loginAccount 用户ID
   * @return 返回结果集
   */
  def searchTwoFriendFrame(loginAccount: String): DataFrame = {
    val sqlContext = sparkBaseFactory.getSQLContext
    val ofRoster = getNewOfRoster()
    //我的所有的一度好友
    val myFriends = searchFriendFrame(loginAccount: String).map(_ (0).toString).collect()
    sqlContext.sql(s"select distinct loginAccount from $ofRoster where username in ('${myFriends.mkString("','")}')")
  }

  def getNewOfRoster(): String = {
    val sqlContext = sparkBaseFactory.getSQLContext
    sqlContext.tableNames().filter(_.startsWith("ofRoster_")).sorted.reverse(0)
  }

  /**
   * 根据当前登录用户id和用户id列表查询共同好友数
   *
   * @param userNames 附近店铺用户集
   * @param loginAccount 登入账号(username)
   * @return 返回结果集
   */
  def searchCommonFriendNum(userNames: util.List[String],loginAccount: String): util.List[CommonFriendNum] = {
    val sqlContext = sparkBaseFactory.getSQLContext
    if (paramIsShow) logInfo(s"searchCommonFriendNum传入参数 loginAccount:$loginAccount; userNames:s$userNames")
    val beginTime = System.currentTimeMillis()
    val ofRoster = getNewOfRoster()

    //得到userNames的朋友 (朋友，username)
    val othersFriends: RDD[(String, String)] = sqlContext.sql(
      s"""select username,loginAccount from $ofRoster where username in
         | ('${userNames.mkString("','")}') and loginAccount != '$loginAccount' and username != loginAccount""".stripMargin)
      .map(a => (a(0).toString, a(1).toString))

    //得到loginAccount的朋友
    val myFriends = sqlContext.sql(s"select loginAccount from $ofRoster where username = '$loginAccount'").map(_ (0).toString).collect()

    //该步骤会计算出共同好友的人数，但是如果共同好友人数为0，则username会被过滤掉
    val comFriendsMap = othersFriends.aggregateByKey(new ArrayBuffer[String](), Props.get("spark.default.parallelism").toString.toInt)((acc: ArrayBuffer[String], value: String) => acc += value, (acc1, acc2) => acc1 ++ acc2)
      .mapValues(a => a.distinct.intersect(myFriends).size).collectAsMap()

    val resultList = new util.ArrayList[CommonFriendNum]()

    userNames.foreach { username =>
      val num = if (comFriendsMap.contains(username)) comFriendsMap(username) else 0
      resultList.add(new CommonFriendNum(username, num))
    }

    logInfo(s"loginAccount:$loginAccount; ofRoster:$ofRoster; searchCommonFriendNum耗時:${(System.currentTimeMillis() - beginTime) / 1000D} s")
    if (paramIsShow) logInfo(s"searchCommonFriendNum结果:loginAccount:$loginAccount; list:$resultList")

    resultList
  }


  /**
   * 根据当前登录用户id和用户id列表返回人脉关系类型列表
   * @param userIds 用户集
   * @param loginAccount 登入账号
   * @return
   */
  override def searchPersonRelation(userIds: util.List[String],loginAccount: String): util.List[PersonRelation] = {
    if (paramIsShow) logInfo(s"searchPersonRelation传入参数:loginAccount:$loginAccount; userIds:s$userIds")
    val sqlContext = sparkBaseFactory.getSQLContext
    val beginTime = System.currentTimeMillis()
    val ofRoster = getNewOfRoster()

    registerMyFriendsTempTableIfNotExist(ofRoster, loginAccount)

    val result = sqlContext.sql(
      s"""select userID, friendType from ${Constants.relationsTable}_$loginAccount
         | where userID in ('${userIds.mkString("','")}') """.stripMargin)
      .map(a => new PersonRelation(a(0).toString, a(1).toString.toInt)).collect()

    logInfo(s"loginAccount:$loginAccount; searchPersonRelation耗時:${(System.currentTimeMillis() - beginTime) / 1000D} s")
    val finalResult = Utils.itrToJavaList(result.iterator)
    if (paramIsShow) logInfo(s"searchPersonRelation结果:loginAccount:$loginAccount; list:$finalResult")
    finalResult

  }

  private def registerMyFriendsTempTableIfNotExist(ofRoster:String, loginAccount: String) = {
    val sqlContext: SQLContext = sparkBaseFactory.getSQLContext
    if (!sqlContext.tableNames.contains(s"${Constants.relationsTable}_$loginAccount")) {
      val oneLevelFriends = sqlContext.sql(
          s""" select distinct userID FROM $ofRoster where username = '$loginAccount'
             | and loginAccount != '$loginAccount' and userID is not null""".stripMargin)
        .map(a => a(0).toString).persist()

      val las = sqlContext.sql(
        s""" select distinct loginAccount from $ofRoster
           | where username =  '$loginAccount' and loginAccount !=  '$loginAccount'""".stripMargin)
        .map(_ (0).toString).collect().mkString("','")

      val twoLevelFriends =
        sqlContext.sql(
          s""" select distinct userID from $ofRoster where username != loginAccount and loginAccount != '$loginAccount'
             | and username in ('$las') and loginAccount not in ('$las') and userID is not null """.stripMargin)
          .map(a => a(0).toString).subtract(oneLevelFriends).map(a => (a, 2))

      import sqlContext.implicits._
      val tb = oneLevelFriends.map(a => (a, 1)).union(twoLevelFriends)
        .toDF("userID", "friendType")

      //再次确认是否存在该临时表
      if(!sqlContext.tableNames.contains(s"${Constants.relationsTable}_$loginAccount")){
        tb.registerTempTable(s"${Constants.relationsTable}_$loginAccount")
        sqlContext.sql(s"cache table ${Constants.relationsTable}_$loginAccount")
        logInfo(s"缓存临时表:${Constants.relationsTable}_$loginAccount")
      }
      oneLevelFriends.unpersist()
    }
  }

}
