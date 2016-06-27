package com.zjlp.face.spark.service.impl

import java.util
import javax.annotation.Resource

import com.zjlp.face.spark.base.{SQLContextSingleton, ISparkBaseFactory, Props}
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

@Service(value = "businessCircleCacheImpl")
class BusinessCircleCacheImpl extends IBusinessCircle with Logging {
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
    //我的所有的一度好友
    val myFriends = searchFriendFrame(loginAccount: String).map(_ (0).toString).collect()
    val ofRoster = getNewOfRoster()
    sqlContext.sql(s"select distinct loginAccount from $ofRoster where username in ('${myFriends.mkString("','")}')")
  }

  /**
   * 根据当前登录用户id和用户id列表查询共同好友数
   *
   * @param userNames 附近店铺用户集 userNames只可能是一度和二度好友
   * @param loginAccount 登入账号(username)
   * @return 返回结果集
   */
  def searchCommonFriendNum(userNames: util.List[String], loginAccount: String): util.List[CommonFriendNum] = {
    val sqlContext = sparkBaseFactory.getSQLContext
    if (paramIsShow) logInfo(s"searchCommonFriendNum传入参数 loginAccount:$loginAccount; userNames:s$userNames")
    val beginTime = System.currentTimeMillis()
    val ofRoster = getNewOfRoster()
    registerMyFriendsTempTableIfNotExist(ofRoster, loginAccount)

    val result = sqlContext.sql(
      s"""select loginAccount, comFriendsNum from my_friends_$loginAccount
         | where loginAccount in ('${userNames.mkString("','")}') """.stripMargin)
      .map(a => new CommonFriendNum(a(0).toString, a(1).toString.toInt)).collect()

    val finalResult = Utils.itrToJavaList(result.iterator)
    logInfo(s"loginAccount:$loginAccount; ofRoster:$ofRoster; searchCommonFriendNum耗時:${(System.currentTimeMillis() - beginTime) / 1000D} s")
    if (paramIsShow) logInfo(s"searchCommonFriendNum结果:loginAccount:$loginAccount; list:$finalResult")
    finalResult
  }

  def getNewOfRoster(): String = {
    val sqlContext = sparkBaseFactory.getSQLContext
    sqlContext.tableNames().filter(_.startsWith("ofRoster_")).sorted.reverse(0)
  }

  /**
   * 根据当前登录用户id和用户id列表返回人脉关系类型列表
   * @param userIds 用户集
   * @param loginAccount 登入账号
   * @return
   */
  override def searchPersonRelation(userIds: util.List[String], loginAccount: String): util.List[PersonRelation] = {
    val sqlContext = sparkBaseFactory.getSQLContext
    if (paramIsShow) logInfo(s"searchPersonRelation传入参数:loginAccount:$loginAccount; userIds:s$userIds")
    val beginTime = System.currentTimeMillis()
    val ofRoster = getNewOfRoster()
    registerMyFriendsTempTableIfNotExist(ofRoster, loginAccount)

    val result = sqlContext.sql(
      s"""select userID, friendType from my_friends_$loginAccount
         | where userID in ('${userIds.mkString("','")}') """.stripMargin)
      .map(a => new PersonRelation(a(0).toString, a(1).toString.toInt)).collect()

    val finalResult = Utils.itrToJavaList(result.iterator)
    logInfo(s"loginAccount:$loginAccount; ofRoster:$ofRoster; searchPersonRelation耗時:${(System.currentTimeMillis() - beginTime) / 1000D} s")
    if (paramIsShow) logInfo(s"searchPersonRelation结果:loginAccount:$loginAccount; list:$finalResult")
    finalResult
  }

  private def getOneAndTwoLevelFriends(ofRoster: String, loginAccount: String) = {
    val sqlContext = sparkBaseFactory.getSQLContext
    val oneLevelFriends = sqlContext.sql(
      s""" select distinct userID,loginAccount FROM $ofRoster where username = '$loginAccount'
         | and loginAccount != '$loginAccount' and userID is not null""".stripMargin)
      .map(a => (a(0).toString, a(1).toString)).persist()
    val las = sqlContext.sql(
      s""" select distinct loginAccount from $ofRoster
         | where username =  '$loginAccount' and loginAccount !=  '$loginAccount'""".stripMargin)
      .map(_ (0).toString).collect().mkString("','")

    val twoLevelFriends =
      sqlContext.sql(
        s""" select distinct userID,loginAccount from $ofRoster where username != loginAccount and loginAccount != '$loginAccount'
           | and username in ('$las') and loginAccount not in ('$las') and userID is not null """.stripMargin)
        .map(a => (a(0).toString, a(1).toString)).subtract(oneLevelFriends).persist()
    (oneLevelFriends, twoLevelFriends)
  }



  private def getComFriendNumMap(ofRoster: String, loginAccount: String, oneLevelFriends: RDD[(String, String)], twoLevelFriends: RDD[(String, String)]) = {
    val sqlContext = sparkBaseFactory.getSQLContext
    //得到loginAccount的朋友
    val myFriends = sqlContext.sql(s"select loginAccount from $ofRoster where username = '$loginAccount'").map(_ (0).toString).collect()
    //得到一度和二度好友的朋友

    val twoLevelFriendsUserName = twoLevelFriends.map(_._2)
    val friends = oneLevelFriends.map(_._2).union(twoLevelFriendsUserName).collect()

    //得到userNames的朋友 (朋友，username)
    val othersFriends: RDD[(String, String)] = sqlContext.sql(
      s"""select username,loginAccount from $ofRoster where username in
         | ('${friends.mkString("','")}') and loginAccount != '$loginAccount' and username != loginAccount""".stripMargin)
      .map(a => (a(0).toString, a(1).toString))

    val comFriendsMap = othersFriends.aggregateByKey(new ArrayBuffer[String](), Props.get("spark.default.parallelism").toString.toInt)((acc: ArrayBuffer[String], value: String) => acc += value, (acc1, acc2) => acc1 ++ acc2)
      .mapValues(a => a.distinct.intersect(myFriends).size).collectAsMap()

    friends.map { username =>
      val num = if (comFriendsMap.contains(username)) comFriendsMap(username) else 0
      (username, num)
    }.toMap
  }

  def registerMyFriendsTempTableIfNotExist(ofRoster: String, loginAccount: String) = {
    val sqlContext = sparkBaseFactory.getSQLContext
    if (!sqlContext.tableNames().contains(s"my_friends_$loginAccount")) {

      val (oneLevelFriends, twoLevelFriends) = getOneAndTwoLevelFriends(ofRoster, loginAccount)
      val friendsNumMap = getComFriendNumMap(ofRoster, loginAccount, oneLevelFriends, twoLevelFriends)

      val friendsNumMapBC = sqlContext.sparkContext.broadcast(friendsNumMap)

      import sqlContext.implicits._
      val twoLevelFriendsInfo = twoLevelFriends.map(a => (a._1, a._2, 2, friendsNumMapBC.value(a._2)))
      val aa = oneLevelFriends.map(a => (a._1, a._2, 1, friendsNumMapBC.value(a._2))).union(twoLevelFriendsInfo)
        .toDF("userID", "loginAccount", "friendType", "comFriendsNum")

      //再次确认该表是否存在，否则可能导致内存中表的重复
      if (!sqlContext.tableNames().contains(s"my_friends_$loginAccount")) {
        aa.registerTempTable(s"my_friends_$loginAccount")
        sqlContext.sql(s"cache table my_friends_$loginAccount")
        logInfo(s"缓存临时表:my_friends_$loginAccount")
      }

      oneLevelFriends.unpersist()
      twoLevelFriends.unpersist()

    }
  }

}
