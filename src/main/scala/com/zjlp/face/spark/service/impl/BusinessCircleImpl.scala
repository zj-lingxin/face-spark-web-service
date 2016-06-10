package com.zjlp.face.spark.service.impl

import com.zjlp.face.spark.base.{ISparkBaseFactory}
import com.zjlp.face.spark.bean.{PersonRelation, CommonFriendNum}
import com.zjlp.face.spark.service.IBusinessCircle
import org.apache.spark.Logging

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.springframework.stereotype.Service
import java.util
import javax.annotation.Resource
import collection.JavaConversions._
import scala.beans.BeanProperty
import scala.collection.mutable.ArrayBuffer
import com.zjlp.face.spark.util.Utils

@Service(value = "businessCircle")
class BusinessCircleImpl extends IBusinessCircle with Logging {
  @Resource
  @BeanProperty var sparkBaseFactory: ISparkBaseFactory = _

  /**
   * 一度好友查询
   * @param loginAccount 用户ID
   * @return 返回结果集
   */
  def searchFriendFrame(loginAccount: String): DataFrame = {
    sparkBaseFactory.getSQLContext
      .sql(s"select distinct loginAccount from ofRoster where username = '$loginAccount' and username <> loginAccount ")
  }

  /**
   * 二度好友查询
   * @param loginAccount 用户ID
   * @return 返回结果集
   */
  def searchTwoFriendFrame(loginAccount: String): DataFrame = {
    //我的所有的一度好友
    val myFriends = searchFriendFrame(loginAccount: String).map(_ (0).toString).collect()
    sparkBaseFactory.getSQLContext.sql(s"select distinct loginAccount from ofRoster where username in ('${myFriends.mkString("','")}')")
  }


  /**
   * 更新数据源 定时任务调取
   * @return 返回执行状态
   */
  def updateDBSources: java.lang.Boolean = {
    sparkBaseFactory.updateSQLContext
    true
  }

  /**
   * 根据当前登录用户id和用户id列表查询共同好友数
   *
   * @param userNames 附近店铺用户集
   * @param loginAccount 登入账号(username)
   * @return 返回结果集
   */
  def searchCommonFriendNum(userNames: util.List[String], loginAccount: String): util.List[CommonFriendNum] = {

    val beginTime = System.currentTimeMillis()
    val sqlContext: SQLContext = sparkBaseFactory.getSQLContext
    //得到userNames的朋友 (朋友，username)
    val othersFriends: RDD[(String, String)] = sqlContext.sql(s"select username,loginAccount from ofRoster where username in ('${userNames.mkString("','")}') and loginAccount != '$loginAccount' and username != loginAccount")
      .map(a => (a(0).toString, a(1).toString))

    //得到loginAccount的朋友
    val myFriends = sqlContext.sql(s"select loginAccount from ofRoster where username = '${loginAccount}'").map(_ (0).toString).collect()

    //该步骤会计算出共同好友的人数，但是如果共同好友人数为0，则username会被过滤掉
    val comFriendsCount = othersFriends.aggregateByKey(new ArrayBuffer[String](), 48)((acc: ArrayBuffer[String], value: String) => acc += value, (acc1, acc2) => acc1 ++ acc2)
      .mapValues(a => a.distinct.intersect(myFriends).size)

    val resultArray = sqlContext.sparkContext.makeRDD(userNames).map((_, "")).leftOuterJoin(comFriendsCount)
      .map(a => new CommonFriendNum(a._1, a._2._2.getOrElse(0))).collect()

    val processTime = (System.currentTimeMillis() - beginTime).toDouble / 1000
    logInfo(s"searchCommonFriendNum耗時:$processTime s")

    Utils.itrToJavaList(resultArray.iterator)
  }

  /**
   * 根据当前登录用户id和用户id列表返回人脉关系类型列表
   * @param userIds 用户集
   * @param loginAccount 登入账号
   * @return
   */
  /*
     def searchPersonRelation(userIds: util.List[String], loginAccount: String): util.List[PersonRelation] = {
      val beginTime = System.currentTimeMillis()
      val sqlContext: SQLContext = sparkBaseFactory.getSQLContext
      val userIdsLoginAccount = sqlContext.sql(s"select distinct loginAccount,userID from ofRoster where userID in ('${userIds.mkString("','")}') and loginAccount != '$loginAccount'")
        .map(a => (a(0).toString, a(1).toString)).persist()

      val userIdsMap = userIdsLoginAccount.collectAsMap()

      val othersUserName: RDD[String] = userIdsLoginAccount.map(a => a._1).persist()

      //我的所有的一度好友
      val myFriends = searchFriendFrame(loginAccount: String).map(_(0).toString).collect()

      //是我的一度好友的用户集
      val othersBelongToOneLevelFriends = othersUserName.filter(a => myFriends.contains(a)).persist()

      //我的所有的二度好友
      val myTwoLevelFriends =
        sqlContext.sql(s"select distinct loginAccount from ofRoster where username in ('${myFriends.mkString("','")}')")
          .map(a => a(0).toString)

      //是我的二度好友的用户集
      val othersBelongToTwoLevelFriends = myTwoLevelFriends.intersection(othersUserName).subtract(othersBelongToOneLevelFriends).persist()
      val oneLevel = othersBelongToOneLevelFriends.collect()
      val twoLevel = othersBelongToTwoLevelFriends.collect()
      val stranger = othersUserName.collect().filter(a => !(oneLevel.contains(a) || twoLevel.contains(a)))

      val list = new util.ArrayList[PersonRelation]()
      oneLevel.foreach(a => list.add(new PersonRelation(userIdsMap(a), 1)))
      twoLevel.foreach(a => list.add(new PersonRelation(userIdsMap(a), 2)))
      stranger.foreach(a => list.add(new PersonRelation(userIdsMap(a), 3)))

      val processTime = (System.currentTimeMillis() - beginTime).toDouble / 1000
      logInfo(s"searchPersonRelation耗時:$processTime s")

      list
    }
   */

  override def searchPersonRelation(userIds: util.List[String], loginAccount: String): util.List[PersonRelation] = {
    val beginTime = System.currentTimeMillis()
    val sqlContext: SQLContext = sparkBaseFactory.getSQLContext

    sqlContext.sql(
      s""" select username,loginAccount,userID from ofRoster  where
         | loginAccount != '$loginAccount' and userID in ('${userIds.mkString("','")}') """.stripMargin).registerTempTable("table_a")

    val oneLevelFriends = sqlContext.sql(s"select distinct userID from table_a where username = '$loginAccount'")

    val las = sqlContext.sql(s"SELECT DISTINCT loginAccount as la FROM ofRoster WHERE username =  '$loginAccount' AND loginAccount !=  '$loginAccount'")
      .map(_ (0).toString).collect().mkString("','")

    val twoLevelFriends = sqlContext.sql(s" select DISTINCT userID FROM table_a where username != loginAccount and username in ('$las') and loginAccount NOT IN ('$las')")

    val oneLevelFriendsArray = oneLevelFriends.map(a => (a(0).toString)).collect()
    val twoLevelFriendsArray = twoLevelFriends.map(a => (a(0).toString)).collect()

    sqlContext.dropTempTable("table_a")

    val list = new util.ArrayList[PersonRelation]()
    val stranger = userIds.filter(a => !(oneLevelFriendsArray.contains(a) || twoLevelFriendsArray.contains(a)))

    oneLevelFriendsArray.foreach(a => list.add(new PersonRelation(a, 1)))
    twoLevelFriendsArray.foreach(a => list.add(new PersonRelation(a, 2)))
    stranger.foreach(a => list.add(new PersonRelation(a, 3)))

    val processTime = (System.currentTimeMillis() - beginTime).toDouble / 1000
    logInfo(s"searchPersonRelation耗時:$processTime s")
    list
  }
}
