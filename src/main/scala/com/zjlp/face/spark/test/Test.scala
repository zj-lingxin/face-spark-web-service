package com.zjlp.face.spark.test

import java.util

import com.zjlp.face.spark.base.factory.SparkBaseFactoryImpl
import com.zjlp.face.spark.service.impl.{BusinessCircleUnCacheImpl, BusinessCircleCacheImpl}


object Test {
  val bc = new BusinessCircleUnCacheImpl()

  def testSearchCommonFriendNum() = {

    val userNames = new java.util.ArrayList[String]()
    userNames.add("13000000001")
    userNames.add("13000000003")
    userNames.add("13000000004")
    userNames.add("13000000002")
    userNames.add("13000000006")
    userNames.add("1300000xx06")
    val loginAccount = "13000000005"
    val list = bc.searchCommonFriendNum(userNames, loginAccount)
    println(list)
  }

  def testSearchPersonRelation() = {
    val userIds = new util.ArrayList[String]()
    userIds.add("3184")
    userIds.add("3186")
    userIds.add("3322")
    userIds.add("3183")
    userIds.add("2807")
    userIds.add("1080")
    val list1 = bc.searchPersonRelation(userIds, "13000000005")

    println(list1)
  }


  def main(args: Array[String]) {
    val f = new SparkBaseFactoryImpl
    f.updateSQLContext

    bc.setSparkBaseFactory(f)
    //bc.updateDBSources


    //bc.registerMyFriendsTempTableIfNotExist("13000000005")

    val beginTime = System.currentTimeMillis()

    println("################## 业务代码 开始计时###################")
    testSearchCommonFriendNum

    testSearchPersonRelation

    println("################### 业务代码 总耗时：" + ((System.currentTimeMillis() - beginTime) / 1000D + "秒"))
    while (true) {

    }
  }
}
