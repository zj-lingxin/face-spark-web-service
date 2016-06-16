package com.zjlp.face.spark.test

import java.util

import com.zjlp.face.spark.base.{Props, SQLContextSingleton}
import com.zjlp.face.spark.base.factory.SparkBaseFactoryImpl
import com.zjlp.face.spark.service.impl.BusinessCircleImpl

object Test {
  val bc = new BusinessCircleImpl()

  def testSearchCommonFriendNum() = {

    val userNames = new java.util.ArrayList[String]()
    userNames.add("13000000001")
    userNames.add("13000000003")
    userNames.add("13000000004")
    userNames.add("13000000005")
    userNames.add("13000000006")
    userNames.add("1300000xx06")
    val loginAccount = "13000000002"
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
    val list = bc.searchPersonRelation(userIds, "13000000005")
    println(list)
  }


  def main(args: Array[String]) {
    bc.setSparkBaseFactory(new SparkBaseFactoryImpl)
    bc.updateDBSources
    val beginTime = System.currentTimeMillis()
    println("################## 业务代码 开始计时###################")
    testSearchPersonRelation
    // testSearchCommonFriendNum
    println("################### 业务代码 总耗时：" + ((System.currentTimeMillis() - beginTime) / 1000D + "秒"))
    while (true) {

    }
  }
}
