package com.zjlp.face.spark.test

import java.util

import com.zjlp.face.spark.base.factory.SparkBaseFactoryImpl
import com.zjlp.face.spark.service.impl.BusinessCircleCacheImpl
import org.apache.spark.{SparkConf, SparkContext}


object Test {
  val bc = new BusinessCircleCacheImpl()

  def testSearchCommonFriendNum() = {

   val userNames = new java.util.ArrayList[String]()
    userNames.add("13000000000")
    userNames.add("13000000001")
    userNames.add("13000000003")
    userNames.add("12400004444")
    userNames.add("12400007777")
    userNames.add("13000000000")

    val loginAccount = "13000000005"
    val list = bc.searchCommonFriendNum(userNames,loginAccount)
    println(list)
  }

  def testSearchPersonRelation() = {
    val userIds = new util.ArrayList[String]()

    userIds.add("1085")
    val list1 = bc.searchPersonRelation(userIds,"13000000005")

    println(list1)
  }


  def main(args: Array[String]) {
    val f = new SparkBaseFactoryImpl
    f.initSparkBase
    //f.updateSQLContext

    bc.setSparkBaseFactory(f)
    //bc.updateDBSources


    //bc.registerMyFriendsTempTableIfNotExist("13000000005")

    val beginTime = System.currentTimeMillis()

    println("################## 业务代码 开始计时###################")
    testSearchCommonFriendNum

    //[(13000000000,3), (13000000003,1)]
    //testSearchPersonRelation

    println("################### 业务代码 总耗时：" + ((System.currentTimeMillis() - beginTime) / 1000D + "秒"))
   /* while (true) {

    }*/
  }
}
