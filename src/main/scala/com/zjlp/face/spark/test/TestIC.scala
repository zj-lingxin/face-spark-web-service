package com.zjlp.face.spark.test

import com.zjlp.face.spark.base.factory.{IndustryContactsFactoryImpl, SparkBaseFactoryImpl}
import com.zjlp.face.spark.service.IIndustryContacts
import com.zjlp.face.spark.service.impl.IndustryContactsImpl

/**
 * Created by root on 7/28/16.
 */
object TestIC {
  def main(args: Array[String]) {

   val f = new SparkBaseFactoryImpl
    f.updateSQLContext
    val icf = new IndustryContactsFactoryImpl
    icf.initSparkBase
    Thread.sleep(20000)
    test2
    println("finish")
  }

  def test1 = {
    val iic = new IndustryContactsImpl
    val userId = 143521L

    val areaCode = 330100
    val industryCode = Array(100001000, 100002000, 100003000, 100004000, 100005000, 100006000, 100007000, 100008000, 100009000, 100010000, 100011000, 100012000, 100013000)
    val result = iic.getContacts(userId,null,null,industryCode,1,100)
    println(result.getCount)
    println(result.getIndustryConnectionsList)
  }


  def test2 = {
    val iic = new IndustryContactsImpl
    //val userId = 143521L
    val userId = 1216L
    val areaCode = Array[Int]()
    val industryCode =Array(100001000, 100002000, 100003000, 100004000, 100005000, 100006000, 100007000, 100008000, 100009000, 100010000, 100011000, 100012000, 100013000)
    val prestigeAmount =  10

    val result = iic.getContacts(userId,prestigeAmount,new Array[Int](0),new Array[Int](0),1,70)

   // println(result.getCount)
   // println(result.getIndustryConnectionsList)
  }
}
