package com.zjlp.face.spark.service.impl

import java.lang
import com.zjlp.face.spark.bean.{IndustryConnectionsResult, IndustryConnectionsDto}
import com.zjlp.face.spark.util.Utils
import org.apache.spark.Logging
import org.springframework.stereotype.Service
import com.zjlp.face.spark.base.SQLContextSingleton
import com.zjlp.face.spark.service.IIndustryContacts

object IndustryContactsImpl extends scala.Serializable {
  implicit val keyOrdering = new Ordering[(Long, String, String, Int, String, Int, String, String, String, String, String, Int, Int, Int, String)] {
    override def compare(x: (Long, String, String, Int, String, Int, String, String, String, String, String, Int, Int, Int, String), y: (Long, String, String, Int, String, Int, String, String, String, String, String, Int, Int, Int, String)): Int = {
      if (x._13 > y._13) 1
      else if (x._13 < y._13) -1
      else {
        if (x._14 > y._14) 1
        else if (x._14 < y._14) -1
        else {
          if (x._6 > y._6) 1
          else if (x._6 < y._6) -1
          else {
            if (x._15 <= y._15) 1
            else -1
          }
        }
      }
    }
  }

  implicit val keyOrdering2 = new Ordering[(Long, String, String, Int, String, Int, String, String, String, String, String,  Int, String)] {
    override def compare(x: (Long, String, String, Int, String, Int, String, String, String, String, String, Int, String), y: (Long, String, String, Int, String, Int, String, String, String, String, String, Int, String)): Int = {
      if(x._6 > y._6) {
        1
      } else if (x._6 < y._6) {
        -1
      } else {
        if(x._13 < y._13) {
          1
        } else {
          -1
        }
      }
    }
  }
}

@Service(value = "IndustryContactsImpl")
class IndustryContactsImpl extends IIndustryContacts with Logging with scala.Serializable {
  override def getContacts(userId: lang.Long, prestigeAmount: Int, areaCodes: Array[Int], industryCodes: Array[Int], pageNo: Int, pageSize: Int): IndustryConnectionsResult = {
    val beginTime = System.currentTimeMillis()
    val industryContactsTable = Utils.getLastTable("IndustryContacts_")

    var query = s"select * from $industryContactsTable where userID != $userId "
    if(industryCodes.length > 0) {
      query += s" and industryCode in ('${industryCodes.mkString("','")}') "
    }
    if(areaCodes.length > 0) {
      query += s" and areaCode in ('${areaCodes.mkString("','")}') "
    }
    query += s" and prestigeAmount > $prestigeAmount "

    val usernames = SQLContextSingleton.getInstance()
      .sql(s"SELECT username FROM ${Utils.getLastTable("ofRoster_")} WHERE userID = $userId").map(_ (0).toString).collect()
    if (usernames.length > 0) {
      query = query + s" and loginAccount not in ('${usernames.mkString("','")}') "
    }
    logInfo(query)
    val allData = SQLContextSingleton.getInstance().sql(query)
      .map(a => (a(0).toString.toLong, a(1).toString, Option(a(2)).getOrElse("").toString, a(3).toString.toInt, Option(a(4)).getOrElse("").toString,
        a(5).toString.toInt, Option(a(6)).getOrElse("").toString, Option(a(7)).getOrElse("").toString, Option(a(8)).getOrElse("").toString,
        Option(a(9)).getOrElse("").toString, Option(a(10)).getOrElse("").toString,
        a(11).toString.toInt, Option(a(13)).getOrElse("").toString)).persist()

    val totalCount = allData.count
    val topData = allData.top(pageNo * pageSize)(IndustryContactsImpl.keyOrdering2)
    allData.unpersist()

    val list = new java.util.ArrayList[IndustryConnectionsDto]()
    for (i <- (pageNo - 1) * pageSize until pageNo * pageSize) {
      if (i < totalCount) {
        val ic = new IndustryConnectionsDto()
        ic.setUserId(topData(i)._1)
        ic.setLoginAccount(topData(i)._2)
        ic.setHeadImgUrl(topData(i)._3)
        ic.setCertify(topData(i)._4)
        ic.setContacts(topData(i)._5)
        ic.setPrestigeAmount(topData(i)._6)
        ic.setPosition(topData(i)._7)
        ic.setCompanyName(topData(i)._8)
        ic.setvAreaName(topData(i)._9)
        ic.setIndustryProvide(topData(i)._10)
        ic.setIndustryRequirement(topData(i)._11)
        list.add(ic)
      }
    }
    val result = new IndustryConnectionsResult
    result.setCount(totalCount)
    result.setIndustryConnectionsList(list)
    logInfo(s"getContacts 耗时：${System.currentTimeMillis() - beginTime}ms")
    result
  }

  override def getContacts(userId: lang.Long, myIndustryCode: Int, areaCode: Int, industryCodes: Array[Int], pageNo: Int, pageSize: Int): IndustryConnectionsResult = {
    val beginTime = System.currentTimeMillis()

    val industryContactsTable = Utils.getLastTable("IndustryContacts_")
    //ofRoster是businessCircle中的临时表，因为是同时运行的，可以借用下
    val ofRosterTable = Utils.getLastTable("ofRoster_")

    var query =
      s"""select * from $industryContactsTable where industryCode in ('${industryCodes.mkString("','")}')
         | and userID != $userId
         """.stripMargin
    val usernames = SQLContextSingleton.getInstance()
      .sql(s"SELECT username FROM $ofRosterTable WHERE userID = $userId").map(_ (0).toString).collect()
    if (usernames.length > 0) {
      query = query + s" and loginAccount not in ('${usernames.mkString("','")}') "
    }

    logInfo(query)

    //|userID|loginAccount|headImgUrl|certify|nickname|prestigeAmount| position| companyName|areaName|industryProvide|industryRequirement|industryCode|
    val allData = SQLContextSingleton.getInstance().sql(query)
      .map(a => (a(0).toString.toLong, a(1).toString, Option(a(2)).getOrElse("").toString, a(3).toString.toInt, Option(a(4)).getOrElse("").toString,
        a(5).toString.toInt, Option(a(6)).getOrElse("").toString, Option(a(7)).getOrElse("").toString, Option(a(8)).getOrElse("").toString,
        Option(a(9)).getOrElse("").toString, Option(a(10)).getOrElse("").toString,
        a(11).toString.toInt, if (a(11).toString.toInt == myIndustryCode) 1 else 0,
        if (Option(a(12)).getOrElse("-1").toString.toInt == areaCode) 1 else 0, Option(a(13)).getOrElse("").toString)).persist()

    val totalCount = allData.count
    val topData = allData.top(pageNo * pageSize)(IndustryContactsImpl.keyOrdering)
    allData.unpersist()

    val list = new java.util.ArrayList[IndustryConnectionsDto]()
    for (i <- (pageNo - 1) * pageSize until pageNo * pageSize) {
      if (i < totalCount) {
        val ic = new IndustryConnectionsDto()
        ic.setUserId(topData(i)._1)
        ic.setLoginAccount(topData(i)._2)
        ic.setHeadImgUrl(topData(i)._3)
        ic.setCertify(topData(i)._4)
        ic.setContacts(topData(i)._5)
        ic.setPrestigeAmount(topData(i)._6)
        ic.setPosition(topData(i)._7)
        ic.setCompanyName(topData(i)._8)
        ic.setvAreaName(topData(i)._9)
        ic.setIndustryProvide(topData(i)._10)
        ic.setIndustryRequirement(topData(i)._11)
        list.add(ic)
      }
    }
    val result = new IndustryConnectionsResult
    result.setCount(totalCount)
    result.setIndustryConnectionsList(list)
    logInfo(s"getContacts 耗时：${System.currentTimeMillis() - beginTime}ms")
    result
  }
}
