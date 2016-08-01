package com.zjlp.face.spark.base

import com.zjlp.face.spark.base.factory.SparkBaseFactoryImpl
import com.zjlp.face.spark.bean.{PersonRelation, CommonFriendNum}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

object SQLContextSingleton {
  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext = SparkContextSingleton.getInstance): SQLContext = {
    synchronized {
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
    }
    instance
  }
}

object SparkContextSingleton extends Logging {
  private var instance: SparkContext = _

  private def showSparkConf() = {
    instance.getConf.getAll.foreach { prop =>
      logInfo(prop.toString())
    }
  }

  private def getSparkConf = {
    val conf = new SparkConf()

    Array(
      "spark.master",
      "spark.app.name",
      "spark.sql.shuffle.partitions",
      "spark.executor.memory",
      "spark.executor.cores",
      "spark.speculation",
      "spark.driver.memory",
      "spark.driver.cores",
      "spark.default.parallelism",
      "spark.home",
      "spark.jars",
      "spark.scheduler.mode"
    ).foreach { prop =>
      conf.set(prop, Props.get(prop))
    }

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(
      classOf[CommonFriendNum],
      classOf[PersonRelation],
      classOf[SparkBaseFactoryImpl]
    ))

    conf
  }

  def getInstance = {
    synchronized{
      if (instance == null) {
        instance = new SparkContext(getSparkConf)
        showSparkConf()
      }
    }

    instance
  }
}
