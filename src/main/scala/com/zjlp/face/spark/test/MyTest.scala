package com.zjlp.face.spark.test

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 6/8/16.
 */
object MyTest {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array(
      (1, "A"),
      (2, "b"),
      (3, "c"),
      (4, "d"),
      (5, "e"),
      (6, "f"),
      (7, "g"),
      (8, "h"),
      (8, "c")
    ), 3)

    val rdd2 = sc.parallelize(Array(
      (3, "c1"),
      (4, "d1"),
      (5, "e1"),
      (6, "f1"),
      (7, "g1"),
      (8, "h1"),
      (8, "c1"),
      (9, "g1"),
      (9, "v1")
    ))
    rdd1.cogroup(rdd2,rdd2,rdd2).collect.foreach(println)
  }

}
