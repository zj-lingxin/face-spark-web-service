package com.zjlp.face.spark.base

import org.apache.spark.Logging
import org.apache.spark.sql.{SQLContext, DataFrame}

object JdbcDF {
  def load(dbtable: String, sqlContext: SQLContext = SQLContextSingleton.getInstance()): DataFrame = {
    sqlContext.read.format("jdbc").options(Map(
      "url" -> Props.get("jdbc_conn"),
      "dbtable" -> dbtable,
      "driver" -> Props.get("jdbc_driver")
    )).load()
  }
}

object CsvDF extends Logging {
  def load(path: String): DataFrame = {
    SQLContextSingleton.getInstance().read.format("com.databricks.spark.csv")
      .option("header", "true").load(path)
  }
}
