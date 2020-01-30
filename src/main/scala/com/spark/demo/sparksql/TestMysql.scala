package com.spark.demo.sparksql

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object TestMysql {
  def main(args: Array[String]): Unit = {
    val context = SparkSession.builder().master("local[*]").appName(this.getClass.getSimpleName).getOrCreate()

    val url = "jdbc:mysql://10.101.71.42:3306/eda_sdk_introduce?useUnicode=true&characterEncoding=utf-8&useSSL=false"
    val table = "sdk_type"
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "hd123456")

    val df = context.read.jdbc(url, table, properties)
    df.createOrReplaceTempView("sdk_type")
    context.sql("select * from sdk_type").show()
  }
}
