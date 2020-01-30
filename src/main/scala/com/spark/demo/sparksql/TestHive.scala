package com.spark.demo.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object TestHive {
  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)
//    val sc = new SparkContext(conf)
//    val sqlContext = new HiveContext(sc)
//    sqlContext.sql("select * from buried_point.dwd_users limit 1").show()

    val sqlContext = SparkSession.builder().master("local").appName(this.getClass.getSimpleName).enableHiveSupport().getOrCreate()
    sqlContext.sparkContext.setLogLevel("WARN")
    sqlContext.sql("select * from buried_point.dwd_users limit 1").show()
  }
}
