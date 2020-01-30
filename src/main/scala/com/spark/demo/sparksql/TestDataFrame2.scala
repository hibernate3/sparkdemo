package com.spark.demo.sparksql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object TestDataFrame2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestDataFrame2").setMaster("local")
    val sc = new SparkContext(conf)
    val context = SparkSession.builder().getOrCreate()
    import context.implicits._
    val df = context.read.json("file:///Users/wangyuhang/Desktop/people.json")
    df.createOrReplaceTempView("people")
    context.sql("select * from people").show()
  }
}
