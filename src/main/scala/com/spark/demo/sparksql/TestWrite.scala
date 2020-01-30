package com.spark.demo.sparksql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object TestWrite {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestWrite").setMaster("local")
    val sc = new SparkContext(conf)

    val context = SparkSession.builder().getOrCreate()

    val df = context.read.json("file:///Users/wangyuhang/Desktop/people.json")
    df.write.mode(SaveMode.Ignore).json("file:///Users/wangyuhang/Desktop/people_out")
  }
}
