package com.spark.demo.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object TestRead {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestRead").setMaster("local")
    val sc = new SparkContext(conf)

    val context = SparkSession.builder().getOrCreate()

    val df1= context.read.json("file:///Users/wangyuhang/Desktop/people.json")
//    val df2 = context.read.parquet("file:///Users/wangyuhang/Desktop/people.parquet")

    df1.show()

  }
}
