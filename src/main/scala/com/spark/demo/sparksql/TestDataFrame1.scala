package com.spark.demo.sparksql

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

case class People(var name:String, var age:Int)
object TestDataFrame1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestDataFrame1").setMaster("local")
    val sc = new SparkContext(conf)
    val context = SparkSession.builder().getOrCreate()


    val peopleRDD = sc.textFile("file:///Users/wangyuhang/Desktop/people.txt").map(line => People(line.split(",")(0), line.split(",")(1).trim.toInt))
    import context.implicits._

    val df = peopleRDD.toDF
    df.createOrReplaceTempView("people")
    context.sql("select * from people").show()
  }
}
