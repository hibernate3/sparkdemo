package com.spark.demo.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("wordcount")
    val sparkContext = new SparkContext(sparkConf)

    val file = sparkContext.textFile("hdfs://10.101.71.41:8020/temp/test.txt")
    val result = file.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect()
    result.foreach(println)
    sparkContext.stop()
  }
}
