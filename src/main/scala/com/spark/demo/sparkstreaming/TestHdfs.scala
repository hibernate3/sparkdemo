package com.spark.demo.sparkstreaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}

object TestHdfs {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new StreamingContext(conf, Seconds(2))
    sc.sparkContext.setLogLevel("WARN")

    val inDStream: DStream[String] = sc.textFileStream("hdfs://bigdata-dev-41:8020/temp/test")
    val resultDStream: DStream[(String, Int)] = inDStream.flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _)
    resultDStream.print()

    sc.start()
    sc.awaitTermination()
    sc.stop()
  }
}
