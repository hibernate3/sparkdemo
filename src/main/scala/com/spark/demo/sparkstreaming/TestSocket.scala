package com.spark.demo.sparkstreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object TestSocket {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val streamContext = new StreamingContext(sc, Seconds(2))

    val inDStream: ReceiverInputDStream[String] = streamContext.socketTextStream("bigdata-dev-42", 9998)
    inDStream.print()

    val resultDStream: DStream[(String, Int)] = inDStream.flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _)
    resultDStream.print()

    streamContext.start()
    streamContext.awaitTermination()
    streamContext.stop()
  }
}
