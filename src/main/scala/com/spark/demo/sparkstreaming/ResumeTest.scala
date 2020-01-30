package com.spark.demo.sparkstreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object ResumeTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val streamContext = new StreamingContext(sc, Seconds(2))
    streamContext.checkpoint("hdfs://bigdata-dev-41:8020/temp/test/SparkStreaming")

    val inDSream: ReceiverInputDStream[String] = streamContext.socketTextStream("bigdata-dev-42", 9998)

    val resultDStream:DStream[(String, Int)] = inDSream.flatMap(_.split(",")).map((_, 1)).updateStateByKey((values: Seq[Int], state: Option[Int]) => {
      val currentCount: Int = values.sum
      val lastCount: Int = state.getOrElse(0)
      Some(currentCount + lastCount)
    })

    resultDStream.print()

    streamContext.start()
    streamContext.awaitTermination()
    streamContext.stop()
  }
}
