package com.spark.demo.sparkstreaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder

object KafkaTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(5))

    ssc.checkpoint("hdfs://bigdata-dev-41:8020/temp/test/SparkStreaming")

    val topics = Set("my_test")
    val kafkaPrams = Map[String, String]("metadata.broker.list" -> "bigdata-dev-190:9092, bigdata-dev-191:9092, bigdata-dev-192:9092")
    val data = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaPrams, topics)

    val updateFunc = (curVal: Seq[Int], preVal: Option[Int]) => {
      val total = curVal.sum
      val previous = preVal.getOrElse(0)
      Some(total + previous)
    }

    val result = data.map(_._2).flatMap(_.split(" ")).map((_, 1)).updateStateByKey(updateFunc).print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
