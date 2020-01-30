package com.spark.demo.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKeyDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("WARN")

    val kvRdd: RDD[(String, Int)] = sparkContext.makeRDD(List(("a", 1), ("a", 4), ("b", 3), ("b", 5), ("b", 6), ("c", 2), ("c", 8), ("c", 4)), 2)
    val partitionInfo: Array[Array[(String, Int)]] = kvRdd.glom().collect()
    partitionInfo.foreach(datas=>{
      datas.foreach(println)
      println("================")
    })

    val resultRdd: RDD[(String, Int)] = kvRdd.aggregateByKey(0)(math.max(_, _), (_ + _))
    resultRdd.foreach(println)
  }
}
