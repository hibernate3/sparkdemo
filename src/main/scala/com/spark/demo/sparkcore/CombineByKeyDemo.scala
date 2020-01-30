package com.spark.demo.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CombineByKeyDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("WARN")

    val kvRdd: RDD[(String, Int)] = sparkContext.makeRDD(List(("a", 1), ("a", 4), ("b", 3), ("b", 5), ("b", 6), ("c", 2), ("c", 8), ("c", 4)), 2)

    val combineRdd: RDD[(String, (Int, Int))] = kvRdd.combineByKey((_, 1), (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

    val resultRdd: RDD[(String, Double)] = combineRdd.map {
      case (key, value) => (
        key, value._1 / value._2.toDouble
      )
    }

    resultRdd.collect.foreach(println)
  }
}
