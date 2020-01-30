package com.spark.demo.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RddDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("WARN")

    val listRdd = sparkContext.parallelize(1 to 10, 3)
//    val mapRdd = listRdd.map(_*2)

//    val mapRdd = listRdd.mapPartitions(datas=>{
//      datas.map(_*2)
//    })

//    val mapRdd = listRdd.mapPartitionsWithIndex {
//      case (num, datas) => {
//        datas.map((_, "partition: " + num))
//      }
//    }

//    mapRdd.collect().foreach(println)

//    listRdd.saveAsTextFile("output")

//    var glomRdd: RDD[Array[Int]] = listRdd.glom()
//
//    glomRdd.collect.foreach(array=>{
//      println(array.mkString(","))
//    })

//    val groupByRdd: RDD[(Int, Iterable[Int])] = listRdd.groupBy(data => {
//      data % 2
//    })

//    val filterRdd: RDD[Int] = listRdd.filter(data => {
//      data % 2 == 0
//    })

    val sampleRdd: RDD[Int] = listRdd.sample(false, 0.5, 1)

    sampleRdd.collect.foreach(println)
  }
}
