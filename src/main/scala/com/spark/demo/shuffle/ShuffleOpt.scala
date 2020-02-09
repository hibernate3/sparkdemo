package com.spark.demo.shuffle

import org.apache.spark.api.java.function.PairFunction
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object ShuffleOpt {
  val debug: Boolean = false

  def main(args: Array[String]): Unit = {
    var sparkConf:SparkConf = null

    if (debug) {
      sparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    } else {
      sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
    }

    val sc = new SparkContext(sparkConf)

    var value:RDD[String] = null
    if (debug) {
      value = sc.textFile("/Users/wangyuhang/Desktop/words.txt")
    } else {
      value = sc.textFile("hdfs://10.101.71.41:8020/temp/words.txt")
    }

    //    execute0(value)

    val pairRdd: RDD[(String, Int)] = value.flatMap(_.split(" ")).map((_, 1))

//    execute1(pairRdd)
//    execute2(pairRdd)
  }

  /**
   * 未经优化的shuffle处理
   **/
  def execute0(value: RDD[String]): Unit = {
    val resultRdd: RDD[(String, Int)] = value.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    val result: Array[(String, Int)] = resultRdd.collect()
    result.foreach(println(_))
  }

  /**
   * 查看导致数据倾斜的key的数据分布情况
   **/
  def dataSample(pairs: RDD[(String, Int)]): Unit = {
    val sampledPairs = pairs.sample(false, 0.1)
    val sampledWordCounts = sampledPairs.countByKey()
    sampledWordCounts.foreach(println(_))
  }

  /**
   * 提高shuffle操作的并行度
   * */
  def execute1(pairs: RDD[(String, Int)]): Unit = {
    pairs.reduceByKey(_+_, 1000)
  }

  /**
   * 两阶段聚合（局部聚合+全局聚合）
   **/
  def execute2(pairs: RDD[(String, Int)]): Unit = {
    // 第一步，给RDD中的每个key都打上一个随机前缀
    val preKeyRdd: RDD[(String, Int)] = pairs.map(data => {
      val random = new Random()
      val prefix: Int = random.nextInt(10)
      (prefix + "_" + data._1, data._2)
    })

    // 第二步，对打上随机前缀的key进行局部聚合
    val preKeyResult: RDD[(String, Int)] = preKeyRdd.reduceByKey(_ + _)

    // 第三步，去除RDD中每个key的随机前缀
    val tempResult: RDD[(String, Int)] = preKeyResult.map(data => {
      (data._1.split("_")(1), data._2)
    })

    // 第四步，对去除了随机前缀的RDD进行全局聚合
    val finalResult: RDD[(String, Int)] = tempResult.reduceByKey(_ + _)

    finalResult.collect().foreach(println)
  }
}
