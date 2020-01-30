package com.spark.demo.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object PartitionByDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("WARN")

    val kvRdd: RDD[(String, Int)] = sparkContext.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("d", 4)))
    val resultRdd: RDD[(String, Int)] = kvRdd.partitionBy(new MyPartitioner(4))
    resultRdd.saveAsTextFile("/Users/wangyuhang/Desktop/output")
  }
}

class MyPartitioner(partitions: Int) extends Partitioner{
  override def numPartitions: Int = {
    partitions
  }

  override def getPartition(key: Any): Int = {
    if(key == "a"){
      1
    } else {
      2
    }
  }
}
