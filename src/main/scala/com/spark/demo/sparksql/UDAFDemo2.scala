package com.spark.demo.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

object UDAFDemo2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder.master("local").appName(this.getClass.getSimpleName).getOrCreate
    import spark.implicits._

    val function: AgeAvgClassFunction = new AgeAvgClassFunction

    //将聚合函数转换为查询列
    val avgCol: TypedColumn[PersonBean, Double] = function.toColumn.name("age_avg")

    val listRdd: RDD[(String, Int)] = spark.sparkContext.makeRDD(List(("Steven", 30), ("Doris", 30), ("Davis", 28), ("James", 34), ("Howard", 33)))
    val df: DataFrame = listRdd.toDF("name", "age")
    val ds: Dataset[PersonBean] = df.as[PersonBean]

    ds.select(avgCol).show()
  }
}

case class PersonBean(name: String, age: Int)

case class AvgBuffer(var sum: Int, var count: Int)

class AgeAvgClassFunction extends Aggregator[PersonBean, AvgBuffer, Double] {
  //初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0)
  }

  //聚合数据
  override def reduce(b: AvgBuffer, a: PersonBean): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  //缓冲区的合并操作
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  //完成计算
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
