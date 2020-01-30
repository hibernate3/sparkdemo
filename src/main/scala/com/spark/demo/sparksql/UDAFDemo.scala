package com.spark.demo.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object UDAFDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder.master("local").appName(this.getClass.getSimpleName).getOrCreate
    import spark.implicits._

    val function: AgeAvgFunction = new AgeAvgFunction

    spark.udf.register("age_avg", function)

    val listRdd: RDD[(String, Int)] = spark.sparkContext.makeRDD(List(("Steven", 30), ("Doris", 30), ("Davis", 28), ("James", 34), ("Howard", 33)))
    val df: DataFrame = listRdd.toDF("name", "age")

    df.createOrReplaceTempView("lakers")

    spark.sql("select age_avg(age) as age_avg from lakers").show()

    spark.stop()
  }
}

class AgeAvgFunction extends UserDefinedAggregateFunction{
  //函数输入的数据结构
  override def inputSchema: StructType = {
    new StructType().add("age", LongType)
  }

  //计算时的数据结构
  override def bufferSchema: StructType = {
    new StructType().add("sum", LongType).add("count", IntegerType)
  }

  //函数返回的数据类型
  override def dataType: DataType = DoubleType

  //函数是否稳定
  override def deterministic: Boolean = true

  //计算之前缓冲区的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //sum
    buffer(0) = 0L
    //count
    buffer(1) = 0
  }

  //根据查询结果更新缓冲区 数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getInt(1) + 1
  }

  //将多个节点的缓冲区合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //sum
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    //count
    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
  }

  //最终结果计算
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getInt(1)
  }
}
