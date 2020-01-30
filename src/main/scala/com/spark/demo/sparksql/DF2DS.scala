package com.spark.demo.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DF2DS {
  //请不要将case Class定义在main 方法中一起使用
  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder.master("local").appName(this.getClass.getSimpleName).getOrCreate
    import spark.implicits._
    val sparkContext = spark.sparkContext
    sparkContext.setLogLevel("WARN")

    val listRdd: RDD[(String, Int)] = sparkContext.makeRDD(List(("Steven", 30), ("Doris", 30), ("Dora", 28), ("Lucy", 25)))

    val df: DataFrame = listRdd.toDF("name", "age")
    val ds: Dataset[Person] = df.as[Person]

    //DataSet转DataFrame
    val lastDs: DataFrame = df.toDF()

    spark.stop()
  }
}
