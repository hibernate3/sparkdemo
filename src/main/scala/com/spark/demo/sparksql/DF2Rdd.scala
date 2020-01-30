package com.spark.demo.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DF2Rdd {

  //请不要将case Class定义在main 方法中与toDF一起使用
  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder.master("local").appName(this.getClass.getSimpleName).getOrCreate
    import spark.implicits._
    val sparkContext = spark.sparkContext
    sparkContext.setLogLevel("WARN")

    val listRdd: RDD[(String, Int)] = sparkContext.makeRDD(List(("Steven", 30), ("Doris", 30), ("Dora", 28), ("Lucy", 25)))

    //普通方式
//    val df: DataFrame = listRdd.toDF("name", "age")

    //样例类方式
    val mapRdd: RDD[Person] = listRdd.map(data => {
      Person(data._1, data._2)
    })
    val df: DataFrame = mapRdd.toDF()

    df.show

    df.foreach(line=>{
      val name = line.getAs[String](0)
      val age = line.getAs[Int](1)
      print("name: " + name + ", age: " + age + "\n")
    })

    //DataFrame转rdd
    val rowRdd: RDD[Row] = df.rdd

    spark.stop()
  }
}
