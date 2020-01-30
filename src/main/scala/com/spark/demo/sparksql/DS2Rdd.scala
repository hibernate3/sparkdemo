package com.spark.demo.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object DS2Rdd {
  //请不要将case Class定义在main 方法中与toDS一起使用
  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder.master("local").appName(this.getClass.getSimpleName).getOrCreate
    import spark.implicits._
    val sparkContext = spark.sparkContext
    sparkContext.setLogLevel("WARN")

    val listRdd: RDD[(String, Int)] = sparkContext.makeRDD(List(("Steven", 30), ("Doris", 30), ("Dora", 28), ("Lucy", 25)))

    val mapRdd: RDD[Person] = listRdd.map(data => {
      Person(data._1, data._2)
    })

    val ds: Dataset[Person] = mapRdd.toDS()
    ds.show()

    ds.foreach(line=>{
      val name: String = line.name
      val age: Int = line.age
      print("name: " + name + ", age: " + age + "\n")
    })

    //DataSet转Rdd
    val rdd: RDD[Person] = ds.rdd

    spark.stop()
  }
}
