package com.spark.demo.sparksql

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object TestJson {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder.master("local").appName(this.getClass.getSimpleName).getOrCreate

    val df: DataFrame = spark.read.json("file:///Users/wangyuhang/Desktop/people.json")
    df.show()

    df.write.format("json").mode(SaveMode.Append).save("file:///Users/wangyuhang/Desktop/output")
  }
}
