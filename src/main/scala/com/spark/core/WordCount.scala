package com.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val sparkConfig = new SparkConf().setMaster("local").setAppName("WordCount")
    val sparkContext = new SparkContext(sparkConfig)

    val lines = sparkContext.textFile("src/main/resources/data")
    lines.flatMap(_.split(" "))
      .groupBy(x=>x)
      .map {
        case (k,v) => (k, v.size)
      }
      .collect().foreach(println)

    sparkContext.stop()
  }

}
