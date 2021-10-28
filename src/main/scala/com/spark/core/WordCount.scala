package com.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val sparkConfig = new SparkConf().setMaster("local").setAppName("WordCount")
    val sparkContext = new SparkContext(sparkConfig)

    val lines = sparkContext.textFile("src/main/resources/data")
    lines.flatMap(_.split(" "))
      .map(e => (e, 1))
      .groupBy(x=>x._1)
      .map {
        case (_,list) =>
          list.reduce(
            (e1, e2) => {
              (e1._1, e1._2 + e2._2)
            }
          )
      }
      .collect().foreach(println)

    sparkContext.stop()
  }

}