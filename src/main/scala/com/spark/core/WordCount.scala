package com.spark.core

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object WordCount {

  def main(args: Array[String]): Unit = {
    val sparkConfig = new SparkConf().setMaster("local").setAppName("WordCount")
    val sparkContext = new SparkContext(sparkConfig)

    val lines = sparkContext.textFile("src/main/resources/data")

    val tupleRdd1 = sparkContext.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("c", 4), ("c", 5)))
    val tupleRdd2 = sparkContext.makeRDD(List(("a", 0), ("a", 6), ("b", 7), ("c", 8), ("c", 9)))

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
      .collect()
      .foreach(println)

    lines.flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .collect()
      .foreach(println)

    tupleRdd1.join(tupleRdd2)
      .collect()
      .foreach(println)

    tupleRdd1.cogroup(tupleRdd2)
      .map(
        s => {
          val list = mutable.ListBuffer[Int]()
          list ++= s._2._1
          list ++= s._2._2
          (s._1, list.toList)
        }
      )
      .collect()
      .foreach(println)

    sparkContext.stop()
  }

}