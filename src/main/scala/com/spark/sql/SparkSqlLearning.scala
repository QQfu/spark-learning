package com.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions

object SparkSqlLearning {

  def main(args: Array[String]): Unit = {

    val sparkConfig = new SparkConf().setMaster("local").setAppName("SqlLearning")
    val sparkSession = SparkSession.builder().config(sparkConfig).getOrCreate()

    val df = sparkSession.read.json("src/main/resources/data/test1.json")

    df.show

    import sparkSession.implicits._
    val ds = df.as[User]

    ds.show

    ds.createOrReplaceTempView("user")

    sparkSession.udf.register("avgAge", functions.udaf(new AvgAgeTest))

    sparkSession.sql("select avgAge(age) from user").show

    sparkSession.close()

  }
}

case class Buffer(var total:Long, var count:Int)

class AvgAgeTest extends Aggregator[Long, Buffer, Double] {

  //初始化
  override def zero: Buffer = {
    Buffer(0L, 0)
  }

  //聚合
  override def reduce(b: Buffer, a: Long): Buffer = {
    b.total = b.total + a
    b.count = b.count + 1
    b
  }

  //分区聚合
  override def merge(b1: Buffer, b2: Buffer): Buffer = {
    b1.total = b1.total + b2.total
    b1.count = b1.count + b2.count
    b1
  }

  //计算结果
  override def finish(reduction: Buffer): Double = {
    reduction.total / reduction.count
  }

  //序列化
  override def bufferEncoder: Encoder[Buffer] = Encoders.product

  //序列化
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
