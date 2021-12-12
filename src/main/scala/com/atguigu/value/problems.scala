package com.atguigu.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object problems {
  def main(args: Array[String]): Unit = {

    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建sparkContext
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("hello", "spark", "hello", "spark", "scala"))

    val value: RDD[(Char, Int)] = rdd.map(s => s.charAt(0))
      .map((_, 1))
      .reduceByKey(_ + _)

    value.collect().foreach(println)

    //4.关闭sc
    sc.stop()

  }
}
