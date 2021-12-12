package com.atguigu.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test07_CombineByKey {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建sparkContext
    val sc: SparkContext = new SparkContext(conf)

    val list: List[(String, Int)] = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
    val tupleRDD: RDD[(String, Int)] = sc.makeRDD(list, 2)

    //聚合相同的key,同时求出value的和及次数
    val result: RDD[(String, (Int, Int))] = tupleRDD.combineByKey(
      i => (i, 1),
      (res: (Int, Int), elem: Int) => (res._1 + elem, res._2 + 1),
      (res1: (Int, Int), res2: (Int, Int)) => (res1._1 + res2._1, res1._2 + res2._2)
    )

    result.mapPartitionsWithIndex((num, list) => list.map((num, _)))
      .collect().foreach(println)

    println("--------------")

    //求平均数
    val value: RDD[(String, Double)] = result.map(
      tuple => (tuple._1, tuple._2._1.toDouble / tuple._2._2))
    value.collect().foreach(println)

    println("---------------")

    val value1: RDD[(String, Double)] = result.map({
      case (str, tuple) => (str, tuple._1 / tuple._2.toDouble)
    })
    value1.collect().foreach(println)

    //4.关闭sc
    sc.stop()
  }
}
