package com.atguigu.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Test01_Partitioner {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建sparkContext
    val sc: SparkContext = new SparkContext(conf)

    val intRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7), 2)

    //一般的RDD是不能够使用keyValue类型的算子的
    //必须是二元组类型的RDD才能够调用keyValue类型的算子
    val tupleRDD: RDD[(Int, Int)] = intRDD.map((_, 1))
    tupleRDD.mapPartitionsWithIndex((num, list) => list.map((num, _)))
      .collect().foreach(println)

    println("=======")

    //所有分区器都是按照key来进行分区的
    val result: RDD[(Int, Int)] = tupleRDD.partitionBy(new HashPartitioner(2))

    result.mapPartitionsWithIndex((num, list) => list.map((num, _)))
      .collect().foreach(println)

    println("=========")

    //repatriation走的也是hash分区器
    val value: RDD[(Int, Int)] = tupleRDD.repartition(2)
    value.mapPartitionsWithIndex((num, list) => list.map((num, _)))
      .collect().foreach(println)

    //4.关闭sc
    sc.stop()
  }
}
