package com.atguigu.keyValue

//省份广告被点击Top3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Problems {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建sparkContext
    val sc: SparkContext = new SparkContext(conf)

    val lineRDD: RDD[String] = sc.textFile("input/agent.log")

    // 步骤一: 选取有用的数据
    val tupleRDD: RDD[(String, String)] = lineRDD.map(s => {
      val strings: Array[String] = s.split(" ")
      (strings(1), strings(4))
    })

    // 步骤二: 聚合统计广告出现多少次
    // 原数据格式 (省份,广告id)
    // 转换格式 ("省份_广告id",1)
    // 转换格式 ((省份,广告id),1)
    val countRDD: RDD[((String, String), Int)] = tupleRDD.map(tuple => (tuple, 1))
      .reduceByKey(_ + _)

    // 合并前两步
    val countRDD1: RDD[((String, String), Int)] = lineRDD.map(s => {
      val strings: Array[String] = s.split(" ")
      ((strings(1), strings(4)), 1)
    }).reduceByKey(_ + _)


    // 步骤三: 聚合相同的省份
    // 转换格式 使用groupByKey 相对简单一些
    // 原(省份,广告id),1)
    // 转换格式  (省份,(广告id,次数))
    val idCountRDD: RDD[(String, (String, Int))] = countRDD.map({
      case ((pr, id), count) => (pr, (id, count))
    })

    // 聚合
    val prRDD: RDD[(String, Iterable[(String, Int)])] = idCountRDD.groupByKey()

    //步骤四: 针对已经分好组的数据  排序取top3
    val result: RDD[(String, List[(String, Int)])] = prRDD.mapValues(list => {
      list.toList
        .sortBy(_._2)(Ordering[Int].reverse)
        .take(3)
    })

    result.collect().foreach(println)


    //4.关闭sc
    sc.stop()
  }
}
