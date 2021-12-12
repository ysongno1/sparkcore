package com.atguigu.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test01_Cache {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建sparkContext
    val sc: SparkContext = new SparkContext(conf)

    val lineRDD: RDD[String] = sc.makeRDD(List("hello world", "hello scala"), 2)

    val lineRDD1: RDD[String] = lineRDD.map(s => {
      println("执行几次")
      s
    })

    val wordRDD: RDD[String] = lineRDD1.flatMap(_.split(" "))

    val wordToOneRDD: RDD[(String, Int)] = wordRDD.map((_, 1))

    // 缓存之前的血缘关系
    println("缓存之前的血缘关系")
    println(wordToOneRDD.toDebugString)
    println("-------------------")

    wordToOneRDD.cache()//有了缓存之后 第二次调用行动算子就不会从头计算 直接从缓存里拿
    //cache底层调用的就是persist方法,缓存级别默认用的是MEMORY_ONLY
    wordToOneRDD.persist()//persist方法可以更改存储级别

    wordToOneRDD.collect().foreach(println)//第一次调用
    wordToOneRDD.collect().foreach(println)//第二次调用

    // 缓存之后的血缘关系
    //cache操作会增加缓存这个血缘关系，不改变原有的血缘关系
    println("缓存之后的血缘关系")
    println(wordToOneRDD.toDebugString)
    println("-------------------")

    // 使用reduceByKey
    // 如果有shuffle的话  自动会进行缓存 如果想重用数据，仍然建议调用persist或cache。
//    val result: RDD[(String, Int)] = wordToOneRDD.reduceByKey(_ + _)

//    result.collect().foreach(println)


    //4.关闭sc
    sc.stop()
  }
}
