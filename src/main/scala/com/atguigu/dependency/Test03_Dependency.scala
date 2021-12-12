package com.atguigu.dependency

/*
    Driver端执行代码先生成DAG 看他有几个job
    Application -> job -> stage -> task

  Application：初始化SC个数,没有意义

  多少个行动算子就多少个job
    (这个逻辑是死的 如果出现特殊情况 那是做了优化 底层可能调用了多个行动算子 所以启动了多个job)

  一个job从后往前找，多少个shuffle就有多少个stage

  task 该阶段只看最后一个RDD有几个分区就有几个Task

  通过localhost:4040查看DAG证明
 */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test03_Dependency {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建sparkContext
    val sc: SparkContext = new SparkContext(conf)  //1个App

    val lineRDD: RDD[String] = sc.textFile("input/1.txt",2)


    val flatRDD: RDD[String] = lineRDD.flatMap(_.split(" "))

    // 1.0 shuffleMapStage
    //    最后一个rdd的分区数 1个分区  1 task
    val tupleRDD: RDD[(String, Int)] = flatRDD.coalesce(1).map((_, 1))

    // shuffle切一刀
    // 2.0 resultStage
    //    2分区  2task
    val result: RDD[(String, Int)] = tupleRDD.reduceByKey(_ + _,2)


    // 一个行动算子代表一个job
    result.collect().foreach(println) //第1个job
    result.saveAsTextFile("output") //第 2个job


    Thread.sleep(600000)


    //4.关闭sc
    sc.stop()
  }
}
