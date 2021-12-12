package com.atguigu.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test03_ReduceByKey {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建sparkContext
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.makeRDD(List(("a",1),("b",5),("a",5),("b",2),("a",2),("b",4),("a",3),("c",6)),2)

    val value: RDD[(String, Int)] = rdd.reduceByKey(_ + _,3) // 不设置分区的话使用上一个RDD分区个数
    value.mapPartitionsWithIndex((num, list) => list.map((num, _)))
      .collect().foreach(println)

    //和上个RDD的分区数一样
    /*计算逻辑：
        预聚合：先reduceByKey各个分区 各个分区的结果再进行reduceByKey计算最后的结果
       去重的时候为什么不用groupByKey而用reduceByKey: 因为reduceByKey有预聚合
     */
    val value1: RDD[(String, Int)] = rdd.reduceByKey((res:Int,elem:Int) => res - elem)
    value1.mapPartitionsWithIndex((num,list) => list.map((num,_)))
      .collect().foreach(println)


    //4.关闭sc
    sc.stop()
  }
}
