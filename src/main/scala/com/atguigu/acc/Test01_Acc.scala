package com.atguigu.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

import java.lang

object Test01_Acc {
  def main(args: Array[String]): Unit = {
    //1.创建spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    //2.创建sparkContext
    val sc: SparkContext = new SparkContext(conf)

    val intRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7), 2)

    // 使用基础的变量无法实现drive和executor端相加的需求
//    var sum = 0
//    val value: RDD[Int] = intRDD.map(i => {
//      println(s"sum:$sum")
//      sum += i
//      println(s"sum:$sum")
//      i
//    })

//    value.collect()

//    println(sum)


    // 需要使用累加器
    //（1）累加器定义（SparkContext.accumulator(initialValue)方法）
    val longAccumulator: LongAccumulator = sc.longAccumulator("sumAcc")

    //（2）累加器添加数据（累加器.add方法）
    // 如果累加器用在转换算子中  出现多少次计算累加器就会加多少次 会出现重复计算
    val value: RDD[Int] = intRDD.map(i => {
      longAccumulator.add(i)
      // 共享只写变量  (不是不能读  是你在executor读的时候数据不对)
      //println(longAccumulator.value)
      i
    })

//    value.collect()//调用一次结果是28
//    value.collect()//调用两次结果就是56

    //所以累加器应该使用在行动算子中
    intRDD.foreach(i => longAccumulator.add(i))

    //（3）累加器获取数据（累加器.value）
    val value1: lang.Long = longAccumulator.value
    println(value1)

    //4.关闭sc
    sc.stop()
  }
}
